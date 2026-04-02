use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use std::ffi::CString;
use std::time::Duration;

static WORKER_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(75);
static WORKER_BATCH_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(4096);
static MAX_ROW_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(8192);
static ENABLED_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);
static TARGET_DATABASE_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

pub fn is_capture_enabled() -> bool {
    ENABLED_GUC.get()
}

fn effective_worker_batch_size() -> usize {
    WORKER_BATCH_SIZE_GUC.get().clamp(128, 50_000) as usize
}

pub fn register_worker_and_guc() {
    GucRegistry::define_int_guc(
        c"pg_flashback.worker_interval_ms",
        c"pg_flashback delta worker flush interval",
        c"How often the background worker flushes staging events to flashback.delta_log (milliseconds).",
        &WORKER_INTERVAL_MS,
        10,
        10_000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.worker_batch_size",
        c"pg_flashback delta worker batch size",
        c"How many events the background worker moves from staging_events to delta_log per cycle.",
        &WORKER_BATCH_SIZE_GUC,
        128,
        50_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_flashback.enabled",
        c"Enable or disable pg_flashback capture",
        c"When OFF, triggers skip capture and worker idles. Emergency kill switch.",
        &ENABLED_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.max_row_size",
        c"pg_flashback maximum captured row size in bytes",
        c"Rows larger than this (in bytes) are skipped during capture to prevent OOM. Default 8192 (8KB).",
        &MAX_ROW_SIZE_GUC,
        512,
        104_857_600, // 100MB hard ceiling
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.target_database",
        c"Database the pg_flashback worker connects to",
        c"The background worker flushes staging_events in this database. Set to the database where the extension is installed. Default: postgres.",
        &TARGET_DATABASE_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    BackgroundWorkerBuilder::new("pg_flashback delta worker")
        .set_function("pg_flashback_delta_worker_main")
        .set_library("pg_flashback")
        .set_start_time(pgrx::bgworkers::BgWorkerStartTime::RecoveryFinished)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(1)))
        .load();
}

pub extern "C-unwind" fn pg_flashback_delta_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_setting = TARGET_DATABASE_GUC.get();
    let db_name = db_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("postgres");
    BackgroundWorker::connect_worker_to_spi(Some(db_name), None);

    log!("pg_flashback delta worker started (database: {db_name})");

    loop {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // When pg_flashback.enabled = off, worker idles completely.
        if is_capture_enabled() {
            flush_staging_to_delta_log();

            // Skip checkpoint and retention during active restore to avoid
            // snapshotting a partially-restored table or purging needed deltas.
            // Worker is a separate process, so check advisory locks via SPI.
            if !is_any_restore_active() {
                run_periodic_checkpoints();
                run_retention_purge();
            }
        }

        let interval_ms = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u64;
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(interval_ms))) {
            break;
        }
    }

    if is_capture_enabled() {
        flush_staging_to_delta_log();
        if !is_any_restore_active() {
            run_periodic_checkpoints();
            run_retention_purge();
        }
    }
    log!("pg_flashback delta worker stopped");
}

/// Check if any backend is performing a flashback restore by looking for
/// the advisory lock key used by flashback_restore (classid = 358944).
/// This runs in the worker process (separate address space) so the
/// process-local AtomicBool is not useful here.
fn is_any_restore_active() -> bool {
    let result: Result<bool, SpiError> = BackgroundWorker::transaction(|| {
        let active = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 358944 AND granted)",
        )?
        .unwrap_or(false);
        Ok(active)
    });
    result.unwrap_or(false)
}

fn flush_staging_to_delta_log() {
    let batch_size = effective_worker_batch_size() as i64;
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        // Skip if staging_events table doesn't exist yet (extension not fully installed)
        let table_exists =
            Spi::get_one::<bool>("SELECT to_regclass('flashback.staging_events') IS NOT NULL")?
                .unwrap_or(false);
        if !table_exists {
            return Ok(());
        }
        Spi::run_with_args(
            "WITH moved AS (
                DELETE FROM flashback.staging_events
                WHERE staging_id IN (
                    SELECT staging_id
                    FROM flashback.staging_events
                    ORDER BY staging_id
                    LIMIT $1
                )
                RETURNING *
            )
            INSERT INTO flashback.delta_log (
                event_time, event_type, table_name, rel_oid, source_xid,
                committed_at, schema_version, old_data, new_data
            )
            SELECT
                m.event_time, m.event_type, m.table_name, m.rel_oid, m.source_xid,
                clock_timestamp(),
                COALESCE((
                    SELECT sv.schema_version
                    FROM flashback.schema_versions sv
                    WHERE sv.rel_oid = m.rel_oid
                      AND sv.applied_at <= m.event_time
                    ORDER BY sv.schema_version DESC
                    LIMIT 1
                ), 1),
                m.old_data, m.new_data
            FROM moved m
            WHERE EXISTS (
                SELECT 1 FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = m.rel_oid
                  AND tt.is_active
                  AND m.event_time >= tt.tracked_since
            )",
            &[batch_size.into()],
        )?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback STAGING_FLUSH_ERROR error={err:?}");
    }
}

fn run_periodic_checkpoints() {
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run(
            "DO $$
                         BEGIN
                             IF to_regprocedure('flashback_take_due_checkpoints()') IS NOT NULL THEN
                                 PERFORM flashback_take_due_checkpoints();
                             END IF;
                         END
                         $$",
        )?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback CHECKPOINT_WORKER_ERROR error={err:?}");
    }
}

fn run_retention_purge() {
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run(
            "DO $$
                         BEGIN
                             IF to_regprocedure('flashback_apply_retention()') IS NOT NULL THEN
                                 PERFORM flashback_apply_retention();
                             END IF;
                         END
                         $$",
        )?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback RETENTION_PURGE_ERROR error={err:?}");
    }
}

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use std::ffi::CString;
use std::time::Duration;

static WORKER_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(75);
static WORKER_BATCH_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(4096);
static MAX_ROW_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(65536);
static ENABLED_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);
static TARGET_DATABASE_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Comma-separated list of databases. Each gets its own background worker.
/// When set, overrides the single `target_database` GUC.
static TARGET_DATABASES_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Maximum background workers to register (Postmaster context, requires restart).
static MAX_WORKERS_GUC: GucSetting<i32> = GucSetting::<i32>::new(4);
/// Capture mode: 'wal' (WAL-based via logical decoding), 'trigger' (legacy trigger-based),
/// or 'auto' (use WAL if wal_level=logical, otherwise fallback to triggers).
static CAPTURE_MODE_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Logical replication slot name. Allows multiple pg_flashback instances on the same cluster.
static SLOT_NAME_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// work_mem override for snapshot bulk load during flashback_restore.
static RESTORE_WORK_MEM_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// maintenance_work_mem override for deferred index builds during flashback_restore.
static INDEX_BUILD_WORK_MEM_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub fn is_capture_enabled() -> bool {
    ENABLED_GUC.get()
}

/// Returns the configured replication slot name, falling back to 'pg_flashback_slot'.
fn effective_slot_name() -> String {
    SLOT_NAME_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(|s| s.to_string()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "pg_flashback_slot".to_string())
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
        c"Rows larger than this (in bytes) are skipped during capture to prevent OOM. Default 65536 (64KB).",
        &MAX_ROW_SIZE_GUC,
        512,
        104_857_600, // 100MB hard ceiling
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.target_database",
        c"Database the pg_flashback worker connects to",
        c"The background worker flushes staging_events in this database. Set to the database where the extension is installed. Default: postgres. Overridden by target_databases if set.",
        &TARGET_DATABASE_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.target_databases",
        c"Comma-separated list of databases for pg_flashback workers",
        c"Each database gets its own background worker. Overrides target_database when set. Example: 'db1,db2,db3'.",
        &TARGET_DATABASES_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.max_workers",
        c"Maximum number of pg_flashback background workers",
        c"Each worker handles one database. Extra workers beyond the database count exit gracefully. Requires restart.",
        &MAX_WORKERS_GUC,
        1,
        8,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.capture_mode",
        c"pg_flashback capture mode: auto, wal, or trigger",
        c"'wal' = WAL-based logical decoding (requires wal_level=logical), 'trigger' = legacy trigger-based capture, 'auto' = detect wal_level and choose (default: auto).",
        &CAPTURE_MODE_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.slot_name",
        c"Logical replication slot name used by pg_flashback",
        c"Override when running multiple pg_flashback installations on the same cluster. Default: pg_flashback_slot.",
        &SLOT_NAME_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.restore_work_mem",
        c"work_mem for snapshot bulk load during flashback_restore",
        c"Passed to set_config('work_mem') before INSERT ... SELECT snapshot load. Higher values speed up large tables. Default: 256MB.",
        &RESTORE_WORK_MEM_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.index_build_work_mem",
        c"maintenance_work_mem for deferred index builds during flashback_restore",
        c"Passed to set_config('maintenance_work_mem') before building PK and secondary indexes on shadow table. Default: 512MB.",
        &INDEX_BUILD_WORK_MEM_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    // Register up to max_workers background workers.
    // Each worker receives its index (0-based) as the argument.
    // At runtime, each worker reads the database list and connects to
    // its assigned database, or exits if no database is assigned.
    let max_w = MAX_WORKERS_GUC.get().clamp(1, 8) as usize;
    for i in 0..max_w {
        let name = if i == 0 {
            "pg_flashback delta worker".to_string()
        } else {
            format!("pg_flashback delta worker {}", i)
        };

        BackgroundWorkerBuilder::new(&name)
            .set_function("pg_flashback_delta_worker_main")
            .set_library("pg_flashback")
            .set_argument((i as i32).into_datum())
            .set_start_time(pgrx::bgworkers::BgWorkerStartTime::RecoveryFinished)
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(1)))
            .load();
    }
}

/// Resolve the list of target databases from GUCs.
/// Priority: target_databases (comma-separated) > target_database > "postgres".
fn resolve_database_list() -> Vec<String> {
    // Check target_databases first (comma-separated list)
    if let Some(cs) = TARGET_DATABASES_GUC.get() {
        if let Ok(s) = cs.to_str() {
            let dbs: Vec<String> = s
                .split(',')
                .map(|d| d.trim().to_string())
                .filter(|d| !d.is_empty())
                .collect();
            if !dbs.is_empty() {
                return dbs;
            }
        }
    }

    // Fallback to single target_database
    let db_setting = TARGET_DATABASE_GUC.get();
    let db_name = db_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("postgres");
    vec![db_name.to_string()]
}

pub extern "C-unwind" fn pg_flashback_delta_worker_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let worker_index = unsafe { i32::from_datum(arg, false) }.unwrap_or(0) as usize;

    let db_list = resolve_database_list();

    // If this worker's index exceeds the database list, exit gracefully.
    if worker_index >= db_list.len() {
        log!(
            "pg_flashback worker {} exiting: only {} database(s) configured",
            worker_index,
            db_list.len()
        );
        return;
    }

    let db_name = &db_list[worker_index];
    BackgroundWorker::connect_worker_to_spi(Some(db_name), None);

    // Detect capture mode once at startup
    // Note: for 'auto', the mode is re-evaluated each cycle in the loop.
    // At startup we just log the initial mode without SPI to avoid
    // polluting the transaction state before slot creation.
    let mode_setting = CAPTURE_MODE_GUC.get();
    let initial_mode_str = mode_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("auto");
    log!(
        "pg_flashback delta worker {worker_index} started (database: {db_name}, capture_mode_guc: {initial_mode_str}, {total} total database(s))",
        total = db_list.len()
    );

    // Track whether slot is ready (lazily created on first WAL cycle)
    let mut slot_ready = false;
    let mut slot_warned = false;

    loop {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // When pg_flashback.enabled = off, worker idles completely.
        if is_capture_enabled() {
            let cycle_start = std::time::Instant::now();

            let mode = effective_capture_mode();

            let t0 = std::time::Instant::now();
            if mode == "wal" {
                // Lazily ensure slot exists (handles upgrade from trigger-only versions)
                if !slot_ready {
                    slot_ready = ensure_replication_slot();
                    if !slot_ready && !slot_warned {
                        log!("pg_flashback: replication slot '{}' not found, waiting for flashback_track() to create it", effective_slot_name());
                        slot_warned = true;
                    }
                }
                if slot_ready {
                    consume_wal_changes();
                }
            } else {
                slot_ready = false; // reset if mode changes away from WAL
            }
            // Always flush staging_events (DDL events go through staging even in WAL mode)
            flush_staging_to_delta_log();
            let flush_ms = t0.elapsed().as_millis();

            // Skip checkpoint and retention during active restore to avoid
            // snapshotting a partially-restored table or purging needed deltas.
            // Worker is a separate process, so check advisory locks via SPI.
            let mut ckpt_ms: u128 = 0;
            let mut retention_ms: u128 = 0;
            if !is_any_restore_active() {
                let t1 = std::time::Instant::now();
                run_periodic_checkpoints();
                run_ensure_partitions();
                ckpt_ms = t1.elapsed().as_millis();

                let t2 = std::time::Instant::now();
                run_retention_purge();
                retention_ms = t2.elapsed().as_millis();
            }

            let cycle_ms = cycle_start.elapsed().as_millis();
            let interval_ms_val = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u128;
            if cycle_ms > interval_ms_val {
                warning!(
                    "pg_flashback WORKER_SLOW_CYCLE cycle_ms={cycle_ms} flush_ms={flush_ms} ckpt_ms={ckpt_ms} retention_ms={retention_ms} interval_ms={interval_ms_val}"
                );
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
    log!("pg_flashback delta worker {worker_index} stopped (database: {db_name})");
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

/// Determine the effective capture mode based on GUC and wal_level.
/// Returns "wal" or "trigger".
///
/// Does NOT use SPI — reads wal_level directly via GetConfigOption
/// to avoid polluting transaction state in the background worker.
fn effective_capture_mode() -> &'static str {
    let mode_setting = CAPTURE_MODE_GUC.get();
    let mode = mode_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("auto");

    match mode {
        "wal" => "wal",
        "trigger" => "trigger",
        _ => {
            // auto: check wal_level via C API (no SPI needed)
            let wal_level = unsafe {
                let opt_name = std::ffi::CString::new("wal_level").unwrap();
                let val = pg_sys::GetConfigOption(opt_name.as_ptr(), true, false);
                if val.is_null() {
                    "replica".to_string()
                } else {
                    std::ffi::CStr::from_ptr(val)
                        .to_str()
                        .unwrap_or("replica")
                        .to_string()
                }
            };
            if wal_level == "logical" {
                "wal"
            } else {
                "trigger"
            }
        }
    }
}

/// Ensure the pg_flashback logical replication slot exists.
/// Returns true if slot is ready.
///
/// NOTE: Slot creation by the background worker is unreliable because
/// pg_create_logical_replication_slot() requires a clean, write-free
/// transaction. Instead, the slot should be created by flashback_track()
/// or manually by the DBA. This function only checks for existence.
fn ensure_replication_slot() -> bool {
    let slot_name = effective_slot_name();
    let result: Result<bool, SpiError> = BackgroundWorker::transaction(|| {
        let exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[slot_name.as_str().into()],
        )?
        .unwrap_or(false);
        Ok(exists)
    });
    result.unwrap_or(false)
}

/// Consume WAL changes from the logical replication slot and insert into delta_log.
/// Each change is a JSON line produced by our output plugin (_PG_output_plugin_init).
fn consume_wal_changes() {
    let slot_name = effective_slot_name();
    let batch_size = effective_worker_batch_size() as i32;
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        let table_exists =
            Spi::get_one::<bool>("SELECT to_regclass('flashback.delta_log') IS NOT NULL")?
                .unwrap_or(false);
        if !table_exists {
            return Ok(());
        }

        // Consume changes from the slot in a batch
        // pg_logical_slot_get_changes returns (lsn, xid, data) rows
        // Handles both DML events (from change_cb) and DDL events (from message_cb)
        //
        // slot_name cannot be passed as a bind parameter to pg_logical_slot_get_changes
        // (it only accepts a literal name). The slot name originates from a superuser-only
        // GUC (pg_flashback.slot_name), so we sanitize to alphanumeric/underscore/hyphen
        // characters only before embedding it in the query string.
        let safe_slot: String = slot_name
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .collect();
        let query = format!(
            "WITH wal_changes AS (
                SELECT lsn, xid, data
                FROM pg_logical_slot_get_changes(
                    '{}', NULL, $1
                )
            ),
            parsed AS (
                SELECT
                    (data::jsonb)->>'op' AS event_type,
                    format('%I.%I', (data::jsonb)->>'schema', (data::jsonb)->>'table') AS table_name,
                    ((data::jsonb)->>'oid')::oid AS rel_oid,
                    xid::text::bigint AS source_xid,
                    (data::jsonb)->'old' AS old_data,
                    (data::jsonb)->'new' AS new_data,
                    (data::jsonb)->'ddl_info' AS ddl_info,
                    ((data::jsonb)->>'schema_version')::bigint AS msg_schema_version
                FROM wal_changes
                WHERE (data::jsonb)->>'op' IS NOT NULL
                  AND (data::jsonb)->>'op' IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
                                                'DROP', 'ALTER')
            )
            INSERT INTO flashback.delta_log (
                event_time, event_type, table_name, rel_oid, source_xid,
                committed_at, schema_version, old_data, new_data, ddl_info
            )
            SELECT
                clock_timestamp(),
                p.event_type,
                p.table_name,
                p.rel_oid,
                p.source_xid,
                clock_timestamp(),
                COALESCE(p.msg_schema_version, (
                    SELECT sv.schema_version
                    FROM flashback.schema_versions sv
                    WHERE sv.rel_oid = p.rel_oid
                      AND sv.applied_at <= clock_timestamp()
                    ORDER BY sv.schema_version DESC
                    LIMIT 1
                ), 1),
                p.old_data,
                p.new_data,
                p.ddl_info
            FROM parsed p
            WHERE EXISTS (
                SELECT 1 FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = p.rel_oid
                  AND tt.is_active
            )",
            safe_slot
        );
        Spi::run_with_args(&query, &[batch_size.into()])?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback WAL_CONSUME_ERROR error={err:?}");
    }
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

        // In WAL mode, DML comes from the slot and DDL comes from WAL messages.
        // staging_events only holds DML events from trigger mode.
        // So in WAL mode, staging_events should be empty — skip the flush.
        let mode = effective_capture_mode();
        if mode == "wal" {
            return Ok(());
        }

        let query = "WITH moved AS (
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
                -- Use the actual transaction commit timestamp when track_commit_timestamp
                -- is enabled. This makes event_time commit-time-correct for PITR accuracy.
                -- Without it, event_time is the trigger's clock_timestamp() at statement
                -- execution, which can precede the actual commit for long-running transactions.
                COALESCE(
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pg_settings
                        WHERE name = 'track_commit_timestamp' AND setting = 'on'
                    ) THEN pg_xact_commit_timestamp(m.source_xid::text::xid) END,
                    m.event_time
                ) AS event_time,
                m.event_type, m.table_name, m.rel_oid, m.source_xid,
                COALESCE(
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pg_settings
                        WHERE name = 'track_commit_timestamp' AND setting = 'on'
                    ) THEN pg_xact_commit_timestamp(m.source_xid::text::xid) END,
                    clock_timestamp()
                ) AS committed_at,
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
            )";

        Spi::run_with_args(&query, &[batch_size.into()])?;
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

/// Ensure delta_log has a partition covering today (and next month if near month-end).
/// No-op if delta_log is not partitioned.
fn run_ensure_partitions() {
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run(
            "DO $$
             BEGIN
                 IF to_regprocedure('flashback_ensure_delta_partition(date)') IS NOT NULL THEN
                     PERFORM flashback_ensure_delta_partition(CURRENT_DATE);
                 END IF;
             END
             $$",
        )?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback PARTITION_ENSURE_ERROR error={err:?}");
    }
}

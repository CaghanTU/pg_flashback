use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use std::ffi::CString;
use std::time::Duration;

static WORKER_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(75);
static WORKER_BATCH_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(4096);
static MAINTENANCE_EVERY_N_CYCLES_GUC: GucSetting<i32> = GucSetting::<i32>::new(4);
static MAINTENANCE_LOCK_TIMEOUT_MS_GUC: GucSetting<i32> = GucSetting::<i32>::new(250);
static MAINTENANCE_STATEMENT_TIMEOUT_MS_GUC: GucSetting<i32> = GucSetting::<i32>::new(2_000);
static MAX_ROW_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(65536);
static ENABLED_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);
static TARGET_DATABASE_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Comma-separated list of databases. Each gets its own background worker.
/// When set, overrides the single `target_database` GUC.
static TARGET_DATABASES_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Maximum database worker pairs to register (capture + maintenance per DB).
static MAX_WORKERS_GUC: GucSetting<i32> = GucSetting::<i32>::new(4);
/// Capture mode: 'wal' (WAL-based via logical decoding), 'trigger' (legacy trigger-based),
/// or 'auto' (use WAL if wal_level=logical, otherwise fallback to triggers).
static CAPTURE_MODE_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// Logical replication slot name override. Default is per-database
/// (pg_flashback_<dbname>) because logical slots are database-specific.
static SLOT_NAME_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// work_mem override for snapshot bulk load during flashback_restore.
static RESTORE_WORK_MEM_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// maintenance_work_mem override for deferred index builds during flashback_restore.
static INDEX_BUILD_WORK_MEM_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Maximum projected local-restore peak size (pg_size_bytes text). Empty/0 disables.
static LOCAL_RESTORE_MAX_PEAK_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Extra safety reserve included in local-restore peak estimates.
static LOCAL_RESTORE_SAFETY_RESERVE_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Root-owned/shared secret used to authenticate repository-derived proofs.
static PROOF_HMAC_KEY_FILE_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub fn is_capture_enabled() -> bool {
    ENABLED_GUC.get()
}

pub fn proof_hmac_key_file() -> Option<String> {
    PROOF_HMAC_KEY_FILE_GUC
        .get()
        .and_then(|value| value.to_str().ok().map(ToOwned::to_owned))
        .filter(|value| !value.trim().is_empty())
}

/// SQL expression yielding the effective replication slot name for the
/// connected database. Mirrors flashback_effective_slot_name() but works
/// even before the extension is installed in this database.
/// Slot names are cluster-wide unique while logical slots are
/// database-specific, so the default derives from the database name.
const EFFECTIVE_SLOT_NAME_SQL: &str = "COALESCE(
        NULLIF(current_setting('pg_flashback.slot_name', true), ''),
        left('pg_flashback_' ||
             lower(regexp_replace(current_database(), '[^a-zA-Z0-9_]', '_', 'g')),
             63)
    )";

fn effective_worker_batch_size() -> usize {
    WORKER_BATCH_SIZE_GUC.get().clamp(128, 50_000) as usize
}

pub fn register_worker_and_guc() {
    GucRegistry::define_int_guc(
        c"pg_flashback.worker_interval_ms",
        c"pg_flashback delta worker flush interval",
        c"How often the background worker flushes staging events to flashback.delta_log (milliseconds).",
        &WORKER_INTERVAL_MS,
        50,
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

    GucRegistry::define_int_guc(
        c"pg_flashback.maintenance_every_n_cycles",
        c"Run pg_flashback maintenance every N capture cycles",
        c"Checkpoints, partition creation, and retention run only after this many successful enabled capture cycles.",
        &MAINTENANCE_EVERY_N_CYCLES_GUC,
        1,
        10_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.maintenance_lock_timeout_ms",
        c"Lock wait limit for pg_flashback maintenance",
        c"Maximum time a checkpoint, partition, or retention maintenance transaction waits for a lock before yielding to capture.",
        &MAINTENANCE_LOCK_TIMEOUT_MS_GUC,
        10,
        60_000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.maintenance_statement_timeout_ms",
        c"Statement limit for pg_flashback maintenance",
        c"Maximum runtime for each checkpoint, partition, or retention maintenance transaction before it yields to capture.",
        &MAINTENANCE_STATEMENT_TIMEOUT_MS_GUC,
        50,
        300_000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
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
        c"Maximum number of pg_flashback database worker pairs",
        c"Each configured database gets one capture worker and one maintenance worker. Requires two background-worker slots per database and a restart.",
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
        c"Overrides the per-database default (pg_flashback_<dbname>). Slots are database-specific, so set this only on single-database installs.",
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

    GucRegistry::define_string_guc(
        c"pg_flashback.local_restore_max_peak_bytes",
        c"Reject local restores whose projected peak exceeds this size",
        c"Accepted by pg_size_bytes(). Empty or 0 disables the gate. Set a conservative filesystem headroom budget so local restore fails closed before ACCESS EXCLUSIVE.",
        &LOCAL_RESTORE_MAX_PEAK_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.local_restore_safety_reserve_bytes",
        c"Safety reserve included in local restore peak estimates",
        c"Accepted by pg_size_bytes() and added to twice the live relation size. Default when unset: 64MB.",
        &LOCAL_RESTORE_SAFETY_RESERVE_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.proof_hmac_key_file",
        c"Server-side HMAC key file for verified backup proofs",
        c"Path to a regular 0600 file containing exactly 32 bytes as 64 hexadecimal characters. Required for non-superuser recovery-agent proof installation.",
        &PROOF_HMAC_KEY_FILE_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    // Register one correctness-critical capture worker and one independently
    // bounded maintenance worker for each configured database. Registering
    // only actual targets avoids consuming max_worker_processes slots for the
    // unused capacity of max_workers.
    let max_w = MAX_WORKERS_GUC.get().clamp(1, 8) as usize;
    let configured = resolve_database_list();
    let worker_pairs = configured.len().min(max_w);
    if configured.len() > max_w {
        warning!(
            "pg_flashback configured {} databases but max_workers permits only {} worker pairs",
            configured.len(),
            max_w
        );
    }
    for i in 0..worker_pairs {
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

        let maintenance_name = if i == 0 {
            "pg_flashback maintenance worker".to_string()
        } else {
            format!("pg_flashback maintenance worker {}", i)
        };
        BackgroundWorkerBuilder::new(&maintenance_name)
            .set_function("pg_flashback_maintenance_worker_main")
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
    let mut last_enabled: Option<bool> = None;
    let mut last_mode: Option<&'static str> = None;

    loop {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        let enabled = is_capture_enabled();
        let mode = effective_capture_mode();
        if last_enabled != Some(enabled) || last_mode != Some(mode) {
            // Reconcile in a LOGGED database transaction before the worker
            // acts on a changed GUC. A disabled/non-WAL qualified stream is
            // frozen with durable gaps first; re-enabling creates a fresh
            // epoch and still requires explicit re-anchor.
            if !reconcile_capture_configuration() {
                // Fail closed: do not apply the new enabled/mode behavior
                // until the LOGGED discontinuity transaction succeeds.
                let interval_ms = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u64;
                if !BackgroundWorker::wait_latch(Some(Duration::from_millis(interval_ms))) {
                    break;
                }
                continue;
            }
            last_enabled = Some(enabled);
            last_mode = Some(mode);
        }

        // When pg_flashback.enabled = off, the worker idles only after the
        // configuration discontinuity above has been durably recorded.
        if enabled {
            let cycle_start = std::time::Instant::now();

            let t0 = std::time::Instant::now();
            if mode == "wal" {
                // Lazily ensure slot exists (handles upgrade from trigger-only versions)
                if !slot_ready {
                    slot_ready = ensure_replication_slot();
                    if !slot_ready && !slot_warned {
                        log!("pg_flashback: no replication slot for database {db_name}, waiting for flashback_track() to create it");
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

            let cycle_ms = cycle_start.elapsed().as_millis();
            let interval_ms_val = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u128;
            if cycle_ms > interval_ms_val {
                warning!(
                    "pg_flashback CAPTURE_SLOW_CYCLE cycle_ms={cycle_ms} flush_ms={flush_ms} interval_ms={interval_ms_val}"
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
    }
    log!("pg_flashback delta worker {worker_index} stopped (database: {db_name})");
}

pub extern "C-unwind" fn pg_flashback_maintenance_worker_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let worker_index = unsafe { i32::from_datum(arg, false) }.unwrap_or(0) as usize;
    let db_list = resolve_database_list();
    if worker_index >= db_list.len() {
        return;
    }
    let db_name = &db_list[worker_index];
    BackgroundWorker::connect_worker_to_spi(Some(db_name), None);
    log!("pg_flashback maintenance worker {worker_index} started (database: {db_name})");

    loop {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        if is_capture_enabled() && !is_any_restore_active() {
            run_periodic_checkpoints();
            run_ensure_partitions();
            run_retention_purge();
        }
        let capture_interval = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u64;
        let cadence = MAINTENANCE_EVERY_N_CYCLES_GUC.get().clamp(1, 10_000) as u64;
        let wait_ms = capture_interval.saturating_mul(cadence).min(600_000);
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(wait_ms))) {
            break;
        }
    }
    log!("pg_flashback maintenance worker {worker_index} stopped (database: {db_name})");
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

fn reconcile_capture_configuration() -> bool {
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        let fn_exists = Spi::get_one::<bool>(
            "SELECT to_regprocedure('flashback_reconcile_capture_configuration()') IS NOT NULL",
        )?
        .unwrap_or(false);
        if fn_exists {
            Spi::run("SELECT flashback_reconcile_capture_configuration()")?;
        }
        Ok(())
    });

    match result {
        Ok(()) => true,
        Err(err) => {
            log!("pg_flashback CAPTURE_CONFIGURATION_RECONCILE_ERROR error={err:?}");
            false
        }
    }
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

/// Ensure the pg_flashback logical replication slot exists FOR THIS DATABASE.
/// Returns true if slot is ready.
///
/// Logical slots are database-specific: a slot with the right name that was
/// created in another database cannot decode this database's changes, so the
/// check is scoped to current_database().
///
/// NOTE: Slot creation by the background worker is unreliable because
/// pg_create_logical_replication_slot() requires a clean, write-free
/// transaction. Instead, the slot should be created by flashback_track()
/// or manually by the DBA. This function only checks for existence.
fn ensure_replication_slot() -> bool {
    let result: Result<bool, SpiError> = BackgroundWorker::transaction(|| {
        let exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(
                 SELECT 1 FROM pg_replication_slots
                 WHERE slot_name = {EFFECTIVE_SLOT_NAME_SQL}
                   AND database = current_database()
             )"
        ))?
        .unwrap_or(false);
        Ok(exists)
    });
    result.unwrap_or(false)
}

/// Consume WAL changes from the logical replication slot and insert into delta_log.
/// The heavy lifting lives in the SQL function flashback_consume_wal(), which
/// stamps events with the real commit time and change LSN. Skips silently when
/// the extension is not (yet) installed in this database.
fn consume_wal_changes() {
    let batch_size = effective_worker_batch_size() as i32;
    let locked: Result<bool, SpiError> = BackgroundWorker::transaction(|| {
        let fn_exists = Spi::get_one::<bool>(
            "SELECT to_regprocedure('flashback_consume_wal(integer)') IS NOT NULL",
        )?
        .unwrap_or(false);
        if !fn_exists {
            return Ok(false);
        }

        // Use a session lock so the transaction that waited for a concurrent
        // track/re-anchor can commit before decoding starts. The next
        // BackgroundWorker::transaction gets a fresh READ COMMITTED snapshot
        // while this backend still owns the stream lock.
        Spi::run(
            "SELECT pg_advisory_lock(
                 358945::integer,
                 (SELECT oid::integer FROM pg_database
                  WHERE datname = current_database())
             )",
        )?;
        Ok(true)
    });

    let Ok(true) = locked else {
        if let Err(err) = locked {
            log!("pg_flashback WAL_CONSUME_LOCK_ERROR error={err:?}");
        }
        return;
    };

    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run_with_args("SELECT flashback_consume_wal($1)", &[batch_size.into()])?;
        Ok(())
    });

    let unlock_result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run(
            "SELECT pg_advisory_unlock(
                 358945::integer,
                 (SELECT oid::integer FROM pg_database
                  WHERE datname = current_database())
             )",
        )
    });

    if let Err(err) = result {
        log!("pg_flashback WAL_CONSUME_ERROR error={err:?}");
    }
    if let Err(err) = unlock_result {
        log!("pg_flashback WAL_CONSUME_UNLOCK_ERROR error={err:?}");
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

        // In WAL mode staging_events is normally empty (DML comes from the
        // slot, DDL from WAL messages), but flushing unconditionally is cheap
        // and drains any events left behind by a trigger→wal mode switch.
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
                  AND tt.recovery_profile = 'local_delta'
                  AND m.event_time >= tt.tracked_since
            )
            -- event_id assignment must follow capture order: replay's
            -- net-effect computation orders events by event_id.
            ORDER BY m.staging_id";

        Spi::run_with_args(query, &[batch_size.into()])?;
        Ok(())
    });

    if let Err(err) = result {
        log!("pg_flashback STAGING_FLUSH_ERROR error={err:?}");
    }
}

fn run_periodic_checkpoints() {
    run_bounded_maintenance(
        "CHECKPOINT_WORKER",
        "DO $$
                         BEGIN
                             IF to_regprocedure('flashback_take_due_checkpoints()') IS NOT NULL THEN
                                 PERFORM flashback_take_due_checkpoints();
                             END IF;
                         END
                         $$",
    );
}

fn run_retention_purge() {
    run_bounded_maintenance(
        "RETENTION_PURGE",
        "DO $$
                         BEGIN
                             IF to_regprocedure('flashback_apply_retention()') IS NOT NULL THEN
                                 PERFORM flashback_apply_retention();
                             END IF;
                         END
                         $$",
    );
}

/// Ensure delta_log has a partition covering today (and next month if near month-end).
/// No-op if delta_log is not partitioned.
fn run_ensure_partitions() {
    run_bounded_maintenance(
        "PARTITION_ENSURE",
        "DO $$
             BEGIN
                 IF to_regprocedure('flashback_ensure_delta_partition(date)') IS NOT NULL THEN
                     PERFORM flashback_ensure_delta_partition(CURRENT_DATE);
                 END IF;
             END
             $$",
    );
}

/// Run a best-effort maintenance transaction that cannot monopolize the
/// capture worker. Errors include lock_timeout and statement_timeout; both
/// abort only this transaction and are retried on a later maintenance cycle.
fn run_bounded_maintenance(operation: &str, query: &str) {
    let lock_timeout_ms = MAINTENANCE_LOCK_TIMEOUT_MS_GUC.get().clamp(10, 60_000);
    let statement_timeout_ms = MAINTENANCE_STATEMENT_TIMEOUT_MS_GUC
        .get()
        .clamp(50, 300_000);
    let result: Result<(), SpiError> = BackgroundWorker::transaction(|| {
        Spi::run(&format!(
            "SELECT set_config('lock_timeout', '{lock_timeout_ms}ms', true);
             SELECT set_config('statement_timeout', '{statement_timeout_ms}ms', true);
             {query}"
        ))?;
        Ok(())
    });

    if let Err(err) = result {
        log!(
            "pg_flashback {operation}_ERROR lock_timeout_ms={lock_timeout_ms} statement_timeout_ms={statement_timeout_ms} error={err:?}"
        );
    }
}

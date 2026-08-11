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
/// Capture mode: only 'wal' is operational. 'trigger' and 'auto' remain as
/// deprecated compatibility inputs and fail closed (no capture).
static CAPTURE_MODE_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"wal"));
/// Logical replication slot name override. Default is per-database
/// (pg_flashback_<dbname>) because logical slots are database-specific.
static SLOT_NAME_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// work_mem override for snapshot bulk load during flashback_restore.
static RESTORE_WORK_MEM_GUC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
/// maintenance_work_mem override for deferred index builds during flashback_restore.
static INDEX_BUILD_WORK_MEM_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Maximum projected local base/snapshot bytes (heap+TOAST). Empty/0 fail-closed.
static LOCAL_MAX_SNAPSHOT_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Maximum projected local restore peak bytes. Empty/0 fail-closed.
static LOCAL_MAX_RESTORE_PEAK_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Minimum filesystem free space that must remain after projected local writes.
static LOCAL_MIN_FILESYSTEM_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Extra safety reserve included in local capacity estimates.
static LOCAL_SAFETY_RESERVE_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Maximum lock-wait / write-stall budget for exact local boundaries (ms).
static LOCAL_BOUNDARY_WRITE_STALL_MS_GUC: GucSetting<i32> = GucSetting::<i32>::new(30_000);
/// Assumed local CTAS copy throughput used only for stall estimation (MiB/s).
static LOCAL_ASSUMED_COPY_MIB_PER_SEC_GUC: GucSetting<i32> = GucSetting::<i32>::new(32);
/// Privileged override that admits local capacity failures; never a silent default.
static LOCAL_CAPACITY_OVERRIDE_GUC: GucSetting<bool> = GucSetting::<bool>::new(false);
/// Test-only restore failpoint name. Empty = disabled (release default).
/// Superuser-only (SUSET). Not a production control plane.
static ALLOW_UNAUDITED_RESTORE_GUC: GucSetting<bool> = GucSetting::<bool>::new(false);
static TEST_RESTORE_FAILPOINT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Test-only consume_wal failpoint name. Empty = disabled (release default).
/// Superuser-only (SUSET). Not a production control plane.
static TEST_CONSUME_WAL_FAILPOINT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Slot retained-WAL warning threshold for health projection.
static SLOT_LAG_WARNING_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Slot retained-WAL at-risk threshold for health projection.
static SLOT_LAG_AT_RISK_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

// ---- Step 9: external_zstd SnapshotStore backend ----
/// Absolute directory outside PGDATA and every tablespace where external_zstd
/// artifacts are stored. Must already exist, be owned by the running OS user,
/// mode 0700. Empty = the external backend is unavailable. POSTMASTER context:
/// changing the artifact root while the server is running is never safe, so
/// this can only be set at server start (config file/command line), never by
/// ALTER SYSTEM + reload and never by an in-session SET.
static EXTERNAL_SNAPSHOT_ROOT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Backend used for the NEXT artifact reservation, cluster-wide: 'heap_v1'
/// (default) or 'external_zstd'. SIGHUP, not SUSET: a single session must
/// never be able to silently change which backend the next generation of a
/// protected table uses -- only an explicit, cluster-visible config file
/// change + reload can. The backend actually used for a given generation is
/// resolved once, at reservation time, and frozen on that generation's own
/// row from then on; this GUC is never re-read afterward.
static SNAPSHOT_STORAGE_BACKEND_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"heap_v1"));
/// Minimum external_snapshot_root filesystem free space that must remain
/// after a projected persist. Accepted by pg_size_bytes(). Empty/0 fails closed.
static EXTERNAL_SNAPSHOT_MIN_FREE_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Extra safety reserve included in external persist capacity estimates.
/// Accepted by pg_size_bytes(). Empty/0 fails closed.
static EXTERNAL_SNAPSHOT_SAFETY_RESERVE_BYTES_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// Row-batch size for the SPI cursor driving external_zstd persist/restore
/// streaming, and the cadence at which the bounded PostgreSQL memory context
/// used for FFI encode/decode calls is reset.
static EXTERNAL_SNAPSHOT_BATCH_ROWS_GUC: GucSetting<i32> = GucSetting::<i32>::new(10_000);
/// Hard ceiling on one row's serialized (pre-compression) byte size during
/// external_zstd persist/restore; a larger row fails the operation closed.
static EXTERNAL_SNAPSHOT_MAX_ROW_BYTES_GUC: GucSetting<i32> = GucSetting::<i32>::new(67_108_864);
/// zstd compression level used for external_zstd artifacts.
static EXTERNAL_SNAPSHOT_ZSTD_LEVEL_GUC: GucSetting<i32> = GucSetting::<i32>::new(3);
/// Test-only external_zstd failpoint name. Empty = disabled (release default).
/// Superuser-only (SUSET). Not a production control plane. Same shape as
/// test_restore_failpoint/test_consume_wal_failpoint above.
static TEST_EXTERNAL_ZSTD_FAILPOINT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub fn is_capture_enabled() -> bool {
    ENABLED_GUC.get()
}

/// Rows whose captured old+new JSON exceeds this many bytes are not decoded
/// inline; the output plugin emits an oversized marker instead and the SQL
/// consumer freezes the stream rather than silently applying a delta with no
/// row data (see fb_decode_change / flashback_apply_decoded_wal_batch).
pub fn max_row_size_bytes() -> i32 {
    MAX_ROW_SIZE_GUC.get()
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

pub fn external_snapshot_root() -> Result<String, String> {
    EXTERNAL_SNAPSHOT_ROOT_GUC
        .get()
        .as_deref()
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| "pg_flashback.external_snapshot_root is not set".to_string())
}

pub fn external_snapshot_batch_rows() -> usize {
    EXTERNAL_SNAPSHOT_BATCH_ROWS_GUC.get().clamp(100, 1_000_000) as usize
}

pub fn external_snapshot_max_row_bytes() -> usize {
    EXTERNAL_SNAPSHOT_MAX_ROW_BYTES_GUC
        .get()
        .clamp(1_024, 1_073_741_824) as usize
}

pub fn external_snapshot_zstd_level() -> i32 {
    EXTERNAL_SNAPSHOT_ZSTD_LEVEL_GUC.get().clamp(1, 19)
}

/// Terminate the current PostgreSQL process at one named external SnapshotStore
/// durability boundary. This is deliberately narrower than a general-purpose
/// fault injector: the SUSET GUC is empty by default, accepts only exact names
/// checked at call sites, and is used solely by isolated crash/retry E2E tests.
/// `proc_exit` skips Rust destructors, which is essential for proving recovery
/// from the same filesystem state a real backend/copier death can leave.
pub fn trigger_external_snapshot_failpoint(name: &str) {
    let configured = TEST_EXTERNAL_ZSTD_FAILPOINT_GUC.get();
    let enabled = configured.as_deref().and_then(|value| value.to_str().ok());
    let pause_name = format!("pause:{name}");
    if enabled == Some(pause_name.as_str()) {
        warning!("pg_flashback TEST ONLY external_zstd pause fired: {name}");
        std::thread::sleep(Duration::from_secs(5));
    } else if enabled == Some(name) {
        warning!("pg_flashback TEST ONLY external_zstd failpoint fired: {name}");
        unsafe { pg_sys::proc_exit(86) };
    }
}

/// SQL-callable entrypoint for the failpoint boundaries above that have no
/// Rust call site of their own (pure PL/pgSQL statement boundaries inside
/// flashback_protect_abort / flashback_protect_finalize). Reuses the exact
/// same GUC-gated mechanism as every other named failpoint -- no new fault-
/// injection system, just a way for PL/pgSQL to reach it at a specific
/// statement boundary.
///
/// `#[cfg(any(test, feature = "pg_test"))]`, not merely GUC-gated like the
/// call sites above: a production build (`--features pg17`, no `pg_test`)
/// never compiles this function in at all, so it cannot appear in that
/// build's generated SQL (proven by check_generated_sql_no_test_surface.sh,
/// which generates with exactly that feature set) -- not just inert-by-
/// default, genuinely absent. Every PL/pgSQL call site guards on
/// `to_regprocedure(...) IS NOT NULL` first, so production bodies are
/// byte-for-byte the same whether or not this function exists.
#[cfg(any(test, feature = "pg_test"))]
#[pg_extern]
fn flashback_internal_test_trigger_failpoint(name: &str) {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!(
            "flashback_internal_test_trigger_failpoint is an internal superuser-only function"
        );
    }
    trigger_external_snapshot_failpoint(name);
}

pub fn register_worker_and_guc() {
    GucRegistry::define_int_guc(
        c"pg_flashback.worker_interval_ms",
        c"pg_flashback delta worker flush interval",
        c"Base capture interval in milliseconds. WAL mode backs off to at most one second while idle and resets immediately after captured activity.",
        &WORKER_INTERVAL_MS,
        50,
        10_000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.worker_batch_size",
        c"pg_flashback delta worker batch size",
        c"How many WAL changes the background worker consumes into delta_log per cycle.",
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
        c"When OFF, the capture worker idles after recording a durable discontinuity. Emergency kill switch.",
        &ENABLED_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.max_row_size",
        c"pg_flashback maximum captured row size in bytes",
        c"A changed row whose captured old+new data exceeds this many bytes cannot be decoded inline; capture freezes and opens a durable gap instead of silently dropping the row. Default 65536 (64KB).",
        &MAX_ROW_SIZE_GUC,
        512,
        104_857_600, // 100MB hard ceiling
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.target_database",
        c"Database the pg_flashback worker connects to",
        c"The background worker runs WAL capture in this database. Set to the database where the extension is installed. Default: postgres. Overridden by target_databases if set.",
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
        c"pg_flashback capture mode (wal only; trigger/auto rejected)",
        c"Only 'wal' is operational (default). Deprecated values 'trigger' and 'auto' fail closed: no DML capture and reconcile breaks any active stream. Set wal_level=logical.",
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
        c"pg_flashback.local_max_snapshot_bytes",
        c"Reject local track/re-anchor when projected base snapshot exceeds this size",
        c"Accepted by pg_size_bytes(). Models CTAS heap+TOAST only (indexes are not copied). Empty or 0 fails closed for the qualified local profile.",
        &LOCAL_MAX_SNAPSHOT_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.local_max_restore_peak_bytes",
        c"Reject local restores whose projected peak exceeds this size",
        c"Accepted by pg_size_bytes(). Empty or 0 fails closed. Budget shadow(with rebuilt indexes)+successor base(heap+TOAST)+reserve before ACCESS EXCLUSIVE.",
        &LOCAL_MAX_RESTORE_PEAK_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.local_min_filesystem_bytes",
        c"Minimum filesystem free space that must remain after projected local writes",
        c"Accepted by pg_size_bytes(). Empty or 0 fails closed. Probed against the destination tablespace with an OS-backed free-space check.",
        &LOCAL_MIN_FILESYSTEM_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.local_safety_reserve_bytes",
        c"Safety reserve included in local capacity estimates",
        c"Accepted by pg_size_bytes() and added to projected track/re-anchor/restore requirements. Default when unset: 64MB.",
        &LOCAL_SAFETY_RESERVE_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.local_boundary_write_stall_ms",
        c"Maximum lock-wait/write-stall budget for exact local boundaries",
        c"Applied as transaction-local lock_timeout before the final relation lock, and compared with the estimated CTAS copy duration. Default 30000 ms.",
        &LOCAL_BOUNDARY_WRITE_STALL_MS_GUC,
        1,
        3_600_000,
        GucContext::Suset,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.local_assumed_copy_mib_per_sec",
        c"Assumed local CTAS throughput for write-stall estimation",
        c"Conservative MiB/s used only to estimate lock/copy risk. Does not replace size or filesystem budgets.",
        &LOCAL_ASSUMED_COPY_MIB_PER_SEC_GUC,
        1,
        1024,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_flashback.local_capacity_override",
        c"Privileged override that admits local capacity/write-stall failures",
        c"Must be set explicitly by a privileged role. Visible in flashback_advise()/health and logged; never a silent default.",
        &LOCAL_CAPACITY_OVERRIDE_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_flashback.allow_unaudited_restore",
        c"Allow flashback_restore_lsn without flashback_recover_begin/execute audit context",
        c"Default off. The supported operator path is flashback_recover_begin then flashback_recover_execute. Enable only for emergency/lab use; failed unaudited restores leave no durable operation audit.",
        &ALLOW_UNAUDITED_RESTORE_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.test_restore_failpoint",
        c"TEST ONLY: named restore failpoint/barrier (empty disables)",
        c"Superuser-only. Release default is empty/off. Valid names are checked inside flashback_restore_lsn for deterministic cancel/retry tests. Never enable in production.",
        &TEST_RESTORE_FAILPOINT_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.test_consume_wal_failpoint",
        c"TEST ONLY: named consume_wal failpoint/barrier (empty disables)",
        c"Superuser-only. Release default is empty/off. Valid names are checked inside flashback_consume_wal for deterministic slot-loss-misclassification regression tests. Never enable in production.",
        &TEST_CONSUME_WAL_FAILPOINT_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.slot_lag_warning_bytes",
        c"Slot retained-WAL warning threshold for flashback_health()",
        c"Accepted by pg_size_bytes(). Default when unset: 256MB. Health becomes slot_lag_warning before slot loss.",
        &SLOT_LAG_WARNING_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.slot_lag_at_risk_bytes",
        c"Slot retained-WAL at-risk threshold for flashback_health()",
        c"Accepted by pg_size_bytes(). Default when unset: 1GB. Also compared with safe_wal_size when PostgreSQL provides it.",
        &SLOT_LAG_AT_RISK_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.external_snapshot_root",
        c"Directory outside PGDATA/tablespaces for external_zstd SnapshotStore artifacts",
        c"Must already exist, be owned by the PostgreSQL OS user, and be mode 0700; the extension never creates it. Empty disables the external backend. POSTMASTER context: server-start only, never SUSET/SIGHUP -- changing the artifact root while the server is running is never safe.",
        &EXTERNAL_SNAPSHOT_ROOT_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.snapshot_storage_backend",
        c"SnapshotStore backend for newly reserved artifacts: heap_v1 or external_zstd",
        c"SIGHUP, not SUSET: only an explicit config file change + reload can change which backend the next generation of a protected table uses, never a single session's in-transaction SET. The backend actually used for a given generation is resolved once at reservation time and frozen on that generation's own row afterward.",
        &SNAPSHOT_STORAGE_BACKEND_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.external_snapshot_min_free_bytes",
        c"Minimum external_snapshot_root filesystem free space after a projected persist",
        c"Accepted by pg_size_bytes(). Empty or 0 fails closed for the external_zstd profile, mirroring pg_flashback.local_min_filesystem_bytes.",
        &EXTERNAL_SNAPSHOT_MIN_FREE_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.external_snapshot_safety_reserve_bytes",
        c"Safety reserve included in external_zstd persist capacity estimates",
        c"Accepted by pg_size_bytes(). Empty or 0 fails closed, mirroring pg_flashback.local_safety_reserve_bytes.",
        &EXTERNAL_SNAPSHOT_SAFETY_RESERVE_BYTES_GUC,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.external_snapshot_batch_rows",
        c"Row-batch size for external_zstd persist/restore SPI cursor streaming",
        c"Also the cadence at which the bounded PostgreSQL memory context used for per-row FFI encode/decode is reset. Default 10000.",
        &EXTERNAL_SNAPSHOT_BATCH_ROWS_GUC,
        100,
        1_000_000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.external_snapshot_max_row_bytes",
        c"Hard ceiling on one row's serialized size during external_zstd persist/restore",
        c"A row whose encoded frame would exceed this fails the operation closed rather than allow unbounded per-row memory growth. Default 64MB.",
        &EXTERNAL_SNAPSHOT_MAX_ROW_BYTES_GUC,
        1024,
        1_073_741_824,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_flashback.external_snapshot_zstd_level",
        c"zstd compression level for external_zstd artifacts",
        c"Standard zstd level range. Default 3.",
        &EXTERNAL_SNAPSHOT_ZSTD_LEVEL_GUC,
        1,
        19,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_flashback.test_external_zstd_failpoint",
        c"TEST ONLY: named external_zstd failpoint/barrier (empty disables)",
        c"Superuser-only. Release default is empty/off. Valid names are checked inside the external_zstd persist/restore/finalize path for deterministic crash/retry regression tests. Never enable in production.",
        &TEST_EXTERNAL_ZSTD_FAILPOINT_GUC,
        GucContext::Suset,
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

/// Canonical target-database list contract shared by worker registration and
/// SQL admission. Comma-split, trim, drop empties, first-wins dedupe. Duplicate
/// names must not consume additional worker indices.
pub fn parse_target_databases(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let name = part.trim();
        if name.is_empty() {
            continue;
        }
        if !out.iter().any(|existing| existing == name) {
            out.push(name.to_string());
        }
    }
    out
}

fn max_worker_pairs() -> usize {
    MAX_WORKERS_GUC.get().clamp(1, 8) as usize
}

/// Resolve the list of target databases from GUCs.
/// Priority: target_databases (comma-separated) > target_database > "postgres".
fn resolve_database_list() -> Vec<String> {
    // Check target_databases first (comma-separated list)
    if let Some(cs) = TARGET_DATABASES_GUC.get() {
        if let Ok(s) = cs.to_str() {
            let dbs = parse_target_databases(s);
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
    parse_target_databases(db_name)
}

fn admitted_database_list() -> Vec<String> {
    let max_w = max_worker_pairs();
    resolve_database_list().into_iter().take(max_w).collect()
}

/// Canonical configured database list (deduped; not truncated by max_workers).
#[pg_extern(stable, name = "flashback_canonical_target_databases")]
fn flashback_canonical_target_databases() -> Vec<String> {
    resolve_database_list()
}

/// Databases that receive a registered capture/maintenance worker pair.
#[pg_extern(stable, name = "flashback_admitted_target_databases")]
fn flashback_admitted_target_databases() -> Vec<String> {
    admitted_database_list()
}

/// Effective `pg_flashback.max_workers` clamp used for admission.
#[pg_extern(stable, name = "flashback_max_worker_pairs")]
fn flashback_max_worker_pairs() -> i32 {
    max_worker_pairs() as i32
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

    // Log the configured capture_mode GUC at startup. Only 'wal' is
    // operational; illegal values are reconciled fail-closed each cycle.
    let mode_setting = CAPTURE_MODE_GUC.get();
    let initial_mode_str = mode_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("wal");
    log!(
        "pg_flashback delta worker {worker_index} started (database: {db_name}, capture_mode_guc: {initial_mode_str}, {total} total database(s))",
        total = db_list.len()
    );

    // Track whether slot is ready (lazily created on first WAL cycle)
    let mut slot_ready = false;
    let mut slot_warned = false;
    let mut last_enabled: Option<bool> = None;
    let mut last_mode: Option<&'static str> = None;
    let mut wal_idle_cycles: u32 = 0;

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
                wait_latch_or_exit_for_restart(Duration::from_millis(interval_ms));
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
                // Lazily ensure slot exists (created by flashback_track)
                if !slot_ready {
                    slot_ready = ensure_replication_slot();
                    if !slot_ready && !slot_warned {
                        log!("pg_flashback: no replication slot for database {db_name}, waiting for flashback_track() to create it");
                        slot_warned = true;
                    }
                }
                if slot_ready {
                    match consume_wal_changes() {
                        Some(inserted) if inserted > 0 => wal_idle_cycles = 0,
                        Some(_) => wal_idle_cycles = wal_idle_cycles.saturating_add(1),
                        None => wal_idle_cycles = 0,
                    }
                }
            } else {
                // trigger/auto/other: never capture. Reconcile above breaks the
                // stream; keep slot_ready false and idle until mode is wal.
                slot_ready = false;
                wal_idle_cycles = 0;
            }
            let flush_ms = t0.elapsed().as_millis();

            let cycle_ms = cycle_start.elapsed().as_millis();
            let interval_ms_val = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u128;
            if cycle_ms > interval_ms_val {
                warning!(
                    "pg_flashback CAPTURE_SLOW_CYCLE cycle_ms={cycle_ms} flush_ms={flush_ms} interval_ms={interval_ms_val}"
                );
            }
        }

        let base_interval_ms = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u64;
        // Logical decoding emits PostgreSQL LOG records even for an empty
        // prefix. Polling it every 50–75 ms while idle creates unbounded log
        // amplification without improving correctness. WAL slots retain
        // unconsumed changes across crashes, so bounded idle backoff affects
        // only visibility latency. Any captured event or consume error resets
        // the delay.
        let interval_ms = if enabled && mode == "wal" {
            let multiplier = 1_u64 << wal_idle_cycles.min(5);
            base_interval_ms
                .saturating_mul(multiplier)
                .min(base_interval_ms.max(1_000))
        } else {
            base_interval_ms
        };
        wait_latch_or_exit_for_restart(Duration::from_millis(interval_ms));
    }
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
            run_ensure_partitions();
            run_external_snapshot_health();
            run_retention_purge();
        }
        let capture_interval = WORKER_INTERVAL_MS.get().clamp(50, 10_000) as u64;
        let cadence = MAINTENANCE_EVERY_N_CYCLES_GUC.get().clamp(1, 10_000) as u64;
        let wait_ms = capture_interval.saturating_mul(cadence).min(600_000);
        wait_latch_or_exit_for_restart(Duration::from_millis(wait_ms));
    }
}

/// Wait for the next worker cycle. On SIGTERM while the postmaster is still
/// alive, exit non-zero so `bgw_restart_time` restarts the worker. A handled
/// SIGTERM that returns from `main` with status 0 is treated as a voluntary
/// permanent stop and would leave capture/maintenance absent until a full
/// PostgreSQL restart.
fn wait_latch_or_exit_for_restart(timeout: Duration) {
    if BackgroundWorker::wait_latch(Some(timeout)) {
        return;
    }
    // PostmasterIsAlive() is a header inline; call the exported internal.
    extern "C" {
        fn PostmasterIsAliveInternal() -> bool;
    }
    let postmaster_alive = unsafe { PostmasterIsAliveInternal() };
    if postmaster_alive {
        std::process::exit(1);
    }
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

/// Determine the operational capture mode from the GUC.
/// Returns "wal" only when the GUC is unset/empty/`wal`.
/// Returns the raw illegal value (`trigger`, `auto`, …) otherwise so the
/// worker never runs the WAL consume path and reconcile can break the stream.
///
/// Does NOT use SPI.
fn effective_capture_mode() -> &'static str {
    let mode_setting = CAPTURE_MODE_GUC.get();
    let mode = mode_setting
        .as_deref()
        .and_then(|cs| cs.to_str().ok())
        .unwrap_or("");

    match mode {
        "" | "wal" => "wal",
        "trigger" => "trigger",
        "auto" => "auto",
        _ => "invalid",
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
fn consume_wal_changes() -> Option<i32> {
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
        return None;
    };

    let result: Result<i32, SpiError> = BackgroundWorker::transaction(|| {
        let inserted = Spi::get_one_with_args::<i32>(
            "SELECT flashback_consume_wal($1)",
            &[batch_size.into()],
        )?
        .unwrap_or(0);
        // Storage-exhaustion freeze/gap recording happens in the SAME
        // transaction as slot consumption: a lifecycle can never advance its
        // consumed watermark past the point a permanent gap is recorded for
        // it, and the gap can never be recorded without the consume that
        // observed the exhaustion also committing.
        let freeze_exists = Spi::get_one::<bool>(
            "SELECT to_regprocedure('flashback_storage_freeze_scan()') IS NOT NULL",
        )?
        .unwrap_or(false);
        if freeze_exists {
            Spi::run("SELECT flashback_storage_freeze_scan()")?;
        }
        Ok(inserted)
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

    let inserted = match result {
        Ok(inserted) => Some(inserted),
        Err(err) => {
            log!("pg_flashback WAL_CONSUME_ERROR error={err:?}");
            None
        }
    };
    if let Err(err) = unlock_result {
        log!("pg_flashback WAL_CONSUME_UNLOCK_ERROR error={err:?}");
        return None;
    }
    inserted
}

fn run_retention_purge() {
    run_bounded_maintenance(
        "RETENTION_PURGE",
        "DO $$
                         BEGIN
                           BEGIN
                             IF to_regprocedure('flashback_finalize_recover_operations()') IS NOT NULL THEN
                                 PERFORM flashback_finalize_recover_operations();
                             END IF;
                             IF to_regprocedure('flashback_finalize_unprotect_operations()') IS NOT NULL THEN
                                 PERFORM flashback_finalize_unprotect_operations();
                             END IF;
                             IF to_regprocedure('flashback_apply_retention()') IS NOT NULL THEN
                                 PERFORM flashback_apply_retention();
                             END IF;
                           EXCEPTION
                             WHEN lock_not_available OR query_canceled THEN
                               RAISE WARNING 'pg_flashback: retention maintenance deferred: %', SQLERRM;
                           END;
                         END
                         $$",
    );
}

fn run_external_snapshot_health() {
    run_bounded_maintenance(
        "EXTERNAL_SNAPSHOT_HEALTH",
        "DO $$
             BEGIN
               BEGIN
                 IF to_regprocedure('flashback_internal_reconcile_external_snapshot_scan(integer)') IS NOT NULL THEN
                     PERFORM flashback_internal_reconcile_external_snapshot_scan(1);
                 END IF;
                 IF to_regprocedure('flashback_internal_reconcile_external_snapshot_retirements(integer)') IS NOT NULL THEN
                     PERFORM flashback_internal_reconcile_external_snapshot_retirements(1);
                 END IF;
                 IF to_regprocedure('flashback_internal_reconcile_external_maintenance(interval,integer)') IS NOT NULL THEN
                     PERFORM flashback_internal_reconcile_external_maintenance(interval '5 minutes', 1);
                 END IF;
                 IF to_regprocedure('flashback_internal_reconcile_external_protect(integer)') IS NOT NULL THEN
                     PERFORM flashback_internal_reconcile_external_protect(5);
                 END IF;
               EXCEPTION
                 WHEN lock_not_available OR query_canceled THEN
                   RAISE WARNING 'pg_flashback: external snapshot maintenance deferred: %', SQLERRM;
               END;
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
               BEGIN
                 IF to_regprocedure('flashback_ensure_delta_partition(date)') IS NOT NULL THEN
                     PERFORM flashback_ensure_delta_partition(CURRENT_DATE);
                 END IF;
               EXCEPTION
                 WHEN lock_not_available OR query_canceled THEN
                   RAISE WARNING 'pg_flashback: partition maintenance deferred: %', SQLERRM;
               END;
             END
             $$",
    );
}

/// Run a best-effort maintenance transaction that cannot monopolize capture.
/// Each SQL body catches the expected lock_timeout/statement_timeout inside a
/// PL/pgSQL subtransaction so those routine conflicts abort only that body and
/// are retried on a later cadence. Unexpected PostgreSQL ERRORs remain fatal:
/// the postmaster restarts the worker instead of hiding a persistent bug.
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

#[cfg(test)]
mod admission_tests {
    use super::parse_target_databases;

    #[test]
    fn parses_trims_and_dedupes_first_wins() {
        assert_eq!(
            parse_target_databases(" db1, db2 ,db1, ,db3,,db2 "),
            vec!["db1".to_string(), "db2".to_string(), "db3".to_string()]
        );
    }

    #[test]
    fn empty_and_whitespace_only_yield_empty() {
        assert!(parse_target_databases("").is_empty());
        assert!(parse_target_databases(" , , ").is_empty());
    }
}

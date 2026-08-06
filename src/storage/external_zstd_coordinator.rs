//! external_zstd online-snapshot marker-transaction coordinator (Step 9,
//! Stage 6 completion). Wires Stage 4's DSM handoff, Stage 5's reservation
//! authority, and Stage 6's boundary-bind authority into the real M1-M7
//! sequence the plan's §1e timeline describes -- not the SQL authority
//! functions tested in isolation, the actual orchestration that will
//! eventually sit behind a public `flashback_reanchor`-style online
//! wrapper.
//!
//! This module does **not** implement the copy/finalizer transactions
//! (C1-C5/F1-F11) -- the real streaming persist to a zstd artifact is
//! Stage 7's job, explicitly deferred. What this module proves is that
//! the *handoff itself* -- lock, revalidate, bind, signal, wait, publish
//! the boundary WAL message -- works for real, in the right order, against
//! a real second OS process, with real DDL adversarially interleaved, and
//! with every pre-commit failure point leaving the durable Stage 5
//! reservation exactly as the reconciler (plan §1h) expects to find it.
//!
//! # M8 (COMMIT) is deliberately not this module's job
//!
//! Every other step here (M1-M7) is a statement issued via SPI inside the
//! caller's own transaction. M8, the actual `COMMIT`, is not something
//! this function -- or any function called via SPI -- is able to issue
//! (PostgreSQL forbids transaction-control statements from SPI-connected
//! contexts). `run_marker_transaction` therefore performs M1-M7 and
//! returns; the caller's own transaction end is M8. This is not a
//! shortcut: it is the only place M8 can correctly live, and it is also
//! why "message emitted strictly before commit" is a single-threaded
//! call-order fact for this function's own steps (no concurrent process
//! is involved in that particular ordering, unlike the lock/signal/pin
//! orderings below, which cross a real process boundary and are proved
//! adversarially instead of by argument).
//!
//! # Authority-only mutation
//!
//! Every state-mutating step here goes through an existing centralized
//! authority function (`flashback_internal_lock_lifecycle`,
//! `flashback_internal_bind_online_boundary`) or a PostgreSQL builtin
//! (`LOCK TABLE`, `pg_logical_emit_message`) -- there is no raw
//! `UPDATE flashback.coverage_generations`/`UPDATE flashback.snapshots`
//! anywhere in this module, and there must never be one added here.
//!
//! `#![allow(dead_code)]` is temporary: nothing outside this module's own
//! tests calls `run_marker_transaction` yet, since the public wrapper that
//! will drive it end-to-end (`flashback_track_online`/an online variant of
//! `flashback_reanchor`) is out of scope until the copy/finalizer
//! transactions (Stage 7) exist to give it something to hand off to.
#![allow(dead_code)]

use crate::storage::external_zstd_handoff::{HandoffPhase, HandoffSegment, HandoffWaitError};
use pgrx::bgworkers::DynamicBackgroundWorker;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::JsonB;
use std::time::Duration;

/// Successful M1-M7 outcome, returned to the caller for M8 (its own
/// transaction commit) to follow.
#[derive(Debug)]
pub struct MarkerOutcome {
    pub boundary_xid: i64,
    pub boundary_marker: String,
    /// The LSN `pg_logical_emit_message` returned for the BOUNDARY
    /// message -- proof the message was durably queued for this
    /// transaction's WAL, not merely attempted, and directly comparable
    /// against `HandoffSegment::read_pinned_wal_lsn()` for the
    /// snapshot-pin-before-message ordering regression.
    pub boundary_message_lsn: u64,
}

#[derive(Debug)]
pub enum MarkerError {
    /// M2's `LOCK TABLE ... NOWAIT`-equivalent (via `lock_timeout`) could
    /// not acquire the lock in time.
    LockNotAvailable(String),
    /// A centralized authority function's own CAS/state-shape check
    /// rejected the call (e.g. the generation is not `building`, or its
    /// boundary is already bound).
    PrerequisiteState(String),
    /// A centralized authority function rejected a parameter outright
    /// (e.g. `flashback_internal_materializable_columns` found zero
    /// columns for a dropped relation).
    InvalidParameter(String),
    /// M3's post-lock identity revalidation found the tracked table gone,
    /// renamed away, or re-pointed at a different relation oid.
    IdentityChanged {
        expected: pg_sys::Oid,
        found: Option<pg_sys::Oid>,
    },
    /// M6's wait for `SnapshotPinned` failed (copier dead, failed, or
    /// timed out).
    CopierWait(HandoffWaitError),
    /// Any other SPI/PostgreSQL error not specifically classified above.
    Other(String),
}

impl std::fmt::Display for MarkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarkerError::LockNotAvailable(m) => write!(f, "lock not available: {m}"),
            MarkerError::PrerequisiteState(m) => write!(f, "prerequisite state violation: {m}"),
            MarkerError::InvalidParameter(m) => write!(f, "invalid parameter: {m}"),
            MarkerError::IdentityChanged { expected, found } => write!(
                f,
                "tracked table identity changed under lock: expected oid {expected:?}, found {found:?}"
            ),
            MarkerError::CopierWait(e) => write!(f, "copier handoff wait failed: {e:?}"),
            MarkerError::Other(m) => write!(f, "{m}"),
        }
    }
}

fn caught_message(e: &CaughtError) -> String {
    match e {
        CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => r.message().to_string(),
        CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
    }
}

/// Run `f` (an SPI call) inside a real `PG_TRY`-equivalent boundary,
/// classifying the specific SQLSTATEs this module's own authority-function
/// callees are documented to raise into distinct [`MarkerError`] variants
/// rather than collapsing everything into one generic failure -- the
/// rollback-point regression (Stage 6 completion, task 5) depends on being
/// able to tell these apart.
fn run_catching<R>(
    f: impl FnOnce() -> Result<R, pgrx::spi::Error> + std::panic::UnwindSafe,
) -> Result<R, MarkerError> {
    PgTryBuilder::new(|| f().map_err(|e| MarkerError::Other(e.to_string())))
        .catch_when(PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE, |e| {
            Err(MarkerError::LockNotAvailable(caught_message(&e)))
        })
        .catch_when(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            |e| Err(MarkerError::PrerequisiteState(caught_message(&e))),
        )
        .catch_when(PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE, |e| {
            Err(MarkerError::InvalidParameter(caught_message(&e)))
        })
        .catch_others(|e| Err(MarkerError::Other(caught_message(&e))))
        .execute()
}

/// Minimal defense-in-depth identifier quoting (schema/table names here
/// always come from catalog metadata, never free user input, but every
/// other dynamic-SQL site in this codebase quotes defensively too).
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The real M1-M7 marker-transaction sequence (plan §1e). Must be called
/// from within an already-open transaction (the caller's own -- this
/// function never issues `BEGIN`/`COMMIT`/`ROLLBACK` itself), after the
/// Stage 5 reservation (`flashback_internal_reserve_online_generation`)
/// has already committed and the DSM segment + copier have already been
/// created/launched (H1-H2, not part of this function either -- those are
/// plain backend-local operations with no transactional meaning).
///
/// On any `Err`, the caller's transaction is expected to roll back
/// (matching every other authority function in this codebase: fail
/// closed, never partially apply). The Stage 5 reservation this marker
/// transaction was attempting to bind is untouched by a failure here --
/// see the rollback-point regression for the exact proof.
pub fn run_marker_transaction(
    tracking_id: i64,
    rel_oid: pg_sys::Oid,
    generation_id: i64,
    snapshot_id: i64,
    segment: &HandoffSegment,
    copier: &DynamicBackgroundWorker,
) -> Result<MarkerOutcome, MarkerError> {
    // M1: same canonical lifecycle lock every other lifecycle-mutating
    // entrypoint in this codebase takes first (plan §1i).
    run_catching(|| {
        Spi::run(&format!(
            "SELECT public.flashback_internal_lock_lifecycle({tracking_id}::bigint)"
        ))
    })?;

    // Fresh, pre-lock resolution of the tracked table's current name --
    // needed to even issue LOCK TABLE, and itself the first half of
    // identity revalidation (a renamed-away or untracked table is
    // detected right here, before ever attempting to lock anything).
    let (schema, table) = run_catching(|| {
        Spi::get_two::<String, String>(&format!(
            "SELECT tt.schema_name, tt.table_name FROM flashback.tracked_tables tt \
             WHERE tt.tracking_id = {tracking_id}::bigint AND tt.rel_oid = {}::oid \
               AND tt.is_active AND tt.recovery_profile = 'local_delta'",
            rel_oid.to_u32()
        ))
    })?;
    let (schema, table) = match (schema, table) {
        (Some(s), Some(t)) => (s, t),
        _ => {
            return Err(MarkerError::IdentityChanged {
                expected: rel_oid,
                found: None,
            })
        }
    };

    // M2: SHARE ROW EXCLUSIVE, not ACCESS EXCLUSIVE -- this is the entire
    // point of "online": concurrent readers and even concurrent row-level
    // writers are unaffected, only other schema-changing/exclusive
    // lockers are blocked. Bounded by the same configurable write-stall
    // budget every other boundary-lock acquisition in this codebase uses.
    run_catching(|| Spi::run("SELECT public.flashback_apply_local_boundary_lock_timeout()"))?;
    let lock_sql = format!(
        "LOCK TABLE {}.{} IN SHARE ROW EXCLUSIVE MODE",
        quote_ident(&schema),
        quote_ident(&table)
    );
    run_catching(|| Spi::run(&lock_sql))?;

    // M3 (second half): re-resolve under the now-held lock and compare --
    // catches the race window between the pre-lock name resolution above
    // and actually acquiring the lock (the table could have been dropped
    // and a same-named-but-different relation created in between).
    let current_oid = run_catching(|| {
        Spi::get_one::<i64>(&format!(
            "SELECT to_regclass({})::oid::bigint",
            quote_literal(&format!("{schema}.{table}"))
        ))
    })?;
    let current_oid_u32 = current_oid.map(|v| pg_sys::Oid::from(v as u32));
    if current_oid_u32 != Some(rel_oid) {
        return Err(MarkerError::IdentityChanged {
            expected: rel_oid,
            found: current_oid_u32,
        });
    }

    // M4: the ONLY sanctioned path that moves boundary_xid/boundary_marker/
    // schema_def/external_column_contract out of their Stage 5 placeholder
    // values (Stage 6). CAS-shaped; fails closed on any prior partial
    // state.
    let (boundary_xid, boundary_marker) = run_catching(|| {
        Spi::get_two::<i64, String>(&format!(
            "SELECT boundary_xid, boundary_marker FROM public.flashback_internal_bind_online_boundary(\
             {generation_id}::bigint, {tracking_id}::bigint, {snapshot_id}::bigint, {}::oid)",
            rel_oid.to_u32()
        ))
    })?;
    let boundary_xid = boundary_xid.ok_or_else(|| {
        MarkerError::Other(
            "flashback_internal_bind_online_boundary returned NULL boundary_xid".to_string(),
        )
    })?;
    let boundary_marker = boundary_marker.ok_or_else(|| {
        MarkerError::Other(
            "flashback_internal_bind_online_boundary returned NULL boundary_marker".to_string(),
        )
    })?;

    // The column contract M4 just bound is only visible inside this
    // (still-uncommitted) transaction -- the copier is a separate session
    // and cannot see it via an ordinary query (MVCC hides uncommitted
    // changes regardless of isolation level). It must be handed over
    // through the DSM payload instead, written here (strictly before M5's
    // signal) and read by the copier only after it observes
    // LockHeldGoAhead -- exactly the write-before-publish contract
    // write_column_list/read_column_list document.
    let column_list = run_catching(|| {
        Spi::get_one::<String>(&format!(
            "SELECT string_agg(quote_ident(col->>'name'), ',' ORDER BY (col->>'attnum')::int) \
             FROM flashback.snapshots s, jsonb_array_elements(s.external_column_contract) AS col \
             WHERE s.snapshot_id = {snapshot_id}::bigint AND s.tracking_id = {tracking_id}::bigint"
        ))
    })?
    .ok_or_else(|| {
        MarkerError::Other("no materializable columns bound for this snapshot".to_string())
    })?;
    segment
        .write_column_list(&column_list)
        .map_err(MarkerError::Other)?;
    let qualified_target = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
    segment
        .write_target_relation(&qualified_target)
        .map_err(MarkerError::Other)?;

    // M5: the table lock is now genuinely held, the boundary is durably
    // bound, and the copier's column list + target relation are published
    // -- only now is it safe to tell the copier to proceed.
    segment.signal(HandoffPhase::LockHeldGoAhead);

    // M6: bounded wait, checking the copier is still alive on every wake
    // (HandoffSegment::wait_for_state's own contract, Stage 4).
    segment
        .wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(30),
            Some(copier),
        )
        .map_err(MarkerError::CopierWait)?;

    // M7: the same mechanism/prefix every other boundary kind in this
    // codebase already uses (flashback_reanchor's 'maintenance_reanchor',
    // initial track's 'initial_track'), decoded by the same already-
    // running logical slot -- only the 'kind' discriminator is new.
    let message_json = format!(
        "{{\"op\":\"BOUNDARY\",\"kind\":\"online_external\",\"tracking_id\":{tracking_id},\"generation_id\":{generation_id}}}"
    );
    let boundary_message_lsn_text = run_catching(|| {
        Spi::get_one::<String>(&format!(
            "SELECT pg_logical_emit_message(true, 'pg_flashback', {}::text)::text",
            quote_literal(&message_json)
        ))
    })?
    .ok_or_else(|| MarkerError::Other("pg_logical_emit_message returned NULL".to_string()))?;
    let boundary_message_lsn = parse_pg_lsn(&boundary_message_lsn_text).ok_or_else(|| {
        MarkerError::Other(format!(
            "could not parse pg_logical_emit_message's returned LSN {boundary_message_lsn_text:?}"
        ))
    })?;

    Ok(MarkerOutcome {
        boundary_xid,
        boundary_marker,
        boundary_message_lsn,
    })
}

// ── Real copier: cursor-open + REPEATABLE READ pin (Stage 6 completion)
// ────────────────────────────────────────────────────────────────────────
//
// NOT Stage 7's real streaming persist -- no zstd, no artifact write, no
// batched row-by-row encode. This worker's job, and only job, is the part
// of H5 that Stage 6 completion needs proven for real: wait for
// LockHeldGoAhead, open a genuine SPI cursor under REPEATABLE READ
// isolation against the coordinator-published column list and target
// relation (pinning a real snapshot), record what it saw, signal
// SnapshotPinned. The row data itself is fetched and immediately
// discarded (only the count is kept) -- Stage 7 is where fetched rows
// actually go somewhere (the zstd-compressed artifact).

/// Packs `dsm_handle` (low 32 bits) and the coordinator's database oid
/// (high 32 bits) into one launch argument -- this worker needs both
/// before it can attach to SPI, and `operation_nonce` inside the segment
/// itself already carries the real protocol's operation_nonce meaning, so
/// it is not available to repurpose the way the Stage 6 lock-order-probe
/// worker repurposed it.
pub fn pack_copier_worker_argument(dsm_h: pg_sys::dsm_handle, db_oid: pg_sys::Oid) -> i64 {
    (dsm_h as i64) | ((db_oid.to_u32() as i64) << 32)
}

fn copier_worker_body(arg: pg_sys::Datum) {
    use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let raw = unsafe { i64::from_datum(arg, false) }.unwrap_or(0);
    let dsm_h = (raw & 0xFFFF_FFFF) as u32;
    let db_oid = pg_sys::Oid::from(((raw >> 32) & 0xFFFF_FFFF) as u32);

    let segment = unsafe { HandoffSegment::attach(dsm_h) };
    let segment = match segment {
        Some(s) => s,
        None => {
            log!("pg_flashback external_zstd copier: dsm_attach failed, exiting");
            return;
        }
    };

    BackgroundWorker::connect_worker_to_spi_by_oid(Some(db_oid), None);
    // Sets the isolation level for the *next* transaction this session
    // starts, rather than issuing `SET TRANSACTION ISOLATION LEVEL` as a
    // statement inside that transaction: SPI's normal per-statement
    // execution path acquires an active snapshot as part of ordinary
    // statement processing regardless of the statement being a plain SET,
    // so `SET TRANSACTION ISOLATION LEVEL` as the "first statement" inside
    // the transaction actually races against (and loses to) that -- found
    // directly via `SET TRANSACTION ISOLATION LEVEL must be called before
    // any query`. This worker's process only ever runs one transaction in
    // its whole life, so a session-level default is exactly scoped.
    //
    // Every SPI call in a background worker must run inside a real
    // transaction (`BackgroundWorker::transaction`, which itself calls
    // `StartTransactionCommand()` first) -- calling `Spi::run` directly
    // here, with no transaction started at all, crashed the backend
    // outright (SIGABRT, "the database system is in recovery mode")
    // rather than merely erroring. This throwaway transaction commits
    // immediately; the GUC it sets (not `SET LOCAL`) persists at the
    // session level for the real transaction that follows.
    let set_result: Result<(), String> =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            BackgroundWorker::transaction(|| {
                Spi::run("SET default_transaction_isolation = 'repeatable read'")
                    .map_err(|e| e.to_string())
            })
        }))
        .unwrap_or_else(|_| Err("panicked setting default_transaction_isolation".to_string()));
    if let Err(e) = set_result {
        log!("pg_flashback external_zstd copier: failed to set default_transaction_isolation: {e}");
        segment.write_error_message(&format!("default_transaction_isolation: {e}"));
        segment.signal(HandoffPhase::Failed);
        return;
    }

    match segment.wait_for_state(HandoffPhase::LockHeldGoAhead, Duration::from_secs(30), None) {
        Ok(()) => {
            let column_list = segment.read_column_list();
            let target_relation = segment.read_target_relation();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                BackgroundWorker::transaction(|| {
                    open_cursor_and_fetch(&column_list, &target_relation)
                })
            }));
            match outcome {
                Ok(Ok((n, pinned_lsn))) => {
                    segment.write_fetched_row_count(n);
                    segment.write_pinned_wal_lsn(pinned_lsn);
                    segment.signal(HandoffPhase::SnapshotPinned);
                }
                Ok(Err(e)) => {
                    log!("pg_flashback external_zstd copier: cursor open/fetch failed: {e}");
                    segment.write_error_message(&e);
                    segment.signal(HandoffPhase::Failed);
                }
                Err(e) => {
                    // BackgroundWorker::transaction uses PgTryBuilder
                    // internally; an uncaught PostgreSQL ERROR it doesn't
                    // have a handler for is re-thrown as resume_unwind(Box
                    // ::new(CaughtError)), not a plain string payload --
                    // downcast to that first.
                    let msg = e
                        .downcast_ref::<CaughtError>()
                        .map(caught_message)
                        .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                        .or_else(|| e.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "<unrecognized panic payload type>".to_string());
                    log!("pg_flashback external_zstd copier: cursor open/fetch panicked: {msg}");
                    segment.write_error_message(&format!("panic: {msg}"));
                    segment.signal(HandoffPhase::Failed);
                }
            }
        }
        Err(e) => {
            log!("pg_flashback external_zstd copier: wait failed: {e:?}");
            segment.signal(HandoffPhase::Failed);
        }
    }
}

/// A real `SPI_cursor_open_with_args` + `SPI_cursor_fetch` against a
/// transaction whose isolation level was already set to REPEATABLE READ
/// before it started (`copier_worker_body`'s `default_transaction_
/// isolation` -- see that function's own comment for why an in-transaction
/// `SET TRANSACTION ISOLATION LEVEL` statement does not work here). The
/// cursor open/fetch is what actually pins the snapshot. Raw SPI, matching
/// this crate's established style for anything pgrx has no safe cursor
/// wrapper for -- and, like every other raw-SPI call site in this crate
/// (`external_zstd_format.rs`'s `spi_select_raw_rows`), must run inside
/// both an explicit `Spi::connect` (raw `SPI_cursor_*` calls are not
/// self-connecting) and an explicit pushed active snapshot
/// (`PushedSnapshotGuard`) -- a raw SPI entry point called from a
/// bgworker with no enclosing executor does not get one for free the way
/// an ordinary query does.
fn open_cursor_and_fetch(column_list: &str, target_relation: &str) -> Result<(u64, u64), String> {
    let query = format!("SELECT {column_list} FROM {target_relation}");
    let query_c = std::ffi::CString::new(query).map_err(|e| e.to_string())?;
    let name_c = std::ffi::CString::new("pg_flashback_copier_probe").unwrap();

    let n = Spi::connect(|_client| -> Result<u64, String> {
        let _snapshot_guard =
            unsafe { crate::storage::external_zstd_format::PushedSnapshotGuard::new() };
        let portal = unsafe {
            pg_sys::SPI_cursor_open_with_args(
                name_c.as_ptr(),
                query_c.as_ptr(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null(),
                true,
                0,
            )
        };
        if portal.is_null() {
            return Err("SPI_cursor_open_with_args returned a null portal".to_string());
        }
        // This is what actually pins the REPEATABLE READ snapshot: the
        // cursor's plan is executed here, against the transaction
        // snapshot established at first use.
        unsafe { pg_sys::SPI_cursor_fetch(portal, true, i64::MAX) };
        let n = unsafe { pg_sys::SPI_processed };
        unsafe { pg_sys::SPI_cursor_close(portal) };
        Ok(n)
    })?;

    // Captured immediately after the pin, for the externally-observable
    // ordering regression (module doc, "pinned_wal_lsn"): a single,
    // cluster-wide, monotonically non-decreasing coordinate comparable
    // against the boundary message's own LSN without needing wall-clock
    // timestamps or trusting this process's own call order.
    let lsn_text = Spi::get_one::<String>("SELECT pg_current_wal_insert_lsn()::text")
        .map_err(|e| format!("pg_current_wal_insert_lsn SPI error: {e}"))?
        .ok_or_else(|| "pg_current_wal_insert_lsn returned NULL".to_string())?;
    let pinned_lsn = parse_pg_lsn(&lsn_text)
        .ok_or_else(|| format!("could not parse pg_lsn text {lsn_text:?}"))?;

    Ok((n, pinned_lsn))
}

/// Parse PostgreSQL's `X/Y` hex `pg_lsn` text representation into the
/// plain `u64` byte offset it represents (`XLogRecPtr` is a `u64`
/// internally; the SQL `pg_lsn` type's on-disk/text form is just this
/// value split into high/low 32-bit hex halves).
fn parse_pg_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u32::from_str_radix(hi, 16).ok()?;
    let lo = u32::from_str_radix(lo, 16).ok()?;
    Some(((hi as u64) << 32) | (lo as u64))
}

/// The real, exported symbol this crate's `lib.rs` `#[unsafe(no_mangle)]`
/// wrapper delegates to (dynamic background worker function lookup is
/// string/symbol-name based -- see the Stage 4 selftest worker's identical
/// requirement).
pub extern "C-unwind" fn pg_flashback_external_zstd_copier_worker_main(arg: pg_sys::Datum) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        copier_worker_body(arg);
    }));
    if let Err(e) = result {
        let msg = e
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        log!("pg_flashback external_zstd copier worker: top-level PANICKED: {msg}");
    }
}

/// Launch the real copier as a dynamic background worker (H2 in the plan's
/// timeline). Tracked (`set_notify_pid`) so the caller can
/// `wait_for_startup`/`pid()`, matching every other dynamic worker in this
/// crate.
pub fn launch_copier_worker(
    dsm_h: pg_sys::dsm_handle,
    db_oid: pg_sys::Oid,
) -> Result<DynamicBackgroundWorker, pgrx::bgworkers::DynamicBackgroundWorkerLoadError> {
    pgrx::bgworkers::BackgroundWorkerBuilder::new("pg_flashback external_zstd copier")
        .set_function("pg_flashback_external_zstd_copier_worker_main")
        .set_library("pg_flashback")
        .set_argument(pack_copier_worker_argument(dsm_h, db_oid).into_datum())
        .set_notify_pid(unsafe { pg_sys::MyProcPid })
        .enable_spi_access()
        .load_dynamic()
}

/// Internal owner-only entry point for the marker transaction. The SQL
/// caller owns the transaction boundary: COMMIT makes both the binding and
/// transactional logical message durable; ROLLBACK removes both.
#[pg_extern]
fn flashback_internal_run_external_marker_transaction(
    tracking_id: i64,
    rel_oid: i64,
    generation_id: i64,
    snapshot_id: i64,
) -> JsonB {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!(
            "flashback_internal_run_external_marker_transaction is an internal owner-only function"
        );
    }
    if tracking_id <= 0 || rel_oid <= 0 || generation_id <= 0 || snapshot_id <= 0 {
        pgrx::error!("external marker transaction identifiers must all be positive");
    }

    let db_oid = unsafe { pg_sys::MyDatabaseId };
    let segment = unsafe { HandoffSegment::coordinator_create(generation_id as u64) };
    let worker = launch_copier_worker(segment.handle(), db_oid)
        .expect("failed to launch external_zstd copier worker");
    worker
        .wait_for_startup()
        .expect("external_zstd copier worker did not start");

    match run_marker_transaction(
        tracking_id,
        pg_sys::Oid::from(rel_oid as u32),
        generation_id,
        snapshot_id,
        &segment,
        &worker,
    ) {
        Ok(outcome) => {
            let result = JsonB(serde_json::json!({
                "boundary_xid": outcome.boundary_xid,
                "boundary_marker": outcome.boundary_marker,
                "boundary_message_lsn": outcome.boundary_message_lsn,
                "pinned_wal_lsn": segment.read_pinned_wal_lsn(),
                "fetched_row_count": segment.read_fetched_row_count(),
            }));
            segment.detach();
            result
        }
        Err(error) => {
            segment.write_error_message(&error.to_string());
            segment.signal(HandoffPhase::Failed);
            segment.detach();
            pgrx::error!("pg_flashback external marker transaction failed: {error}")
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    struct Fixture {
        db_oid: pg_sys::Oid,
        stream_id: i64,
        tracking_id: i64,
        rel_oid: pg_sys::Oid,
        /// Real online-create only ever reanchors an *already-tracked*
        /// table (the plan frames this feature as a reanchor variant
        /// throughout) -- an already-'active' parent generation always
        /// coexists with the new 'building' online reservation for the
        /// whole duration of its build window (plan §1f: "the generation
        /// stays 'building', the parent stays 'active' ... for as long as
        /// the artifact remains 'creating'"). This matters for more than
        /// realism: flashback_capture_configuration_guard's DDL gate looks
        /// for the *most preferred* (active-first) qualified generation
        /// for a tracking_id, so a bare online reservation with no active
        /// parent (not a real topology) would spuriously look like a
        /// stalled epoch and block ordinary DDL -- discovered directly by
        /// this fixture initially omitting it.
        parent_generation_id: i64,
    }

    /// One capture stream, shared by every test in this module and never
    /// retired (see `setup`'s own comment for exactly why retiring a
    /// per-fixture stream deadlocked against the test's own session).
    /// `capture_streams_one_active_idx` permits only one 'active' row per
    /// database; created once per test-binary run via `std::sync::
    /// OnceLock`, through a real committing worker so it is durably
    /// visible to every later worker session too.
    fn shared_stream_id(db_oid: pg_sys::Oid) -> i64 {
        static STREAM_ID: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
        *STREAM_ID.get_or_init(|| {
            crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
                "DO $do$
                 BEGIN
                     IF NOT EXISTS (
                         SELECT 1 FROM flashback.capture_streams
                         WHERE slot_name = 'it_coord_shared_slot' AND state = 'active'
                     ) THEN
                         PERFORM public.flashback_internal_create_capture_stream(
                             p_database_oid => {}::oid, p_initial_state => 'active',
                             p_slot_name => 'it_coord_shared_slot', p_plugin_name => 'pg_flashback_decoder'
                         );
                     END IF;
                 END;
                 $do$;",
                db_oid.to_u32()
            ));
            Spi::get_one::<i64>(
                "SELECT stream_id FROM flashback.capture_streams WHERE slot_name = 'it_coord_shared_slot'",
            )
            .unwrap()
            .unwrap()
        })
    }

    /// Creates `public.<table_name>` with `cols`, tracks it against the
    /// module's one shared active capture stream (`shared_stream_id`),
    /// establishes a real active parent generation (the realistic
    /// pre-online-create topology -- see `Fixture::parent_generation_id`),
    /// and returns the identifiers every step
    /// below needs.
    fn setup(table_name: &str, cols: &str, _slot_name: &str) -> Fixture {
        let db_oid = unsafe { pg_sys::MyDatabaseId };

        // One capture stream shared by every test in this module, created
        // (once) via a committing worker and never retired mid-run --
        // NOT one per fixture. flashback_internal_reserve_online_
        // generation (called by reserve(), directly in each test's own
        // session) takes a FOR SHARE lock on the capture_streams row that
        // is held for the rest of that test (a #[pg_test] session is
        // never committed/rolled back mid-test, only at the very end).
        // An earlier version of this fixture created *and later retired*
        // a fresh stream per test; retiring it required a FOR UPDATE lock
        // on that same row from a separate worker session, which blocked
        // indefinitely on the still-held FOR SHARE lock until this
        // module's own 30s wait bound gave up -- a real, found (not
        // anticipated) cross-session self-block, not a hypothetical one.
        // Reusing one never-retired stream sidesteps it entirely: nothing
        // ever needs a conflicting lock on this row again once it exists.
        let stream_id = shared_stream_id(db_oid);

        // Every #[pg_test] function's own session is unconditionally
        // rolled back at test end (pgrx-tests' own harness), so anything
        // created via a plain Spi::run here would never be durably
        // visible to the separate copier worker session later in this
        // same test -- a real cross-session-visibility gap, not just a
        // theoretical one (see external_zstd_handoff.rs's test_support
        // module doc for how this was found). The entire fixture is
        // therefore built as one DO block and run via a real, committing
        // background worker; every identifier it produces is re-derived
        // afterward via ordinary SELECTs, safe now that it's committed.
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "DO $do$
             DECLARE
                 v_rel_oid oid;
                 v_stream_id bigint := {stream_id}::bigint;
                 v_tracking_id bigint;
                 v_snapshot_id bigint;
                 v_generation_id bigint;
             BEGIN
                 CREATE TABLE IF NOT EXISTS public.{table_name} ({cols});
                 v_rel_oid := 'public.{table_name}'::regclass::oid;

                 INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile)
                 VALUES (v_rel_oid, 'public', '{table_name}', NULL, 'local_delta')
                 RETURNING tracking_id INTO v_tracking_id;

                 v_snapshot_id := public.flashback_internal_snapshot_create(
                     v_tracking_id, v_rel_oid, 'public', '{table_name}', '0/1000'::pg_lsn, 'initial_track'
                 );

                 v_generation_id := public.flashback_internal_create_coverage_generation(
                     p_tracking_id => v_tracking_id, p_generation_no => 1, p_stream_id => v_stream_id,
                     p_boundary_kind => 'initial_track', p_rel_oid_at_boundary => v_rel_oid,
                     p_boundary_snapshot_id => v_snapshot_id, p_boundary_xid => txid_current(),
                     p_boundary_marker => 'coord-test-parent:{table_name}'
                 );

                 PERFORM public.flashback_internal_transition_coverage_generation(
                     v_generation_id, v_tracking_id, 'building', 'active', 'activate',
                     '0/1000'::pg_lsn, clock_timestamp(), '0/1000'::pg_lsn, clock_timestamp(), NULL, NULL, '{{}}'::jsonb
                 );

                 -- flashback_track's own bootstrap (lifecycle_bootstrap_core.sql)
                 -- populates flashback.schema_versions as part of initial tracking;
                 -- this fixture constructs lifecycle state directly (flashback_
                 -- track itself requires an admitted running capture worker and a
                 -- dedicated pre-write transaction, neither available here) and
                 -- must populate the same row by hand, or ordinary schema-changing
                 -- DDL later (the ADD COLUMN test) fails the pre-existing
                 -- local_compatibility.sql schema-contract check that DDL already
                 -- requires independent of anything Step 9 adds. schema_def
                 -- specifically (not just columns) must be set: flashback_require_
                 -- current_schema_contract's lookup treats a matching row with a
                 -- NULL schema_def as \"no complete schema contract\" -- found
                 -- directly by this fixture initially omitting it.
                 INSERT INTO flashback.schema_versions (
                     rel_oid, tracking_id, generation_id, stream_id, source_xid,
                     schema_version, applied_at, applied_lsn, columns, schema_def
                 ) VALUES (
                     v_rel_oid, v_tracking_id, v_generation_id, v_stream_id, txid_current(),
                     1, clock_timestamp(), '0/1000'::pg_lsn,
                     COALESCE(public.flashback_collect_schema_def(v_rel_oid)->'columns', '[]'::jsonb),
                     public.flashback_collect_schema_def(v_rel_oid)
                 );
             END;
             $do$;"
        ));

        let rel_oid_i64 = Spi::get_one::<i64>(&format!(
            "SELECT 'public.{table_name}'::regclass::oid::bigint"
        ))
        .unwrap()
        .unwrap();
        let rel_oid = pg_sys::Oid::from(rel_oid_i64 as u32);
        let tracking_id = Spi::get_one::<i64>(&format!(
            "SELECT tracking_id FROM flashback.tracked_tables WHERE rel_oid = {}::oid",
            rel_oid.to_u32()
        ))
        .unwrap()
        .unwrap();
        let parent_generation_id = Spi::get_one::<i64>(&format!(
            "SELECT generation_id FROM flashback.coverage_generations \
             WHERE tracking_id = {tracking_id}::bigint AND generation_no = 1"
        ))
        .unwrap()
        .unwrap();

        Fixture {
            db_oid,
            stream_id,
            tracking_id,
            rel_oid,
            parent_generation_id,
        }
    }

    /// Stage 5's reservation, called directly (not through this module --
    /// it belongs to snapshot_store.sql). generation_no is always 2: every
    /// fixture's parent (setup, above) is generation_no 1.
    /// Stage 5's reservation (`flashback_internal_reserve_online_
    /// generation`, snapshot_store.sql, called directly -- it belongs to
    /// that file, not this module). Run via a real committing worker, not
    /// this test's own session: in real production this is Transaction R,
    /// which commits (R4) -- releasing the lifecycle/stream locks it
    /// took -- before the marker transaction ever begins. Calling it
    /// directly in this #[pg_test] function's own (never-committed-mid-
    /// test) session would hold those same locks for the rest of the
    /// test, which was found to deadlock the DDL-window test's own ALTER
    /// TABLE (pg_flashback's DDL hook needs the same lifecycle lock for
    /// any DDL on the tracked table) -- not merely a test-infrastructure
    /// workaround, but the more realistic simulation of the real,
    /// two-separate-transactions design this whole stage exists to prove.
    fn reserve(fx: &Fixture, nonce: i64) -> (i64, i64) {
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "SELECT public.flashback_internal_reserve_online_generation(\
             p_tracking_id => {}::bigint, p_rel_oid => {}::oid, p_stream_id => {}::bigint, \
             p_generation_no => 2::bigint, p_parent_generation_id => {}::bigint, \
             p_storage_backend => 'external_zstd', p_operation_nonce => {nonce}::bigint)",
            fx.tracking_id,
            fx.rel_oid.to_u32(),
            fx.stream_id,
            fx.parent_generation_id
        ));
        let (gen_id, snap_id) = Spi::get_two::<i64, i64>(&format!(
            "SELECT generation_id, boundary_snapshot_id FROM flashback.coverage_generations \
             WHERE tracking_id = {}::bigint AND generation_no = 2",
            fx.tracking_id
        ))
        .unwrap();
        (gen_id.unwrap(), snap_id.unwrap())
    }

    fn generation_snapshot(
        tracking_id: i64,
        generation_id: i64,
        snapshot_id: i64,
    ) -> (String, Option<i64>, String, String) {
        // (generation.state, generation.boundary_xid, generation.boundary_marker, snapshot.payload_state)
        let (state, boundary_xid, boundary_marker) = Spi::connect(|c| {
            let table = c
                .select(
                    &format!(
                        "SELECT state, boundary_xid, boundary_marker FROM flashback.coverage_generations \
                         WHERE generation_id = {generation_id}::bigint AND tracking_id = {tracking_id}::bigint"
                    ),
                    None,
                    &[],
                )
                .unwrap();
            let row = table.first();
            (
                row.get_by_name::<String, _>("state").unwrap().unwrap(),
                row.get_by_name::<i64, _>("boundary_xid").unwrap(),
                row.get_by_name::<String, _>("boundary_marker")
                    .unwrap()
                    .unwrap(),
            )
        });
        let payload_state = Spi::get_one::<String>(&format!(
            "SELECT payload_state FROM flashback.snapshots \
             WHERE snapshot_id = {snapshot_id}::bigint AND tracking_id = {tracking_id}::bigint"
        ))
        .unwrap()
        .unwrap();
        (state, boundary_xid, boundary_marker, payload_state)
    }

    /// H1-H2: create the DSM segment and launch the real copier. Not part
    /// of `run_marker_transaction` itself (plain backend-local operations,
    /// no transactional meaning -- module doc).
    fn start_handoff(fx: &Fixture, nonce: u64) -> (HandoffSegment, DynamicBackgroundWorker) {
        let segment = unsafe { HandoffSegment::coordinator_create(nonce) };
        let worker = launch_copier_worker(segment.handle(), fx.db_oid)
            .expect("failed to launch real copier worker");
        worker
            .wait_for_startup()
            .expect("real copier worker did not start");
        (segment, worker)
    }

    fn marker_sql(fx: &Fixture, generation_id: i64, snapshot_id: i64) -> String {
        format!(
            "SELECT public.flashback_internal_run_external_marker_transaction(\
             {}::bigint, {}::bigint, {generation_id}::bigint, {snapshot_id}::bigint)",
            fx.tracking_id,
            fx.rel_oid.to_u32()
        )
    }

    fn assert_reservation_unbound(fx: &Fixture, generation_id: i64, snapshot_id: i64, nonce: i64) {
        let (state, boundary_xid, boundary_marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(state, "building");
        assert_eq!(
            boundary_xid, None,
            "failed marker transaction leaked a boundary xid"
        );
        assert_eq!(boundary_marker, format!("online_pending:{nonce}"));
        assert_eq!(payload_state, "creating");
        let bound_fields = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM flashback.snapshots WHERE snapshot_id = {snapshot_id}::bigint \
             AND tracking_id = {}::bigint AND (snapshot_lsn IS NOT NULL \
             OR schema_def <> '{{}}'::jsonb OR external_column_contract <> '[]'::jsonb)",
            fx.tracking_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            bound_fields, 0,
            "failed marker transaction leaked snapshot binding fields"
        );
    }

    fn wait_for_relation_lock(rel_oid: pg_sys::Oid, mode: &str) {
        for _ in 0..100 {
            let held = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' \
                 AND relation = {}::oid AND mode = {} AND granted)",
                rel_oid.to_u32(),
                quote_literal(mode)
            ))
            .unwrap()
            .unwrap_or(false);
            if held {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("timed out waiting for {mode} on relation {rel_oid:?}");
    }

    #[pg_test]
    fn test_marker_m2_conflict_atomic_retry() {
        let fx = setup(
            "it_coord_m2_conflict",
            "id int primary key, note text",
            "it_coord_m2_conflict_slot",
        );
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            "INSERT INTO public.it_coord_m2_conflict VALUES (1, 'before')",
        );
        let nonce = 700007;
        let (generation_id, snapshot_id) = reserve(&fx, nonce);

        let (lock_segment, lock_worker) =
            crate::storage::external_zstd_handoff::test_support::launch_commit_sql_worker(
                "LOCK TABLE public.it_coord_m2_conflict IN ACCESS EXCLUSIVE MODE; \
                 SELECT pg_sleep(3)",
            );
        wait_for_relation_lock(fx.rel_oid, "AccessExclusiveLock");

        let marker_call = format!(
            "SET pg_flashback.local_boundary_write_stall_ms = 200; {}",
            marker_sql(&fx, generation_id, snapshot_id)
        );
        let (marker_segment, marker_worker) =
            crate::storage::external_zstd_handoff::test_support::launch_commit_sql_worker(
                &marker_call,
            );
        let marker_result = marker_segment.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(10),
            Some(&marker_worker),
        );
        assert_eq!(marker_result, Err(HandoffWaitError::PeerFailed));
        let failure = marker_segment.read_error_message();
        assert!(
            failure.contains("lock not available") || failure.contains("lock timeout"),
            "M2 failure must identify the real lock conflict, got: {failure}"
        );
        marker_segment.detach();

        assert_reservation_unbound(&fx, generation_id, snapshot_id, nonce);

        let lock_result = lock_segment.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(10),
            Some(&lock_worker),
        );
        assert_eq!(lock_result, Ok(()));
        lock_segment.detach();

        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&marker_sql(
            &fx,
            generation_id,
            snapshot_id,
        ));
        let boundary_xid = Spi::get_one::<i64>(&format!(
            "SELECT boundary_xid FROM flashback.coverage_generations \
             WHERE generation_id = {generation_id}::bigint"
        ))
        .unwrap()
        .unwrap();
        assert!(
            boundary_xid > 0,
            "retry did not commit the boundary binding"
        );
    }

    #[pg_test]
    fn test_marker_db_commit_visible_rollback_hidden() {
        let fx = setup(
            "it_coord_tx_visibility",
            "id int primary key, note text",
            "it_coord_tx_visibility_slot",
        );
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            "INSERT INTO public.it_coord_tx_visibility VALUES (1, 'before')",
        );
        let nonce = 700008;
        let (generation_id, snapshot_id) = reserve(&fx, nonce);

        let rollback_sql = format!(
            "{}; SELECT 1 / 0",
            marker_sql(&fx, generation_id, snapshot_id)
        );
        let (rollback_segment, rollback_worker) =
            crate::storage::external_zstd_handoff::test_support::launch_commit_sql_worker(
                &rollback_sql,
            );
        let rollback_result = rollback_segment.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(15),
            Some(&rollback_worker),
        );
        assert_eq!(rollback_result, Err(HandoffWaitError::PeerFailed));
        assert!(
            rollback_segment
                .read_error_message()
                .contains("division by zero"),
            "rollback worker must fail at the deliberate post-message abort point"
        );
        rollback_segment.detach();

        assert_reservation_unbound(&fx, generation_id, snapshot_id, nonce);

        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&marker_sql(
            &fx,
            generation_id,
            snapshot_id,
        ));
        let boundary_xid = Spi::get_one::<i64>(&format!(
            "SELECT boundary_xid FROM flashback.coverage_generations \
             WHERE generation_id = {generation_id}::bigint"
        ))
        .unwrap()
        .unwrap();
        assert!(
            boundary_xid > 0,
            "committed retry did not persist the boundary binding"
        );
    }

    /// Happy path + externally observable ordering: the copier's real
    /// cursor-fetch-pinned WAL LSN must be <= the boundary message's own
    /// LSN (task 10) -- WAL LSNs are a single, cluster-wide, monotonically
    /// non-decreasing sequence, so this is independent corroboration of
    /// the structural "pin (H5, inside the M5->M6 wait) happens before
    /// message (M7)" call-order argument, not merely a restatement of it.
    /// "target lock acquired before copier proceeds" is the exact property
    /// Stage 6's existing test_copier_cannot_observe_go_ahead_before_lock_
    /// is_held (external_zstd_handoff.rs) already proves against this same
    /// DSM primitive; the real copier here consumes the identical signal
    /// through the identical mechanism, so that proof transfers directly
    /// and is not re-derived here. "message before commit" is a
    /// single-threaded call-order fact (module doc) that cannot be tested
    /// inside pg_test's harness at all, since it always rolls back and
    /// SPI-connected code can never issue COMMIT -- documented, not
    /// silently skipped.
    #[pg_test]
    fn test_marker_transaction_happy_path_and_ordering() {
        let fx = setup(
            "it_coord_happy",
            "id int primary key, note text",
            "it_coord_happy_slot",
        );
        // Must be committed, not just run in this test's own session --
        // the real copier's cursor query runs in a genuinely separate
        // session and would not see uncommitted rows (test_support's
        // module doc, external_zstd_handoff.rs).
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            "INSERT INTO public.it_coord_happy VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        );
        let (generation_id, snapshot_id) = reserve(&fx, 700001);
        let (segment, worker) = start_handoff(&fx, 700001);

        let outcome = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment,
            &worker,
        )
        .unwrap_or_else(|e| {
            panic!(
                "run_marker_transaction should succeed on the happy path: {e} (copier: {})",
                segment.read_error_message()
            )
        });

        assert!(outcome.boundary_xid > 0);
        // generation_no 2: 1 is always this fixture's parent (setup).
        assert_eq!(
            outcome.boundary_marker,
            format!(
                "online_external:{}:2:{}",
                fx.tracking_id, outcome.boundary_xid
            )
        );

        // Ordering: pin (recorded by the real copier) <= message (M7).
        let pinned_lsn = segment.read_pinned_wal_lsn();
        assert!(pinned_lsn > 0, "copier never recorded a pinned WAL LSN");
        assert!(
            pinned_lsn <= outcome.boundary_message_lsn,
            "snapshot-pin LSN ({pinned_lsn}) must not be after the boundary message LSN ({})",
            outcome.boundary_message_lsn
        );

        // The real copier's real cursor really saw the 3 committed rows.
        assert_eq!(segment.read_fetched_row_count(), 3);

        let (state, boundary_xid, boundary_marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(state, "building"); // activation is a later stage
        assert_eq!(boundary_xid, Some(outcome.boundary_xid));
        assert_eq!(boundary_marker, outcome.boundary_marker);
        assert_eq!(payload_state, "creating"); // finalizer (Stage 7) not run

        segment.detach();
    }

    /// DDL window 1 (compatible): a column added after Stage 5 reservation
    /// but before the marker transaction's lock must be reflected in the
    /// bound column contract AND in the real copier's actual SELECT list
    /// -- not the stale contract from reservation time (there wasn't one:
    /// Stage 5 leaves it empty precisely so a fresh, lock-protected
    /// capture is the only source, task 11).
    #[pg_test]
    fn test_marker_transaction_ddl_add_column_is_reflected() {
        let fx = setup(
            "it_coord_addcol",
            "id int primary key",
            "it_coord_addcol_slot",
        );
        // Must be committed -- see test_marker_transaction_happy_path_and_
        // ordering's identical note.
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            "INSERT INTO public.it_coord_addcol VALUES (1), (2)",
        );
        let (generation_id, snapshot_id) = reserve(&fx, 700002);

        // DDL committed after reservation, before the marker transaction --
        // and must itself be durably committed for the same reason: the
        // real copier's cursor query, in its own separate session, would
        // not see an uncommitted ADD COLUMN (the new attribute simply
        // would not exist in its view of pg_attribute) or an uncommitted
        // UPDATE.
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            "ALTER TABLE public.it_coord_addcol ADD COLUMN extra text DEFAULT 'x'; \
             UPDATE public.it_coord_addcol SET extra = 'y' WHERE id = 2",
        );

        let (segment, worker) = start_handoff(&fx, 700002);
        let outcome = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment,
            &worker,
        )
        .expect("compatible ADD COLUMN must not fail the marker transaction");
        assert!(outcome.boundary_xid > 0);

        // quote_ident() only adds quotes when actually needed (reserved
        // word, uppercase, special characters) -- a plain lowercase name
        // like "extra" comes back unquoted, so check membership by
        // splitting rather than assuming quotes are always present.
        let column_list = segment.read_column_list();
        let columns: Vec<&str> = column_list
            .split(',')
            .map(|s| s.trim_matches('"'))
            .collect();
        assert!(
            columns.contains(&"extra"),
            "copier's published column list did not include the added column: {column_list}"
        );
        assert_eq!(segment.read_fetched_row_count(), 2);

        let contract_len = Spi::get_one::<i64>(&format!(
            "SELECT jsonb_array_length(external_column_contract) FROM flashback.snapshots \
             WHERE snapshot_id = {snapshot_id}::bigint AND tracking_id = {}::bigint",
            fx.tracking_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(contract_len, 2, "bound contract should include id + extra");

        segment.detach();
    }

    /// DDL window 2 (incompatible): the table is dropped entirely after
    /// reservation, before the marker transaction ever runs. Must fail
    /// closed with no partial boundary/schema binding, and the Stage 5
    /// reservation must be left exactly as it was for the reconciler
    /// (task 11 + task 12).
    #[pg_test]
    fn test_marker_transaction_ddl_drop_fails_closed() {
        let fx = setup("it_coord_drop", "id int primary key", "it_coord_drop_slot");
        let (generation_id, snapshot_id) = reserve(&fx, 700003);

        PgTryBuilder::new(|| {
            Spi::run("SELECT flashback_set_restore_in_progress(true)").unwrap();
            Spi::run("DROP TABLE public.it_coord_drop").unwrap();
        })
        .finally(|| {
            Spi::run("SELECT flashback_set_restore_in_progress(false)").unwrap();
        })
        .execute();

        let (segment, worker) = start_handoff(&fx, 700003);
        let result = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment,
            &worker,
        );
        assert!(
            result.is_err(),
            "marker transaction against a dropped relation must fail"
        );

        let (state, boundary_xid, boundary_marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(state, "building");
        assert_eq!(boundary_xid, None, "boundary_xid must remain unbound");
        assert_eq!(boundary_marker, format!("online_pending:700003"));
        assert_eq!(payload_state, "creating");
        let schema_def = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT schema_def FROM flashback.snapshots \
             WHERE snapshot_id = {snapshot_id}::bigint AND tracking_id = {}::bigint",
            fx.tracking_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(schema_def.0, serde_json::json!({}));

        // run_marker_transaction failed before M5 (the signal that would
        // ever wake the copier), so the copier -- launched by start_
        // handoff above and still real and alive -- is left waiting on
        // its own internal 30s bound with no peer to check liveness
        // against (it never learns the coordinator gave up). Left alone,
        // it lingers a full 30 real seconds past this test's own return,
        // which was found to visibly contend with whichever test runs
        // immediately afterward. Signal it directly so it exits promptly
        // instead.
        segment.signal(HandoffPhase::Failed);
        segment.detach();
    }

    /// Rollback-at-every-point (task 12), part 1: the durable admission
    /// check itself -- a bind attempt against a generation that is not
    /// (or no longer) a fresh 'building' reservation with an unbound
    /// boundary fails via flashback_internal_bind_online_boundary's own
    /// CAS, leaving whatever was already durably bound completely
    /// untouched. Exercised here by calling the real coordinator twice in
    /// a row against the same reservation -- the second call's M4 must
    /// reject without disturbing the first call's already-durable bind.
    #[pg_test]
    fn test_marker_rerun_leaves_first_bind_untouched() {
        let fx = setup(
            "it_coord_rebind",
            "id int primary key",
            "it_coord_rebind_slot",
        );
        let (generation_id, snapshot_id) = reserve(&fx, 700004);

        let (segment1, worker1) = start_handoff(&fx, 700004);
        let outcome1 = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment1,
            &worker1,
        )
        .expect("first marker transaction should succeed");
        segment1.detach();

        let (segment2, worker2) = start_handoff(&fx, 700005);
        let result2 = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment2,
            &worker2,
        );
        assert!(
            result2.is_err(),
            "a second marker transaction against an already-bound generation must fail"
        );
        // Same reasoning as test_marker_transaction_ddl_drop_fails_closed:
        // this failed before M5, so worker2 is still alive and would
        // otherwise wait out its own internal 30s bound.
        segment2.signal(HandoffPhase::Failed);
        segment2.detach();

        let (_, boundary_xid, boundary_marker, _) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(
            boundary_xid,
            Some(outcome1.boundary_xid),
            "the rejected second call must not have disturbed the first call's durable bind"
        );
        assert_eq!(boundary_marker, outcome1.boundary_marker);
    }

    /// Rollback-at-every-point (task 12), part 2: M6 (waiting for
    /// SnapshotPinned) fails when the copier never launches at all --
    /// proves the marker transaction fails closed even after M1-M5 have
    /// all genuinely succeeded (lifecycle locked, table locked, identity
    /// revalidated, boundary durably bound, signal sent), with no message
    /// ever emitted and the bind from M4 durably recorded but the
    /// generation correctly left non-terminal (task 12).
    #[pg_test]
    fn test_marker_no_copier_leaves_bound_not_activated() {
        let fx = setup(
            "it_coord_nocop",
            "id int primary key",
            "it_coord_nocop_slot",
        );
        let (generation_id, snapshot_id) = reserve(&fx, 700006);

        // A genuinely alive peer process that never signals anything (a
        // real `SELECT pg_sleep(...)` inside the commit-sql test worker,
        // reused here specifically for its "launch and never finish
        // quickly" shape -- not because this is setup SQL): exercises
        // M6's plain bounded-timeout path with a live process, not a dead
        // one (that is HandoffWaitError::PeerDead, a different path,
        // covered by external_zstd_handoff.rs's own Stage 4/6 tests). The
        // real M1-M5 prefix (lock, revalidate, bind, publish, signal)
        // still runs for real; only the copier's own behavior is
        // substituted. Sleeps just past run_marker_transaction's own 30s
        // M6 bound, not e.g. 60s: a worker left lingering well beyond the
        // point this test stops waiting on it was found to starve later
        // tests' own worker launches of bgworker slots.
        let (segment, worker) =
            crate::storage::external_zstd_handoff::test_support::launch_commit_sql_worker(
                "SELECT pg_sleep(33)",
            );
        let result = run_marker_transaction(
            fx.tracking_id,
            fx.rel_oid,
            generation_id,
            snapshot_id,
            &segment,
            &worker,
        );
        match &result {
            Err(MarkerError::CopierWait(_)) => {}
            other => panic!("expected MarkerError::CopierWait, got {other:?}"),
        }

        let (state, boundary_xid, _boundary_marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(state, "building");
        assert!(
            boundary_xid.is_some(),
            "M4's bind already durably succeeded before M6 timed out -- this is expected \
             and is exactly why activation (a later, separate stage) re-validates \
             everything fresh rather than trusting a generation reaching 'building' \
             with a bound boundary as sufficient on its own"
        );
        assert_eq!(payload_state, "creating");

        segment.detach();
    }
}

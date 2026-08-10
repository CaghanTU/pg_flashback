//! Production external_zstd online-snapshot coordinator. It wires the DSM
//! handoff, durable reservation, lock-protected boundary bind, streaming
//! copier, immutable artifact finalization, generation activation, and
//! restore materialization into the SnapshotStore authority surface.
//!
//! The marker transaction and copy transaction deliberately remain separate:
//! the copy pins its repeatable-read snapshot only after the marker-side table
//! lock is held, while the WAL message is emitted only after that pin is
//! acknowledged. Publication and generation activation are later, independently
//! durable milestones so either can be resumed after a crash.
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
//! Some orchestration helpers remain callable only through the background
//! worker and test seams until the public operator workflow is wired. Keep
//! the module-level allowance until that final call-site closure, rather than
//! weakening the production lifecycle protocol to manufacture Rust callers.
#![allow(dead_code)]

use crate::storage::external_zstd_artifact::{
    finalize_staged_artifact, open_published_artifact, probe_external_artifact_state,
    purge_published_artifact, purge_staged_artifact, ArtifactStream, FinalManifest,
    FinalizationInput, PendingArtifact, ProvisionalManifest,
};
use crate::storage::external_zstd_format::{
    decode_datum, encode_datum, read_header, read_row, read_trailer, resolve_receive_info,
    resolve_send_info, write_header, write_row, write_trailer, ColumnDescriptor, TypeReceiveInfo,
    TypeSendInfo, FORMAT_VERSION,
};
use crate::storage::external_zstd_handoff::{
    CopyIdentity, HandoffPhase, HandoffSegment, HandoffWaitError,
};
use pgrx::bgworkers::DynamicBackgroundWorker;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::JsonB;
use sha2::{Digest, Sha256};
use std::ffi::{CStr, CString};
use std::io::Read;
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
    let (column_list, column_contract) = run_catching(|| {
        Spi::get_two::<String, String>(&format!(
            "SELECT string_agg(quote_ident(col->>'name'), ',' ORDER BY (col->>'attnum')::int), \
                    s.external_column_contract::text \
             FROM flashback.snapshots s, jsonb_array_elements(s.external_column_contract) AS col \
             WHERE s.snapshot_id = {snapshot_id}::bigint AND s.tracking_id = {tracking_id}::bigint \
             GROUP BY s.external_column_contract"
        ))
    })?;
    let column_list = column_list.ok_or_else(|| {
        MarkerError::Other("no materializable columns bound for this snapshot".to_string())
    })?;
    let column_contract = column_contract.ok_or_else(|| {
        MarkerError::Other("no materializable column contract bound for this snapshot".to_string())
    })?;
    segment
        .write_column_list(&column_list)
        .map_err(MarkerError::Other)?;
    segment
        .write_column_contract(&column_contract)
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
    struct UnpinOnExit(pg_sys::dsm_handle);
    impl Drop for UnpinOnExit {
        fn drop(&mut self) {
            unsafe { pg_sys::dsm_unpin_segment(self.0) };
        }
    }
    let _unpin = UnpinOnExit(segment.handle());

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
            let copy_identity = segment.read_copy_identity();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(identity) = copy_identity {
                    let pending = BackgroundWorker::transaction(|| {
                        stream_cursor_to_staging(
                            &segment,
                            &column_list,
                            &target_relation,
                            &segment.read_column_contract(),
                            identity,
                        )
                    })?;
                    crate::storage::worker::trigger_external_snapshot_failpoint(
                        "copier_after_copy_commit",
                    );
                    let manifest = pending.mark_copy_committed()?;
                    crate::storage::worker::trigger_external_snapshot_failpoint(
                        "copier_after_commit_receipt",
                    );
                    log!(
                        "pg_flashback external_zstd copier committed staged artifact: operation_nonce={}, rows={}, compressed_bytes={}",
                        manifest.operation_nonce,
                        manifest.row_count,
                        manifest.compressed_bytes
                    );
                    Ok(())
                } else {
                    BackgroundWorker::transaction(|| {
                        open_cursor_and_fetch(&column_list, &target_relation)
                    })
                    .map(|(n, pinned_lsn)| {
                        segment.write_fetched_row_count(n);
                        segment.write_pinned_wal_lsn(pinned_lsn);
                        segment.signal(HandoffPhase::SnapshotPinned);
                    })
                }
            }));
            match outcome {
                Ok(Ok(())) => {}
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

fn parse_column_contract(contract: &str) -> Result<Vec<ColumnDescriptor>, String> {
    fn json_u32(value: &serde_json::Value, name: &str) -> Result<u32, String> {
        value
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .or_else(|| value.as_str().and_then(|v| v.parse::<u32>().ok()))
            .ok_or_else(|| format!("{name} is outside u32"))
    }

    let value: serde_json::Value =
        serde_json::from_str(contract).map_err(|e| format!("invalid column contract JSON: {e}"))?;
    let entries = value
        .as_array()
        .ok_or_else(|| "column contract must be a JSON array".to_string())?;
    if entries.is_empty() {
        return Err("column contract is empty".to_string());
    }
    entries
        .iter()
        .map(|entry| {
            let field = |name: &str| {
                entry
                    .get(name)
                    .ok_or_else(|| format!("column contract entry is missing {name}"))
            };
            let attidentity = field("attidentity")?
                .as_str()
                .ok_or_else(|| "attidentity must be a string".to_string())?
                .as_bytes()
                .first()
                .copied()
                .unwrap_or(0);
            Ok(ColumnDescriptor {
                attnum: field("attnum")?
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or_else(|| "attnum is outside i32".to_string())?,
                // PostgreSQL's jsonb conversion renders OID-typed values as
                // decimal strings, while hand-built test contracts may use
                // JSON numbers. Accept both representations, with the same
                // exact u32 bound.
                atttypid: json_u32(field("atttypid")?, "atttypid")?,
                atttypmod: field("atttypmod")?
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or_else(|| "atttypmod is outside i32".to_string())?,
                attcollation: json_u32(field("attcollation")?, "attcollation")?,
                attnotnull: field("attnotnull")?
                    .as_bool()
                    .ok_or_else(|| "attnotnull must be boolean".to_string())?,
                attidentity,
                name: field("name")?
                    .as_str()
                    .ok_or_else(|| "column name must be a string".to_string())?
                    .to_string(),
            })
        })
        .collect()
}

struct CountingHashReader<R> {
    inner: R,
    hasher: Sha256,
    bytes: u64,
}

impl<R> CountingHashReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.bytes, format!("{:x}", self.hasher.finalize()))
    }
}

impl<R: Read> Read for CountingHashReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        self.bytes = self.bytes.saturating_add(read as u64);
        Ok(read)
    }
}

fn quote_identifier(identifier: &str) -> Result<String, String> {
    let input = CString::new(identifier).map_err(|_| "identifier contains NUL".to_string())?;
    let quoted = unsafe { pg_sys::quote_identifier(input.as_ptr()) };
    if quoted.is_null() {
        return Err("quote_identifier returned NULL".to_string());
    }
    Ok(unsafe { CStr::from_ptr(quoted) }
        .to_string_lossy()
        .into_owned())
}

fn insert_decoded_batch(
    destination: &str,
    columns: &[ColumnDescriptor],
    receivers: &[TypeReceiveInfo],
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<u64, String> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut parameter = 1_usize;
    let values_sql = rows
        .iter()
        .map(|_| {
            let tuple = (0..columns.len())
                .map(|_| {
                    let item = format!("${parameter}");
                    parameter += 1;
                    item
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("({tuple})")
        })
        .collect::<Vec<_>>()
        .join(",");
    let column_list = columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Result<Vec<_>, _>>()?
        .join(",");
    let identity_override = if columns.iter().any(|column| column.attidentity != 0) {
        " OVERRIDING SYSTEM VALUE"
    } else {
        ""
    };
    let query = CString::new(format!(
        "INSERT INTO {destination} ({column_list}){identity_override} VALUES {values_sql}"
    ))
    .map_err(|_| "external materialize query contains NUL".to_string())?;
    let mut argument_types = Vec::with_capacity(rows.len() * columns.len());
    for _ in &rows {
        argument_types.extend(
            columns
                .iter()
                .map(|column| pg_sys::Oid::from(column.atttypid)),
        );
    }

    let inserted = unsafe {
        let parent = pg_sys::CurrentMemoryContext;
        let mut batch_context = pgrx::PgMemoryContexts::Transient {
            parent,
            name: "pg_flashback external restore batch",
            min_context_size: 8 * 1024,
            initial_block_size: 64 * 1024,
            max_block_size: 8 * 1024 * 1024,
        };
        batch_context.switch_to(|_| {
            let mut datums = Vec::with_capacity(rows.len() * columns.len());
            let mut nulls = Vec::with_capacity(rows.len() * columns.len());
            for row in &rows {
                for ((value, column), receiver) in row.iter().zip(columns).zip(receivers) {
                    match value {
                        Some(bytes) => {
                            datums.push(decode_datum(bytes, receiver, column.atttypmod));
                            nulls.push(b' ' as std::os::raw::c_char);
                        }
                        None => {
                            datums.push(pg_sys::Datum::from(0));
                            nulls.push(b'n' as std::os::raw::c_char);
                        }
                    }
                }
            }
            let status = pg_sys::SPI_execute_with_args(
                query.as_ptr(),
                i32::try_from(argument_types.len())
                    .map_err(|_| "too many external restore parameters".to_string())?,
                argument_types.as_mut_ptr(),
                datums.as_mut_ptr(),
                nulls.as_ptr(),
                false,
                0,
            );
            if status != pg_sys::SPI_OK_INSERT as i32 {
                return Err(format!(
                    "external restore INSERT returned SPI status {status}"
                ));
            }
            let processed = pg_sys::SPI_processed;
            if !pg_sys::SPI_tuptable.is_null() {
                pg_sys::SPI_freetuptable(pg_sys::SPI_tuptable);
            }
            Ok(processed)
        })
    }?;
    if inserted != rows.len() as u64 {
        return Err(format!(
            "external restore inserted {inserted} rows from a {}-row batch",
            rows.len()
        ));
    }
    Ok(inserted)
}

fn stream_cursor_to_staging(
    segment: &HandoffSegment,
    column_list: &str,
    target_relation: &str,
    column_contract: &str,
    identity: CopyIdentity,
) -> Result<PendingArtifact, String> {
    let root = crate::storage::worker::external_snapshot_root()?;
    crate::storage::external_zstd::validate_root_os_level(&root)?;
    crate::storage::external_zstd::validate_root_spi_level(&root)?;
    let columns = parse_column_contract(column_contract)?;
    let sends: Vec<TypeSendInfo> = columns
        .iter()
        .map(|column| resolve_send_info(pg_sys::Oid::from(column.atttypid)))
        .collect::<Result<_, _>>()?;
    let system_identifier = unsafe { pg_sys::GetSystemIdentifier() };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let mut artifact = ArtifactStream::create(
        std::path::Path::new(&root),
        system_identifier,
        database_oid,
        segment.operation_nonce(),
        crate::storage::worker::external_snapshot_zstd_level(),
    )?;
    crate::storage::worker::trigger_external_snapshot_failpoint("copier_after_staging_create");
    write_header(artifact.writer()?, &columns)
        .map_err(|e| format!("write artifact header: {e}"))?;

    let query = format!("SELECT {column_list} FROM {target_relation}");
    let query_c = std::ffi::CString::new(query).map_err(|e| e.to_string())?;
    let name_c = std::ffi::CString::new("pg_flashback_external_copy").unwrap();
    let batch_rows = crate::storage::worker::external_snapshot_batch_rows();
    let max_row_bytes = crate::storage::worker::external_snapshot_max_row_bytes();
    let mut row_count = 0_u64;

    Spi::connect(|_client| -> Result<(), String> {
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

        let mut first_fetch = true;
        loop {
            unsafe { pg_sys::SPI_cursor_fetch(portal, true, batch_rows as i64) };
            let processed = unsafe { pg_sys::SPI_processed as usize };
            if first_fetch {
                let pinned_lsn = unsafe { pg_sys::GetXLogInsertRecPtr() };
                segment.write_pinned_wal_lsn(pinned_lsn);
                segment.signal(HandoffPhase::SnapshotPinned);
                first_fetch = false;
            }
            if processed == 0 {
                break;
            }

            let tuptable = unsafe { pg_sys::SPI_tuptable };
            if tuptable.is_null() {
                unsafe { pg_sys::SPI_cursor_close(portal) };
                return Err("SPI_tuptable was null after cursor fetch".to_string());
            }
            let tupdesc = unsafe { (*tuptable).tupdesc };
            let tuples = unsafe { (*tuptable).vals };
            for row_index in 0..processed {
                let tuple = unsafe { *tuples.add(row_index) };
                let mut values = Vec::with_capacity(columns.len());
                let mut encoded_size = columns.len().div_ceil(8);
                for (column_index, send) in sends.iter().enumerate() {
                    let mut is_null = false;
                    let datum = unsafe {
                        pg_sys::SPI_getbinval(
                            tuple,
                            tupdesc,
                            (column_index + 1) as i32,
                            &mut is_null,
                        )
                    };
                    if is_null {
                        values.push(None);
                    } else {
                        let bytes = unsafe { encode_datum(datum, send) };
                        encoded_size = encoded_size
                            .checked_add(4 + bytes.len())
                            .ok_or_else(|| "encoded row size overflow".to_string())?;
                        if encoded_size > max_row_bytes {
                            unsafe { pg_sys::SPI_cursor_close(portal) };
                            return Err(format!(
                                "encoded row exceeds pg_flashback.external_snapshot_max_row_bytes ({max_row_bytes})"
                            ));
                        }
                        values.push(Some(bytes));
                    }
                }
                write_row(artifact.writer()?, &values)
                    .map_err(|e| format!("write artifact row: {e}"))?;
                row_count = row_count.saturating_add(1);
            }
            unsafe { pg_sys::SPI_freetuptable(tuptable) };
            if row_count == processed as u64 {
                crate::storage::worker::trigger_external_snapshot_failpoint(
                    "copier_during_compression",
                );
            }
            pg_sys::check_for_interrupts!();
            if pgrx::bgworkers::BackgroundWorker::sigterm_received() {
                unsafe { pg_sys::SPI_cursor_close(portal) };
                return Err("external_zstd copy cancelled by SIGTERM".to_string());
            }
        }
        unsafe { pg_sys::SPI_cursor_close(portal) };
        Ok(())
    })?;

    write_trailer(artifact.writer()?, row_count)
        .map_err(|e| format!("write artifact trailer: {e}"))?;
    segment.write_fetched_row_count(row_count);
    let pending = artifact.finish_copy(ProvisionalManifest {
        operation_nonce: segment.operation_nonce(),
        database_oid,
        tracking_id: identity.tracking_id,
        generation_id: identity.generation_id,
        snapshot_id: identity.snapshot_id,
        format_version: FORMAT_VERSION,
        codec: "zstd".to_string(),
        row_count,
        uncompressed_bytes: 0,
        compressed_bytes: 0,
        checksum_sha256: String::new(),
    })?;
    crate::storage::worker::trigger_external_snapshot_failpoint(
        "copier_after_staged_fsync_before_commit",
    );
    Ok(pending)
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

    let operation_nonce = Spi::get_one::<i64>(&format!(
        "SELECT operation_nonce FROM flashback.coverage_generations \
         WHERE generation_id = {generation_id}::bigint \
           AND tracking_id = {tracking_id}::bigint \
           AND boundary_snapshot_id = {snapshot_id}::bigint \
           AND rel_oid_at_boundary = {rel_oid}::oid \
           AND state = 'building' AND storage_backend = 'external_zstd'"
    ))
    .unwrap_or_else(|error| pgrx::error!("cannot resolve external copy reservation: {error}"))
    .unwrap_or_else(|| pgrx::error!("external copy reservation does not exist"));
    if operation_nonce <= 0 {
        pgrx::error!("external copy reservation has an invalid operation_nonce");
    }

    let db_oid = unsafe { pg_sys::MyDatabaseId };
    let segment = unsafe { HandoffSegment::coordinator_create(operation_nonce as u64) };
    // pg_test cannot set a POSTMASTER-context artifact root per test. Its
    // coordinator regressions therefore keep exercising the exact real
    // lock/DSM/marker protocol in probe mode; production builds always bind
    // the copy identity and execute the real staged persist path.
    #[cfg(not(feature = "pg_test"))]
    segment
        .write_copy_identity(CopyIdentity {
            tracking_id,
            generation_id,
            snapshot_id,
            rel_oid: rel_oid as u32,
        })
        .unwrap_or_else(|error| pgrx::error!("cannot bind external copier identity: {error}"));
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
            // Last point before this call returns to its PL/pgSQL caller
            // (flashback_protect_external_copy / flashback_maintain_external_copy),
            // which the caller's own psql session then commits (M8). Crash
            // here exercises "backend dies after the marker transaction is
            // fully assembled but before its COMMIT reaches the client."
            crate::storage::worker::trigger_external_snapshot_failpoint(
                "protect_before_copy_commit",
            );
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

/// Finalize a copy-committed staged artifact after ordinary WAL consumption
/// has resolved its exact boundary LSN. The caller's SQL transaction owns
/// the database commit. Filesystem publication happens first; an abort after
/// the atomic rename leaves a manifest-bound orphan that this same function
/// resumes idempotently on its next call.
#[pg_extern]
fn flashback_internal_finalize_external_snapshot(
    tracking_id: i64,
    generation_id: i64,
    snapshot_id: i64,
) -> JsonB {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_finalize_external_snapshot is owner-only");
    }
    if tracking_id <= 0 || generation_id <= 0 || snapshot_id <= 0 {
        pgrx::error!("external finalizer identifiers must all be positive");
    }

    let evidence = Spi::connect(|client| -> Result<(i64, String, String, String), String> {
        let table = client
            .select(
                &format!(
                    "SELECT cg.operation_nonce, s.snapshot_lsn::text AS boundary_lsn, \
                            public.flashback_sha256(s.schema_def::text) AS schema_hash, \
                            s.external_column_contract::text AS column_contract \
                     FROM flashback.coverage_generations cg \
                     JOIN flashback.snapshots s \
                       ON s.snapshot_id = cg.boundary_snapshot_id \
                      AND s.tracking_id = cg.tracking_id \
                     WHERE cg.generation_id = {generation_id}::bigint \
                       AND cg.tracking_id = {tracking_id}::bigint \
                       AND cg.boundary_snapshot_id = {snapshot_id}::bigint \
                       AND cg.state IN ('building','capturing','active') \
                       AND cg.storage_backend = 'external_zstd' \
                       AND s.payload_state IN ('creating','available') \
                       AND s.storage_backend = 'external_zstd'"
                ),
                None,
                &[],
            )
            .map_err(|e| e.to_string())?;
        let row = table.first();
        let nonce = row
            .get_by_name::<i64, _>("operation_nonce")
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "external generation is not ready or does not exist".to_string())?;
        let boundary_lsn = row
            .get_by_name::<String, _>("boundary_lsn")
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "external generation boundary LSN is not resolved yet".to_string())?;
        let schema_hash = row
            .get_by_name::<String, _>("schema_hash")
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "external snapshot schema hash is missing".to_string())?;
        let column_contract = row
            .get_by_name::<String, _>("column_contract")
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "external snapshot column contract is missing".to_string())?;
        Ok((nonce, boundary_lsn, schema_hash, column_contract))
    })
    .unwrap_or_else(|error| pgrx::error!("external finalizer evidence query failed: {error}"));

    if evidence.0 <= 0 {
        pgrx::error!("external generation operation_nonce is invalid");
    }
    let column_contract: serde_json::Value = serde_json::from_str(&evidence.3)
        .unwrap_or_else(|error| pgrx::error!("external column contract is invalid: {error}"));
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|error| pgrx::error!("external snapshot root is unsafe: {error}"));

    let finalized = finalize_staged_artifact(
        std::path::Path::new(&root),
        FinalizationInput {
            system_identifier: unsafe { pg_sys::GetSystemIdentifier() },
            database_oid: unsafe { pg_sys::MyDatabaseId }.to_u32(),
            tracking_id,
            generation_id,
            snapshot_id,
            operation_nonce: evidence.0 as u64,
            boundary_lsn: evidence.1.clone(),
            schema_def_sha256: evidence.2.clone(),
            column_contract: column_contract.clone(),
            pg_major: pg_sys::PG_VERSION_NUM / 10_000,
        },
    )
    .unwrap_or_else(|error| pgrx::error!("external artifact finalization failed: {error}"));

    let locator_text = finalized.locator.to_string();
    let contract_text = finalized.manifest.column_contract.to_string();
    Spi::run(&format!(
        "SELECT public.flashback_internal_publish_external_snapshot(\
         {snapshot_id}::bigint, {tracking_id}::bigint, {generation_id}::bigint, {}::bigint, \
         {}::jsonb, {}::bigint, {}, {}::integer, {}::bigint, {}::bigint, {}, {}::jsonb, {})",
        finalized.provisional.operation_nonce,
        quote_literal(&locator_text),
        finalized.provisional.row_count,
        quote_literal(&finalized.manifest.codec),
        finalized.manifest.format_version,
        finalized.provisional.uncompressed_bytes,
        finalized.provisional.compressed_bytes,
        quote_literal(&finalized.provisional.checksum_sha256),
        quote_literal(&contract_text),
        quote_literal(&finalized.manifest.schema_def_sha256),
    ))
    .unwrap_or_else(|error| pgrx::error!("external snapshot publication rejected: {error}"));
    crate::storage::worker::trigger_external_snapshot_failpoint("finalizer_after_db_available");
    Spi::run(&format!(
        "SELECT public.flashback_internal_activate_external_generation(\
         {generation_id}::bigint, {tracking_id}::bigint, {snapshot_id}::bigint)"
    ))
    .unwrap_or_else(|error| pgrx::error!("external generation activation rejected: {error}"));
    crate::storage::worker::trigger_external_snapshot_failpoint("finalizer_after_activation");
    finalized
        .cleanup_coordination_files()
        .unwrap_or_else(|error| pgrx::error!("external finalizer cleanup failed: {error}"));

    JsonB(serde_json::json!({
        "status": "available",
        "tracking_id": tracking_id,
        "generation_id": generation_id,
        "snapshot_id": snapshot_id,
        "locator": finalized.locator,
        "row_count": finalized.provisional.row_count,
        "compressed_bytes": finalized.provisional.compressed_bytes,
        "checksum_sha256": finalized.provisional.checksum_sha256,
    }))
}

fn external_reservation_nonce(
    tracking_id: i64,
    generation_id: i64,
    snapshot_id: i64,
    require_aborted: bool,
) -> Result<u64, String> {
    let state_clause = if require_aborted {
        "AND cg.state='aborted' AND s.payload_state='aborted'"
    } else {
        "AND cg.state IN ('building','capturing','active','aborted') \
         AND s.payload_state IN ('creating','available','aborted')"
    };
    let nonce = Spi::get_one::<i64>(&format!(
        "SELECT cg.operation_nonce \
         FROM flashback.coverage_generations cg \
         JOIN flashback.snapshots s \
           ON s.snapshot_id=cg.boundary_snapshot_id \
          AND s.tracking_id=cg.tracking_id \
         WHERE cg.generation_id={generation_id}::bigint \
           AND cg.tracking_id={tracking_id}::bigint \
           AND cg.boundary_snapshot_id={snapshot_id}::bigint \
           AND cg.storage_backend='external_zstd' \
           AND s.storage_backend='external_zstd' {state_clause}"
    ))
    .map_err(|error| format!("external reservation evidence query failed: {error}"))?
    .ok_or_else(|| {
        "external reservation identity does not exist in the required state".to_string()
    })?;
    if nonce <= 0 {
        return Err("external reservation operation_nonce is invalid".to_string());
    }
    Ok(nonce as u64)
}

/// Read-only, exact-identity filesystem state used by the maintenance
/// reconciler. It never scans another database or trusts mtimes.
#[pg_extern]
fn flashback_internal_external_artifact_state(
    tracking_id: i64,
    generation_id: i64,
    snapshot_id: i64,
) -> JsonB {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_external_artifact_state is owner-only");
    }
    let nonce = external_reservation_nonce(tracking_id, generation_id, snapshot_id, false)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|error| pgrx::error!("external snapshot root is unsafe: {error}"));
    let state = probe_external_artifact_state(
        std::path::Path::new(&root),
        unsafe { pg_sys::GetSystemIdentifier() },
        unsafe { pg_sys::MyDatabaseId }.to_u32(),
        tracking_id,
        snapshot_id,
        nonce,
    )
    .unwrap_or_else(|error| pgrx::error!("external artifact state probe failed: {error}"));
    JsonB(serde_json::json!({
        "status": state.as_str(),
        "tracking_id": tracking_id,
        "generation_id": generation_id,
        "snapshot_id": snapshot_id,
        "operation_nonce": nonce,
    }))
}

/// Physical cleanup for a reservation whose DB generation and snapshot have
/// already committed their aborted states. A live copier lock refuses purge;
/// a crash between physical cleanup and this transaction's commit is safe
/// because the durable state was established by an earlier transaction.
#[pg_extern]
fn flashback_internal_purge_aborted_external_artifact(
    tracking_id: i64,
    generation_id: i64,
    snapshot_id: i64,
) -> bool {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_purge_aborted_external_artifact is owner-only");
    }
    let nonce = external_reservation_nonce(tracking_id, generation_id, snapshot_id, true)
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|error| pgrx::error!("external snapshot root is unsafe: {error}"));
    let root = std::path::Path::new(&root);
    let system_identifier = unsafe { pg_sys::GetSystemIdentifier() };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let staged = purge_staged_artifact(root, system_identifier, database_oid, nonce)
        .unwrap_or_else(|error| pgrx::error!("external staging purge failed: {error}"));
    let published = purge_published_artifact(
        root,
        system_identifier,
        database_oid,
        tracking_id,
        snapshot_id,
        nonce,
    )
    .unwrap_or_else(|error| pgrx::error!("external published-orphan purge failed: {error}"));
    staged || published
}

fn external_snapshot_health_check(
    snapshot_id: i64,
    tracking_id: i64,
    deep: bool,
) -> Result<(), String> {
    let (locator, expected) = Spi::connect(
        |client| -> Result<(serde_json::Value, FinalManifest), String> {
            let table = client
                .select(
                    &format!(
                        "SELECT s.locator::text AS locator, s.row_count, s.snapshot_lsn::text, \
                                s.external_codec, s.external_format_version, \
                                s.external_uncompressed_bytes, s.external_compressed_bytes, \
                                s.external_checksum_sha256, s.external_column_contract::text, \
                                s.schema_def_sha256 \
                         FROM flashback.snapshots s \
                         WHERE s.snapshot_id={snapshot_id}::bigint \
                           AND s.tracking_id={tracking_id}::bigint \
                           AND s.payload_state='available' \
                           AND s.storage_backend='external_zstd'",
                    ),
                    None,
                    &[],
                )
                .map_err(|e| e.to_string())?;
            let row = table.first();
            let required_string = |name: &str| -> Result<String, String> {
                row.get_by_name::<String, _>(name)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("external snapshot evidence {name} is missing"))
            };
            let locator: serde_json::Value = serde_json::from_str(&required_string("locator")?)
                .map_err(|e| format!("invalid external locator: {e}"))?;
            let parse_locator_u32 = |name: &str| -> Result<u32, String> {
                locator
                    .get(name)
                    .and_then(|v| v.as_str())
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| format!("external locator {name} is invalid"))
            };
            let contract: serde_json::Value =
                serde_json::from_str(&required_string("external_column_contract")?)
                    .map_err(|e| format!("invalid external column contract: {e}"))?;
            let expected = FinalManifest {
                format_version: row
                    .get_by_name::<i32, _>("external_format_version")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external format version is missing".to_string())?
                    .try_into()
                    .map_err(|_| "external format version is negative".to_string())?,
                codec: required_string("external_codec")?,
                system_identifier: locator
                    .get("system_identifier")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "external locator system_identifier is missing".to_string())?
                    .to_string(),
                database_oid: parse_locator_u32("database_oid")?,
                tracking_id,
                snapshot_id,
                boundary_lsn: required_string("snapshot_lsn")?,
                schema_def_sha256: required_string("schema_def_sha256")?,
                column_contract: contract,
                row_count: row
                    .get_by_name::<i64, _>("row_count")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external row_count is missing".to_string())?
                    .try_into()
                    .map_err(|_| "external row_count is negative".to_string())?,
                external_uncompressed_bytes: row
                    .get_by_name::<i64, _>("external_uncompressed_bytes")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external uncompressed bytes are missing".to_string())?
                    .try_into()
                    .map_err(|_| "external uncompressed bytes are negative".to_string())?,
                external_compressed_bytes: row
                    .get_by_name::<i64, _>("external_compressed_bytes")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external compressed bytes are missing".to_string())?
                    .try_into()
                    .map_err(|_| "external compressed bytes are negative".to_string())?,
                external_checksum_sha256: required_string("external_checksum_sha256")?,
                pg_major: pg_sys::PG_VERSION_NUM / 10_000,
            };
            Ok((locator, expected))
        },
    )?;

    let system_identifier = unsafe { pg_sys::GetSystemIdentifier() };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let locator_string = |name: &str| -> Result<&str, String> {
        locator
            .get(name)
            .and_then(|value| value.as_str())
            .ok_or_else(|| format!("external locator is missing {name}"))
    };
    if locator_string("system_identifier")? != system_identifier.to_string()
        || locator_string("database_oid")? != database_oid.to_string()
        || locator_string("tracking_id")? != tracking_id.to_string()
        || locator_string("snapshot_id")? != snapshot_id.to_string()
    {
        return Err("external locator does not belong to this database/lifecycle".to_string());
    }
    let operation_nonce: u64 = locator_string("nonce")?
        .parse()
        .map_err(|_| "external locator nonce is invalid".to_string())?;
    let root = crate::storage::worker::external_snapshot_root()?;
    crate::storage::external_zstd::validate_root_os_level(&root)?;
    crate::storage::external_zstd::validate_root_spi_level(&root)?;
    let (artifact, disk_manifest) = open_published_artifact(
        std::path::Path::new(&root),
        system_identifier,
        database_oid,
        tracking_id,
        snapshot_id,
        operation_nonce,
    )?;
    if disk_manifest != expected {
        return Err("external artifact manifest conflicts with database evidence".to_string());
    }
    let compressed_len = artifact
        .metadata()
        .map_err(|e| format!("stat external artifact: {e}"))?
        .len();
    if compressed_len != expected.external_compressed_bytes {
        return Err(format!(
            "external artifact compressed length {compressed_len} does not match manifest {}",
            expected.external_compressed_bytes
        ));
    }
    if !deep {
        return Ok(());
    }

    let columns = parse_column_contract(&expected.column_contract.to_string())?;
    let decoder = zstd::stream::read::Decoder::new(artifact)
        .map_err(|e| format!("external zstd decoder failed: {e}"))?;
    let mut reader = CountingHashReader::new(decoder);
    let header = read_header(&mut reader)
        .map_err(|e| format!("external artifact header is invalid: {e}"))?;
    if header != columns {
        return Err("external artifact header does not match column contract".to_string());
    }
    let max_row_bytes = crate::storage::worker::external_snapshot_max_row_bytes();
    for _ in 0..expected.row_count {
        read_row(&mut reader, columns.len(), max_row_bytes as i64)
            .map_err(|e| format!("external artifact row is invalid: {e}"))?;
        pg_sys::check_for_interrupts!();
    }
    let trailer = read_trailer(&mut reader)
        .map_err(|e| format!("external artifact trailer is invalid: {e}"))?;
    if trailer != expected.row_count {
        return Err(format!(
            "external artifact trailer row count {trailer} does not match manifest {}",
            expected.row_count
        ));
    }
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .map_err(|e| format!("external artifact EOF check failed: {e}"))?
        != 0
    {
        return Err("external artifact contains trailing uncompressed data".to_string());
    }
    let (uncompressed_bytes, checksum) = reader.finish();
    if uncompressed_bytes != expected.external_uncompressed_bytes {
        return Err(format!(
            "external artifact uncompressed byte count {uncompressed_bytes} does not match manifest {}",
            expected.external_uncompressed_bytes
        ));
    }
    if checksum != expected.external_checksum_sha256 {
        return Err("external artifact checksum does not match manifest".to_string());
    }
    Ok(())
}

/// Read-only health probe. It never mutates snapshot or coverage state; the
/// maintenance reconciler is the sole authority allowed to turn an unhealthy
/// result into a durable `missing` state and coverage gap.
#[pg_extern]
fn flashback_internal_external_snapshot_health(
    snapshot_id: i64,
    tracking_id: i64,
    deep: default!(bool, false),
) -> JsonB {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_external_snapshot_health is owner-only");
    }
    match external_snapshot_health_check(snapshot_id, tracking_id, deep) {
        Ok(()) => JsonB(serde_json::json!({"status": "healthy", "reason": null})),
        Err(reason) => JsonB(serde_json::json!({"status": "unhealthy", "reason": reason})),
    }
}

/// Physical phase of external artifact retirement. Database state must have
/// committed `available -> retiring` in an earlier transaction. This function
/// owns no state transition; it only purges the exact locator-bound directory.
#[pg_extern]
fn flashback_internal_purge_external_snapshot(snapshot_id: i64, tracking_id: i64) -> bool {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_purge_external_snapshot is owner-only");
    }
    let locator_text = Spi::get_one::<String>(&format!(
        "SELECT locator::text FROM flashback.snapshots \
         WHERE snapshot_id={snapshot_id}::bigint \
           AND tracking_id={tracking_id}::bigint \
           AND payload_state='retiring' \
           AND storage_backend='external_zstd'"
    ))
    .unwrap_or_else(|error| pgrx::error!("external purge evidence query failed: {error}"))
    .unwrap_or_else(|| pgrx::error!("external purge requires an exact retiring snapshot artifact"));
    let locator: serde_json::Value = serde_json::from_str(&locator_text)
        .unwrap_or_else(|error| pgrx::error!("external purge locator is invalid: {error}"));
    let locator_string = |name: &str| {
        locator
            .get(name)
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| pgrx::error!("external purge locator is missing {name}"))
    };
    let system_identifier = unsafe { pg_sys::GetSystemIdentifier() };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    if locator_string("system_identifier") != system_identifier.to_string()
        || locator_string("database_oid") != database_oid.to_string()
        || locator_string("tracking_id") != tracking_id.to_string()
        || locator_string("snapshot_id") != snapshot_id.to_string()
    {
        pgrx::error!("external purge locator does not belong to this database/lifecycle");
    }
    let operation_nonce: u64 = locator_string("nonce")
        .parse()
        .unwrap_or_else(|_| pgrx::error!("external purge locator nonce is invalid"));
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|error| pgrx::error!("external snapshot root is unsafe: {error}"));
    let purged = purge_published_artifact(
        std::path::Path::new(&root),
        system_identifier,
        database_oid,
        tracking_id,
        snapshot_id,
        operation_nonce,
    )
    .unwrap_or_else(|error| pgrx::error!("external artifact purge failed: {error}"));
    crate::storage::worker::trigger_external_snapshot_failpoint("retire_after_delete");
    purged
}

/// Stream one verified external_zstd artifact into an already-created shadow
/// relation. Every immutable database/manifest/locator field is rebound before
/// the first INSERT; decode, row count, trailer, byte count, and checksum must
/// all agree before this function can return successfully.
#[pg_extern]
fn flashback_internal_materialize_external_snapshot(
    snapshot_id: i64,
    tracking_id: i64,
    destination_schema: &str,
    destination_table: &str,
) -> i64 {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("flashback_internal_materialize_external_snapshot is owner-only");
    }
    if snapshot_id <= 0 || tracking_id <= 0 {
        pgrx::error!("external materialize identifiers must be positive");
    }

    let evidence = Spi::connect(
        |client| -> Result<(serde_json::Value, FinalManifest, serde_json::Value), String> {
            let table = client
                .select(
                    &format!(
                        "SELECT s.locator::text AS locator, s.row_count, s.snapshot_lsn::text, \
                                s.external_codec, s.external_format_version, \
                                s.external_uncompressed_bytes, s.external_compressed_bytes, \
                                s.external_checksum_sha256, s.external_column_contract::text, \
                                s.schema_def_sha256, \
                                public.flashback_internal_materializable_columns(\
                                  format('%I.%I', {}, {})::regclass)::text AS destination_contract \
                         FROM flashback.snapshots s \
                         WHERE s.snapshot_id={snapshot_id}::bigint \
                           AND s.tracking_id={tracking_id}::bigint \
                           AND s.payload_state='available' \
                           AND s.storage_backend='external_zstd'",
                        quote_literal(destination_schema),
                        quote_literal(destination_table),
                    ),
                    None,
                    &[],
                )
                .map_err(|e| e.to_string())?;
            let row = table.first();
            let required_string = |name: &str| -> Result<String, String> {
                row.get_by_name::<String, _>(name)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("external snapshot evidence {name} is missing"))
            };
            let locator: serde_json::Value = serde_json::from_str(&required_string("locator")?)
                .map_err(|e| format!("invalid external locator: {e}"))?;
            let column_contract: serde_json::Value =
                serde_json::from_str(&required_string("external_column_contract")?)
                    .map_err(|e| format!("invalid external column contract: {e}"))?;
            let destination_contract: serde_json::Value =
                serde_json::from_str(&required_string("destination_contract")?)
                    .map_err(|e| format!("invalid destination column contract: {e}"))?;
            if destination_contract != column_contract {
                return Err(
                    "destination relation column contract does not match external artifact"
                        .to_string(),
                );
            }
            let manifest = FinalManifest {
                format_version: row
                    .get_by_name::<i32, _>("external_format_version")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external format version is missing".to_string())?
                    as u32,
                codec: required_string("external_codec")?,
                system_identifier: locator
                    .get("system_identifier")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "locator system_identifier is missing".to_string())?
                    .to_string(),
                database_oid: locator
                    .get("database_oid")
                    .and_then(|v| v.as_str())
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| "locator database_oid is invalid".to_string())?,
                tracking_id,
                snapshot_id,
                boundary_lsn: required_string("snapshot_lsn")?,
                schema_def_sha256: required_string("schema_def_sha256")?,
                column_contract,
                row_count: row
                    .get_by_name::<i64, _>("row_count")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external row_count is missing".to_string())?
                    .try_into()
                    .map_err(|_| "external row_count is negative".to_string())?,
                external_uncompressed_bytes: row
                    .get_by_name::<i64, _>("external_uncompressed_bytes")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external uncompressed byte count is missing".to_string())?
                    .try_into()
                    .map_err(|_| "external uncompressed byte count is negative".to_string())?,
                external_compressed_bytes: row
                    .get_by_name::<i64, _>("external_compressed_bytes")
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "external compressed byte count is missing".to_string())?
                    .try_into()
                    .map_err(|_| "external compressed byte count is negative".to_string())?,
                external_checksum_sha256: required_string("external_checksum_sha256")?,
                pg_major: pg_sys::PG_VERSION_NUM / 10_000,
            };
            Ok((locator, manifest, destination_contract))
        },
    )
    .unwrap_or_else(|error| pgrx::error!("external materialize admission failed: {error}"));

    let system_identifier = unsafe { pg_sys::GetSystemIdentifier() };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let locator_string = |name: &str| {
        evidence
            .0
            .get(name)
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| pgrx::error!("external locator is missing {name}"))
    };
    if locator_string("system_identifier") != system_identifier.to_string()
        || locator_string("database_oid") != database_oid.to_string()
        || locator_string("tracking_id") != tracking_id.to_string()
        || locator_string("snapshot_id") != snapshot_id.to_string()
    {
        pgrx::error!("external locator does not belong to this database/lifecycle");
    }
    let operation_nonce: u64 = locator_string("nonce")
        .parse()
        .unwrap_or_else(|_| pgrx::error!("external locator nonce is invalid"));
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|error| pgrx::error!("{error}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|error| pgrx::error!("external snapshot root is unsafe: {error}"));
    let (artifact, disk_manifest) = open_published_artifact(
        std::path::Path::new(&root),
        system_identifier,
        database_oid,
        tracking_id,
        snapshot_id,
        operation_nonce,
    )
    .unwrap_or_else(|error| pgrx::error!("external artifact open failed: {error}"));
    if disk_manifest != evidence.1 {
        pgrx::error!("external artifact manifest conflicts with database evidence");
    }
    let compressed_len = artifact
        .metadata()
        .unwrap_or_else(|error| pgrx::error!("external artifact stat failed: {error}"))
        .len();
    if compressed_len != disk_manifest.external_compressed_bytes {
        pgrx::error!(
            "external artifact compressed length {compressed_len} does not match manifest {}",
            disk_manifest.external_compressed_bytes
        );
    }

    let columns = parse_column_contract(&disk_manifest.column_contract.to_string())
        .unwrap_or_else(|error| pgrx::error!("external artifact contract is invalid: {error}"));
    let receivers = columns
        .iter()
        .map(|column| resolve_receive_info(pg_sys::Oid::from(column.atttypid)))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| pgrx::error!("external receive type is unsupported: {error}"));
    let destination = format!(
        "{}.{}",
        quote_identifier(destination_schema)
            .unwrap_or_else(|error| pgrx::error!("invalid destination schema: {error}")),
        quote_identifier(destination_table)
            .unwrap_or_else(|error| pgrx::error!("invalid destination table: {error}")),
    );
    let decoder = zstd::stream::read::Decoder::new(artifact)
        .unwrap_or_else(|error| pgrx::error!("external zstd decoder failed: {error}"));
    let mut reader = CountingHashReader::new(decoder);
    let header = read_header(&mut reader)
        .unwrap_or_else(|error| pgrx::error!("external artifact header is invalid: {error}"));
    if header != columns {
        pgrx::error!("external artifact header does not match immutable column contract");
    }
    let max_parameters = 65_535_usize;
    let batch_limit = crate::storage::worker::external_snapshot_batch_rows()
        .min(max_parameters / columns.len().max(1))
        .max(1);
    let max_row_bytes = crate::storage::worker::external_snapshot_max_row_bytes();
    let mut inserted = 0_u64;
    Spi::connect_mut(|_client| {
        let mut batch = Vec::with_capacity(batch_limit);
        while inserted + (batch.len() as u64) < disk_manifest.row_count {
            batch.push(
                read_row(&mut reader, columns.len(), max_row_bytes as i64).unwrap_or_else(
                    |error| pgrx::error!("external artifact row decode failed: {error}"),
                ),
            );
            if batch.len() == batch_limit
                || inserted + batch.len() as u64 == disk_manifest.row_count
            {
                inserted += insert_decoded_batch(&destination, &columns, &receivers, batch)
                    .unwrap_or_else(|error| pgrx::error!("external batch insert failed: {error}"));
                batch = Vec::with_capacity(batch_limit);
                if inserted > 0 {
                    crate::storage::worker::trigger_external_snapshot_failpoint(
                        "restore_during_decode",
                    );
                }
                pg_sys::check_for_interrupts!();
            }
        }
    });
    let trailer = read_trailer(&mut reader)
        .unwrap_or_else(|error| pgrx::error!("external artifact trailer is invalid: {error}"));
    if trailer != disk_manifest.row_count || inserted != disk_manifest.row_count {
        pgrx::error!(
            "external artifact row count mismatch: manifest={}, trailer={trailer}, inserted={inserted}",
            disk_manifest.row_count
        );
    }
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .unwrap_or_else(|error| pgrx::error!("external artifact EOF check failed: {error}"))
        != 0
    {
        pgrx::error!("external artifact contains trailing uncompressed data");
    }
    let (uncompressed_bytes, checksum) = reader.finish();
    if uncompressed_bytes != disk_manifest.external_uncompressed_bytes
        || checksum != disk_manifest.external_checksum_sha256
    {
        pgrx::error!(
            "external artifact integrity mismatch: bytes={uncompressed_bytes}, checksum={checksum}"
        );
    }
    inserted as i64
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

    /// Step 9 initial-protect fixture: genuinely NO parent/predecessor
    /// generation exists for this tracking_id -- the real new topology this
    /// stage adds, unlike `Fixture`/`setup()` above (whose own doc comment
    /// explains why every *existing* online-create reservation always has an
    /// active heap_v1 parent absorbing writes; that mechanism is exactly
    /// what a parentless reservation cannot rely on). Reuses the module's
    /// one shared 'active' capture stream (`capture_streams_one_active_idx`
    /// permits only one per database).
    struct InitialFixture {
        tracking_id: i64,
        rel_oid: pg_sys::Oid,
        stream_id: i64,
    }

    fn setup_initial(table_name: &str, cols: &str) -> InitialFixture {
        let db_oid = unsafe { pg_sys::MyDatabaseId };
        let stream_id = shared_stream_id(db_oid);

        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "DO $do$
             DECLARE
                 v_rel_oid oid;
                 v_tracking_id bigint;
             BEGIN
                 CREATE TABLE IF NOT EXISTS public.{table_name} ({cols});
                 v_rel_oid := 'public.{table_name}'::regclass::oid;

                 -- Deliberately NOT flashback_bootstrap_local_delta_lifecycle_
                 -- core: no heap CTAS, no coverage_generations row at all yet
                 -- -- this tracking_id has zero generations until reserve_
                 -- initial() below, which is exactly the state a real Step 9
                 -- reservation transaction (task 3) must produce. is_active
                 -- must be true from this exact row for flashback_consume_
                 -- wal()'s tracked_oids computation to ever decode this
                 -- relation at all (api_track_capture.sql).
                 INSERT INTO flashback.tracked_tables (
                     rel_oid, schema_name, table_name, base_snapshot_table,
                     recovery_profile, is_active
                 )
                 VALUES (v_rel_oid, 'public', '{table_name}', NULL, 'local_delta', true)
                 RETURNING tracking_id INTO v_tracking_id;
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

        InitialFixture {
            tracking_id,
            rel_oid,
            stream_id,
        }
    }

    /// Mirrors reserve() above but generation_no => 1, p_parent_generation_id
    /// => NULL -- the exact call shape flashback_internal_reserve_online_
    /// generation already accepted at the primitive layer before this stage
    /// (confirmed by reading it directly: it only validates a non-NULL
    /// parent, never requires one), but which nothing durably routed WAL for
    /// while 'building' until wal_promote_core.sql's new 'capturing' state.
    fn reserve_initial(fx: &InitialFixture, nonce: i64) -> (i64, i64) {
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "SELECT public.flashback_internal_reserve_online_generation(\
             p_tracking_id => {}::bigint, p_rel_oid => {}::oid, p_stream_id => {}::bigint, \
             p_generation_no => 1::bigint, p_parent_generation_id => NULL, \
             p_storage_backend => 'external_zstd', p_operation_nonce => {nonce}::bigint)",
            fx.tracking_id,
            fx.rel_oid.to_u32(),
            fx.stream_id,
        ));
        let (gen_id, snap_id) = Spi::get_two::<i64, i64>(&format!(
            "SELECT generation_id, boundary_snapshot_id FROM flashback.coverage_generations \
             WHERE tracking_id = {}::bigint AND generation_no = 1",
            fx.tracking_id
        ))
        .unwrap();
        (gen_id.unwrap(), snap_id.unwrap())
    }

    fn marker_sql_initial(fx: &InitialFixture, generation_id: i64, snapshot_id: i64) -> String {
        format!(
            "SELECT public.flashback_internal_run_external_marker_transaction(\
             {}::bigint, {}::bigint, {generation_id}::bigint, {snapshot_id}::bigint)",
            fx.tracking_id,
            fx.rel_oid.to_u32()
        )
    }

    /// The central proof this whole stage exists for: reservation and the
    /// M1-M7 marker transaction run through real, production entry points
    /// in a genuinely separate, committing session (not synthetic), binding
    /// a real boundary_xid. Whether the *decoded WAL batch* reaching
    /// flashback_apply_decoded_wal_batch (wal_promote_core.sql) for that
    /// real boundary_xid is real or staged by the established
    /// flashback_test_inject_commit / flashback_test_inject_batch seams is
    /// the one thing this harness cannot decide either way:
    /// pg_create_logical_replication_slot refuses to run in every session
    /// this test framework provides -- confirmed directly, not assumed, by
    /// trying it as the literal first statement of this function's own
    /// session (still "cannot create logical replication slot in
    /// transaction that has performed writes"), and by routing it through
    /// the shared commit-sql worker with nothing else in its payload (same
    /// error). No existing test anywhere in this codebase creates a real
    /// slot either (confirmed by search), which is exactly why the
    /// production-facing dml_*/ddl_* integration tests all use this same
    /// seam. Neither seam is a fake: both stage pg_temp._fb_wal_batch and
    /// call the real, unmodified flashback_apply_decoded_wal_batch, so
    /// every one of wal_promote_core.sql's new lines (the building ->
    /// capturing transition, the qualified CTE's widened state list and
    /// strict `>` boundary comparison) executes for real here. What is
    /// synthetic is only the batch's own manufacture, not the promotion
    /// logic under test.
    ///
    /// This is NOT the production WAL handoff fully qualified end to end.
    /// It does not exercise the real output plugin, a real logical slot, or
    /// a real background worker consuming it continuously under concurrent
    /// load -- only wal_promote_core.sql's own promotion logic, called
    /// directly with a hand-staged batch. The fully end-to-end proof -- a
    /// real slot, a real decoder, a real separately-running cluster, real
    /// concurrent traffic -- is qualification stage 7 (a throttled
    /// small-table online initial-protect test against a normally-started
    /// PostgreSQL instance, not this harness) and the stage 8 1 GiB
    /// production run; nothing in this test may be cited as evidence that
    /// either of those has already happened.
    ///
    /// Proves, for a tracking_id with NO predecessor generation:
    ///  1. No heap_v1 payload table is ever created for it (its snapshot
    ///     row's storage_backend is external_zstd and snapshot_table is
    ///     empty/absent from the very first generation).
    ///  2. The generation reaches 'capturing' (not 'active') the moment its
    ///     real, marker-bound boundary commit is promoted -- proving WAL
    ///     consumption itself, not a later authority, is what makes it a
    ///     write target.
    ///  3. Every ordinary INSERT/UPDATE/DELETE committed strictly after the
    ///     boundary is captured into flashback.delta_log under this exact
    ///     generation_id, exactly once (re-promoting is idempotent).
    ///  4. The boundary interval is exact: commit_lsn < boundary_lsn and
    ///     commit_lsn == boundary_lsn are both entirely excluded, proven
    ///     together with #3 in one single mixed batch (pre-boundary,
    ///     boundary-with-an-attached-event, and three post-boundary
    ///     commits staged together via flashback_test_inject_batch) -- the
    ///     only way to prove the procedural building -> capturing
    ///     transition partway through a batch does not make the qualified
    ///     CTE, which runs once at the end using whatever boundary_lsn is
    ///     current by then, absorb the wrong prefix.
    ///  5. flashback_health() reports the exact 'maintenance_required' /
    ///     'wait_for_boundary_commit_resolution' code while only
    ///     'capturing' -- not merely "not healthy" (any unrelated fault
    ///     would also satisfy that), and doctor() never reports 'ok'.
    #[pg_test]
    fn test_initial_protect_no_parent_captures_wal_exactly_once() {
        let fx = setup_initial("it_coord_initial_protect", "id int primary key, note text");

        let nonce = 700200;
        let (generation_id, snapshot_id) = reserve_initial(&fx, nonce);

        // Proof #1: reserving the generation must never create a heap_v1
        // payload table -- the snapshot row is external_zstd from the very
        // first INSERT (flashback_internal_snapshot_reserve), not a later
        // conversion.
        let (backend, snapshot_table) = Spi::get_two::<String, String>(&format!(
            "SELECT storage_backend, snapshot_table FROM flashback.snapshots \
             WHERE snapshot_id = {snapshot_id}::bigint AND tracking_id = {}::bigint",
            fx.tracking_id
        ))
        .unwrap();
        assert_eq!(backend.unwrap(), "external_zstd");
        assert_eq!(
            snapshot_table.unwrap_or_default(),
            "",
            "external_zstd initial protect must never populate snapshot_table (that column is heap_v1-only)"
        );
        assert!(
            Spi::get_one::<bool>(&format!(
                "SELECT to_regclass('flashback.base_snapshot_t{}') IS NULL",
                fx.tracking_id
            ))
            .unwrap()
            .unwrap(),
            "no base_snapshot_t<tracking_id> heap payload table may exist for external initial protect"
        );

        // Run the real production marker transaction (M1-M7): binds the
        // boundary, pins the copier's real snapshot, all in one committing
        // transaction in a genuinely separate session.
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(
            &marker_sql_initial(&fx, generation_id, snapshot_id),
        );
        let boundary_xid = Spi::get_one::<i64>(&format!(
            "SELECT boundary_xid FROM flashback.coverage_generations \
             WHERE generation_id = {generation_id}::bigint"
        ))
        .unwrap()
        .unwrap();

        // Boundary-interval proof: one single batch mixing a pre-boundary
        // commit, the boundary-resolving commit itself (deliberately given
        // its own row event, at commit_lsn EXACTLY equal to the boundary --
        // flashback_test_inject_commit's own generation lookup refuses this
        // for a real boundary-resolving commit, since a real marker
        // transaction never carries row DML, but the qualified CTE's
        // strict `>` (not `>=`) is exactly what must reject it here), and
        // three post-boundary commits -- staged together via
        // flashback_test_inject_batch into ONE call to the real, unmodified
        // flashback_apply_decoded_wal_batch. This is the only way to prove
        // that the pending-generation loop's procedural building ->
        // capturing transition, which happens partway through this one
        // batch, does not cause the qualified CTE (which runs once, after
        // the whole loop, using whatever boundary_lsn is current by then)
        // to absorb the wrong prefix -- three separate single-commit calls
        // could never demonstrate that, since each would see the
        // generation's state exactly as the previous call left it.
        let pre_boundary_xid = 910099i64;
        let batch_json = format!(
            r#"[
                {{"commit_lsn":"0/1000","source_xid":{pre_boundary_xid},
                  "events":[{{"op":"INSERT","new":{{"id":1,"note":"pre-boundary"}}}}]}},
                {{"commit_lsn":"0/2000","source_xid":{boundary_xid},
                  "events":[{{"op":"INSERT","new":{{"id":99,"note":"smuggled-at-boundary"}}}}]}},
                {{"commit_lsn":"0/3000","source_xid":910300,
                  "events":[{{"op":"INSERT","new":{{"id":2,"note":"after-insert"}}}}]}},
                {{"commit_lsn":"0/3100","source_xid":910301,
                  "events":[{{"op":"UPDATE","old":{{"id":2,"note":"after-insert"}},"new":{{"id":2,"note":"after-update"}}}}]}},
                {{"commit_lsn":"0/3200","source_xid":910302,
                  "events":[{{"op":"DELETE","old":{{"id":2,"note":"after-update"}}}}]}}
            ]"#
        );
        Spi::run(&format!(
            "SELECT flashback_test_inject_batch({}::bigint, {}::bigint, '{batch_json}'::jsonb)",
            fx.tracking_id, fx.stream_id
        ))
        .expect("mixed pre-boundary/boundary/post-boundary batch promotion failed");

        // Proof #2: the generation must now be 'capturing', not 'active' --
        // its boundary commit was observed and durably recorded, but the
        // external artifact is not published/verified yet, so it must not
        // be recoverable.
        let (state, gen_boundary_xid, _marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(
            state, "capturing",
            "a parentless external_zstd generation must reach 'capturing' once its boundary commit is decoded"
        );
        assert_eq!(gen_boundary_xid, Some(boundary_xid));
        assert_eq!(
            payload_state, "creating",
            "the artifact must still be 'creating' -- this test never publishes/finalizes it"
        );

        // Proof #3 + boundary interval: exact delta_log content under this
        // generation_id -- the pre-boundary insert (commit_lsn < boundary,
        // id=1) and the smuggled boundary-commit insert (commit_lsn ==
        // boundary, id=99) must both be entirely absent; only the three
        // strictly-post-boundary events (commit_lsn > boundary, id=2) may
        // appear, exactly once, in commit order.
        let rows = Spi::connect(|c| {
            c.select(
                &format!(
                    "SELECT event_type, old_data, new_data \
                     FROM flashback.delta_log \
                     WHERE generation_id = {generation_id}::bigint \
                       AND tracking_id = {}::bigint \
                     ORDER BY event_id",
                    fx.tracking_id
                ),
                None,
                &[],
            )
            .unwrap()
            .map(|row| row.get_by_name::<String, _>("event_type").unwrap().unwrap())
            .collect::<Vec<_>>()
        });
        assert_eq!(
            rows,
            vec!["INSERT", "UPDATE", "DELETE"],
            "only the strictly-post-boundary INSERT/UPDATE/DELETE (id=2) may be captured, exactly once, in order; \
             neither the pre-boundary (id=1) nor the exactly-at-boundary (id=99) event may appear"
        );
        let smuggled_or_pre_boundary = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM flashback.delta_log \
             WHERE generation_id = {generation_id}::bigint \
               AND (new_data->>'id' IN ('1', '99') OR old_data->>'id' IN ('1', '99'))"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            smuggled_or_pre_boundary, 0,
            "the pre-boundary (id=1, commit_lsn < boundary) and exactly-at-boundary \
             (id=99, commit_lsn == boundary) events must never reach delta_log at all"
        );

        // Re-promoting the exact same commit_lsns must not duplicate
        // anything -- flashback_test_inject_commit itself checks
        // capture_commits for an existing row at that commit_lsn and
        // returns a no-op (test_wal_seam.sql), the identical idempotency
        // guarantee flashback_consume_wal's real slot-advancement path
        // relies on, now proven for a parentless 'capturing' generation.
        Spi::run(&format!(
            "SELECT flashback_test_inject_commit({}::bigint, '0/3000'::pg_lsn, clock_timestamp(), 910300::bigint, \
             jsonb_build_array(jsonb_build_object('op','INSERT','new',jsonb_build_object('id',2,'note','after-insert'))))",
            fx.tracking_id
        )).unwrap();
        let rows_after_replay = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM flashback.delta_log WHERE generation_id = {generation_id}::bigint"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            rows_after_replay, 3,
            "re-promoting an already-seen commit_lsn must not duplicate delta_log rows"
        );

        // Proof #5: an exact pending/capturing health code and an
        // actionable recommended_action -- not merely "not healthy", which
        // could just as easily mean a random unrelated fault. A 'capturing'
        // generation already has a resolved boundary_lsn/boundary_time (it
        // could not have reached 'capturing' otherwise -- state_authority.
        // sql's own cross-table check proves this), so health_runtime.sql
        // must NOT describe its boundary as still awaiting COMMIT LSN; what
        // is actually in progress is the external artifact's own
        // copy/publish/verify, a distinct, later phase with its own action.
        let (health, action, reason) = Spi::connect(|c| {
            let t = c
                .select(
                    &format!(
                        "SELECT health, recommended_action, reason FROM flashback_health() \
                         WHERE tracking_id = {}::bigint",
                        fx.tracking_id
                    ),
                    None,
                    &[],
                )
                .unwrap();
            let row = t.first();
            (
                row.get_by_name::<String, _>("health").unwrap().unwrap(),
                row.get_by_name::<String, _>("recommended_action")
                    .unwrap()
                    .unwrap(),
                row.get_by_name::<String, _>("reason").unwrap().unwrap(),
            )
        });
        assert_eq!(
            health, "maintenance_required",
            "a capturing-only lifecycle must report the exact 'maintenance_required' health code"
        );
        assert_eq!(
            action, "wait_for_external_snapshot_publish",
            "a capturing (not building) lifecycle must report the external-publish action, \
             never the boundary-resolution action -- its boundary is already resolved"
        );
        assert_eq!(
            reason, "external snapshot copy/publish verification is in progress",
            "a capturing lifecycle's reason must never describe its already-resolved boundary as pending"
        );

        // doctor() must never report 'ok' either. A freshly-created,
        // normally-progressing capturing lifecycle (well under the 5-minute
        // stale-after bound) is now classified 'warning', not the generic
        // 'error' every other fault uses -- operator_diagnosis.sql's own
        // pending/stale split.
        let doctor_status = Spi::get_one::<String>(
            "SELECT status FROM flashback_doctor() WHERE check_name = 'tracked_lifecycle_health'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            doctor_status, "warning",
            "a young, normally-progressing capturing lifecycle must be doctor 'warning', \
             not the generic 'error' a genuine fault would report"
        );
        assert_ne!(
            doctor_status, "ok",
            "must never report 'ok' before activation"
        );
    }

    /// Step 9 Phase 2: flashback_protect_prepare_replica_identity +
    /// flashback_protect_external_copy (sql/functions/protect_online.sql)
    /// are the public SQL entrypoints that wrap this same real M1-M7 marker
    /// transaction for a brand-new, parentless lifecycle, additionally
    /// capturing/setting REPLICA IDENTITY FULL -- a step run_marker_
    /// transaction itself has never needed for its existing callers
    /// (re-anchor always operates on an already-FULL-identity table, set at
    /// the original heap_v1 track time).
    ///
    /// These are deliberately TWO separate committed transactions, not one:
    /// an earlier version of this test discovered a real deadlock doing the
    /// ALTER inside the same transaction as the marker transaction --
    /// ALTER TABLE ... REPLICA IDENTITY takes AccessExclusiveLock, which is
    /// still held (PostgreSQL never downgrades a lock mid-transaction)
    /// while the marker transaction's M6 waits for the background copier to
    /// open its own read cursor (needs only AccessShareLock, blocked behind
    /// the still-held AccessExclusiveLock, which can only be released once
    /// the marker transaction commits -- which never happens until the
    /// copier signals). See flashback_protect_prepare_replica_identity's
    /// own header comment for the full correctness argument for why
    /// splitting the ALTER into its own, earlier, committed transaction is
    /// still race-free.
    ///
    /// This proves that pair of entrypoints, not run_marker_transaction
    /// directly (already proven above and by online_boundary_bind.sql): the
    /// table starts at the default identity ('d'), both calls run end to
    /// end through the real production entrypoints (SQL wrapper -> owner-
    /// only Rust function -> real marker transaction -> real background
    /// copier), and afterward the table's replica identity is FULL,
    /// tracked_tables.replica_identity_was durably records the original
    /// 'd', and the boundary is bound -- all while a second, concurrent
    /// session was blocked from writing until each transaction committed,
    /// so no window ever existed where a write to this table could have
    /// been WAL-logged with less than a full old-row image once this
    /// generation could claim it.
    #[pg_test]
    fn test_protect_external_copy_sets_replica_identity_full() {
        let db_oid = unsafe { pg_sys::MyDatabaseId };
        let stream_id = shared_stream_id(db_oid);
        let table_name = "it_coord_protect_copy";

        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "DO $do$
             DECLARE
                 v_rel_oid oid;
             BEGIN
                 CREATE TABLE IF NOT EXISTS public.{table_name} (id int primary key, note text);
                 v_rel_oid := 'public.{table_name}'::regclass::oid;
                 INSERT INTO flashback.tracked_tables (
                     rel_oid, schema_name, table_name, base_snapshot_table,
                     recovery_profile, is_active, replica_identity_was, protection_state
                 )
                 VALUES (v_rel_oid, 'public', '{table_name}', NULL, 'local_delta', true, 'd', 'starting');
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

        let fx = InitialFixture {
            tracking_id,
            rel_oid,
            stream_id,
        };
        let nonce = 700900;

        // Pre-condition: the table starts at the ordinary default identity,
        // never FULL, before flashback_protect_external_copy runs.
        let relident_before = Spi::get_one::<String>(&format!(
            "SELECT relreplident::text FROM pg_class WHERE oid = {}::oid",
            fx.rel_oid.to_u32()
        ))
        .unwrap()
        .unwrap();
        assert_eq!(relident_before, "d");

        // Reservation + operation_begin combined into one committed session
        // (matching the existing coordinator tests' 3-launch shape: setup,
        // reserve, marker/copy -- rather than a 4th sequential dynamic
        // worker launch for a separate operation_begin call).
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "DO $do$
             DECLARE
                 v_gen bigint;
                 v_snap bigint;
             BEGIN
                 SELECT generation_id, snapshot_id
                   INTO v_gen, v_snap
                 FROM public.flashback_internal_reserve_online_generation(
                     p_tracking_id => {}::bigint, p_rel_oid => {}::oid, p_stream_id => {stream_id}::bigint,
                     p_generation_no => 1::bigint, p_parent_generation_id => NULL,
                     p_storage_backend => 'external_zstd', p_operation_nonce => {nonce}::bigint);
                 PERFORM public.flashback_operation_begin(
                     p_command => 'protect', p_table => 'public.{table_name}',
                     p_tracking_id => {}::bigint, p_generation_id => v_gen,
                     p_details => jsonb_build_object(
                         'snapshot_id', v_snap, 'rel_oid', {}::bigint,
                         'operation_nonce', {nonce}::bigint, 'storage_backend', 'external_zstd'));
             END;
             $do$;",
            fx.tracking_id,
            fx.rel_oid.to_u32(),
            fx.tracking_id,
            fx.rel_oid.to_u32(),
        ));
        let (generation_id, snapshot_id) = Spi::get_two::<i64, i64>(&format!(
            "SELECT generation_id, boundary_snapshot_id FROM flashback.coverage_generations \
             WHERE tracking_id = {}::bigint AND generation_no = 1",
            fx.tracking_id
        ))
        .unwrap();
        let generation_id = generation_id.unwrap();
        let snapshot_id = snapshot_id.unwrap();
        let operation_id = Spi::get_one::<i64>(&format!(
            "SELECT operation_id FROM flashback.operations \
             WHERE command = 'protect' AND tracking_id = {}::bigint \
             ORDER BY operation_id DESC LIMIT 1",
            fx.tracking_id
        ))
        .unwrap()
        .unwrap();

        // Its own, separate committed transaction -- see flashback_protect_
        // prepare_replica_identity's header comment in protect_online.sql
        // for exactly why this cannot share a transaction with the marker
        // transaction below (a real ALTER TABLE ... REPLICA IDENTITY
        // AccessExclusiveLock vs. the copier's own AccessShareLock deadlock,
        // found empirically while writing this very test).
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "SELECT public.flashback_protect_prepare_replica_identity({operation_id}::bigint)"
        ));
        crate::storage::external_zstd_handoff::test_support::run_sql_committed(&format!(
            "SELECT public.flashback_protect_external_copy({operation_id}::bigint)"
        ));

        let relident_after = Spi::get_one::<String>(&format!(
            "SELECT relreplident::text FROM pg_class WHERE oid = {}::oid",
            fx.rel_oid.to_u32()
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            relident_after, "f",
            "flashback_protect_external_copy must set REPLICA IDENTITY FULL for a brand-new parentless lifecycle"
        );

        let replica_identity_was = Spi::get_one::<String>(&format!(
            "SELECT replica_identity_was::text FROM flashback.tracked_tables WHERE tracking_id = {}::bigint",
            fx.tracking_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            replica_identity_was, "d",
            "the original (pre-change) replica identity must be durably recorded for unprotect to restore later"
        );

        let (state, boundary_xid, _marker, payload_state) =
            generation_snapshot(fx.tracking_id, generation_id, snapshot_id);
        assert_eq!(
            state, "building",
            "flashback_protect_external_copy alone (no WAL consumption yet) must leave the generation building, \
             not capturing -- that transition belongs to wal_promote_core.sql, not this entrypoint"
        );
        assert!(
            boundary_xid.is_some(),
            "the marker transaction must have bound a real boundary_xid"
        );
        assert_eq!(payload_state, "creating");

        let operation_state = Spi::get_one::<String>(&format!(
            "SELECT state FROM flashback.operation_current_state WHERE operation_id = {operation_id}::bigint"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            operation_state, "started",
            "flashback_protect_external_copy does not itself append any operation-journal event -- \
             it leaves the operation 'started' for flashback_protect_finalize to later move to 'activated'"
        );
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

use pgrx::pg_sys;
use pgrx::prelude::*;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

static RESTORE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub fn set_restore_in_progress(val: bool) {
    RESTORE_IN_PROGRESS.store(val, Ordering::SeqCst);
}

pub fn is_restore_in_progress() -> bool {
    RESTORE_IN_PROGRESS.load(Ordering::SeqCst)
}

// Backend-local (process-local static; each backend is its own OS process, so
// this can never be shared with or forged by another session) audited-recover
// execution context. NOT a GUC: a Userset string GUC of the same name could be
// set directly by any role holding EXECUTE on flashback_recover_execute via
// plain `SET`/`set_config()`, letting them dress up an arbitrary
// flashback_restore_lsn call as though it were the audited output of some
// other operation_id. Only flashback_recover_execute's owner-run SECURITY
// DEFINER body may set or clear this; every other backend/session has its own
// independent copy that starts and stays unset (i64::MIN sentinel) unless
// this backend itself sets it.
const NO_AUDITED_OPERATION: i64 = i64::MIN;
static AUDITED_RECOVER_OPERATION_ID: AtomicI64 = AtomicI64::new(NO_AUDITED_OPERATION);

fn set_audited_recover_operation(operation_id: i64) {
    AUDITED_RECOVER_OPERATION_ID.store(operation_id, Ordering::SeqCst);
}

fn clear_audited_recover_operation() {
    AUDITED_RECOVER_OPERATION_ID.store(NO_AUDITED_OPERATION, Ordering::SeqCst);
}

pub fn audited_recover_operation() -> Option<i64> {
    match AUDITED_RECOVER_OPERATION_ID.load(Ordering::SeqCst) {
        NO_AUDITED_OPERATION => None,
        op => Some(op),
    }
}

/// Permanent (whole-backend-lifetime) transaction-end callback: unconditionally
/// clears the audited-recover context on every commit/abort, independent of
/// whether flashback_recover_execute's own plpgsql exception handling ran.
/// This is the actual leak-proof guarantee -- a bug, a statement_timeout, a
/// client-initiated cancel, or connection reuse for an unrelated later
/// transaction in the same backend can never observe a stale operation_id.
#[pg_guard]
unsafe extern "C-unwind" fn audited_recover_context_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    use pg_sys::XactEvent::*;
    if matches!(
        event,
        XACT_EVENT_COMMIT
            | XACT_EVENT_ABORT
            | XACT_EVENT_PARALLEL_COMMIT
            | XACT_EVENT_PARALLEL_ABORT
    ) {
        clear_audited_recover_operation();
    }
}

pub fn install_audited_recover_context_xact_callback() {
    unsafe {
        pg_sys::RegisterXactCallback(
            Some(audited_recover_context_xact_callback),
            std::ptr::null_mut(),
        );
    }
}

#[pg_extern]
fn flashback_internal_set_audited_recover_context(operation_id: i64) -> bool {
    // Only the extension-owner SECURITY DEFINER call chain (flashback_recover_execute)
    // may set this; delegating it would let an operator dress up an arbitrary
    // flashback_restore_lsn call as though it were audited for a different operation.
    let is_su = unsafe { pgrx::pg_sys::superuser() };
    if !is_su {
        pgrx::error!(
            "flashback_internal_set_audited_recover_context is an internal owner-only function"
        );
    }
    if operation_id < 0 {
        pgrx::error!(
            "flashback_internal_set_audited_recover_context: operation_id must be non-negative"
        );
    }
    set_audited_recover_operation(operation_id);
    true
}

#[pg_extern]
fn flashback_internal_clear_audited_recover_context() -> bool {
    let is_su = unsafe { pgrx::pg_sys::superuser() };
    if !is_su {
        pgrx::error!(
            "flashback_internal_clear_audited_recover_context is an internal owner-only function"
        );
    }
    clear_audited_recover_operation();
    true
}

/// Internal owner-only, same as set/clear above -- NOT "safe for any caller"
/// despite being read-only: only flashback_restore_lsn's own owner-run body
/// consumes this to decide whether/how a restore is audited, and no other
/// role should be able to probe it even for read access. Deny-by-default via
/// rbac_grants.sql's blanket revoke (no allowlist entry for this function)
/// plus this Rust-level superuser() check are both required, not either/or.
#[pg_extern]
fn flashback_internal_get_audited_recover_context() -> Option<i64> {
    let is_su = unsafe { pgrx::pg_sys::superuser() };
    if !is_su {
        pgrx::error!(
            "flashback_internal_get_audited_recover_context is an internal owner-only function"
        );
    }
    audited_recover_operation()
}

#[pg_extern]
fn flashback_set_restore_in_progress(val: bool) -> bool {
    // This flag suppresses capture hooks in the current backend. Delegating it
    // would let an operator disguise arbitrary DDL as an internal restore, so
    // only the extension-owner SECURITY DEFINER call chain may toggle it.
    let is_su = unsafe { pgrx::pg_sys::superuser() };
    if !is_su {
        pgrx::error!("flashback_set_restore_in_progress is an internal superuser-only function");
    }
    set_restore_in_progress(val);
    true
}

/// Returns true if the CURRENT backend is running a restore.
/// Uses only the process-local AtomicBool — no SPI, no pg_locks query.
/// Other sessions should NOT have their triggers suppressed.
/// The advisory lock (acquired in flashback_restore) is exclusively for
/// serialising concurrent restores of the same table.
#[pg_extern]
fn flashback_is_restore_in_progress(_rel_oid: Option<pgrx::pg_sys::Oid>) -> bool {
    is_restore_in_progress()
}

use pgrx::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};

static RESTORE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub fn set_restore_in_progress(val: bool) {
    RESTORE_IN_PROGRESS.store(val, Ordering::SeqCst);
}

pub fn is_restore_in_progress() -> bool {
    RESTORE_IN_PROGRESS.load(Ordering::SeqCst)
}

#[pg_extern]
fn flashback_set_restore_in_progress(val: bool) -> bool {
    // Only superusers / flashback_admin should toggle the restore flag
    if unsafe { !pgrx::pg_sys::superuser() } {
        pgrx::error!("flashback_set_restore_in_progress requires superuser");
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

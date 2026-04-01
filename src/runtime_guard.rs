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
    set_restore_in_progress(val);
    true
}

/// Returns true if ANY backend (including the current one) is running a restore.
/// Local AtomicBool = fast path for the restore backend itself.
/// Advisory lock probe = detects restore in other backends.
#[pg_extern]
fn flashback_is_restore_in_progress() -> bool {
    // Fast local check first
    if is_restore_in_progress() {
        return true;
    }
    // Check if another backend holds the restore advisory lock (key 3589442679)
    match Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 3589442679 AND granted)"
    ) {
        Ok(Some(val)) => val,
        _ => false,
    }
}

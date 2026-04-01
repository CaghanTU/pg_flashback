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

#[pg_extern]
fn flashback_is_restore_in_progress() -> bool {
    is_restore_in_progress()
}

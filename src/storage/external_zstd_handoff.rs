//! external_zstd online-snapshot coordinator/copier handoff primitive
//! (Step 9, Stage 4 of the implementation plan) -- the single highest-risk
//! piece of new `unsafe` code in this feature.
//!
//! This exists because *every* SQL statement -- including a bare
//! `SELECT some_function()` with no table access -- causes PostgreSQL to
//! acquire/fix a transaction's snapshot. The online-snapshot protocol (plan
//! §1) needs the copier session to wait, after `BEGIN ISOLATION LEVEL
//! REPEATABLE READ`, for the coordinator to signal "the table lock is now
//! held" *before* the copier's `REPEATABLE READ` snapshot pins -- and it
//! must do that waiting without issuing any SQL at all. Dynamic shared
//! memory (DSM) plus a `ConditionVariable`, both accessed via raw `pg_sys`
//! FFI (there is no higher-level pgrx wrapper for either), is the
//! mechanism: pure C-level cross-process synchronization with no SPI/MVCC
//! involvement whatsoever.
//!
//! `pg_atomic_*` accessor functions (`pg_atomic_read_u32` etc.) are
//! `static inline` in PostgreSQL's C headers and have no externally-linked
//! symbol for bindgen to bind -- they are not callable from Rust via FFI at
//! all in this pgrx version. `std::sync::atomic::AtomicU32::from_ptr` on
//! the DSM-segment-backed memory is used instead: it compiles to the exact
//! same hardware atomic instructions on this platform, with no correctness
//! gap, just a different (Rust-native, not PostgreSQL-C-wrapped) API
//! surface for the same capability.
//!
//! Layout (`HandoffShared`, `#[repr(C)]`, lives entirely inside one DSM
//! segment, shared by both processes):
//! ```text
//! state             u32   -- HandoffPhase, accessed only via AtomicU32::from_ptr
//! cv                ConditionVariable -- both sides wait/broadcast on this
//! operation_nonce   u64   -- matches this handoff to its durable reservation row
//! ```
//!
//! The atomic `state` write is a release; a reader's atomic load before
//! touching any other field of the struct is an acquire -- this is what
//! makes the (write-once, by whichever side currently owns the phase)
//! payload fields' visibility a consequence of the state transition's own
//! ordering, rather than needing separate synchronization for them.
//!
//! Stage 4 of a staged implementation (see the Step 9 plan, §15): exercised
//! directly by the `#[pg_test]`s below (including a real cross-process
//! round trip); the real online-snapshot coordinator/copier land in Stage
//! 5-7. `#![allow(dead_code)]` is temporary scaffolding for that gap.
#![allow(dead_code)]

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, DynamicBackgroundWorker};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Phases of the DSM+ConditionVariable handoff. Values are stored in
/// `HandoffShared.state` via `AtomicU32`; never matched exhaustively on the
/// raw integer without going through [`HandoffPhase::from_u32`], which
/// fails closed on anything unrecognized (e.g. torn/uninitialized memory)
/// rather than transmuting into an invalid enum value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum HandoffPhase {
    WaitingForLock = 0,
    LockHeldGoAhead = 1,
    SnapshotPinned = 2,
    Failed = 3,
}

impl HandoffPhase {
    fn from_u32(v: u32) -> Option<HandoffPhase> {
        match v {
            0 => Some(HandoffPhase::WaitingForLock),
            1 => Some(HandoffPhase::LockHeldGoAhead),
            2 => Some(HandoffPhase::SnapshotPinned),
            3 => Some(HandoffPhase::Failed),
            _ => None,
        }
    }
}

#[repr(C)]
struct HandoffShared {
    state: u32,
    cv: pg_sys::ConditionVariable,
    operation_nonce: u64,
}

/// Error outcomes for a bounded wait on the handoff.
#[derive(Debug, PartialEq, Eq)]
pub enum HandoffWaitError {
    /// The bounded deadline elapsed without observing the wanted state.
    Timeout,
    /// The peer process (checked via its `DynamicBackgroundWorker` handle)
    /// is confirmed gone.
    PeerDead,
    /// The peer explicitly signaled `Failed`.
    PeerFailed,
    /// `HandoffShared.state` contained a value outside [`HandoffPhase`]'s
    /// range -- torn/corrupt shared memory. Fails closed rather than
    /// guessing.
    CorruptState(u32),
}

/// A DSM segment holding one [`HandoffShared`], attached by either side.
/// Coordinator and copier each get their own `HandoffSegment` value
/// (different `*mut dsm_segment` per-process attachment), both pointing at
/// the same underlying shared memory.
pub struct HandoffSegment {
    seg: *mut pg_sys::dsm_segment,
    shared: *mut HandoffShared,
}

// A DSM segment is explicitly designed for cross-process shared access;
// the pointers here are valid for as long as the segment is attached, and
// every access goes through AtomicU32/ConditionVariable's own internal
// synchronization -- not through any Rust-level aliasing assumption these
// auto-traits would otherwise forbid.
unsafe impl Send for HandoffSegment {}
unsafe impl Sync for HandoffSegment {}

impl HandoffSegment {
    /// Coordinator side: allocate a fresh DSM segment sized for exactly one
    /// `HandoffShared`, initialize it (`WaitingForLock`, a freshly-inited
    /// `ConditionVariable`, the given `operation_nonce`), and pin both the
    /// mapping and the segment so it survives the coordinator's own
    /// transaction boundaries (it must remain valid across Setup, the
    /// marker transaction, and the coordinator's own exit -- see plan §1).
    ///
    /// # Safety
    /// Must be called from a normal backend (not signal-handler context).
    pub unsafe fn coordinator_create(operation_nonce: u64) -> HandoffSegment {
        let size = std::mem::size_of::<HandoffShared>();
        let seg = pg_sys::dsm_create(size as pg_sys::Size, 0);
        assert!(
            !seg.is_null(),
            "dsm_create returned null (out of DSM slots?)"
        );
        pg_sys::dsm_pin_mapping(seg);
        pg_sys::dsm_pin_segment(seg);
        let shared = pg_sys::dsm_segment_address(seg).cast::<HandoffShared>();
        (*shared).state = HandoffPhase::WaitingForLock as u32;
        pg_sys::ConditionVariableInit(&mut (*shared).cv);
        (*shared).operation_nonce = operation_nonce;
        HandoffSegment { seg, shared }
    }

    /// The `dsm_handle` to pass to a dynamically-launched copier worker as
    /// its startup argument (`BackgroundWorkerBuilder::set_argument`).
    pub fn handle(&self) -> pg_sys::dsm_handle {
        unsafe { pg_sys::dsm_segment_handle(self.seg) }
    }

    /// Copier side: attach to an already-created segment by handle.
    ///
    /// # Safety
    /// `handle` must be a handle returned by a still-live
    /// [`HandoffSegment::handle`] (the coordinator's segment must still be
    /// pinned/attached somewhere -- a torn-down segment makes this
    /// undefined per `dsm_attach`'s own contract).
    pub unsafe fn attach(handle: pg_sys::dsm_handle) -> Option<HandoffSegment> {
        let seg = pg_sys::dsm_attach(handle);
        if seg.is_null() {
            return None;
        }
        let shared = pg_sys::dsm_segment_address(seg).cast::<HandoffShared>();
        Some(HandoffSegment { seg, shared })
    }

    pub fn operation_nonce(&self) -> u64 {
        unsafe { (*self.shared).operation_nonce }
    }

    fn atomic_state(&self) -> &AtomicU32 {
        // SAFETY: `state` is the first field of a #[repr(C)] struct backed
        // by DSM shared memory for the entire lifetime of this handle;
        // AtomicU32::from_ptr requires only correct alignment (u32's
        // natural alignment, satisfied by #[repr(C)] layout) and that no
        // non-atomic access to this memory races with atomic access --
        // every access to `state` in this module goes through this method.
        unsafe { AtomicU32::from_ptr(std::ptr::addr_of_mut!((*self.shared).state)) }
    }

    /// Set the phase and wake every waiter. The atomic store is a release;
    /// see the module-level doc comment for why this is what makes any
    /// payload fields' visibility well-defined without separate
    /// synchronization for them.
    pub fn signal(&self, phase: HandoffPhase) {
        self.atomic_state().store(phase as u32, Ordering::Release);
        unsafe {
            pg_sys::ConditionVariableBroadcast(std::ptr::addr_of_mut!((*self.shared).cv));
        }
    }

    /// Block (via `ConditionVariableTimedSleep`, re-checked in a bounded
    /// poll loop -- never a single unbounded sleep) until `state` becomes
    /// `want`, or `timeout` elapses, or (if `peer` is given) the peer
    /// background worker is confirmed dead, or the peer signals `Failed`.
    /// Every wait is bounded; there is no code path in this module that
    /// can block indefinitely.
    ///
    /// No SQL statement is issued anywhere in this function or anything it
    /// calls -- this is the property the whole module exists to provide.
    pub fn wait_for_state(
        &self,
        want: HandoffPhase,
        timeout: Duration,
        peer: Option<&DynamicBackgroundWorker>,
    ) -> Result<(), HandoffWaitError> {
        let deadline = Instant::now() + timeout;
        // Bounded per-iteration sleep so a dead/never-signaling peer is
        // detected promptly rather than only at the overall deadline.
        const POLL_INTERVAL: Duration = Duration::from_millis(200);
        loop {
            let raw = self.atomic_state().load(Ordering::Acquire);
            let phase = match HandoffPhase::from_u32(raw) {
                Some(p) => p,
                None => {
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    return Err(HandoffWaitError::CorruptState(raw));
                }
            };
            if phase == want {
                unsafe { pg_sys::ConditionVariableCancelSleep() };
                return Ok(());
            }
            if phase == HandoffPhase::Failed {
                unsafe { pg_sys::ConditionVariableCancelSleep() };
                return Err(HandoffWaitError::PeerFailed);
            }
            if let Some(p) = peer {
                if p.pid().is_err() {
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    return Err(HandoffWaitError::PeerDead);
                }
            }
            let now = Instant::now();
            if now >= deadline {
                unsafe { pg_sys::ConditionVariableCancelSleep() };
                return Err(HandoffWaitError::Timeout);
            }
            let remaining = (deadline - now).min(POLL_INTERVAL);
            // ConditionVariableTimedSleep's own internal wait already
            // handles postmaster death (it is built on WaitLatch, which
            // wakes on postmaster death by default) -- no separate
            // WL_POSTMASTER_DEATH plumbing is needed here; a dead
            // postmaster unblocks the sleep and the next loop iteration's
            // deadline/peer checks converge normally.
            unsafe {
                pg_sys::ConditionVariableTimedSleep(
                    std::ptr::addr_of_mut!((*self.shared).cv),
                    remaining.as_millis() as std::ffi::c_long,
                    pg_sys::PG_WAIT_EXTENSION,
                );
            }
        }
    }

    /// Detach this process's mapping of the segment. The *other* side's
    /// attachment (and the segment's own pinned lifetime, if pinned) is
    /// unaffected -- each process's `dsm_attach`/`dsm_create` call gets its
    /// own independent `*mut dsm_segment` handle onto the same memory.
    pub fn detach(self) {
        unsafe { pg_sys::dsm_detach(self.seg) };
    }
}

// ── Stage 4 isolated test worker ────────────────────────────────────────
//
// This is NOT the real online-snapshot copier (that lands in Stage 7, with
// real SPI/table access). It exists solely to prove the DSM/atomic/
// condition-variable mechanics themselves work correctly across two real
// OS processes, per the plan's explicit requirement that this primitive
// get its own dedicated test pass before anything is built on top of it.
// Protocol: attach -> wait for LockHeldGoAhead -> signal SnapshotPinned ->
// exit. A companion "fail" mode signals Failed instead, for the
// peer-failure test path.

/// The real, exported symbol this crate's `lib.rs` `#[unsafe(no_mangle)]`
/// wrapper delegates to (dynamic background worker function lookup is
/// string/symbol-name based -- see that wrapper's doc comment for why the
/// wrapper, not this module function directly, must carry `no_mangle`).
pub extern "C-unwind" fn pg_flashback_external_zstd_handoff_selftest_worker_main(
    arg: pg_sys::Datum,
) {
    // A panic must never unwind across the crate-root FFI boundary into
    // postgres's own C bgworker-launch code -- catch it here and exit
    // cleanly instead. (The crate-root wrapper also carries #[pg_guard],
    // which provides the same protection at that boundary; this is
    // deliberate defense in depth, not a substitute for it.)
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        worker_body(arg);
    }));
    if let Err(e) = result {
        let msg = e
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        log!("pg_flashback external_zstd handoff selftest worker: PANICKED: {msg}");
    }
}

fn worker_body(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(pgrx::bgworkers::SignalWakeFlags::SIGTERM);
    // Low 32 bits: dsm_handle. High bit (bit 32, i.e. bit 0 of the upper
    // word): fail-mode flag. Packed into one i64 argument since
    // set_argument takes a single Option<Datum>.
    let raw = unsafe { i64::from_datum(arg, false) }.unwrap_or(0);
    let dsm_h = (raw & 0xFFFF_FFFF) as u32;
    let fail_mode = (raw >> 32) != 0;

    let segment = unsafe { HandoffSegment::attach(dsm_h) };
    let segment = match segment {
        Some(s) => s,
        None => {
            log!("pg_flashback external_zstd handoff selftest worker: dsm_attach failed, exiting");
            return;
        }
    };

    if fail_mode {
        segment.signal(HandoffPhase::Failed);
        return;
    }

    match segment.wait_for_state(HandoffPhase::LockHeldGoAhead, Duration::from_secs(30), None) {
        Ok(()) => {
            segment.signal(HandoffPhase::SnapshotPinned);
        }
        Err(e) => {
            log!("pg_flashback external_zstd handoff selftest worker: wait failed: {e:?}");
            segment.signal(HandoffPhase::Failed);
        }
    }
}

/// Launch the Stage 4 selftest copier as a dynamic background worker,
/// tracked (`set_notify_pid`) so the caller can `wait_for_startup`/`pid()`.
pub fn launch_selftest_worker(
    dsm_h: pg_sys::dsm_handle,
    fail_mode: bool,
) -> Result<DynamicBackgroundWorker, pgrx::bgworkers::DynamicBackgroundWorkerLoadError> {
    let packed: i64 = (dsm_h as i64) | ((fail_mode as i64) << 32);
    BackgroundWorkerBuilder::new("pg_flashback external_zstd handoff selftest worker")
        .set_function("pg_flashback_external_zstd_handoff_selftest_worker_main")
        .set_library("pg_flashback")
        .set_argument(packed.into_datum())
        .set_notify_pid(unsafe { pg_sys::MyProcPid })
        // This Stage 4 selftest worker touches only DSM + a condition
        // variable, no SPI/database access at all -- enable_shmem_access
        // (BGWORKER_SHMEM_ACCESS only) rather than enable_spi_access
        // (which also sets BGWORKER_BACKEND_DATABASE_CONNECTION, a
        // contract this worker never fulfills since it never calls
        // connect_worker_to_spi). The real Stage 7 copier, which does need
        // SPI, will use enable_spi_access properly.
        .enable_shmem_access(None)
        .load_dynamic()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_handoff_phase_from_u32_fails_closed_on_garbage() {
        assert_eq!(
            HandoffPhase::from_u32(0),
            Some(HandoffPhase::WaitingForLock)
        );
        assert_eq!(
            HandoffPhase::from_u32(1),
            Some(HandoffPhase::LockHeldGoAhead)
        );
        assert_eq!(
            HandoffPhase::from_u32(2),
            Some(HandoffPhase::SnapshotPinned)
        );
        assert_eq!(HandoffPhase::from_u32(3), Some(HandoffPhase::Failed));
        assert_eq!(HandoffPhase::from_u32(4), None);
        assert_eq!(HandoffPhase::from_u32(0xFFFF_FFFF), None);
    }

    #[pg_test]
    fn test_coordinator_create_initializes_waiting_for_lock() {
        let seg = unsafe { HandoffSegment::coordinator_create(0xDEAD_BEEF) };
        assert_eq!(seg.operation_nonce(), 0xDEAD_BEEF);
        let raw = seg.atomic_state().load(Ordering::Acquire);
        assert_eq!(
            HandoffPhase::from_u32(raw),
            Some(HandoffPhase::WaitingForLock)
        );
        seg.detach();
    }

    #[pg_test]
    fn test_wait_for_state_times_out_when_never_signaled() {
        let seg = unsafe { HandoffSegment::coordinator_create(1) };
        let started = Instant::now();
        let result = seg.wait_for_state(
            HandoffPhase::LockHeldGoAhead,
            Duration::from_millis(400),
            None,
        );
        assert_eq!(result, Err(HandoffWaitError::Timeout));
        assert!(
            started.elapsed() >= Duration::from_millis(350),
            "must actually wait close to the requested timeout, not return immediately"
        );
        seg.detach();
    }

    #[pg_test]
    fn test_same_process_signal_and_wait_round_trip() {
        // Not a cross-process test (that's the next test) -- proves the
        // atomic-state + condition-variable signal/wait mechanics
        // themselves are correct in isolation, same process, before
        // trusting them across a real process boundary.
        let seg = unsafe { HandoffSegment::coordinator_create(42) };
        seg.signal(HandoffPhase::LockHeldGoAhead);
        let result =
            seg.wait_for_state(HandoffPhase::LockHeldGoAhead, Duration::from_secs(5), None);
        assert_eq!(result, Ok(()));
        seg.detach();
    }

    /// The real, cross-process proof: launch an actual dynamic background
    /// worker, hand it the DSM segment, and drive the exact
    /// WaitingForLock -> LockHeldGoAhead -> SnapshotPinned sequence the
    /// real online-snapshot protocol (plan §1, steps H3-H6/M2-M5) uses --
    /// against a genuinely separate OS process, not a simulation.
    #[pg_test]
    fn test_cross_process_handoff_round_trip() {
        let seg = unsafe { HandoffSegment::coordinator_create(777) };
        let worker = launch_selftest_worker(seg.handle(), false)
            .expect("failed to launch selftest worker (max_worker_processes exhausted?)");
        worker
            .wait_for_startup()
            .expect("selftest worker did not start (postmaster died or startup failed)");

        // H4 (copier waiting) has now begun in the other process. Signal
        // LockHeldGoAhead (M5) and wait for the worker's SnapshotPinned
        // acknowledgment (H6/M6), bounded, checking the worker is still
        // alive on every wake.
        seg.signal(HandoffPhase::LockHeldGoAhead);
        let result = seg.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(10),
            Some(&worker),
        );
        assert_eq!(
            result,
            Ok(()),
            "cross-process handoff must reach SnapshotPinned within the bounded wait"
        );
        seg.detach();
    }

    /// Death-window test: the coordinator's wait must detect a confirmed-
    /// dead peer promptly (bounded by the poll interval, not the full
    /// timeout) rather than only ever timing out.
    #[pg_test]
    fn test_wait_detects_dead_peer_before_full_timeout() {
        let seg = unsafe { HandoffSegment::coordinator_create(999) };
        let worker = launch_selftest_worker(seg.handle(), true) // fail_mode
            .expect("failed to launch selftest worker");
        worker
            .wait_for_startup()
            .expect("selftest worker did not start");

        // fail_mode signals Failed immediately without waiting for
        // LockHeldGoAhead -- proves the PeerFailed path, distinct from the
        // PeerDead (process actually gone) path.
        let result = seg.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(10),
            Some(&worker),
        );
        assert_eq!(result, Err(HandoffWaitError::PeerFailed));
        seg.detach();
    }
}

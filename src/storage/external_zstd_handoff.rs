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
//! ## Safety assumptions, made explicit
//!
//! This module is the single highest-risk piece of new `unsafe` code in
//! Step 9. Four distinct assumptions are relied on; each is stated here
//! precisely, and each has a corresponding test below (`ordering`/
//! `visibility`/`alignment` are properties a single test run on one
//! strongly-ordered ISA cannot *prove* in the general sense -- the repeated
//! stress test empirically exercises them; the argument for correctness is
//! this doc comment, not the test alone).
//!
//! **Alignment.** `AtomicU32::from_ptr`'s safety contract requires the
//! pointer to be valid for reads and writes and to have `u32`'s natural
//! (4-byte) alignment for its entire liveness. `shared` is always exactly
//! `dsm_segment_address(seg)` with no offset applied, and `state` is the
//! first field of `#[repr(C)] struct HandoffShared` (`repr(C)` guarantees
//! the first field starts at offset 0). `dsm_segment_address` returns a
//! pointer into memory obtained by `mmap`/`shmat`, which the platform
//! guarantees is at least page-aligned (4096 bytes on every target this
//! extension builds for) -- far in excess of the 4-byte requirement. Both
//! `coordinator_create` and `attach` assert this at runtime (not just
//! `debug_assert!`, since the check is O(1) and this is exactly the kind
//! of assumption that must fail loudly, not silently compile away in a
//! release build, if it is ever wrong).
//!
//! **Memory ordering.** Two distinct publication mechanisms are in play,
//! not one:
//! 1. *Steady-state signaling* (`signal`/`wait_for_state`, used for every
//!    phase transition after the segment exists): `Ordering::Release` on
//!    the store, `Ordering::Acquire` on the load. Standard
//!    release-acquire message passing -- any non-atomic write made by the
//!    signaling side strictly before its `store` is guaranteed visible to
//!    the waiting side strictly after its `load` observes that value.
//! 2. *Initial publication* (`operation_nonce`, `cv`'s own internal
//!    state): written once, non-atomically, inside `coordinator_create`,
//!    entirely *before* the segment's handle is ever handed to anything
//!    that could read it. The handle only becomes reachable by the copier
//!    through `BackgroundWorkerBuilder::set_argument` + `load_dynamic`,
//!    which crosses a real process-launch boundary (registration through
//!    shared memory the postmaster itself synchronizes, plus the fork
//!    that creates the worker process) -- an unconditionally stronger
//!    barrier than a single atomic release/acquire pair. This is a
//!    distinct argument from (1), not an instance of it, so it is called
//!    out separately rather than folded into "the atomic ordering handles
//!    it." To avoid leaning on that argument implicitly anywhere reads
//!    happen post-attach, `operation_nonce()` still performs an
//!    `Ordering::Acquire` load of `state` first and discards it purely as
//!    a synchronization fence -- every payload read in this module goes
//!    through an acquire, without exception, rather than two different
//!    reasoning paths depending on which field is being read.
//!
//! **Process death.** Three distinct death windows, three distinct
//! detections, none of which is "wait until the bounded timeout and hope":
//! coordinator dies before the copier's `dsm_attach` (handle invalid or
//! segment torn down -> `attach` returns `None`, tested directly below
//! without needing to actually kill a process); copier dies or exits
//! without ever reaching `Failed`/`SnapshotPinned` while the coordinator
//! waits (`wait_for_state`'s `peer.pid().is_err()` check, re-evaluated
//! every `POLL_INTERVAL`, tested below via a worker that exits immediately
//! with no signal at all -- distinct from the existing explicit-`Failed`
//! peer test, which exercises a *live* peer reporting its own failure, not
//! an actually-dead one); postmaster itself dies (`ConditionVariableTimedSleep`
//! is built on `WaitLatch`, which wakes on postmaster death unconditionally
//! -- no separate `WL_POSTMASTER_DEATH` plumbing is needed).
//!
//! **Cross-process visibility.** `AtomicU32::from_ptr` on DSM-segment
//! memory relies on x86_64/aarch64 hardware cache coherency operating at
//! the physical-memory/cache-line level, not the virtual-address or
//! process level -- two processes each `mmap`ing the same physical page
//! observe each other's atomic stores exactly as two threads in one
//! process would, because the CPU's coherency protocol has no notion of
//! "process" at all. This is the identical guarantee PostgreSQL's own
//! `pg_atomic_uint32` (used across postmaster/backends today) already
//! depends on; using `std::sync::atomic::AtomicU32` instead of the
//! `pg_atomic_*` C API changes only which language's atomic-intrinsics
//! syntax is used to emit the same hardware instructions, not the
//! underlying guarantee.
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
        assert_eq!(
            (shared as usize) % std::mem::align_of::<HandoffShared>(),
            0,
            "DSM segment address is not aligned for HandoffShared -- AtomicU32::from_ptr's \
             safety contract would be violated"
        );
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
        assert_eq!(
            (shared as usize) % std::mem::align_of::<HandoffShared>(),
            0,
            "DSM segment address is not aligned for HandoffShared -- AtomicU32::from_ptr's \
             safety contract would be violated"
        );
        Some(HandoffSegment { seg, shared })
    }

    /// `operation_nonce` is written once, non-atomically, before the
    /// segment is ever published (see the module doc's "Memory ordering"
    /// section) -- but every payload read in this module still goes
    /// through an explicit acquire first, rather than relying on that
    /// argument implicitly. The loaded value itself is discarded; only
    /// the acquire fence it establishes matters here.
    pub fn operation_nonce(&self) -> u64 {
        let _ = self.atomic_state().load(Ordering::Acquire);
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

/// Selftest worker behavior, packed into the high bits of the launch
/// argument alongside the `dsm_handle`. `Crash` exists specifically to
/// exercise the *actually dead peer* path (`HandoffWaitError::PeerDead`,
/// detected via `peer.pid().is_err()`) as distinct from `Fail`, which
/// exercises a *live* peer explicitly reporting `HandoffPhase::Failed`
/// (`HandoffWaitError::PeerFailed`) -- two different code paths in
/// `wait_for_state` that must not be conflated.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SelftestMode {
    Normal = 0,
    Fail = 1,
    /// Exit immediately without attaching or signaling anything, simulating
    /// a hard crash before the copier ever reaches the handoff protocol.
    Crash = 2,
}

impl SelftestMode {
    fn from_u8(v: u8) -> SelftestMode {
        match v {
            1 => SelftestMode::Fail,
            2 => SelftestMode::Crash,
            _ => SelftestMode::Normal,
        }
    }
}

fn worker_body(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(pgrx::bgworkers::SignalWakeFlags::SIGTERM);
    // Low 32 bits: dsm_handle. Bits 32-39: SelftestMode. Packed into one
    // i64 argument since set_argument takes a single Option<Datum>.
    let raw = unsafe { i64::from_datum(arg, false) }.unwrap_or(0);
    let dsm_h = (raw & 0xFFFF_FFFF) as u32;
    let mode = SelftestMode::from_u8(((raw >> 32) & 0xFF) as u8);

    if mode == SelftestMode::Crash {
        // Deliberately exit without attaching to the segment at all -- the
        // coordinator must detect this as a dead peer via its own
        // DynamicBackgroundWorker handle, never by any signal through the
        // DSM segment (there is none).
        return;
    }

    let segment = unsafe { HandoffSegment::attach(dsm_h) };
    let segment = match segment {
        Some(s) => s,
        None => {
            log!("pg_flashback external_zstd handoff selftest worker: dsm_attach failed, exiting");
            return;
        }
    };

    if mode == SelftestMode::Fail {
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
fn launch_selftest_worker_mode(
    dsm_h: pg_sys::dsm_handle,
    mode: SelftestMode,
) -> Result<DynamicBackgroundWorker, pgrx::bgworkers::DynamicBackgroundWorkerLoadError> {
    let packed: i64 = (dsm_h as i64) | ((mode as i64) << 32);
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
        let worker = launch_selftest_worker_mode(seg.handle(), SelftestMode::Normal)
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

    /// Empirical stress pass for the memory-ordering/cross-process-
    /// visibility argument in the module doc comment: a single run cannot
    /// *prove* an ordering guarantee, but a real cross-process round trip
    /// repeated many times, each with a fresh segment and a fresh process,
    /// is the practical bar this codebase already uses elsewhere for
    /// concurrency-sensitive primitives -- any latent ordering bug (e.g. a
    /// missing acquire/release pairing) is expected to surface as a flake
    /// under repetition, not just in theory.
    #[pg_test]
    fn test_cross_process_handoff_repeated_round_trips() {
        for i in 0..20u64 {
            let seg = unsafe { HandoffSegment::coordinator_create(1000 + i) };
            let worker = launch_selftest_worker_mode(seg.handle(), SelftestMode::Normal)
                .unwrap_or_else(|e| panic!("iteration {i}: failed to launch worker: {e:?}"));
            worker
                .wait_for_startup()
                .unwrap_or_else(|e| panic!("iteration {i}: worker did not start: {e:?}"));
            seg.signal(HandoffPhase::LockHeldGoAhead);
            let result = seg.wait_for_state(
                HandoffPhase::SnapshotPinned,
                Duration::from_secs(10),
                Some(&worker),
            );
            assert_eq!(result, Ok(()), "iteration {i} failed");
            seg.detach();
        }
    }

    /// Death-window test: the coordinator's wait must detect a confirmed-
    /// dead peer promptly (bounded by the poll interval, not the full
    /// timeout) rather than only ever timing out. This exercises a *live*
    /// peer explicitly reporting its own failure (`HandoffPhase::Failed`),
    /// which is the `PeerFailed` path -- see
    /// `test_wait_detects_dead_peer_via_process_exit` below for the
    /// distinct `PeerDead` path (a peer that is actually gone, never
    /// having signaled anything).
    #[pg_test]
    fn test_wait_detects_dead_peer_before_full_timeout() {
        let seg = unsafe { HandoffSegment::coordinator_create(999) };
        let worker = launch_selftest_worker_mode(seg.handle(), SelftestMode::Fail)
            .expect("failed to launch selftest worker");
        worker
            .wait_for_startup()
            .expect("selftest worker did not start");

        // Fail mode signals Failed immediately without waiting for
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

    /// The other death path: a peer that exits immediately, without ever
    /// attaching to the segment or signaling anything -- a hard crash
    /// simulation. `wait_for_state` must detect this via the peer's own
    /// `DynamicBackgroundWorker` handle (`pid().is_err()`), not by any
    /// signal through shared memory (there is none), and must not wait out
    /// the full bounded timeout to do so.
    #[pg_test]
    fn test_wait_detects_dead_peer_via_process_exit() {
        let seg = unsafe { HandoffSegment::coordinator_create(1234) };
        let worker = launch_selftest_worker_mode(seg.handle(), SelftestMode::Crash)
            .expect("failed to launch selftest worker");
        worker
            .wait_for_startup()
            .expect("selftest worker did not start");

        let started = Instant::now();
        let result = seg.wait_for_state(
            HandoffPhase::LockHeldGoAhead,
            Duration::from_secs(30),
            Some(&worker),
        );
        assert_eq!(result, Err(HandoffWaitError::PeerDead));
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "a genuinely dead peer must be detected well before the 30s bound, \
             not only by exhausting it"
        );
        seg.detach();
    }

    /// Coordinator-death-before-attach direction: `dsm_attach` against a
    /// handle that does not correspond to any live segment must fail
    /// cleanly (`None`), never panic or hang -- this is what lets the
    /// copier's own `worker_body` exit quietly instead of waiting on
    /// memory that was never valid.
    #[pg_test]
    fn test_attach_with_invalid_handle_returns_none() {
        let bogus: pg_sys::dsm_handle = 0xDEAD_0001;
        let result = unsafe { HandoffSegment::attach(bogus) };
        assert!(
            result.is_none(),
            "attaching a handle with no corresponding live segment must return None"
        );
    }

    /// Alignment assumption, made concrete: `coordinator_create` must hand
    /// back a `HandoffShared` pointer meeting `AtomicU32::from_ptr`'s
    /// alignment requirement. The runtime `assert_eq!` inside both
    /// `coordinator_create` and `attach` already enforces this
    /// unconditionally for every segment either constructor ever hands
    /// out (including every `attach` call the cross-process tests above
    /// make from the copier's own process -- a single backend cannot
    /// `dsm_attach` a segment it already holds a second time, so `attach`'s
    /// path is exercised there, not by a same-process double-attach here);
    /// this test additionally checks the `coordinator_create` path
    /// explicitly from the outside so the property has its own named,
    /// readable failure rather than only ever surfacing as an assertion
    /// panic deep inside segment creation.
    #[pg_test]
    fn test_segment_address_is_aligned_for_atomic_access() {
        let seg = unsafe { HandoffSegment::coordinator_create(55) };
        assert_eq!(
            (seg.shared as usize) % std::mem::align_of::<HandoffShared>(),
            0
        );
        seg.detach();
    }
}

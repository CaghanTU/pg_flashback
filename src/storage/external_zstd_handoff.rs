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
//! The production online-snapshot coordinator/copier uses this protocol;
//! the tests below additionally exercise it in isolation, including a real
//! cross-process round trip. A few fault-injection helpers remain test-only,
//! hence the temporary module-level dead-code allowance.
#![allow(dead_code)]

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, DynamicBackgroundWorker};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
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

/// Bound on the quoted, comma-separated column list carried in
/// `HandoffShared.column_list`. A fixed-size buffer, not a fully dynamic
/// variable-length payload (that is Stage 7's job, alongside the rest of
/// the real streaming copy) -- sufficient to prove the property Stage 6
/// completion actually needs: the copier's real SELECT list reflects
/// whatever column_contract the coordinator captured under the lock,
/// including columns added between reservation and lock. Exceeding it is
/// a hard, fail-closed error (`write_column_list`), never silent
/// truncation.
const COLUMN_LIST_CAP: usize = 8192;

/// Bound on the schema-qualified, quoted target relation name carried in
/// `HandoffShared.target_relation` (e.g. `"public"."my_table"`) -- same
/// write-before-publish contract as `column_list`.
const TARGET_RELATION_CAP: usize = 256;
const COLUMN_CONTRACT_CAP: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyIdentity {
    pub tracking_id: i64,
    pub generation_id: i64,
    pub snapshot_id: i64,
    pub rel_oid: u32,
}

#[repr(C)]
struct HandoffShared {
    state: u32,
    cv: pg_sys::ConditionVariable,
    operation_nonce: u64,
    persist_requested: u32,
    tracking_id: i64,
    generation_id: i64,
    snapshot_id: i64,
    rel_oid: u32,
    /// Written once, in full, by the coordinator strictly before the
    /// `LockHeldGoAhead` atomic store (see `signal`) -- its visibility to
    /// the copier is a consequence of that store's Release ordering and
    /// the copier's own Acquire load before ever reading this field,
    /// exactly like `operation_nonce` (module doc's "Memory ordering"
    /// section). Never mutated again for the lifetime of the segment.
    column_list_len: u32,
    column_list: [u8; COLUMN_LIST_CAP],
    target_relation_len: u32,
    target_relation: [u8; TARGET_RELATION_CAP],
    column_contract_len: u32,
    column_contract: [u8; COLUMN_CONTRACT_CAP],
    /// The reverse direction: written once by the copier, strictly before
    /// its `SnapshotPinned` atomic store, read by the coordinator only
    /// after observing `SnapshotPinned` -- the row count its real cursor
    /// fetch actually saw. Used to prove the copier's snapshot was pinned
    /// strictly before a witness row the coordinator inserts after
    /// observing `SnapshotPinned` (the row can never appear in this
    /// count, by construction: the count is finalized, by causality,
    /// before the insert could possibly have happened).
    fetched_row_count: u64,
    /// The reverse direction, alongside `fetched_row_count`: `pg_current_
    /// wal_insert_lsn()` as observed by the copier immediately after its
    /// cursor fetch pins the snapshot, strictly before its `SnapshotPinned`
    /// atomic store. WAL LSNs are a single, cluster-wide, strictly
    /// monotonically non-decreasing sequence -- comparing this value
    /// against the LSN `pg_logical_emit_message` (M7) returns gives
    /// externally observable, non-structural corroboration that the pin
    /// happened before the boundary message, independent of trusting this
    /// module's own call-order argument.
    pinned_wal_lsn: u64,
    /// Diagnostic only: written by whichever side signals `Failed`,
    /// strictly before that signal, describing why. Operationally useful
    /// (the coordinator can log a real reason instead of just "the peer
    /// failed") and not load-bearing for correctness -- a failure to write
    /// or read this never changes the `Failed`/`PeerFailed` outcome
    /// itself, only how legible the reason is afterward.
    error_message_len: u32,
    error_message: [u8; ERROR_MESSAGE_CAP],
}

const ERROR_MESSAGE_CAP: usize = 512;

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
        (*shared).persist_requested = 0;
        (*shared).tracking_id = 0;
        (*shared).generation_id = 0;
        (*shared).snapshot_id = 0;
        (*shared).rel_oid = 0;
        (*shared).column_list_len = 0;
        (*shared).target_relation_len = 0;
        (*shared).column_contract_len = 0;
        (*shared).fetched_row_count = 0;
        (*shared).pinned_wal_lsn = 0;
        (*shared).error_message_len = 0;
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

    /// Coordinator side: bind the durable reservation identity to this
    /// one copier before it is launched. A zero field is never accepted;
    /// the worker fails closed instead of consulting mutable names.
    pub fn write_copy_identity(&self, identity: CopyIdentity) -> Result<(), String> {
        if identity.tracking_id <= 0
            || identity.generation_id <= 0
            || identity.snapshot_id <= 0
            || identity.rel_oid == 0
        {
            return Err("external_zstd copy identity must be complete and positive".to_string());
        }
        unsafe {
            (*self.shared).tracking_id = identity.tracking_id;
            (*self.shared).generation_id = identity.generation_id;
            (*self.shared).snapshot_id = identity.snapshot_id;
            (*self.shared).rel_oid = identity.rel_oid;
            (*self.shared).persist_requested = 1;
        }
        Ok(())
    }

    pub fn read_copy_identity(&self) -> Option<CopyIdentity> {
        let _ = self.atomic_state().load(Ordering::Acquire);
        unsafe {
            if (*self.shared).persist_requested == 0 {
                return None;
            }
            let identity = CopyIdentity {
                tracking_id: (*self.shared).tracking_id,
                generation_id: (*self.shared).generation_id,
                snapshot_id: (*self.shared).snapshot_id,
                rel_oid: (*self.shared).rel_oid,
            };
            (identity.tracking_id > 0
                && identity.generation_id > 0
                && identity.snapshot_id > 0
                && identity.rel_oid != 0)
                .then_some(identity)
        }
    }

    /// Coordinator side only: write the quoted, comma-separated column
    /// list. Must be called strictly before [`HandoffSegment::signal`]
    /// with [`HandoffPhase::LockHeldGoAhead`] -- writing after signaling
    /// would race the copier's read with no defined visibility (the
    /// module doc's "Memory ordering" section covers exactly why the
    /// write-before-publish ordering is what makes this safe without a
    /// second synchronization mechanism for the payload itself).
    pub fn write_column_list(&self, list: &str) -> Result<(), String> {
        let bytes = list.as_bytes();
        if bytes.len() > COLUMN_LIST_CAP {
            return Err(format!(
                "column list ({} bytes) exceeds the {COLUMN_LIST_CAP}-byte handoff payload bound",
                bytes.len()
            ));
        }
        unsafe {
            let dst = std::ptr::addr_of_mut!((*self.shared).column_list);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*dst).as_mut_ptr(), bytes.len());
            (*self.shared).column_list_len = bytes.len() as u32;
        }
        Ok(())
    }

    /// Copier side only: read the column list written by the coordinator.
    /// Callers must have already observed `LockHeldGoAhead` (via
    /// `wait_for_state`, whose Acquire load is what makes this read
    /// well-defined) before calling this -- reading beforehand would
    /// observe whatever was there before publication (zero-length,
    /// harmlessly, since `coordinator_create` zero-initializes
    /// `column_list_len`, but still not the real payload).
    pub fn read_column_list(&self) -> String {
        unsafe {
            let len = (*self.shared).column_list_len as usize;
            let len = len.min(COLUMN_LIST_CAP);
            let src = std::ptr::addr_of!((*self.shared).column_list);
            let slice = std::slice::from_raw_parts((*src).as_ptr(), len);
            String::from_utf8_lossy(slice).into_owned()
        }
    }

    /// Coordinator side only: write the schema-qualified, quoted target
    /// relation name. Same write-before-publish contract as
    /// `write_column_list`.
    pub fn write_target_relation(&self, qualified_name: &str) -> Result<(), String> {
        let bytes = qualified_name.as_bytes();
        if bytes.len() > TARGET_RELATION_CAP {
            return Err(format!(
                "target relation name ({} bytes) exceeds the {TARGET_RELATION_CAP}-byte handoff payload bound",
                bytes.len()
            ));
        }
        unsafe {
            let dst = std::ptr::addr_of_mut!((*self.shared).target_relation);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*dst).as_mut_ptr(), bytes.len());
            (*self.shared).target_relation_len = bytes.len() as u32;
        }
        Ok(())
    }

    /// Copier side only: read the target relation name written by the
    /// coordinator. Same post-LockHeldGoAhead-only contract as
    /// `read_column_list`.
    pub fn read_target_relation(&self) -> String {
        unsafe {
            let len = (*self.shared).target_relation_len as usize;
            let len = len.min(TARGET_RELATION_CAP);
            let src = std::ptr::addr_of!((*self.shared).target_relation);
            let slice = std::slice::from_raw_parts((*src).as_ptr(), len);
            String::from_utf8_lossy(slice).into_owned()
        }
    }

    /// Coordinator side: publish the exact lock-captured JSON column
    /// contract before `LockHeldGoAhead`.
    pub fn write_column_contract(&self, contract: &str) -> Result<(), String> {
        let bytes = contract.as_bytes();
        if bytes.len() > COLUMN_CONTRACT_CAP {
            return Err(format!(
                "column contract ({} bytes) exceeds the {COLUMN_CONTRACT_CAP}-byte handoff bound",
                bytes.len()
            ));
        }
        unsafe {
            let dst = std::ptr::addr_of_mut!((*self.shared).column_contract);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*dst).as_mut_ptr(), bytes.len());
            (*self.shared).column_contract_len = bytes.len() as u32;
        }
        Ok(())
    }

    pub fn read_column_contract(&self) -> String {
        unsafe {
            let len = ((*self.shared).column_contract_len as usize).min(COLUMN_CONTRACT_CAP);
            let src = std::ptr::addr_of!((*self.shared).column_contract);
            let slice = std::slice::from_raw_parts((*src).as_ptr(), len);
            String::from_utf8_lossy(slice).into_owned()
        }
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

    /// Copier side only: record the row count its real cursor fetch saw.
    /// Must be called strictly before `signal(SnapshotPinned)`.
    pub fn write_fetched_row_count(&self, count: u64) {
        unsafe {
            (*self.shared).fetched_row_count = count;
        }
    }

    /// Coordinator/observer side: read the row count the copier recorded.
    /// Only well-defined after observing `SnapshotPinned`.
    pub fn read_fetched_row_count(&self) -> u64 {
        unsafe { (*self.shared).fetched_row_count }
    }

    /// Copier side only: record the WAL LSN observed at pin time. Must be
    /// called strictly before `signal(SnapshotPinned)`.
    pub fn write_pinned_wal_lsn(&self, lsn: u64) {
        unsafe {
            (*self.shared).pinned_wal_lsn = lsn;
        }
    }

    /// Coordinator/observer side: read the WAL LSN the copier recorded at
    /// pin time. Only well-defined after observing `SnapshotPinned`.
    pub fn read_pinned_wal_lsn(&self) -> u64 {
        unsafe { (*self.shared).pinned_wal_lsn }
    }

    /// Either side: record a diagnostic message, truncated (never panics)
    /// to fit. Must be called strictly before `signal(Failed)` to be
    /// visible to the other side under the same acquire-after-load
    /// discipline as every other payload field.
    pub fn write_error_message(&self, msg: &str) {
        let bytes = msg.as_bytes();
        let len = bytes.len().min(ERROR_MESSAGE_CAP);
        unsafe {
            let dst = std::ptr::addr_of_mut!((*self.shared).error_message);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*dst).as_mut_ptr(), len);
            (*self.shared).error_message_len = len as u32;
        }
    }

    /// Read whatever diagnostic message the failing side recorded. Only
    /// meaningful after observing `Failed`/`PeerFailed`; empty otherwise.
    pub fn read_error_message(&self) -> String {
        unsafe {
            let len = (*self.shared).error_message_len as usize;
            let len = len.min(ERROR_MESSAGE_CAP);
            let src = std::ptr::addr_of!((*self.shared).error_message);
            let slice = std::slice::from_raw_parts((*src).as_ptr(), len);
            String::from_utf8_lossy(slice).into_owned()
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
    /// Exit without attaching or signaling anything, simulating a hard crash
    /// before the copier reaches the handoff protocol.  A short startup grace
    /// makes the test deterministic: the coordinator first observes a started
    /// worker, then observes that same peer die.
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
        // DSM segment (there is none).  Do not race wait_for_startup(): that
        // would test the bgworker registry's startup-status timing instead of
        // HandoffSegment::wait_for_state's PeerDead path.
        std::thread::sleep(Duration::from_secs(1));
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

// ── Stage 6 adversarial regression: exact lock-before-snapshot-pin
// ordering ──────────────────────────────────────────────────────────────
//
// The online-snapshot protocol's entire correctness depends on one
// ordering fact: the copier's first real SQL (which pins its REPEATABLE
// READ snapshot) must never be reachable before the coordinator's table
// lock (M2) is genuinely held -- that's what makes it safe for the marker
// COMMIT (M8) to be the dividing line between "rows the copy will see"
// and "rows post-marker WAL replay will see" (plan §1e). Stage 4's cross-
// process round-trip test proves the *signal* is delivered correctly; it
// does not, by itself, prove a worker that raced ahead of the lock would
// have been detectable. This worker makes that concrete: on waking from
// LockHeldGoAhead, instead of trusting the signal, it independently
// verifies the lock is real by attempting a *conflicting* lock
// (ACCESS EXCLUSIVE, which conflicts with every other lock mode including
// SHARE ROW EXCLUSIVE) with NOWAIT from its own, separate session. If the
// coordinator's lock is genuinely held, this must fail with
// lock_not_available; if it unexpectedly succeeds, the ordering guarantee
// is broken and this test must fail loudly, not pass by coincidence.
//
// Reuses HandoffPhase's existing terminal states rather than adding a new
// shared-memory field: SnapshotPinned means "conflict correctly detected"
// (the expected, good outcome), Failed means either the lock was
// unexpectedly acquired (an ordering violation) or an unrelated error
// occurred (logged either way).

const LOCK_ORDER_PROBE_TABLE: &str = "public.it_lock_order_probe";

fn lock_order_probe_worker_body(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(pgrx::bgworkers::SignalWakeFlags::SIGTERM);
    let dsm_h = (unsafe { i64::from_datum(arg, false) }.unwrap_or(0) & 0xFFFF_FFFF) as u32;

    let segment = unsafe { HandoffSegment::attach(dsm_h) };
    let segment = match segment {
        Some(s) => s,
        None => {
            log!("pg_flashback lock-order probe worker: dsm_attach failed, exiting");
            return;
        }
    };

    // operation_nonce is repurposed here to carry the coordinator's
    // current database oid (a u32, fits easily in the u64 field) -- this
    // worker's own use of HandoffShared, distinct from the real protocol's
    // eventual meaning for this field.
    let db_oid = pg_sys::Oid::from(segment.operation_nonce() as u32);
    BackgroundWorker::connect_worker_to_spi_by_oid(Some(db_oid), None);

    match segment.wait_for_state(HandoffPhase::LockHeldGoAhead, Duration::from_secs(30), None) {
        Ok(()) => {
            let lock_query =
                format!("LOCK TABLE {LOCK_ORDER_PROBE_TABLE} IN ACCESS EXCLUSIVE MODE NOWAIT");
            // Classify precisely by SQLSTATE, not "any error/panic means
            // the conflict was detected" -- that blanket classification
            // previously made this test pass for the wrong reason (the
            // table wasn't durably visible to this worker at all, so the
            // NOWAIT lock failed with undefined_table, not lock_not_
            // available, and both were being treated as the same "good"
            // outcome). See test_support's module doc.
            #[derive(Debug)]
            enum ProbeOutcome {
                Conflicted,
                Acquired,
                UnexpectedError(String),
            }
            // Every SPI call in a background worker must run inside a real
            // transaction (BackgroundWorker::transaction, which calls
            // StartTransactionCommand() first) -- calling Spi::run with no
            // transaction started crashes the backend outright rather
            // than merely erroring (found directly while building the
            // Stage 6 completion coordinator's own worker).
            let outcome = PgTryBuilder::new(|| {
                match BackgroundWorker::transaction(|| Spi::run(&lock_query)) {
                    Ok(()) => ProbeOutcome::Acquired,
                    Err(e) => ProbeOutcome::UnexpectedError(e.to_string()),
                }
            })
            .catch_when(PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE, |_| {
                ProbeOutcome::Conflicted
            })
            .catch_others(|e| {
                let msg = match &e {
                    CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => {
                        r.message().to_string()
                    }
                    CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
                };
                ProbeOutcome::UnexpectedError(msg)
            })
            .execute();
            match outcome {
                ProbeOutcome::Conflicted => {
                    // The NOWAIT lock genuinely conflicted (SQLSTATE
                    // lock_not_available) -- the coordinator's lock was
                    // really held. Correct, expected outcome.
                    segment.signal(HandoffPhase::SnapshotPinned);
                }
                ProbeOutcome::Acquired => {
                    // No conflict at all -- the coordinator's lock was NOT
                    // actually held when this worker was told to proceed.
                    // An ordering violation.
                    log!(
                        "pg_flashback lock-order probe worker: ACQUIRED a conflicting lock -- \
                         ordering violation, coordinator's lock was not actually held"
                    );
                    segment.write_error_message("acquired a conflicting lock (ordering violation)");
                    segment.signal(HandoffPhase::Failed);
                }
                ProbeOutcome::UnexpectedError(msg) => {
                    log!("pg_flashback lock-order probe worker: unexpected error (not lock_not_available): {msg}");
                    segment.write_error_message(&msg);
                    segment.signal(HandoffPhase::Failed);
                }
            }
        }
        Err(e) => {
            log!("pg_flashback lock-order probe worker: wait failed: {e:?}");
            segment.write_error_message(&format!("wait failed: {e:?}"));
            segment.signal(HandoffPhase::Failed);
        }
    }
}

/// The real, exported symbol this crate's `lib.rs` `#[unsafe(no_mangle)]`
/// wrapper delegates to -- see that wrapper's doc comment (and the
/// Stage 4 selftest worker's identical requirement) for why a thin
/// crate-root wrapper, not this module function directly, must carry
/// `no_mangle`.
pub extern "C-unwind" fn pg_flashback_external_zstd_lock_order_probe_worker_main(
    arg: pg_sys::Datum,
) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        lock_order_probe_worker_body(arg);
    }));
    if let Err(e) = result {
        let msg = e
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        log!("pg_flashback lock-order probe worker: top-level PANICKED: {msg}");
    }
}

fn launch_lock_order_probe_worker(
    dsm_h: pg_sys::dsm_handle,
) -> Result<DynamicBackgroundWorker, pgrx::bgworkers::DynamicBackgroundWorkerLoadError> {
    BackgroundWorkerBuilder::new("pg_flashback external_zstd lock-order probe worker")
        .set_function("pg_flashback_external_zstd_lock_order_probe_worker_main")
        .set_library("pg_flashback")
        .set_argument((dsm_h as i64).into_datum())
        .set_notify_pid(unsafe { pg_sys::MyProcPid })
        .enable_spi_access()
        .load_dynamic()
}

// ── Test-only: commit-via-worker (fixes a real cross-session-visibility
// gap in this crate's own test suite) ───────────────────────────────────
//
// pgrx's own #[pg_test] harness wraps every test function in one
// postgres-client-side transaction and *unconditionally* rolls it back at
// the end, pass or fail (pgrx-tests' framework.rs: "and abort the
// transaction when complete"). Anything a #[pg_test] function creates via
// ordinary `Spi::run` -- a table, a tracked_tables row, a reservation --
// is therefore *never* durably committed, and is consequently invisible
// to any genuinely separate session, including a dynamically-launched
// background worker connecting via its own SPI connection: MVCC only
// exposes committed data across sessions, and this data is never
// committed. A worker attempting to touch such a table sees "relation
// does not exist", not whatever the test intended to exercise.
//
// This was found directly, not anticipated: a lock-order-probe test
// (external_zstd_handoff.rs's own test_copier_cannot_observe_go_ahead_
// before_lock_is_held, and independently while building the Stage 6
// completion coordinator tests) both create their target table via plain
// `Spi::run` inside the test function, then have a worker interact with
// it. Both were passing, but for the wrong reason: the worker's `LOCK
// TABLE ... NOWAIT` (or, in the coordinator tests, `SELECT ... FROM
// target`) was failing with `undefined_table`, not the condition the test
// actually meant to exercise (`lock_not_available` / a real cursor read)
// -- and the worker's blanket "any error/panic here means the thing I was
// testing for happened" classification silently absorbed the difference.
//
// `run_sql_committed` fixes this at the root: it runs arbitrary setup SQL
// inside a *real* background worker (via `BackgroundWorker::transaction`,
// which genuinely calls `CommitTransactionCommand()`), so anything it
// creates is durably visible to every subsequent session for the rest of
// the test, including the #[pg_test] function's own later reads (a
// session that never commits its own writes can still *see* what another
// session committed, via ordinary MVCC). This is test-only infrastructure
// -- real production code never needs to work around its own test
// harness -- and is cfg-gated out of every non-test build accordingly.
#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod test_support {
    use super::{HandoffPhase, HandoffSegment};
    use pgrx::bgworkers::{
        BackgroundWorker, BackgroundWorkerBuilder, DynamicBackgroundWorker, SignalWakeFlags,
    };
    use pgrx::pg_sys;
    use pgrx::pg_sys::panic::CaughtError;
    use pgrx::prelude::*;
    use std::time::Duration;

    fn commit_sql_worker_body(arg: pg_sys::Datum) {
        BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
        let raw = unsafe { i64::from_datum(arg, false) }.unwrap_or(0);
        let dsm_h = (raw & 0xFFFF_FFFF) as u32;
        let db_oid = pg_sys::Oid::from(((raw >> 32) & 0xFFFF_FFFF) as u32);

        let segment = unsafe { HandoffSegment::attach(dsm_h) };
        let segment = match segment {
            Some(s) => s,
            None => {
                log!("pg_flashback test commit-sql worker: dsm_attach failed, exiting");
                return;
            }
        };

        BackgroundWorker::connect_worker_to_spi_by_oid(Some(db_oid), None);
        // column_list is repurposed here to carry arbitrary setup SQL
        // rather than a column list -- test-only use of the same bounded
        // payload mechanism, not a second protocol meaning layered onto
        // the real one.
        let sql = segment.read_column_list();
        // A genuine lock conflict against another still-open session (the
        // calling #[pg_test] function's own session is never committed/
        // rolled back until the test ends, so it can hold locks for the
        // rest of the test) would otherwise block here for the full
        // duration of whatever bound the caller is using to wait on this
        // worker, surfacing only as an opaque Timeout with no indication
        // of what was actually blocked. A short lock_timeout turns that
        // into an immediate, diagnosable lock_not_available naming the
        // conflicting relation instead.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            BackgroundWorker::transaction(|| {
                Spi::run("SET lock_timeout = '5s'")?;
                Spi::run(&sql)
            })
        }));
        match result {
            Ok(Ok(())) => segment.signal(HandoffPhase::SnapshotPinned),
            Ok(Err(e)) => {
                log!("pg_flashback test commit-sql worker: SQL failed: {e}");
                segment.write_error_message(&e.to_string());
                segment.signal(HandoffPhase::Failed);
            }
            Err(e) => {
                // BackgroundWorker::transaction uses PgTryBuilder
                // internally; an uncaught PostgreSQL ERROR it has no
                // handler for is re-thrown as resume_unwind(Box::new(
                // CaughtError)), not a plain string payload.
                let msg = e
                    .downcast_ref::<CaughtError>()
                    .map(|ce| match ce {
                        CaughtError::PostgresError(r) | CaughtError::ErrorReport(r) => {
                            r.message().to_string()
                        }
                        CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
                    })
                    .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                    .or_else(|| e.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<unrecognized panic payload type>".to_string());
                log!("pg_flashback test commit-sql worker: SQL panicked: {msg}");
                segment.write_error_message(&format!("panic: {msg}"));
                segment.signal(HandoffPhase::Failed);
            }
        }
    }

    /// The real, exported symbol this crate's `lib.rs` `#[unsafe(no_mangle)]`
    /// wrapper delegates to -- see the Stage 4 selftest worker's identical
    /// requirement.
    pub extern "C-unwind" fn pg_flashback_test_commit_sql_worker_main(arg: pg_sys::Datum) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            commit_sql_worker_body(arg);
        }));
        if let Err(e) = result {
            let msg = e
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| e.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            log!("pg_flashback test commit-sql worker: top-level PANICKED: {msg}");
        }
    }

    /// Run `sql` to completion in a real, separate, committing background
    /// worker, and only return once it has -- so that anything `sql`
    /// creates is durably visible to every session for the rest of the
    /// test (see the module doc for exactly why this is necessary and
    /// what it fixes). Panics with the worker's own reported error on
    /// failure.
    /// Launch the commit-sql worker with `sql` and return immediately,
    /// without waiting for it to finish -- exposed separately from
    /// `run_sql_committed` so callers that need a genuinely alive-but-
    /// never-signaling peer (e.g. a rollback-point regression exercising
    /// M6's plain timeout path with a real, live process rather than a
    /// dead one) can drive the wait themselves.
    pub fn launch_commit_sql_worker(sql: &str) -> (HandoffSegment, DynamicBackgroundWorker) {
        let db_oid = unsafe { pg_sys::MyDatabaseId };
        let segment = unsafe { HandoffSegment::coordinator_create(db_oid.to_u32() as u64) };
        segment
            .write_column_list(sql)
            .expect("setup SQL exceeds the handoff payload bound");
        let packed: i64 = (segment.handle() as i64) | ((db_oid.to_u32() as i64) << 32);
        let worker = BackgroundWorkerBuilder::new("pg_flashback test commit-sql worker")
            .set_function("pg_flashback_test_commit_sql_worker_main")
            .set_library("pg_flashback")
            .set_argument(packed.into_datum())
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .enable_spi_access()
            .load_dynamic()
            .expect("failed to launch commit-sql worker");
        worker
            .wait_for_startup()
            .expect("commit-sql worker did not start");
        (segment, worker)
    }

    pub fn run_sql_committed(sql: &str) {
        let (segment, worker) = launch_commit_sql_worker(sql);
        let result = segment.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(30),
            Some(&worker),
        );
        if result.is_err() {
            panic!(
                "run_sql_committed failed: {result:?} ({})",
                segment.read_error_message()
            );
        }
        segment.detach();
    }
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
        // Fail mode intentionally signals and exits immediately. On a fast
        // scheduler the bgworker registry may already report `Stopped` by the
        // time the coordinator observes startup, even though the durable DSM
        // assertion we care about (`Failed`) was published correctly. Do not
        // turn that harmless registry timing into a flaky prerequisite; the
        // exact PeerFailed assertion below remains the authority.
        let _ = worker.wait_for_startup();

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

    /// Stage 6 adversarial regression: proves exact lock-before-snapshot-
    /// pin ordering against a real second OS process and a real table lock
    /// -- not just the abstract signal-delivery proof the Stage 4 tests
    /// above already give. See the worker's own doc comment (module scope)
    /// for the full design; this test's job is to create the real table,
    /// hold the real coordinator-side lock across the entire signal
    /// exchange, and fail loudly if the worker ever reports back that it
    /// managed to acquire a conflicting lock.
    #[pg_test]
    fn test_copier_cannot_observe_go_ahead_before_lock_is_held() {
        // Must be durably committed, not just run in this #[pg_test]
        // function's own (always-rolled-back) session -- otherwise the
        // probe worker's separate session cannot see the table at all
        // ("relation does not exist"), which its own blanket error
        // handling would silently misclassify as "conflict correctly
        // detected". See test_support's module doc for the full story
        // (found directly, not anticipated, while building the Stage 6
        // completion coordinator tests).
        test_support::run_sql_committed(&format!(
            "CREATE TABLE IF NOT EXISTS {LOCK_ORDER_PROBE_TABLE} (id int)"
        ));

        let db_oid = unsafe { pg_sys::MyDatabaseId }.to_u32() as u64;
        let seg = unsafe { HandoffSegment::coordinator_create(db_oid) };
        let worker = launch_lock_order_probe_worker(seg.handle())
            .expect("failed to launch lock-order probe worker");
        worker
            .wait_for_startup()
            .expect("lock-order probe worker did not start");

        // The real ordering guarantee under test: acquire the lock FIRST,
        // signal LockHeldGoAhead only AFTER it is genuinely held --
        // mirroring the real protocol's M2 (lock) strictly before M5
        // (signal) ordering (plan §1e).
        Spi::run(&format!(
            "LOCK TABLE {LOCK_ORDER_PROBE_TABLE} IN SHARE ROW EXCLUSIVE MODE"
        ))
        .expect("coordinator failed to acquire its own lock");
        seg.signal(HandoffPhase::LockHeldGoAhead);

        let result = seg.wait_for_state(
            HandoffPhase::SnapshotPinned,
            Duration::from_secs(15),
            Some(&worker),
        );
        assert_eq!(
            result,
            Ok(()),
            "worker did not report SnapshotPinned (the correct, expected outcome -- \
             it should have observed the lock as genuinely held and failed to acquire \
             a conflicting one); Err(PeerFailed) here means the ordering guarantee was \
             violated -- see the worker's own log output above"
        );
        seg.detach();
        // The SHARE ROW EXCLUSIVE lock above is released automatically at
        // this #[pg_test]'s own transaction rollback (pgrx's test harness
        // convention), same as every other test in this file/module.
    }
}

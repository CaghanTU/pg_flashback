use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{PGRXSharedMemory, PgLwLock};

use crate::capture::logical_decoding::emit_output_line;
use crate::storage::worker::effective_queue_capacity;

#[cfg(feature = "pg_test")]
const DELTA_RING_CAPACITY: usize = 512;
#[cfg(not(feature = "pg_test"))]
const DELTA_RING_CAPACITY: usize = 100_000;
const BACKPRESSURE_SLEEP_US: u64 = 1_000;
const EVENT_TYPE_LEN: usize = 16;
const TABLE_NAME_LEN: usize = 128;
const REL_OID_LEN: usize = 16;
const SOURCE_XID_LEN: usize = 32;
const JSON_PAYLOAD_LEN: usize = 256;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct DeltaEventSlot {
    used: bool,
    rel_oid: [u8; REL_OID_LEN],
    source_xid: [u8; SOURCE_XID_LEN],
    event_time_us: i64,
    event_type: [u8; EVENT_TYPE_LEN],
    table_name: [u8; TABLE_NAME_LEN],
    old_data: [u8; JSON_PAYLOAD_LEN],
    new_data: [u8; JSON_PAYLOAD_LEN],
}

impl Default for DeltaEventSlot {
    fn default() -> Self {
        Self {
            used: false,
            rel_oid: [0; REL_OID_LEN],
            source_xid: [0; SOURCE_XID_LEN],
            event_time_us: 0,
            event_type: [0; EVENT_TYPE_LEN],
            table_name: [0; TABLE_NAME_LEN],
            old_data: [0; JSON_PAYLOAD_LEN],
            new_data: [0; JSON_PAYLOAD_LEN],
        }
    }
}

unsafe impl PGRXSharedMemory for DeltaEventSlot {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct DeltaRingBuffer {
    head: u32,
    tail: u32,
    count: u32,
    dropped: u64,
    overflow_events: u64,
    degraded: bool,
    slots: [DeltaEventSlot; DELTA_RING_CAPACITY],
}

impl Default for DeltaRingBuffer {
    fn default() -> Self {
        // This shared-memory POD struct is safe to zero-init and avoids giant stack temporaries.
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for DeltaRingBuffer {}

#[derive(Debug, Clone)]
pub struct DeltaEvent {
    pub rel_oid: String,
    pub source_xid: String,
    pub event_time_us: i64,
    pub event_type: String,
    pub table_name: String,
    pub old_data: Option<String>,
    pub new_data: Option<String>,
}

pub static DELTA_RING: PgLwLock<DeltaRingBuffer> = unsafe { PgLwLock::new(c"pg_flashback_delta_ring") };

fn sanitize_ring_state(ring: &mut DeltaRingBuffer, configured_capacity: usize) {
    let invalid_index = (ring.head as usize) >= DELTA_RING_CAPACITY || (ring.tail as usize) >= DELTA_RING_CAPACITY;
    let invalid_count = (ring.count as usize) > configured_capacity;

    if invalid_index || invalid_count {
        warning!(
            "pg_flashback RING_STATE_RESET head={} tail={} count={} configured_capacity={} compiled_capacity={}",
            ring.head,
            ring.tail,
            ring.count,
            configured_capacity,
            DELTA_RING_CAPACITY
        );
        ring.head = 0;
        ring.tail = 0;
        ring.count = 0;
        ring.degraded = false;
    }
}

pub unsafe fn enqueue_delta_event(ctx: *mut pg_sys::LogicalDecodingContext, event: DeltaEvent) {
    enqueue_delta_event_internal(Some(ctx), event);
}

pub fn enqueue_delta_event_background(event: DeltaEvent) {
    unsafe {
        enqueue_delta_event_internal(None, event);
    }
}

fn emit_if_possible(ctx: Option<*mut pg_sys::LogicalDecodingContext>, msg: &str) {
    if let Some(ptr) = ctx {
        unsafe {
            emit_output_line(ptr, msg);
        }
    }
}

unsafe fn enqueue_delta_event_internal(ctx: Option<*mut pg_sys::LogicalDecodingContext>, event: DeltaEvent) {
    let configured_capacity = effective_queue_capacity().min(DELTA_RING_CAPACITY);

    loop {
        let mut ring = DELTA_RING.exclusive();
        sanitize_ring_state(&mut ring, configured_capacity);

        if (ring.count as usize) < configured_capacity {
            let idx = ring.tail as usize;
            write_slot(&mut ring.slots[idx], &event);
            ring.tail = ((idx + 1) % DELTA_RING_CAPACITY) as u32;
            ring.count += 1;

            let recovery_threshold = (configured_capacity / 2) as u32;
            if ring.degraded && ring.count <= recovery_threshold {
                ring.degraded = false;
                let msg = format!(
                    "COVERAGE_RECOVERED queue_count={} queue_capacity={} dropped_total={}",
                    ring.count, configured_capacity, ring.dropped
                );
                warning!("pg_flashback {msg}");
                emit_if_possible(ctx, &msg);
            }

            return;
        }

        ring.overflow_events += 1;
        if !ring.degraded {
            ring.degraded = true;
            let msg = format!(
                "COVERAGE_DEGRADED queue_full=true queue_count={} queue_capacity={} overflow_events={} dropped_total={}",
                ring.count, configured_capacity, ring.overflow_events, ring.dropped
            );
            warning!("pg_flashback {msg}");
            emit_if_possible(ctx, &msg);
        }

        drop(ring);
        pg_sys::pg_usleep(BACKPRESSURE_SLEEP_US as i64);
    }
}

pub fn dequeue_batch(limit: usize) -> Vec<DeltaEvent> {
    let mut out = Vec::with_capacity(limit);
    let mut ring = DELTA_RING.exclusive();
    let configured_capacity = effective_queue_capacity().min(DELTA_RING_CAPACITY);
    sanitize_ring_state(&mut ring, configured_capacity);

    while (out.len() < limit) && (ring.count > 0) {
        let idx = ring.head as usize;
        let event = read_slot(&ring.slots[idx]);
        out.push(event);

        ring.slots[idx] = DeltaEventSlot::default();
        ring.head = ((idx + 1) % DELTA_RING_CAPACITY) as u32;
        ring.count -= 1;
    }

    out
}

fn write_slot(slot: &mut DeltaEventSlot, event: &DeltaEvent) {
    slot.used = true;
    slot.event_time_us = event.event_time_us;
    write_fixed(&mut slot.rel_oid, &event.rel_oid);
    write_fixed(&mut slot.source_xid, &event.source_xid);
    write_fixed(&mut slot.event_type, &event.event_type);
    write_fixed(&mut slot.table_name, &event.table_name);
    write_fixed(&mut slot.old_data, event.old_data.as_deref().unwrap_or(""));
    write_fixed(&mut slot.new_data, event.new_data.as_deref().unwrap_or(""));
}

fn read_slot(slot: &DeltaEventSlot) -> DeltaEvent {
    let old = read_fixed(&slot.old_data);
    let new = read_fixed(&slot.new_data);
    DeltaEvent {
        rel_oid: read_fixed(&slot.rel_oid),
        source_xid: read_fixed(&slot.source_xid),
        event_time_us: slot.event_time_us,
        event_type: read_fixed(&slot.event_type),
        table_name: read_fixed(&slot.table_name),
        old_data: if old.is_empty() { None } else { Some(old) },
        new_data: if new.is_empty() { None } else { Some(new) },
    }
}

fn write_fixed<const N: usize>(dst: &mut [u8; N], src: &str) {
    dst.fill(0);
    let bytes = src.as_bytes();
    let len = bytes.len().min(N.saturating_sub(1));
    dst[..len].copy_from_slice(&bytes[..len]);
}

fn read_fixed<const N: usize>(src: &[u8; N]) -> String {
    let end = src.iter().position(|b| *b == 0).unwrap_or(N);
    String::from_utf8_lossy(&src[..end]).into_owned()
}

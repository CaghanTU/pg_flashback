//! external_zstd artifact binary format, `format_version = 1`
//! (Step 9, Stage 3 of the implementation plan).
//!
//! This module owns the frame layout (§5 of the plan) and the PostgreSQL
//! binary-send/receive plumbing for one column value. It does **not** wrap
//! a zstd stream and does **not** touch SPI/cursors -- those land in later
//! stages (the copier's streaming persist/restore). What's tested here is
//! round-tripped directly: write a header + rows + trailer into an
//! in-memory buffer, read it back, and confirm the decoded values are
//! byte-identical to what was encoded, using real PostgreSQL binary
//! send/receive functions against a real table's real columns.
//!
//! Format (all multi-byte integers big-endian):
//! ```text
//! MAGIC        8 bytes   ASCII "PGFBZST1"
//! VERSION      4 bytes   BE u32, = 1
//! COLCOUNT     4 bytes   BE u32
//! COLUMN[COLCOUNT]: attnum(4B BE i32) atttypid(4B BE u32) atttypmod(4B BE i32)
//!   attcollation(4B BE u32) attnotnull(1B) attidentity(1B) namelen(2B BE u16)
//!   name(namelen bytes, UTF-8)
//! ROW[] until ROWCOUNT:
//!   ROWLEN(4B BE u32)  -- total byte length of NULLBITMAP + all VALLEN/
//!                         VALBYTES that follow for this row
//!   NULLBITMAP(ceil(COLCOUNT/8) bytes)
//!   per non-null column, in COLUMN order: VALLEN(4B BE i32) VALBYTES
//! TRAILER: MAGIC_END(8B "PGFBEND1") ROWCOUNT(8B BE u64)
//! ```
//!
//! Decode is bounds-checked and fails closed: a row's declared `ROWLEN`
//! must exactly equal the actual decoded `NULLBITMAP` + value bytes for
//! that row (checked both directions, not just an upper bound), every
//! `VALLEN` is checked against the configured max-row-bytes ceiling before
//! any allocation, and `MAGIC`/`VERSION`/`MAGIC_END`/`ROWCOUNT` are all
//! verified exactly. This is pg_flashback's own artifact format, not a
//! claim of `COPY BINARY` wire compatibility -- no external tool reads it.
//!
//! Stage 3 of a staged implementation (see the Step 9 plan, §15): exercised
//! directly by the `#[pg_test]`s below; real production callers (the
//! zstd-streaming copier/finalizer) land in later stages.
//! `#![allow(dead_code)]` is temporary scaffolding for that gap, removed
//! once Stage 6+ wires real callers in.
#![allow(dead_code)]

use pgrx::pg_sys;
#[cfg(any(test, feature = "pg_test"))]
use pgrx::prelude::*;
use pgrx::varlena::{vardata_any, varsize_any_exhdr};
use std::io::{self, Read, Write};

pub const FORMAT_VERSION: u32 = 1;
pub const MAGIC: &[u8; 8] = b"PGFBZST1";
pub const MAGIC_END: &[u8; 8] = b"PGFBEND1";

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

// ── Column contract ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    pub attnum: i32,
    pub atttypid: u32,
    pub atttypmod: i32,
    pub attcollation: u32,
    pub attnotnull: bool,
    /// '\0' = not an identity column, 'a' = ALWAYS, 'd' = BY DEFAULT.
    pub attidentity: u8,
    pub name: String,
}

/// Explicit allowlist of types proven to round-trip correctly through this
/// format: base scalars and one-dimensional arrays of them. Anything else
/// -- domains, composites, ranges, enums, other extension types -- is
/// rejected by the caller before persist ever starts (see
/// `is_allowed_type`), not attempted and silently mis-serialized.
pub fn is_allowed_type(oid: pg_sys::Oid) -> bool {
    matches!(
        oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::TEXTOID
            | pg_sys::VARCHAROID
            | pg_sys::BPCHAROID
            | pg_sys::BOOLOID
            | pg_sys::NUMERICOID
            | pg_sys::DATEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::INTERVALOID
            | pg_sys::BYTEAOID
            | pg_sys::UUIDOID
            | pg_sys::JSONBOID
            | pg_sys::INT2ARRAYOID
            | pg_sys::INT4ARRAYOID
            | pg_sys::INT8ARRAYOID
            | pg_sys::TEXTARRAYOID
            | pg_sys::VARCHARARRAYOID
            | pg_sys::BPCHARARRAYOID
            | pg_sys::BOOLARRAYOID
            | pg_sys::NUMERICARRAYOID
            | pg_sys::DATEARRAYOID
            | pg_sys::TIMESTAMPARRAYOID
            | pg_sys::TIMESTAMPTZARRAYOID
            | pg_sys::INTERVALARRAYOID
            | pg_sys::BYTEAARRAYOID
            | pg_sys::UUIDARRAYOID
            | pg_sys::JSONBARRAYOID
    )
}

// ── PostgreSQL binary send/receive plumbing ─────────────────────────────

pub struct TypeSendInfo {
    pub send_oid: pg_sys::Oid,
}

pub struct TypeReceiveInfo {
    pub receive_oid: pg_sys::Oid,
    pub typioparam: pg_sys::Oid,
}

/// Resolve `type_oid`'s binary send function via the catalog (not guessed/
/// hardcoded), rejecting anything outside [`is_allowed_type`] up front.
pub fn resolve_send_info(type_oid: pg_sys::Oid) -> Result<TypeSendInfo, String> {
    if !is_allowed_type(type_oid) {
        return Err(format!(
            "type oid {type_oid:?} is outside the external_zstd materializable-type allowlist"
        ));
    }
    let mut send_oid = pg_sys::InvalidOid;
    let mut is_varlena = false;
    unsafe {
        pg_sys::getTypeBinaryOutputInfo(type_oid, &mut send_oid, &mut is_varlena);
    }
    if send_oid == pg_sys::InvalidOid {
        return Err(format!(
            "type oid {type_oid:?} has no binary output function"
        ));
    }
    Ok(TypeSendInfo { send_oid })
}

/// Resolve `type_oid`'s binary receive function and typioparam via the
/// catalog, rejecting anything outside [`is_allowed_type`] up front.
pub fn resolve_receive_info(type_oid: pg_sys::Oid) -> Result<TypeReceiveInfo, String> {
    if !is_allowed_type(type_oid) {
        return Err(format!(
            "type oid {type_oid:?} is outside the external_zstd materializable-type allowlist"
        ));
    }
    let mut receive_oid = pg_sys::InvalidOid;
    let mut typioparam = pg_sys::InvalidOid;
    unsafe {
        pg_sys::getTypeBinaryInputInfo(type_oid, &mut receive_oid, &mut typioparam);
    }
    if receive_oid == pg_sys::InvalidOid {
        return Err(format!(
            "type oid {type_oid:?} has no binary input function"
        ));
    }
    Ok(TypeReceiveInfo {
        receive_oid,
        typioparam,
    })
}

/// Encode one non-null column value to its PostgreSQL binary send-function
/// representation.
///
/// # Safety
/// `datum` must be a valid, currently-alive Datum of exactly the type
/// `send.send_oid` was resolved for.
pub unsafe fn encode_datum(datum: pg_sys::Datum, send: &TypeSendInfo) -> Vec<u8> {
    let bytea_ptr = pg_sys::OidSendFunctionCall(send.send_oid, datum);
    // A send function's returned bytea is an ordinary varlena (its header
    // form is not guaranteed to be the 4-byte "uncompressed" shape --
    // vardata_any/varsize_any_exhdr handle every valid varlena header shape
    // correctly, unlike assuming a fixed 4-byte header).
    let varlena_ptr = bytea_ptr.cast::<pg_sys::varlena>();
    let len = varsize_any_exhdr(varlena_ptr);
    let data_ptr = vardata_any(varlena_ptr) as *const u8;
    std::slice::from_raw_parts(data_ptr, len).to_vec()
}

/// Decode one non-null column value from its PostgreSQL binary
/// receive-function representation.
///
/// # Safety
/// The returned Datum is allocated in the current PostgreSQL memory
/// context; the caller is responsible for that context's lifetime exactly
/// as with any other Datum-returning FFI call.
pub unsafe fn decode_datum(bytes: &[u8], recv: &TypeReceiveInfo, typmod: i32) -> pg_sys::Datum {
    // StringInfoData wraps the caller's own buffer for a read-only receive
    // call: receive functions only ever read forward from `cursor` via
    // pq_getmsg*, they never append/realloc `data` on an input buffer, so
    // it is safe to point `data` at a Vec's storage for the duration of
    // this call without PostgreSQL taking ownership of it. `maxlen` is set
    // equal to `len` since nothing appends.
    let mut buf = pg_sys::StringInfoData {
        data: bytes.as_ptr() as *mut std::os::raw::c_char,
        len: bytes.len() as i32,
        maxlen: bytes.len() as i32,
        cursor: 0,
    };
    pg_sys::OidReceiveFunctionCall(
        recv.receive_oid,
        &mut buf as *mut pg_sys::StringInfoData,
        recv.typioparam,
        typmod,
    )
}

// ── Frame writer ────────────────────────────────────────────────────────

fn write_u16_be<W: Write>(w: &mut W, v: u16) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}
fn write_u32_be<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}
fn write_i32_be<W: Write>(w: &mut W, v: i32) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}
fn write_u64_be<W: Write>(w: &mut W, v: u64) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

pub fn write_header<W: Write>(w: &mut W, columns: &[ColumnDescriptor]) -> io::Result<()> {
    w.write_all(MAGIC)?;
    write_u32_be(w, FORMAT_VERSION)?;
    write_u32_be(w, columns.len() as u32)?;
    for c in columns {
        write_i32_be(w, c.attnum)?;
        write_u32_be(w, c.atttypid)?;
        write_i32_be(w, c.atttypmod)?;
        write_u32_be(w, c.attcollation)?;
        w.write_all(&[c.attnotnull as u8])?;
        w.write_all(&[c.attidentity])?;
        let name_bytes = c.name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(invalid_data(format!(
                "column name {:?} exceeds the 65535-byte frame limit",
                c.name
            )));
        }
        write_u16_be(w, name_bytes.len() as u16)?;
        w.write_all(name_bytes)?;
    }
    Ok(())
}

/// Write one row. `values[i] == None` means column `i` is NULL.
pub fn write_row<W: Write>(w: &mut W, values: &[Option<Vec<u8>>]) -> io::Result<()> {
    let ncols = values.len();
    let bitmap_len = ncols.div_ceil(8);
    let mut body = Vec::new();
    let mut bitmap = vec![0u8; bitmap_len];
    for (i, v) in values.iter().enumerate() {
        if v.is_none() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    body.extend_from_slice(&bitmap);
    for bytes in values.iter().flatten() {
        if bytes.len() > i32::MAX as usize {
            return Err(invalid_data(
                "column value exceeds the i32 length frame limit",
            ));
        }
        body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        body.extend_from_slice(bytes);
    }
    if body.len() > u32::MAX as usize {
        return Err(invalid_data("row exceeds the u32 ROWLEN frame limit"));
    }
    write_u32_be(w, body.len() as u32)?;
    w.write_all(&body)?;
    Ok(())
}

pub fn write_trailer<W: Write>(w: &mut W, row_count: u64) -> io::Result<()> {
    w.write_all(MAGIC_END)?;
    write_u64_be(w, row_count)
}

// ── Frame reader ────────────────────────────────────────────────────────

fn read_exact_vec<R: Read>(r: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
fn read_u16_be<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_be_bytes(b))
}
fn read_u32_be<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}
fn read_i32_be<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(i32::from_be_bytes(b))
}
fn read_u64_be<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

pub fn read_header<R: Read>(r: &mut R) -> io::Result<Vec<ColumnDescriptor>> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid_data("artifact MAGIC mismatch"));
    }
    let version = read_u32_be(r)?;
    if version != FORMAT_VERSION {
        return Err(invalid_data(format!(
            "artifact format_version {version} != expected {FORMAT_VERSION}"
        )));
    }
    let colcount = read_u32_be(r)? as usize;
    let mut columns = Vec::with_capacity(colcount);
    for _ in 0..colcount {
        let attnum = read_i32_be(r)?;
        let atttypid = read_u32_be(r)?;
        let atttypmod = read_i32_be(r)?;
        let attcollation = read_u32_be(r)?;
        let mut b1 = [0u8; 1];
        r.read_exact(&mut b1)?;
        let attnotnull = b1[0] != 0;
        let mut b2 = [0u8; 1];
        r.read_exact(&mut b2)?;
        let attidentity = b2[0];
        let namelen = read_u16_be(r)? as usize;
        let name_bytes = read_exact_vec(r, namelen)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|_| invalid_data("column name is not valid UTF-8"))?;
        columns.push(ColumnDescriptor {
            attnum,
            atttypid,
            atttypmod,
            attcollation,
            attnotnull,
            attidentity,
            name,
        });
    }
    Ok(columns)
}

/// Read one row given the column count and a hard per-row byte ceiling
/// (`pg_flashback.external_snapshot_max_row_bytes`). Returns one
/// `Option<Vec<u8>>` per column (`None` = NULL). Fails closed if the
/// declared `ROWLEN` disagrees with the actual decoded content length in
/// either direction, or if any single value's declared length would push
/// the row past `max_row_bytes`.
pub fn read_row<R: Read>(
    r: &mut R,
    ncols: usize,
    max_row_bytes: i64,
) -> io::Result<Vec<Option<Vec<u8>>>> {
    let row_len = read_u32_be(r)? as usize;
    if row_len as i64 > max_row_bytes {
        return Err(invalid_data(format!(
            "row declares {row_len} bytes, exceeding external_snapshot_max_row_bytes ({max_row_bytes})"
        )));
    }
    let body = read_exact_vec(r, row_len)?;
    let mut cursor = 0usize;
    let bitmap_len = ncols.div_ceil(8);
    if bitmap_len > body.len() {
        return Err(invalid_data(
            "row body shorter than its own null bitmap -- truncated artifact",
        ));
    }
    let bitmap = &body[..bitmap_len];
    cursor += bitmap_len;

    let mut values = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let is_null = (bitmap[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            values.push(None);
            continue;
        }
        if cursor + 4 > body.len() {
            return Err(invalid_data(
                "row body truncated before a declared non-null column's length prefix",
            ));
        }
        let vallen = i32::from_be_bytes(body[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        if vallen < 0 {
            return Err(invalid_data("negative VALLEN in row body"));
        }
        let vallen = vallen as usize;
        if vallen as i64 > max_row_bytes {
            return Err(invalid_data(format!(
                "column value declares {vallen} bytes, exceeding external_snapshot_max_row_bytes ({max_row_bytes})"
            )));
        }
        if cursor + vallen > body.len() {
            return Err(invalid_data(
                "row body truncated before a declared column value's end -- truncated artifact",
            ));
        }
        values.push(Some(body[cursor..cursor + vallen].to_vec()));
        cursor += vallen;
    }
    if cursor != body.len() {
        return Err(invalid_data(format!(
            "row ROWLEN ({row_len}) does not exactly match its decoded content ({cursor} bytes consumed) -- malformed frame"
        )));
    }
    Ok(values)
}

pub fn read_trailer<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC_END {
        return Err(invalid_data(
            "artifact MAGIC_END mismatch -- truncated or corrupt stream",
        ));
    }
    read_u64_be(r)
}

/// RAII guard for `PushActiveSnapshot`/`PopActiveSnapshot` balance. A real
/// heap scan via raw SPI (unlike a constant expression or an aggregate's
/// single output row) needs a *current* MVCC snapshot pushed for the
/// duration of the call; this guarantees the matching pop happens exactly
/// once, including when a Rust panic unwinds between construction and the
/// point where the caller would otherwise have popped it explicitly (e.g.
/// an `assert!`/`.expect()`/slice-index panic added later in a longer
/// streaming loop, not just the single FFI call this module uses today).
///
/// This guard does **not** cover every unbalancing path by itself -- see
/// the safety note below.
///
/// # What this guard does not protect against, and why that is still sound
///
/// A genuine PostgreSQL `ERROR` raised *inside* a raw FFI call made while
/// this guard is live (e.g. `SPI_execute_with_args` itself failing) is a
/// C-level `sigsetjmp`/`siglongjmp`, not a Rust panic -- it does not run
/// Rust `Drop` glue for stack frames between the raise point and whatever
/// `PG_TRY` catches it, because a raw longjmp does not walk Rust's unwind
/// tables at all. This guard's `Drop` impl is therefore not reached in
/// that case. Balance across *that* path is guaranteed by a different,
/// independent mechanism instead: PostgreSQL's own transaction/
/// subtransaction abort processing (`AtSubAbort_Snapshot` /
/// `AtEOXact_Snapshot`) unconditionally resets the active-snapshot stack
/// to what it was before the (sub)transaction began, regardless of what
/// was pushed inside it -- this is exactly why ordinary PostgreSQL C code
/// does not defensively `PG_CATCH` + pop around every `PushActiveSnapshot`
/// call either.
///
/// This places a hard, binding requirement on every caller of this guard,
/// present and future (including the Stage 7 online-snapshot copier this
/// primitive exists to support): an error raised while this guard is live
/// must always be allowed to propagate all the way to a (sub)transaction
/// abort. Never catch it and continue executing further statements in the
/// *same* (sub)transaction without first re-establishing a clean snapshot
/// stack (e.g. via an explicit subtransaction boundary). The design this
/// guard is built for already satisfies this: the online-snapshot copy
/// transaction either completes normally or aborts as a whole on any
/// error (plan §1h's failure table), with the reconciler resuming in a
/// fresh transaction afterward -- never a catch-and-continue within the
/// same transaction.
pub(crate) struct PushedSnapshotGuard;

impl PushedSnapshotGuard {
    /// # Safety
    /// Must be called from a backend with a valid transaction context in
    /// which `GetTransactionSnapshot()` is legal to call (i.e. inside an
    /// active transaction, not during startup/shutdown).
    pub(crate) unsafe fn new() -> Self {
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        PushedSnapshotGuard
    }
}

impl Drop for PushedSnapshotGuard {
    fn drop(&mut self) {
        unsafe { pg_sys::PopActiveSnapshot() };
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use std::io::Cursor;

    const DEFAULT_MAX_ROW_BYTES: i64 = 64 * 1024 * 1024;

    /// Execute a read-only SELECT and return, for each result row, the raw
    /// Datum (or None if NULL) for each of the first `ncols` columns in
    /// tupdesc order. Bypasses pgrx's higher-level `SpiHeapTupleData`
    /// wrapper, which only exposes compile-time-typed access
    /// (`.value::<T>()`) and has no public accessor for a row's underlying
    /// dynamically-typed Datum -- exactly what reading an arbitrary table's
    /// arbitrary column types needs. Mirrors, at the same level, what
    /// `SpiHeapTupleData::new` already does internally
    /// (`SPI_getbinval`/`SPI_gettypeid` against the global `SPI_tuptable`).
    ///
    /// # Safety
    /// Must be called from within an already-connected SPI context (inside
    /// `Spi::connect`/`connect_mut`).
    unsafe fn spi_select_raw_rows(query: &str, ncols: usize) -> Vec<Vec<Option<pg_sys::Datum>>> {
        // A real heap scan (unlike a constant expression or an aggregate's
        // single output row) needs a *current* MVCC snapshot to see rows
        // committed earlier in this same transaction. ActiveSnapshotSet()
        // being true is not sufficient proof of that -- an older snapshot
        // predating this transaction's own INSERTs can still be active.
        // Explicitly push a fresh GetTransactionSnapshot() (reflecting
        // every command run so far in this transaction, per ordinary
        // command-counter semantics) around the raw SPI_execute_with_args
        // call, exactly as the executor would for a normal query, rather
        // than relying on whatever snapshot happened to already be active.
        // PushedSnapshotGuard (see its doc comment for exactly what it does
        // and does not cover) guarantees the matching pop runs even if a
        // Rust panic unwinds before the end of this function.
        let _snapshot_guard = PushedSnapshotGuard::new();
        let c_query = std::ffi::CString::new(query).expect("query must not contain a NUL byte");
        let rc = pg_sys::SPI_execute_with_args(
            c_query.as_ptr(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null(),
            true,
            0,
        );
        assert_eq!(
            rc,
            pg_sys::SPI_OK_SELECT as i32,
            "SPI_execute_with_args did not return SPI_OK_SELECT for {query:?}"
        );
        let tuptable = pg_sys::SPI_tuptable;
        assert!(
            !tuptable.is_null(),
            "SPI_tuptable was null after a successful SELECT"
        );
        // pgrx's own SpiClient::prepare_tuple_table uses the global
        // SPI_processed for row count, not SPITupleTable.numvals -- mirrored
        // here for the same reason.
        let numvals = pg_sys::SPI_processed as usize;
        let tupdesc = (*tuptable).tupdesc;
        let vals = (*tuptable).vals;
        let mut rows = Vec::with_capacity(numvals);
        for i in 0..numvals {
            let htup = *vals.add(i);
            let mut row = Vec::with_capacity(ncols);
            for col in 1..=ncols as i32 {
                let mut is_null = false;
                let datum = pg_sys::SPI_getbinval(htup, tupdesc, col, &mut is_null);
                row.push(if is_null { None } else { Some(datum) });
            }
            rows.push(row);
        }
        rows
    }

    fn sample_columns() -> Vec<ColumnDescriptor> {
        vec![
            ColumnDescriptor {
                attnum: 1,
                atttypid: pg_sys::INT4OID.to_u32(),
                atttypmod: -1,
                attcollation: 0,
                attnotnull: true,
                attidentity: 0,
                name: "id".to_string(),
            },
            ColumnDescriptor {
                attnum: 2,
                atttypid: pg_sys::TEXTOID.to_u32(),
                atttypmod: -1,
                attcollation: 100,
                attnotnull: false,
                attidentity: 0,
                name: "payload".to_string(),
            },
        ]
    }

    #[pg_test]
    fn test_header_round_trip() {
        let cols = sample_columns();
        let mut buf = Vec::new();
        write_header(&mut buf, &cols).unwrap();
        let mut cur = Cursor::new(buf);
        let decoded = read_header(&mut cur).unwrap();
        assert_eq!(decoded, cols);
    }

    #[pg_test]
    fn test_header_rejects_bad_magic() {
        let mut buf = b"NOTMAGIC".to_vec();
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut cur = Cursor::new(buf);
        assert!(read_header(&mut cur).is_err());
    }

    #[pg_test]
    fn test_row_round_trip_with_nulls() {
        let values: Vec<Option<Vec<u8>>> =
            vec![Some(vec![1, 2, 3]), None, Some(vec![]), Some(vec![9; 100])];
        let mut buf = Vec::new();
        write_row(&mut buf, &values).unwrap();
        let mut cur = Cursor::new(buf);
        let decoded = read_row(&mut cur, values.len(), DEFAULT_MAX_ROW_BYTES).unwrap();
        assert_eq!(decoded, values);
    }

    #[pg_test]
    fn test_row_rejects_truncated_rowlen() {
        let values: Vec<Option<Vec<u8>>> = vec![Some(vec![1, 2, 3, 4, 5])];
        let mut buf = Vec::new();
        write_row(&mut buf, &values).unwrap();
        // Corrupt: shrink the declared ROWLEN by one byte relative to the
        // actual body that follows it, without touching the body itself.
        let declared = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        buf[0..4].copy_from_slice(&(declared - 1).to_be_bytes());
        let mut cur = Cursor::new(buf);
        let err = read_row(&mut cur, values.len(), DEFAULT_MAX_ROW_BYTES).unwrap_err();
        // The shrunk ROWLEN is still large enough for read_exact to succeed,
        // so this is caught by the body-internal consistency check (a
        // VALLEN pointing past the now-short body), not by hitting EOF.
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[pg_test]
    fn test_row_rejects_over_max_row_bytes() {
        let values: Vec<Option<Vec<u8>>> = vec![Some(vec![7; 1000])];
        let mut buf = Vec::new();
        write_row(&mut buf, &values).unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_row(&mut cur, values.len(), 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[pg_test]
    fn test_trailer_round_trip() {
        let mut buf = Vec::new();
        write_trailer(&mut buf, 42).unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(read_trailer(&mut cur).unwrap(), 42);
    }

    /// Proves `PushedSnapshotGuard`'s `Drop` runs even when a Rust panic
    /// unwinds between construction and the point where a caller would
    /// otherwise have popped it explicitly -- the case this guard exists
    /// to cover (a genuine PostgreSQL `ERROR`/longjmp is a different,
    /// independently-sound path; see the guard's own doc comment). A thin
    /// wrapper records, via a plain (non-atomic -- single-threaded,
    /// same-backend) flag, whether the pop actually executed, so this test
    /// observes the mechanism directly rather than inferring it indirectly
    /// from unrelated later SPI behavior.
    #[pg_test]
    fn test_pushed_snapshot_guard_pops_across_rust_panic() {
        struct RecordingGuard {
            inner: PushedSnapshotGuard,
            popped: *mut bool,
        }
        impl Drop for RecordingGuard {
            fn drop(&mut self) {
                // `inner`'s own Drop (the real PopActiveSnapshot call) runs
                // after this body per Rust's field-drop order, but the
                // side effect we need to observe is "did this guard's Drop
                // run at all" -- sufficient to prove the panic did not
                // bypass Drop glue for this frame (which is exactly what a
                // raw C longjmp would do, and exactly what a Rust panic
                // does not do).
                unsafe { *self.popped = true };
                let _ = &self.inner;
            }
        }

        let mut popped = false;
        let popped_ptr: *mut bool = &mut popped;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            let _guard = RecordingGuard {
                inner: PushedSnapshotGuard::new(),
                popped: popped_ptr,
            };
            panic!("intentional test panic while a snapshot is pushed");
        }));
        assert!(result.is_err(), "the panic must have actually unwound");
        assert!(
            popped,
            "PushedSnapshotGuard::drop (and therefore PopActiveSnapshot) must run \
             even when a Rust panic unwinds through it"
        );

        // Same transaction, immediately after: a real SPI query must still
        // work correctly, proving no corrupted/imbalanced state was left
        // for subsequent statements in this backend.
        Spi::connect(|_c| {
            let rows = unsafe { spi_select_raw_rows("SELECT 1", 1) };
            assert_eq!(rows.len(), 1);
        });
    }

    #[pg_test]
    fn test_allowlist_rejects_unknown_type() {
        // A made-up, definitely-not-allowlisted OID.
        let bogus = pg_sys::Oid::from(999999u32);
        assert!(resolve_send_info(bogus).is_err());
        assert!(resolve_receive_info(bogus).is_err());
    }

    /// End-to-end: create a real table with real data, pull real Datums via
    /// SPI, encode every allowlisted-type column through this module's
    /// send/receive plumbing plus the frame writer/reader, decode them back,
    /// and prove correctness by re-encoding the decoded Datum through the
    /// exact same send function and comparing bytes: if
    /// `encode(decode(encode(original))) == encode(original)`, the decoded
    /// Datum is canonically identical to the original for every type's own
    /// binary wire representation -- a self-contained, real-FFI proof that
    /// does not depend on constructing dynamic SQL parameters from a
    /// runtime-typed raw Datum (pgrx's `DatumWithOid` is built for
    /// compile-time-typed values via `IntoDatum`, not for rebinding an
    /// already-decoded Datum whose type is only known via an `Oid` learned
    /// at runtime).
    #[pg_test]
    fn test_full_row_round_trip_against_real_table() {
        Spi::run(
            "CREATE TABLE ext_zstd_fmt_rt (
                id int4 NOT NULL,
                t text,
                n numeric,
                b bytea,
                ok bool,
                ts timestamptz
             );
             INSERT INTO ext_zstd_fmt_rt VALUES
                (1, 'hello world', 123.456, '\\xdeadbeef', true, '2026-01-01T00:00:00Z'),
                (2, NULL, NULL, NULL, NULL, NULL),
                (3, repeat('x', 5000), -99999.5, decode('00ff00ff', 'hex'), false, now());",
        )
        .unwrap();

        let oid: pg_sys::Oid = Spi::get_one("SELECT 'ext_zstd_fmt_rt'::regclass::oid")
            .unwrap()
            .unwrap();

        // Column contract straight from pg_attribute, mirroring the exact
        // predicate the existing restore path already uses elsewhere
        // (attnum > 0, not dropped, not generated).
        let columns: Vec<ColumnDescriptor> = Spi::connect(|c| {
            let table = c
                .select(
                    "SELECT a.attnum, a.atttypid, a.atttypmod, a.attcollation,
                            a.attnotnull, a.attidentity::text, a.attname::text
                     FROM pg_attribute a
                     WHERE a.attrelid = $1 AND a.attnum > 0
                       AND NOT a.attisdropped AND a.attgenerated = ''
                     ORDER BY a.attnum",
                    None,
                    &[oid.into()],
                )
                .unwrap();
            table
                .map(|row| ColumnDescriptor {
                    attnum: row.get::<i16>(1).unwrap().unwrap() as i32,
                    atttypid: row.get::<pg_sys::Oid>(2).unwrap().unwrap().to_u32(),
                    atttypmod: row.get::<i32>(3).unwrap().unwrap(),
                    attcollation: row.get::<pg_sys::Oid>(4).unwrap().unwrap().to_u32(),
                    attnotnull: row.get::<bool>(5).unwrap().unwrap(),
                    attidentity: row
                        .get::<String>(6)
                        .unwrap()
                        .unwrap()
                        .bytes()
                        .next()
                        .unwrap_or(0),
                    name: row.get::<String>(7).unwrap().unwrap(),
                })
                .collect()
        });
        assert_eq!(columns.len(), 6);

        let send_infos: Vec<TypeSendInfo> = columns
            .iter()
            .map(|c| resolve_send_info(pg_sys::Oid::from(c.atttypid)).unwrap())
            .collect();
        let receive_infos: Vec<TypeReceiveInfo> = columns
            .iter()
            .map(|c| resolve_receive_info(pg_sys::Oid::from(c.atttypid)).unwrap())
            .collect();

        // Encode every row from a real SPI select into the frame format.
        // Raw SPI_getbinval extraction, not pgrx's SpiHeapTupleData wrapper:
        // that wrapper only exposes compile-time-typed access
        // (`.value::<T>()`) with no public accessor for a row's underlying
        // dynamically-typed Datum -- exactly what's needed here, since the
        // column types are only known at runtime (they came from
        // pg_attribute above). This mirrors, at the same level, what
        // SpiHeapTupleData::new already does internally.
        let mut buf = Vec::new();
        write_header(&mut buf, &columns).unwrap();
        let row_count = Spi::connect(|_c| {
            let raw_rows = unsafe {
                spi_select_raw_rows("SELECT * FROM ext_zstd_fmt_rt ORDER BY id", columns.len())
            };
            let mut n = 0u64;
            for raw_row in &raw_rows {
                let mut values = Vec::with_capacity(columns.len());
                for (datum, send) in raw_row.iter().zip(&send_infos) {
                    values.push(datum.map(|d| unsafe { encode_datum(d, send) }));
                }
                write_row(&mut buf, &values).unwrap();
                n += 1;
            }
            n
        });
        write_trailer(&mut buf, row_count).unwrap();
        assert_eq!(row_count, 3);

        // Decode every row back and prove canonical correctness: decode
        // then re-encode must reproduce the exact original bytes, for every
        // non-null value of every column, in every row.
        let mut cur = Cursor::new(buf);
        let decoded_columns = read_header(&mut cur).unwrap();
        assert_eq!(decoded_columns, columns);
        let mut total_non_null_values_checked = 0u32;
        for _ in 0..row_count {
            let values = read_row(&mut cur, columns.len(), 64 * 1024 * 1024).unwrap();
            for ((original, recv), send) in values.iter().zip(&receive_infos).zip(&send_infos) {
                if let Some(original_bytes) = original {
                    let decoded_datum = unsafe { decode_datum(original_bytes, recv, -1) };
                    let re_encoded = unsafe { encode_datum(decoded_datum, send) };
                    assert_eq!(
                        &re_encoded, original_bytes,
                        "decode-then-re-encode must reproduce the exact original bytes"
                    );
                    total_non_null_values_checked += 1;
                }
            }
        }
        let trailer_count = read_trailer(&mut cur).unwrap();
        assert_eq!(trailer_count, row_count);
        // 3 rows x 6 columns, minus the 5 NULLs in row 2 (every column
        // except `id`, which is NOT NULL) -- confirms NULLs were correctly
        // skipped, not merely that some values were checked.
        assert_eq!(total_non_null_values_checked, 3 * 6 - 5);
    }
}

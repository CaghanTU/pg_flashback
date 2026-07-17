/// pg_flashback WAL-based capture via logical decoding output plugin.
///
/// Exports `_PG_output_plugin_init` so that PostgreSQL can load pg_flashback
/// as a logical decoding output plugin. The background worker creates a
/// replication slot using this plugin and consumes changes via
/// `pg_logical_slot_get_changes()`.
///
/// Each committed DML change is emitted as a single JSON line:
///   {"op":"INSERT","schema":"public","table":"orders","oid":16384,"new":{"id":1,"name":"John"}}
///   {"op":"UPDATE","schema":"public","table":"orders","oid":16384,"old":{"id":1},"new":{"id":1,"name":"Jane"}}
///   {"op":"DELETE","schema":"public","table":"orders","oid":16384,"old":{"id":1,"name":"John"}}
///   {"op":"TRUNCATE","schema":"public","table":"orders","oid":16384}
use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::pg_sys::*;
use std::collections::HashSet;
use std::ffi::CStr;
use std::sync::{Mutex, OnceLock};

static EMITTED_TRANSACTIONS: OnceLock<Mutex<HashSet<TransactionId>>> = OnceLock::new();

struct DecoderState {
    tracked_relations: HashSet<u32>,
    metadata_only: bool,
}

fn emitted_transactions() -> &'static Mutex<HashSet<TransactionId>> {
    EMITTED_TRANSACTIONS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn mark_transaction_emitted(xid: TransactionId) {
    emitted_transactions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(xid);
}

fn take_transaction_emitted(xid: TransactionId) -> bool {
    emitted_transactions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&xid)
}

fn is_internal_schema(schema: &str) -> bool {
    schema == "flashback"
        || schema == "pg_catalog"
        || schema == "information_schema"
        || schema.starts_with("pg_toast")
        || schema.starts_with("pg_temp_")
}

fn parse_tracked_oids(value: &str) -> HashSet<u32> {
    value
        .split(',')
        .filter_map(|part| {
            let oid = part.trim().parse::<u32>().ok()?;
            (oid != pg_sys::InvalidOid.to_u32()).then_some(oid)
        })
        .collect()
}

unsafe fn decoder_options(options: *mut pg_sys::List) -> DecoderState {
    let mut tracked_relations = HashSet::new();
    let mut metadata_only = false;

    if !options.is_null() {
        let options_ref = unsafe { &*options };
        if options_ref.length > 0 && !options_ref.elements.is_null() {
            for index in 0..options_ref.length as usize {
                let cell = unsafe { &*options_ref.elements.add(index) };
                let elem = cell.ptr_value.cast::<pg_sys::DefElem>();
                if elem.is_null() || unsafe { (*elem).defname }.is_null() {
                    continue;
                }

                let name = unsafe { CStr::from_ptr((*elem).defname) }
                    .to_str()
                    .unwrap_or("");
                let value_ptr = unsafe { pg_sys::defGetString(elem) };
                if value_ptr.is_null() {
                    continue;
                }
                let value = unsafe { CStr::from_ptr(value_ptr) }.to_str().unwrap_or("");
                match name {
                    "tracked_oids" => tracked_relations.extend(parse_tracked_oids(value)),
                    "metadata_only" => metadata_only = value == "true",
                    _ => {}
                }
            }
        }
    }

    DecoderState {
        tracked_relations,
        metadata_only,
    }
}

unsafe fn relation_is_tracked(ctx: *mut LogicalDecodingContext, oid: u32) -> bool {
    if ctx.is_null() || unsafe { (*ctx).output_plugin_private }.is_null() {
        return false;
    }

    let state = unsafe { &*((*ctx).output_plugin_private.cast::<DecoderState>()) };
    state.tracked_relations.contains(&oid)
}

unsafe fn metadata_only(ctx: *mut LogicalDecodingContext) -> bool {
    if ctx.is_null() || unsafe { (*ctx).output_plugin_private }.is_null() {
        return false;
    }

    let state = unsafe { &*((*ctx).output_plugin_private.cast::<DecoderState>()) };
    state.metadata_only
}

// ─── Output Plugin Entry Point ──────────────────────────────────────

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut OutputPluginCallbacks) {
    let cb = unsafe { &mut *cb };
    cb.startup_cb = Some(fb_decode_startup);
    cb.begin_cb = Some(fb_decode_begin);
    cb.change_cb = Some(fb_decode_change);
    cb.commit_cb = Some(fb_decode_commit);
    cb.message_cb = Some(fb_decode_message);
    cb.shutdown_cb = Some(fb_decode_shutdown);
}

// ─── Startup ────────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_startup(
    ctx: *mut LogicalDecodingContext,
    options: *mut OutputPluginOptions,
    _is_init: bool,
) {
    let opt = unsafe { &mut *options };
    opt.output_type = OutputPluginOutputType::OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    opt.receive_rewrites = false;
    let ctx_ref = unsafe { &mut *ctx };
    let state = unsafe { decoder_options(ctx_ref.output_plugin_options) };
    ctx_ref.output_plugin_private = Box::into_raw(Box::new(state)).cast();
    emitted_transactions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

// ─── Begin Transaction ──────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_begin(
    _ctx: *mut LogicalDecodingContext,
    _txn: *mut ReorderBufferTXN,
) {
    // BEGIN used to be emitted eagerly for every decoded transaction. That
    // makes the consumer's own flashback.* metadata writes generate another
    // output row forever. A transaction is now visible only if a user-table
    // change or pg_flashback logical DDL message is emitted below.
}

// ─── DML Change (INSERT / UPDATE / DELETE) ──────────────────────────

unsafe extern "C-unwind" fn fb_decode_change(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
) {
    let change_ref = unsafe { &*change };
    let action = change_ref.action;

    let op = match action {
        ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => "INSERT",
        ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => "UPDATE",
        ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => "DELETE",
        _ => return,
    };

    let rel = unsafe { &*relation };
    let rd_rel = unsafe { &*rel.rd_rel };
    let oid: u32 = rd_rel.oid.into();

    // The caller supplies the exact relation set that is recoverable in the
    // current generation graph.  Reject unrelated relations before namespace
    // lookup or tuple-to-JSON conversion: otherwise a large, untracked table
    // would still consume decoder CPU and memory even though SQL discarded it.
    if !unsafe { relation_is_tracked(ctx, oid) } {
        return;
    }

    let nsp_oid = rd_rel.relnamespace;
    let nsp_name_ptr = unsafe { get_namespace_name(nsp_oid) };
    let schema: std::string::String = if nsp_name_ptr.is_null() {
        "public".into()
    } else {
        unsafe { CStr::from_ptr(nsp_name_ptr) }
            .to_str()
            .unwrap_or("public")
            .to_owned()
    };

    if is_internal_schema(&schema) {
        return;
    }

    let table_name_ptr = rd_rel.relname.data.as_ptr();
    let table: std::string::String = unsafe { CStr::from_ptr(table_name_ptr) }
        .to_str()
        .unwrap_or("unknown")
        .to_owned();

    let metadata_only = unsafe { metadata_only(ctx) };
    let tupdesc = rel.rd_att;
    let reloid: pg_sys::Oid = rd_rel.oid;
    let tp = unsafe { change_ref.data.tp };

    // PG15/PG16: oldtuple/newtuple are *mut ReorderBufferTupleBuf; extract inner HeapTupleData.
    // PG17+: they are already HeapTuple (*mut HeapTupleData).
    let old_json = if !metadata_only && (op == "UPDATE" || op == "DELETE") && !tp.oldtuple.is_null()
    {
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        let ht: HeapTuple = unsafe { &raw mut (*tp.oldtuple).tuple };
        #[cfg(not(any(feature = "pg15", feature = "pg16")))]
        let ht: HeapTuple = tp.oldtuple;
        Some(unsafe { heap_tuple_to_json(ht, tupdesc, reloid) })
    } else {
        None
    };

    let new_json = if !metadata_only && (op == "INSERT" || op == "UPDATE") && !tp.newtuple.is_null()
    {
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        let ht: HeapTuple = unsafe { &raw mut (*tp.newtuple).tuple };
        #[cfg(not(any(feature = "pg15", feature = "pg16")))]
        let ht: HeapTuple = tp.newtuple;
        Some(unsafe { heap_tuple_to_json(ht, tupdesc, reloid) })
    } else {
        None
    };

    let xid = unsafe { (*txn).xid };
    mark_transaction_emitted(xid);
    let mut json = std::string::String::with_capacity(256);
    json.push_str("{\"op\":\"");
    json.push_str(op);
    json.push_str("\",\"schema\":\"");
    json_escape_into(&mut json, &schema);
    json.push_str("\",\"table\":\"");
    json_escape_into(&mut json, &table);
    json.push_str(&format!("\",\"oid\":{oid},\"xid\":{xid}"));
    if let Some(ref old) = old_json {
        json.push_str(",\"old\":");
        json.push_str(old);
    }
    if let Some(ref new) = new_json {
        json.push_str(",\"new\":");
        json.push_str(new);
    }
    json.push('}');

    let c_json = std::ffi::CString::new(json).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_json.as_ptr());
        OutputPluginWrite(ctx, true);
    }
}

// ─── Commit ─────────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_commit(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let txn_ref = unsafe { &*txn };
    let xid = txn_ref.xid;
    if !take_transaction_emitted(xid) {
        return;
    }
    let commit_time = txn_ref.xact_time.commit_time;

    let lsn_str = format!("{:X}/{:X}", commit_lsn >> 32, commit_lsn & 0xFFFFFFFF);
    let json = format!("{{\"commit\":{xid},\"lsn\":\"{lsn_str}\",\"commit_time\":{commit_time}}}");
    let c_json = std::ffi::CString::new(json).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_json.as_ptr());
        OutputPluginWrite(ctx, true);
    }
}

// ─── Transactional Commit Marker ──────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_message(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    _message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const ::core::ffi::c_char,
    _message_size: Size,
    _message: *const ::core::ffi::c_char,
) {
    if prefix.is_null() || !transactional || txn.is_null() {
        return;
    }

    let prefix_str = unsafe { CStr::from_ptr(prefix) }.to_str().unwrap_or("");

    if prefix_str != "pg_flashback" {
        return;
    }

    // pg_logical_emit_message() is executable by PUBLIC, so its body is
    // untrusted input.  Never forward it.  Legitimate DDL payload lives in
    // flashback.pending_wal_events, which ordinary roles cannot write.  This
    // fixed marker only makes the transaction's real COMMIT record visible.
    let xid = unsafe { (*txn).xid };
    mark_transaction_emitted(xid);
    let marker = format!("{{\"marker\":{xid}}}");
    let c_msg = std::ffi::CString::new(marker).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_msg.as_ptr());
        OutputPluginWrite(ctx, true);
    }
}

// ─── Shutdown ───────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_shutdown(ctx: *mut LogicalDecodingContext) {
    emitted_transactions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();

    if !ctx.is_null() && !unsafe { (*ctx).output_plugin_private }.is_null() {
        let state = unsafe { (*ctx).output_plugin_private.cast::<DecoderState>() };
        unsafe { drop(Box::from_raw(state)) };
        unsafe { (*ctx).output_plugin_private = std::ptr::null_mut() };
    }
}

// ─── Utility: HeapTuple → JSON string ───────────────────────────────

unsafe fn heap_tuple_to_json(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
    #[cfg_attr(not(feature = "pg18"), allow(unused_variables))] reloid: pg_sys::Oid,
) -> std::string::String {
    let td = unsafe { &*tupdesc };
    let natts = td.natts as usize;

    let mut values: Vec<Datum> = vec![Datum::from(0); natts];
    let mut nulls: Vec<bool> = vec![false; natts];

    unsafe {
        heap_deform_tuple(tuple, tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    }

    let mut json = std::string::String::with_capacity(128);
    json.push('{');
    let mut first = true;

    for i in 0..natts {
        // PG18: TupleDescData no longer embeds Form_pg_attribute — use compact_attrs for
        // dropped-column check, and catalog functions for name + type OID.
        // PG15/16/17: attrs flexible array has full FormData_pg_attribute.
        #[cfg(feature = "pg18")]
        let attisdropped = unsafe { td.compact_attrs.as_slice(natts)[i].attisdropped };
        #[cfg(not(feature = "pg18"))]
        let attisdropped = unsafe { (*td.attrs.as_ptr().add(i)).attisdropped };

        if attisdropped {
            continue;
        }

        #[cfg(feature = "pg18")]
        let (col_name_owned, atttypid) = {
            let attnum = (i + 1) as pg_sys::AttrNumber;
            let name_ptr = unsafe { pg_sys::get_attname(reloid, attnum, false) };
            let name = if name_ptr.is_null() {
                "?".to_owned()
            } else {
                let s = unsafe { CStr::from_ptr(name_ptr) }
                    .to_str()
                    .unwrap_or("?")
                    .to_owned();
                unsafe { pfree(name_ptr as *mut _) };
                s
            };
            let typid = unsafe { pg_sys::get_atttype(reloid, attnum) };
            (name, typid)
        };
        #[cfg(not(feature = "pg18"))]
        let (col_name_owned, atttypid) = {
            let attr = unsafe { &*td.attrs.as_ptr().add(i) };
            let name = unsafe {
                CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_str()
                    .unwrap_or("?")
                    .to_owned()
            };
            (name, attr.atttypid)
        };

        let col_name: &str = &col_name_owned;

        if !first {
            json.push(',');
        }
        first = false;

        json.push('"');
        json_escape_into(&mut json, col_name);
        json.push_str("\":");

        if nulls[i] {
            json.push_str("null");
        } else {
            let mut typoutput: Oid = pg_sys::InvalidOid;
            let mut typvarlena: bool = false;
            unsafe {
                getTypeOutputInfo(atttypid, &mut typoutput, &mut typvarlena);
            }
            let val_cstr = unsafe { OidOutputFunctionCall(typoutput, values[i]) };
            let val_str = unsafe { CStr::from_ptr(val_cstr) }.to_str().unwrap_or("");

            push_json_value(&mut json, val_str, atttypid);

            unsafe { pfree(val_cstr as *mut _) };
        }
    }

    json.push('}');
    json
}

fn is_numeric_type(typoid: Oid) -> bool {
    typoid == pg_sys::INT2OID
        || typoid == pg_sys::INT4OID
        || typoid == pg_sys::INT8OID
        || typoid == pg_sys::FLOAT4OID
        || typoid == pg_sys::FLOAT8OID
        || typoid == pg_sys::NUMERICOID
        || typoid == pg_sys::OIDOID
        || typoid == pg_sys::BOOLOID
}

/// Append one column value in valid JSON. Numeric types are emitted bare,
/// EXCEPT the special values NaN / Infinity / -Infinity, which JSON has no
/// literal for — those are emitted as quoted strings (replay casts text
/// back to the column type, so 'NaN'::numeric round-trips losslessly).
fn push_json_value(buf: &mut std::string::String, val_str: &str, typoid: Oid) {
    if is_numeric_type(typoid) {
        if typoid == pg_sys::BOOLOID {
            buf.push_str(if val_str == "t" { "true" } else { "false" });
            return;
        }
        if !matches!(val_str, "NaN" | "Infinity" | "-Infinity") && !val_str.is_empty() {
            buf.push_str(val_str);
            return;
        }
    }
    buf.push('"');
    json_escape_into(buf, val_str);
    buf.push('"');
}

fn json_escape_into(buf: &mut std::string::String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => buf.push(c),
        }
    }
}

#[cfg(test)]
mod json_format_tests {
    use super::{json_escape_into, parse_tracked_oids, push_json_value};
    use pgrx::pg_sys;

    fn escaped(s: &str) -> String {
        let mut buf = String::new();
        json_escape_into(&mut buf, s);
        buf
    }

    fn value(val: &str, typoid: pg_sys::Oid) -> String {
        let mut buf = String::new();
        push_json_value(&mut buf, val, typoid);
        buf
    }

    #[test]
    fn escapes_quoted_identifiers() {
        // CREATE TABLE "we""ird" is legal — its relname contains a quote
        assert_eq!(escaped(r#"we"ird"#), r#"we\"ird"#);
        assert_eq!(escaped(r"back\slash"), r"back\\slash");
        assert_eq!(escaped("tab\there"), "tab\\there");
    }

    #[test]
    fn nan_and_infinity_are_quoted() {
        assert_eq!(value("NaN", pg_sys::NUMERICOID), "\"NaN\"");
        assert_eq!(value("Infinity", pg_sys::FLOAT8OID), "\"Infinity\"");
        assert_eq!(value("-Infinity", pg_sys::FLOAT4OID), "\"-Infinity\"");
        assert_eq!(value("", pg_sys::NUMERICOID), "\"\"");
    }

    #[test]
    fn normal_values_keep_their_shape() {
        assert_eq!(value("42", pg_sys::INT4OID), "42");
        assert_eq!(value("-1.5", pg_sys::NUMERICOID), "-1.5");
        assert_eq!(value("t", pg_sys::BOOLOID), "true");
        assert_eq!(value("f", pg_sys::BOOLOID), "false");
        assert_eq!(value("hello \"x\"", pg_sys::TEXTOID), "\"hello \\\"x\\\"\"");
    }

    #[test]
    fn tracked_oid_option_is_strict_and_deduplicated() {
        let parsed = parse_tracked_oids("16384, 16385,16384,invalid,0");
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&16384));
        assert!(parsed.contains(&16385));
        assert!(!parsed.contains(&0));
    }
}

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
use std::ffi::CStr;

// ─── Output Plugin Entry Point ──────────────────────────────────────

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut OutputPluginCallbacks) {
    let cb = unsafe { &mut *cb };
    cb.startup_cb = Some(fb_decode_startup);
    cb.begin_cb = Some(fb_decode_begin);
    cb.change_cb = Some(fb_decode_change);
    cb.truncate_cb = Some(fb_decode_truncate);
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
    ctx_ref.output_plugin_private = std::ptr::null_mut();
}

// ─── Begin Transaction ──────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_begin(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
) {
    let txn_ref = unsafe { &*txn };
    let xid = txn_ref.xid;

    let msg = format!("{{\"begin\":{xid}}}");
    let c_msg = std::ffi::CString::new(msg).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_msg.as_ptr());
        OutputPluginWrite(ctx, true);
    }
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

    let table_name_ptr = rd_rel.relname.data.as_ptr();
    let table: std::string::String = unsafe { CStr::from_ptr(table_name_ptr) }
        .to_str()
        .unwrap_or("unknown")
        .to_owned();

    let tupdesc = rel.rd_att;
    let reloid: pg_sys::Oid = rd_rel.oid;
    let tp = unsafe { change_ref.data.tp };

    // PG15/PG16: oldtuple/newtuple are *mut ReorderBufferTupleBuf; extract inner HeapTupleData.
    // PG17+: they are already HeapTuple (*mut HeapTupleData).
    let old_json = if (op == "UPDATE" || op == "DELETE") && !tp.oldtuple.is_null() {
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        let ht: HeapTuple = unsafe { &raw mut (*tp.oldtuple).tuple };
        #[cfg(not(any(feature = "pg15", feature = "pg16")))]
        let ht: HeapTuple = tp.oldtuple;
        Some(unsafe { heap_tuple_to_json(ht, tupdesc, reloid) })
    } else {
        None
    };

    let new_json = if (op == "INSERT" || op == "UPDATE") && !tp.newtuple.is_null() {
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        let ht: HeapTuple = unsafe { &raw mut (*tp.newtuple).tuple };
        #[cfg(not(any(feature = "pg15", feature = "pg16")))]
        let ht: HeapTuple = tp.newtuple;
        Some(unsafe { heap_tuple_to_json(ht, tupdesc, reloid) })
    } else {
        None
    };

    let xid = unsafe { (*txn).xid };
    let mut json = std::string::String::with_capacity(256);
    json.push_str(&format!(
        "{{\"op\":\"{op}\",\"schema\":\"{schema}\",\"table\":\"{table}\",\"oid\":{oid},\"xid\":{xid}"
    ));
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

// ─── TRUNCATE ───────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_truncate(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    nrelations: ::core::ffi::c_int,
    relations: *mut Relation,
    _change: *mut ReorderBufferChange,
) {
    let xid = unsafe { (*txn).xid };

    for i in 0..nrelations as usize {
        let relation = unsafe { *relations.add(i) };
        let rel = unsafe { &*relation };
        let rd_rel = unsafe { &*rel.rd_rel };
        let oid: u32 = rd_rel.oid.into();

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

        let table_name_ptr = rd_rel.relname.data.as_ptr();
        let table: std::string::String = unsafe { CStr::from_ptr(table_name_ptr) }
            .to_str()
            .unwrap_or("unknown")
            .to_owned();

        let json = format!(
            "{{\"op\":\"TRUNCATE\",\"schema\":\"{schema}\",\"table\":\"{table}\",\"oid\":{oid},\"xid\":{xid}}}"
        );
        let c_json = std::ffi::CString::new(json).unwrap_or_default();
        unsafe {
            OutputPluginPrepareWrite(ctx, true);
            let buf = (*ctx).out;
            appendStringInfoString(buf, c_json.as_ptr());
            OutputPluginWrite(ctx, true);
        }
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

// ─── Logical Message (DDL events via pg_logical_emit_message) ────

unsafe extern "C-unwind" fn fb_decode_message(
    ctx: *mut LogicalDecodingContext,
    _txn: *mut ReorderBufferTXN,
    _message_lsn: XLogRecPtr,
    _transactional: bool,
    prefix: *const ::core::ffi::c_char,
    message_size: Size,
    message: *const ::core::ffi::c_char,
) {
    if prefix.is_null() || message.is_null() || message_size == 0 {
        return;
    }

    let prefix_str = unsafe { CStr::from_ptr(prefix) }.to_str().unwrap_or("");

    if prefix_str != "pg_flashback" {
        return;
    }

    // The message payload is the DDL event JSON — emit it as-is
    let msg_bytes = unsafe { std::slice::from_raw_parts(message.cast::<u8>(), message_size) };
    let msg_str = std::str::from_utf8(msg_bytes).unwrap_or("{}");

    let c_msg = std::ffi::CString::new(msg_str).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_msg.as_ptr());
        OutputPluginWrite(ctx, true);
    }
}

// ─── Shutdown ───────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_shutdown(_ctx: *mut LogicalDecodingContext) {}

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

            if is_numeric_type(atttypid) {
                if atttypid == pg_sys::BOOLOID {
                    json.push_str(if val_str == "t" { "true" } else { "false" });
                } else {
                    json.push_str(val_str);
                }
            } else {
                json.push('"');
                json_escape_into(&mut json, val_str);
                json.push('"');
            }

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

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
    let tp = unsafe { change_ref.data.tp };

    let old_json = if (op == "UPDATE" || op == "DELETE") && !tp.oldtuple.is_null() {
        Some(unsafe { heap_tuple_to_json(tp.oldtuple, tupdesc) })
    } else {
        None
    };

    let new_json = if (op == "INSERT" || op == "UPDATE") && !tp.newtuple.is_null() {
        Some(unsafe { heap_tuple_to_json(tp.newtuple, tupdesc) })
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

    let json = format!(
        "{{\"commit\":{xid},\"lsn\":\"{lsn}\",\"commit_time\":{commit_time}}}",
        lsn = format!("{:X}/{:X}", commit_lsn >> 32, commit_lsn & 0xFFFFFFFF)
    );
    let c_json = std::ffi::CString::new(json).unwrap_or_default();
    unsafe {
        OutputPluginPrepareWrite(ctx, true);
        let buf = (*ctx).out;
        appendStringInfoString(buf, c_json.as_ptr());
        OutputPluginWrite(ctx, true);
    }
}

// ─── Shutdown ───────────────────────────────────────────────────────

unsafe extern "C-unwind" fn fb_decode_shutdown(_ctx: *mut LogicalDecodingContext) {}

// ─── Utility: HeapTuple → JSON string ───────────────────────────────

unsafe fn heap_tuple_to_json(tuple: HeapTuple, tupdesc: TupleDesc) -> std::string::String {
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
        let attr = unsafe { &*td.attrs.as_ptr().add(i) };

        if attr.attisdropped {
            continue;
        }

        let col_name = unsafe {
            CStr::from_ptr(attr.attname.data.as_ptr())
                .to_str()
                .unwrap_or("?")
        };

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
                getTypeOutputInfo(attr.atttypid, &mut typoutput, &mut typvarlena);
            }
            let val_cstr = unsafe { OidOutputFunctionCall(typoutput, values[i]) };
            let val_str = unsafe { CStr::from_ptr(val_cstr) }
                .to_str()
                .unwrap_or("");

            if is_numeric_type(attr.atttypid) {
                if attr.atttypid == pg_sys::BOOLOID {
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

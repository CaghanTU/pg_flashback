use pgrx::pg_sys;
use pgrx::prelude::*;
use std::ffi::{CStr, CString};

pub unsafe extern "C-unwind" fn output_plugin_init(cb: *mut pg_sys::OutputPluginCallbacks) {
    if cb.is_null() {
        return;
    }

    (*cb).startup_cb = Some(tv_startup_cb);
    (*cb).begin_cb = Some(tv_begin_cb);
    (*cb).change_cb = Some(tv_change_cb);
    (*cb).commit_cb = Some(tv_commit_cb);
}

unsafe extern "C-unwind" fn tv_startup_cb(
    _ctx: *mut pg_sys::LogicalDecodingContext,
    options: *mut pg_sys::OutputPluginOptions,
    _is_init: bool,
) {
    if options.is_null() {
        return;
    }

    (*options).output_type = pg_sys::OutputPluginOutputType::OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    (*options).receive_rewrites = false;
    log!("pg_flashback output plugin startup callback initialized");
}

unsafe extern "C-unwind" fn tv_begin_cb(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
) {
    if txn.is_null() {
        return;
    }

    let xid = (*txn).xid;
    log!("pg_flashback BEGIN xid={xid}");
    emit_output_line(ctx, &format!("BEGIN xid={xid}"));
}

unsafe extern "C-unwind" fn tv_change_cb(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
    relation: pg_sys::Relation,
    change: *mut pg_sys::ReorderBufferChange,
) {
    if txn.is_null() || relation.is_null() || change.is_null() {
        return;
    }

    let xid = (*txn).xid;
    let rel_oid = (*relation).rd_id;
    let op = change_action_label((*change).action);
    let (schema_name, table_name) = relation_name_parts(rel_oid);

    let line = format!(
        "CHANGE xid={xid} schema={schema_name} table={table_name} rel_oid={rel_oid} op={op}"
    );
    log!("pg_flashback {line}");
    emit_output_line(ctx, &line);

    let tuple_change = (*change).data.tp;
    match (*change).action {
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => {
            let new_values = tuple_to_text(relation, tuple_change.newtuple);
            let tuple_line = format!(
                "TUPLE xid={xid} schema={schema_name} table={table_name} op=INSERT new={new_values}"
            );
            log!("pg_flashback {tuple_line}");
            emit_output_line(ctx, &tuple_line);
        }
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => {
            let old_values = tuple_to_text(relation, tuple_change.oldtuple);
            let tuple_line = format!(
                "TUPLE xid={xid} schema={schema_name} table={table_name} op=DELETE old={old_values}"
            );
            log!("pg_flashback {tuple_line}");
            emit_output_line(ctx, &tuple_line);
        }
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => {
            let old_values = tuple_to_text(relation, tuple_change.oldtuple);
            let new_values = tuple_to_text(relation, tuple_change.newtuple);
            let tuple_line = format!(
                "TUPLE xid={xid} schema={schema_name} table={table_name} op=UPDATE old={old_values} new={new_values}"
            );
            log!("pg_flashback {tuple_line}");
            emit_output_line(ctx, &tuple_line);
        }
        _ => {}
    }
}

unsafe extern "C-unwind" fn tv_commit_cb(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
    commit_lsn: pg_sys::XLogRecPtr,
) {
    if txn.is_null() {
        return;
    }

    let xid = (*txn).xid;
    log!("pg_flashback COMMIT xid={xid} lsn={commit_lsn}");
    emit_output_line(ctx, &format!("COMMIT xid={xid} lsn={commit_lsn}"));
}

pub(crate) unsafe fn emit_output_line(ctx: *mut pg_sys::LogicalDecodingContext, line: &str) {
    if ctx.is_null() {
        return;
    }

    let sanitized = line.replace('\0', " ");
    let c_line = match CString::new(sanitized) {
        Ok(s) => s,
        Err(_) => return,
    };

    pg_sys::OutputPluginPrepareWrite(ctx, true);
    pg_sys::resetStringInfo((*ctx).out);
    pg_sys::appendStringInfoString((*ctx).out, c_line.as_ptr());
    pg_sys::OutputPluginWrite(ctx, true);
}

unsafe fn relation_name_parts(rel_oid: pg_sys::Oid) -> (String, String) {
    let schema_oid = pg_sys::get_rel_namespace(rel_oid);
    let schema_ptr = pg_sys::get_namespace_name(schema_oid);
    let table_ptr = pg_sys::get_rel_name(rel_oid);

    let schema_name = c_ptr_to_string(schema_ptr);
    let table_name = c_ptr_to_string(table_ptr);

    if !schema_ptr.is_null() {
        pg_sys::pfree(schema_ptr.cast());
    }
    if !table_ptr.is_null() {
        pg_sys::pfree(table_ptr.cast());
    }

    (schema_name, table_name)
}

unsafe fn tuple_to_text(relation: pg_sys::Relation, tuple: pg_sys::HeapTuple) -> String {
    if relation.is_null() || tuple.is_null() {
        return "<null-tuple>".to_string();
    }

    let tupdesc = (*relation).rd_att;
    if tupdesc.is_null() {
        return "<null-tupdesc>".to_string();
    }

    let natts = (*tupdesc).natts as usize;
    if natts == 0 {
        return "{}".to_string();
    }

    let mut values: Vec<pg_sys::Datum> = vec![pg_sys::Datum::from(0usize); natts];
    let mut isnull: Vec<bool> = vec![false; natts];
    pg_sys::heap_deform_tuple(tuple, tupdesc, values.as_mut_ptr(), isnull.as_mut_ptr());

    let attrs = (*tupdesc).attrs.as_ptr();
    let mut items: Vec<String> = Vec::with_capacity(natts);

    for i in 0..natts {
        let attr = attrs.add(i);
        if (*attr).attisdropped {
            continue;
        }

        let column_name = name_data_to_string(&(*attr).attname);
        let value_text = if isnull[i] {
            "NULL".to_string()
        } else {
            datum_to_text(values[i], (*attr).atttypid)
        };

        items.push(format!("{}={}", column_name, value_text));
    }

    format!("{{{}}}", items.join(", "))
}

unsafe fn datum_to_text(value: pg_sys::Datum, typoid: pg_sys::Oid) -> String {
    let mut typ_output = pg_sys::Oid::from(0u32);
    let mut typ_is_varlena = false;
    pg_sys::getTypeOutputInfo(typoid, &mut typ_output, &mut typ_is_varlena);

    let out_ptr = pg_sys::OidOutputFunctionCall(typ_output, value);
    let out = c_ptr_to_string(out_ptr);
    if !out_ptr.is_null() {
        pg_sys::pfree(out_ptr.cast());
    }

    out
}

fn name_data_to_string(name: &pg_sys::NameData) -> String {
    let ptr = name.data.as_ptr();
    if ptr.is_null() {
        return "<null-column-name>".to_string();
    }

    unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() }
}

unsafe fn c_ptr_to_string(ptr: *mut std::os::raw::c_char) -> String {
    if ptr.is_null() {
        return "<null>".to_string();
    }

    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}

fn change_action_label(action: pg_sys::ReorderBufferChangeType::Type) -> &'static str {
    match action {
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => "INSERT",
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => "UPDATE",
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => "DELETE",
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_TRUNCATE => "TRUNCATE",
        _ => "OTHER",
    }
}

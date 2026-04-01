use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use crate::runtime_guard::is_restore_in_progress;
use std::ffi::CStr;
use std::os::raw::c_void;

static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

#[derive(Debug, Clone)]
struct UtilityTarget {
    schema: Option<String>,
    table: String,
}

pub fn install_process_utility_hook() {
    unsafe {
        if let Some(current_hook) = pg_sys::ProcessUtility_hook {
            if current_hook as usize == tv_process_utility_hook as usize {
                return;
            }
        }

        PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(tv_process_utility_hook);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn tv_process_utility_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    // Fast-path: for non-table DDL (CREATE/DROP DATABASE, CREATE ROLE, etc.)
    // skip all pg_flashback logic and delegate directly.  This avoids any
    // interaction between pgrx panic handling and PG's internal longjmp
    // during heavy DDL like CREATE DATABASE.
    let is_table_ddl = !pstmt.is_null() && is_table_related_utility(pstmt);
    if !is_table_ddl {
        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            prev_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        } else {
            pg_sys::standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        }
        return;
    }

    if is_restore_in_progress() {
        log!("pg_flashback DDL_CAPTURE_SKIPPED reason=restore_in_progress");

        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            prev_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
        return;
    }

    let skip_capture = should_skip_capture_for_query(query_string);
    let capture_enabled = if skip_capture {
        false
    } else {
        // Skip extension check for non-table DDL (CREATE/DROP DATABASE, etc.)
        // to avoid SPI/catalog access in unsafe contexts.
        let is_table_ddl = !pstmt.is_null() && is_table_related_utility(pstmt);
        is_table_ddl && is_extension_installed_current_db()
    };

    if !skip_capture
        && capture_enabled
        && context == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL
        && !pstmt.is_null()
    {
        // Do NOT use catch_unwind around SPI calls — it leaks the SPI
        // connection and corrupts the portal snapshot state (PG17 assertion).
        if let Some((event_type, targets)) = parse_pre_utility_targets(pstmt) {
            if let Err(err) = capture_ddl_for_targets(event_type, &targets) {
                log!("pg_flashback DDL_CAPTURE_ERROR stage=pre event_type={} error={err:?}", event_type);
            }
        }
    }

    if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
        prev_hook(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    } else {
        pg_sys::standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    }

    if !skip_capture
        && capture_enabled
        && context == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL
        && !pstmt.is_null()
    {
        if let Some((event_type, targets)) = parse_post_utility_targets(pstmt) {
            if let Err(err) = capture_ddl_for_targets(event_type, &targets) {
                log!("pg_flashback DDL_CAPTURE_ERROR stage=post event_type={} error={err:?}", event_type);
            }
        }
    }
}

fn should_skip_capture_for_query(query_string: *const std::ffi::c_char) -> bool {
    if query_string.is_null() {
        return false;
    }

    let query = unsafe { CStr::from_ptr(query_string).to_string_lossy() };
    query.contains("flashback_restore(") || query.contains("flashback_recreate_table_from_ddl(")
}

fn is_extension_installed_current_db() -> bool {
    unsafe {
        if pg_sys::creating_extension {
            return false;
        }

        pg_sys::get_extension_oid(c"pg_flashback".as_ptr(), true) != pg_sys::InvalidOid
    }
}

/// Only consider the hook for DDL that can affect tracked tables.
/// Avoids calling get_extension_oid / SPI during CREATE/DROP DATABASE etc.
unsafe fn is_table_related_utility(pstmt: *mut pg_sys::PlannedStmt) -> bool {
    let utility_stmt = (*pstmt).utilityStmt;
    if utility_stmt.is_null() {
        return false;
    }
    match (*utility_stmt).type_ {
        pg_sys::NodeTag::T_TruncateStmt => true,
        pg_sys::NodeTag::T_AlterTableStmt => {
            let stmt = utility_stmt as *mut pg_sys::AlterTableStmt;
            (*stmt).objtype == pg_sys::ObjectType::OBJECT_TABLE
        }
        pg_sys::NodeTag::T_DropStmt => {
            let stmt = utility_stmt as *mut pg_sys::DropStmt;
            (*stmt).removeType == pg_sys::ObjectType::OBJECT_TABLE
        }
        _ => false,
    }
}

unsafe fn parse_pre_utility_targets(
    pstmt: *mut pg_sys::PlannedStmt,
) -> Option<(&'static str, Vec<UtilityTarget>)> {
    let utility_stmt = (*pstmt).utilityStmt;
    if utility_stmt.is_null() {
        return None;
    }

    match (*utility_stmt).type_ {
        pg_sys::NodeTag::T_TruncateStmt => {
            let stmt = utility_stmt as *mut pg_sys::TruncateStmt;
            let mut targets = Vec::new();
            for ptr in list_ptr_values((*stmt).relations) {
                let range_var = ptr as *mut pg_sys::RangeVar;
                if range_var.is_null() {
                    continue;
                }

                let table = c_ptr_to_option_string((*range_var).relname).unwrap_or_default();
                if table.is_empty() {
                    continue;
                }

                let schema = c_ptr_to_option_string((*range_var).schemaname);
                targets.push(UtilityTarget { schema, table });
            }

            if targets.is_empty() {
                None
            } else {
                Some(("TRUNCATE", targets))
            }
        }
        pg_sys::NodeTag::T_DropStmt => {
            let stmt = utility_stmt as *mut pg_sys::DropStmt;
            if (*stmt).removeType != pg_sys::ObjectType::OBJECT_TABLE {
                return None;
            }

            let mut targets = Vec::new();
            for ptr in list_ptr_values((*stmt).objects) {
                let object_name_list = ptr as *mut pg_sys::List;
                let parts = list_string_parts(object_name_list);
                if parts.is_empty() {
                    continue;
                }

                let table = parts.last().cloned().unwrap_or_default();
                if table.is_empty() {
                    continue;
                }

                let schema = if parts.len() >= 2 {
                    Some(parts[parts.len() - 2].clone())
                } else {
                    None
                };

                targets.push(UtilityTarget { schema, table });
            }

            if targets.is_empty() {
                None
            } else {
                Some(("DROP", targets))
            }
        }
        _ => None,
    }
}

unsafe fn parse_post_utility_targets(
    pstmt: *mut pg_sys::PlannedStmt,
) -> Option<(&'static str, Vec<UtilityTarget>)> {
    let utility_stmt = (*pstmt).utilityStmt;
    if utility_stmt.is_null() {
        return None;
    }

    match (*utility_stmt).type_ {
        pg_sys::NodeTag::T_AlterTableStmt => {
            let stmt = utility_stmt as *mut pg_sys::AlterTableStmt;
            if (*stmt).objtype != pg_sys::ObjectType::OBJECT_TABLE {
                return None;
            }

            let relation = (*stmt).relation;
            if relation.is_null() {
                return None;
            }

            let table = c_ptr_to_option_string((*relation).relname).unwrap_or_default();
            if table.is_empty() {
                return None;
            }

            let schema = c_ptr_to_option_string((*relation).schemaname);
            Some(("ALTER", vec![UtilityTarget { schema, table }]))
        }
        _ => None,
    }
}

fn capture_ddl_for_targets(event_type: &str, targets: &[UtilityTarget]) -> Result<(), SpiError> {
    for target in targets {
        let schema = target.schema.as_deref().unwrap_or("");
        Spi::run_with_args(
            "SELECT public.flashback_capture_ddl_event($1, NULLIF($2, ''), $3)",
            &[
                event_type.into(),
                schema.into(),
                target.table.as_str().into(),
            ],
        )?;
    }

    Ok(())
}

unsafe fn list_ptr_values(list: *mut pg_sys::List) -> Vec<*mut c_void> {
    let mut out = Vec::new();
    if list.is_null() {
        return out;
    }

    let len = (*list).length.max(0) as usize;
    let elements = (*list).elements;
    if elements.is_null() {
        return out;
    }

    for idx in 0..len {
        out.push((*elements.add(idx)).ptr_value);
    }

    out
}

unsafe fn list_string_parts(list: *mut pg_sys::List) -> Vec<String> {
    let mut out = Vec::new();
    for ptr in list_ptr_values(list) {
        let str_node = ptr as *mut pg_sys::String;
        if str_node.is_null() {
            continue;
        }

        if let Some(value) = c_ptr_to_option_string((*str_node).sval) {
            if !value.is_empty() {
                out.push(value);
            }
        }
    }

    out
}

unsafe fn c_ptr_to_option_string(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }

    Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
}

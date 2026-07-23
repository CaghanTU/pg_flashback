//! Streaming restore data fingerprints (SHA-256 over all rows).
//!
//! Ordering comes from an immutable `order_spec` derived from target
//! `schema_def`, never from the live/shadow catalog PK presence.

use pgrx::prelude::*;
use pgrx::PgRelation;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Derive immutable fingerprint order_spec from target schema_def.
#[pg_extern(immutable, parallel_safe, name = "flashback_fingerprint_order_spec")]
fn flashback_fingerprint_order_spec(schema_def: pgrx::JsonB) -> pgrx::JsonB {
    let root = schema_def.0;
    let pk = root
        .get("primary_key")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let columns: Vec<String> = pk
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .filter(|s| !s.is_empty())
        .collect();
    if columns.is_empty() {
        pgrx::JsonB(json!({
            "mode": "full_row",
            "columns": []
        }))
    } else {
        pgrx::JsonB(json!({
            "mode": "pk",
            "columns": columns
        }))
    }
}

/// SHA-256 hex digest of every row encoding, streamed via SPI cursor batches.
///
/// `order_spec` must be the immutable spec from `flashback_fingerprint_order_spec`.
/// Digest input is the canonical row encoding only — never `ctid`.
#[pg_extern(
    stable,
    parallel_safe,
    name = "flashback_relation_full_data_fingerprint"
)]
fn flashback_relation_full_data_fingerprint(
    relation: PgRelation,
    order_spec: pgrx::JsonB,
) -> String {
    let oid = relation.oid().to_u32();
    let (schema, name) = Spi::get_two::<String, String>(&format!(
        "SELECT n.nspname::text, c.relname::text \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = {oid}"
    ))
    .unwrap_or_else(|e| pgrx::error!("pg_flashback: cannot resolve relation {oid}: {e}"));
    let schema = schema.unwrap_or_else(|| pgrx::error!("pg_flashback: relation {oid} missing"));
    let name = name.unwrap_or_else(|| pgrx::error!("pg_flashback: relation {oid} missing name"));
    let (order_sql, mode) = match order_clause_from_spec(&order_spec.0) {
        Ok(v) => v,
        Err(msg) => pgrx::error!("pg_flashback: invalid fingerprint order_spec: {msg}"),
    };

    let query = format!(
        "SELECT row_to_json(t)::text AS row_enc \
         FROM {}.{} t \
         ORDER BY {order_sql}",
        quote_ident(&schema),
        quote_ident(&name),
    );

    let mut hasher = Sha256::new();
    hasher.update(b"flashback_full_data_fingerprint_v1|");
    hasher.update(mode.as_bytes());
    hasher.update(b"|");

    let mut row_count: u64 = 0;

    // A single SPI connection for the whole cursor loop. pgrx's documented
    // cross-session cursor::detach_into_name()/find_cursor() reattach pattern
    // is unreliable in practice for the auto-named ("<unnamed portal N>")
    // cursors SPI_cursor_open_with_args() creates here, so this keeps the
    // cursor and every fetched SpiTupleTable batch alive only for the
    // duration of one Spi::connect_mut call. The local_max_snapshot_bytes /
    // local_max_restore_peak_bytes admission gates already bound how large a
    // local_delta table can be, so the resulting bounded per-call memory use
    // is acceptable; this is not intended for unbounded/backup-scale tables.
    Spi::connect_mut(|client| -> Result<(), spi::Error> {
        let mut cursor = client.open_cursor(&query, &[]);
        loop {
            let mut table = cursor.fetch(512)?;
            if table.is_empty() {
                break;
            }
            while table.next().is_some() {
                let enc = table
                    .get_by_name::<String, _>("row_enc")?
                    .unwrap_or_default();
                row_count += 1;
                hasher.update(enc.as_bytes());
                hasher.update([0u8]);
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| pgrx::error!("pg_flashback: fingerprint fetch failed: {e}"));

    hasher.update(format!("|rows={row_count}").as_bytes());
    format!("{:x}", hasher.finalize())
}

fn order_clause_from_spec(spec: &Value) -> Result<(String, String), String> {
    let mode = spec
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("full_row");
    match mode {
        "pk" => {
            let cols = spec
                .get("columns")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let names: Vec<String> = cols
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| format!("t.{}", quote_ident(s)))
                .collect();
            if names.is_empty() {
                return Err("pk mode requires non-empty columns".to_owned());
            }
            // ctid is a last-resort internal tie-break only — not hashed.
            Ok((format!("{}, t.ctid", names.join(", ")), "pk".to_string()))
        }
        "full_row" => Ok((
            "row_to_json(t)::text, t.ctid".to_string(),
            "full_row".to_string(),
        )),
        other => Err(format!("unknown mode '{other}'")),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

//! Streaming restore data fingerprints (SHA-256 over all rows).
//!
//! v2 hashes each row in the database and sorts only the resulting
//! 32-byte digests.  v1 sorted the full `row_to_json(t)::text`, so the
//! sort carried every row's whole encoding: restoring a 1 GiB table
//! spilled 5.6 GiB of temp files, and the two independent proof scans
//! made a 10 GiB restore write about 58 GiB.  Hashing before the sort
//! keeps the whole row cryptographically covered while the sort moves a
//! fixed 32 bytes per row.  Digests are not comparable across versions.
//!
//! Stream order comes from the row digest itself, so it is independent of
//! both the heap's physical order and the catalog.  `order_spec` is still
//! required and still derived from target `schema_def` rather than the
//! live/shadow catalog, but in v2 it contributes only the `mode` label bound
//! into the digest and the primary-key column names, which are validated
//! against the relation.

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
    let (mode, pk_columns) = match mode_and_columns_from_spec(&order_spec.0) {
        Ok(v) => v,
        Err(msg) => pgrx::error!("pg_flashback: invalid fingerprint order_spec: {msg}"),
    };
    // v1 embedded these columns in an ORDER BY, so a name that did not exist
    // was rejected when the query was planned. v2 does not order by them, so
    // the check has to be explicit or a bogus spec would silently produce a
    // digest that looks valid.
    reject_unknown_columns(oid, &pk_columns);

    // Hash first, sort second: the sort then moves 32 bytes per row instead
    // of the row's whole JSON encoding. Ordering by the digest keeps the
    // result independent of the heap's physical order, and identical rows
    // still yield identical digests, so duplicate multiplicity survives.
    let query = format!(
        "SELECT pg_catalog.sha256(\
             pg_catalog.convert_to(\
                 pg_catalog.row_to_json(t)::pg_catalog.text, 'UTF8')) AS row_h \
         FROM {}.{} t \
         ORDER BY 1",
        quote_ident(&schema),
        quote_ident(&name),
    );

    let mut hasher = Sha256::new();
    hasher.update(b"flashback_full_data_fingerprint_v2|");
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
                // A row digest is fixed width, so an absent value is a real
                // fault rather than something to paper over with a default.
                let enc = table
                    .get_by_name::<Vec<u8>, _>("row_h")?
                    .unwrap_or_else(|| pgrx::error!("pg_flashback: null row digest"));
                row_count += 1;
                hasher.update(&enc);
                hasher.update([0u8]);
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| pgrx::error!("pg_flashback: fingerprint fetch failed: {e}"));

    hasher.update(format!("|rows={row_count}").as_bytes());
    format!("{:x}", hasher.finalize())
}

fn mode_and_columns_from_spec(spec: &Value) -> Result<(String, Vec<String>), String> {
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
                .map(|s| s.to_string())
                .collect();
            if names.is_empty() {
                return Err("pk mode requires non-empty columns".to_owned());
            }
            Ok(("pk".to_string(), names))
        }
        "full_row" => Ok(("full_row".to_string(), Vec::new())),
        other => Err(format!("unknown mode '{other}'")),
    }
}

/// Fail closed on a spec naming a column the relation does not have.
fn reject_unknown_columns(oid: u32, columns: &[String]) {
    for column in columns {
        let found = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped \
             AND a.attname OPERATOR(pg_catalog.=) {}::pg_catalog.name)",
            quote_literal(column)
        ))
        .unwrap_or_else(|e| {
            pgrx::error!("pg_flashback: cannot validate fingerprint order_spec: {e}")
        })
        .unwrap_or(false);
        if !found {
            pgrx::error!(
                "pg_flashback: fingerprint order_spec names unknown column '{column}' on relation {oid}"
            );
        }
    }
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

# pg_flashback

Table‑level point‑in‑time restore and time‑travel queries for PostgreSQL.  
Built with Rust + pgrx (v0.16.1). Tested on PostgreSQL 15–17.

## 1. Why This Extension?

- Restores any tracked table to any past timestamp — in seconds, not hours.
- No full‑cluster PITR, no `pg_basebackup`, no manual `pg_dump` surgery.
- Captures INSERT / UPDATE / DELETE via triggers and DDL (TRUNCATE, DROP, ALTER) via `ProcessUtility_hook`.
- Ships with a background worker, UNLOGGED staging table, and asynchronous JSONB pipeline for minimal write overhead.
- Includes `flashback_query()` — query past table state without actually restoring (`SELECT AS OF` semantics).
- Per‑table advisory locks allow concurrent restores of unrelated tables.
- Designed for production: TOAST guard, progress reporting, restore audit log, global kill switch, and monitoring views.

## 2. Architecture Overview

| Component | Purpose |
|-----------|---------|
| AFTER Triggers (statement + row) | Capture DML events into `staging_events` (UNLOGGED) using native JSONB. |
| `ProcessUtility_hook` | Intercepts DDL (TRUNCATE, DROP, ALTER, RENAME) and snapshots schema state. |
| Background Worker (`flashback_worker`) | Flushes staging → `delta_log` every 75 ms; runs periodic checkpoints and retention purge. |
| `delta_log` | Append‑only JSONB event store with composite indexes for fast restore scans. |
| Snapshots / Checkpoints | Materialised table copies for O(1) base‑image loading. |
| `schema_versions` | Tracks column definitions, constraints, indexes, triggers, and RLS policies per schema change. |
| `flashback_restore()` | Loads nearest snapshot, replays delta events to target time, restores sequences and DDL. |
| `flashback_query()` | Reconstructs past state in a temp table and executes arbitrary queries against it. |

The extension is transparent to applications. Tables work normally; capture and restore happen behind the scenes.

```
DML (INSERT / UPDATE / DELETE)
  │
  ▼
AFTER triggers ──► staging_events (UNLOGGED, JSONB)
                       │
                       ▼  background worker (every 75 ms)
                  delta_log (JSONB) ──► snapshots (checkpoints)
                       │
DDL hook ──────────────┘
(TRUNCATE / DROP / ALTER / RENAME)

flashback_restore(table, timestamp)
  ├─ Find nearest snapshot / checkpoint
  ├─ Recreate table from schema_versions
  ├─ Bulk‑load base image (INSERT … SELECT)
  ├─ Replay deltas forward to target time
  ├─ Restore serial sequences
  └─ Log to restore_log + RAISE NOTICE progress
```

## 3. Requirements

### PostgreSQL

**Supported versions:** PostgreSQL 15, 16, or 17

**Required `postgresql.conf` settings:**
```
wal_level = logical
shared_preload_libraries = 'pg_flashback'
```

A restart is required after changing `shared_preload_libraries`.

### Rust Toolchain

- Rust ≥ 1.70 (stable)
- `cargo-pgrx 0.16.1`

```bash
cargo install --locked cargo-pgrx --version 0.16.1
```

## 4. Build & Install

### Initialise pgrx

```bash
cargo pgrx init --pg17 /path/to/pg_config
```

*Run once per PostgreSQL major version.*

### Build & Install

```bash
cargo pgrx install --no-default-features -F pg17
```

### Enable the Extension

```bash
# Restart PostgreSQL after adding shared_preload_libraries
sudo systemctl restart postgresql-17

# Create the extension
psql -c "CREATE EXTENSION pg_flashback;"
```

### Multi‑Version Build

```bash
cargo pgrx install --no-default-features -F pg16
cargo pgrx install --no-default-features -F pg15
```

## 5. Quick Start

```sql
-- Start tracking a table
SELECT flashback_track('orders');

-- … normal operations happen …

-- Disaster: accidental mass delete
DELETE FROM orders WHERE created_at < '2026-01-01';

-- Restore to 30 seconds ago
SELECT flashback_restore('orders', now() - interval '30 seconds');
-- NOTICE: flashback_restore [orders]: snapshot loaded from flashback._snap_orders_16384
-- NOTICE: flashback_restore [orders]: replaying 847 events …
-- NOTICE: flashback_restore [orders]: complete — 847 events applied

-- Or query the past WITHOUT restoring
SELECT * FROM flashback_query(
    'orders',
    now() - interval '30 seconds',
    'SELECT * FROM $FB_TABLE WHERE total > 100'
) AS t(id int, total numeric, status text);
```

## 6. Configuration (GUCs)

All GUCs live under `pg_flashback.*`. They can be set globally (`postgresql.conf`, `ALTER SYSTEM`) or per role/database (`ALTER ROLE … SET`).

| GUC | Default | Description |
|-----|---------|-------------|
| `enabled` | `on` | Global kill switch. `off` stops all capture; worker idles. Superuser only. |
| `max_row_size` | `8kB` | Rows larger than this are skipped with a WARNING (TOAST protection). |
| `worker_interval_ms` | `75` | Background worker flush interval in milliseconds. |
| `worker_batch_size` | `4096` | Maximum rows per worker flush cycle. |

Changes take effect immediately; no restart is required after `shared_preload_libraries` is set.

## 7. SQL API Cheatsheet

### Tracking

| Function | Description |
|----------|-------------|
| `flashback_track(table)` | Start tracking a table. Creates triggers, base snapshot, and schema version entry. |
| `flashback_untrack(table)` | Stop tracking. Removes triggers and cleans up all metadata. |

### Restore

| Function | Description |
|----------|-------------|
| `flashback_restore(table, timestamptz)` | Restore a single table to a past timestamp. Returns number of events applied. |
| `flashback_restore(tables[], timestamptz)` | Restore multiple tables in dependency order within one transaction. |
| `flashback_query(table, timestamptz [, query])` | Query past table state without restoring. Replace `$FB_TABLE` in custom queries. Returns `SETOF record`. |

### Checkpoints & Retention

| Function | Description |
|----------|-------------|
| `flashback_checkpoint(table)` | Create a manual checkpoint (materialised snapshot). |
| `flashback_retention_status()` | Per‑table delta counts and retention warning flags. |

### Monitoring & Audit

| Function / View | Description |
|-----------------|-------------|
| `flashback.pg_stat_flashback` | Dashboard view: tracked tables, pending events, delta storage, restore counts. |
| `flashback_history(table, interval)` | Recent change history for a table. |
| `flashback.restore_log` | Audit log of all restore operations (who, when, what, success/failure). |

### Schema & Lock Management

| Function | Description |
|----------|-------------|
| `flashback_collect_schema_def(oid)` | Collect full schema definition (columns, PKs, constraints, indexes, triggers, RLS). |
| `flashback_is_restore_in_progress(oid)` | Check if a restore is running for a specific table. |
| `flashback_set_restore_in_progress(bool)` | Set restore‑in‑progress flag (internal use by restore functions). |

## 8. Write Overhead Benchmark

Measured on PostgreSQL 17, single‑node:

| Scenario | Without | With pg_flashback | Overhead |
|----------|---------|-------------------|----------|
| Bulk INSERT 100K rows | 133 ms | 261 ms | **2.0×** |
| Single‑row UPDATE 10K | 49 ms | 132 ms | **2.7×** |
| Mixed DML 15K ops | 37 ms | 99 ms | **2.7×** |
| Wide table UPDATE (15 cols, 5K rows) | 11 ms | 91 ms | **8.0×** |

Bulk INSERT uses statement‑level triggers with transition tables. UPDATE uses per‑row triggers. All stages use native JSONB — no `json` → `jsonb` conversion overhead.

## 9. Features

| Feature | Status |
|---------|--------|
| Single‑table restore to any timestamp | ✅ |
| Multi‑table restore in one transaction | ✅ |
| Flashback Query (`SELECT AS OF`) | ✅ |
| Schema evolution awareness (ADD / DROP / ALTER COLUMN) | ✅ |
| DDL capture (TRUNCATE, DROP TABLE, ALTER TABLE, RENAME) | ✅ |
| Automatic checkpoints for fast restore | ✅ |
| Configurable retention policy | ✅ |
| Serial / sequence restoration | ✅ |
| Trigger & RLS policy preservation during restore | ✅ |
| Generated column awareness | ✅ |
| Global kill switch (`pg_flashback.enabled`) | ✅ |
| Monitoring view (`pg_stat_flashback`) | ✅ |
| Restore audit log + progress reporting | ✅ |
| Large row protection (TOAST guard) | ✅ |
| Per‑table concurrent restore safety (advisory lock) | ✅ |
| Native JSONB pipeline (zero conversion) | ✅ |
| Bulk snapshot restore (`INSERT … SELECT`) | ✅ |
| Composite delta_log indexes for fast scans | ✅ |
| FK‑aware multi‑table restore ordering | ✅ |
| Circular FK protection (depth limit) | ✅ |

## 10. Testing & Observability

### Test Suite

29 integration tests covering DML, DDL, schema evolution, multi‑table FK, checkpoints, edge cases, and flashback query:

```bash
cargo pgrx test pg17
# test result: ok. 29 passed; 0 failed
```

### Monitoring Queries

```sql
-- Dashboard
SELECT * FROM flashback.pg_stat_flashback;

-- Retention health
SELECT * FROM flashback_retention_status();

-- Recent restores
SELECT * FROM flashback.restore_log ORDER BY restored_at DESC LIMIT 10;

-- Active restores (any table)
SELECT * FROM flashback_is_restore_in_progress(NULL);
```

### CI

GitHub Actions pipeline runs on every push:
- Build matrix: PostgreSQL 15, 16, 17
- `cargo clippy` + `cargo fmt --check`
- Full integration test suite

## 11. Operations & Integration

### Common Tasks

```sql
-- Start tracking
SELECT flashback_track('public.orders');

-- Check what is being tracked
SELECT * FROM flashback.tracked_tables WHERE is_active;

-- Manual checkpoint before risky migration
SELECT flashback_checkpoint('public.orders');

-- Disable capture temporarily (all tables, all sessions)
SET pg_flashback.enabled = off;
-- … run migration …
SET pg_flashback.enabled = on;

-- Query past state without restoring
SELECT * FROM flashback_query('orders', now() - interval '1 hour')
    AS t(id int, customer_id int, total numeric, status text);

-- Multi‑table restore (FK‑safe ordering)
SELECT flashback_restore(
    ARRAY['order_items', 'orders', 'customers'],
    now() - interval '10 minutes'
);
```

### Application Integration

No application changes required. Track tables once; the extension captures changes transparently. Restore is a single SQL call from any PostgreSQL client.

## 12. Troubleshooting

| Symptom | Check / Fix |
|---------|-------------|
| Extension fails to load | Ensure `shared_preload_libraries = 'pg_flashback'` and restart PostgreSQL. |
| Background worker missing | `SELECT * FROM pg_stat_activity WHERE backend_type LIKE 'pg_flashback%';` |
| Restore returns 0 events | Check that `committed_at IS NOT NULL` on delta_log rows (worker must flush staging first). |
| Triggers not firing | Verify tracking: `SELECT * FROM flashback.tracked_tables WHERE is_active;` |
| TOAST / large row warnings | Increase `pg_flashback.max_row_size` or accept that oversized rows are skipped. |
| Socket connection issues (pgrx dev) | Try: `psql -h ~/.pgrx -p 28817 postgres` |
| Feature flag conflict | Use explicit features: `cargo pgrx install --no-default-features -F pg17` |

## 13. Caveats & Limitations

- **Crash window:** `staging_events` is UNLOGGED for performance. Events not yet flushed to `delta_log` (up to `worker_interval_ms`, default 75 ms) are lost on a PostgreSQL crash. If you need zero‑loss guarantees, set `pg_flashback.staging_logged = on` (planned) or reduce the flush interval.
- **DDL snapshots:** TRUNCATE and DROP events snapshot the full table contents into `delta_log.old_data` as JSONB. For very large tables this can consume significant memory. A `pg_flashback.max_ddl_snapshot_rows` guard is planned.
- **Single database:** The background worker connects to one database. Multi‑database deployments require one `shared_preload_libraries` entry per database (planned).
- **No extension upgrade path yet:** The extension ships as version 0.1.0. Future versions will include `ALTER EXTENSION … UPDATE` migration scripts.

## License

MIT

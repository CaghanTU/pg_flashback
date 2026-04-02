# pg_flashback

[![CI](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml/badge.svg)](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml)

Table‑level point‑in‑time restore and time‑travel queries for PostgreSQL.  
Built with Rust + pgrx (v0.16.1). Tested on PostgreSQL 16 and 17.

## 1. Why This Extension?

- Restores any tracked table to any past timestamp — in seconds, not hours.
- No full‑cluster PITR, no `pg_basebackup`, no manual `pg_dump` surgery.
- Captures INSERT / UPDATE / DELETE via triggers and DDL (TRUNCATE, DROP, ALTER) via `ProcessUtility_hook`.
- **Diff‑only UPDATE capture** — stores only PK columns + changed columns per UPDATE, reducing delta storage by up to 40× on wide tables.
- Ships with a background worker, UNLOGGED staging table, and asynchronous JSONB pipeline for minimal write overhead.
- Includes `flashback_query()` — query past table state without actually restoring (`SELECT AS OF` semantics).
- Per‑table advisory locks allow concurrent restores of unrelated tables.
- **Multi‑database worker** — single extension install can track tables across multiple databases simultaneously.
- **Native partitioned table support** — automatically uses per‑row triggers on partitioned tables (PostgreSQL does not support transition tables on partitioned tables).
- Designed for production: TOAST guard, progress reporting, restore audit log, global kill switch, and monitoring views.

## 2. Architecture Overview

| Component | Purpose |
|-----------|---------|
| AFTER Triggers (statement + row) | Capture DML events into `staging_events` (UNLOGGED) using native JSONB. Partitioned tables use per‑row triggers automatically. |
| `ProcessUtility_hook` | Intercepts DDL (TRUNCATE, DROP, ALTER, RENAME) and snapshots schema state. |
| Background Worker (`flashback_worker`) | Flushes staging → `delta_log` every 75 ms; runs periodic checkpoints and retention purge. Supports multi‑DB via `target_databases` GUC. |
| `delta_log` | Append‑only JSONB event store with lz4 compression (where available) and composite indexes for fast restore scans. |
| Snapshots / Checkpoints | Materialised table copies for O(1) base‑image loading. |
| `schema_versions` | Tracks column definitions, constraints, indexes, triggers, and RLS policies per schema change. |
| `flashback_restore()` | Loads nearest snapshot, replays delta events to target time, restores sequences and DDL. PK tables use batch (set‑based) replay; non‑PK uses row‑by‑row. |
| `flashback_restore_parallel()` | Restore with parallel query hints (`max_parallel_workers_per_gather`). Emits per‑partition guidance for partitioned tables. |
| `flashback_query()` | Reconstructs past state in a temp table and executes arbitrary queries against it. |

The extension is transparent to applications. Tables work normally; capture and restore happen behind the scenes.

```
DML (INSERT / UPDATE / DELETE)
  │
  ▼
AFTER triggers ──► staging_events (UNLOGGED, JSONB, diff-only UPDATE)
                       │
                       ▼  background worker (every 75 ms)
                  delta_log (JSONB, lz4 compressed) ──► snapshots (checkpoints)
                       │
DDL hook ──────────────┘
(TRUNCATE / DROP / ALTER / RENAME)

flashback_restore(table, timestamp)
  ├─ Find nearest snapshot / checkpoint
  ├─ Recreate table from schema_versions (shadow table, crash-safe)
  ├─ Bulk-load base image (INSERT … SELECT)
  ├─ Replay deltas (batch/net-effect for PK tables; row-by-row for no-PK)
  ├─ Atomic swap: DROP original → RENAME shadow (brief exclusive lock)
  ├─ Restore serial sequences
  └─ Log to restore_log + RAISE NOTICE progress
```

## 3. Requirements

### PostgreSQL

**Supported versions:** PostgreSQL 16 or 17

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

### Upgrading from a Previous Version

```bash
# Install new binary
cargo pgrx install --no-default-features -F pg17

# Restart PostgreSQL to load the new .so
sudo systemctl restart postgresql-17

# Upgrade the extension in-place (preserves all tracking data)
psql -c "ALTER EXTENSION pg_flashback UPDATE TO '0.4.0';"
```

### Multi‑Version Build

```bash
cargo pgrx install --no-default-features -F pg16
cargo pgrx install --no-default-features -F pg17
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
-- NOTICE: flashback_restore [orders]: snapshot loaded into shadow from flashback.snap_16384_42
-- NOTICE: flashback_restore [orders]: using batch replay (PK table, no DDL events)
-- NOTICE: flashback_restore [orders]: 847 events → 847 unique PKs
-- NOTICE: flashback_restore [orders]: complete — 847 events applied, duration 00:00:00.182

-- Or use parallel restore hint on large tables
SELECT * FROM flashback_restore_parallel('orders', now() - interval '30 seconds', 4);

-- Or query the past WITHOUT restoring
SELECT * FROM flashback_query(
    'orders',
    now() - interval '30 seconds',
    'SELECT * FROM $FB_TABLE WHERE total > 100'
) AS t(id int, total numeric, status text);
```

## 6. Configuration (GUCs)

All GUCs live under `pg_flashback.*`. They can be set globally (`postgresql.conf`, `ALTER SYSTEM`) or per role/database (`ALTER ROLE … SET`).

| GUC | Default | Reload | Description |
|-----|---------|--------|-------------|
| `enabled` | `on` | SIGHUP | Global kill switch. `off` stops all capture; worker idles. Superuser only. |
| `max_row_size` | `64kB` | SIGHUP | Rows larger than this are skipped with a WARNING (TOAST protection). |
| `worker_interval_ms` | `75` | SIGHUP | Background worker flush interval in milliseconds. |
| `worker_batch_size` | `4096` | SIGHUP | Maximum rows per worker flush cycle. |
| `target_database` | `postgres` | Restart | Database the background worker connects to (single‑DB mode). Overridden by `target_databases`. |
| `target_databases` | *(unset)* | Restart | Comma-separated list of databases for multi‑DB mode. Each database gets its own worker. Example: `'app,analytics,audit'`. |
| `max_workers` | `4` | Restart | Maximum number of background workers registered at startup. Extra workers beyond the database count exit gracefully. |

All GUCs except those marked *Restart* take effect immediately via `SIGHUP` reload.

## 7. SQL API Reference

### Tracking

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_track(table)` | `boolean` | Start tracking a table. Creates triggers (partition‑aware), base snapshot, and schema version entry. |
| `flashback_untrack(table)` | `void` | Stop tracking. Removes triggers and cleans up all metadata. |

### Restore

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_restore(table, timestamptz)` | `bigint` | Restore a single table to a past timestamp. Returns number of events applied. |
| `flashback_restore(tables[], timestamptz)` | `bigint` | Restore multiple tables in FK dependency order within one transaction. |
| `flashback_restore_parallel(table, timestamptz [, num_workers])` | `TABLE(restored_table text, events_applied bigint)` | Restore with parallel query hints. Default `num_workers = 4`. |
| `flashback_query(table, timestamptz [, query])` | `SETOF record` | Query past table state without restoring. Replace `$FB_TABLE` in custom queries. |

### Checkpoints & Retention

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_checkpoint(table)` | `bigint` | Create a manual checkpoint (materialised snapshot). Returns snapshot_id. |
| `flashback_retention_status()` | `SETOF record` | Per‑table delta counts, restorable window, and retention warning flags. |

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
| `flashback_is_restore_in_progress(oid)` | Check if a restore is running in the current backend. |
| `flashback_set_restore_in_progress(bool)` | Set restore‑in‑progress flag (internal use; superuser only). |

## 8. Restore Performance

Measured on PostgreSQL 17, single‑node, batch replay path (PK tables):

| Scenario | Rows | Events | Restore Time | Throughput |
|----------|------|--------|-------------|------------|
| All‑rows UPDATE | 10K | 10K | ~25 ms | ~400K events/s |
| All‑rows UPDATE | 100K | 100K | ~180 ms | ~560K events/s |
| All‑rows UPDATE | 500K | 500K | ~1.1 s | ~450K events/s |
| All‑rows UPDATE | 1M | 1M | ~2.5 s | ~400K events/s |
| Partial DELETE (33%) | 1M | 333K | ~0.9 s | ~370K events/s |

*Batch replay computes net‑effect per PK and applies bulk DELETE/UPSERT/UPDATE — no row‑by‑row scanning.*

## 9. Write Overhead Benchmark

Measured on PostgreSQL 17, single‑node:

| Scenario | Without | With pg_flashback | Overhead |
|----------|---------|-------------------|----------|
| Bulk INSERT 100K rows | 133 ms | 261 ms | **2.0×** |
| Single‑row UPDATE 10K | 49 ms | 132 ms | **2.7×** |
| Mixed DML 15K ops | 37 ms | 99 ms | **2.7×** |
| Wide table UPDATE (15 cols, 5K rows) | 11 ms | 91 ms | **8.0×** |
| Wide table UPDATE (diff-only, 5K rows) | 11 ms | 38 ms | **3.5×** |

Wide table UPDATE overhead drops significantly with diff-only capture enabled (v0.3.0+).

## 10. Features

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
| **Diff‑only UPDATE capture** (PK + changed cols only) | ✅ v0.3.0 |
| **Batch / net‑effect restore replay** | ✅ v0.3.0 |
| **lz4 compression on delta_log** (where available) | ✅ v0.3.0 |
| **Multi‑database worker** (`target_databases` GUC) | ✅ v0.3.0 |
| **Native partitioned table support** (per‑row triggers) | ✅ v0.4.0 |
| **Parallel restore hints** (`flashback_restore_parallel`) | ✅ v0.4.0 |

## 11. Testing & Observability

### Test Suite

39 integration tests covering DML, DDL, schema evolution, multi‑table FK, checkpoints, edge cases, flashback query, partitioned tables, diff‑only UPDATE, batch replay, and RBAC:

```bash
# Remove stale test data first (prevents mutex lock conflicts)
rm -rf target/test-pgdata
cargo pgrx test pg17
# test result: ok. 39 passed; 0 failed
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

-- Storage breakdown per tracked table
SELECT
    table_name,
    delta_count,
    pg_size_pretty(delta_size_bytes) AS delta_size,
    restorable_from,
    retention_warning
FROM flashback_retention_status();
```

### CI

GitHub Actions pipeline runs on every push to `main` and on every pull request:

- **Lint job**: `cargo fmt --check` + `cargo clippy -D warnings`
- **Test matrix**: PostgreSQL 16, 17 — `cargo pgrx test`
- **Security audit**: `cargo audit`
- **Release workflow**: on `v*.*.*` tags, builds and publishes GitHub Releases

Local:
```bash
cargo fmt --all
cargo clippy --no-default-features -F pg17
rm -rf target/test-pgdata && cargo pgrx test pg17
```

## 12. Benchmarks

Run the write‑overhead benchmark:

```bash
./scripts/run_benchmark.sh
```

Run the restore performance benchmark (10K → 1M rows):

```bash
./scripts/run_restore_benchmark.sh
```

## 13. Operations & Integration

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

-- Parallel restore for large tables (uses PostgreSQL parallel query internally)
SELECT * FROM flashback_restore_parallel('orders', now() - interval '30 minutes', 4);
```

### Partitioned Tables

pg_flashback natively supports partitioned tables from v0.4.0. `flashback_track` automatically detects partitioned parents and attaches per‑row triggers (instead of the statement‑level transition‑table triggers used for regular tables):

```sql
CREATE TABLE events (
    id      bigserial,
    region  text,
    ts      timestamptz,
    payload jsonb,
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- Track the parent — partitions are covered automatically
SELECT flashback_track('public.events');

-- Normal DML on any partition is captured
INSERT INTO events (region, ts, payload) VALUES ('EU', now(), '{}');

-- Restore the parent (all partitions restored atomically)
SELECT flashback_restore('public.events', now() - interval '5 minutes');
```

### Multi‑Database Worker

```ini
# postgresql.conf
shared_preload_libraries = 'pg_flashback'
pg_flashback.target_databases = 'app_db,analytics_db,audit_db'
pg_flashback.max_workers = 3
```

Each database gets its own background worker process. Extra workers beyond the database count exit gracefully.

### Application Integration

No application changes required. Track tables once; the extension captures changes transparently. Restore is a single SQL call from any PostgreSQL client.

## 14. Troubleshooting

| Symptom | Check / Fix |
|---------|-------------|
| Extension fails to load | Ensure `shared_preload_libraries = 'pg_flashback'` and restart PostgreSQL. |
| Background worker missing | `SELECT * FROM pg_stat_activity WHERE backend_type LIKE 'pg_flashback%';` |
| Restore returns 0 events | Check that `committed_at IS NOT NULL` on delta_log rows (worker must flush staging first). |
| Triggers not firing on partitioned table | Upgrade to v0.4.0+ which uses per‑row triggers automatically for partitioned tables. |
| TOAST / large row warnings | Increase `pg_flashback.max_row_size` or accept that oversized rows are skipped. |
| Test mutex conflict | Run `rm -rf target/test-pgdata` before `cargo pgrx test`. |
| Socket connection issues (pgrx dev) | Try: `psql -h ~/.pgrx -p 28817 postgres` |

## 15. Caveats & Limitations

- **Crash window:** `staging_events` is UNLOGGED for performance. Events not yet flushed to `delta_log` (up to `worker_interval_ms`, default 75 ms) are lost on a PostgreSQL crash. Reduce the flush interval for stricter durability.
- **DDL snapshots:** TRUNCATE and DROP events snapshot the full table contents into `delta_log.old_data` as JSONB. For very large tables this can consume significant memory.
- **Partitioned table INSERT/DELETE capture:** Uses per‑row triggers (v0.4.0+). This is correct but slightly slower than statement‑level bulk capture on regular tables. For very high INSERT/DELETE throughput on partitioned tables, consider pre‑checkpointing frequently.
- **Upgrade path:** `ALTER EXTENSION pg_flashback UPDATE TO '0.4.0';` is supported from 0.3.0 onwards. Earlier versions: chain the upgrades (0.1.0 → 0.2.0 → 0.3.0 → 0.4.0).
- **pg_upgrade / major version:** Extension data is JSONB and schema‑version‑tracked. pg_upgrade is supported but you must re‑install the extension binary for the new major version and run `ALTER EXTENSION pg_flashback UPDATE` after the upgrade.

## License

MIT

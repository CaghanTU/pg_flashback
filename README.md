# pg_flashback

[![CI](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml/badge.svg)](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml)

Table‑level point‑in‑time restore and time‑travel queries for PostgreSQL.  
Built with Rust + pgrx (v0.16.1). CI-tested on PostgreSQL 15, 16, 17, and 18.

## 1. Why This Extension?

- Restores any tracked table to any past timestamp — in seconds, not hours.
- No full‑cluster PITR, no `pg_basebackup`, no manual `pg_dump` surgery.
- **Dual capture modes:** WAL (async, near-zero overhead) or trigger (no `wal_level` requirement).
- In WAL mode, DML capture carries essentially zero write overhead — changes are consumed asynchronously by the background worker from the logical replication slot.
- **Diff‑only UPDATE capture** — stores only PK columns + changed columns per UPDATE, reducing delta storage by up to 40× on wide tables.
- DDL captured (TRUNCATE, DROP, ALTER) via `ProcessUtility_hook`.
- Includes `flashback_query()` — query past table state without restoring (`SELECT AS OF` semantics). Uses historical schema from `schema_versions`; handles DROP events.
- **Non-destructive row recovery** — `flashback_recover_deleted()` re-inserts only missing rows without touching surviving data. Safe for partial-delete accidents.
- **DROP TABLE recovery** — `flashback_restore()` can reconstruct a dropped table from delta history alone.
- **SET SCHEMA and RENAME TABLE** are auto-tracked: OID-based lookup detects the change and recreates the capture trigger under the new name/schema.
- **Classical table inheritance preserved** during restore: child tables are detached before the DROP and re-attached after shadow rename.
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
| `delta_log` | Append‑only JSONB event store. Partitioned by `committed_at` (monthly range). lz4 compression where available. Composite indexes for fast restore scans. |
| Snapshots / Checkpoints | Materialised table copies for O(1) base‑image loading. |
| `schema_versions` | Tracks column definitions, constraints, indexes, triggers, and RLS policies per schema change. |
| `flashback_restore()` | Loads nearest snapshot, replays delta events to target time, restores sequences and DDL. PK tables use batch (set‑based) replay; non‑PK uses row‑by‑row. Works on dropped tables. |
| `flashback_recover_deleted()` | Non-destructive: re-inserts only rows missing at the current time. Survivors untouched. Requires a PK. |
| `flashback_restore_parallel()` | Restore with parallel query hints (`max_parallel_workers_per_gather`). Emits per‑partition guidance for partitioned tables. |
| `flashback_query()` | Reconstructs past state in a temp table and executes arbitrary queries against it. |

The extension is transparent to applications. Tables work normally; capture and restore happen behind the scenes.

```
── Trigger mode ──────────────────────────────────────────────────────────
DML (INSERT / UPDATE / DELETE)
  │
  ▼
AFTER triggers ──► staging_events (UNLOGGED, JSONB, diff-only UPDATE)
                       │
                       ▼  background worker (every 75 ms)
                  delta_log (JSONB, lz4 compressed) ──► snapshots (checkpoints)
                       ▲
── WAL mode ────────────┼────────────────────────────────────────────────
DML (INSERT / UPDATE / DELETE)
  │
  ▼
WAL (wal_level=logical)
  │
  ▼
logical replication slot (pg_flashback_slot)
  │
  ▼  background worker reads slot (every 75 ms)
  └──────────────────────────────────────────►  delta_log (direct, no staging)
                       ▲
── Both modes ──────────┼────────────────────────────────────────────────
DDL hook ──────────────┘
(TRUNCATE / DROP / ALTER / RENAME / SET SCHEMA — always via staging_events)

flashback_restore(table, timestamp)
  ├─ Find nearest snapshot / checkpoint
  ├─ Recreate table from schema_versions (shadow table, crash-safe)
  ├─ Bulk-load base image (INSERT … SELECT)
  ├─ Replay deltas filtered by event_time ≤ target (partition-pruned via committed_at)
  ├─ Batch/net-effect path for PK tables; row-by-row for tables without PK
  ├─ Atomic swap: detach INHERITS children → DROP original → RENAME shadow → re-attach children (brief exclusive lock)
  ├─ Recreate dependent views/matviews (owner, reloptions, indexes, populate; ACL via NOTICE)
  ├─ Restore serial sequences
  └─ Log to restore_log + RAISE NOTICE progress
```

## 3. Requirements

### PostgreSQL

**CI-tested versions:** PostgreSQL 15, 16, 17, 18 (59/59 tests pass on all four)  
**Compile-supported:** PostgreSQL 13 – 18 (pgrx feature flags; untested on 13–14)

**End-to-end verified (manual):** Both capture modes tested with 1 000+ row tables, mass-delete/update disaster scenarios, and full restore — trigger mode: ~58 ms restore, WAL mode: ~82 ms restore, 0 data integrity errors.

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
-- filter_clause is a WHERE predicate (not a full query) — SQL injection guard
SELECT * FROM flashback_query(
    'orders',
    now() - interval '30 seconds',
    'total > 100'   -- WHERE condition only; semicolons and DML keywords are rejected
) AS t(id int, total numeric, status text);
```

## 6. Configuration (GUCs)

All GUCs live under `pg_flashback.*`. They can be set globally (`postgresql.conf`, `ALTER SYSTEM`) or per role/database (`ALTER ROLE … SET`).

| GUC | Default | Reload | Description |
|-----|---------|--------|-------------|
| `enabled` | `on` | SIGHUP | Global kill switch. `off` stops all capture; worker idles. Superuser only. |
| `capture_mode` | `auto` | SIGHUP | Capture backend: `auto` (WAL if `wal_level=logical`, else trigger), `wal`, or `trigger`. |
| `slot_name` | `pg_flashback_slot` | Suset | Logical replication slot name. Override when running multiple pg_flashback installations on the same cluster. |
| `restore_work_mem` | `256MB` | Suset | `work_mem` override for snapshot bulk load during `flashback_restore`. Higher values speed up large table restores. |
| `index_build_work_mem` | `512MB` | Suset | `maintenance_work_mem` override for deferred index builds on the shadow table during restore. |
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
| `flashback_query(table, timestamptz [, filter_clause])` | `SETOF record` | Query past table state without restoring. `filter_clause` is a `WHERE` predicate (e.g. `'id = 5 AND status = ''active'''`). Runs as **SECURITY INVOKER** (caller's privileges). Semicolons and DML/DDL keywords are rejected to prevent SQL injection. |

### Recovery

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_recover_deleted(table, timestamptz)` | `bigint` | Non-destructive: re-inserts only rows that existed at `timestamptz` and are missing now. Surviving rows are untouched. Table must have a primary key. Returns count of recovered rows. |

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

### Methodology

- **Hardware:** AMD Ryzen 9 5900X, 64 GB RAM, NVMe SSD (single machine, no network).
- **Dataset:** Integer PK, 3–5 text columns of mixed width (≈1 KB average row). No TOAST overflow.
- **PostgreSQL settings:** `synchronous_commit = on`, `fsync = on` (default durability). Shared buffers 1 GB.
- **Cache state:** Buffer cache warm (tables fit in shared_buffers). Cold‑cache numbers are roughly 1.5–2× slower for the snapshot load phase.
- **Measurement:** Median of 3 consecutive runs. `pg_stat_reset()` called before each run. Results rounded to nearest 10 ms.
- **Scope:** Restore time includes snapshot load + delta replay + shadow swap. Does not include `flashback_track()` or base snapshot capture time.
- **Row‑by‑row path (no PK):** Not shown above. Throughput is roughly 50–80K events/s depending on predicate complexity.

## 9. Write Overhead Benchmark

Measured on PostgreSQL 17, single‑node (median of 3 runs each):

| Scenario | Baseline | Trigger mode | WAL mode |
|----------|----------|--------------|----------|
| Bulk INSERT 100K rows | 126 ms | 330 ms (+162%) | **123 ms (~0%)** |
| 10K single-row UPDATEs | 65 ms | 462 ms (+611%) | **80 ms (+23%)** |
| Mixed DML (5K ins+upd+del) | 40 ms | 119 ms (+198%) | **44 ms (+10%)** |
| Wide table UPDATE (15 cols, 5K rows) | 17 ms | 314 ms (+18×) | **25 ms (+50%)** |
| pgbench concurrent (8 clients, TPS) | 26 501 | 14 049 (−47%) | **25 873 (−2%)** |

WAL mode carries near-zero foreground write overhead because capture is fully asynchronous — the background worker reads the logical replication slot after the transaction commits. The remaining WAL mode overhead (~2–23%) reflects increased WAL volume from `wal_level=logical` (full column images) and any `synchronous_commit` interaction; it is not paid by the DML transaction itself. Trigger mode overhead scales with row count and column width because row-level triggers execute synchronously inside every DML transaction.

### Methodology

- **Hardware:** Same machine as Section 8 (AMD Ryzen 9 5900X, 64 GB RAM, NVMe SSD).
- **Dataset:** 4-column rows (~500 bytes each), integer PK. Single client unless noted (pgbench row uses 8 concurrent clients).
- **PostgreSQL settings:** `synchronous_commit = on`, `fsync = on`, `wal_level = logical`.
- **Cache state:** Buffer cache warm. All timing excludes `flashback_track()` setup.
- **WAL mode worker lag:** Worker configured at `worker_interval_ms = 75` ms. During the write benchmark, the worker runs concurrently but its lag is not included in the foreground numbers — the overhead column reflects only the foreground DML transaction cost.
- **Measurement:** Median of 3 runs. `VACUUM ANALYZE` run before each scenario.
- **Reproduce:** `./scripts/run_mode_comparison.sh` (trigger vs WAL) and `./scripts/run_benchmark.sh` (detailed trigger-mode breakdown).

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
| **Diff‑only UPDATE capture** (PK + changed cols only) | ✅ |
| **Batch / net‑effect restore replay** | ✅ |
| **lz4 compression on delta_log** (where available) | ✅ |
| **Multi‑database worker** (`target_databases` GUC) | ✅ |
| **Native partitioned table support** (per‑row triggers) | ✅ |
| **Parallel restore hints** (`flashback_restore_parallel`) | ✅ |
| **WAL capture mode** (async, near-zero write overhead) | ✅ |
| **`capture_mode` GUC** (`auto` / `wal` / `trigger`) | ✅ |
| **delta_log time‑partitioned** (monthly, auto‑managed) | ✅ |
| **Slot / memory GUCs** (`slot_name`, `restore_work_mem`, `index_build_work_mem`) | ✅ |
| **REPLICA IDENTITY preservation** (`FULL` / `DEFAULT` / `USING INDEX` round-trip) | ✅ |
| **Dependent view/matview recreation** (owner, reloptions, indexes, populate) | ✅ |
| **Non-destructive row recovery** (`flashback_recover_deleted` — re-inserts only missing rows, survivors untouched) | ✅ |
| **SET SCHEMA tracking** (schema move auto-detected via OID lookup; triggers recreated) | ✅ |
| **RENAME TABLE auto-tracking** (OID-based; capture trigger recreated transparently) | ✅ |
| **DROP TABLE recovery** (`flashback_restore` reconstructs a dropped table from delta history) | ✅ |
| **Classical INHERITS child preservation** (children detached before DROP, re-attached after swap) | ✅ |

## 11. Testing & Observability

### Test Suite

59 integration tests covering DML, DDL, schema evolution, multi‑table FK, checkpoints, edge cases, flashback query, partitioned tables, diff‑only UPDATE, batch replay, RBAC, WAL capture mode, SET SCHEMA tracking, classical INHERITS preservation, and non-destructive row recovery behaviors:

```bash
# Remove stale test data first (prevents mutex lock conflicts)
rm -rf target/test-pgdata
cargo pgrx test pg15  # test result: ok. 59 passed; 0 failed
cargo pgrx test pg16  # test result: ok. 59 passed; 0 failed
cargo pgrx test pg17  # test result: ok. 59 passed; 0 failed
cargo pgrx test pg18  # test result: ok. 59 passed; 0 failed
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
- **Test matrix**: PostgreSQL 15, 16, 17, 18 — `cargo pgrx test pg{15..18}` (59 tests each, verified locally and in CI)
- **Security audit**: `cargo audit`
- **Release workflow**: on `v*.*.*` tags, builds and publishes GitHub Releases

Local:
```bash
cargo fmt --all
cargo clippy --no-default-features -F pg17
rm -rf target/test-pgdata && cargo pgrx test pg17
```

## 12. Benchmarks

Capture mode comparison (baseline vs trigger vs WAL, 3-run median):

```bash
./scripts/run_mode_comparison.sh
```

Write-overhead benchmark (legacy trigger-mode baseline):

```bash
./scripts/run_benchmark.sh
```

Restore performance benchmark (10K → 1M rows):

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

pg_flashback natively supports partitioned tables. `flashback_track` automatically detects partitioned parents and attaches per‑row triggers (instead of the statement‑level transition‑table triggers used for regular tables):

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

### ⚠️ Critical Production Caveats

These are silent or hard-to-diagnose issues that can surprise you in production. Read before deploying.

**1. WAL slot disk accumulation**
In WAL mode the replication slot retains WAL segments until the background worker consumes them. If the worker crashes, is disabled (`pg_flashback.enabled = off`), or falls behind on a write-heavy cluster, unread WAL accumulates and **can fill disk**. PostgreSQL will not delete it automatically.

```sql
-- Monitor slot lag
SELECT slot_name, wal_status, lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'pg_flashback_slot';
```

Set a hard cap in `postgresql.conf` to prevent runaway growth:
```
max_slot_wal_keep_size = 10GB   -- adjust to your disk headroom
```

**2. `staging_events` is UNLOGGED — crash window in trigger mode**
In trigger mode, DML captured by triggers writes to `staging_events` (an UNLOGGED table). Events not yet flushed to `delta_log` by the background worker (up to `worker_interval_ms`, default 75 ms) are **lost on a PostgreSQL crash or hard reboot**. WAL mode has no DML crash window (events are durable in the replication slot at commit time); use it when DML durability matters.

**3. `flashback_restore` exclusive lock can pause under a long-running query**
The atomic shadow swap (`DROP original → RENAME shadow`) requires an `AccessExclusiveLock`. If there is a long-running `SELECT`, `VACUUM`, or open transaction on the table at restore time, the lock acquisition will block — and will in turn block all subsequent reads/writes behind it. Always restore during a low-traffic window or set a `lock_timeout` in your session first:
```sql
SET lock_timeout = '5s';
SELECT flashback_restore('orders', now() - interval '10 minutes');
```

**4. `flashback_track()` on a large table is expensive**
`flashback_track()` takes an immediate full-table snapshot. On a table with millions of rows this is a large `INSERT … SELECT` into `flashback.snapshots` and will hold a `ShareLock` for its duration. For large tables, run it during off-peak hours or use `pg_flashback.restore_work_mem` to speed up the snapshot.

**5. `wal_level = logical` is cluster-wide**
Setting `wal_level = logical` affects **all databases** on the cluster — not just the one using pg_flashback. It increases WAL volume by ~20–40% (full column images) and requires a PostgreSQL restart. On managed PostgreSQL services (AWS RDS, Google Cloud SQL, Azure Flexible Server) where `wal_level` cannot be raised, use `capture_mode = 'trigger'` instead. The `auto` default detects this and falls back automatically.

**6. Sequence restore can cause PK conflicts after restoring to an older state**
When `flashback_restore` replays a table to an older timestamp, `max(id)` in the restored data may be lower than the current sequence value. The sequence is rewound to match. This means the **next `INSERT` after restore reuses IDs** that were assigned after the restore point, causing a potential PK conflict if those rows still exist elsewhere (e.g. in a referencing FK table that was not restored). Always restore all FK-related tables together using the array form, or run `flashback_checkpoint()` on all related tables first.

---

### Symptom → Fix

| Symptom | Check / Fix |
|---------|-------------|
| Extension fails to load | Ensure `shared_preload_libraries = 'pg_flashback'` and restart PostgreSQL. |
| Background worker missing | `SELECT * FROM pg_stat_activity WHERE backend_type LIKE 'pg_flashback%';` |
| Restore returns 0 events | Worker must have flushed staging_events to delta_log. Check `SELECT count(*) FROM flashback.staging_events;` — should be 0 after the worker cycle. In WAL mode, check that the replication slot exists and the worker is running. |
| WAL capture not working | Confirm `wal_level = logical` and replication slot exists: `SELECT slot_name FROM pg_replication_slots;`. Run `flashback_track()` to create the slot. |
| Slot creation error in `flashback_track` | Occurs when called inside a transaction that already has writes. Worker will retry slot creation on its next cycle. |
| Triggers not firing on partitioned table | Ensure per‑row triggers are attached: `SELECT * FROM pg_triggers WHERE tgrelid = 'your_table'::regclass;`. Re-run `flashback_track()`. |
| TOAST / large row warnings | Increase `pg_flashback.max_row_size` or accept that oversized rows are skipped. Note: rows silently skipped at capture time will be missing after restore — check NOTICE output. |
| UNLOGGED table silently skipped (WAL mode) | UNLOGGED tables do not generate WAL; they are skipped in WAL mode with no error. Switch to `capture_mode = 'trigger'` or use a regular (logged) table. |
| Restore missing rows after `max_row_size` trim | Any row exceeding `pg_flashback.max_row_size` at capture time is skipped with a WARNING. Raise the GUC before tracking if you have wide JSONB/text columns. |
| Retention window expired error | `flashback_restore` raises an error when the target timestamp predates the oldest event in `delta_log`. Run `flashback_retention_status()` to see the restorable window. Add checkpoints more frequently to extend effective coverage without growing `delta_log`. |
| Dependent view ACLs not restored | ACL grants on views cannot be restored automatically; a NOTICE lists affected views. Re-grant manually after restore. |
| Test mutex conflict | Run `rm -rf target/test-pgdata` before `cargo pgrx test`. |
| Socket connection issues (pgrx dev) | Try: `psql -h ~/.pgrx -p 28817 postgres` |

## 15. Caveats & Limitations

- **WAL crash window:** In WAL mode, DML events are durable in the replication slot from the moment the transaction commits — no staging crash window. DDL events still flow through `staging_events` (UNLOGGED) and carry up to a `worker_interval_ms` (default 75 ms) crash window.
- **Trigger crash window:** `staging_events` is UNLOGGED. Events not yet flushed to `delta_log` are lost on a PostgreSQL crash. Use WAL mode for stricter DML durability.
- **WAL mode requirement:** `wal_level = logical` must be set cluster-wide before enabling WAL capture. `capture_mode = 'auto'` falls back to trigger mode when `wal_level < logical`.
- **Trigger-mode PITR accuracy — requires `track_commit_timestamp = on`:** In trigger mode, `event_time` is set to `clock_timestamp()` at statement execution inside the trigger, not at transaction commit. A long-running transaction that starts at T₀ and commits at T₁ will have `event_time ≈ T₀`, so `flashback_restore` and `flashback_query` may replay it even when the target timestamp is between T₀ and T₁. To get commit-time-correct PITR in trigger mode, add `track_commit_timestamp = on` to `postgresql.conf` (restart required). The background worker will then use `pg_xact_commit_timestamp()` for both `event_time` and `committed_at`. `flashback_track()` emits a NOTICE when trigger mode is active and `track_commit_timestamp` is off. WAL mode is always commit-time-correct regardless of this setting.
- **Long-running transaction visibility:** Logical decoding delivers changes in commit order, not statement order. Events from transactions that start before but commit after a target restore timestamp may land in a later partition window. For practical workloads (OLTP, < 1-minute transactions) this is not observable; for long-running batch transactions spanning multiple minutes, committed_at can diverge from event_time by the batch duration.
- **Large DDL snapshot cost:** TRUNCATE and DROP events inline the full table contents into `delta_log.old_data` as JSONB. On tables > ~100K rows this creates a large ephemeral write. Consider taking a manual `flashback_checkpoint()` before planned large-scale truncations to contain this cost.
- **Non-PK table restore ceiling:** Row-by-row replay path (tables without a primary key) processes ~50–80K events/s vs ~400–550K events/s on the batch path. Restoring > 500K events on a no-PK table will be noticeably slow. Adding a surrogate PK or using `flashback_checkpoint()` before large operations is strongly recommended.
- **Partitioned table INSERT/DELETE capture:** Per-row triggers fire on each partition individually. This is correct but carries higher per-row overhead than statement-level bulk triggers on regular tables. For very high-throughput partitioned workloads, prefer WAL mode.
- **Replication & HA topologies:** pg_flashback is tested on single-node PostgreSQL. On streaming replication standbys the extension is typically not active (no shared_preload_libraries on replicas by default). Logical replication subscribers are not supported as capture sources. pg_flashback should work on Patroni/repmgr primaries; behaviour after failover (slot continuity) has not been tested and manual slot recreation may be required.
- **pg_upgrade / major version:** Extension data is JSONB and schema-version-tracked. pg_upgrade is supported but requires reinstalling the extension binary for the new major version and re-running `CREATE EXTENSION` or `pg_restore` of the schema.

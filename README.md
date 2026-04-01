# pg_flashback

**Table-level point-in-time restore for PostgreSQL.** No full-cluster PITR, no pg_basebackup — just restore the table you need, to the second you need.

```sql
-- Oops, someone ran DELETE FROM orders WHERE status = 'pending';
SELECT flashback_restore('orders', '2026-04-01 14:30:00');
-- ↑ Table restored. 847 rows recovered. Done.
```

## Why?

PostgreSQL has no native way to restore a single table without restoring the entire cluster. When a developer runs a bad `DELETE` or `UPDATE`, your only options today are:

- Full cluster PITR (restore everything, extract one table)
- Logical backups + manual surgery
- Hope you have a recent `pg_dump`

**pg_flashback** captures row-level changes continuously and restores any tracked table to any point in time — in seconds, not hours.

## Quick Start

```bash
# Build & install (requires Rust + cargo-pgrx 0.16.x)
cargo pgrx install --no-default-features -F pg17

# Add to postgresql.conf
shared_preload_libraries = 'pg_flashback'

# Restart PostgreSQL, then:
psql -c "CREATE EXTENSION pg_flashback;"
```

```sql
-- Start tracking a table
SELECT flashback_track('orders');

-- ... normal operations happen ...

-- Disaster: accidental mass delete
DELETE FROM orders WHERE created_at < '2026-01-01';

-- Restore to 30 seconds ago
SELECT flashback_restore('orders', now() - interval '30 seconds');

-- Verify
SELECT count(*) FROM orders;  -- All rows back
```

## Features

| Feature | Status |
|---------|--------|
| Single-table restore to any timestamp | ✅ |
| Multi-table restore in one transaction | ✅ |
| Schema evolution awareness (ADD/DROP COLUMN) | ✅ |
| DDL capture (TRUNCATE, DROP TABLE, ALTER TABLE) | ✅ |
| Automatic checkpoints for fast restore | ✅ |
| Configurable retention policy | ✅ |
| Serial/sequence restoration | ✅ |
| Global kill switch (`pg_flashback.enabled`) | ✅ |
| Monitoring view (`pg_stat_flashback`) | ✅ |
| Restore audit log | ✅ |
| Large row protection (TOAST guard) | ✅ |
| Concurrent restore safety (advisory lock) | ✅ |

## Write Overhead Benchmark

Measured on PostgreSQL 17, single-node:

| Scenario | Without | With pg_flashback | Overhead |
|----------|---------|-------------------|----------|
| Bulk INSERT 100K rows | 133 ms | 261 ms | **2.0x** |
| Single-row UPDATE 10K | 49 ms | 132 ms | **2.7x** |
| Mixed DML 15K ops | 37 ms | 99 ms | **2.7x** |
| Wide table UPDATE (15 cols, 5K rows) | 11 ms | 91 ms | **8.0x** |

Bulk INSERT uses statement-level triggers with transition tables. UPDATE uses per-row triggers. Staging writes use `json`; the background worker converts to `jsonb` asynchronously.

## Architecture

```
DML (INSERT/UPDATE/DELETE)
  │
  ▼
AFTER triggers ──► staging_events (UNLOGGED, json)
                       │
                       ▼ (background worker, every 75ms)
                  delta_log (jsonb) ──► snapshots (checkpoints)
                       │
DDL hook ──────────────┘
(TRUNCATE/DROP/ALTER)

flashback_restore(table, timestamp)
  │
  ├─ Find nearest snapshot/checkpoint
  ├─ Recreate table structure from schema_versions
  ├─ Load base image
  ├─ Replay deltas forward to target time
  └─ Restore serial sequences
```

## API Reference

### Tracking

```sql
-- Start tracking
SELECT flashback_track('schema.table');

-- Stop tracking (cleans up all metadata)
SELECT flashback_untrack('schema.table');
```

### Restore

```sql
-- Single table
SELECT flashback_restore('orders', '2026-04-01 14:30:00'::timestamptz);

-- Multiple tables (same transaction)
SELECT flashback_restore(ARRAY['orders', 'order_items'], now() - interval '5 minutes');
```

### Checkpoints & Retention

```sql
-- Manual checkpoint
SELECT flashback_checkpoint('orders');

-- Check retention status
SELECT * FROM flashback_retention_status();
```

### Monitoring

```sql
-- Dashboard view
SELECT * FROM flashback.pg_stat_flashback;

-- Change history for a table
SELECT * FROM flashback_history('orders', interval '1 hour');

-- Audit log of all restores
SELECT * FROM flashback.restore_log;
```

### Configuration (GUCs)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pg_flashback.enabled` | `on` | Global kill switch. `SET pg_flashback.enabled = off;` stops all capture. Superuser only. |
| `pg_flashback.max_row_size` | `8kB` | Rows larger than this are skipped with a WARNING (TOAST protection). |
| `pg_flashback.worker_interval_ms` | `75` | Background worker flush interval. |
| `pg_flashback.worker_batch_size` | `4096` | Rows per worker flush cycle. |

## Requirements

- PostgreSQL 17
- Rust stable + [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx) 0.16.x
- `shared_preload_libraries = 'pg_flashback'` in postgresql.conf

## Build

```bash
# Install pgrx if needed
cargo install cargo-pgrx --version '=0.16.1'
cargo pgrx init --pg17 /path/to/pg_config

# Build & install
cargo pgrx install --no-default-features -F pg17

# Run test suite (14 tests)
psql -f sql/senior_dba_test_suite.sql
```

## Test Suite

14 production-grade tests covering:

| Test | What it validates |
|------|-------------------|
| T1 | Large volume (5K rows) restore |
| T2 | Column type fidelity (JSONB, boolean, numeric) |
| T3 | FK multi-table restore with orphan check |
| T4 | Boundary time precision (microsecond accuracy) |
| T5 | Schema evolution (ADD/DROP COLUMN) |
| T6 | Track/untrack/re-track lifecycle |
| T7 | TRUNCATE + re-insert restore |
| T8 | Serial sequence post-restore |
| T9 | Checkpoint-based restore |
| T10 | Sequential restores (3 points in time) |
| T11 | NULL-heavy + JSONB data |
| T12 | flashback_history() query |
| T13 | Wide table (15+ columns) |
| T14 | DO block + serial restore |

## License

PostgreSQL License

## Roadmap

See [ROADMAP.md](ROADMAP.md) for Phase 3 (enterprise) plans.

Highlights:

- Step 2-4: logical slot and tuple extraction pipeline
- Step 5: async flush path
- Step 6: single-table restore
- Step 7: DDL restore behavior
- Step 8: checkpoint + multi-table restore
- Step 9: history, retention, untrack cleanup
- Step 10: schema evolution restore

## Troubleshooting

### pgrx command mismatch

If a command is missing, verify with:

```bash
cargo pgrx --help
```

### Feature conflicts across PostgreSQL versions

Prefer explicit feature selection:

```bash
cargo pgrx install --no-default-features -F pg17
```

### Socket connection issues with pgrx-managed PostgreSQL

If local connection fails, try:

```bash
psql -h ~/.pgrx -p 28817 postgres
```

### shared_preload_libraries formatting issues

If extension preload fails after shell-based edits, normalize the value to a clean comma-separated list without nested quotes.

### Startup crash with large shared memory setup

Try starting with unlimited stack:

```bash
ulimit -s unlimited
cargo pgrx start pg17
```

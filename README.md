# pg_flashback

[![CI](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml/badge.svg)](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml)

Table-level point-in-time recovery and time-travel queries for PostgreSQL.
The adopted design has a preflighted local snapshot/delta profile and a
pgBackRest-backed profile for tables that do not fit the local capacity,
change-rate, write-stall or RTO budget. Profile choice is not a size-only
heuristic. Built with Rust + pgrx 0.16.1 for PostgreSQL 15–18.

> **Development status:** the WAL-local COMMIT-LSN milestone is implemented and
> qualified, but the project as a whole is still pre-release. Generation-aware
> retention, capture/maintenance worker isolation, backup-profile coverage
> finalization and versioned upgrade packaging remain release gates. Read the binding
> [storage policy](docs/STORAGE_POLICY.md),
> [coverage design](docs/COVERAGE_MODEL.md) and
> [release scope](docs/RELEASE_SCOPE.md) before evaluating the project.
> The first-release target is an ordinary LOGGED, non-partitioned table;
> broader legacy demos are not release-qualified merely because they run.

## 1. Why This Extension?

- The correctness-qualified local path restores only a target COMMIT LSN that
  belongs to exactly one verified coverage generation and lies at or before
  its proven capture watermark.
- The backup helper hides full-cluster PITR and table extraction behind a
  validated workflow; no manual `pg_dump` surgery against production.
- **WAL-only qualified local capture:** PostgreSQL logical decoding supplies a
  total COMMIT-LSN order. Explicit trigger mode remains available only as a
  legacy/experimental compatibility path and creates no qualified generation.
- WAL decoding is asynchronous, but it is not free: `REPLICA IDENTITY FULL`
  increases UPDATE/DELETE WAL volume and the worker stores captured history.
- **Diff‑only UPDATE capture** — stores only PK columns + changed columns per UPDATE, reducing delta storage by up to 40× on wide tables.
- DDL captured (TRUNCATE, DROP, ALTER) via `ProcessUtility_hook`.
- Includes `flashback_query_lsn()` — query an admitted past table state without
  restoring. It uses the historical schema from `schema_versions` and handles
  DROP events.
- **Non-destructive row recovery** — `flashback_recover_deleted_lsn()`
  re-inserts only rows missing now, after the same generation admission used by
  restore/query.
- **DROP TABLE recovery** — the WAL-local LSN path captures trusted DDL through
  protected pending metadata and reconstructs a dropped table from one proven
  generation.
- **SET SCHEMA and RENAME TABLE** are auto-tracked: OID-based lookup detects the change and recreates the capture trigger under the new name/schema.
- **Classical table inheritance preserved** during restore: child tables are detached before the DROP and re-attached after shadow rename.
- Per‑table advisory locks allow concurrent restores of unrelated tables.
- **Multi‑database worker** — single extension install can track tables across multiple databases simultaneously.
- **Partitioned-table implementation demo** — the current code uses per-row
  triggers, but partitioned targets are outside the first-release contract.
- Includes a TOAST size limit, progress reporting, restore audit log, capture
  switch and monitoring views; their coverage-safe failure semantics are still
  release blockers.
- **Backup profile for local-budget-ineligible tables:** no base-table copy and no row-delta
  duplication; native PostgreSQL PITR runs in a private cluster and returns a
  validated one-table artifact through snapshot-direct or classic pgBackRest
  restore.

## 2. Architecture Overview

| Component | Purpose |
|-----------|---------|
| Logical decoder | Filters by tracked relation OID, decodes DML, and records transaction COMMIT LSN separately from row-change LSN. Client-supplied logical-message bodies are never trusted. |
| AFTER Triggers (statement + row) | Legacy/experimental compatibility capture into LOGGED `staging_events`; it does not create qualified local coverage. |
| `ProcessUtility_hook` | Intercepts DDL and writes authoritative TRUNCATE/DROP/ALTER metadata to a protected LOGGED pending table in the user's transaction. |
| Background Worker (`flashback_worker`) | Consumes the logical slot, promotes complete commits, resolves pending generation boundaries and advances the inclusive watermark. Capture/maintenance worker isolation remains a release gate. |
| `delta_log` | Generation/stream-bound JSONB event store, partitioned by `committed_at`; qualified events carry row-change and transaction COMMIT LSNs. |
| Coverage generations | Exact locked base + one immutable WAL stream epoch + an inclusive complete-commit watermark. Slot discontinuity freezes the old frontier and opens a durable gap; `flashback_reanchor()` creates a new exact base. |
| `schema_versions` | Tracks column definitions, constraints, indexes, triggers, and RLS policies per schema change. |
| `flashback_restore_lsn()` | Admits one COMMIT-LSN prefix, materializes it, swaps atomically, then leaves a successor base pending until its real commit record is consumed. |
| `flashback_query_lsn()` / `flashback_recover_deleted_lsn()` | Read or recover from the same admitted immutable generation without nearest-snapshot fallback. |
| `flashback_restore_parallel()` | Restore with parallel query hints (`max_parallel_workers_per_gather`). Emits per‑partition guidance for partitioned tables. |
| `flashback_query_lsn()` | Reconstructs one admitted COMMIT-LSN state in a temporary table; caller-side filtering remains SECURITY INVOKER. |
| `pg-flashback-recovery` | External backup-profile executor: pgBackRest selection, private native PITR, validation, extraction and crash-safe cleanup. |
| Backup restore controller | Verifies the artifact checksum, imports into `flashback_import` and asks the extension to perform the validated transactional swap. |

Local capture does not require DML statement rewrites, but deployment,
preflight, maintenance and recovery remain explicit operator responsibilities.
The backup profile additionally requires the external controller.

The diagram keeps the trigger path visible for compatibility, but only the WAL
path creates correctness-qualified coverage.

```
── Trigger mode ──────────────────────────────────────────────────────────
DML (INSERT / UPDATE / DELETE)
  │
  ▼
AFTER triggers ──► staging_events (LOGGED, JSONB, diff-only UPDATE)
                       │
                       ▼  background worker (every 75 ms)
                  delta_log (JSONB, lz4 compressed)
                       ▲
── WAL mode ────────────┼────────────────────────────────────────────────
DML (INSERT / UPDATE / DELETE)
  │
  ▼
WAL (wal_level=logical)
  │
  ▼
logical replication slot (pg_flashback_<dbname>)
  │
  ▼  background worker reads slot (every 75 ms)
  └──────────────────────────────────────────►  delta_log (direct, no staging)
                       ▲
── Both modes ──────────┼────────────────────────────────────────────────
DDL hook ──────────────┘
(Trigger DDL uses staging; qualified WAL DDL uses protected
`pending_wal_events` and is promoted only with its trusted COMMIT record.)

Legacy compatibility flashback_restore(table, timestamp)
  ├─ Find nearest snapshot / checkpoint
  ├─ Recreate table from schema_versions (shadow table, crash-safe)
  ├─ Bulk-load base image (INSERT … SELECT)
  ├─ Replay deltas filtered by event_time ≤ target (legacy; not a proven WAL prefix)
  ├─ Batch/net-effect path for PK tables; row-by-row for tables without PK
  ├─ Atomic swap: detach INHERITS children → DROP original → RENAME shadow → re-attach children (brief exclusive lock)
  ├─ Recreate dependent views/matviews (owner, reloptions, indexes, populate; ACL via NOTICE)
  ├─ Restore serial sequences
  └─ Log to restore_log + RAISE NOTICE progress
```

The local profile is WAL-only by policy decision T-01/A. Its primitive target
is a transaction COMMIT LSN. `flashback_resolve_target()` is only a convenience
planner: it pins a closed frontier and returns one LSN if the observed
timestamp mapping describes exactly one contiguous WAL prefix. Equal-time
collisions, clock inversions, gaps, incomplete frontiers and multiple matching
generations are rejected. Execution always uses the returned LSN; it never
filters individual events by timestamp.

A restore creates the successor base under the swap transaction, but that
generation remains `building` until the worker observes the transaction's real
COMMIT record. Until then `flashback_health()` reports `pending` and no new
target is admitted. Slot loss or unexplained external advancement freezes the
last proven watermark, opens a durable gap and requires `flashback_reanchor()`;
the missing interval never becomes valid retroactively.

## 3. Requirements

### PostgreSQL

**Tested versions:** PostgreSQL 15, 16, 17, 18 (68/68 tests pass on all four,
verified locally; CI runs the same matrix)
**Compile-supported:** PostgreSQL 15 – 18 (pgrx feature flags)

**Legacy smoke evidence:** both capture modes completed limited 1,000+ row
mass-delete/update scenarios (trigger restore ~58 ms, WAL restore ~82 ms). Those
runs validate functional paths only; they are not coverage/correctness
qualification and do not supersede the lifecycle audit's silent-wrong-result
reproductions.

**Required `postgresql.conf` settings:**
```
shared_preload_libraries = 'pg_flashback'

# WAL mode only:
wal_level = logical

```

A restart is required after changing `shared_preload_libraries` or `wal_level`.
The qualified WAL-local profile does not depend on `track_commit_timestamp`.

### Rust Toolchain

- Rust ≥ 1.85 (stable; required by the locked dependency set)
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

Build the backup recovery helper separately:

```bash
cargo build --release --locked \
  --manifest-path tools/pg_flashback_recovery/Cargo.toml
```

### Install a tagged binary archive

The PostgreSQL-major release archives use a prefix-independent layout. Check
that `PG_MAJOR` matches the target server, then install the three extension
files into the directories reported by that server's `pg_config`:

```bash
test "$(cat PG_MAJOR)" = "$(pg_config --version | awk '{print $2}' | cut -d. -f1)"
sudo install -m 0755 lib/pg_flashback.so "$(pg_config --pkglibdir)/pg_flashback.so"
sudo install -m 0644 share/extension/pg_flashback.control \
  share/extension/pg_flashback--*.sql "$(pg_config --sharedir)/extension/"
```

Prebuilt release archives target x86_64 Linux. Other Linux architectures can
build the same tagged source with the commands above.

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

Configure this database in `pg_flashback.target_databases`, restart PostgreSQL,
and run each statement below in normal psql autocommit mode. Tracking must be
the first write in a dedicated READ COMMITTED transaction.

```sql
SELECT flashback_track('public.orders');

-- Wait until the worker resolves the initial boundary COMMIT record.
SELECT * FROM flashback_health();

-- … normal operations happen …

-- Disaster: accidental mass delete
SELECT clock_timestamp() AS before_delete \gset
DELETE FROM orders WHERE created_at < '2026-01-01';

-- Once the delete COMMIT has been consumed, resolve the human timestamp to
-- one proven WAL prefix. Collision/inversion/incomplete evidence is rejected.
SELECT resolved_lsn AS target_lsn
FROM flashback_resolve_target(
    'public.orders', :'before_delete'::timestamptz
) \gset

-- Inspect past state without changing the live table.
SELECT *
FROM flashback_query_lsn(
    'public.orders', :'target_lsn'::pg_lsn, NULL
) AS t(id bigint, total numeric, status text)
WHERE total > 100;

-- Or atomically restore the table to that exact COMMIT-LSN prefix.
SELECT flashback_restore_lsn('public.orders', :'target_lsn'::pg_lsn);

-- The successor is pending until the worker consumes the restore COMMIT.
SELECT * FROM flashback_health();
```

### Backup-helper functional demo

The backup profile avoids the initial table copy and continuous row-delta
duplication. It records DDL markers while pgBackRest remains responsible for
physical backups and archived WAL:

```sql
SELECT flashback_track_backup('public.orders', 'app_repo2');

-- After a DROP/TRUNCATE/ALTER, choose the pre-DDL LSN marker.
SELECT *
FROM flashback_backup_disaster_points('public.orders', interval '2 hours');
```

The operating-system controller then performs native PITR in a private
cluster, imports one validated artifact and asks the extension to swap it:

```bash
scripts/pg_flashback_backup_restore.sh \
  --config /etc/pg_flashback/app_repo2.json \
  --dbname appdb \
  --table public.orders \
  --target-lsn 0/8F12340
```

This path has a deliberately narrower first-release support contract. Read
the [operator runbook](docs/BACKUP_RESTORE_RUNBOOK.md) and
[supported scope](docs/RELEASE_SCOPE.md) before enabling it.
The helper/result contract is implemented, but coverage-generation integration
is still pending; this is not yet an end-to-end release-qualified procedure.
Release-qualified initial backup tracking must first commit and resolve a
durable tracking marker, then remain unanchored until a new full backup whose
start LSN is strictly after that marker activates at its verified stop
anchor. It cannot adopt a pre-existing or already-running backup.
After a backup-profile production swap, pg_flashback must create no local row
snapshot. The adopted protocol records a pending swap transaction/unanchored
gap, seals the predecessor after resolving the swap commit and deliberately has
zero active generations. New targets remain rejected until a new completed
full backup whose start LSN is strictly after that resolved commit is
verified and activated at its stop boundary. The current finalizer does not
wire that protocol yet.

## 6. Configuration (GUCs)

All GUCs live under `pg_flashback.*`. They can be set globally (`postgresql.conf`, `ALTER SYSTEM`) or per role/database (`ALTER ROLE … SET`).

| GUC | Default | Reload | Description |
|-----|---------|--------|-------------|
| `enabled` | `on` | SIGHUP | `off` stops capture, but the current runtime does not persist the required coverage break. Do not toggle it while tables are tracked. |
| `capture_mode` | `auto` | SIGHUP | `auto` selects WAL only when `wal_level=logical`; it never silently downgrades qualified tracking to triggers. Explicit `trigger` is legacy/experimental. Do not change mode while qualified lifecycles are active: synchronous break recording remains a release gate. |
| `slot_name` | `pg_flashback_<dbname>` | Suset | Per-database logical slot name. Slot loss, replacement, identity change or unexplained external advancement freezes the old epoch, opens a gap and requires re-anchor. |
| `restore_work_mem` | `256MB` | Suset | `work_mem` override for snapshot bulk load during `flashback_restore`. Higher values speed up large table restores. |
| `index_build_work_mem` | `512MB` | Suset | `maintenance_work_mem` override for deferred index builds on the shadow table during restore. |
| `max_row_size` | `64kB` | SIGHUP | Legacy trigger-capture limit. The qualified decoder path does not silently skip events through this GUC. |
| `worker_interval_ms` | `75` | SIGHUP | Background worker flush interval in milliseconds. |
| `worker_batch_size` | `4096` | SIGHUP | Maximum rows per worker flush cycle. |
| `target_database` | `postgres` | Restart | Database the background worker connects to (single‑DB mode). Overridden by `target_databases`. |
| `target_databases` | *(unset)* | Restart | Comma-separated list of databases for multi‑DB mode. Each database gets its own worker. Example: `'app,analytics,audit'`. |
| `max_workers` | `4` | Restart | Maximum number of background workers registered at startup. Extra workers beyond the database count exit gracefully. |

All GUCs except those marked *Restart* take effect via `SIGHUP`. Do not toggle
`enabled` while qualified tables are active: synchronous disable/enable gap
recording remains a release gate.

## 7. SQL API Reference

### Tracking

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_track(table)` | `boolean` | In WAL/auto-logical mode, creates a dedicated lifecycle, verified stream binding and exact locked base. Must be the first write in a dedicated READ COMMITTED transaction and the database must have a configured worker. Explicit trigger mode creates legacy state only. |
| `flashback_reanchor(table)` | `bigint` | After a broken stream, creates a new exact base on the current WAL epoch. The intervening gap remains permanently rejected. The new generation activates only when its real COMMIT record is consumed. |
| `flashback_untrack(table)` | `void` | Stop tracking and restore the original replica identity. Retires the lifecycle; retracking allocates a new identity. |
| `flashback_track_backup(table, helper_profile)` | `boolean` | Enable legacy metadata-only backup tracking: no row snapshot or DML deltas. Ordinary LOGGED, non-partitioned tables only. |
| `flashback_set_backup_coverage(table, first_lsn, latest_lsn)` | `void` | Record the legacy controller assertion; it does not yet create a verified coverage generation. |
| `flashback_backup_disaster_points(table [, lookback])` | `SETOF record` | List DDL disaster markers and pre-DDL target LSNs. |

### Restore

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_restore_lsn(table, pg_lsn)` | `bigint` | Correctness-qualified single-table restore. Pins one generation, replays one contiguous COMMIT-LSN prefix and creates a pending post-restore successor base. |
| `flashback_restore_lsn(tables[], pg_lsn)` | `bigint` | Multi-table qualified restore. Acquires stable lifecycle locks in ID order, orders FK parents before children and rejects cycles. |
| `flashback_resolve_target(table, timestamptz)` | `SETOF record` | Convenience planner returning one `resolved_lsn` only when the timestamp is a unique, complete WAL-prefix cut inside one pinned frontier. Collisions/inversions fail closed. |
| `flashback_restore(table, timestamptz)` | `bigint` | Legacy compatibility API; explicitly rejects a correctness-qualified WAL lifecycle. Resolve the timestamp and call `flashback_restore_lsn()` instead. |
| `flashback_restore(tables[], timestamptz)` | `bigint` | Legacy FK-ordered timestamp API; outside the qualified contract. |
| `flashback_restore_parallel(table, timestamptz [, num_workers])` | `TABLE(restored_table text, events_applied bigint)` | Legacy timestamp restore with parallel-query hints; outside the WAL-first release contract. |
| `flashback_query_lsn(table, pg_lsn, NULL)` | `SETOF record` | Qualified read-only materialization. Apply filters in the caller's outer query; the SECURITY DEFINER function rejects a non-NULL free-form filter. |
| `flashback_query(table, timestamptz [, filter_clause])` | `SETOF record` | Legacy compatibility API; rejects correctness-qualified WAL lifecycles. |

### Recovery

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_recover_deleted_lsn(table, pg_lsn)` | `bigint` | Qualified PK-based recovery that inserts only rows missing from the live table. |
| `flashback_recover_deleted(table, timestamptz)` | `bigint` | Legacy compatibility API; rejects correctness-qualified WAL lifecycles. |

### Checkpoints & Retention

| Function | Returns | Description |
|----------|---------|-------------|
| `flashback_checkpoint(table)` | `bigint` | Legacy trigger checkpoint; explicitly rejected for qualified WAL generations. Use controlled `flashback_reanchor()` for a new local boundary. |
| `flashback_retention_status()` | `SETOF record` | Legacy age/storage status; it is not generation/gap-aware coverage health. |

### Monitoring & Audit

| Function / View | Description |
|-----------------|-------------|
| `flashback.pg_stat_flashback` | Dashboard view: tracked tables, pending events, delta storage, restore counts. |
| `flashback_health()` | Generation/stream health, watermark, pending boundary and open-gap projection for every active lifecycle. |
| `flashback_history(table, interval)` | Recent change history for a table. |
| `flashback.restore_log` | Audit log of all restore operations (who, when, what, success/failure). |

### Schema & Lock Management

| Function | Description |
|----------|-------------|
| `flashback_collect_schema_def(oid)` | Collect full schema definition (columns, PKs, constraints, indexes, triggers, RLS). |
| `flashback_is_restore_in_progress(oid)` | Check if a restore is running in the current backend. |
| `flashback_set_restore_in_progress(bool)` | Set restore‑in‑progress flag (internal use; superuser only). |

## 8. Restore Performance

These numbers are legacy replay microbenchmarks. They measure execution speed
after a base/event set has been chosen; they do not validate that the chosen
set is complete, race-free or admissible under the adopted coverage model.

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

WAL decoding removes synchronous row-trigger work from the application
transaction, but it is not zero-overhead. The foreground figures above are
historical microbenchmarks, not a capacity guarantee. In the current real WAL
E2E workload, changing the same UPDATE/DELETE table from default replica
identity to `REPLICA IDENTITY FULL` raised application WAL from about 4.69 MiB
to 6.79 MiB (1.45×); including worker/delta writes, tracked total WAL was about
12.6 MiB (2.69× the default-identity application baseline). Measure the real
row width and update/delete mix before choosing the local profile.

### Methodology

- **Hardware:** Same machine as Section 8 (AMD Ryzen 9 5900X, 64 GB RAM, NVMe SSD).
- **Dataset:** 4-column rows (~500 bytes each), integer PK. Single client unless noted (pgbench row uses 8 concurrent clients).
- **PostgreSQL settings:** `synchronous_commit = on`, `fsync = on`, `wal_level = logical`.
- **Cache state:** Buffer cache warm. All timing excludes `flashback_track()` setup.
- **WAL mode worker lag:** Worker configured at `worker_interval_ms = 75` ms. During the write benchmark, the worker runs concurrently but its lag is not included in the foreground numbers — the overhead column reflects only the foreground DML transaction cost.
- **Measurement:** Median of 3 runs. `VACUUM ANALYZE` run before each scenario.
- **Reproduce:** `./scripts/run_mode_comparison.sh` (trigger vs WAL) and `./scripts/run_benchmark.sh` (detailed trigger-mode breakdown).

## 10. Features

`✅` below means that the functional code path exists. It does not make a
recovery API release-qualified; all past-state reads/restores remain subject to
the generation-admission gates called out explicitly below.

| Feature | Status |
|---------|--------|
| Single-table restore to a verified generation target | ✅ WAL-local COMMIT-LSN path |
| Release-qualified local COMMIT-LSN target API | ✅ T-01/A implemented |
| Multi‑table restore in one transaction | ✅ COMMIT-LSN path |
| Flashback Query (`SELECT AS OF`) | ✅ COMMIT-LSN path; timestamp API legacy |
| Schema evolution awareness (ADD / DROP / ALTER COLUMN) | ✅ |
| DDL capture (TRUNCATE, DROP TABLE, ALTER TABLE, RENAME) | ✅ |
| Automatic periodic full checkpoints | ❌ rejected by adopted policy |
| Generation-aware retention | 🚧 legacy age-based purge is not release-safe |
| Serial / sequence restoration | ✅ |
| Trigger & RLS policy preservation during restore | ✅ |
| Generated column awareness | ✅ |
| Coverage-safe capture disable/enable | 🚧 synchronous `enabled` transition gap remains a release gate |
| Monitoring view (`pg_stat_flashback`) | ✅ |
| Restore audit log + progress reporting | ✅ |
| Large row coverage invalidation | ✅ qualified WAL decoder does not use the legacy trigger size-skip path |
| Common per-tracking coverage lock | ✅ WAL lifecycle/restore/re-anchor paths |
| Native JSONB pipeline (zero conversion) | ✅ |
| Bulk snapshot restore (`INSERT … SELECT`) | ✅ |
| Composite delta_log indexes for fast scans | ✅ |
| FK‑aware multi‑table restore ordering | ✅ |
| Circular FK protection (depth limit) | ✅ |
| **Diff‑only UPDATE capture** (PK + changed cols only) | ✅ |
| **Batch / net‑effect restore replay** | ✅ |
| **lz4 compression on delta_log** (where available) | ✅ |
| **Multi‑database worker** (`target_databases` GUC) | ✅ |
| **Partitioned-table path** (per-row triggers) | legacy demo; ❌ first release |
| **Parallel restore hints** (`flashback_restore_parallel`) | ✅ |
| **WAL capture mode** (async; measured WAL amplification) | ✅ |
| **Coverage-safe `capture_mode` changes** | 🚧 synchronous break + new epoch before mode switch remains a release gate |
| **delta_log time‑partitioned** (monthly, auto‑managed) | ✅ |
| **Slot lifecycle changes** (`slot_name`) | ✅ slot loss/replacement/external advancement freeze epoch and open a gap |
| **Restore/index memory GUCs** | ✅ |
| **REPLICA IDENTITY preservation** (`FULL` / `DEFAULT` / `USING INDEX` round-trip) | ✅ |
| **Dependent view/matview recreation** (owner, reloptions, indexes, populate) | ✅ |
| **Non-destructive row recovery** (`flashback_recover_deleted` — re-inserts only missing rows, survivors untouched) | ✅ |
| **SET SCHEMA tracking** (schema move auto-detected via OID lookup; triggers recreated) | ✅ |
| **RENAME TABLE auto-tracking** (OID-based; capture trigger recreated transparently) | ✅ |
| **DROP TABLE recovery** (`flashback_restore` reconstructs a dropped table from delta history) | ✅ |
| **Classical INHERITS child preservation** (children detached before DROP, re-attached after swap) | ✅ |
| **Backup-backed recovery profile** (pgBackRest + private native PITR + validated table swap) | ✅ helper path; coverage integration still gated |

## 11. Testing & Observability

### Test Suite

64 PostgreSQL tests plus 4 decoder unit tests cover DML, DDL, schema
evolution, multi-table FK, checkpoints, edge cases, query/recovery, RBAC,
generation/stream contracts, timestamp collision/inversion, frozen frontiers,
persistent gaps and WAL-mode behavior:

```bash
# Remove stale test data first (prevents mutex lock conflicts)
rm -rf target/test-pgdata
cargo pgrx test pg15  # test result: ok. 68 passed; 0 failed
cargo pgrx test pg16  # test result: ok. 68 passed; 0 failed
cargo pgrx test pg17  # test result: ok. 68 passed; 0 failed
cargo pgrx test pg18  # test result: ok. 68 passed; 0 failed
```

### Monitoring Queries

The retention function reports legacy age/storage state only; it is not proof
of recoverability. `flashback_health()` is the coverage view.

```sql
-- Dashboard
SELECT * FROM flashback.pg_stat_flashback;

-- Qualified generation, stream, watermark and gap health
SELECT * FROM flashback_health();

-- Legacy retention/storage status (not coverage health)
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
- **Test matrix**: PostgreSQL 15, 16, 17, 18 — `cargo pgrx test pg{15..18}` (68 tests each, verified locally on all four; CI runs the same matrix on every push)
- **Security audit**: `cargo audit`
- **Recovery E2E**: 27 real pgBackRest/native-PITR success and fail-closed checks
- **Release workflow**: signed-off `v*.*.*` tags build portable x86_64 Linux PostgreSQL 15–18 and helper artifacts, checksums, and a draft GitHub Release

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

### Backup-backed recovery

This profile is selected when local capacity, change rate, write-stall or RTO
budgets do not fit. It selects a completed pgBackRest full backup, uses an XFS
reflink clone when the real capability probe succeeds (or a safe classic
restore fallback), runs native PostgreSQL LSN recovery, validates/extracts one
ordinary table and completes a checksum-verified extension shadow swap.

- [PoC design and reproduction guide](docs/LARGE_DB_POC.md)
- [Measured results and architecture decision](docs/LARGE_DB_POC_RESULTS.md)
- [Machine-readable benchmark summary](docs/benchmarks/large-db-poc-20260716.json)
- [Machine-readable recovery qualification](docs/qualification/recovery-e2e-pg17-pgbackrest-2.53.1.json)
- [External recovery helper contract](docs/RECOVERY_HELPER_DESIGN.md)
- [Backup restore operator runbook](docs/BACKUP_RESTORE_RUNBOOK.md)
- [First-release support contract](docs/RELEASE_SCOPE.md)

Run a 500 MiB local comparison:

```bash
./scripts/run_large_db_restore_poc.sh 500
```

Probe the configured repository and snapshot capability before enabling a
profile:

```bash
cargo run --manifest-path tools/pg_flashback_recovery/Cargo.toml -- \
  probe --config tools/pg_flashback_recovery/examples/helper.json
```

The real E2E suite exercises snapshot-direct, classic fallback, missing WAL,
old targets, backup/expire races, cancellation, SIGKILL reconciliation,
timeouts, quota, identity/schema/fingerprint mismatch, artifact corruption and
controller failure cleanup, the real pre-DROP marker and final extension
import/swap:

```bash
./scripts/run_recovery_helper_e2e.sh
```

## 13. Operations & Integration

### Common Tasks

These commands exercise the WAL-local APIs in a development instance. Resolve
human time to LSN first; use the returned LSN for every operation.

```sql
-- Start tracking
SELECT flashback_track('public.orders');

-- Check what is being tracked
SELECT * FROM flashback.tracked_tables WHERE is_active;

-- Resolve one unique timestamp cut and use its COMMIT LSN
SELECT resolved_lsn AS target_lsn
FROM flashback_resolve_target('public.orders', now() - interval '1 hour') \gset

-- Query past state without restoring
SELECT * FROM flashback_query_lsn(
    'public.orders', :'target_lsn'::pg_lsn, NULL
)
    AS t(id int, customer_id int, total numeric, status text);

-- Multi‑table restore (FK‑safe ordering)
SELECT flashback_restore_lsn(
    ARRAY['order_items', 'orders', 'customers'],
    :'target_lsn'::pg_lsn
);
```

`flashback_checkpoint()` rejects qualified WAL generations. Use
`flashback_reanchor()` only as a controlled exact boundary. Slot identity
changes freeze the old epoch and require re-anchor. Do not change
`capture_mode` or toggle `pg_flashback.enabled` while tracking is active because
synchronous mode/disable transition gaps remain release gates.

### Partitioned tables (legacy functional demo)

The implementation can attach per-row triggers to partitioned parents. This is
useful test evidence, but partitioned targets are not release-qualified; the
first-release contract is an ordinary LOGGED, non-partitioned table.

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

-- Legacy restore behavior demo; not a first-release-supported target
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

Supported local capture is intended not to require DML statement rewrites, but
it does require PostgreSQL configuration, admission preflight and maintenance.
The backup profile is operator-driven through the reference controller because
PostgreSQL extensions do not launch privileged operating-system recovery
processes.

## 14. Troubleshooting

### ⚠️ Development and release blockers

Do not deploy this branch as a correctness-guaranteed recovery system. These
items explain current behavior and remaining release gates.

**1. WAL slot disk accumulation**
In WAL mode the replication slot retains WAL segments until the background
worker consumes them. If the worker crashes, is disabled
(`pg_flashback.enabled = off`), or falls behind on a write-heavy cluster,
unread WAL accumulates and **can fill disk**. Disabling also breaks coverage;
the current runtime does not persist that break. PostgreSQL will not delete
retained slot WAL automatically.

```sql
-- Monitor slot lag
SELECT slot_name, database, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'pg_flashback_%';
```

Set a hard cap in `postgresql.conf` to prevent runaway growth:
```
max_slot_wal_keep_size = 10GB   -- adjust to your disk headroom
```

This cap bounds disk exposure by allowing PostgreSQL to invalidate a lagging
slot. Invalidation is data loss for capture, not automatic recovery. The WAL
runtime freezes the old stream watermark and opens a persistent gap when the
missing/replaced/externally advanced slot is observed; restore beyond that
frontier is rejected until `flashback_reanchor()` establishes a new boundary.

**2. Capture visibility can be delayed by maintenance head-of-line blocking**
`staging_events` is LOGGED, so committed trigger events survive a PostgreSQL
crash. However, the current worker serializes WAL consumption, staging flush,
checkpoint and retention work. A slow checkpoint or lock wait can delay when
events become visible in `delta_log`, and in WAL mode it can grow slot lag.
Separating capture drain from maintenance is a release gate.

**3. `flashback_restore` exclusive lock can pause under a long-running query**
The atomic shadow swap (`DROP original → RENAME shadow`) requires an `AccessExclusiveLock`. If there is a long-running `SELECT`, `VACUUM`, or open transaction on the table at restore time, the lock acquisition will block — and will in turn block all subsequent reads/writes behind it. Always restore during a low-traffic window or set a `lock_timeout` in your session first:
```sql
SET lock_timeout = '5s';
SELECT flashback_restore_lsn('orders', '0/8F12340'::pg_lsn);
```

**4. `flashback_track()` on a large table is expensive**
`flashback_track()` takes an immediate full-table snapshot. The current local
path takes the exact-base write lock, but automated capacity/write-stall
admission is not implemented yet. Do not select a profile from size alone: use
the backup profile when local headroom, observed change rate, write-stall or
RTO budgets do not fit.

**5. `wal_level = logical` is cluster-wide**
Setting `wal_level = logical` affects **all databases** on the cluster — not
just the one using pg_flashback. It increases WAL volume and requires a
PostgreSQL restart. Trigger mode avoids that requirement but is explicitly
legacy/experimental and creates no correctness-qualified generation.

**6. Sequence restore can cause PK conflicts after restoring to an older state**
When `flashback_restore_lsn` replays a table to an older state, `max(id)` in
the restored data may be lower than the current sequence value. The sequence is
rewound to match, so the next `INSERT` can reuse IDs still referenced elsewhere.
The legacy manual checkpoint is not a safe workaround. Related-table recovery
must be admitted and coordinated as one verified operation, or handled with
cluster-level recovery.

---

### Symptom → Fix

| Symptom | Check / Fix |
|---------|-------------|
| Extension fails to load | Ensure `shared_preload_libraries = 'pg_flashback'` and restart PostgreSQL. |
| Background worker missing | `SELECT * FROM pg_stat_activity WHERE backend_type LIKE 'pg_flashback%';` |
| Restore/query target rejected or appears stale | Inspect `flashback_health()`, the per-database worker and the logical slot. A pending boundary must resolve before use; a broken stream requires re-anchor. |
| WAL capture not working | Confirm `wal_level = logical` and replication slot exists: `SELECT slot_name FROM pg_replication_slots;`. Run `flashback_track()` to create the slot. |
| Slot creation error in `flashback_track` | Occurs when called inside a transaction that already has writes. The call fails closed; retry it in a fresh transaction or create the slot manually as shown in the error HINT. |
| Partitioned-table trigger behavior | This is legacy functional behavior, not a first-release-supported topology. Do not treat re-running `flashback_track()` as release qualification. |
| TOAST / large row warnings | These come from the legacy trigger path. They are not accepted coverage evidence; use the qualified WAL path or stop/re-anchor before trusting legacy history. |
| UNLOGGED table skipped in WAL mode | UNLOGGED targets are outside the first-release contract in either capture mode. Use an ordinary LOGGED table; switching to trigger does not make the topology supported. |
| Restore missing rows after `max_row_size` trim | The table used legacy trigger capture; that path is outside correctness claims. Qualified WAL capture does not use this size-skip GUC. |
| Retention window expired/error | Legacy age-based retention is not the adopted contract. Do not add automatic checkpoints; generation-aware retention must preserve a complete boundary/replay chain or block cleanup. |
| Dependent view ACLs not restored | ACL grants on views cannot be restored automatically; a NOTICE lists affected views. Re-grant manually after restore. |
| Test mutex conflict | Run `rm -rf target/test-pgdata` before `cargo pgrx test`. |
| Socket connection issues (pgrx dev) | Try: `psql -h ~/.pgrx -p 28817 postgres` |

## 15. Caveats & Limitations

- **Capture durability and lag:** `staging_events` is LOGGED. WAL DML is durable
  in its logical slot at commit. Current serial worker maintenance can still
  delay visibility and increase slot lag; it does not create an UNLOGGED
  staging crash window.
- **WAL mode requirement:** `wal_level = logical` must be set cluster-wide.
  `capture_mode = 'auto'` never falls back to a qualified trigger generation;
  tracking fails closed when logical WAL is unavailable.
- **Trigger mode:** explicit trigger capture is retained only for legacy tests
  and compatibility. Statement timestamps and `track_commit_timestamp` do not
  provide a proven total order, so trigger history is not admitted by the LSN
  APIs.
- **Long-running transactions:** no workload-duration assumption converts
  statement time into commit time. Targets are accepted only from proven
  commit-coordinate coverage.
- **Large DDL snapshot cost:** TRUNCATE and DROP events may inline table contents
  into `delta_log.old_data`. Do not use an automatic/manual checkpoint as an
  unpreflighted workaround; choose the backup profile or an explicit exact
  maintenance boundary when the local budget does not fit.
- **Non-PK table restore ceiling:** Row-by-row replay is materially slower than
  the PK batch path. Adding a surrogate PK or choosing the backup profile is
  preferred; periodic full checkpoints are not the adopted workaround.
- **Partitioned tables:** Per-row trigger code exists as legacy functional
  evidence, but partitioned targets are outside the first-release contract in
  either capture mode. Switching to WAL does not make them supported.
- **Replication & HA topologies:** HA/failover and logical-subscriber capture
  are outside the first-release contract. Slot loss/recreation after failover
  creates a permanent coverage gap and requires an exact local-base re-anchor;
  manually recreating a slot never resumes old coverage.
- **pg_upgrade / major version:** Cross-major qualification is not complete.
  After schema migration, legacy tracking rows are unanchored until an explicit
  profile-specific generation anchor is established; reinstalling binaries or
  replaying schema alone does not prove coverage.
- **Backup-profile scope:** The first release targets local POSIX repositories,
  completed full backups, LSN targets and ordinary LOGGED, non-partitioned
  tables without tablespaces. Rename/schema moves across the target,
  HA/failover, managed services, remote repositories, partitions, incoming
  foreign-key reconstruction and dependent-view reconstruction are rejected.
  See
  [RELEASE_SCOPE.md](docs/RELEASE_SCOPE.md).
- **Backup/expire coordination:** Snapshot-direct reads a completed backup tree
  directly. Every backup/expire job for that repository must use the supplied
  exclusive lock wrapper; otherwise the helper refuses to claim race safety.

## 16. License

pg_flashback is released under the [MIT License](LICENSE). pgBackRest remains
an external MIT-licensed program; its source is neither embedded nor linked.
Rust dependency and external-tool notices are recorded in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

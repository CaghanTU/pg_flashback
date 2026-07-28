# ADR 0004: Storage backend PoC benchmark (Step 8)

## Status: COMPLETE (1 GiB). 10 GiB: BLOCKED_BY_CAPACITY.

Every result in this document comes from a single, verified-consistent
executable harness commit: **`c4a4a456916b57298aee733bb1a1844b31f9047c`**
(extension binary SHA-256
`2d3421a77e893213740aa3454c8e1098e50828b3e8cb98558f5f69d0fc66ebae`). This is
not a manual claim -- `docs/evidence/step8-storage-backend-benchmark.json`
was produced by a generator script that asserts every one of the 61 runs
below (selftest, all 18 1 GiB correctness/perf/storage combinations, all 9
1 GiB scale-sensitive crash scenarios, all 31 128 MiB adversarial/logic
crash scenarios, both backup-footprint runs) shares the same
`source_commit` and `extension_binary_sha256` and reports `status=PASS`,
and refuses to write `status: COMPLETE` otherwise.

This supersedes an earlier PARTIAL round on this same ADR, whose own 1 GiB
matrix mixed results from three different executable commits (`833ed33`,
`b03243f`, `a117ce2`) into an invalid "COMPLETE" claim, and whose
methodology had several real gaps: an invariant relaxed instead of fixed, a
no-op restore measurement, inferred (not measured) WAL/backup claims, and
unvalidated manifest fields. That round's raw data is preserved unchanged
under `prior_partial_round` in the evidence JSON, not deleted or rewritten
-- see "Corrections made this round" below for the full list, several of
which were themselves found by *this* correction round's own new checks.

This ADR is a decision-gate report, not a production design: no production
`SnapshotStore` backend was added, `storage_backend`'s CHECK constraint was
not touched, no public API/GUC changed, Step 9 was not started, and
10/25/50 GiB scale and the 24-hour run were not started.

## Purpose

Step 7 proved Protocol B (existing-stream reanchor) correct at real 1 GiB
scale, as a PoC boundary/marker mechanism only -- it never specified how a
snapshot artifact is actually *stored*. Step 8's job is to compare three
storage strategies for that artifact, at PoC level, sharing the identical
Protocol B boundary and the identical WAL-replay correctness oracle, so the
comparison isolates the storage decision from everything else.

## Backends compared

- **heap_v1** -- baseline/control. `CREATE TABLE ... AS SELECT`, same
  mechanism Step 7 already qualified. Artifact is an ordinary heap table
  living in the same cluster/database as the source.
- **in_db_logged_zstd** -- `COPY (SELECT ...) TO STDOUT (FORMAT binary)`,
  chunked and zstd-compressed, each chunk stored as a `bytea` row (`STORAGE
  EXTERNAL`) in a `LOGGED` table in the same database.
- **external_zstd** -- same COPY BINARY + zstd chunking, but chunks are
  written to files outside the PostgreSQL data directory: temp file -> fsync
  -> atomic rename -> parent-directory fsync. The database holds only a PoC
  metadata/manifest pointer row.

All three reuse the *exact* coordinator-lock + snapshot-fixed-while-locked +
transactional-marker choreography from Step 7's `run_protocol_b` -- only the
statement that runs once the snapshot is fixed differs (CTAS vs
`\copy`-to-file). All three are checked against a ground-truth table
captured from the same fixed snapshot every backend materializes from,
independent of whichever storage mechanism is under test.

## Corrections made this round (found by user review, then by this round's
own new checks)

Ten real issues were found and fixed, each its own commit
(`0c9638f`..`c4a4a45` on `scripts/poc/run_poc_storage_backend_benchmark.sh`):

1. **Writer commit-count race, eliminated not relaxed.** The prior round's
   `commits_replayed >= copy_window_commits` papered over a real race: the
   writer's bash-side counter file was written *after* a statement already
   committed durably, and could legitimately lag if SIGTERM landed in that
   gap. Replaced the counter file with a durable ledger table
   (`poc_bench_writer_ledger`), reset per snapshot attempt: every writer
   statement now runs as one atomic `BEGIN; DML; INSERT INTO the ledger;
   COMMIT;` transaction, so `copy_window_commits` is a plain `SELECT
   count(*)` against a table that can only ever reflect genuinely durable
   commits. Strict `==` restored and verified true on all 18 matrix rows.
2. **heap_v1 restore was a no-op measurement.** All three call sites
   aliased `restored_tbl` directly to the artifact table, so `restore_ms`
   was always ~0 ms. Added `restore_heap_v1`: a real, timed
   `CREATE TABLE restored AS SELECT * FROM artifact_heap`, materializing a
   genuinely separate table -- the same shape of cost the other two
   backends' restore steps pay.
3. **WAL amplification, measured not inferred.** Added
   `pg_current_wal_insert_lsn()`/`pg_wal_lsn_diff()` around each phase
   (materialize, persist, restore) for every run. See the real numbers
   below -- they both confirm and refine what the prior round only
   guessed at.
4. **Backup-footprint, measured not inferred.** New `backup-footprint`
   mode: one cluster/source table, a real `pg_basebackup` baseline, then
   each backend's real incremental byte delta (artifact reset between
   backends via DROP+recreate, not DELETE -- see item 9).
5. **Manifest binding, validated not just recorded.** Added
   `validate_manifest_binding`: format version, backend identity, pg_major,
   architecture, system_identifier, current db oid, encoding, tracking_id,
   semantic fingerprint format version, schema fingerprint (column
   order/type/typmod/collation via `pg_attribute`, not a bare
   `information_schema` string), row_count, and a cross-check of
   boundary_lsn/boundary_xid against the durable WAL-decode ledger
   (`commit_log`) -- all fail-closed on mismatch, checked before any
   restore work happens.
6. **Canonical restore verification strengthened.** `verify_restore` now
   also checks table owner and ACL, and uses the same richer schema
   fingerprint as (5).
7. **in_db_logged_zstd adversarial parity closed.** `missing_chunk` and
   `manifest_mismatch` were coded but never actually run for
   `in_db_logged_zstd`; now run and passing. Added two new crash points
   for both zstd backends: `row_count_mismatch` (isolates the row_count
   cross-check specifically, without also tampering the chunk-hash path)
   and `chunk_order_mismatch` (proves scrambled chunk ordering is caught).
8. **Manifest row_count/logical_bytes bug**, found by (5)'s new row_count
   cross-check on the very first post-fix smoke run: both `persist_*`
   functions computed these fields from the *live, still-mutating* source
   table instead of the snapshot-consistent ground truth, so they recorded
   a count that never matched what was actually captured. Fixed to use
   `$GT_TBL` (already loaded from the same rawfile) instead of the live
   table.
9. **Backup-footprint cleanup bug**, found by the first backup-footprint
   smoke run: `in_db_logged_zstd` and `external_zstd` both reported the
   identical nonzero delta, which should have been impossible. A plain
   `DELETE` on `poc_bench_chunks` marks rows dead but does not shrink the
   table's physical file -- `in_db_logged_zstd`'s ~126 MB of chunk data was
   still physically present when `external_zstd`'s backup was measured
   next. Fixed with DROP+recreate (plus a follow-up fix for the resulting
   FK drop-order error).
10. **Writer-subshell death on transient failure**, found by the first full
    requalification pass: 100% of `mid_materialize` crash tests (5/5)
    failed with a duplicate-key error on the retry's first writer INSERT.
    Root cause: `mid_materialize` crashes immediately after the writer's
    first iteration is confirmed ready, so the crashed attempt's `id=-1`
    insert is already durable before the crash; the retry's fresh writer
    genuinely, deterministically collides on that same id -- which is
    expected and should just be tolerated. The bug was that the ledger
    rewrite (item 1) turned each per-statement call into a bare, unguarded
    statement; since the writer loop runs as a background subshell
    inheriting the script's `set -Eeuo pipefail`, the first failing bare
    statement killed the *entire* writer subshell immediately, before it
    ever reached the ledger insert or `touch "$ready_file"` for that or any
    later iteration. The old design tolerated this the same way, via
    `cmd && commits=$((commits+1))` -- the `&&` happened to exempt the LHS
    from errexit as a side effect, which the ledger rewrite's bare
    statements lost. Fixed with explicit `|| true` on every per-statement
    call, restoring that tolerance without reintroducing the original
    race (a failed transaction never commits its ledger row either way).

None of these were product/correctness bugs in pg_flashback itself --
`production_code_changed: false` holds throughout; the built `.so` is
unchanged from Step 7's closing evidence.

## 1 GiB results (verified-consistent commit, 18/18 combinations)

### Correctness

All 18 PASS (3 backends x {ordinary_bad, toast_bad, good_compress} x 2
reps). Every run proved: Protocol B's existing-stream reanchor path with
marker identity bound transactionally by marker_text and XID;
**`commits_replayed == copy_window_commits` exactly** (strict equality, not
`>=`, verified on every row); real concurrent writer with non-zero
INSERT/UPDATE/DELETE; zero historical payload events replayed; no
duplicate/out-of-order replay; restored table matching ground truth on row
count, semantic fingerprint, canonical schema (column order/type/typmod/
collation), owner, ACL, and for `toast_bad`, TOAST byte-equality.

### Performance, storage, and real WAL bytes (mean of 2 reps, `ordinary_bad`
shown as the representative narrow/poorly-compressing shape unless noted)

| Metric | heap_v1 | in_db_logged_zstd | external_zstd |
|---|---|---|---|
| Compression ratio, ordinary_bad | 0.966 (expansion) | 2.108x | 2.108x |
| Compression ratio, toast_bad | 0.790 | 1.0022x | 1.0022x |
| Compression ratio, good_compress | 0.961 | ~120.6x | ~120.0x |
| Artifact bytes, ordinary_bad | ~1.077 GB | ~493 MB | ~493 MB |
| Snapshot create, ordinary_bad | ~8.2 s | ~2.0 s | ~1.9 s |
| Persist, ordinary_bad | 0 ms (no separate phase) | ~6.1 s | ~5.0 s |
| Restore, ordinary_bad | ~4.7 s (now real) | ~10.0 s | ~6.0 s |
| Total RTO, ordinary_bad | ~42.9 s | ~49.7 s | ~42.9 s |
| **WAL: materialize, ordinary_bad** | **~1.226 GB** | ~538 KB | ~537 KB |
| **WAL: persist, ordinary_bad** | 0 (n/a) | **~536 MB** | ~6.7 MB |
| **WAL: restore, ordinary_bad** | **~1.224 GB** | ~960 MB | ~960 MB |
| **WAL: total, ordinary_bad** | **~2.45 GB** | **~1.49 GB** | **~0.97 GB** |
| **Backup delta, ordinary_bad (real, `pg_basebackup`)** | **269,500,856 B** | **126,484,650 B** | **24,580 B** |
| **Backup delta, toast_bad (real)** | **341,025,217 B** | **278,921,386 B** | **24,580 B** |

**heap_v1's WAL cost is real and now measured twice over (materialize and
restore), not once inferred.** Both its materialize and restore steps are
full `CREATE TABLE ... AS SELECT` operations under `wal_level = logical`,
each generating ~1.2 GB of WAL for this 1 GiB table -- consistently the
highest of the three backends on every WAL metric, and confirmed
independently by the real backup-footprint delta (heap_v1 adds
269.5-341.0 MB to a physical backup, by far the most).

**in_db_logged_zstd's WAL cost scales with how much gets stored, and
becomes comparable to heap_v1's for poorly-compressible data.** Its persist
WAL is small for `good_compress` (~9.6 MB, matching the ~9 MB compressed
artifact) but substantial for `ordinary_bad` (~536 MB) and dominant for
`toast_bad` (~1.15 GB, since near-zero compression means the LOGGED chunk
table is inserting nearly the full logical size). Its real backup delta for
`toast_bad` (278.9 MB) approaches heap_v1's (341.0 MB) -- for
incompressible data, storing chunks in a LOGGED table loses most of its WAL
advantage over the baseline.

**external_zstd's WAL and backup cost are the lowest measured, by a wide
and consistent margin.** Persist WAL is 2.6-8.3 MB across every shape
(pure filesystem I/O, never touching WAL, plus only the tiny manifest row);
materialize WAL is just the concurrent writer's own churn (300-820 KB, not
the extraction itself, which is a client-side `\copy` generating no server
WAL at all). Its real backup delta is **24,580 bytes for both shapes
tested** -- the same tiny manifest-row overhead regardless of source data
size or shape, empirically confirming its artifact truly lives outside the
backup boundary.

**Restore cost, real for all three now:** `in_db_logged_zstd` is
consistently the slowest to restore (~10.0 s for `ordinary_bad`, ~14.1 s for
`toast_bad`), plausibly a PoC implementation artifact (its chunk-extraction
path round-trips through `psql` text-mode hex encoding + a `python3`
hex-decode pass, a real ~2x data-size-in-transit penalty a production
implementation could likely avoid with a different extraction method) more
than an inherent property of storing chunks in the database. `heap_v1`'s
restore, now a real second full-table copy, lands between the two zstd
backends' costs for `ordinary_bad` but is the *fastest* to restore for
`toast_bad` and `good_compress` (no decompression step).

**RTO caveat, stated plainly:** `total_rto_ms` sums
`snapshot_create_ms + persist_ms + restore_ms + verify_ms`, where
`snapshot_create_ms` includes this harness's own WAL-replay correctness-
oracle overhead (decoding and replaying the writer's churn against a
throwaway shadow table) and `verify_ms` includes fingerprint/schema/TOAST-
hash/owner/ACL checking that a real backend would likely not redo on every
restore in production. Treat these numbers as internally consistent for
comparing the three backends against each other (the same methodology
overhead applies equally to all three), not as an absolute production RTO
estimate.

### Crash safety

Real postmaster-crash scenarios, all PASS at genuine 1 GiB scale (9
scenarios: `mid_materialize` x3 backends, `metadata_commit_before`/`after`
x2 zstd backends, `duplicate_retry` x2 zstd backends), plus a 31-scenario
128 MiB adversarial/logic matrix covering corrupt chunk, missing chunk
(now including `in_db_logged_zstd`), manifest mismatch (now including
`in_db_logged_zstd`), wrong pg_major/system_identifier/schema binding, row
count mismatch, chunk order mismatch, and for `external_zstd` specifically:
temp-write/fsync/rename ordering at every step, file-without-metadata,
metadata-without-file, and orphan-GC safety. All 40 crash scenarios PASS on
the same verified-consistent commit.

**Fsync-durability caveat, unchanged and still honest:** the fsync-adjacent
crash points are modeled as an abrupt kill of the writing step (or a real
`pg_ctl -m immediate` crash+restart for DB-side points). This proves
incomplete userspace state is never mistaken for valid. It does not prove
real fsync-durability against an actual host power loss, which this
environment cannot safely test.

**Known, disclosed gap:** the fsync/rename-ordering, split-state, and
orphan-GC crash points remain `external_zstd`-only (structurally unique to
the filesystem backend); `in_db_logged_zstd` now has full parity on the
generic adversarial points (corrupt/missing chunk, manifest mismatch, wrong
identity, row-count/chunk-order mismatch, duplicate retry,
metadata-commit atomicity).

## Elimination / advancement decision (1 GiB gate)

Unchanged from the prior round's reasoning, now on firmer (measured, not
inferred) footing:

- **heap_v1**: passes every correctness and crash-safety gate outright, not
  eliminated on a technicality. But it is now demonstrated, on *three*
  independent measurements (compression ratio, real WAL bytes, real backup
  delta) rather than one inferred claim, to be the most WAL- and
  backup-footprint-expensive of the three for every data shape tested,
  including the highly-redundant `good_compress` shape where the other two
  achieve ~120x compression and heap_v1 achieves none. **Does not advance
  to a 10 GiB round.** Remains the existing baseline/control.
- **in_db_logged_zstd** and **external_zstd**: both pass every correctness
  and crash-safety gate, including full adversarial parity now.
  Compression ties. `external_zstd` wins persist speed, restore speed, WAL
  cost on every phase, and real backup-footprint delta, by a wide and
  now-measured margin -- not just "a bit better." `in_db_logged_zstd`
  retains a real, non-trivial operational advantage (the artifact travels
  automatically with any routine backup of the database; no second
  filesystem location's lifecycle, permissions, or backup coverage to
  manage separately), and its WAL/backup cost is only close to heap_v1's
  for the worst case (incompressible TOAST-heavy data) -- for compressible
  data it is meaningfully better than heap_v1 on every axis. **Both advance
  as finalists.**

## 10 GiB finalist round: BLOCKED_BY_CAPACITY

Unchanged from the prior round's preflight (the correctness/measurement
fixes in this round do not change PostgreSQL's actual disk usage patterns,
so the original projection remains valid): available capacity in this
environment (43 GB) was less than the low-end projected single-run peak
(59.6-75.0 GB) for any of the four finalist/shape combinations, let alone
with the required +8 GB safety reserve or additional transient overhead.
**No 10 GiB run was attempted.** No unrelated files or other targets were
deleted to force capacity.

Runnable when capacity is available (sequentially, so the harness's own
cleanup reclaims disk between runs; 2 reps each per this round's own
repeatability rule):

```bash
./scripts/poc/run_poc_storage_backend_benchmark.sh bench in_db_logged_zstd ordinary_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench in_db_logged_zstd toast_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench external_zstd ordinary_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench external_zstd toast_bad 10240 rep1
# repeat each with rep2
```

## Decision-gate table

| Backend | Correctness | Crash safety | Artifact size (ordinary_bad, 1 GiB) | WAL total (real, ordinary_bad) | Backup delta (real) | Snapshot time | Restore RTO (real) | Ops complexity | 1 GiB | 10 GiB |
|---|---|---|---|---|---|---|---|---|---|---|
| heap_v1 | PASS (18/18) | PASS (9 scale + 2 logic) | ~1.08 GB (no compression) | **~2.45 GB (highest)** | **269.5-341.0 MB (highest)** | Slowest (~8.2 s) | ~4.7 s (real, now measured) | Lowest (nothing new to operate) | PASS, not competitive on storage/WAL | Not attempted (not a finalist) |
| in_db_logged_zstd | PASS (18/18) | PASS (9 scale + 9 logic, full parity) | ~493 MB (2.1x) | ~1.49 GB (moderate; ~toast_bad approaches heap_v1) | 126.5-278.9 MB | Fast (~2.0 s) | Slowest of the two zstd (~10.0 s) | Low (single backup story) | PASS, finalist | BLOCKED_BY_CAPACITY |
| external_zstd | PASS (18/18) | PASS (9 scale + 20 logic, incl. full fsync/rename/split/GC matrix) | ~493 MB (2.1x, ties) | **~0.97 GB (lowest)** | **24,580 B (lowest, both shapes)** | Fast (~1.9 s) | Fastest of the two zstd (~6.0 s) | Moderate (separate filesystem location) | PASS, finalist | BLOCKED_BY_CAPACITY |

## Recommendation

**Unchanged: `external_zstd` as the primary candidate for a future
production design, `in_db_logged_zstd` as a documented fallback** for
operators who cannot provision a separate artifact filesystem/backup path.
This recommendation is now backed by measured WAL bytes and measured
backup-footprint deltas on every row, not inference:

- `external_zstd` wins persist speed, restore speed, every WAL-phase
  metric, and real backup delta, consistently and by a wide margin (not "a
  bit better") across all three data shapes.
- Its cost is operational, not performance: a second filesystem location
  to provision, secure, and back up on its own schedule, plus real
  orphan-GC discipline (now exercised by this round's crash matrix, but a
  production implementation still needs to build retention/scheduling for
  real).
- `in_db_logged_zstd`'s single-backup-story simplicity is a legitimate,
  real consideration, not a strawman -- but its WAL/backup cost advantage
  over heap_v1 shrinks to nearly nothing for incompressible data, which
  should weigh into any final choice for workloads dominated by such data.
- heap_v1 remains correct and simple but does not solve the problem this
  comparison exists to address; nothing here suggests removing it as a
  fallback/control path.

## What is proven, what is not

**Proven at 1 GiB, this round, on one consistent commit:** correctness
(18/18), crash safety (40/40 across scale-sensitive and adversarial
matrices, full parity between the two zstd backends on generic points),
real measured WAL bytes per phase for every run, real measured
backup-footprint deltas for two data shapes, and `commits_replayed ==
copy_window_commits` exactly (not `>=`) on every row.

**Not proven, anywhere in this round:**
- Behavior at 10, 25, or 50 GiB scale -- entirely unknown, blocked by
  capacity in this environment.
- Real fsync-durability against host power loss (stated above).
- Backup-footprint measured for the `good_compress` shape (only
  `ordinary_bad` and `toast_bad` were run -- a reasonable inference from
  the other two shapes' pattern, but not itself measured).
- Whether `in_db_logged_zstd`'s restore-speed disadvantage is fundamental
  or an artifact of this PoC's specific hex-roundtrip extraction
  implementation.
- A 24-hour run, multi-artifact retention/lifecycle behavior, concurrent
  multi-table protection, or anything beyond a single artifact per run.

## Explicitly out of scope for this round (per its own instructions)

No production `SnapshotStore` backend was added. `storage_backend`'s CHECK
constraint was not extended. No public API or GUC was added. No external
artifact path was wired into production. Step 9 was not started. 25/50 GiB
qualification was not started. The 24-hour run was not started. No push,
no main merge, no tag/release.

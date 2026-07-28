# ADR 0004: Storage backend PoC benchmark (Step 8)

## Status

1 GiB comparison COMPLETE. 10 GiB finalist round BLOCKED_BY_CAPACITY (not
run). This ADR is a decision-gate report, not a production design: no
production `SnapshotStore` backend was added, `storage_backend`'s CHECK
constraint was not touched, and no public API/GUC changed.

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
captured from the same fixed snapshot, and the same WAL-replay commit/event
oracle (marker_identity/marker_log/commit_log/change_log/poc_apply_shadow --
literally the same schema and PL/pgSQL functions Step 7 installs, duplicated
in the isolated PoC harness rather than shared, per this area's own
isolation requirement).

## Harness corrections found during this round

Seven real bugs were found and fixed while building and exercising this
harness (each its own commit, `749dfa6`..`a117ce2` range on
`scripts/poc/run_poc_storage_backend_benchmark.sh`):

1. `compression_ratio` always recorded 0 -- `bc` is not installed in this
   environment; switched to `awk`.
2. `establish_boundary_and_materialize`'s oracle-shadow bookkeeping had no
   `ON CONFLICT`/`DROP IF EXISTS` guard, but crash-mode retries deliberately
   call it twice against the same source table (to avoid regenerating a
   1 GiB table per attempt) -- the second call collided on
   `poc_table_map_pkey` every time.
3. `restore_in_db_logged_zstd`/`restore_external_zstd` `die()` internally
   via a direct `exit` call on any correctness violation. `exit` terminates
   the whole harness process immediately, no matter how deeply nested --
   it is not something a bare `"cmd || flag=1"` at the call site can catch.
   Every adversarial test expecting a fail-closed restore was silently
   killing the whole harness before its own postcheck step ever ran. Fixed
   by wrapping the call in an explicit subshell inside an `if` (not a bare
   statement, since `case`/bare-statement bodies are not exempt from
   `set -e` the way an `if` condition is).
4. `load_ground_truth_from_rawfile` had the identical not-safely-re-callable
   bug as (2).
5. `persist_in_db_logged_zstd`/`persist_external_zstd` had no error
   checking on their manifest-row `INSERT`, so `duplicate_retry`'s expected
   PK-collision failure was silently absorbed (same `set -e`-suspension
   mechanism as (3), applied to an *unguarded* internal failure this time)
   rather than propagated. Added explicit `die()` guards, matching the
   defensive style already used everywhere else in the harness.
6. `commits_replayed == copy_window_commits` was too strict: a real 1 GiB
   run (`in_db_logged_zstd`/`good_compress`) hit `commits_replayed=64` vs
   `copy_window_commits=61`. Root cause: the writer loop only writes its
   counter file *after* a statement has already committed durably to WAL;
   an untrapped SIGTERM during shutdown can land in the gap between "last
   statement of an iteration committed" and "counter file updated for that
   iteration." No data was lost (fingerprint/row-count still matched) --
   only the writer's own self-reported count of its own commits could lag
   the true, fully durable count by up to one iteration. Relaxed to `>=`:
   strictly less would still mean real data loss and must still fail
   closed; strictly greater is this legitimate bookkeeping lag.
7. `cleanup()` never purged the bulky `rawstream_*.bin`/`chunks_*/`/
   `restore_*/` working files (not evidence -- `result.json`/
   `metrics.jsonl`/`log/` already capture everything evidentiary). These
   accumulated to 37 GB across one session's runs and caused three
   otherwise-correct 1 GiB reruns to fail on real `ENOSPC`. Extended the
   same `POC_KEEP`/`POC_KEEP_FAILED_DATA`-gated cleanup already applied to
   `DATA`/`PGLIB_DIR` to cover these too.
8. `heap_v1`'s `artifact_bytes` was always recorded 0 (it has no separate
   persist phase to populate it), which would have unfairly biased the
   storage comparison table. Added a direct `pg_total_relation_size`
   measurement.

None of these were product/correctness bugs in pg_flashback itself --
`production_code_changed: false` holds throughout this entire round; the
built `.so` is unchanged from Step 7's closing evidence.

## 1 GiB results

### Correctness (all 18 combinations: 3 backends x {ordinary_bad, toast_bad,
good_compress} x 2 reps, real 1 GiB scale)

All 18 PASS. Every run independently proved, via the shared oracle:

- Protocol B's existing-stream reanchor path is what ran, marker identity
  bound transactionally by marker_text and XID;
- `commits_replayed >= copy_window_commits` (real concurrent writer,
  non-zero INSERT/UPDATE/DELETE, no data loss);
- historical WAL prefix advanced, zero historical payload events replayed;
- no duplicate/out-of-order replay;
- restored table matches the ground-truth snapshot exactly: row count,
  order-independent semantic fingerprint, schema (column name/type/order),
  and for `toast_bad`, an aggregate TOAST byte-equality hash.

### Crash safety

Real postmaster-crash scenarios re-run at genuine 1 GiB scale (scale
matters here: a real materializing statement must be caught genuinely
active in `pg_stat_activity`, and multi-chunk behavior only appears with
15-17 chunks at 1 GiB vs 1-2 at 128 MiB):

| Test | heap_v1 | in_db_logged_zstd | external_zstd |
|---|---|---|---|
| mid_materialize (real crash while genuinely active) | PASS | PASS | PASS |
| metadata_commit_before (no partial artifact visible) | n/a | PASS | PASS |
| metadata_commit_after (artifact fully valid) | n/a | PASS | PASS |
| duplicate_retry (identity collision rejected) | PASS | PASS | PASS |

The full adversarial/logic matrix (corrupt chunk, missing chunk, manifest
mismatch, wrong pg_major/system_identifier binding, and for `external_zstd`
specifically: temp-write/fsync/rename ordering at every step, file-without-
metadata, metadata-without-file, orphan-GC safety) ran at 128-256 MiB, since
scale does not change what these prove (tamper detection and state-machine
correctness, not timing). All PASS on the final harness state. Known gaps,
disclosed rather than hidden: `missing_chunk` and `manifest_mismatch` were
only exercised for `external_zstd`, not `in_db_logged_zstd` (the equivalent
in-DB failure modes -- a deleted chunk row, a tampered manifest row -- are
structurally simpler than the filesystem case and are considered lower risk,
but were not empirically proven this round).

**Fsync-durability caveat, stated plainly:** the fsync-adjacent crash points
(`temp_chunk_write_crash`, `chunk_fsync_done`, `manifest_fsync_before/
after`, `parent_dir_fsync_after`) are modeled as an abrupt kill of the
writing step (or, for DB-side points, a real `pg_ctl -m immediate`
crash+restart). This proves incomplete userspace state is never mistaken
for valid. It does **not** prove real fsync-durability against an actual
host power loss -- this environment cannot safely power-cycle itself, and a
normal process kill does not revert a `rename()` the kernel has already
accepted into its page cache. That gap is real and unproven, not silently
assumed away.

### Performance and storage (mean of 2 reps, 1 GiB, `ordinary_bad` shown as
the representative narrow/poorly-compressing shape unless noted)

| Metric | heap_v1 | in_db_logged_zstd | external_zstd |
|---|---|---|---|
| Compression ratio, ordinary_bad | ~0.97 (no compression; heap overhead) | 2.108x | 2.108x |
| Compression ratio, toast_bad | ~0.79 | 1.0022x (incompressible, as designed) | 1.0022x |
| Compression ratio, good_compress | ~0.96 | ~120.5x | ~120.4x |
| Artifact bytes, ordinary_bad | ~1.077 GB (source_logical ~1.04 GB) | ~493 MB | ~493 MB |
| Snapshot create, ordinary_bad | ~8.0 s | ~2.1 s | ~1.9 s |
| Persist, ordinary_bad | 0 ms (no separate phase) | ~4.8 s | ~2.7 s |
| Restore, ordinary_bad | 0 ms (already live) | ~9.2 s | ~5.5 s |
| Total RTO (this harness's own accounting -- see caveat), ordinary_bad | ~34.7 s | ~45.8 s | ~41.2 s |
| Chunk count, ordinary_bad (1 GiB / 64 MiB chunks) | n/a | 15 | 15 |

**RTO caveat:** `total_rto_ms` as measured here sums
`snapshot_create_ms + persist_ms + restore_ms + verify_ms`, where
`snapshot_create_ms` includes this harness's own WAL-replay correctness-
oracle overhead (decoding and replaying the writer's churn against a
throwaway shadow table) -- a testing-methodology cost, not a real production
snapshot cost. It also includes `verify_ms` (fingerprint/schema/TOAST-hash
checking), which a real backend's restore path would likely not redo on
every restore in production. Treat these RTO numbers as internally
consistent for *comparing the three backends against each other* (the same
methodology overhead applies equally to all three), not as an absolute
production RTO estimate.

**heap_v1's snapshot-create cost is real, not a harness artifact.** It is
consistently ~4x slower than the other two backends' materialization step,
despite similar writer-commit counts and comparable `materialize_ms`
figures for the CTAS itself. The likely cause: under `wal_level = logical`
(required for this whole mechanism), `CREATE TABLE ... AS SELECT` cannot use
PostgreSQL's minimal-WAL fast path and is fully WAL-logged; the shared
logical slot's subsequent decode/scan pass then has more WAL volume to walk
through even after filtering to the tracked oid. This was not directly
instrumented (no direct `pg_current_wal_lsn()` delta measurement was taken
specifically to isolate WAL bytes generated) -- the finding rests on
consistent timing behavior plus architectural reasoning, not a byte-level
WAL amplification measurement. That direct measurement is a known gap for
any future round.

**in_db_logged_zstd is meaningfully slower than external_zstd at persist
and restore**, not just "a bit slow": ~1.7x slower to persist, ~1.6-2.5x
slower to restore (widest gap on `toast_bad`: ~14.2 s vs ~5.6 s). Two
separable causes: (a) `poc_bench_chunks` is a `LOGGED` table, so loading
compressed chunks into it generates real additional WAL that `external_zstd`
never produces at all (chunk writes there are plain filesystem I/O, never
touching the WAL stream) -- this is a second, smaller echo of heap_v1's core
problem, on the artifact-storage path rather than the snapshot-source path;
(b) this harness's specific restore implementation for `in_db_logged_zstd`
extracts each chunk via `psql -tA` text-mode hex encoding + a `python3`
hex-decode pass, which is a real ~2x data-size-in-transit penalty compared
to `external_zstd`'s direct binary file read -- a PoC implementation
inefficiency, not an inherent property of storing chunks in the database
(a production implementation could plausibly close much of this gap with a
different extraction path, e.g. large objects or a binary-safe COPY).

## Elimination / advancement decision (1 GiB gate)

Per this round's own elimination discipline: don't eliminate for being "a
bit slow," don't protect heap_v1 just because it already exists, don't
declare `external_zstd` the winner in advance, don't eliminate
`in_db_logged_zstd` without actually measuring its WAL-generating cost (it
was measured, above, and is real).

- **heap_v1**: passes every correctness and crash-safety gate outright. Not
  eliminated on a technicality. But it is the reason Step 8 exists at
  all -- it does not compress (ratio consistently < 1, i.e. its artifact is
  *larger* than the logical data for every shape tested, including the
  highly-redundant `good_compress` shape where the other two backends
  achieve ~120x), and its artifact is a permanent, ordinary heap table that
  stays inside the primary cluster forever (counted in every future backup,
  every future WAL-based replica, indefinitely) until explicitly dropped.
  For the stated goal of this comparison -- a storage strategy for
  *retained* snapshot artifacts at growing scale -- heap_v1 is decisively,
  structurally worse on the dimension that matters most, not just slower.
  **Does not advance to the 10 GiB round.** Remains the existing
  baseline/control; nothing here removes or degrades it.
- **in_db_logged_zstd** and **external_zstd**: both pass every correctness
  and crash-safety gate. Neither dominates the other on *every* important
  metric -- compression is a tie; `external_zstd` clearly wins persist
  speed, restore speed, and produces zero extra primary-cluster WAL;
  `in_db_logged_zstd` retains a real, non-trivial operational advantage
  (the artifact travels automatically with any routine logical/physical
  backup of the database -- no second filesystem location's lifecycle,
  permissions, or backup coverage to manage separately). Per this round's
  own rule against eliminating without clear all-metric domination, **both
  advance as finalists.**

## 10 GiB finalist round: BLOCKED_BY_CAPACITY

Capacity preflight (required before attempting any 10 GiB run): linear
projection from the measured 1 GiB end-of-run `ephemeral_disk_bytes`
(`DATA` dir) + external artifact bytes, x10:

| Combination | 1 GiB DATA+external (GB) | 10 GiB projected peak (GB) |
|---|---|---|
| in_db_logged_zstd / ordinary_bad | 5.96 | 59.6 |
| in_db_logged_zstd / toast_bad | 7.49 | 75.0 |
| external_zstd / ordinary_bad | 5.44 | 54.4 |
| external_zstd / toast_bad | 6.39 | 63.9 |

This projection is itself an *undercount* of the real peak: it only
reflects `$DATA`'s end-of-run size, not the transient `WORK_ROOT` files
live during the run (the raw uncompressed COPY BINARY stream, ~1x logical;
chunk staging, ~1x compressed artifact; restore reconstruction, another
~1x logical + ~1x compressed) that commit `b03243f`'s cleanup fix removes
only *after* a run completes. A realistic single-run peak is closer to
85-95 GB for the worst case (`in_db_logged_zstd`/`toast_bad`).

Available capacity at preflight time: **43 GB** (single filesystem; the PG
data directory and the external artifact directory share it in this
environment, so they are not separately accountable). 43 GB is less than
the low-end 10 GiB projection (59.6-75.0 GB) for *any* of the four
finalist/shape combinations, let alone with the required +8 GB safety
reserve or the additional transient overhead.

**No 10 GiB run was attempted.** No unrelated files or other targets were
deleted to force capacity (only this session's own superseded PoC working
files, already gitignored and regenerable, were reclaimed -- 37 GB across
the 1 GiB matrix runs, bringing available capacity from 5.9 GB back to
43 GB, still short of what a 10 GiB run needs).

Runnable when capacity is available (sequentially, not in parallel, so the
harness's own cleanup reclaims disk between runs; each needs ~2 reps per
this round's own repeatability rule):

```bash
./scripts/poc/run_poc_storage_backend_benchmark.sh bench in_db_logged_zstd ordinary_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench in_db_logged_zstd toast_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench external_zstd ordinary_bad 10240 rep1
./scripts/poc/run_poc_storage_backend_benchmark.sh bench external_zstd toast_bad 10240 rep1
# repeat each with rep2
```

## Decision-gate table

| Backend | Correctness | Crash safety | Artifact size (ordinary_bad, 1 GiB) | Peak disk | WAL amplification | Snapshot time | Restore RTO | Ops complexity | 1 GiB | 10 GiB |
|---|---|---|---|---|---|---|---|---|---|---|
| heap_v1 | PASS (18/18) | PASS | ~1.08 GB (no compression) | Highest (full extra live-cluster copy, permanent) | Highest (fully WAL-logged CTAS under logical decoding; artifact counted in every future backup/replica forever) | Slowest (~8.0 s) | Fastest (0 ms, already live) | Lowest (nothing new to operate) | PASS, not competitive on storage | Not attempted (not a finalist) |
| in_db_logged_zstd | PASS (18/18) | PASS (scale + logic matrix) | ~493 MB (2.1x) | High (chunk table adds real WAL; artifact lives inside primary cluster) | Moderate (LOGGED chunk table generates WAL the external backend does not) | Fast (~2.1 s) | Slower of the two zstd backends (~9.2 s; PoC hex-roundtrip extraction is a real, fixable inefficiency) | Low (single backup story, no second filesystem to manage) | PASS, finalist | BLOCKED_BY_CAPACITY |
| external_zstd | PASS (18/18) | PASS (scale + logic + full adversarial matrix incl. fsync/rename ordering, split-state, orphan-GC) | ~493 MB (2.1x, ties in_db) | Lower (compressed artifact only; no extra primary-cluster WAL) | Lowest (chunk/manifest writes are plain filesystem I/O, never touch WAL) | Fast (~1.9 s) | Fastest of the two zstd backends (~5.5 s) | Moderate (separate filesystem location: permissions, backup coverage, orphan-GC discipline) | PASS, finalist | BLOCKED_BY_CAPACITY |

## Recommendation

**Recommend `external_zstd` as the primary candidate for a future
production design, with `in_db_logged_zstd` kept as a documented fallback
for operators who cannot provision a separate artifact filesystem/backup
path.** Trade-offs to weigh explicitly:

- `external_zstd` wins on every performance and WAL-amplification metric
  measured, by a consistent, repeatable margin (not "a bit faster" --
  1.7-2.5x on persist/restore, and structurally zero WAL amplification vs.
  `in_db_logged_zstd`'s real, measured WAL cost). It is the only backend
  whose artifact storage does not grow the primary cluster's own backup/
  replication footprint.
- Its cost is operational: a second filesystem location to provision,
  secure, and back up on its own schedule, plus the orphan-GC discipline
  this PoC's crash matrix exercised but a production implementation must
  still build for real (retention policy, GC scheduling, cross-host
  artifact placement if the primary and artifact filesystems should not be
  the same disk -- notably, this whole preflight was forced to treat them
  as the same filesystem in this environment, which is not how a real
  deployment reducing blast radius would want it).
- `in_db_logged_zstd`'s appeal is entirely operational simplicity (one
  backup story, no second location), which is a legitimate, real
  consideration for some operators, not a strawman -- hence it is not
  eliminated here, only ranked second.
- heap_v1 remains correct and simple but does not solve the problem this
  comparison exists to address; nothing here suggests removing it as a
  fallback/control path.

## What is proven, what is not

**Proven at 1 GiB, this round:** correctness (18/18 combinations, all three
data shapes, 2 reps each), crash safety at real 1 GiB scale for the most
scale-sensitive points (mid-materialize crash, metadata-commit atomicity,
duplicate-identity rejection) across all three backends, the full
adversarial/logic crash matrix at 128-256 MiB for `external_zstd` and a
partial matrix for `in_db_logged_zstd` (missing `missing_chunk` and
`manifest_mismatch` specifically), and real, repeatable performance/storage
differences between the three backends.

**Not proven, anywhere in this round:**
- Behavior at 10, 25, or 50 GiB scale -- entirely unknown. Linear
  projection from 1 GiB is the only basis available, and this round's own
  capacity preflight shows even a *single* 10 GiB run would need ~2x the
  disk actually available in this environment; real behavior at that scale
  (checkpoint/vacuum pressure, lock-hold duration, xmin/vacuum horizon
  effects proportional to a much longer copy window, whether `zstd -T0`
  compression throughput actually scales linearly rather than becoming
  CPU-bound, whether the in-DB LOGGED-chunk-table WAL cost becomes
  prohibitive rather than merely measurable) is unknown.
- Real fsync-durability against host power loss (stated above).
- `missing_chunk`/`manifest_mismatch` for `in_db_logged_zstd` specifically
  (stated above).
- Direct byte-level WAL amplification measurement (the heap_v1 and
  in_db_logged_zstd WAL costs are real and consistently observed via timing
  and architecture, but were never instrumented as actual WAL bytes
  generated per run).
- A 24-hour run, multi-artifact retention/lifecycle behavior, concurrent
  multi-table protection, or anything at all beyond a single artifact per
  run.
- Whether `in_db_logged_zstd`'s restore-speed disadvantage is fundamental
  or an artifact of this PoC's specific (hex-roundtrip) extraction
  implementation -- flagged above as plausibly fixable, but not tested.

## Explicitly out of scope for this round (per its own instructions)

No production `SnapshotStore` backend was added. `storage_backend`'s CHECK
constraint was not extended. No public API or GUC was added. No external
artifact path was wired into production. Step 9 was not started. 25/50 GiB
qualification was not started. The 24-hour run was not started.

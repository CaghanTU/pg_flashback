# Step 8 decision analysis: 1 GiB three-storage-method comparison

**Source commit:** `4bb69b208dcc96ed24b194fd1f83064fe4cec010` (tree clean, `tree_clean: true`)
**Extension binary sha256:** `1b32ba259e700b6898ce48f502c49bb75828ed10cf3897ad627bb9ab171963b4`
**Harness:** `scripts/poc/run_poc_storage_backend_benchmark.sh`
**Raw evidence:** `docs/evidence/step8-1gib-matrix-4bb69b208dcc.json` (18 `bench`-mode runs, all `status: PASS`, one consistent `source_commit`/`extension_binary_sha256`, `source_tree_dirty: false`, `commits_replayed == copy_window_commits` exactly on every row, `failed_steps: 0` / `missing_steps: []` on every row)

**This document is analysis only.** No production code changed. No `SnapshotStore` backend was added. No `storage_backend` CHECK constraint change. No 10/25/50 GiB run was started. No 24-hour soak was started. No backend was chosen or implemented. Nothing was pushed.

**Relationship to prior Step 8 evidence:** `docs/adr/0004-storage-backend-benchmark-poc.md` and `docs/evidence/step8-storage-backend-benchmark.json` are both pinned to source commit `c4a4a456916b57298aee733bb1a1844b31f9047c`. Between that commit and `4bb69b208dcc`, production code changed materially (`sql/functions/api_track_capture.sql`, `restore_lsn.sql`, `restore_verify.sql`, `restore_helpers.sql`, `local_compatibility.sql`, `drop_dependency_manifest.sql`, `recover_plan.sql`, `coverage_runtime.sql`, `ddl_staging_core.sql`, `lifecycle_bootstrap_core.sql`, `schema_bootstrap.sql`, `src/capture/ddl_hook.rs`, `src/lib.rs` — 1,449 insertions / 335 deletions across 13 files), and the extension binary sha256 changed (`2d3421a7...` &rarr; `1b32ba25...`). Per this project's own evidence-provenance discipline (see the `superseded_notice` pattern in `docs/evidence/step7-online-snapshot-wal-alignment.json`), **the old ADR's crash-safety, adversarial, and backup-footprint numbers must not be cited as still validating current HEAD.** This round only re-ran the 18 happy-path `bench` combinations; it did **not** re-run the 9 scale-sensitive crash scenarios, the 31 128 MiB adversarial scenarios, or the 2 backup-footprint runs. Section 4 below is a **code-level design review** of operational correctness, not a re-execution of that crash matrix — this distinction is called out explicitly throughout and again in "What is still missing" (§6).

---

## 1. Ratio terminology (corrected)

The harness's own `compression_ratio` metric is `source_bytes / artifact_bytes` (bigger = better compression; `<1` means the artifact is *larger* than the source). That is kept, but named explicitly below alongside its inverse so neither number is ambiguous:

| Term | Definition | Reading |
|---|---|---|
| `source_bytes` | `source_logical_bytes` — snapshot-consistent logical size of the ground-truth table at boundary time | — |
| `artifact_bytes` | On-disk size of the persisted artifact (heap table via `pg_total_relation_size`, in-DB chunk rows via `sum(octet_length(chunk_bytes))`, or external chunk+manifest files via `du -sb`) | — |
| `artifact_bytes / source_bytes` | **storage footprint ratio** | `<1.0` = artifact smaller than source (good); `>1.0` = artifact larger than source (bad) |
| `source_bytes / artifact_bytes` | **compression_ratio** (harness's field name, kept) | `>1.0` = compression achieved; `<1.0` = expansion |

| backend | shape | source_bytes | artifact_bytes | artifact/source (storage footprint) | compression_ratio (source/artifact) |
|---|---|---|---|---|---|
| heap_v1 | ordinary_bad | 1039.8 MB | 1076.9 MB | 1.0356 | 0.9656 |
| heap_v1 | toast_bad | 1077.4 MB | 1364.0 MB | 1.2660 | 0.7899 |
| heap_v1 | good_compress | 1083.7 MB | 1127.8 MB | 1.0407 | 0.9609 |
| in_db_logged_zstd | ordinary_bad | 1039.8 MB | 495.2 MB | 0.4763 | 2.0997 |
| in_db_logged_zstd | toast_bad | 1077.4 MB | 1075.1 MB | 0.9978 | 1.0022 |
| in_db_logged_zstd | good_compress | 1083.7 MB | 9.1 MB | 0.0084 | 119.7294 |
| external_zstd | ordinary_bad | 1039.8 MB | 495.3 MB | 0.4764 | 2.0992 |
| external_zstd | toast_bad | 1077.4 MB | 1075.1 MB | 0.9978 | 1.0022 |
| external_zstd | good_compress | 1083.7 MB | 9.1 MB | 0.0084 | 118.7930 |

`heap_v1` always has a storage footprint **>1.0** (it never compresses; TOAST-heavy data actually *expands* by 26.6% due to heap overhead vs the TOASTed source). Both zstd backends tie on storage footprint at every shape (as designed — same COPY BINARY + zstd chunking); their small artifact-byte differences (e.g. 495.2 MB vs 495.3 MB) are ordinary zstd non-determinism in header/frame overhead across independent client runs, not a backend-driven difference.

---

## 2. Per-backend x shape aggregated metrics (median of 2 reps)

Field mapping, stated explicitly (the harness's own instrumentation does not use the exact phase names given in the request, so this is an explicit correspondence, not a renaming):

- **snapshot/materialization time** &rarr; `snapshot_create_ms` (coordinator-lock acquisition, writer bootstrap, boundary-marker fix, and the artifact-materialization sub-step); `materialize_ms` is reported alongside as the nested sub-timer for just the CTAS/`\copy` extraction inside that window (confirmed by direct arithmetic: `materialize_ms <= snapshot_create_ms` on every one of the 18 runs).
- **recovery/materialization read time** &rarr; `restore_ms` (rebuilding a fresh, independently queryable table from the persisted artifact).
- **WAL replay time** &rarr; **not separately instrumented.** The harness's own documented caveat (carried over from the prior ADR and confirmed by re-reading the current script) states this cost is bundled inside `snapshot_create_ms`, which includes "decoding and replaying the writer's churn against a throwaway shadow table" as part of establishing/proving the boundary. There is no standalone `wal_replay_ms` metric to report; see §6.
- **verification time** &rarr; `verify_ms` (row count, semantic fingerprint, canonical schema fingerprint, TOAST byte-equality for `toast_bad`, owner, ACL — see §4).
- **total RTO** &rarr; `total_rto_ms`, which the script computes as `snapshot_create_ms + persist_ms + restore_ms + verify_ms` (verified by direct arithmetic on every run; `materialize_ms` is **not** additively counted — it is inside `snapshot_create_ms`, not beside it).

| backend | shape | snapshot_create_ms (of which materialize_ms) | persist_ms | restore_ms | verify_ms | total_rto_ms |
|---|---|---|---|---|---|---|
| heap_v1 | ordinary_bad | 32530 (mat 8306) | 34 | 9994 | 44912 | 87470 |
| heap_v1 | toast_bad | 31706 (mat 22498) | 31 | 21415 | 72296 | 125448 |
| heap_v1 | good_compress | 16940 (mat 6246) | 30 | 7884 | 19598 | 44450 |
| in_db_logged_zstd | ordinary_bad | 41315 (mat 40322) | 14394 | 36747 | 46414 | 138870 |
| in_db_logged_zstd | toast_bad | 16023 (mat 15138) | 25718 | 66426 | 82598 | 190764 |
| in_db_logged_zstd | good_compress | 12426 (mat 11796) | 5467 | 15705 | 19168 | 52766 |
| external_zstd | ordinary_bad | 41473 (mat 40408) | 8392 | 16016 | 43662 | 109542 |
| external_zstd | toast_bad | 15588 (mat 14702) | 12362 | 22991 | 71569 | 122511 |
| external_zstd | good_compress | 12426 (mat 11846) | 5490 | 14568 | 20880 | 53362 |

WAL bytes per phase (real, measured via `pg_current_wal_insert_lsn()`/`pg_wal_lsn_diff()`, not inferred):

| backend | shape | WAL materialize | WAL persist | WAL restore | WAL total |
|---|---|---|---|---|---|
| heap_v1 | ordinary_bad | 1228.0 MB | 0.0 MB | 1225.5 MB | 2.453 GB |
| heap_v1 | toast_bad | 1186.4 MB | 0.0 MB | 1179.2 MB | 2.366 GB |
| heap_v1 | good_compress | 1140.6 MB | 0.0 MB | 1139.9 MB | 2.280 GB |
| in_db_logged_zstd | ordinary_bad | 15.1 MB | 532.1 MB | 964.7 MB | 1.512 GB |
| in_db_logged_zstd | toast_bad | 6.2 MB | 1154.1 MB | 1180.3 MB | 2.341 GB |
| in_db_logged_zstd | good_compress | 2.4 MB | 9.7 MB | 1068.2 MB | 1.080 GB |
| external_zstd | ordinary_bad | 15.3 MB | 1.3 MB | 961.8 MB | 0.978 GB |
| external_zstd | toast_bad | 5.9 MB | 2.1 MB | 1171.4 MB | 1.179 GB |
| external_zstd | good_compress | 2.4 MB | 0.0 MB | 1068.2 MB | 1.071 GB |

**Notable pattern:** `wal_bytes_restore` is essentially the same (~0.96-1.18 GB) across all three backends for a given shape, regardless of artifact size. This is expected — `restore_*` always writes a full, freshly-materialized heap table under `wal_level = logical`, so restore WAL tracks the *restored* table's logical size, not the artifact's compressed size. Compression only pays off on the **persist** side (write-once into the artifact) and on **backup footprint** (not re-measured this round — see §6).

Disk-footprint fields, and what they actually are (see §6 for the "peak" caveat):

| backend | shape | `ephemeral_disk_bytes` (whole PGDATA dir, snapshot taken at end-of-run before cleanup) | `external_artifact_bytes_at_cleanup` | commits (`copy_window_commits == commits_replayed`, exact) |
|---|---|---|---|---|
| heap_v1 | ordinary_bad | 5.890 GB | 0.0 MB | 240 |
| heap_v1 | toast_bad | 6.297 GB | 0.0 MB | 793 |
| heap_v1 | good_compress | 5.752 GB | 0.0 MB | 190 |
| in_db_logged_zstd | ordinary_bad | 6.439 GB | 0.0 MB | 1510 |
| in_db_logged_zstd | toast_bad | 8.083 GB | 0.0 MB | 458 |
| in_db_logged_zstd | good_compress | 5.628 GB | 0.0 MB | 490 |
| external_zstd | ordinary_bad | 5.388 GB | 495.3 MB | 1476 |
| external_zstd | toast_bad | 5.810 GB | 1075.1 MB | 426 |
| external_zstd | good_compress | 5.602 GB | 9.1 MB | 488 |

`ephemeral_disk_bytes` is `du -sb $DATA` (the **entire** PGDATA directory: base tables + `pg_wal`, not just the artifact) taken once, right before cleanup. It is **not a per-phase breakdown** — it includes the source table, the writer's churn, the artifact (for `heap_v1`/`in_db_logged_zstd`), the separately-materialized restored table, and whatever WAL segments had not yet been recycled. `in_db_logged_zstd`'s `toast_bad` row is the largest (8.08 GB) because it is the only combination stacking a non-compressing ~1.08 GB chunk table *and* a ~1 GB restored table *and* the largest WAL total (2.341 GB) inside one PGDATA directory at once.

**CPU time:** not recorded by this harness in any run. No `/usr/bin/time`, `getrusage`, or `/proc/<pid>/stat` sampling exists in `scripts/poc/run_poc_storage_backend_benchmark.sh`. Reported as **not available**, not estimated.

**Final fingerprint and schema-proof result:** `PASS` for all 18/18 runs, uniformly, via the single shared `verify_restore()` function (script lines ~1253-1273) that every backend's `bench_verify` step calls: row-count equality, order-independent semantic fingerprint equality, canonical schema fingerprint equality (column order/type/typmod/collation via `pg_attribute`), TOAST aggregate-hash byte-equality (`toast_bad` shape only), table-owner equality, and ACL equality — all against the same snapshot-consistent ground-truth table, all `die()`-on-mismatch (fail-closed). No run in this matrix hit any of those `die()` paths.

---

## 3. Repetition variance

Every metric across all 9 (backend, shape) combinations was checked for `|rep1 - rep2| / mean * 100`. Two flags exceeded a 15% relative-spread threshold:

| combo | metric | rep1 | rep2 | spread |
|---|---|---|---|---|
| external_zstd / ordinary_bad | `lock_hold_ms` | 45 | 54 | 18.2% |
| external_zstd / toast_bad | `lock_hold_ms` | 83 | 47 | 55.4% |
| heap_v1 / ordinary_bad | `wal_bytes_persist` | 2376 B | 1728 B | 31.6% |

**None of these are material.** `lock_hold_ms` varies between 45-83 **milliseconds** total across the whole matrix — ordinary OS-scheduling jitter on a sub-100ms measurement, three orders of magnitude below any `total_rto_ms` value. `heap_v1`'s `wal_bytes_persist` spread is between 1,728 and 2,376 **bytes** (`heap_v1` has no real persist phase — this is stray WAL from the trivial no-op statement the timer wraps), against a `wal_bytes_total` of ~2.3-2.5 **GB** for that same row — six orders of magnitude smaller than the total it's a component of.

Every metric that actually matters for a storage-backend decision — `source_bytes`, `artifact_bytes`, `total_rto_ms`, every `wal_bytes_*` field, `ephemeral_disk_bytes`, `copy_window_commits`/`commits_replayed` — showed **no material rep-to-rep variance** (all well under 15%, most under 3%). Two reps per cell is a thin sample, but nothing here suggests a hidden bimodal or unstable result on this host.

---

## 4. Operational correctness review (code-level design review, not new test execution)

This section reviews what the harness's implementation actually does and guarantees, by re-reading `scripts/poc/run_poc_storage_backend_benchmark.sh` at the current commit. **It is not a re-run of the crash/adversarial matrix** (that matrix is stale — see the header note and §6). Where a claim is backed by this round's 18 happy-path runs, it says so; where it is backed only by reading the crash-injection code paths (`crash_checkpoint`, `persist_external_zstd`, `persist_in_db_logged_zstd`, `validate_manifest_binding`) without executing them this round, it says that instead.

### Crash during create

- **`external_zstd`**: two-phase commit, code-verified. Each chunk: write to `.tmp-<artifact_id>/chunk_N.zst.tmp` &rarr; `fsync_path` &rarr; `mv -f` (atomic rename within the same filesystem) to `$art_dir/chunk_N.zst` &rarr; `fsync_path` again. After all chunks: manifest JSON gets the identical temp-write &rarr; fsync &rarr; rename &rarr; fsync(parent dir) treatment. **Only after** the on-disk manifest is durably renamed into place does the DB-side `poc_bench_manifest` row flip `state='creating'` &rarr; `state='available'` (script lines ~1182-1189). A crash at any point before that final `UPDATE` leaves files potentially present but the DB state still `'creating'`.
- **`in_db_logged_zstd`**: chunk rows are inserted individually as autocommitted statements (each `INSERT INTO poc_bench_chunks ...` is its own implicit transaction) while the manifest row stays `state='creating'`; only the final `UPDATE ... SET state='available'` (script lines ~996-1000) makes the artifact visible as restorable.
- **`heap_v1`**: no manifest/state row exists at all for this backend — the "artifact" *is* the table, created by a single `CREATE TABLE ... AS SELECT` DDL statement (script line ~743). PostgreSQL's own transactional DDL semantics apply directly: the CTAS is a single WAL-logged transaction that either fully commits (table + all rows visible) or fully rolls back (table does not exist). This is a **structural** advantage for `heap_v1` on this specific axis — it needs no bespoke state machine because it borrows PostgreSQL's native commit atomicity — but it also means `heap_v1` has **no equivalent of `validate_manifest_binding`** (see "Corruption detection" below).

### Partial artifact visibility

- `external_zstd` and `in_db_logged_zstd` both fail closed the same way: `restore_*` reads `state` from `poc_bench_manifest` first and `die()`s unless it is exactly `'available'` (script lines ~1069-1070, ~1207-1208). A crash after files/rows are partially written but before the state flip is invisible to any restore attempt — not silently wrong, but not silently *clean* either: the partial bytes remain on disk/in-table (see "Retention/retirement" below).
- `heap_v1` has no state gate; there is nothing to check but "does the table exist and is it queryable," which PostgreSQL's transactional DDL already guarantees is all-or-nothing.

### Atomic finalize

- `external_zstd`'s finalize order (data fsync &rarr; data rename &rarr; data fsync &rarr; manifest fsync &rarr; manifest rename &rarr; parent-dir fsync &rarr; DB state flip) is the standard "durable-write-then-atomically-publish" pattern and is the most defensively layered of the three (it is the only backend with a real filesystem-durability step independent of PostgreSQL's own WAL/fsync machinery).
- `in_db_logged_zstd`'s finalize is a single `UPDATE` statement, riding entirely on PostgreSQL's own transactional durability (the `LOGGED` table + WAL) — simpler to reason about, but means artifact durability is entirely coupled to the source cluster's own crash-recovery correctness (no independent second copy of the durability guarantee).
- `heap_v1`'s finalize is implicit in the CTAS commit itself, as above.

### Restart cleanup

**Gap, uniform across all three backends, confirmed by reading the code (not by a new test):** nothing in this harness automatically reclaims orphaned/partial state after a crash+restart.
- `external_zstd`: an orphaned `.tmp-<artifact_id>/` directory left by a crash is only removed by the manual sweep pattern demonstrated in `run_crash_orphan_gc` (script lines ~1770-1799) — a `for d in "$EXTERNAL_ARTIFACT_ROOT"/.tmp-*; do rm -rf "$d"; done` loop that exists **only inside that one test function**, not as a startup hook, scheduled job, or anything invoked automatically after a crash+restart.
- `in_db_logged_zstd`: a crash leaves a `poc_bench_manifest` row stuck at `state='creating'` plus however many `poc_bench_chunks` rows were inserted before the crash. Nothing in the harness re-scans for stale `'creating'` rows on cluster restart or ever deletes them.
- `heap_v1`: a crash mid-CTAS leaves nothing behind (PostgreSQL's transaction rollback handles it), so there is no restart-cleanup gap here — the one backend where this is a non-issue by construction.

### Corruption detection

- **`in_db_logged_zstd` / `external_zstd`**: both go through `validate_manifest_binding()` (script lines ~1025-1050) **before any restore work happens**: format_version, backend identity, `pg_major`, architecture, `system_identifier`, current db OID, encoding, `tracking_id`, semantic-fingerprint format version, canonical schema fingerprint, row count — each individually `die()`s on mismatch — plus a cross-check of `boundary_lsn`/`boundary_xid` against the independently-decoded WAL commit ledger (`commit_log`), i.e. the manifest's own provenance claim must agree with what the WAL-decode oracle actually observed, not just with itself. Chunk-level integrity is checked separately: each chunk's SHA-256 is verified against the value recorded at persist time, and the fully reassembled stream's SHA-256 is checked against the manifest's `raw_stream_sha256` before any `\copy ... FROM ... (FORMAT binary)` is attempted.
- **`heap_v1`**: has no equivalent pre-restore binding check (no manifest row exists to check `pg_major`/`system_identifier`/schema fingerprint against). Its only corruption detection is the shared post-restore `verify_restore()` (see §2) — which *would* still catch a schema or data mismatch, but only after the (wasted) restore work, and it has no way to refuse a restore attempted against, say, a different `pg_major` or a different `system_identifier` host before doing that work. This is the clearest, most concrete correctness gap `heap_v1` has relative to the other two, grounded directly in what code exists vs. doesn't.
- **Not re-verified this round:** whether these `die()` paths actually fire correctly under real tampering (corrupt chunk, missing chunk, wrong identity, row-count mismatch, chunk-order mismatch, duplicate retry) is exactly what the *stale* 31-scenario 128 MiB adversarial matrix in the old ADR tested, at `c4a4a45`. The code paths above still exist unchanged in the current script, but "the code exists" and "the code was proven to work at this commit" are different claims — see §6.

### Retention/retirement behavior

**None of the three backends implement a real retention/retirement system.** This PoC always keeps exactly one artifact per run and either lets the whole cluster get torn down (happy path) or leaves orphaned partial state behind (crash path, see above). The one place multi-generation retention was exercised at all — `backup-footprint` mode resetting the artifact between backend measurements — needed a real bug fix to even work correctly: a plain `DELETE FROM poc_bench_chunks` marks rows dead but does not shrink the table's physical file, so a naive multi-generation retention scheme built the same way would accumulate bloat in the `LOGGED` chunk table indefinitely; the harness had to switch to `DROP`+recreate to get a clean measurement (documented in the old ADR's correction #9 — an implementation detail this round did not need to touch, since this round didn't run `backup-footprint` mode, but the underlying PostgreSQL behavior it depends on has not changed). **A production `SnapshotStore` needs to design retention/expiry for real; nothing here is that design**, for any of the three backends.

### Permission/security boundary

- `in_db_logged_zstd` and `heap_v1` inherit PostgreSQL's own role-based ACL/permission model automatically — the artifact is just rows/tables in the same database, subject to the same `GRANT`/`REVOKE`, `pg_hba.conf`, and role membership as everything else.
- `external_zstd` writes plain files under `$EXTERNAL_ARTIFACT_ROOT` with **no explicit permission hardening anywhere in the script** — no `umask`, `chmod`, or `chown` call exists in `persist_external_zstd` or anywhere else in the file. Files and directories are created with whatever the ambient process umask produces. This means the compressed chunks (full table contents) and the manifest are only as protected as the filesystem directory permissions happen to be — there is no PostgreSQL-role-equivalent access control on that data at all in this PoC. This is a **real, unaddressed gap** for `external_zstd` specifically, not something this round measured as failing (nothing tests file permissions), just something that is absent from the implementation as read.

### WAL/database-bloat amplification

- **`heap_v1`** amplifies WAL the most on every phase measured (materialize ~1.14-1.23 GB, restore ~1.14-1.23 GB, total ~2.28-2.45 GB per run — see §2). It also permanently occupies base-table disk space for both the artifact and the restored table, ordinary heap bloat/VACUUM rules apply to both.
- **`in_db_logged_zstd`** trades WAL cost for storage cost depending on compressibility: cheap on WAL for `good_compress` (persist WAL ~9.7 MB) but nearly as expensive as `heap_v1` for `toast_bad` (persist WAL ~1.15 GB, because near-zero compression means the `LOGGED` chunk table is inserting close to the full logical size). It also carries the DELETE-doesn't-shrink bloat risk described above under any real retention scheme — a legitimate, and currently unsolved, database-bloat exposure specific to storing chunks as rows in an ordinary table.
- **`external_zstd`** has the lowest WAL cost on every phase and every shape measured this round (persist WAL 0.0-2.1 MB; see §2), because chunk writes are pure filesystem I/O that never touches the WAL stream at all — only the tiny manifest pointer row does. Its disk-bloat exposure moves entirely off the database (ordinary filesystem lifecycle tools apply, not `VACUUM`/heap-bloat semantics) — at the cost of needing that lifecycle managed somewhere else (see "Retention/retirement" above).

---

## 5. Pareto comparison

Using this round's 18 fresh measurements (`ordinary_bad` shown as the representative shape; `good_compress`/`toast_bad` follow the same relative ordering except where noted):

| Axis | Winner | Basis |
|---|---|---|
| **Fastest RTO** | `heap_v1` on 2 of 3 shapes (87,470 ms `ordinary_bad`, 44,450 ms `good_compress`); `external_zstd` only on `toast_bad` (122,511 vs `heap_v1`'s 125,448) | §2 `total_rto_ms`, see note below |
| **Lowest persistent storage (artifact bytes)** | `in_db_logged_zstd` and `external_zstd` tie exactly (both COPY BINARY + zstd; 495.2 vs 495.3 MB `ordinary_bad`, 9.1 vs 9.1 MB `good_compress`) | §1/§2 |
| **Lowest peak storage (WAL + disk footprint)** | `external_zstd` — lowest `wal_bytes_total` on every shape (0.978-1.179 GB vs `in_db_logged_zstd`'s 1.080-2.341 GB vs `heap_v1`'s 2.280-2.453 GB) and lowest `ephemeral_disk_bytes` on 2 of 3 shapes | §2 |
| **Simplest and safest lifecycle** | `heap_v1` — no manifest/state machine, no second filesystem location, atomicity inherited directly from PostgreSQL DDL commit semantics; but it is also the backend with the *least* corruption-detection coverage (no pre-restore binding check) — "simplest" and "safest" pull in different directions here, see §4 | §4 |

**Note on "fastest RTO":** `in_db_logged_zstd` is the slowest RTO on every shape (138,870 / 190,764 / 52,766 ms), consistent with the prior ADR's finding that its chunk-extraction path (`psql` text-mode hex round-trip + `python3` hex-decode) is the dominant restore-time cost, not necessarily an inherent property of storing chunks in the database (see §6 — this specific implementation-vs-fundamental question was never resolved and still isn't). `heap_v1` winning RTO on 2 of 3 shapes is notable precisely because it is the backend with the *worst* WAL/storage numbers — RTO and storage/WAL cost are different axes that do not move together here.

No backend is Pareto-dominant across all four axes simultaneously: `heap_v1` wins simplicity and (mostly) RTO but loses storage and WAL/bloat by a wide margin; `external_zstd` wins storage-tie-plus-peak/WAL but has the least simple lifecycle (external filesystem location, no OS-level ACL) and unaddressed permission hardening; `in_db_logged_zstd` never uniquely wins any single axis outright (ties `external_zstd` on artifact size, is slower on RTO than both others on every shape, and is not the lowest on WAL/bloat except vs. `heap_v1` on compressible data) but offers "artifact travels with routine DB backup" operational simplicity that neither measurement axis captures numerically.

---

## 6. Explicit answers

### Is `in_db_logged_zstd` dominated by `external_zstd`?

**On every measured numeric axis this round, yes — with one asterisk.** Artifact size ties exactly. On every shape, `external_zstd` has lower `total_rto_ms`, lower `wal_bytes_total`, and (on 2/3 shapes) lower `ephemeral_disk_bytes` than `in_db_logged_zstd`. `in_db_logged_zstd` does not win any numeric axis measured this round.

**The asterisk is not numeric — it is operational, and this round's data cannot resolve it either way:** `in_db_logged_zstd`'s artifact lives inside the same database, so it automatically inherits whatever backup coverage, retention policy, and access-control model already protects the rest of the database; `external_zstd` requires a second filesystem location with its own backup schedule, its own retention/GC (currently unimplemented — §4), and its own access control (currently absent — §4). Whether that operational cost outweighs `external_zstd`'s measured advantage is a judgment call this document is explicitly not making (see the "do not choose" instruction this document is written under). What *can* be said precisely: nothing measured this round gives `in_db_logged_zstd` a numeric edge that would offset that judgment call in its favor — its case rests entirely on the unmeasured operational-simplicity argument, not on any benchmark result.

### For the 50 GiB target, is `heap_v1`'s storage cost acceptable?

**Not evaluable from this round's data, and this round did not test at anywhere near that scale.** What can be extrapolated *linearly* from the 1 GiB numbers (with the explicit caveat that PostgreSQL WAL/disk behavior at 50x scale is not guaranteed to be linear — checkpoint behavior, `max_wal_size` pressure, and TOAST/heap bloat patterns can all change qualitatively at larger scale):
- `heap_v1`'s storage footprint ratio is already `>1.0` at 1 GiB for every shape (1.04-1.27x), meaning a naive linear projection puts a 50 GiB source table's `heap_v1` artifact at roughly 52-63 GB, before counting the separately-materialized restored table (another ~50-63 GB) or its ~2.3-2.5x WAL amplification (~115-125 GB of WAL for materialize+restore combined, linearly projected).
- The prior (stale, `c4a4a45`) ADR's own 10 GiB preflight found available capacity (43 GB in that environment) was already below the *low end* of its projected single-run peak (59.6-75.0 GB) for the zstd finalists alone — `heap_v1`, being the most expensive of the three on every WAL/storage axis measured, would be worse, not better, under the same linear projection.
- **This is a projection from 1 GiB data, not a 50 GiB measurement.** The honest answer to "is it acceptable" is: **unknown, and the direction of the unknowns (checkpoint behavior, non-linear TOAST/bloat effects) is at least as likely to make it worse as better.** A real answer requires the 10/25/50 GiB runs this task explicitly says not to start yet.

### Does `external_zstd` preserve all correctness guarantees needed by `SnapshotStore`?

**Partially proven, partially not re-verified this round, and at least one gap identified that was never proven even in the stale round.**
- **Proven fresh, this round (18/18):** row count, semantic fingerprint, canonical schema fingerprint (column order/type/typmod/collation), owner, and ACL equality on every restore; `commits_replayed == copy_window_commits` exactly; manifest-binding validation code path exists and is structurally sound (read, not re-executed under tampering this round).
- **Proven only by the stale (`c4a4a45`) round, not re-verified at current HEAD:** the fsync/rename-ordering, split-state, missing-chunk, corrupt-chunk, manifest-mismatch, wrong-identity, and orphan-GC crash scenarios. The code implementing those checks (§4) still exists unchanged in the current script, but "unchanged code" is not the same evidentiary weight as "passed at this commit" — this project's own convention (the step7 `superseded_notice`) treats that distinction as load-bearing, and this document follows the same convention rather than asserting the stale PASS still holds.
- **Never proven, in either round:** real fsync-durability against actual host power loss (explicitly disclosed as untestable in this environment, both then and now) — crash points are simulated via `pg_ctl -m immediate` restarts and abrupt kills of the writing shell step, which proves incomplete state is never mistaken for valid, but does not prove durability against real power loss.
- **Never proven, in either round, and newly identified this round (§4):** the permission/security boundary. No test, in either round, has ever exercised file permissions on `$EXTERNAL_ARTIFACT_ROOT`, and the implementation has no hardening to test in the first place.
- **Net:** `external_zstd`'s *data*-correctness guarantees (the ones `SnapshotStore` most directly needs — does a restore return exactly what was protected) are well-exercised in design and were fully proven under tampering as of `c4a4a45`, not re-proven as of `4bb69b208dcc`. Its *operational* guarantees (retention, permissions, real fsync durability) were never proven in either round and are not close to production-ready as implemented.

### Which backend should proceed to Step 9, and what evidence is still missing before that decision is binding?

**No selection is made here — this document is explicitly scoped not to choose one.** What can be said: the numeric case for eliminating `in_db_logged_zstd` in favor of `external_zstd` is stronger after this round's fresh data than before (no axis where `in_db_logged_zstd` wins outright), while `heap_v1` remains correct and simple but not competitive on storage/WAL at any shape tested, and its 50 GiB viability is actively in question, not just unconfirmed.

**Evidence still missing before any advancement decision is binding:**
1. **Re-running the crash-safety and adversarial matrix at current HEAD (`4bb69b208dcc`).** The existing 40-scenario matrix is stale; nothing in this round re-validated it, and production code changed materially since it last ran.
2. **Re-running (or running for the first time) `backup-footprint` mode at current HEAD**, including the `good_compress` shape, which the stale round itself never measured for backup delta.
3. **A true continuous peak-disk/peak-WAL measurement**, not the end-of-run snapshot this round (and the stale round) both relied on. `checkpoint_timeout=900s` for these short runs means no mid-run checkpoint recycling occurred, which makes the end-of-run snapshot a reasonable proxy for *this specific run length and configuration* — but it is not a directly sampled peak, and transient files (e.g. the uncompressed 64 MB split chunks that `zstd --rm` deletes as it goes) are invisible to an end-of-run snapshot.
4. **CPU time**, not recorded in either round.
5. **A resolution of whether `in_db_logged_zstd`'s restore-speed disadvantage is fundamental or a PoC-implementation artifact** of its `psql` hex-round-trip extraction path — explicitly still open per the prior ADR and untouched this round.
6. **A real retention/expiry design** for whichever backend(s) advance — none of the three have one; this round's code review only reconfirmed the gap, it did not close it.
7. **Permission/security-boundary hardening and testing for `external_zstd`** specifically — currently neither implemented nor tested, in either round.
8. **Any data at 10/25/50 GiB scale** — entirely absent, by this task's own explicit instruction not to start it yet, and (per the "50 GiB" answer above) the direction of the open unknowns argues for measuring before assuming linear scaling holds.

---

## What this document is not

Not a production design. Not a chosen winner. Not a re-validation of the stale crash/adversarial/backup-footprint evidence. Not a claim that `external_zstd`'s or any other backend's operational gaps (retention, permissions) have been closed. No product code, SQL, GUC, or public API was touched to produce it — this document and its companion raw-data file (`docs/evidence/step8-1gib-matrix-4bb69b208dcc.json`) are the only new artifacts.

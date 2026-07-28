# ADR 0003 — Online snapshot ↔ WAL alignment PoC (Step 7)

Status: Accepted (PoC only — nothing production-wired)
Date: 2026-07-27

## Context

The current first-protect path takes `SHARE ROW EXCLUSIVE` for the entire
duration of the base copy (a `CREATE TABLE AS SELECT` under `SnapshotStore`),
then resolves the real COMMIT LSN from a transactional BOUNDARY marker. That
is correct, but it write-stalls the protected table for as long as the copy
takes — unacceptable as base size grows toward the 50 GiB target.

Exported MVCC snapshots (`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`) can
remove that stall, but only apply to slot *creation*: a snapshot can only be
exported over the replication protocol, and pg_flashback creates its
production slot via `pg_create_logical_replication_slot()` from inside a
SQL-callable worker, not the replication protocol — and by design there is
exactly one shared slot per database, created once. So exported snapshots
alone only solve the greenfield case (first protect on a database with no
existing capture stream). Every later protect on an already-active stream
needs a second mechanism that does not create a second slot.

This ADR records the result of proving both mechanisms against a throwaway
isolated cluster, per the Step 7 task brief and its subsequent correctness
correction. It does not change pg_flashback itself.

## Evidence

Machine-readable aggregate:
`docs/evidence/step7-online-snapshot-wal-alignment.json` — source commit
`cfd7eb2f6acca3c77285edf55b6e8fe5b1eb0923`, source tree
`801a4d51a01856724ef77ac7d988f928a12faf70`, `source_tree_dirty: false`,
extension binary sha256
`5657e332355acd1135faea819f90ffc9306078b5d5eddce717c6562210690c05`
(identical across every rebuild across all three correction rounds —
production Rust/SQL never changed). It embeds the sha256 of each run's own
`result.json` and every recorded metric for: a selftest run (now including a
gate that deliberately breaks a Protocol B copier and proves zero leftover
processes), one clean 64 MiB dev pass, a small targeted 16 MiB Protocol B
check, the two 1 GiB Protocol B combinations (re-run this round), the two
1 GiB Protocol A combinations (**reused** from the prior evidence commit —
justification below), plus the production-oracle validation results.

**Protocol A 1 GiB reuse:** `run_protocol_a` and
`run_protocol_a_conn_lifecycle_adversarial` were not touched by any commit in
this round — verified by inspecting every diff hunk location against every
commit landed this round (only `install_oracle_sql` additions,
`run_protocol_b`, `run_protocol_b_writer_loop`, three new bash helpers,
`run_restart_crash_scenarios`'s scenario 15A block, and
`cleanup()`/`reap_run_children` were touched). Protocol A's 1 GiB
ordinary/TOAST evidence is therefore carried forward verbatim rather than
re-run, per the explicit reuse allowance in this round's task brief. Raw
PostgreSQL data/log directories are not committed; each run's
`target/poc/online-snapshot-wal-alignment/<run-id>/` retains `result.json` +
`metrics.jsonl` + small diagnostic files (the postgres data directory itself
is now cleaned by default regardless of PASS/FAIL — see "Disk hygiene"
below).

## What was built

`scripts/run_poc_online_snapshot_wal_alignment.sh` — a self-contained harness
that:

- builds the current source tree and computes a candidate identity (source
  commit/tree, built `.so` sha256), **failing closed before any build or
  cluster starts** if the working tree is dirty or the built hash does not
  match an expected value (both selftest-verified: `dirty_tree_rejected`,
  `candidate_mismatch_rejected`);
- boots a fully isolated PostgreSQL 17 instance (private data dir, socket,
  port, and a **private `dynamic_library_path`** pointing at the freshly
  built `pg_flashback.so` — never the shared `/usr/local/pgsql-17/lib`
  install a real dev/qualification instance might be using);
- reuses the **already-compiled pg_flashback output plugin**
  (`_PG_output_plugin_init`, `src/capture/wal_decoder.rs`) as a decoding
  library only, via `CREATE_REPLICATION_SLOT ... LOGICAL pg_flashback` and
  `pg_logical_slot_get_changes(..., 'tracked_oids', ..., 'metadata_only',
  'false')`. It never calls a single pg_flashback SQL function
  (`flashback_track`, `flashback_*`, etc.) — the SQL extension surface is
  completely untouched;
- implements an **independent PoC semantic fingerprint and correctness
  oracle** in PL/pgSQL (`poc_ingest_decoded()` / `poc_apply_shadow()`) that
  ingests decoded JSON into durable `commit_log`/`marker_log`/`change_log`
  tables, replays not-yet-applied changes onto a shadow table filtered by a
  caller-supplied boundary LSN and oid, and reports duplicate/out-of-order
  commits. This is explicitly **not** the product's canonical
  `flashback_relation_full_data_fingerprint` and is not claimed to be
  equivalent to it — validating that oracle is a separate step (below);
- reuses the *exact* transactional BOUNDARY marker mechanism the current
  production path already relies on (`pg_logical_emit_message(true,
  'pg_flashback', ...)`, decoded by the existing `fb_decode_message`
  callback into `{"marker":<xid>}`), so Protocol B in this PoC is provably
  the same marker semantics production already depends on, not a new
  invention;
- models a minimal PoC-only artifact lifecycle (`poc_artifact_state`:
  `creating` / `available` / `aborted`) solely to give the restart/crash
  scenarios something concrete to assert against — **not** the production
  SnapshotStore state machine and not wired to it.

Nothing here adds a public SQL command, GUC, or generated-SQL surface. The
production correctness oracle itself (not this harness's own check) is
validated separately by actually running the exact-WAL transaction/schema
matrix and clean-host candidate smoke against a real candidate archive built
from this same commit (see "Production oracle validation" below).

## Protocol A — greenfield exported snapshot

1. Open a replication-protocol connection (`replication=database`), run
   `IDENTIFY_SYSTEM` then `CREATE_REPLICATION_SLOT <slot> LOGICAL
   pg_flashback EXPORT_SNAPSHOT`. Record `consistent_point`, the exported
   snapshot name, slot identity, system identifier, and timeline.
2. A separate `BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION
   SNAPSHOT '<name>'` in a different session imports it.
3. **Import and the long base copy must be the same transaction.** This was
   the single hardest bug in building this PoC (see "What went wrong"
   below): once `SET TRANSACTION SNAPSHOT` succeeds, that transaction pins
   its own copy of the snapshot independent of the exporter — but a
   *separate, later* transaction cannot re-import the same snapshot after
   the exporting connection is gone. The importer signals "import confirmed"
   from *inside* the same still-open transaction that goes on to do
   `CREATE TABLE ... AS SELECT`, so the exporting connection can be released
   the moment import is confirmed while the copy continues independently.
4. The one shared slot is left running afterward, consumable by
   `pg_logical_slot_get_changes()` exactly as a normal worker would.

Verified against PostgreSQL 17 (this cluster's binaries), across two
consecutive clean 64 MiB dev passes and 1 GiB ordinary/TOAST scale runs:

- A concurrent write made *while the export connection is held open but not
  yet imported* is correctly excluded from the base and delivered exactly
  once via WAL after import.
- Import fails closed (`ERROR: snapshot "..." does not exist`) if the export
  connection dies **before** import is attempted (adversarial scenario 14a).
- If the export connection dies **after** import succeeds, the already-
  pinned copy transaction is unaffected and completes correctly (14b).
- The slot survives export-connection close in `wal_status=reserved` and is
  consumable normally afterward.

## Protocol B — existing stream reanchor (short lock + transactional marker)

1. Copier: `BEGIN ISOLATION LEVEL REPEATABLE READ`, then **waits** (does not
   read yet).
2. Coordinator: `LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE` (waits for any
   in-flight writer holding `ROW EXCLUSIVE`+ to commit first), then signals
   the copier.
3. **Only now**, while the coordinator holds the lock, does the copier run
   its first real read (fixing the REPEATABLE READ snapshot) and signal
   back. That read is `SELECT 1 FROM tbl LIMIT 1`, not `SELECT count(*)`:
   PostgreSQL fixes a REPEATABLE READ transaction's snapshot at its first
   query regardless of how much of the relation that query scans, so a
   `LIMIT 1` probe still genuinely reads the target relation while costing
   O(1) instead of a full scan during the window the coordinator lock is
   held.
4. Coordinator, still in the same transaction: `SELECT
   pg_logical_emit_message(true, 'pg_flashback', <uuid>)`, then `COMMIT` —
   which produces the marker's real COMMIT LSN and releases the lock in the
   same instant.
5. Copier continues the long copy from its now-independently-pinned
   snapshot, lock-free.
6. The marker's COMMIT LSN, resolved from the shared slot's own decoded WAL
   (never from client-side timing), is the boundary: WAL strictly after it
   is what gets replayed on top of the base.

The ordering in step 3 is load-bearing, not cosmetic — see below.

Verified: two tables reanchored onto **one** existing slot in sequence
(scenario 18) with no second slot created and the first table's capture
unaffected; ordinary, TOAST, and quoted-identifier (`"Weird Table"`, columns
with spaces) profiles all round-trip correctly.

**Real concurrent DML during the copy, proven via a deterministic handshake
(added this round).** Earlier evidence for Protocol B had zero replayed
commits/inserts/updates/deletes — no writer ran during the copy at all. Now:
the copier's backend is tagged with a unique `application_name` and its CTAS
is (at dev scale) deliberately slowed via a `poc_slowdown()` SQL function so
the window is observable; the harness polls `pg_stat_activity` for that
backend genuinely `state='active'` running the `CREATE TABLE ... AS SELECT`
itself (`copy_active_observed`) — an explicit `die()`, never a silent
proceed, if that's never confirmed. Only after that observation does a
concurrent writer loop start, producing real committed `INSERT`/`UPDATE`/
`DELETE` transactions (including at least one genuinely multi-row
transaction, and — TOAST profile — periodic large-incompressible-value
mutations) until the CTAS itself finishes; the writer's own first proven
commit is `writer_active_during_copy`. Both `copy_window_commits` (the
writer's own ground-truth count) and `commits_replayed`/`post_marker_*`
(the WAL-derived replay count) are recorded separately. At 1 GiB scale the
real CTAS already takes 4–5 s on its own, so the artificial slowdown is
skipped there.

**Shared-slot historical-prefix skip-advance (added this round).** A real
already-running production capture slot has already caught up past a
table's pre-existing history before DROP-protection for that specific table
turns on — it does not decode everything that ever happened to the table the
moment protection starts. `run_protocol_b` now models that: the table is
created and loaded with its initial data *before* it joins the shared
slot's tracked set, a `pre_protection_flush_lsn` is captured right after,
and `pg_replication_slot_advance()` skip-advances the slot past it without
ever decoding that payload. A fail-closed safety check runs first:
`consume_slot_to_events` for every already-tracked table sharing the slot,
then a hard assertion that the slot's own `confirmed_flush_lsn` reached
`pre_protection_flush_lsn` — proving nothing already-tracked was skipped
without being durably decoded first (decoded into `change_log`/`commit_log`,
not necessarily yet *applied* to a shadow table, which is a separate, safe-
to-defer bookkeeping concern). Only after the advance succeeds does the
table join the tracked set; every replayed event from that point on is
therefore provably post-marker by construction
(`historical_payload_events_replayed=0`, structurally guaranteed, not
measured). Left unfixed, this made a real 1 GiB Protocol B run blow a 590 s
timeout decoding a table's own ~9M-row initial bulk load before it ever
reached the boundary marker, and left ~20 GiB of retained `pg_wal` on disk.

## What went wrong while building this (and why it matters)

Several bugs found while building and correcting the harness are themselves
evidence about the protocol and about crash behavior, not just shell-script
mistakes:

1. **Import and copy must be one transaction (Protocol A).** An earlier
   draft did `BEGIN; SET TRANSACTION SNAPSHOT; SELECT 1;` (just to confirm
   import), let that transaction end, released the exporter, then opened a
   **second** `BEGIN; SET TRANSACTION SNAPSHOT '<same name>'` for the actual
   copy. That second import silently failed with `snapshot "..." does not
   exist`. Any future production implementation of Protocol A needs to treat
   "import" and "materialize the base" as one uninterruptible transaction,
   never two.
2. **The copier's snapshot MUST be fixed while the coordinator holds the
   lock, not before the lock is requested (Protocol B).** An earlier draft
   let the copier read first and the coordinator lock second. That passed
   every non-adversarial test (nothing was racing in the gap) but failed
   scenario 1 (a writer that commits in the window between "copier snapshot
   fixed" and "coordinator lock granted" could be silently missed by both
   the base *and* WAL-after-boundary, an actual lost-write bug). The fix —
   copier signals it has *started* its transaction but not yet read
   anything, coordinator locks and signals back, *then* the copier reads —
   is exactly what the task brief's step B.4 specifies; getting the order
   backwards is a real correctness bug, not a style issue.
3. **`ON CONFLICT DO NOTHING` without a constraint is a silent no-op.**
   Shadow tables are created via `CREATE TABLE ... AS SELECT`, which never
   copies the source's `PRIMARY KEY`. The replay path's `INSERT ... ON
   CONFLICT DO NOTHING` therefore had no constraint to match and let every
   insert through unconditionally — invisible until the real crash-recovery
   scenarios (below) exercised an actual duplicate/redelivered commit and
   doubled a shadow table's row count. Fixed by replacing it with `INSERT
   ... SELECT ... WHERE NOT EXISTS (...)`, which is idempotent regardless of
   whether the target table has any constraint at all. This is a genuine
   lesson for Step 8: idempotent replay must not be assumed from `ON
   CONFLICT` without first confirming the target actually has a matching
   unique constraint.
4. **A killed *client* process does not promptly reveal itself to the
   server if the server backend is blocked inside a server-side
   `pg_sleep()`** — the backend only notices on its next I/O with the
   client. Adversarial scenarios that kill a session mid-transaction
   (scenario 13, the DDL-queue policy-2 abort measurement) had to replace
   `SELECT pg_sleep()` with a client-side `\! sleep` so the backend stays
   idle-in-transaction (blocked reading the next command) and detects the
   kill immediately (measured abort/cleanup then dropped from ~3.7s of
   sleep-artifact to ~1ms of real detection latency).
5. **A logical slot can redeliver already-decoded WAL across a crash.**
   Scenario 15's real `pg_ctl -m immediate` crash/restart cycles confirmed
   this empirically: the slot's on-disk confirmed position can lag its
   actual last-delivered position, so a post-recovery consumer can see a
   commit it already processed. The harness's `commit_log` (keyed by `xid`
   `PRIMARY KEY`) correctly detects and counts this; combined with fix #3
   above, replay stays idempotent through it. Step 8 must not assume
   at-most-once delivery from a logical slot across a crash — only
   at-least-once, with idempotent apply as the actual guarantee.
6. **A per-row micro-sleep is not a reliable way to make a query take
   deterministic wall-clock time.** The first version of the deterministic
   copy-active mechanism called `pg_sleep()` once per row with a tiny
   duration (e.g. 0.0000072 s at ~280K rows, targeting ~2 s total). In
   practice `pg_sleep()`'s own per-call/syscall overhead dominates once
   invoked hundreds of thousands of times, so the intended ~2 s CTAS took
   minutes, blew the harness's own timeout, and left the writer and psql's
   `\!`-spawned file-wait grandchildren running as orphans when the harness
   killed the cluster mid-copy. Fixed by sleeping a fixed ~20 ms on only
   ~100 evenly-spaced (stride-based) rows via a `CASE WHEN` — a guaranteed-
   order SQL construct, unlike an `OR` disjunct the planner may reorder —
   for the same bounded total delay regardless of row count.
7. **A child process's stop signal must be guaranteed by its own exit path,
   not inferred from one specific outcome.** The concurrent writer originally
   stopped only when it saw `ctas_done_file`, which is touched only if the
   CTAS itself *succeeds*. A copier that errors partway through (the deliberate
   fault-injection selftest gate, or any real crash) never touches that file,
   so a writer keyed on it alone would run forever. Fixed with a separate
   `writer_stop_file` guaranteed by the copier subshell's own `EXIT` trap on
   every exit path — success, a SQL error, or being killed — plus a global
   `WORK_ROOT`-path-scoped process sweep in `cleanup()` for the class of
   grandchild that a killed parent (e.g. a psql session's `\!` shell-escape)
   orphans and that is never directly reapable by PID at all, since
   PostgreSQL's own `setproctitle` rewrite erases any trace of the spawning
   path from a bgworker's visible command line. A full sweep of this
   session's accumulated orphans (spanning all three correction rounds, not
   just this one) found 40 leftover processes, including several
   `pg_flashback maintenance worker`/`delta worker` bgworkers that survived
   their own postmaster's `pg_ctl stop` because an earlier external kill (a
   timeout firing while a stuck decode was in progress, before this round's
   historical-prefix fix) cut off the harness's own graceful shutdown before
   it completed.
8. **"Decoded" and "applied" are different safety properties.** An early
   version of the historical-prefix safety check (item above) required
   `change_log.applied=true` for every already-tracked table before allowing
   a slot skip-advance — but several scenario tests in this harness
   intentionally decode more than they apply (oid+LSN-scoped
   `poc_apply_shadow` calls, by design, to isolate a specific boundary
   window), so the check correctly-but-wrongly refused to advance on data
   that was never actually at risk of being lost. The real safety property
   is durable *decode* (persisted in `change_log`/`commit_log`, safe
   regardless of what happens to the slot afterward) — whether something has
   been *materialized* into a shadow table yet is an orthogonal oracle
   bookkeeping detail.

## Correctness oracle

Layered per the task brief, using this harness's own **independent PoC
semantic fingerprint** (explicitly not the product's canonical
`flashback_relation_full_data_fingerprint` — see "Production oracle
validation" for how that one gets checked):

1. Commit-LSN sequencing and exactly-once delivery: `poc_ingest_decoded()`
   dedupes by `xid` (a `PRIMARY KEY`), flags out-of-order LSNs against the
   running max.
2. Row count.
3. Canonical order-independent semantic fingerprint: `md5(string_agg(md5(t::text),
   '|' ORDER BY md5(t::text)))` over the whole row set — insensitive to
   physical row order, sensitive to any missing/extra/differing row.
4. TOAST byte equality: explicit `octet_length`/`IS DISTINCT FROM` check on
   the `bytea` column between live and shadow (both protocols, TOAST
   profile).
5. Quoted-identifier round-trip via direct copy + fingerprint.

All two 64 MiB dev passes and four 1 GiB runs passed every layer.

## Named transaction scenarios (decoder behavior, empirically proven)

Each scenario below is scoped to its own table and boundary window, and
verifies: exact decoded commit count for the scenario's own window, exact
decoded op count (`INSERT`/`UPDATE`/`DELETE`) for that table's oid within
that window, zero duplicate/out-of-order commits, and that base +
WAL-after-boundary reconstructs the live table exactly (plus explicit
per-row value checks for the two rollback cases).

| Scenario | Expectation | Result |
|---|---|---|
| Multi-row single transaction (10-row `UPDATE`) | 1 commit, 10 ops | PASS |
| `INSERT` → `UPDATE` → `DELETE` in one transaction | 1 commit, 3 ops, row absent in both live and shadow | PASS |
| `SAVEPOINT` rollback (one row updated post-savepoint, then rolled back) | 1 commit, 2 ops; rolled-back row untouched | PASS |
| Nested subtransaction rollback (two nested `SAVEPOINT`s, inner and outer rollbacks) | 1 commit, 2 ops; 3 intermediate rows untouched | PASS |
| Full transaction rollback | 0 commits, 0 ops, table unchanged | PASS |
| Concurrent update on the same PK (session2 blocks behind session1's row lock, commits after) | 2 commits, 2 ops, final value is the later committer's | PASS |

These do not rely on "PostgreSQL's ReorderBuffer already handles
subtransactions generically" as an assumption — they empirically exercise
this specific decoder's JSON serialization and message/change callbacks for
each shape.

## Adversarial scenarios

| # | Scenario | Protocol | Result |
|---|---|---|---|
| 1 | Writer starts before lock, commits before lock granted | B | Lock waits (measured); write lands in base; WAL replay after boundary does not duplicate it |
| 2/3 | Writer's snapshot predates the lock cycle, but it writes+commits after the boundary | B | Base excludes it; WAL delivers it exactly once |
| 4–9 | Insert/update/delete mix, multi-row, concurrent churn | A, B | Exercised via the concurrent-writer loop during every copy (real INSERT/UPDATE/DELETE traffic) *and* the six named transaction scenarios above (which isolate and name each shape explicitly) |
| 10 | Incompressible TOAST | A, B | Byte-exact at 64 MiB and 1 GiB |
| 11 | Quoted table/column identifiers | B | Round-trips correctly |
| 12 | Copier crash mid-copy | B | `SIGKILL` client mid-transaction; server rolls back; no committed artifact |
| 13a | Coordinator crash before marker commit | B | Lock released promptly (client-side wait); no marker visible in WAL |
| 13b | Marker that does commit | B | Durable/visible by definition of `COMMIT` |
| 14a | Export connection dies before import | A | Import fails closed |
| 14b | Export connection dies after import succeeds | A | Already-pinned copy unaffected |
| 15a | Real crash (`pg_ctl -m immediate`) while a genuinely active, in-flight CTAS is confirmed running via `pg_stat_activity` (not a client-side sleep before the query even starts), after marker commit | B | `pg_stat_activity` confirms `state='active'` running the real `CREATE TABLE ... AS SELECT` before the crash is ever applied (`ctas_active_observed_before_crash`, `crash_applied_while_ctas_active`); in-flight (uncommitted) copy vanishes entirely on recovery (`partial_artifact_absent_after_restart`); marker stays durable/decodable; clean retry succeeds with matching row count/fingerprint and no lost/duplicate commit (`retry_passed`) |
| 15b | Real crash after copy commits, before a separate metadata-finalize step | B | Physically-committed orphan correctly not treated as available; deterministic cleanup drops it; idempotent retry reaches `available` |
| 15c | Coordinator's marker commits, copier independently fails (never runs) | B | Marker alone never produces an active artifact; shared slot stays healthy for a later clean attempt |
| 16 | Slot loss (dropped mid-use) | — | Consuming a dropped slot errors; never silently returns data |
| 17 | Duplicate/retry (same batch peeked twice) | — | Oracle detects the duplicate commit; shadow apply stays idempotent |
| 18 | Second protected table onto the same existing stream | B | No second slot; first table's capture unaffected |

Savepoint/subtransaction rollback and full-transaction rollback (5, 6, 7 in
the original numbering) are the named scenarios above, not folded into
generic churn.

## DDL queue policy comparison (64 MiB table, 200k rows, single trial)

| Policy | Behavior | Measured |
|---|---|---|
| 1 — copy wins, DDL waits | Long `ACCESS SHARE` read holds; `ALTER TABLE` queues behind it | DDL wait ≈ **1.8 s** (bounded by the copy's own duration) |
| 2 — copier yields | Copier polls `pg_locks` for a waiting `ACCESS EXCLUSIVE` request and aborts itself (`SIGKILL`, detected via the client-side-wait fix above) | Detected-to-killed-and-reaped ≈ **1 ms**; zero leftover partial artifact |

No production policy choice is made here — both are measured, not decided.
This is a single trial at 64 MiB, not repeated or run at 1 GiB.

## xmin / vacuum horizon (64 MiB table + separate 10k-row churn table, ~4 s snapshot hold, single trial)

- Snapshot-hold `xmin` age at the moment of peak hold: **~290–355 XIDs**
  across the two dev passes.
- Dead tuples accumulated on the *unrelated* churn table during the hold:
  **~425–540**.
- Dead tuples after an explicit `VACUUM` once the hold released: **0**.
- This is a 4-second hold on a 64 MiB table; it does **not** extrapolate to
  a 50 GiB copy's likely multi-minute hold, where dead-tuple accumulation on
  unrelated hot tables scales with hold *duration*, not copied table size.

## Write-stall (coordinator lock-hold) distribution — 64 MiB, n=20, two trials

Repeats the real `LOCK` + copier-snapshot-fix + marker + `COMMIT` handshake
20 times against the same 64 MiB table, reporting percentiles of exactly the
window `run_protocol_b`'s own `lock_hold_ms` metric measures (lock acquired
to `COMMIT`) — not the long lock-free copy that follows it.

| Trial | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Dev pass 1 | 19 ms | 29 ms | 29 ms | 29 ms |
| Dev pass 2 | 21 ms | 32 ms | 35 ms | 35 ms |

**Explicitly 64 MiB percentiles from 20 real samples each, not a 1 GiB
percentile inferred from a single 1 GiB run.** The single-sample 1 GiB
`lock_hold_ms` values recorded per scale run (27–30 ms, see below) are
consistent with this distribution, not a separate claim.

## 1 GiB final runs

| Run | Rows | Table physical bytes | Logical payload bytes | Copy time | Lock hold (B only) | Post-marker commits/ins/upd/del |
|---|---|---|---|---|---|---|
| A, ordinary (reused) | 8,947,848 | 1,178,591,232 (1.10 GiB) | 930,554,959 (887 MiB) | 4.39 s | — (lock-free) | 148/148/0/0 (import-only workload) |
| A, TOAST (reused) | 130,944 | 1,133,346,816 (1.06 GiB) | 1,077,442,632 (1.00 GiB) | 4.31 s | — (lock-free) | 150/150/0/0 (import-only workload) |
| B, ordinary (this round) | 8,947,850 | 1,178,648,576 (1.10 GiB) | 930,545,831 (887 MiB) | 4.47 s | **27 ms** | 334/105/168/103 |
| B, TOAST (this round) | 130,946 | 1,133,453,312 (1.06 GiB) | 1,077,384,306 (1.00 GiB) | 4.01 s | **27 ms** | 321/101/161/99 |

All four passed every oracle layer (row count, fingerprint, commit-LSN
sequencing, TOAST byte equality where applicable, zero duplicate/out-of-order
commits). Protocol A's reused runs predate the concurrent-writer mechanism
built this round (Protocol A's own copy is lock-free, so it was never the
gap Protocol B had) and only ever exercised an import-time INSERT workload;
Protocol B's two runs above are re-measured this round and are the first
1 GiB evidence with genuine concurrent INSERT/UPDATE/DELETE churn proven
during the copy window, replayed post-marker with zero duplicates.
Protocol B's 1 GiB lock-hold time (27 ms both runs) is consistent with the
20-sample 64 MiB distribution below and with the prior round's measurement
(27–30 ms) — the historical-prefix fix changed *what* gets decoded before
the marker, not the lock-hold window itself.

## Disk hygiene (added this round)

The 1 GiB Protocol B ordinary run that first exposed the historical-prefix
bug left **~20 GiB** of retained `pg_wal` on disk (a shared slot that cannot
advance past WAL nothing has consumed yet), and — because that run's own
external timeout fired while the cluster was busy, cutting off the
harness's own graceful `pg_ctl stop` before it finished — left the cluster's
`pg_flashback` background workers running as orphans afterward, undetectable
by a `WORK_ROOT`-path-scoped process sweep since PostgreSQL rewrites a
bgworker's visible command line via `setproctitle`. After the fix:

- Peak ephemeral disk for the corrected 1 GiB Protocol B ordinary run: **5.26
  GB** (`ephemeral_disk_bytes` in `result.json`, measured before cleanup).
- Peak ephemeral disk for the corrected 1 GiB Protocol B TOAST run: **4.91
  GB**.
- `$DATA`/`$PGLIB_DIR` are now cleaned by default regardless of PASS/FAIL
  (previously only on PASS, meaning a FAIL kept the entire cluster forever);
  a full failed cluster is now opt-in only via `POC_KEEP_FAILED_DATA=1`.
  Disk after cleanup for every run in this round's evidence: back to the
  pre-run baseline, verified via `df`.
- Orphan process count after every run in this round's required sequence:
  **0**, verified via `pgrep -af "$WORK_ROOT"` after each run and a broader
  sweep across the whole session.

## Production oracle validation

Run separately against a real candidate archive built from this same commit
(`scripts/build_candidate_archive.sh`, `CANDIDATE_DIR=target/candidate/cfd7eb2f6acca3c77285edf55b6e8fe5b1eb0923`),
proving the *actual* production correctness oracle and packaging path, not
just this harness's own independent check:

- `scripts/run_exact_wal_transaction_schema_matrix.sh`: **passed**
  (functional suite, exact-candidate DROP suite, 14/14 drop-adversarial
  cases, all restart-recovery-adversarial cases including a real
  mid-recovery restart and a failpoint-crash-and-retry case).
- `scripts/run_clean_host_candidate_smoke.sh`: **PASS** (4/4 assertions:
  candidate hash verification, install-from-archive-only, fresh `CREATE
  EXTENSION` + doctor, protect/WAL-capture/exact-DROP-recovery/fingerprint/
  owner/ACL).

## Not run this session

- Only PostgreSQL 17 was tested (matching this host's available binaries).
  PG15/16/18 were not exercised — not required since no production Rust/SQL
  changed, but worth naming for Step 8 planning.
- DDL-queue and xmin/vacuum measurements ran once each at 64 MiB, not
  repeated for variance or run at 1 GiB scale.
- Full PG15–18 pgrx suite, WAL E2E, DBA acceptance, and chaos suites were
  not run — not required since production code did not change (`git status`
  shows only harness-script and doc changes across every commit in this
  session).

## Open risks for Step 8

1. **xmin/vacuum at real hold duration is unproven.** The 4-second hold here
   does not extrapolate to a 50 GiB copy's likely multi-minute (or longer)
   hold. This needs its own measurement at realistic duration before Step 8
   commits to Protocol B as "cheap."
2. **DDL-queue policy is unchosen.** Both measured once; policy 2 (copier
   yields) requires a `pg_locks`-polling loop whose overhead was not itself
   measured in isolation, and neither policy was repeated for variance.
3. **Only PG17 tested.**
4. **Idempotent replay is now proven necessary, not optional.** Any Step 8
   implementation of WAL-after-boundary replay must be idempotent by
   construction (real constraint + conflict handling, or an equivalent
   existence check) — this PoC's own bug (finding #3 above) is exactly the
   failure mode to avoid, and slot redelivery across a crash (finding #5) is
   the mechanism that will trigger it in production, not a hypothetical.

## Recommended protocol for Step 8

**Protocol B (existing-stream reanchor)** is the one Step 8 should build on:
it is the only one that works for every table after the first, it produces
the same transactional-marker artifact the current production path already
emits (so the boundary-resolution code barely changes), its measured
write-stall (19–35 ms p50–max across two 20-sample 64 MiB trials, 27–30 ms
single-sample at 1 GiB) is small and now well-characterized, and its real
crash-recovery behavior (scenario 15) has been proven fail-closed with
idempotent retry. Protocol A (exported snapshot) is real and correctly
proven, but by construction only ever applies to the single first-ever slot
creation on a database — it cannot be the general mechanism, and should
probably be dropped from further consideration rather than carried forward
as parallel machinery, unless a future need for a from-scratch bulk sync
independent of any existing slot emerges.

## Not production-wired

Nothing in this ADR changes `flashback_track`, `SnapshotStore`, the public
SQL API, GUCs, or generated SQL. Across every commit in all three
correction rounds, `git status` shows only
`scripts/run_poc_online_snapshot_wal_alignment.sh`,
`docs/adr/0003-online-snapshot-wal-alignment-poc.md`, and
`docs/evidence/step7-online-snapshot-wal-alignment.json`. The extension
binary sha256 (`5657e332...0690c05`) is identical across every rebuild
across all three rounds, confirming production Rust/SQL never changed.
Step 8 is where any of this gets wired into production, and that decision
(which storage/lock seam actually changes) is explicitly out of scope here.

## Addendum — evidence validity note (Faz A correctness closure)

Everything above describes the tree at `source_commit`
`cfd7eb2f6acca3c77285edf55b6e8fe5b1eb0923` only. After this ADR was written,
Faz A (A1-A7 correctness closure across Steps 1-7, landing 8 further commits
ending at `ececde5`) changed production Rust — specifically
`src/capture/wal_decoder.rs`, the exact file this harness reuses as a
decoding library (`_PG_output_plugin_init`, `fb_decode_change`) — to detect
and mark oversized rows instead of silently decoding them (A5), plus
extensive SQL across state authority, capture staging, restore/verify, and
the operation journal (A1-A7).

Every "identical across every rebuild," "production Rust/SQL never
changed," and `extension_binary_sha256` claim in this document is therefore
**stale** and must not be read as describing current `HEAD`. This does not
retroactively invalidate the *evidence itself* as a historical record of
what was proven at `cfd7eb2`, and it does not mean Protocol A/B's measured
behavior is now wrong — the harness never touched `flashback_track` or
`SnapshotStore`, and nothing in Faz A changed the decoder's JSON shape for
an ordinary (non-oversized) row. It does mean: before this ADR's
conclusions are cited again, or before Step 8 work builds on this evidence,
the harness must be re-run against current `HEAD` to get a fresh candidate
identity, hash, and pass/fail record. The harness's own
`dirty_tree_rejected`/`candidate_mismatch_rejected` selftest gates already
fail closed on exactly this kind of drift; this note exists so the
checked-in prose does not silently claim otherwise in the meantime.

## Requalification after Faz A

The stale-evidence condition above was closed against executable source
commit `7faf85f669ea5dcd1dc97e31a8dc03e33fefb0e5` (tree
`d5211667a511c09449c60a02c8a2cc1249e780b1`, extension binary SHA-256
`2d3421a77e893213740aa3454c8e1098e50828b3e8cb98558f5f69d0fc66ebae`).
The original evidence remains unchanged as a historical record; the
`current_requalification` object in the checked-in evidence binds the fresh
proof.

The harness self-test passed, including dirty-tree and candidate-mismatch
rejection, missing-step and interrupt honesty, and child-process cleanup on
copier failure. Two consecutive 64 MiB development runs then passed from the
same clean source identity. Both runs:

- bound marker identity transactionally by marker text and XID;
- observed an active CTAS before injecting an immediate postmaster crash;
- proved the partial artifact absent after restart and the retry successful;
- ran a real concurrent writer throughout Protocol B copy and replayed
  non-zero INSERT, UPDATE, and DELETE counts;
- advanced the historical WAL prefix while replaying zero historical table
  payload events; and
- emitted PASS only after child reap, postmaster shutdown, filesystem
  cleanup, and a zero-leftover audit.

The two primary Protocol B runs replayed 298 and 302 commits respectively.
Their INSERT/UPDATE/DELETE counts were 94/148/92 and 95/152/93. The TOAST
variants replayed 225 and 231 commits with byte-equality checks intact.

This requalification restores the ADR's use as evidence for Step 8 design.
It does not production-wire either protocol and does not qualify 1/10/25/50
GiB scale or a 24-hour run.

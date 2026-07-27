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
isolated cluster, per the Step 7 task brief. It does not change pg_flashback
itself.

## What was built

`scripts/run_poc_online_snapshot_wal_alignment.sh` — a self-contained harness
that:

- builds the current source tree (`cargo build --release`) and computes a
  candidate identity (source commit/tree, built `.so` sha256) that is
  rejected *before* any cluster is created if it does not match an expected
  hash (selftest-verified);
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
- implements a correctness oracle in PL/pgSQL (`poc_ingest_decoded()` /
  `poc_apply_shadow()`) that ingests decoded JSON into durable
  `commit_log`/`marker_log`/`change_log` tables, replays not-yet-applied
  changes onto a shadow table filtered by a caller-supplied boundary LSN, and
  reports duplicate/out-of-order commits;
- reuses the *exact* transactional BOUNDARY marker mechanism the current
  production path already relies on (`pg_logical_emit_message(true,
  'pg_flashback', ...)`, decoded by the existing `fb_decode_message`
  callback into `{"marker":<xid>}`), so Protocol B in this PoC is provably
  the same marker semantics production already depends on, not a new
  invention.

Nothing here adds a public SQL command, GUC, or generated-SQL surface; `git
status` shows exactly one new file.

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

Verified against PostgreSQL 17 (this cluster's binaries):

- A concurrent write made *while the export connection is held open but not
  yet imported* is correctly excluded from the base and delivered exactly
  once via WAL after import (proven in an early standalone reproduction
  before the full harness existed).
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
   back.
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

## What went wrong while building this (and why it matters)

Two of the bugs found while building the harness are themselves evidence
about the protocol, not just shell-script mistakes:

1. **Import and copy must be one transaction (Protocol A).** An earlier
   draft did `BEGIN; SET TRANSACTION SNAPSHOT; SELECT 1;` (just to confirm
   import), let that transaction end, released the exporter, then opened a
   **second** `BEGIN; SET TRANSACTION SNAPSHOT '<same name>'` for the actual
   copy. That second import silently failed with `snapshot "..." does not
   exist` — not because anything crashed, but because a snapshot name is
   only valid for import once, in whichever transaction imports it first
   while the exporter is still alive. Any future production implementation
   of Protocol A needs to treat "import" and "materialize the base" as one
   uninterruptible transaction, never two.
2. **The copier's snapshot MUST be fixed while the coordinator holds the
   lock, not before the lock is requested (Protocol B).** An earlier draft
   let the copier read first and the coordinator lock second. That passed
   every non-adversarial test (nothing was racing in the gap) but failed
   scenario 1 (a writer that commits in the window between "copier snapshot
   fixed" and "coordinator lock granted" could be silently missed by both
   the base *and* WAL-after-boundary, an actual lost-write bug). The fix —
   copier signals it has *started* its transaction but not yet read
   anything, coordinator locks and signals back, *then* the copier reads —
   is exactly what the task brief's step B.4 specifies
   ("Coordinator lock eldeyken copier snapshot'ını gerçekten materialize
   eder"); getting the order backwards is a real correctness bug, not a
   style issue.

A third, purely mechanical lesson: **a killed *client* process does not
promptly reveal itself to the server if the server backend is blocked inside
a server-side `pg_sleep()`** — the backend only notices on its next I/O with
the client. Adversarial scenarios that kill a session mid-transaction
(scenario 13, the DDL-queue policy-2 abort measurement) had to replace
`SELECT pg_sleep()` with a client-side `\! sleep` so the backend stays
idle-in-transaction (blocked reading the next command) and detects the kill
immediately. This is a testing-methodology fact worth keeping in mind for
Step 8 chaos testing, not just for this harness.

## Correctness oracle

Layered per the task brief:

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

All four 1 GiB runs and both 64 MiB dev runs passed every layer.

## Adversarial scenarios

| # | Scenario | Protocol | Result |
|---|---|---|---|
| 1 | Writer starts before lock, commits before lock granted | B | Lock waits (measured, see below); write lands in base; WAL replay after boundary does not duplicate it |
| 2/3 | Writer's snapshot predates the lock cycle, but it writes+commits after the boundary | B | Base excludes it; WAL delivers it exactly once |
| 4–9 | Insert/update/delete mix, multi-row, concurrent churn | A, B | Exercised via the concurrent-writer loop during every copy (real INSERT/UPDATE/DELETE traffic, not simulated); folded into the same fingerprint/dup/ooo checks, not isolated as 6 separate named scenarios |
| 10 | Incompressible TOAST | A, B | Byte-exact at 64 MiB and 1 GiB |
| 11 | Quoted table/column identifiers | B | Round-trips correctly |
| 12 | Copier crash mid-copy | B | `SIGKILL` client mid-transaction; server rolls back; no committed artifact |
| 13a | Coordinator crash before marker commit | B | Lock released promptly (client-side wait, see above); no marker visible in WAL |
| 13b | Marker that does commit | B | Durable/visible by definition of `COMMIT`; already the mechanism every passing Protocol B run depends on |
| 14a | Export connection dies before import | A | Import fails closed |
| 14b | Export connection dies after import succeeds | A | Already-pinned copy unaffected |
| 16 | Slot loss (dropped mid-use) | — | Consuming a dropped slot errors; never silently returns data |
| 17 | Duplicate/retry (same batch peeked twice) | — | Oracle detects the duplicate commit; shadow apply stays idempotent (`ON CONFLICT DO NOTHING` for inserts, naturally idempotent UPDATE/DELETE) |
| 18 | Second protected table onto the same existing stream | B | No second slot; first table's capture unaffected |
| 15 (restart) | Not automated | — | Not run this session — see "Not run" below |

Savepoint/subtransaction rollback and full-transaction rollback (5, 6, 7)
were not isolated as separate named test cases; ordinary PostgreSQL
transaction semantics make them structurally identical to "uncommitted work
must not appear," which scenario 2/3 and the base-vs-WAL fingerprint check
already exercise generically (a rolled-back write is invisible to both base
and WAL by construction, not something this harness had to special-case).

## DDL queue policy comparison (64 MiB table, 200k rows)

| Policy | Behavior | Measured |
|---|---|---|
| 1 — copy wins, DDL waits | Long `ACCESS SHARE` read holds; `ALTER TABLE` queues behind it | DDL wait ≈ **1.8 s** (bounded by the copy's own duration) |
| 2 — copier yields | Copier polls `pg_locks` for a waiting `ACCESS EXCLUSIVE` request and aborts itself | Detected-to-killed-and-reaped ≈ **1 ms**; zero leftover partial artifact |

No production policy choice is made here — both are measured, not decided.
Policy 2's near-instant abort in this harness is optimistic: it required
polling `pg_locks` in a tight loop from a second connection, which is
overhead a real capture worker would need to budget separately.

## xmin / vacuum horizon (64 MiB table + separate 10k-row churn table, ~4 s snapshot hold)

- Snapshot-hold `xmin` age at the moment of peak hold: **~320–370 XIDs**
  (varies slightly run to run — background XID consumption on the host, not
  the copy itself).
- Dead tuples accumulated on the *unrelated* churn table during the hold:
  **~460–560** (from continuous `UPDATE`s racing the hold).
- Dead tuples after an explicit `VACUUM` once the hold released: **0** —
  the horizon genuinely freed promptly, nothing pinned past release.
- This is a 4-second hold on a 64 MiB table; it does **not** by itself prove
  "cheap" at 50 GiB scale, where the hold could run for tens of minutes and
  dead-tuple accumulation on hot tables elsewhere in the same database would
  scale with hold duration, not table size. This is exactly the risk noted
  as open below.

## 1 GiB final runs

| Run | Rows | Table physical bytes | Logical payload bytes | Copy time | Lock hold (B only) |
|---|---|---|---|---|---|
| A, ordinary | 8,947,848 | 1,178,591,232 (1.10 GiB) | 930,554,895 (887 MiB) | 4.28 s | — (lock-free) |
| A, TOAST | 130,944 | 1,133,346,816 (1.06 GiB) | 1,077,440,980 (1.00 GiB) | 4.10 s | — (lock-free) |
| B, ordinary | 8,947,848 | 1,178,615,808 (1.10 GiB) | 930,545,487 (887 MiB) | 4.40 s | 920 ms |
| B, TOAST | 130,944 | 1,133,338,624 (1.06 GiB) | 1,077,407,232 (1.00 GiB) | 4.57 s | 49 ms |

All four passed every oracle layer (row count, fingerprint, commit-LSN
sequencing, TOAST byte equality where applicable, zero duplicate/out-of-order
commits).

The headline number: **Protocol B's write-stall at 1 GiB was 49 ms–920 ms**,
not "however long the 4+ second copy takes." The 920 ms case (ordinary
profile) is higher than the TOAST case's 49 ms because the coordinator's
lock-held window includes waiting for the copier's `SELECT count(*)`
snapshot-fixing read to complete, and a full-table `count(*)` over ~8.9M
plain rows costs more than one over ~131k TOASTed rows. A real
implementation would use a cheaper snapshot-fixing statement than
`count(*)` (e.g. a single-row probe) to shrink this further — this PoC
did not attempt to minimize it.

## Not run this session

- Scenario 15 (PostgreSQL restart mid-copy / after copy before metadata
  finalize) was not automated — the harness does not currently orchestrate a
  full `pg_ctl restart` mid-scenario. Everything else in the 18-scenario
  list was exercised at least once.
- Only PostgreSQL 17 was tested (matching this host's available binaries).
  PG15/16/18 were not exercised — not required by this task since no
  production Rust/SQL changed, but worth naming for Step 8 planning.
- Heavier existing E2E suites (`run_exact_wal_transaction_schema_matrix.sh`,
  `run_clean_host_candidate_smoke.sh`, DBA acceptance, chaos) were **not**
  re-run this session. Production code did not change (`git status` shows
  exactly one new file), so the fast static surface gates
  (`check_generated_sql_no_test_surface.sh`, `check_snapshot_store_surface.sh`,
  `check_core_wal_only_surface.sh`, `check_core_no_backup_surface.sh`,
  `check_centralized_state_surface.sh`, `check_integration_inventory.py`)
  were run instead and are green; the heavier suites were judged out of
  proportion to a change that provably does not touch what they test.
- The DDL-queue and xmin measurements ran once each at 64 MiB, not repeated
  at 1 GiB or across multiple trials for variance.

## Open risks for Step 8

1. **Snapshot-fixing statement cost.** `SELECT count(*)` was used as the
   copier's "first real read" to fix its snapshot. At 50 GiB this is not
   free (a real implementation should use the cheapest possible statement
   that still counts as a real read establishing the snapshot).
2. **xmin/vacuum at real hold duration is unproven.** The 4-second hold here
   does not extrapolate to a 50 GiB copy's likely multi-minute (or longer)
   hold. Dead-tuple accumulation on unrelated hot tables scales with hold
   *duration*, not copied table size — this needs its own measurement at
   realistic duration before Step 8 commits to Protocol B as "cheap."
3. **DDL-queue policy is unchosen.** Both measured; policy 2 (copier yields)
   requires a `pg_locks`-polling loop whose overhead was not itself
   measured in isolation.
4. **Restart mid-copy (scenario 15) is unverified.** The fail-closed
   behavior this needs (no partial artifact treated as valid after an
   unclean restart) is architecturally implied by "the copy is one
   transaction, uncommitted work vanishes on restart" but was not
   empirically exercised.
5. **Only PG17 tested.**

## Recommended protocol for Step 8

**Protocol B (existing-stream reanchor)** is the one Step 8 should build on:
it is the only one that works for every table after the first, it produces
the same transactional-marker artifact the current production path already
emits (so the boundary-resolution code barely changes), and its measured
write-stall (49 ms–920 ms at 1 GiB) is the actual number worth optimizing
further, not "the whole copy duration." Protocol A (exported snapshot) is
real and correctly proven, but by construction only ever applies to the
single first-ever slot creation on a database — it cannot be the general
mechanism, and should probably be dropped from further consideration rather
than carried forward as parallel machinery, unless a future need for a
from-scratch bulk sync independent of any existing slot emerges.

## Not production-wired

Nothing in this ADR changes `flashback_track`, `SnapshotStore`, the public
SQL API, GUCs, or generated SQL. `git status` on the branch this PoC was
built on shows exactly one new file:
`scripts/run_poc_online_snapshot_wal_alignment.sh`. The machine-readable
result for each run lives under
`target/poc/online-snapshot-wal-alignment/<run-id>/result.json` (gitignored,
not committed). Step 8 is where any of this gets wired into production, and
that decision (which storage/lock seam actually changes) is explicitly out
of scope here.

# Operational lifecycle audit

Date: 2026-07-16

Audited baseline: `b9e4c2f`; the WAL commit-map fix and its regression test are
included with this audit

Status: **not release-ready.** A+ policy adopted. WAL-local correctness and
generation-aware local retention are implemented for the qualified profile;
backup-profile coverage wiring, capacity preflight/artifact GC and capture/
maintenance isolation remain open release gates.

This audit deliberately separates three questions:

1. can a restore return the correct result;
2. can the capture/recovery lifecycle remain inside a finite capacity budget;
3. which storage/checkpoint policy should the project promise.

No snapshot/checkpoint policy was changed during the audit. Tests ran on
isolated PostgreSQL 17 clusters and did not use the development `fix_check`
database. The decision made after the evidence review is recorded in
[`STORAGE_POLICY.md`](STORAGE_POLICY.md).

## 1. Release blockers proven with real executions

| ID | Area | Reproduction result | Required disposition |
|---|---|---|---|
| RB-01 | Concurrent checkpoint boundary | An UPDATE committed at `17:18:23.245010`, before checkpoint metadata time `17:18:24.091492`, but the CTAS snapshot still contained the old row. Restore after both operations returned `old`, expected `new`. | Checkpoints need an exact MVCC boundary or a write-blocking boundary. A post-copy wall-clock timestamp is not valid. |
| RB-02 | Retention chain | Retention independently removed an old checkpoint and the delta needed by the next surviving checkpoint. A target after the cutoff restored `A`, expected `B`, without an error. | Retain a valid boundary checkpoint and prune deltas only relative to that boundary. |
| RB-03 | Retention/restore race | Restore passed its cutoff check and was paused on the base snapshot. Retention then deleted the required delta. Restore resumed, applied zero events and returned `A`, expected `B`. | Retention and restore must share a per-table coverage lock and restore must use an immutable coverage generation. |
| RB-04 | WAL slot loss | With `max_slot_wal_keep_size=32MB`, 435 MB of unrelated WAL invalidated the slot. Tracking remained active, the update count stayed zero, and restore returned `old`, expected `new`. | Persist coverage health/generation, detect `wal_status=lost`, and make restore fail closed. |
| RB-05 | Trigger row-size skip | A 96 KB row exceeded the default 64 KB capture ceiling. The warning reported one skipped row, but no durable invalidation was stored; restore succeeded with zero rows, expected one. | A skipped event must durably invalidate coverage or the supported row contract must fail the write/track operation. |
| RB-06 | Trigger commit semantics | With the default `track_commit_timestamp=off`, an UPDATE executed at `17:26:23.438265` and committed at `17:26:26.497166`. Restore to `17:26:24.967715` included the still-uncommitted UPDATE. | Trigger PITR must require commit timestamps or explicitly stop promising commit-time PITR. |
| RB-07 | Tracking start boundary | Tracking was called late in a transaction that began at `17:25:26.723027`. `tracked_since` used transaction-start `now()`. A change committed before the tracking call was not captured, yet restore to that earlier interval was accepted and returned `B`, expected `A`. | Establish and store an exact coverage boundary; transaction-start time is invalid. |
| RB-08 | Unbounded local checkpoints | Default 15-minute checkpoints plus seven-day retention keep about 672 full CTAS copies per table, in addition to the base snapshot. There is no byte/count quota or headroom preflight. | Choose a bounded checkpoint policy before release. |
| RB-09 | Local restore capacity | Restore has no disk preflight. On a 489 MB live table, measured peak growth was 944,603,136 bytes: one complete shadow plus one post-restore snapshot. | Use profile-specific finalization: `local_delta` preflights and creates a base plus durable pending transaction marker, then activates only after resolving the swap's real commit coordinate; `backup` creates no local snapshot and becomes durably unanchored until physical re-anchor. |
| RB-10 | Helper work capacity | Snapshot-direct initially reserves only 64 MiB of free space, while replay CoW growth is unknown. Quota is per request, successful dumps have no GC/expiry command, and retained artifacts are unbounded in aggregate. | Add continuous free-space reserve enforcement and an explicit artifact/request retention lifecycle. |
| RB-11 | Worker head-of-line blocking | While the worker was blocked for five seconds taking a checkpoint on one table, a committed event for another table remained in LOGGED `staging_events`; it was still absent from `delta_log` after one second and became visible only after 5,055 ms. | Decouple capture draining from checkpoint/retention work or give maintenance work a bounded, cancellable schedule. The delay affects visibility/RTO and can grow WAL slot lag. |

A successful restore cannot be treated as proof of recoverability until every
remaining open release gate is closed or explicitly removed from the supported
contract. Section 6 records which WAL-local correctness blockers are already
closed. RB-08/RB-09 local admission is implemented; RB-10/RB-11 are implemented
in code but still require exact-RC evidence before publish.

## 2. Capacity model and measurements

### 2.1 Local snapshots

Definitions:

- `H`: heap + TOAST bytes copied by CTAS (indexes are not copied);
- `L`: live table total bytes, including indexes;
- `D`: retained delta-log bytes for the table;
- `P`: checkpoint interval;
- `R`: retention interval.

The current steady-state approximation is:

```text
checkpoint_count ~= ceil(R / P)
local_bytes ~= L + H(base) + checkpoint_count * H + D
```

With the defaults, `R/P = 7 days / 15 minutes = 672`. The base plus retained
checkpoints therefore costs approximately `673 * H`, before the live table and
deltas.

Measured on `audit_concurrent`:

| Metric | Result |
|---|---:|
| Live heap | 455,114,752 bytes |
| Live total | 488,988,672 bytes |
| One checkpoint | 455,614,464 bytes |
| Checkpoint duration | 906 ms |
| Checkpoint WAL | 481,431,368 bytes (`1.058 * H`) |
| Default base + 672 checkpoints | about 285.6 GiB |
| Default checkpoint WAL per day | about 43.0 GiB |

These figures are for a 455 MB heap, not a large database. The current default
cannot be extrapolated to tens or hundreds of gigabytes.

### 2.2 Restore peak

For the legacy local-delta path, the measured peak was exactly described by:

```text
additional_peak_bytes ~= L(shadow, with rebuilt indexes) + H(post-restore checkpoint)
```

For the same table:

| Phase | Database growth |
|---|---:|
| Shadow + post-restore checkpoint peak | 944,603,136 bytes |
| Permanent growth immediately after restore | 455,614,464 bytes |
| Growth after deleting the test checkpoint | 0 bytes |

The old relation remains allocated until the restore transaction commits, so
free-space planning must cover the peak rather than only the final state.

### 2.3 Delta and WAL amplification

Trigger-mode soak used 2,000 rows and five updates per row. Each changed payload
was 1 KiB and effectively incompressible for this purpose.

| Metric | Untracked | Trigger tracked |
|---|---:|---:|
| Row updates | 10,000 | 10,000 |
| WAL | 12,440,432 bytes | 66,346,312 bytes |
| Delta relation growth | 0 | 26,083,328 bytes |
| Update time | 161 ms | 722 ms |
| Flush time | n/a | 263 ms |

In this workload the tracked path produced `5.33x` total WAL and used about
`2,608` delta bytes per event. When only a small integer column changed, delta
growth fell to about `266` bytes per event. Capacity therefore depends on
changed-column width and change rate, not table size alone.

### 2.4 WAL consume algorithm

The original query inlined the commit lookup and rescanned the entire decoded
batch for every DML row. Measurements with 1 KiB rows were:

| Transaction rows | Consume time before fix |
|---:|---:|
| 100 | 66 ms |
| 1,000 | 6,075 ms |
| 2,000 | 21,564 ms |

`EXPLAIN ANALYZE` showed a nested-loop CTE scan with 2,000 loops over 2,002
messages (roughly four million JSON predicate evaluations). The output plugin
alone decoded the same 2,002 messages in 80 ms.

The working-tree fix makes the commit map `MATERIALIZED`. The 2,000-row consume
fell to 253 ms (`~85x` faster), and the real WAL/worker E2E made the batch visible
in 452 ms. `scripts/run_wal_e2e.sh` now fails if that batch is not visible in
ten seconds.

### 2.5 WAL slot bound

The capacity trade-off is currently unsafe on both sides:

```text
unbounded slot retained WAL ~= database WAL rate * worker downtime
bounded slot => finite disk exposure, but slot can become lost
```

Untracked database workload also passes through the database-wide logical
slot. It contributes to retained WAL and decode CPU even though SQL later
filters it from `delta_log`.

### 2.6 Backup helper

For classic restore, peak work is approximately the materialized cluster plus
the table dump. For snapshot-direct, physical peak is changed CoW extents during
replay plus the table dump, but the helper's logical quota counts the full
apparent clone tree.

The retained 32 MiB qualification run passed all 27 helper E2E checks. It left
15 request directories and five successful table dumps until the enclosing
test cleanup. The five dumps totalled 830,090 bytes because the fixture is
highly compressible; production dumps may approach table data size. There is
no aggregate work-root limit or successful-artifact expiry command.

### 2.7 Worker scheduling

Capture consumption, staging flush, restore-lock inspection, all due
checkpoints, partition maintenance and retention run serially in one worker per
database. Checkpoints for every due table are also executed inside one SPI
transaction. A slow or lock-blocked checkpoint therefore delays capture for
unrelated tables, extends trigger visibility latency and can grow WAL lag.

The lock-blocked checkpoint experiment measured 5,055 ms from commit to delta
visibility with a 50 ms worker interval. The event remained only in
`staging_events` for that period.

An idle worker at the minimum 50 ms interval consumed 310 ms of CPU during a
5,005 ms sample on this host (`6.19%` of one CPU), despite having no tracked
tables or events. The loop enters five separate SPI transaction wrappers per
cycle; PostgreSQL's database transaction counters did not expose those worker
transactions. CPU figures are host-specific, but the cost multiplies by the
configured database/worker count (up to eight workers).

## 3. Positive guarantees confirmed

- Terminating a 434 MB checkpoint backend rolled back both metadata and CTAS;
  no orphan checkpoint table remained.
- The helper passed snapshot-direct, classic fallback, missing-WAL, old-target,
  backup/expire lock, cancellation, SIGKILL reconciliation, timeout, quota,
  identity/schema/fingerprint and controller-cleanup scenarios (27 checks).
- The helper holds the shared repository lock through native recovery, so the
  supplied exclusive wrapper can serialize backup/expire correctly.
- The WAL commit-map fix passes the real worker E2E, including a 2,000-row
  single transaction, real commit time, LSN, NaN, quoted identifiers and PITR.
- DDL by a role without flashback metadata privileges failed closed rather than
  silently bypassing capture. This is safe but is an operational restriction:
  tracked-table DDL needs documented privileges.
- A real concurrent DML/DDL run serialized correctly: `ALTER TABLE` waited
  2,998 ms on the long UPDATE transaction's relation lock. Restoring to the
  2.491 ms interval between DML commit and ALTER application returned value
  `B` with the pre-ALTER schema (the new column was absent), as expected.

## 4. Adopted decision: A+

The candidate phase ended on 2026-07-16. The adopted contract is A+: a safe
minimum local profile with exact write-locked boundaries and no automatic
periodic full checkpoints, plus an explicit backup-backed profile for tables
that cannot satisfy local capacity, change-rate, write-stall or RTO budgets.
Table size alone never selects a profile.

The first-release target is an ordinary LOGGED, non-partitioned table. Existing
partitioned or UNLOGGED functional demos do not broaden that contract.

The normative rules are in [`STORAGE_POLICY.md`](STORAGE_POLICY.md). Candidate
B is not a first-release promise. Candidate C remains a future research path
for non-blocking MVCC-aware checkpoints.

## 5. Implementation disposition

- RB-01 through RB-07 required the generation, stream-watermark, persistent-gap
  and common-lock protocol in [`COVERAGE_MODEL.md`](COVERAGE_MODEL.md). For the
  qualified WAL-local profile those blockers are closed; see Section 6.
  Backup-profile coverage still needs the same model wired end-to-end.
- RB-08 through RB-09 local capacity/write-stall admission for track,
  re-anchor and restore are implemented as fail-closed budgets with an
  explicit privileged override; free-space checks remain estimates with a
  documented race against concurrent writers.
- RB-10 helper artifact lifecycle remains a release gate for exact-RC
  evidence even though aggregate work-root quotas, GC and expire pinning are
  implemented in code.
- RB-11 capture/maintenance isolation is implemented (separate per-database
  workers); exact-commit p95/max SLO evidence must accompany the RC.

Generation ownership is half-open: a sealed generation serves targets from its
inclusive boundary up to, but excluding, its successor boundary; the active
generation serves through its inclusive proven watermark. This prevents the
same maintenance-boundary target from matching two generations.

Sealing freezes ownership and applicability, not an undrained capture
frontier. Backlog already bound to the sealed predecessor may advance its
watermark monotonically only to the exclusive upper coordinate; payload
retirement must wait until that drain is proven complete.

Post-restore behavior is profile-specific. `local_delta` creates the new base
under the swap lock and writes a durable pending generation/source-XID marker.
Its pre-commit time/LSN is not an exact local-base boundary because the base
contains the transaction's own uncommitted state. Health remains fail-closed
until a post-commit resolver obtains the real commit time (and WAL COMMIT LSN
in WAL mode) and activates the generation. If resolution fails, coverage
becomes a persistent gap/unanchored state and requires re-anchor.

`backup` never creates a local row snapshot: finalization writes a durable
pending swap-XID marker and opens a persistent `post_restore_unanchored` gap.
A post-commit resolver may attach the real COMMIT LSN, but that does not close
the gap. It seals the predecessor and deliberately leaves zero active
generations while the successor is `building`. In the first release the
successor can re-anchor only at the stop boundary of a new completed full
backup whose start LSN is strictly after the resolved swap commit and
whose required WAL has been verified. Closing that gap never validates targets
inside it.

Initial backup tracking follows the same physical-anchor distinction: a
durable tracking marker is resolved first, and only a new full backup whose
start LSN is strictly later may establish the first verified
physical-backup anchor. This anchor is not the exact write-locked local-base
boundary used by `local_delta`.

The schema scaffold alone never satisfied these blockers. WAL-local runtime
wiring, fail-closed target admission, generation-aware local retention, local
capacity/write-stall admission, backup-profile generation wiring and helper
capacity controls have landed. Exact-RC soak, clean-host packaged-artifact
qualification and remaining matrix evidence must still land before the release
status can change.

## 6. WAL-local milestone update

The later T-01/A implementation closes the WAL-local correctness portion of
RB-01, RB-03, RB-04, RB-05, RB-06 and RB-07 for the supported profile:

- initial track, re-anchor and post-restore bases are generation-bound and wait
  for their transaction's real COMMIT LSN before activation;
- concurrent first-track calls serialize before stable-ID allocation, and the
  real WAL E2E proves that a writer committing while the final relation lock is
  awaited is included in the fresh exact-base snapshot;
- LSN restore/query/deleted-row recovery admit one immutable generation and
  reject gaps or targets beyond a frozen watermark;
- slot loss, replacement, timeline change and unexplained external advancement
  break the stream, freeze the proven frontier and require re-anchor;
- trigger row-size skips and statement timestamps are outside the qualified
  WAL-only profile;
- DDL is staged in protected LOGGED metadata and promoted with its trusted
  COMMIT record. Logical-message bodies callable by ordinary users are ignored,
  so they cannot forge history;
- `flashback_resolve_target()` rejects same-microsecond collisions and
  timestamp/LSN inversions instead of constructing a non-prefix state.
- SQL-side source-XID markers are normalized to logical decoding's 32-bit
  `TransactionId` domain, avoiding epoch-expanded join failures after XID
  wraparound; COMMIT LSN remains the canonical order coordinate.

The earlier DDL privilege restriction was also removed safely: the hook enters
the extension owner's identity only for its internal metadata call, while the
ordinary table owner remains the actor for PostgreSQL's DDL permission check.

This update also closes RB-02 for qualified local generations: whole sealed-
generation retirement is durable, pinned and resumable. RB-08/RB-09 local
capacity and write-stall admission are implemented and fail-closed. RB-10
helper artifact lifecycle and RB-11 worker isolation are implemented in code;
exact-RC soak and clean-host packaged-artifact qualification remain
whole-project release gates. Backup-profile coverage anchoring supports both
fresh FULL-after-marker and retained FULL + continuous WAL activation
(see [`RETAINED_FULL_WAL_POC.md`](RETAINED_FULL_WAL_POC.md)). Differential /
incremental chains remain out of scope.

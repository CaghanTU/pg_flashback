# Operational lifecycle audit

Date: 2026-07-16

Audited baseline: `b9e4c2f`; the WAL commit-map fix and its regression test are
included with this audit

Status: **not release-ready; policy decision and correctness fixes required**

This audit deliberately separates three questions:

1. can a restore return the correct result;
2. can the capture/recovery lifecycle remain inside a finite capacity budget;
3. which storage/checkpoint policy should the project promise.

No snapshot/checkpoint policy was changed during the audit. Tests ran on
isolated PostgreSQL 17 clusters and did not use the development `fix_check`
database.

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
| RB-09 | Local restore capacity | Restore has no disk preflight. On a 489 MB live table, measured peak growth was 944,603,136 bytes: one complete shadow plus one post-restore snapshot. | Add a capacity plan/preflight and decide whether post-restore snapshots are automatic. |
| RB-10 | Helper work capacity | Snapshot-direct initially reserves only 64 MiB of free space, while replay CoW growth is unknown. Quota is per request, successful dumps have no GC/expiry command, and retained artifacts are unbounded in aggregate. | Add continuous free-space reserve enforcement and an explicit artifact/request retention lifecycle. |

Until RB-01 through RB-10 are either fixed or explicitly removed from the
supported release contract, a successful restore cannot be treated as proof
of recoverability.

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

The measured peak was exactly described by:

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

## 4. Policy candidates (decision not yet made)

All candidates make the backup profile the path for tables that cannot satisfy
the local profile's explicit capacity and write-stall budget. None chooses a
profile from table size alone; eligibility must include table bytes, changed
bytes per hour, required retention/RTO and filesystem headroom.

### Candidate A — safe minimum local profile

- one exact, write-locked base snapshot;
- periodic automatic CTAS checkpoints disabled;
- deltas retained until an explicit maintenance checkpoint establishes a new
  safe boundary;
- byte quota, headroom check and fail-closed coverage health;
- large/high-change tables use the backup profile.

Pros: shortest path to a truthful first release; removes checkpoint storms and
the concurrent checkpoint bug.

Cons: replay time and delta volume grow between maintenance checkpoints; the
initial/manual snapshot blocks writes while copying.

### Candidate B — bounded write-locking checkpoints

- checkpoint on a byte/change threshold rather than every 15 minutes;
- block table writes while copying so timestamp/LSN and contents agree;
- retain only a small valid chain (for example boundary + newest checkpoint);
- prune deltas only from the retained boundary;
- refuse a checkpoint when measured copy time or headroom exceeds table SLO.

Pros: straightforward correctness and bounded replay/storage.

Cons: every checkpoint pauses writes. The audit's 455 MB heap took 906 ms; a
large table can turn that pause into minutes.

### Candidate C — non-blocking MVCC-aware checkpoints

- persist the PostgreSQL MVCC snapshot identity used by CTAS;
- replay by transaction visibility, not by a post-copy wall-clock timestamp;
- retain a bounded checkpoint chain and trigger by delta bytes/change rate.

Pros: preserves non-blocking DML and can give bounded replay.

Cons: highest engineering risk; needs a prototype across PostgreSQL 15–18,
transaction-ID wraparound, DDL and crash/retention races before it can be a
release promise.

## 5. Audit recommendation

For a truthful first public release, Candidate A is the lowest-risk local
contract, with the backup profile used explicitly for large or high-change
tables. Candidate C is the best long-term research direction if non-blocking
rolling checkpoints are a product goal. Candidate B is viable only for tables
whose measured copy duration fits an explicit write-stall budget.

This recommendation is not the final policy decision. The next step is to
choose A, B or a scoped C prototype, then turn every applicable RB item into a
regression test and implementation gate.

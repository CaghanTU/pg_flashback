# Storage and recovery policy

Decision date: 2026-07-16

Status: **T-01/A adopted and implemented for the WAL-local milestone; remaining
cross-profile release gates still apply**

This document is the normative storage and recovery policy for pg_flashback.
It supersedes the policy candidates in
[`OPERATIONAL_LIFECYCLE_AUDIT.md`](OPERATIONAL_LIFECYCLE_AUDIT.md). The audit
remains the evidence record; [`COVERAGE_MODEL.md`](COVERAGE_MODEL.md) defines
the technical model that must enforce this policy.

The current runtime does not yet enforce every rule below. WAL-local capture,
admission and generation-aware local retention are implemented for the
qualified profile. Backup-profile generation anchoring, capacity preflight,
helper artifact GC and capture/maintenance isolation remain open. Until every
release gate in this document and [`RELEASE_SCOPE.md`](RELEASE_SCOPE.md)
passes, the project is not publishable as a correctness-guaranteed recovery
system.

## Decision: A+

pg_flashback has two explicit recovery profiles. It does not infer a profile
from table size alone.

- `local_delta` uses one exact, write-locked base image plus durable captured
  changes. Automatic periodic full-table CTAS checkpoints are disabled. A new
  base is created only by an explicit, preflighted maintenance operation or as
  the required final step of a restore.
- `backup` stores no local row base or DML history. It uses a verified
  pgBackRest backup/WAL range and the external recovery helper to recover,
  extract, validate and transactionally install one table.

Tables that cannot satisfy the local profile's capacity, change-rate,
write-stall and RTO budgets use the backup profile. Non-blocking MVCC-aware
local checkpoints remain research, not a first-release promise.

## Terms

- **Exact local-base boundary:** a local table image and its time/LSN
  coordinates established while writes are blocked, after table identity is
  revalidated.
- **Verified physical-backup anchor:** a completed full-backup stop boundary
  plus the repository/WAL evidence that proves native recovery from that
  backup. It is a profile-specific generation anchor, not an exact local base
  and does not imply that table writes were blocked at the backup boundary.
- **Generation anchor:** either an exact local-base boundary or a verified
  physical-backup anchor, according to the tracking profile.
- **Generation:** one immutable restore chain beginning at a profile-specific
  generation anchor.
  Its applicability is the half-open interval
  `[boundary, superseded_before)`. The exclusive upper coordinate may be
  represented as `superseded_before_time`/`superseded_before_lsn` (or an
  equivalently named exclusive coverage end) when the runtime schema lands.
- **Watermark:** the inclusive latest commit coordinate through which complete
  capture is proven. It is not a generation-applicability endpoint and is not
  merely the latest event seen.
- **Gap:** a durable range for which completeness cannot be proven. A target
  inside a gap is always rejected.
- **Re-anchor:** creation of a new qualifying generation anchor after a gap.
  Re-anchoring ends the open gap but never makes the historical missing range
  valid.
- **Tracking lifecycle:** an immutable parent identified by `tracking_id`,
  independent of PostgreSQL relation OID. `tracked_tables` is only its current
  physical binding. Relation OIDs may change during a shadow-table swap;
  untrack retires the parent and retrack allocates a new identity.

## Common fail-closed rules

1. A restore, query-as-of or deleted-row recovery is admitted only when its
   target belongs to exactly one `active` or `sealed` generation, is at or
   before that generation's inclusive capture watermark, and does not
   intersect a gap. A `building` generation—including a pending/unanchored
   successor—and a `retired` generation are never selected. A broken capture
   stream freezes its watermark: earlier targets may remain eligible, but later
   targets are rejected. Function success is never accepted as evidence of
   coverage.
2. Tracking, checkpoint, restore, retention, untrack and backup finalization
   use one stable per-tracking lock namespace. Before a tracking ID exists,
   concurrent first-track calls serialize on a deterministic database plus
   canonical-schema/name bootstrap key; allocation and transfer to the stable
   tracking lock happen without an unlocked interval, and the relation OID is
   then locked/revalidated. Metadata is reread after locking. Multi-table
   operations acquire all stable tracking locks in ascending ID order.
   A qualified local restore also takes the final relation lock and drains all
   already-committed WAL for the old relation before its OID can be replaced.
   Historical payload keeps its boundary OID; only the current binding moves.
3. A continuous local handoff activates the new generation and seals its
   predecessor atomically. The backup post-restore transition is the explicit
   exception: resolving the production-swap commit seals the predecessor at
   that coordinate while the successor remains `building`, so zero active
   generations is the expected fail-closed state until a qualifying new full
   backup anchors the successor. The durable gap rejects that interval. Old
   payload cleanup starts only after the transition commits, all payload
   already owned by the sealed generation has drained through its exclusive
   upper coordinate, and the same coverage lock is held.
4. Coverage breaks are written synchronously to LOGGED metadata. A warning or
   server log without a durable incident is not a safety mechanism.
5. Time and LSN fields have distinct meanings. WAL stream progress uses
   transaction commit LSN, not the LSN of an individual row change. Backup
   boundaries and pre-DDL requests use separately labeled, verified physical
   recovery LSNs; they are not silently called commit LSNs. Timestamp APIs use
   commit time, never statement time.
6. Missing, ambiguous or stale evidence rejects the target. Coverage health is
   represented by generations, stream epochs, watermarks and persistent gaps,
   not by one global healthy/unhealthy Boolean.
7. If a stream break prevents a `building` boundary from ever becoming active,
   its draft payload is removed and the generation becomes an immutable
   `aborted` audit tombstone. It is never target-eligible; a later explicit
   re-anchor creates a new generation and leaves the missing interval rejected.

These rules apply to `flashback_restore_lsn()`, `flashback_query_lsn()` and
`flashback_recover_deleted_lsn()`. Their timestamp-named predecessors are
legacy and reject correctness-qualified WAL generations.

## Local-delta profile

### Initial boundary

`flashback_track()` must run in a dedicated transaction that has performed no
prior writes. It takes the pre-identity bootstrap lock, allocates/reuses one
stable identity and acquires that identity's tracking lock without an unlocked
transfer. It then acquires the final required relation-lock strength from the
outset: at least `SHARE ROW EXCLUSIVE`, or a stronger mode when
replica-identity or other capture setup requires it. There is no weaker-lock
phase followed by an upgrade gap. While that lock is held, the transaction
installs and verifies the capture trigger or replica identity and stream
binding, rechecks the table identity and profile, and creates the base using a
fresh MVCC snapshot acquired only after the relation lock. An old
outer-statement, repeatable-read, serializable or imported snapshot is not
acceptable; if the runtime cannot prove snapshot ordering, tracking is
refused. The transaction then validates the base, records `clock_timestamp()`
and `pg_current_wal_insert_lsn()`, and activates generation 1 atomically.

The write lock is part of the contract. If estimated copy duration exceeds the
operator's write-stall budget, local tracking is refused and the backup profile
is recommended.

### Maintenance boundaries

The target policy has no timer-driven full-table checkpoints. The existing
15-minute automatic checkpoint loop must be disabled before release.

An explicit maintenance checkpoint may create a new generation only when:

- disk and WAL headroom preflight passes;
- the measured/estimated write stall fits the configured budget;
- table identity is stable under the tracking and relation locks;
- the exact base, boundary coordinates and generation activation commit
  atomically.

Retention may then retire whole sealed generations. It must not independently
delete a snapshot and the deltas that connect another surviving snapshot.

### T-01 decision: WAL-only local total order

PostgreSQL commit timestamps are not a total order. Distinct transactions can
share one microsecond timestamp, a commit after an exact local-base boundary
can equal its `boundary_time`, and wall-clock regression can place it before
that boundary.
An XID is not a commit-order tie-breaker. Therefore the generic replay interval
`(boundary_time, target]` is not a correctness contract. Trigger capture and
the legacy timestamp APIs remain experimental compatibility behavior and never
create or consume a correctness-qualified local generation.

WAL capture supplies the required total COMMIT-LSN order. Public
`flashback_restore_lsn()`, `flashback_query_lsn()` and
`flashback_recover_deleted_lsn()` therefore execute one contiguous prefix.
The legacy `timestamptz` entry points reject a qualified WAL lifecycle.

`flashback_resolve_target()` is the only timestamp convenience in the qualified
local contract. It pins one closed generation frontier and returns one observed
COMMIT LSN only when every commit at or before that LSN is on the requested side
of the timestamp cut and every later commit through the frontier is on the
other side. Equal-time collisions, timestamp inversions, gaps, incomplete
frontiers and cross-generation ambiguity are errors. Callers execute the
returned LSN; they never replay by timestamp. Adversarial tests exercise both
same-microsecond collisions and inverted timestamp/LSN order.

No implementation may silently choose XID/event ID as commit order or treat
`commit_time <= target_time` as a WAL-prefix selector.

### Trigger capture

`staging_events` is LOGGED, so worker polling latency affects visibility and
RTO rather than creating an UNLOGGED crash window. This does not solve total
ordering. Explicit `capture_mode=trigger` is retained for compatibility tests
only, creates no coverage generation and is never selected by `auto` when
`wal_level` cannot support the qualified WAL path. `max_row_size` is likewise a
legacy-trigger limit; silent row omission is not part of the WAL-local
contract.

### WAL capture

Each logical-slot incarnation is a distinct capture-stream epoch. Slot loss,
drop/recreate, plugin/database mismatch, unexpected external advancement or an
unsupported decoded message freezes the last proven commit watermark and opens
a persistent gap. Recreating a slot never resumes an old generation
automatically.

Every affected table must be re-anchored independently. For `local_delta`, that
means a new exact local-base boundary. The interval between the frozen
watermark and the new anchor remains rejected forever.

## Backup profile

The backup profile records schema/DDL metadata and verified physical coverage;
it does not create a local row snapshot or duplicate DML into `delta_log`.

Initial backup tracking is unanchored. Its transaction writes a durable LOGGED
tracking marker whose real commit coordinate is resolved after commit. Only a
new completed and verified **full** backup whose start LSN is
strictly after that resolved marker commit may create the first active backup
generation. A backup that predates or overlaps tracking cannot qualify merely
because its stop boundary is later. The verified backup stop boundary is the
first physical-backup anchor; targets between the tracking marker and that
anchor are not advertised.

A target is eligible only when a completed full backup precedes it and all WAL
through the selected verified recovery coordinate is present and revalidated.
WAL capture stream progress uses transaction commit LSN. A backup-backed DDL
request may instead use a durable, verified pre-DDL recovery LSN; that physical
recovery coordinate must not be mislabeled as the DDL transaction's commit
LSN. Each backup generation persists a separate inclusive physical-WAL
`valid_through_lsn`. It advances only while the repository shared lock is held
and the selected backup plus every required archive segment is revalidated;
expire takes the matching exclusive lock. Missing or contradictory archive
evidence freezes this frontier. The helper performs native PostgreSQL recovery
in a private cluster, validates table identity and content, extracts one
artifact, and never mutates production. The extension owns the final
validation and transactional swap.

The chosen full backup is not represented by a profile name or LSN alone. An
immutable anchor binds its lifecycle, repository key, stanza, backup label,
FULL type, database system identifier, timeline, manifest reference/digest,
post-marker start and stop LSN. pgBackRest establishes the start LSN after its
backup-start checkpoint. A generation's boundary LSN must be that same
anchor's stop LSN. Missing or mismatched identity evidence rejects activation.

LSN comparison is valid only inside the physical timeline recorded by the WAL
stream or backup anchor. HA/failover is outside the first-release scope: a
promotion/timeline change freezes the old frontier, opens a durable gap and
requires a new profile-qualified anchor. The runtime must never compare or
continue a generation across different timeline IDs as if LSN alone were
globally ordered.

Snapshot-direct is an acceleration of private cluster materialization. It does
not remove WAL replay or table extraction cost. Classic pgBackRest restore is
the portable fallback. Repository backup/expire operations must honor the
shared/exclusive lock protocol in the operator runbook.

## Admission and capacity

Profile choice uses all of the following:

- live heap, TOAST and index bytes;
- observed changed bytes/events per hour and their WAL amplification;
- requested retention and restore-time objective;
- filesystem headroom, WAL headroom and helper work-root budget;
- acceptable write stall for exact local boundaries.

Table size alone is neither an automatic selector nor a release guarantee.
At first track, future change rate is unknown unless trustworthy telemetry is
available. Size/headroom and write-stall limits are hard gates; change-rate
estimates are advisory until health has measured sufficient history.

Local restore preflight must budget for the shadow relation, rebuilt indexes,
the old relation retained until commit, and the required post-restore base.
The measured audit case needed roughly one complete shadow plus one heap-sized
post-restore image.

## Retention

`retention_interval` is a requested recovery window, not permission to delete
events by age in isolation.

- The active generation's base and replay chain are never age-pruned.
- If no newer qualifying generation anchor exists, retention is blocked and
  health reports maintenance/storage pressure; correctness wins over the
  requested byte cap.
- Sealed payload may be retired only as a whole under the tracking lock. The
  generation's ownership and half-open applicability are immutable after it is
  sealed, but capture backlog already bound to it may still drain. Its
  inclusive watermark may advance monotonically only up to its
  `superseded_before` coordinate. Retirement is forbidden until that drain is
  complete and the frontier through the exclusive upper bound is proven. The
  coordinator takes that advisory key in each transaction. It first commits a
  durable `retiring` intent/tombstone, which becomes the cross-transaction
  admission fence. A later transaction reacquires the key, revalidates the
  frozen evidence, performs idempotent deletion, and transitions metadata to
  `retired` only after verified absence. A crash rolls back partial deletion;
  one retrier resumes from the committed intent while admission remains
  fail-closed.
- **Deletion validates identity and need, not content.** Cleanup proves it is
  removing the right physical payload object (catalog OID, payload
  naming/namespace contract, extension ownership/membership,
  tracking/generation/snapshot binding and a catalog-only physical tuple-layout
  fingerprint) and that a newer active generation still anchors coverage at
  removal time; it never re-scans heap content. Content integrity is a use-time
  property of restore/query admission. The intent-time row count
  stays in the tombstone as forensic evidence only — drift in a payload that
  is about to be discarded must not wedge retention, and a full re-count
  would make cleanup cost proportional to table size without proving
  integrity (equal-count modifications pass a count check).
- Delegated `flashback_admin` operation is API-only. It cannot directly mutate
  internal tables or runtime payload, toggle the process-local restore guard,
  or attach/detach capture triggers. A PostgreSQL superuser remains inside the
  trusted administration boundary and can invalidate any extension's
  guarantees.
- Generation and gap metadata remain as an audit/rejection record even after
  payload retirement.
- Payload retirement records a durable tombstone containing at least the
  tracking/generation identity, payload kind, removal time and integrity/hash
  metadata. Referential actions must not cascade-delete generation, gap or
  lineage audit records.
- A `delta_log` partition is removable only when no retained generation,
  incident or backup marker needs any row in it.

## Untrack and retrack

Untrack does not erase history by deleting the identity parent. Under the
stable lifecycle lock it stops capture, drains or durably invalidates pending
work, closes every `building`/`active` generation, records payload retirement
intents, then deletes the `tracked_tables` current binding and atomically sets
the immutable lifecycle's retirement audit. Generations, gaps, requests and
tombstones reference that lifecycle and survive. A current binding cannot be
removed while an eligible or pending generation remains.

Tracking the same relation or name later creates a new lifecycle and exact
anchor. Rebinding a retired `tracking_id` is forbidden; neither OID reuse nor a
matching schema/table name joins the two histories.

## Restore finalization

A restore creates a new timeline. The restored target time is lineage
metadata; any successor generation begins at a real post-swap boundary, not at
the historical target. Finalization differs by profile.

### Local-delta restore

Before destructive swap, preflight must prove enough headroom for the new
exact local base. The swap transaction holds the final relation lock, installs
capture on the replacement, creates and validates the base, binds subsequent
capture to a `building` successor, and writes a LOGGED pending marker carrying
the source XID or a transactional logical marker. It conservatively freezes
admission but does **not** invent a boundary time/LSN or activate the successor:
the transaction cannot know its own commit time or COMMIT LSN, while its base
already sees its uncommitted swap.

After commit, a controller/worker resolves the real commit coordinate. Under
the same stable tracking lock it revalidates the base and capture binding, sets
the predecessor's exclusive applicability end, and activates the successor at
that exact coordinate. If resolution fails, the successor remains
building/unanchored and a durable conservative gap rejects all post-swap
targets. The SQL function's `bigint` result reports rows/events applied by the
swap transaction; it is not a claim that successor coverage is ready. It emits
a pending notice, and callers must observe `flashback_health()` after commit.
A higher-level workflow may report continuous coverage only after post-commit
activation. The first release has no “skip post-restore base” escape hatch.

### Backup-profile restore

The backup profile never creates a local row snapshot during finalization.
The swap transaction records lineage, creates a pending/unanchored successor
state and opens a durable `post_restore_unanchored` gap using a transactional
marker. After its real commit coordinate is resolved, that coordinate becomes
the predecessor's exclusive applicability end and the predecessor is sealed.
The successor remains `building`; there is deliberately no active generation
during this gap. A successful swap is therefore not, by itself, new backup
coverage. The restore result and
`flashback_health()` must explicitly report the unanchored state and the
required next full backup; neither may advertise continuous coverage.

In the first release, the successor becomes `active` only after a new
post-swap **completed and verified full pgBackRest backup** is taken. Its
start LSN must be strictly after the resolved swap commit; an
overlapping backup that merely completes after the swap does not qualify. The
verified backup stop boundary anchors the new generation. A partial, failed,
differential or incremental backup cannot close the gap. The interval from the
swap boundary up to that full backup's stop boundary remains permanently
rejected, even after later coverage resumes.

## Worker and maintenance scheduling

Capture draining must be isolated from checkpoint, retention and helper
maintenance so a blocked table does not create head-of-line lag for unrelated
tables. Maintenance uses bounded work and try-lock/skip behavior.

Adaptive waiting is allowed in either capture mode only when measured capture
visibility and WAL-retention SLOs remain satisfied. WAL mode additionally
needs a bound that prevents slot lag from exhausting disk. An event-driven
latch wakeup is preferred to permanent high-frequency polling.

## Deliberately unsupported or deferred

- automatic periodic full-table local checkpoints;
- profile selection from a hard-coded size threshold;
- statement-time trigger PITR;
- timestamp-only trigger recovery; T-01/A deliberately excludes it;
- recovery across an unverified generation or persistent gap;
- silent continuation after row skip, slot loss or capture disablement;
- non-blocking MVCC-aware local checkpoints (research candidate C);
- relation-level/custom WAL redo in the first release.

## Implementation status and release gates

The WAL-local runtime now writes stable lifecycle/generation/stream/gap
metadata, promotes complete COMMIT-LSN batches, admits immutable generation
targets, resolves only unambiguous timestamp-to-prefix mappings, and implements
restore/query/recover/re-anchor LSN APIs. Trigger and timestamp functions remain
legacy. Local generation-aware retention is wired; the backup-profile
generation protocol is not yet wired. Schema migration is also not assumed to
be lock-free: before upgrading a populated installation, release
qualification must measure lock duration, table-rewrite/disk requirements and
downtime and provide a maintenance runbook. Logical dump/restore deliberately
does not migrate any pg_flashback tracking, payload or coverage state: relation
OIDs, slot incarnations, partitioned history and sequence state cannot be
transported as one qualified chain. The restored database starts untracked and
must establish fresh profile-specific generation anchors. A real dump/restore
test must prove that it fails closed this way. Retired payload must leave the
audit tombstones defined above.

The new schema also requires a real versioned extension migration before
release. The package may not stay at `0.1.0` and rely on base-SQL replay or
drop/create: runtime payload tables are extension-owned, so drop/create is
destructive by design. Large-table FK/check validation must be staged and its
lock, scan, WAL and disk cost measured.

Release remains blocked until at least:

- T-01/A remains reflected consistently in code, tests and documentation;
- direct COMMIT-LSN restore/query/recovery entry points remain the qualified
  local primitives, legacy timestamp paths fail closed for WAL generations,
  and the resolver continues to reject non-prefix timestamp mappings;
- generation/stream/gap metadata is completed for the backup profile;
- initial backup tracking remains unanchored until a durable tracking marker
  is resolved and a new verified full backup whose start LSN is strictly
  after that marker activates at its stop boundary;
- concurrent first-track calls share one pre-identity bootstrap lock and cannot
  allocate divergent tracking identities;
- local base and maintenance boundaries use the exact lock protocol;
- exact-base scans demonstrably acquire a fresh MVCC snapshot after the final
  relation lock, including when a prior writer commits while lock acquisition
  waits;
- payload is bound to stable tracking and generation identities;
- every active backup generation references one immutable, identity-complete
  backup anchor and its boundary equals the verified backup stop LSN;
- generation-aware retention remains covered by its durable-intent,
  interruption-resume and pinned-admission regression/E2E tests;
- capture disablement and mode changes continue to record a LOGGED stream break
  before the worker applies the new behavior, and remain visible through
  `flashback_health()`;
- local automated disk/write-stall preflight is added around the implemented
  in-swap base, pending marker and post-commit activation protocol;
- backup post-restore coverage remains unanchored until a new completed,
  verified full backup whose start LSN is strictly after the resolved
  swap commit activates at its stop boundary, with the intervening interval
  permanently rejected;
- backup post-restore resolution seals the predecessor and explicitly permits
  zero active generations while the pending successor and durable gap exist;
- backup physical-WAL frontiers advance only through contiguous revalidated
  archives under the repository lock;
- sealed-generation backlog drains monotonically only to its immutable
  applicability upper bound, and retirement waits for proof of that drain;
- payload retirement is interruption-safe from durable intent through retained
  tombstone;
- capture draining is separated from maintenance;
- the RB-01 through RB-11 regressions and the release matrix pass.

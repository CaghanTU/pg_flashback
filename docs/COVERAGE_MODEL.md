# Coverage generation model

Status: **T-01/A WAL-local runtime and generation-aware local retention
implemented; backup-profile runtime phase remains open**

This document turns the policy in [`STORAGE_POLICY.md`](STORAGE_POLICY.md)
into a data model and transaction protocol. It addresses RB-01, RB-02, RB-03,
RB-04, RB-05 and RB-07 from
[`OPERATIONAL_LIFECYCLE_AUDIT.md`](OPERATIONAL_LIFECYCLE_AUDIT.md).

Legacy rows are never backfilled as valid. The WAL-local runtime now creates
and consumes the model atomically for tracking, capture, target admission,
timestamp resolution, LSN restore/query/recovery, stream breaks and re-anchor.
Legacy trigger/timestamp paths remain separate. Generation-aware cleanup is
now wired for qualified local generations; backup-profile anchoring still must
land with its own regression tests.

## Why the legacy model is insufficient

The pre-generation model used a mutable relation OID as identity, one
`tracked_since` timestamp, optional backup start/end LSNs and an age-based
retention cutoff. That cannot represent:

- an exact local-base boundary or a verified physical-backup anchor;
- multiple timeline generations after restore;
- a WAL slot incarnation and its proven commit frontier;
- a permanent missing interval followed by a safe re-anchor;
- an immutable generation pinned by a concurrent restore;
- the distinction between row-change LSN and transaction commit LSN.

A single `healthy` flag would also discard which historical targets remain
safe after a break.

## Entities

### Stable tracking identity

`tracking_lifecycles` is the immutable parent of one track-to-untrack
lifecycle. It owns `tracking_id`, the recovery profile, initial table identity
and creation/retirement audit. `tracked_tables` is only the current physical
binding for a non-retired lifecycle and references that parent.

The ID never changes when a shadow swap changes `tracked_tables.rel_oid`.
Untrack removes the current binding only after pending/active generations are
closed, then atomically retires—but never deletes—the lifecycle parent. A later
track of the same relation or canonical name allocates a new `tracking_id`;
retired IDs cannot be rebound.

`rel_oid` remains the current physical relation identity. Historical assets
record the OID seen at their boundary for diagnosis, but do not use it as their
ownership key.

### Capture stream epoch

`capture_streams` represents one database-wide capture incarnation.

- In WAL mode an epoch binds the database, slot name, plugin and slot
  incarnation. A dropped/recreated or externally advanced slot starts a new
  epoch.
- Trigger capture remains unqualified legacy behavior and creates no eligible
  local generation. T-01/A intentionally defines no trigger stream frontier.
- Backup generations do not require a local capture stream.

`valid_through_lsn` is the last complete transaction COMMIT LSN proven stored,
not a row-change LSN. `valid_through_time` is its commit-time projection when
known. A broken stream freezes these values.

### Coverage generation

`coverage_generations` owns one profile-specific restore chain:

```text
local: exact base boundary + events committed after it up to watermark
backup: verified full-backup anchor + contiguous physical WAL frontier
```

Canonical generation states are:

```text
building -> active -> sealed -> retired
        `-> aborted
```

- `building` is never target-eligible.
- `aborted` is an immutable audit tombstone for a boundary that could not be
  established. Its draft physical payload is removed, it is never
  target-eligible, and an explicit re-anchor may use it only as lineage when no
  active predecessor ever existed.
- `active` is the sole current chain for a tracking identity.
- `sealed` has immutable ownership and half-open applicability and may continue
  serving historical targets. Payload already bound to it may still drain;
  its watermark may advance monotonically only as far as its exclusive upper
  coordinate.
- `retired` retains audit metadata but its payload is no longer advertised.

Only `active` and `sealed` generations are target-eligible. `aborted` and
`retired` are terminal audit states. Capture failure is
represented by a broken stream, a frozen inclusive watermark and a persistent
gap; `broken` is a stream/health condition and must never be stored as a
generation lifecycle state.
Likewise, `unanchored` is the health projection of a pending `building`
successor plus its marker/gap, not a fifth canonical generation state.
An affected generation may remain `active` temporarily so targets at or before
its frozen watermark continue to work, then become `sealed` when a successor
is anchored. Targets after the frozen watermark remain rejected.

At most one `active` and one `building` generation may exist for a tracking
identity. They may coexist while a maintenance boundary is prepared. Zero
active generations is valid only as an explicit fail-closed lifecycle state,
including the backup interval after a resolved production swap has sealed its
predecessor and before a qualifying full backup activates its successor, or
after an initial local boundary aborts before any generation became active.

`boundary_snapshot_id` is nullable because backup generations have no local
snapshot. WAL-local track, restore and re-anchor create a real snapshot row for
every new local boundary. Before an `active`/`sealed` transition, profile-shape constraints
must hold: `local_delta` has a validated local boundary snapshot and required
capture-stream binding; `backup` has no local row snapshot and has a verified
full-backup reference plus verified physical-backup stop anchor. Temporary
NULLs are permitted only for non-eligible `building`/pending rows.

`backup_anchors` is the append-only proof behind that backup reference. It
binds `tracking_id`, helper profile, repository key, stanza, backup label,
FULL type, database system identifier, timeline, manifest reference and
SHA-256, durable tracking/swap marker LSN, backup start/stop LSNs and
verification audit. pgBackRest establishes the start LSN only after its
backup-start checkpoint; that start LSN must be strictly after the marker.
`coverage_generations` references the tuple
`(backup_anchor_id, tracking_id, backup_stop_lsn)`, so neither an anchor from a
different lifecycle nor a naked/mismatched stop LSN can activate a generation.
The anchor row is immutable after insertion.

For a backup generation, `valid_through_lsn` is an inclusive contiguous
physical-recovery frontier, not a capture-stream commit watermark. It starts at
the verified full-backup stop boundary and advances only after backup metadata,
timeline history and every required WAL archive segment are revalidated under
the repository shared lock. The persisted update and generation metadata
update are atomic. Backup/expire takes the matching exclusive lock and may not
remove an active generation's required range; missing external assets freeze
admission and create a durable incident/gap when detected.

The first backup generation cannot adopt a backup that already existed when
tracking began. Initial tracking commits a LOGGED marker, resolves that
marker's real commit coordinate, and remains `building`/unanchored with zero
active generations. Only a new completed full backup whose start LSN is
strictly after the resolved marker commit may activate the generation at its
verified stop anchor. A pre-existing or overlapping backup is not qualifying
evidence even when it stops later.

### Persistent coverage gap

`coverage_gaps` is LOGGED and durable. A gap records a lower coordinate,
whether that lower endpoint is inclusive, and an optional re-anchor coordinate.
The rejected interval is normally:

```text
(last proven watermark, new generation anchor)
```

For an exactly resolved skipped event it may instead be:

```text
[missing event commit, new generation anchor)
```

An unresolved transaction marker carries its source XID and a conservative
statement-time lower bound. Until resolved, admission closes at the preceding
generation/stream watermark.

Re-anchoring updates the gap's upper endpoint and
`reanchored_by_generation_id`; it never deletes the gap or makes a target
inside it valid.

Canonical reasons include `wal_slot_lost`, `row_too_large`,
`post_restore_unanchored`, `capture_disabled`, `unsupported_wal_message`,
`retention_violation` and `operator_invalidated`. The database column remains
open text so adding a reason does not require a schema migration.

Every audit/qualification row references immutable `tracking_lifecycles`, not
the deletable `tracked_tables` binding. Every reference that carries both a row
ID and `tracking_id` enforces them as one composite identity. A gap's source
generation, its re-anchor generation, snapshot ownership and restore-lineage
parent must belong to the same lifecycle unless a future cross-lifecycle
operation is explicitly modeled. Cleanup uses restrictive references and
tombstones, never cascades that erase these audit relationships. Coverage and
payload rows are not extension configuration data and never cross a logical
dump/restore boundary.

## Payload binding

Metadata alone is not enough. Qualified WAL-local runtime stamps all relevant payload:

- `delta_log`: `tracking_id`, `generation_id`, `stream_id`, change LSN and
  separate transaction `commit_lsn`;
- `pending_wal_events`: authoritative DDL tied to stable tracking, generation,
  stream and source XID inside the user's transaction;
- `staging_events`: either all-null legacy trigger binding or a fully bound
  compatibility shape; it is not admitted by the WAL-local APIs;
- `snapshots` and `schema_versions`: stable tracking identity and the owning
  boundary/generation where applicable.

`source_xid` is only a correlation key and is stored in PostgreSQL's 32-bit
`TransactionId` domain (`txid_current() mod 2^32`), matching the XID emitted by
logical decoding. Epoch-expanded transaction IDs must not be joined to decoder
XIDs: that comparison fails after transaction-ID wraparound. COMMIT LSN remains
the canonical, stream-scoped ordering and uniqueness coordinate.

The schema permits only two delta/staging shapes: all three binding IDs NULL
for explicitly legacy rows, or all three present. Composite foreign keys bind
the present tuple to one generation and its stream; three independently valid
IDs are not sufficient.

Trigger events must capture their generation before commit. Looking up the
currently active generation during worker flush is wrong: a maintenance base
may already include the row while an older staging event is still queued.

WAL events are routed by commit time/COMMIT LSN. An event generated before a
boundary but decoded afterward belongs to the preceding generation; it must
not be replayed on top of the new base.

The existing `delta_log.lsn` is retained as change LSN for ordering/audit. It
cannot drive visibility or the stream watermark because a row change precedes
its transaction COMMIT record.

## Coordinate and range semantics

- LSN is the canonical total-order coordinate for WAL and backup profiles. A
  release-qualified WAL-local request targets a transaction COMMIT LSN, not a
  timestamp filter over individual events.
- LSN is ordered only within one PostgreSQL timeline. WAL stream epochs and
  backup anchors persist that timeline; generation/stream identity supplies
  the comparison context. A timeline change breaks coverage and requires a
  gap plus re-anchor. HA/failover remains outside the first release.
- The stored `canonical_coordinate_kind` is derived from the profile:
  `commit_lsn` for local WAL generations and `physical_lsn` for backup
  generations. Timestamp fields are projections/diagnostics and have no
  monotonic ordering constraint.
- Trigger timestamp APIs are legacy and are not a qualified coordinate.
- `boundary_*` is inclusive and denotes the profile-specific generation
  anchor. For `local_delta`, the base is the table state at an exact local-base
  boundary. For `backup`, it is the verified physical full-backup stop anchor;
  it is not represented as a write-locked local base.
- `superseded_before_*` is the exclusive applicability end of a generation.
  A generation applies on `[boundary, superseded_before)`, with NULL meaning
  that no successor boundary has yet been established. The runtime schema may
  use an equivalently named exclusive coverage-end field.
- WAL targets replay generation-bound events in `(boundary_lsn, target_lsn]`
  by COMMIT LSN. Backup generations replay no local delta payload. The lower
  bound and tie semantics for timestamp-only trigger replay are deliberately
  outside T-01/A; code must not reuse the WAL inequality.
- Commit time is not monotonic in COMMIT-LSN order. The existing
  `timestamptz` APIs therefore cannot be made WAL-safe by replacing
  `event_time` with decoder commit time and retaining `<= target_time`. Under
  T-01/A they remain outside the supported contract.
  `flashback_resolve_target()` first pins a closed frontier and resolves the
  request to one observed COMMIT-LSN prefix; it rejects equality, inversion,
  incomplete evidence and cross-generation ambiguity.
- `valid_through_*` is inclusive: completeness is proven through that point.
- An eligible generation has `valid_through >= boundary`; activation
  initializes completeness at least through its exact local-base boundary or
  verified physical-backup anchor.
- Applicability and capture completeness are independent predicates. A target
  must satisfy both `boundary <= target < superseded_before` (when an upper
  bound exists) and `target <= valid_through`.
- The re-anchor's new generation anchor belongs to the new generation, so a
  gap's upper endpoint is exclusive.
- At a continuous local handoff coordinate the predecessor is excluded and
  only the successor matches; two generations must never match the same
  target. Backup post-restore is deliberately discontinuous: its predecessor
  is excluded from the resolved swap coordinate onward, no generation matches
  while the durable gap is open, and the later backup stop anchor belongs only
  to the successor.
- When both fields describe a capture watermark, they refer to the same commit
  frontier. A write-locked boundary's sampled time/insert-LSN pair describes
  one table-state fence but is not itself mislabeled as a transaction COMMIT
  record. Code must not substitute one coordinate for another without an
  explicit mapping.

### T-01/A: WAL-only COMMIT-LSN ordering

Commit timestamp is not a total order: equal-microsecond commits and clock
regression can invert relative to WAL. XID order is not commit order. The
adopted local coordinate is therefore transaction COMMIT LSN, scoped to one
verified stream epoch. Trigger rows remain legacy/unqualified and cannot be
activated. The legacy timestamp restore/query/recovery entry points reject any
qualified WAL lifecycle.

The timestamp resolver is a planner, not an alternate replay engine. It returns
one `resolved_lsn` only if all observed commits through a pinned frontier prove
one contiguous prefix. Same-time collisions and timestamp inversions are
adversarially tested and rejected. The subsequent operation must call an LSN
API with that result.

## Target-admission algorithm

For every restore, query-as-of or deleted-row recovery:

1. Resolve the stable `tracking_id` without trusting a same-name replacement.
2. Acquire the coverage advisory transaction lock.
3. Reread tracking/profile/current OID and reject an identity change.
4. Select exactly one `active` or `sealed` generation whose half-open
   applicability interval contains the target and whose inclusive proven
   watermark is not before it. `building` (including an unanchored pending
   successor), `aborted` and `retired` states are never selected.
5. Pin that generation row (`FOR SHARE` or an equivalent immutable reference)
   for the operation.
6. Reject if any persistent gap contains the target. A broken stream does not
   invalidate already proven history, but it rejects targets after its frozen
   watermark until a later exact local-base re-anchor begins a successor
   generation.
7. Verify the profile-specific anchor still exists: the local boundary snapshot
   and complete replay payload, or the selected full backup plus contiguous
   required WAL evidence.
8. Build the shadow/read result using only that generation's assets.
9. Before swap or return, revalidate the pinned generation under the same lock.

Zero or multiple matching generations is an error. The algorithm never falls
back to the nearest surviving snapshot.

## Common lock protocol

All lifecycle operations use one advisory key derived from `tracking_id`.
Local and backup profiles must not use different namespaces.

First track is the only pre-identity exception. It first takes a deterministic
bootstrap advisory key derived from database OID plus the canonical resolved
schema/name. While retaining that lock it rereads tracking metadata, allocates
at most one `tracking_id`, then acquires the stable tracking lock and later
locks/revalidates the resolved relation OID. There is no unlocked transfer.
Concurrent first-track calls for the same logical target therefore cannot
allocate different IDs and enter different lifecycle namespaces; a concurrent
rename/drop/replacement fails the later identity recheck.

Lock order is:

1. pre-identity bootstrap advisory locks, in deterministic key order, only for
   identities that may need creation;
2. all stable tracking advisory locks, ascending by `tracking_id`;
3. generation/stream metadata rows;
4. relation locks in deterministic relation order;
5. request/helper locks when the backup profile requires them.

This order applies to multi-table restore as well as single-table operations.
Retention uses try-lock and skips a busy tracking identity rather than waiting
in the capture path.

## Exact local-base boundary protocol

Initial local tracking, maintenance checkpoint and local re-anchor use one
protocol:

1. Require a controlled dedicated transaction with no previously assigned
   write XID and no reusable old snapshot. Repeatable-read, serializable and
   imported snapshots are rejected for this path.
2. If no tracking identity exists, acquire the deterministic pre-identity
   bootstrap lock, reread metadata, allocate one stable `tracking_id`, and
   acquire its advisory transaction lock before releasing the bootstrap lock.
   Otherwise acquire the existing stable tracking lock directly.
3. Acquire the final required relation-lock strength from the outset: at least
   `SHARE ROW EXCLUSIVE`, or a stronger mode if replica-identity, trigger or
   other capture setup requires it. Do not acquire a weaker lock and later
   upgrade it.
4. Install and verify the capture trigger, or replica identity and stream
   binding, inside this transaction while the same relation lock is held.
5. Reread OID, schema/name, profile and active generation under lock.
6. Acquire a fresh MVCC scan snapshot after the relation lock (the runtime must
   prove that an outer statement/SPI snapshot is not reused), create and
   validate the base image, then record its exact local-base boundary with
   `clock_timestamp()` and `pg_current_wal_insert_lsn()` while writes remain
   blocked.
7. Insert the `building` generation and bind its snapshot/stream.
8. In one metadata transition, set the predecessor's exclusive
   `superseded_before_*` coordinate to the new boundary, seal it and activate
   the new generation. The boundary target now matches only the successor.
9. Commit before any old payload cleanup begins.

The caller transaction restriction matters because a transaction's own
uncommitted writes are visible to its CTAS but would commit after a boundary
captured inside that transaction.

## Initial backup-profile anchor protocol

Backup tracking starts without an eligible generation:

1. Under the bootstrap and stable tracking locks, create the tracking identity,
   schema/DDL metadata, a `building` backup generation and a LOGGED transition
   marker in the tracking transaction. Do not bind an existing backup.
2. After commit, resolve and persist the marker's real commit coordinate. Until
   that succeeds, health is `unanchored` and target admission finds zero active
   generations.
3. Start a new full pgBackRest backup only after the resolved marker commit.
   Its start LSN must be strictly later than the marker; a full
   already in progress or completed before tracking is ineligible.
4. Under the repository lock, verify the completed full backup and required
   WAL evidence, then activate the generation at its stop boundary and
   initialize the physical `valid_through_lsn` to that same anchor.

The interval before the verified stop anchor is not advertised as tracked
coverage. Marker resolution and backup verification are idempotent; neither
may invent an in-transaction LSN or silently choose an overlapping backup.

## Maintenance and retention

Maintenance first preflights headroom, WAL growth and expected lock duration.
Failure leaves the active generation untouched.

After a new generation commits, capture backlog already stamped with the
predecessor generation may continue draining into that sealed generation.
Sealing makes ownership and `superseded_before` immutable; it does not freeze
an incomplete watermark. The sealed watermark may advance monotonically only
to that exclusive upper coordinate, never beyond it and never by accepting new
ownership. A separate maintenance transaction may retire sealed payload only
after the drain frontier proves completeness through that upper coordinate.
It removes a generation's replay assets as a unit and marks metadata `retired`;
it does not delete generation or gap records. Cleanup is an idempotent
fail-closed state machine:

1. under the stable coverage transaction lock, write and commit a durable
   payload tombstone/intent in `retiring` state before destructive work;
   admission treats that committed intent as a fence and rejects the payload;
2. remove the identified database/partition/external assets idempotently;
3. in a later transaction, reacquire the same lock, revalidate the immutable
   intent evidence, remove the snapshot/delta/schema assets, verify exact
   absence, record removal counts/time, mark the tombstone `removed` and
   transition the generation to `retired` atomically;
4. after interruption PostgreSQL rolls back that destructive transaction; a
   retrier reacquires the lock and resumes from the committed intent. Never
   infer retained payload merely because the generation row still says
   `sealed`.

Foreign keys and cleanup procedures must not cascade-delete generation, gap,
lineage or tombstone audit records.

If the requested retention cutoff falls inside the only active chain, cleanup
does nothing and health reports `maintenance_required`. Partition deletion is
allowed only after proving no retained generation or incident references any
row in that partition.

## Untrack and retrack

Untrack uses the stable lifecycle lock and is fail-closed while a generation
is `building` or `active`. It first stops capture, resolves pending capture,
closes or retires eligible generations, and records payload retirement intents.
Only then may it delete the `tracked_tables` current binding. The same
transaction writes the lifecycle retirement time, actor, final physical
identity and reason. The immutable lifecycle, generations, gaps, requests and
tombstones remain as audit; physical payload follows its explicit retirement
state machine.

Retrack never revives that parent. It takes the bootstrap identity lock again,
allocates a new lifecycle ID and establishes a new profile-specific generation
anchor. Historical IDs cannot be selected through the new current binding,
even if PostgreSQL reuses an OID or the schema/table name is identical.

## Restore and post-restore branch

Restore pins a source generation before reading its base or deltas. Retention
cannot retire that generation while the restore holds the coverage lock/pin.
A qualified local restore then takes the target relation's final
`ACCESS EXCLUSIVE` lock, fixes a bounded WAL barrier and proves every
already-committed change for that relation was previously consumed through the
trusted decoder before materialization or shadow swap. Logical-slot advancement
cannot be committed from inside the restore transaction. If the bounded prefix
is pending or cannot be proven drained, restore fails closed, leaves the live
relation intact and is retried after the normal worker catches up.
This prevents a committed change from becoming undecodable when the old
relation is dropped and replaced with a new OID.

Qualified history keeps the immutable relation OID recorded at each generation
boundary. A post-restore swap updates only the current `tracked_tables` binding;
it never rewrites historical generation, snapshot, schema or delta identities
to the new OID. Stable `tracking_id`/`generation_id` ownership connects those
historical assets to the current lifecycle.

A restore creates a new timeline, but the two profiles establish its successor
boundary differently.

### Local-delta two-phase finalize

A swap transaction cannot know its own commit time or COMMIT LSN. A base made
after the swap sees the transaction's uncommitted table changes, so labeling it
with an earlier in-transaction clock/insert-LSN would make the base newer than
its claimed boundary. Local finalization is therefore explicitly two phase:

1. Preflight budgets the shadow relation, indexes, old relation through commit
   and post-swap base. The controller acquires the stable tracking lock and
   final `ACCESS EXCLUSIVE` relation lock.
2. In the swap transaction, validate the new relation, install capture on it,
   create and validate the base, allocate a `building` successor, bind
   post-swap capture to that successor, and write a LOGGED pending-transition
   marker containing the source XID (or a transactional logical marker). The
   predecessor is conservatively frozen for admission. No exact boundary is
   invented and the successor is not yet eligible.
3. Commit the swap. Capture after commit is already bound to the pending
   successor, even if activation has not completed.
4. A controller/worker resolves the swap's real commit time and, for WAL mode,
   COMMIT LSN. Under the same stable tracking lock it revalidates the pending
   base and capture binding, sets the predecessor's exclusive applicability
   end to that coordinate, records lineage and activates the successor at the
   exact same coordinate.
5. If the marker or commit coordinate cannot be proven, the successor remains
   `building`/unanchored, a durable conservative gap stays open and no
   post-swap target is admitted.

The pending marker has a unique transition ID and the resolver is idempotent.
A rollback removes the swap, base and marker together. A crash after commit but
before activation leaves the durable `building` successor discoverable and
fail-closed; retry either performs the one valid activation or recognizes that
the identical transition already did so. DML committed during this pending
window remains bound to the successor and is replayable after activation; it
must never be flushed into the predecessor or discarded as “not active”.

The SQL function returns only an applied-event count and emits an explicit
pending notice; neither is a claim of finalized continuous coverage. After
commit, callers inspect `flashback_health()` and a higher-level workflow may
report coverage-ready success only after step 4. The first release does not
support an explicit “skip post-restore base” escape hatch.

### Backup-profile finalize

The backup profile creates no local row base. Its swap transaction records
lineage, a LOGGED pending/unanchored transition and an open
`post_restore_unanchored` gap. Once the swap commit coordinate is resolved, the
predecessor receives that exclusive applicability end and is sealed; the
successor remains `building` and ineligible. This intentional zero-active state
is the backup exception to continuous atomic generation handoff. Admission
fails on the durable gap rather than falling back to the sealed predecessor.

Only a new post-swap completed and revalidated **full** pgBackRest backup may
anchor and activate the backup successor in the first release. Its
start LSN must be strictly after the resolved swap commit; a
backup already in progress at the swap cannot qualify merely because it
finishes afterward. Its verified backup stop boundary is the new inclusive
generation boundary and initial physical `valid_through_lsn`. The interval from
swap commit through, but excluding, that stop boundary remains a permanent
rejected gap; closing the open endpoint never validates it. Failed, partial,
differential or incremental backups cannot activate the generation.

## Slot-loss protocol

Before/after every WAL consume transaction, validate slot database, plugin,
`wal_status`, invalidation reason and persisted slot epoch. Delta insertion,
slot consumption and stream watermark advancement commit together.

On loss or unexpected advancement:

1. freeze the stream and generation watermarks;
2. durably mark the stream broken in a separate transaction if the failed
   consume transaction rolled back;
3. insert persistent gaps for dependent active generations;
4. keep already proven targets at or before the frozen frontier admissible
   through the still-`active` generation; reject every later target;
5. create a new stream epoch if the slot is recreated;
6. require a new exact local-base boundary for each affected table before new
   coverage begins, then seal the predecessor with that boundary as its
   exclusive applicability end.

The worker must not cache `slot_ready=true` forever, and malformed/unsupported
messages must not be filtered while advancing the slot without an incident.

## Oversized-row protocol

Every trigger skip path must call one internal incident writer before it emits
a warning. The writer inserts stable tracking/generation/stream IDs, source
XID, statement time, reason and skipped count into LOGGED metadata inside the
application transaction.

- Application rollback removes both the data change and incident.
- Incident-write failure aborts the data change.
- On commit, health immediately becomes red for targets after the preceding
  watermark.
- A worker may resolve commit time from `pg_xact_commit_timestamp()` and refine
  the lower endpoint, but may never widen accepted coverage.

Disabling capture while tracking is active needs the same durable-break
semantics; a GUC bypass with no marker is unsupported.

## Trigger DDL commit-coordinate protocol

A trigger/DDL event stores its source XID and transaction-local marker in
LOGGED metadata. Long-running DDL is ordered at its resolved transaction commit
time, not when the DDL statement starts and not when a worker flushes it.
Post-commit resolution uses `pg_xact_commit_timestamp()`; the release contract
therefore requires `track_commit_timestamp=on` with its restart requirement.

If the committed XID yields a NULL or otherwise unprovable commit timestamp,
the resolver writes/keeps a durable gap from the preceding proven watermark,
freezes admission and reports the incident in `flashback_health()`. It must not
fall back to statement time, wall-clock observation or flush time. A later
profile-qualified generation anchor may resume coverage but cannot validate
the missing interval.

## Health projection

`flashback_health()` is a read-only projection, not an independently stored
Boolean. The current WAL-local projection exposes lifecycle/profile,
generation/stream states, watermark, open-gap count and actionable reason.
Before the whole project is released it must additionally expose:

- tracking identity, profile and current OID;
- active generation and profile-specific anchor kind/coordinate;
- generation and stream watermarks;
- open gap and permanent historical-gap count;
- slot epoch/status for WAL mode;
- oldest/newest admitted target coordinates;
- payload bytes, estimated growth and headroom;
- `healthy`, `maintenance_required`, `broken`, `unanchored` or `retired` state;
- an actionable reason and required next operation.

These are health projection values, not generation lifecycle states. In
particular, `broken` means a broken capture stream or durable coverage incident;
the owning generation remains `active` or `sealed` with its proven watermark
frozen.

No generation after schema migration means `unanchored`, not healthy. Existing
legacy rows must not receive a fabricated valid generation because their old
boundaries were not established by this protocol.

## Schema migration phases

1. **Schema — implemented:** immutable tracking lifecycle/current binding;
   LOGGED stream, backup-anchor, generation and gap tables; nullable legacy or
   fully composite-bound payload IDs; separate commit-LSN columns; constraints
   and schema-contract tests.
2. **WAL-local capture — implemented:** exact track/re-anchor/restore boundaries,
   WAL payload stamping, complete-commit ledger, protected DDL pending events,
   stream epochs and slot-discontinuity gaps. Trigger remains legacy.
3. **WAL-local read/restore — implemented:** common lock, generation
   admission, LSN query/recover/restore, fail-closed timestamp resolver and
   post-restore pending-generation activation.
4. **Generation-aware retention — implemented for local generations:** durable
   two-transaction intent, admission fence, whole-generation retirement,
   interruption-safe resume and lock-then-recheck empty-partition cleanup.
5. **Migration qualification — open:** existing installations become `unanchored`
   until an explicit profile-qualified re-anchor; no automatic trust backfill.

Schema installation is not operationally lock-free. Before release, a
populated-version upgrade must be measured for relation-lock
duration, table rewrites, temporary disk/WAL growth and required downtime, and
documented in a maintenance runbook. Qualification must include a real
`pg_dump`/restore round trip proving that the restored extension has no stale
tracking, payload or coverage state and requires fresh tracking boundaries.
It must also prove that retired payload leaves its audit tombstone.

The legacy `coverage_start_lsn`, `coverage_end_lsn`, `tracked_since`,
`retention_cutoff`, `checkpoint_interval` and `base_snapshot_table` columns
remain temporarily for compatibility. They are no longer sources of truth for
qualified local generations; backup-profile anchoring still must stop treating
legacy coverage LSN columns as authoritative when that runtime phase lands.

## Required regression matrix

- concurrent DML crossing track/checkpoint boundary (RB-01/RB-07);
- two concurrent first-track calls serialize through one bootstrap namespace
  and cannot create different tracking identities;
- a writer that commits while exact local-base lock acquisition waits is present
  in the base, proving the scan snapshot was acquired after the relation lock;
- track/checkpoint after caller DML in the same transaction;
- trigger and WAL backlog crossing a generation boundary;
- sealing freezes ownership/applicability while predecessor backlog advances
  its watermark monotonically no farther than `superseded_before`; retirement
  is rejected until that drain is proven complete;
- retention versus restore/query race and immutable generation pin (RB-02/03);
- target between cutoff and the next exact local-base boundary;
- slot loss, slot drop/recreate and external slot advancement (RB-04);
- oversized marker commit and rollback, for every trigger path (RB-05);
- OID-changing local and backup swaps preserving stable tracking identity;
- post-restore base success, preflight failure rollback and first-release
  rejection of an explicit skip-base request;
- local restore pending marker, post-commit commit-time/COMMIT-LSN resolution,
  and refusal to report terminal success before successor activation;
- local swap rollback, crash after swap commit/before activation, duplicate
  resolver retry and DML committed while the successor is pending;
- backup restore pending/unanchored state, a new completed full-backup stop
  boundary whose start LSN is strictly after the resolved swap commit,
  persisted physical-WAL
  frontier advancement under repository lock, and permanent rejection of
  `[swap commit, backup stop)`;
- initial backup tracking with a durable resolved marker, zero active
  generations before anchoring, rejection of pre-existing/overlapping backups,
  and activation only by a new full whose start LSN is strictly after
  that marker;
- backup post-swap resolution sealing the predecessor, preserving zero active
  generations while the gap is open, and refusing fallback to the predecessor;
- trusted DDL pending metadata promoted only with its real COMMIT record, plus
  rejection of forged client logical-message payloads;
- T-01/A WAL-only local support, direct COMMIT-LSN
  restore/query/deleted-row targets and rejection of legacy timestamp paths;
  the timestamp resolver must continue proving one WAL prefix across
  equal/inverted commit times and incomplete frontiers;
- crash/interruption before, during and after payload deletion resumes from the
  tombstone intent without advertising partial assets;
- coverage rejection in restore, query-as-of and deleted-row recovery;
- reverse-order concurrent multi-table restores without deadlock;
- capture mode, slot-name or enable/disable changes requiring a new epoch and
  re-anchor.

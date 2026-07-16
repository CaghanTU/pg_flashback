# First release scope

Status: **T-01/A WAL-local scope implemented; not publishable until every
remaining release gate passes**

This document defines the fail-closed supported subset for the first
publishable pg_flashback release. The cross-profile rules in
[`STORAGE_POLICY.md`](STORAGE_POLICY.md) are authoritative. A topology or
coverage state not listed here is rejected; it is never attempted
optimistically.

## Common supported contract

- PostgreSQL 15–18 on Linux x86_64 or aarch64 source builds; tagged prebuilt
  extension archives are x86_64
- ordinary logged tables whose stable tracking identity can be proven
- a target contained by exactly one `active` or `sealed` generation's
  half-open applicability interval and at or before its separate inclusive
  capture watermark
- direct commit-coordinate recovery: a transaction COMMIT-LSN target for the
  currently scoped WAL local profile; a backup DDL request may use a separately
  labeled, durable and verified pre-DDL physical recovery LSN
- structural schema, row fingerprint, owner and table ACL validation where the
  selected restore profile supplies them
- one stable per-tracking lock namespace shared by restore, checkpoint,
  retention, untrack and backup finalization
- `building` (including a pending/unanchored successor), retired and ambiguous
  generations are never selected; a broken stream preserves only targets at
  or before its frozen watermark

## Local-delta profile

The local profile is supported only after its coverage runtime gates pass:

- one exact, write-locked base per active generation
- replica-identity/stream setup and exact base creation in
  the same transaction under the final relation-lock strength acquired from
  the outset (`SHARE ROW EXCLUSIVE` minimum, stronger when required)
- a fresh MVCC base-scan snapshot acquired after that relation lock; reusable
  old, repeatable-read, serializable and imported snapshots are rejected
- durable delta payload bound to stable tracking and generation identities
- WAL mode only with a verified logical-slot epoch and complete commit-LSN
  watermark
- a release-qualified COMMIT-LSN target API whose replay selects one contiguous
  WAL prefix; the existing `timestamptz` local APIs are not that interface
- explicit, preflighted maintenance boundaries; no timer-driven full-table
  checkpoints
- retention of the complete active replay chain, with whole sealed-generation
  retirement only after a newer exact local-base boundary commits and the
  predecessor backlog drains through its upper coordinate
- restore/query admission against immutable generation and persistent-gap
  metadata
- two-phase post-restore finalization: the swap transaction creates the base,
  binds capture and writes a LOGGED pending marker; only post-commit resolution
  of the real commit time/COMMIT LSN may activate the successor
- no API for skipping the required post-restore base. The SQL row-count result
  explicitly means “swap applied, coverage pending”; coverage-ready success
  requires post-commit `flashback_health()` activation

## Backup profile

- local POSIX pgBackRest repository
- completed full backups
- PostgreSQL 15–18 when `pg_bin_dir` matches the backup major version
- pgBackRest 2.53.1 (the version exercised by the release E2E suite)
- LSN recovery targets
- ordinary and quoted table identifiers passed as separate JSON fields
- extension-created requests after DROP/TRUNCATE/ALTER using durable pre-DDL
  LSN markers and separately versioned post-ALTER schemas
- XFS reflink snapshot-direct when an actual CoW probe succeeds
- classic pgBackRest restore as the fallback
- custom-format `pg_dump` artifact output
- one idempotent request per `request_id`
- structural schema, row fingerprint, owner and table ACL restoration
- the reference controller's checksum-verified, single-transaction import and
  extension shadow swap
- an initial durable tracking marker followed only by a new full backup whose
  start LSN is strictly after the resolved marker commit; that backup's
  verified stop boundary is the first physical-backup anchor, and tracking has
  zero active generations before it
- an immutable backup anchor that binds repository/stanza/label, FULL type,
  system identifier, timeline, manifest reference/digest and the marker plus
  start/stop LSNs to one tracking lifecycle
- a pending/unanchored state and permanent gap after every production swap;
  only a new post-swap completed and verified full backup activates the next
  generation at its backup stop boundary, and that backup's start LSN
  must be strictly after the resolved swap commit
- a persisted inclusive physical-WAL `valid_through_lsn`, advanced only after
  contiguous archive revalidation under the repository shared lock
- restore results and `flashback_health()` explicitly expose that unanchored
  state and the required next full backup

## Rejected in the first release

- automatic periodic full-table local checkpoints
- non-blocking/MVCC-aware local checkpoints
- choosing a profile from table size alone
- trigger capture and every timestamp-filtered trigger recovery path;
  `track_commit_timestamp` does not turn it into a total order
- the existing local `timestamptz` restore/query/recover paths for a qualified
  WAL lifecycle. The supported timestamp convenience is
  `flashback_resolve_target()`, which returns one proven target LSN and rejects
  equal/inverted, incomplete or cross-generation evidence
- a target outside one verified generation or inside a persistent gap
- silent continuation after oversized-row skip, slot loss, capture disablement
  or an unsupported WAL message
- local post-restore tracking without the required pending-marker/base and
  post-commit activation protocol
- a local “skip post-restore base” escape hatch
- backup post-restore coverage before a new completed, verified full backup
  stop boundary
- adopting a backup that predates or overlaps initial backup tracking or a
  production swap, even when it completes later
- differential or incremental backup selection
- tablespaces/symlinked relation storage
- symlinked PostgreSQL configuration files in the recovered cluster
- time and transaction-ID targets in the backup helper
- remote/object-store direct snapshot access
- importing or swapping an artifact into production without extension-side
  schema and fingerprint validation
- an expire schedule that does not honor the configured external lock
- partitioned, foreign, materialized-view or unlogged backup targets
- a table renamed or moved to another schema across a backup recovery point
- tables dropped with incoming foreign keys or dependent views that must be
  recreated automatically by the backup profile
- HA/failover, managed-service and cross-major recovery topologies
- relation-level/custom WAL redo

## Common safety invariants

1. Every table lifecycle operation resolves immutable `tracking_lifecycles`;
   `tracked_tables` is only the current physical binding and relation OID is
   only the current physical identity. Untrack retires the parent without
   deleting audit; retrack allocates a new ID. Concurrent first-track calls use
   one deterministic pre-identity bootstrap lock before stable-ID allocation.
2. A restore pins one immutable generation. Retention cannot alter or retire
   its assets until the operation releases the common coverage lock.
3. A continuous local boundary handoff activates the successor and seals the
   predecessor atomically. Backup post-restore is deliberately discontinuous:
   resolving the swap seals the predecessor while the successor stays
   `building`, and zero active generations is required until the qualifying
   full backup anchor. No predecessor payload is cleaned before any bound
   backlog drains through its immutable applicability upper coordinate.
4. Generation applicability is `[boundary, superseded_before)` while capture
   `valid_through` is a separate inclusive watermark. At a handoff coordinate
   only the successor matches.
5. Commit LSN and row-change LSN are stored separately. A stream watermark is
   advanced only through a complete committed transaction.
6. Coverage breaks are synchronous LOGGED incidents. Re-anchor closes an open
   interval but never validates the missing historical range.
7. Local track/checkpoint boundaries perform headroom and lock-duration
   preflight, take their final-strength relation lock from the outset and set
   up capture under that same lock.
8. A local restore base made inside the swap transaction remains in a
   non-eligible successor until the real swap commit coordinate is resolved
   after commit. Backup restore remains unanchored until a new verified full
   backup stop anchor whose start LSN is strictly after that resolved
   commit.
9. Missing or contradictory evidence fails with an actionable stable error;
   no read/restore API falls back to the nearest surviving snapshot.
10. Payload cleanup commits a durable `retiring` intent before deletion and is
    idempotently resumable; audit tombstones survive the transition to
    `retired`.

## Backup-helper safety invariants

1. The helper never connects to or mutates the production database.
2. It never starts PostgreSQL on a repository backup directory; recovery runs
   only on a private CoW clone or a normal pgBackRest restore directory.
3. A request lock prevents concurrent reuse of a request ID.
4. A shared repository lock is held while the selected backup is revalidated,
   materialized and recovered through promotion. Backup/expire automation must
   take the matching exclusive lock.
5. Every executable is invoked with an argument vector, never by constructing a
   shell command. The sole shell string is PostgreSQL `restore_command`; all
   values in it are shell-quoted.
6. Temporary PostgreSQL listens only on a mode-0700 Unix socket directory and
   uses a generated local-only HBA file.
7. An artifact is successful only after native recovery promotes, the exact
   OID/table exists, structural/row/owner/ACL validation completes, `pg_dump`
   succeeds and the artifact checksum is recorded.
8. Cancellation, timeout and command failure stop temporary PostgreSQL and
   remove materialized cluster data. Logs/state remain for diagnosis.
9. Unsupported or ambiguous states fail closed with a stable error code.
10. The helper never performs the final production table swap. The extension
    refuses a same-name replacement OID and validates the imported artifact in
    the swap transaction.

## Release gates

- `STORAGE_POLICY.md`, `COVERAGE_MODEL.md`, README and runtime behavior agree;
- T-01/A remains implemented consistently: local capture is WAL-only, public
  COMMIT-LSN APIs are primitive, legacy timestamp entry points reject qualified
  lifecycles and the resolver passes collision/inversion ambiguity tests;
- verified physical-anchor, generation and persistent-gap runtime is fully
  wired for the backup profile;
- RB-01 through RB-11 have regression or qualification evidence;
- capture disablement fails closed synchronously and appears in
  `flashback_health()`; the qualified decoder never silently skips by the
  legacy trigger row-size limit;
- retention/restore concurrency pins one immutable generation;
- local post-restore preflight, pending marker, base, post-commit coordinate
  resolution and pre-activation non-success behavior are tested;
- backup swap remains pending/unanchored until a new completed, verified full
  backup whose start LSN is strictly after the resolved swap commit
  activates at its stop boundary, and `[swap commit, backup stop)` stays
  permanently rejected;
- initial backup tracking remains unanchored with zero active generations until
  a durable marker is resolved and a new full whose start LSN is strictly
  after that marker activates at its verified stop anchor;
- sealed ownership/applicability remains immutable while bound backlog advances
  the watermark monotonically only to the upper coordinate; retirement waits
  for complete drain proof;
- backup physical-WAL frontiers advance only across contiguous archives under
  the repository lock, and expire honors the active-generation pins;
- capture draining is isolated from maintenance head-of-line blocking;
- unit tests, fmt and clippy are clean;
- extension PostgreSQL 15–18 regression matrix remains green;
- snapshot-direct and classic restore E2E paths recover identical content;
- old target, missing WAL, repository lock race, cancellation, PostgreSQL start
  failure and stale-request cleanup are exercised;
- no PostgreSQL process, socket or materialized cluster remains after any E2E
  scenario;
- real DROP-marker request, reference-controller import, owner/ACL restore and
  production shadow swap are exercised;
- a real ALTER recovers the pre-ALTER schema from a marker proven earlier than
  the post-ALTER schema-version LSN;
- helper aggregate work-root quota and successful-artifact expiry/GC are
  implemented and tested;
- third-party notices, user guide, limitations and release checklist are
  complete.

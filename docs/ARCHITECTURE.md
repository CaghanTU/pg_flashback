# Architecture

pg_flashback's local product restores selected PostgreSQL tables from a
transactionally aligned base image plus a proven prefix of logical WAL.

## Components

| Component | Responsibility |
|---|---|
| DDL hook | Records DROP and schema events in the user's transaction while relation metadata still exists |
| Logical decoder | Decodes relevant row changes and transaction commit metadata |
| Capture worker | Drains the logical slot, stores complete transactions, and advances coverage |
| Maintenance worker | Runs bounded retention and lifecycle work without blocking capture |
| Coverage generation | Binds one base image to one WAL stream and a complete-commit watermark |
| SnapshotStore | Pluggable base-image storage: `heap_v1` (default, in-database) or `external_zstd` (opt-in, compressed artifact on an external filesystem root) |
| Recovery planner | Selects one disaster event and proves that it is recoverable |
| Recovery executor | Materializes, verifies, and atomically swaps a shadow table |
| Operation journal | Records started, failed, abandoned, applied, and verified operations |
| `pg_flashback` CLI | Presents the safe operator workflow without exposing LSNs by default |

## Capture path

pg_flashback is WAL-only. There is no trigger-based DML capture path and no
`auto` fallback. `pg_flashback.capture_mode` remains as a deprecated
compatibility GUC; only `wal` is operational. Capture requires
`wal_level=logical`, an admitted capture worker, and a logical replication
slot per configured database. `track_commit_timestamp` is not required.
Ordinary user triggers on protected tables are preserved; the extension does
not attach `flashback_capture_*` DML triggers.

```text
application transaction
        |
        +-- row changes ----------------------+
        |                                     |
        +-- DROP/DDL -> protected DDL record  |
                                              v
                                       PostgreSQL WAL
                                              |
                                      logical decoding slot
                                              |
                                        capture worker
                                              |
                              complete transactions + watermark
```

The decoder may observe row changes before it sees their commit record.
pg_flashback does not make them recoverable until the complete transaction is
known. The coverage watermark is therefore a COMMIT-LSN boundary, not an event
timestamp.

### Upgrading from a legacy trigger install

Older builds used DML capture triggers and `flashback.staging_events`. Before
reloading a WAL-only binary:

1. Flush or drain any non-empty staging with the previous binary
   (`flashback_flush_staging`), or unprotect/re-anchor after draining capture.
2. Do not manually `DELETE` staging rows.
3. Reload/upgrade; empty leftover staging and `flashback_capture_*` triggers
   are dropped automatically. Non-empty staging fails closed.

## Coverage generations

A local generation contains:

- the protected table identity and lifecycle;
- a transactionally aligned base image;
- the logical stream identity and timeline;
- the lower boundary;
- the greatest fully consumed COMMIT LSN;
- any durable gap or invalidation state.

Recovery is admitted only inside one generation's proven interval. There is no
nearest-snapshot fallback. Slot loss or unexplained advancement freezes the
last known frontier and creates a gap that cannot later be declared valid.

## DROP evidence

PostgreSQL catalogs no longer describe a table after it is dropped. The DDL
hook therefore records a pre-DROP manifest inside the original transaction.
The manifest is bound to the disaster event and includes the table identity,
schema metadata, sequence identity, and dependency information needed by the
planner.

If the dependency graph cannot be reconstructed safely, the plan is
`non_restorable`. Dry-run and execution use the same event-bound manifest.

## Recovery protocol

1. `flashback_recover_begin` validates the request and commits a durable
   operation header.
2. The planner selects one exact DROP and emits an expiring plan token.
3. `flashback_recover_execute` recomputes the plan and requires the same token
   and operation ID.
4. A shadow table is materialized from the base and WAL changes through the
   target COMMIT LSN.
5. Schema, data identity, dependencies, owner, ACL, and sequence state are
   validated.
6. The shadow relation is swapped into the original identity.
7. A successor generation is created and bound to that exact recovery.
8. The worker observes the real commit record; only then can the operation
   become `verified`.

A client-side failure is durably marked failed by the CLI. If the client
disappears, the reconciler can classify stale `started` operations as
abandoned.

## Locking

Canonical advisory-lock order for lifecycle paths:

1. Pre-identity lock (`358943`, `rel_oid`) — table registration pre-identity
2. Database / capture-stream lock (`358945`, `database_oid`) — database stream scope
3. Lifecycle locks (`358944`, `hashint8(tracking_id)`), sorted and DISTINCT in ascending order — tracking lifecycle scope
4. Relation / partition lock (`358946`, `rel_oid`) — table partition/relation scope
5. Physical payload FOR UPDATE locks

Helpers live in `flashback_internal_lock_*` (`sql/functions/state_authority.sql`).
The Rust capture worker keeps its existing **session** drain lock on namespace
`358945`; that must not be confused with transaction lifecycle locks.

## State and progress authority

Direct `SET state=` mutations for capture streams, coverage generations, and
payload retirements, as well as initial state `INSERT`s, go through narrow SECURITY DEFINER primitives in
`state_authority.sql`. Domain commands (for example
`flashback_mark_capture_stream_broken`) still own side effects such as durable
gaps and payload cleanup; they call the transition primitives inside the same
transaction. Trigger/CHECK guards remain the last line of defense.

Progress fields (`valid_through_*`, `confirmed_flush_lsn`, generation
watermarks) advance through separate helpers and cannot heal a broken stream
or an open coverage gap back to healthy.

Legal coverage generation edges (enforced by guard + authority):

```text
building   → active | aborted | capturing
capturing  → active | aborted
active     → sealed
sealed     → retired
```

`capturing` is the online `external_zstd` protect/maintain path's
intermediate state (`protect_online.sql`): the boundary marker has committed
and WAL is already being absorbed for it, but the generation is not yet a
recoverable boundary — that only happens once the background copier's
artifact is verified and publish/activate commits (`building`/`capturing` →
`active`). A crash or an explicit `protect-abort` while a generation is
`building` or `capturing` is reconciled to `aborted` rather than left
half-published; `heap_v1`'s synchronous CTAS path never enters `capturing`
and goes directly `building` → `active`.

Capture stream edges used by runtime:

```text
initializing → active | broken | retired
active       → broken | retired
broken       → retired
```

The operation journal keeps strictly immutable `operations` headers and append-only
`operation_events`. Restore proof, verification, and successor identity are written
to `operation_events` payloads. Terminal states (`verified`, `failed`, `abandoned`, `unprotected`, `cleaned`, `sealed`) refuse
further non-matching progress; retrying the exact same terminal payload is an idempotent no-op.

## Storage

The base image is stored through one of two SnapshotStore backends:

- **`heap_v1`** (default) — inside PostgreSQL as an ordinary heap table,
  created synchronously by `flashback_track()`'s CTAS under a brief lock.
- **`external_zstd`** (supported opt-in production backend) — streamed as a
  zstd-compressed artifact to `pg_flashback.external_snapshot_root`, an
  operator-provisioned directory (owner/mode `0700`) outside PGDATA and
  outside any tablespace, with explicit positive
  `external_snapshot_min_free_bytes` / `external_snapshot_safety_reserve_bytes`
  budgets. Created via the online, non-blocking `protect`/`maintain`
  orchestration (see "State and progress authority" above for the
  `capturing` generation state); admission never assumes unlimited external
  space. Row-batching and compression are tunable via
  `external_snapshot_batch_rows`, `external_snapshot_zstd_level`, and
  `external_snapshot_max_row_bytes`. A superseded generation's external
  artifact is retired (payload removed) only once no recovery path can still
  reference it and retention has elapsed.

Admission estimates, for either backend:

- heap and TOAST size (source table) or artifact size estimate (`external_zstd`);
- index rebuild requirements;
- simultaneous old/new relation peak during recovery;
- configured reserve;
- current filesystem free space (PGDATA's filesystem, or the external root's
  filesystem for `external_zstd`).

The estimate is a guard, not a reservation: tables and filesystems can change
after the check. Runtime limits and fail-closed behavior remain necessary.
`flashback_doctor()` and `flashback_health()` report the active backend,
external-root health, and each generation's `snapshot_payload_state`.

## Security boundary

The extension is superuser-installed. Public execution is revoked by default,
then explicit read-only and mutating functions are granted to dedicated roles.
Internal functions use fixed `search_path` values. Row-change payloads are
more sensitive than health metadata and are not exposed through the monitoring
role.

## Physical-backup recovery (deferred)

A prototype that materialized a table from an existing physical backup plus
archived WAL used to live here. It has been removed from the supported tree
and its redesign is deferred; see [deferred backup](DEFERRED_BACKUP.md). The
architecture above describes the local product in full.

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
| Recovery planner | Selects one disaster event and proves that it is recoverable |
| Recovery executor | Materializes, verifies, and atomically swaps a shadow table |
| Operation journal | Records started, failed, abandoned, applied, and verified operations |
| `pg_flashback` CLI | Presents the safe operator workflow without exposing LSNs by default |

## Capture path

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

Lifecycle operations share advisory-lock identities and acquire them in a
stable order. Recovery refuses to wait indefinitely for an unsafe write stall.
Capture and maintenance use separate workers so retention or checkpoint work
does not stop logical-slot draining.

## Storage

The active local path stores its base image and changes inside PostgreSQL.
Admission estimates:

- heap and TOAST size;
- index rebuild requirements;
- simultaneous old/new relation peak during recovery;
- configured reserve;
- current filesystem free space.

The estimate is a guard, not a reservation: tables and filesystems can change
after the check. Runtime limits and fail-closed behavior remain necessary.

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

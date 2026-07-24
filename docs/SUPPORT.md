# Support matrix

This document defines the current local DROP-recovery contract (`local_delta`
+ logical WAL). This is the only supported product path. Physical-backup
recovery is deferred and not part of the tree (see
[deferred backup](DEFERRED_BACKUP.md)).

Capacity is sized by **protected table size**, **change rate**, and **free
disk**, not by total database size. The three capacity GUCs
(`local_max_snapshot_bytes`, `local_max_restore_peak_bytes`,
`local_min_filesystem_bytes`) must be set explicitly; otherwise protect/restore
fail closed. Use `pg_flashback config recommend` for read-only advice.

## Platforms

| Area | Supported |
|---|---|
| PostgreSQL | 15, 16, 17, 18 |
| Operating system | Linux |
| Capture | Logical WAL only (`local_delta`); capture worker + logical slot required |
| Topology | Writable primary with one local logical slot per configured database |
| CLI dependencies | `psql`, `jq`, libpq connection settings |
| Operator role | Login role with `flashback_admin` (superuser only for install) |
| `wal_level` | `logical` (required) |
| `track_commit_timestamp` | Not required |
| `capture_mode` | Deprecated compatibility GUC; only `wal` is valid |
| DML capture triggers | Not installed on user tables |
| Ordinary (non-internal) triggers | Preserved through protect/restore |

Native macOS is not supported. Linux/aarch64 development under Lima and
Linux/x86_64 builds are separate environments; evidence from one architecture
is not silently generalized to the other.

## Tables

This matrix is generated from the machine-checked gate in
`sql/functions/local_compatibility.sql` (`flashback_local_compatibility` /
`flashback_local_compatibility_schema_def`). `flashback_track()` calls
`flashback_require_local_compatibility()` and refuses to protect a table
outside this contract; `flashback_recover_plan()` runs the same check against
the target schema epoch and refuses (`unsupported_schema_epoch`) if the
epoch being recovered to is outside it.

### Preserved (supported)

| Feature | Behavior |
|---|---|
| Ordinary columns | Preserved |
| Identity and serial columns | Preserved; original names restored |
| Primary key, `UNIQUE`, `CHECK` constraints | Preserved |
| Outgoing foreign keys | Preserved |
| Plain btree indexes | Preserved |
| Owner and table/column ACL | Preserved |
| TOAST / large values | Preserved |
| Replica identity | Preserved |
| Basic row-level security policies | Preserved |
| Ordinary (non-internal) triggers | Preserved |
| Comments | Preserved |
| Tablespace and storage `reloptions` | Preserved |
| Owned sequences | Preserved |
| Quoted names and non-`public` schemas | Supported |

### Rejected (fail closed)

| Feature | Behavior |
|---|---|
| Partitioned table or partition | Rejected |
| Classical inheritance (parent or child) | Rejected |
| Foreign table | Rejected |
| TEMP or `UNLOGGED` table | Rejected |
| Materialized view | Rejected |
| Extension-owned relation | Rejected |
| Exclusion constraints | Rejected |
| Rules | Rejected |
| Security labels | Rejected |
| Publications | Rejected |
| Incoming foreign keys | Rejected |
| Non-btree, expression, or partial indexes | Rejected |
| Generated columns | Rejected (reconstruction is not proven; rejected even for "simple" cases) |

## Incidents

| Incident | Behavior |
|---|---|
| Ordinary `DROP TABLE` | Recover latest proven pre-DROP state |
| Multiple historical DROP events | Latest safe DROP by default; explicit event selection available |
| Ambiguous or non-restorable latest DROP | Rejected; never falls back silently |
| Same-name table recreated after DROP | Rejected to prevent overwrite |
| `DROP ... CASCADE` | Planned from the pre-DROP manifest; rejected if any dependency is unsupported |
| `TRUNCATE` or destructive DML | Captured by the engine; advanced LSN recovery remains available |
| DDL across an unproven schema epoch | Rejected |
| Maintenance / reanchor | Opt-in via `pg_flashback maintain`; never silent auto-maintain |
| Uninstall | `pg_flashback prepare-uninstall` refuses active lifecycles/pending restores |

## Transaction semantics

- Only committed transactions become recoverable.
- A transaction that changes rows and drops the table atomically is recovered
  to the state before that entire transaction.
- Rollbacks and aborted subtransactions do not become history.
- Event ordering follows transaction COMMIT LSN, not wall-clock timestamps.
- Duplicate delivery is tolerated; incomplete transactions do not advance the
  coverage watermark.

## Failure and HA behavior

| Condition | Behavior |
|---|---|
| PostgreSQL restart | Worker resumes from the logical slot |
| Worker crash | Postmaster restarts it; health reports the interruption |
| Slot lost/replaced/externally advanced | Coverage freezes and a durable gap opens |
| Timeline mismatch or promotion without proven continuity | Recovery rejected; re-anchor required |
| Command run on a standby | Mutating operation rejected |
| Split brain and external fencing | Operator responsibility; not automated |

## Recovery guarantees

A supported recovery:

1. selects one exact disaster event and one complete WAL prefix;
2. recomputes the plan at execution time;
3. materializes a shadow relation;
4. validates data/schema identity and required metadata;
5. swaps atomically;
6. records a durable operation result;
7. waits for a correctly bound successor generation before reporting verified.

An error may leave a journal entry to reconcile, but must not be reported as a
successful verified recovery.

## Lifecycle

| Operation | Support |
|---|---|
| `protect` | Supported |
| `unprotect` | Supported two-phase stop |
| Re-protect before old cleanup | Supported as a new lifecycle |
| `cleanup --tracking-id` | Supported with dry-run and safety checks |
| Downgrade | Unsupported |
| Extension upgrade | `0.1.0` to `0.2.0` only |
| Upgrade from legacy trigger installs | Flush old `staging_events` (previous binary) or unprotect/drain, then upgrade; nonempty staging refuses WAL-only migration |
| PostgreSQL major `pg_upgrade` | Not yet a supported workflow |

## Physical-backup subsystem (deferred)

The physical-backup recovery prototype has been removed from the tree and its
redesign is deferred; see [deferred backup](DEFERRED_BACKUP.md). There is no
supported large-database or backup-provider claim.

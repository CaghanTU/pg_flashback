# Backup-backed recovery architecture

Status: implemented first-release contract (result format 3)

The backup profile is the large-database path. It does not copy the tracked
table into extension-owned snapshots and it does not duplicate row changes in
`delta_log`. PostgreSQL physical recovery reconstructs the database; the
helper extracts only the requested table; the extension validates and swaps
that table into production.

This is not a fork or reimplementation of pgBackRest. pgBackRest remains an
external executable and owns backup, archive and classic restore semantics.

```mermaid
sequenceDiagram
    participant O as Operator/controller
    participant E as pg_flashback extension
    participant H as Recovery helper
    participant R as pgBackRest repository
    participant T as Temporary PostgreSQL

    O->>E: prepare + claim immutable request
    O->>H: restore-table(config, request)
    H->>R: select and revalidate full backup under shared lock
    H->>R: reflink clone or classic restore
    H->>T: native LSN PITR
    H->>T: verify OID, schema, rows, owner and ACL
    H-->>O: custom dump + result manifest + SHA-256
    O->>E: import into flashback_import
    O->>E: accept manifest + finalize
    E->>E: verify imported schema/rows; transactional swap
```

## Recovery profiles

| Profile | Row snapshots | Row deltas | DDL markers | Recovery source |
|---|---:|---:|---:|---|
| `local_delta` | yes | yes | yes | in-database snapshot + replay |
| `backup` | no | no | yes | physical backup + archived WAL |

`flashback_track(text)` selects `local_delta`. The explicit
`flashback_track_backup(text, text)` API selects `backup` and binds the table to
an operator-owned helper profile.

## Trust boundary

The extension never starts an operating-system process. The helper never
connects to or mutates production. The reference controller is the narrow
bridge: it invokes the helper, checks the artifact digest, imports into the
restricted `flashback_import` schema and asks the extension to finalize.

The recovery service role is trusted. It can claim requests, submit manifests
and create import tables, but it cannot call the internal swap primitive
directly. The finalizer independently verifies all of the following:

- immutable request equality and configured helper profile;
- result format, completion and cleanup flags;
- deterministic import table name;
- recovered and imported structural schema hashes;
- recovered and imported row fingerprints;
- original table OID, including same-name replacement detection;
- recovered owner, grantee role existence and table privilege types.

## Helper commands

```text
pg-flashback-recovery probe --config helper.json
pg-flashback-recovery plan --config helper.json --request request.json
pg-flashback-recovery restore-table --config helper.json --request request.json
```

Successful output is JSON on stdout. Failures are JSON on stderr with a stable
`code`. Requests never contain executable paths, repository credentials or
arbitrary pgBackRest options; those are available only in the operator-owned
configuration.

## Planning and execution

1. Validate absolute, non-overlapping, non-symlink configuration roots.
2. Probe the exact pgBackRest and PostgreSQL tools and perform a real reflink
   test when snapshot-direct is requested.
3. Read `pgbackrest info --output=json` and select the newest completed full
   backup whose stop LSN is not later than the target.
4. Acquire the request lock and bind the request ID immutably to both request
   JSON and helper profile.
5. Acquire the single-execution work-root lock and reconcile any abandoned
   process group, PostgreSQL cluster or socket from a previous crash.
6. Acquire the shared external repository lock, re-read the backup catalog and
   confirm the selected label still exists.
7. Materialize a private cluster using either an XFS reflink clone or a normal
   `pgbackrest restore` fallback.
8. Keep the repository lock through promotion so archived WAL cannot be
   expired while recovery still needs it.
9. Start the matching PostgreSQL 15–18 binaries on a private mode-0700 Unix
   socket, with network listeners and preload libraries disabled and
   `data_directory` forced to the private clone.
10. Recover to the exact LSN, verify the original relation OID and ordinary
    table kind, calculate structural SHA-256 and a generic row fingerprint,
    and read target-time owner/ACL metadata.
11. Move the table inside the temporary cluster to the deterministic
    `flashback_import.r_<request hash>` name and create a custom-format dump.
12. Stop PostgreSQL, remove pgdata/socket data, fsync the durable result and
    return only after cleanup is complete.

The artifact remains below `work_root/<request_id>/target-table.dump` so an
identical retry can return the checksum-verified cached result. A different
request or profile using the same ID fails with `request_conflict`.

## Crash and cancellation model

Every child command runs in its own process group. `SIGINT`, `SIGTERM` and
command timeouts terminate the group, stop temporary PostgreSQL and remove
materialized data. Before spawning a child, the helper durably records its
process group and Linux `/proc` start ticks; this prevents PID-reuse mistakes
during crash reconciliation. A hard-killed helper is reconciled by the next
execution under the global lock.

State/result writes use fsync plus atomic rename. Work, socket and contract
directories are mode 0700; files are mode 0600; symlink runtime paths and
parent traversal are rejected.

## Repository coordination

pgBackRest's own locks do not cover a direct filesystem clone. Scheduled
`backup` and `expire` operations for the recovery repository must therefore
take the exclusive side of `expire_lock_path`. The helper takes the shared
side. `scripts/pgbackrest_with_flashback_lock.sh` is the reference wrapper.

The same-stanza deployment is preferred: keep the normal long-retention
repository, and add a local short-retention repository key configured without
compression, bundle or block storage for snapshot-direct. Classic restore is
the safe fallback when the selected full backup is not a directly startable
plain tree.

## Result contract (format 3)

The result includes the immutable request, profile, engine, selected backup,
tool versions, row count, structural hashes, row fingerprint, target-time
owner/ACL, deterministic artifact identity and SHA-256, phase durations and
`cleanup_complete=true`.

Result format changes are explicit. The extension currently requires format
3 or newer; the helper only reuses cached results from its exact current
format and version.

## Deliberate first-release limits

- LSN targets and completed full backups only.
- Local POSIX repositories; snapshot-direct requires an actual XFS reflink
  probe and a plain directly startable backup tree.
- Ordinary tables only; no partitions, foreign tables, materialized views,
  unlogged tables, tablespaces or symlinked relation storage.
- The schema/name and OID at the target must match the tracked identity.
  RENAME or SET SCHEMA across the recovery point is not supported yet.
- When a table was dropped, objects owned by other relations—such as incoming
  foreign keys and dependent views—are not present in a table-only dump and
  are not recreated by the backup profile. Recover related objects manually
  or use cluster PITR.
- Time/XID targets, differential/incremental closure and remote/object-store
  snapshot-direct are future work.

The authoritative operator procedure and recovery steps are in
[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md).

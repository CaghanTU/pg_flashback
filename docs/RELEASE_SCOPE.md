# First backup-backed release scope

This document is the fail-closed support contract for the first publishable
backup-backed table-recovery release. A topology not listed here is rejected;
it is never attempted optimistically.

## Supported

- Linux x86_64 or aarch64 source builds; tagged prebuilt archives are x86_64
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

## Rejected in the first release

- differential or incremental backup selection
- tablespaces/symlinked relation storage
- symlinked PostgreSQL configuration files in the recovered cluster
- time and transaction-ID targets
- remote/object-store direct snapshot access
- importing or swapping the artifact into production without extension-side
  schema and fingerprint validation
- an expire schedule that does not honor the configured external lock
- partitioned, foreign, materialized-view or unlogged targets
- a table renamed or moved to another schema across the requested point
- tables dropped with incoming foreign keys or dependent views that must be
  recreated automatically
- HA/failover, managed-service and cross-major recovery topologies

## Safety invariants

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

- unit tests, fmt and clippy are clean;
- snapshot-direct and classic restore E2E paths recover identical content;
- old target, missing WAL, repository lock race, cancellation, PostgreSQL start
  failure and stale-request cleanup are exercised;
- no PostgreSQL process, socket or materialized cluster remains after any E2E
  scenario;
- extension PostgreSQL 15–18 regression matrix remains green;
- real DROP-marker request, reference-controller import, owner/ACL restore and
  production shadow swap are exercised;
- a real ALTER recovers the pre-ALTER schema from a marker proven earlier than
  the post-ALTER schema-version LSN;
- third-party notices, user guide, limitations and release checklist are
  complete.

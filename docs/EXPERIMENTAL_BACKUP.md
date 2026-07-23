# Experimental backup-backed recovery

This subsystem explores recovery of a table from an existing physical backup
and archived WAL. It is separate from the supported local DROP-recovery
product.

## Current provider

The only implemented provider is pgBackRest. pg_flashback does not take a
backup automatically. The PostgreSQL operator remains responsible for:

- configuring the pgBackRest repository and archive command;
- scheduling and monitoring FULL backups;
- retaining required backup and WAL dependencies;
- routing supported expiration through the coordination lock.

The helper can discover and verify an eligible FULL, validate system identity,
timeline, manifest checksums, and continuous WAL, start a private PostgreSQL
recovery cluster, extract one table, and hand a checksum-protected artifact to
the extension's swap controller.

## Why it is experimental

The code is deliberately not part of the local support contract because:

- pgBackRest is the only implemented provider;
- differential and incremental dependency chains are not supported;
- replay time depends on backup age and archived WAL volume;
- external repository deletion cannot be prevented by the extension alone;
- the operating model requires additional OS roles, locks, storage, and
  failure handling.

No general “large databases are solved” claim follows from this subsystem.

## Components

- `tools/pg_flashback_recovery/` — Rust recovery helper
- `scripts/pg_flashback_backup_restore.sh` — reference controller
- `scripts/pgbackrest_with_flashback_lock.sh` — coordinated repository wrapper
- `deploy/pg-flashback-reconcile-anchors.*` — optional anchor reconciliation
- backup-specific E2E and PoC scripts under `scripts/`

The internal `PhysicalRecoveryProvider` interface is intentionally unstable.
It may change when a second provider is implemented.

## Development

Build the helper:

```bash
cargo build --release --locked \
  --manifest-path tools/pg_flashback_recovery/Cargo.toml
```

Inspect the command surface:

```bash
tools/pg_flashback_recovery/target/release/pg-flashback-recovery --help
```

Use only disposable repositories and clusters when running the experimental
E2E suites. This document is not an operator runbook for production.

# pg-flashback-recovery

External recovery executor for pg_flashback's `backup` profile. It selects a
pgBackRest full backup, materializes a private PostgreSQL cluster, performs
native LSN PITR, validates one ordinary table and emits a checksum-protected
custom-format dump. It never connects to production and never performs the
final table swap.

## Build

```bash
cargo build --release --locked \
  --manifest-path tools/pg_flashback_recovery/Cargo.toml
```

Rust 1.85 or newer is required by the locked dependency set.

## Commands

```bash
pg-flashback-recovery --version
pg-flashback-recovery probe --config helper.json
pg-flashback-recovery plan --config helper.json --request request.json
pg-flashback-recovery restore-table --config helper.json --request request.json
pg-flashback-recovery gc --config helper.json --dry-run
pg-flashback-recovery gc --config helper.json
pg-flashback-recovery unpin --config helper.json --request-id <id>
```

All success output is JSON on stdout. Errors are JSON on stderr:

```json
{"status":"error","code":"repository_busy","message":"..."}
```

See the checked-in [example configuration](examples/helper.json), the
[architecture contract](../../docs/RECOVERY_HELPER_DESIGN.md), and the
[operator runbook](../../docs/BACKUP_RESTORE_RUNBOOK.md).

## Service rules

- Run as a dedicated non-root account that may start the temporary PostgreSQL
  cluster; normally this is the PostgreSQL OS account.
- Make `work_root`, `socket_root`, the lock parent and helper config private to
  that account.
- The config must be a regular non-symlink file and must not be writable by its
  group or other users; the CLI rejects an unsafe file before parsing it.
- Use the exact PostgreSQL major binaries matching the physical backup.
- Route repository backup/expire jobs through the exclusive lock wrapper.
- Keep secrets only in pgBackRest configuration/credential providers, never in
  request JSON.
- Treat `request_id` as an immutable idempotency key bound to one profile.

First-release support and rejected topologies are defined in
[`docs/RELEASE_SCOPE.md`](../../docs/RELEASE_SCOPE.md).

# pg_flashback

[![CI](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml/badge.svg)](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Recover an accidentally dropped PostgreSQL table without restoring the whole
database.

pg_flashback keeps a protected base image of a table, captures later changes
from logical WAL, detects `DROP TABLE`, and reconstructs the last proven state
before the DROP. The normal recovery command does not ask the operator for an
LSN or a timestamp.

```console
$ pg_flashback protect public.orders
Protection enabled for public.orders.

# Later: DROP TABLE public.orders;

$ pg_flashback recover public.orders
DROP found for public.orders.
Recovery point: immediately before the DROP
Status: recoverable
Recover public.orders? [y/N]
```

> pg_flashback is under active development. The primary product is local
> recovery (`local_delta` + WAL) for small and medium ordinary PostgreSQL
> tables. Capacity is sized by protected table size, change rate, and free
> disk — not by whole-database size. Physical-backup recovery is deferred and
> not part of the supported product. Test in staging before
> production use.

## Why pg_flashback?

A normal point-in-time recovery restores an entire PostgreSQL cluster to a
separate location, replays WAL, and then extracts the missing table.
pg_flashback provides a narrower recovery path for a common incident:

- protect selected tables rather than restore the whole cluster;
- recover the latest safe point before an accidental DROP;
- restore data, table definition, indexes, constraints, identity sequences,
  owner, and ACLs;
- refuse recovery when the available evidence is incomplete or ambiguous.

Local protection does not require any external backup product. It does consume
additional database storage for the base image and retained changes.

## Supported scope

The local recovery path supports:

- PostgreSQL 15, 16, 17, and 18;
- ordinary permanent `LOGGED` tables;
- primary and secondary indexes, named/deferrable primary keys, unique and
  check constraints;
- qualified column collations;
- identity/serial sequences, TOAST values, quoted identifiers, and
  non-`public` schemas;
- `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and ordinary `DROP TABLE`;
- PostgreSQL restart and background-worker restart without losing confirmed
  WAL coverage.

pg_flashback rejects unsupported or unproven cases instead of guessing. The
current local product does not support partitioned, foreign, temporary,
`UNLOGGED`, or materialized-view targets. Recovery is also rejected across a
lost logical slot, a coverage gap, an ambiguous DROP, an identity conflict, or
unsupported `DROP ... CASCADE` dependencies.

See [the support matrix](docs/SUPPORT.md) for the precise contract.

## Requirements

- A supported PostgreSQL server and matching development/package files
- `wal_level = logical` (required; capture is WAL-only)
- `shared_preload_libraries = 'pg_flashback'`
- An admitted capture worker and a logical replication slot per configured
  database (`pg_flashback.target_databases`)
- Explicit capacity budgets:
  `pg_flashback.local_max_snapshot_bytes`,
  `pg_flashback.local_max_restore_peak_bytes`,
  `pg_flashback.local_min_filesystem_bytes`
- Enough `max_worker_processes` capacity for one capture worker and one
  maintenance worker per configured database
- `psql` and `jq` for the `pg_flashback` command

`track_commit_timestamp` is **not** required. `pg_flashback.capture_mode` is a
deprecated compatibility GUC; only `wal` is valid (`trigger` and `auto` fail
closed). pg_flashback does **not** install DML capture triggers on user tables;
ordinary user triggers are preserved through protect/restore.

See [Quickstart](docs/QUICKSTART.md) and
[`docs/samples/postgresql.pg_flashback.conf`](docs/samples/postgresql.pg_flashback.conf).
Use `pg_flashback config recommend` for read-only conf line suggestions.
Day-to-day operations should use a login role granted `flashback_admin`
(`pgfb_operator` in the Quickstart), not a standing superuser session.

## Installation

### From a release archive

Use the archive matching the PostgreSQL major version reported by
`pg_config`.

```bash
test "$(cat PG_MAJOR)" = \
  "$(pg_config --version | awk '{print $2}' | cut -d. -f1)"

sudo install -m 0755 lib/pg_flashback.so \
  "$(pg_config --pkglibdir)/pg_flashback.so"
sudo install -m 0644 share/extension/pg_flashback.control \
  share/extension/pg_flashback--*.sql \
  "$(pg_config --sharedir)/extension/"
sudo install -m 0755 bin/pg_flashback /usr/local/bin/pg_flashback
```

### From source

Install Rust 1.85 or newer and `cargo-pgrx` 0.16.1, then initialize the target
PostgreSQL installation once:

```bash
cargo install --locked cargo-pgrx --version 0.16.1
cargo pgrx init --pg17 /path/to/pg_config
cargo pgrx install --no-default-features --features pg17
sudo install -m 0755 scripts/pg_flashback /usr/local/bin/pg_flashback
```

Replace `pg17` with `pg15`, `pg16`, or `pg18` as appropriate.

## Configuration

Add the extension and protected databases to `postgresql.conf`:

```conf
shared_preload_libraries = 'pg_flashback'
wal_level = logical

pg_flashback.target_databases = 'appdb'
pg_flashback.max_workers = 4
```

Each configured database uses two background-worker slots. Restart PostgreSQL
after changing these startup settings, then install the SQL extension in every
configured database:

```bash
psql -d appdb -c "CREATE EXTENSION pg_flashback;"
pg_flashback doctor
```

`doctor` reports configuration, worker, slot, storage, and coverage problems
with a non-zero exit status.

## Protect and recover a table

Create or choose an ordinary table:

```sql
CREATE TABLE public.orders (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    customer text NOT NULL,
    total numeric(12,2) NOT NULL
);
```

Enable protection and wait for healthy coverage:

```bash
export PGDATABASE=appdb
pg_flashback protect public.orders
pg_flashback status public.orders
```

Before protection starts, pg_flashback checks table topology, worker
availability, write-stall limits, configured storage budgets, and filesystem
free space. The initial base image is stored inside PostgreSQL.

After an accidental DROP:

```sql
DROP TABLE public.orders;
```

Preview the recovery without changing the database:

```bash
pg_flashback recover public.orders --dry-run
```

Recover interactively, or explicitly confirm a non-interactive operation:

```bash
pg_flashback recover public.orders
pg_flashback recover public.orders --latest-drop --yes
```

Recovery is journaled. A successful operation is not reported as verified
until the reconstructed table and its successor coverage have passed the
post-recovery checks.

## Operator commands

```text
pg_flashback version
pg_flashback doctor [--reconcile] [--all-databases]
pg_flashback protect TABLE
pg_flashback list
pg_flashback status [TABLE] [--all-databases]
pg_flashback history [TABLE]
pg_flashback changes [TABLE]
pg_flashback recover TABLE [--dry-run] [--latest-drop] [--yes]
pg_flashback unprotect TABLE --yes
pg_flashback cleanup --tracking-id ID [--dry-run] --yes
```

Global `--json` and `--verbose` flags may appear before or after the command.
The CLI uses normal libpq environment variables and `.pgpass`; it does not
store passwords.

`history` shows lifecycle and recovery operations. `changes` can expose old
and new row values and therefore has a stricter authorization boundary.

## How recovery works

1. `protect` takes a transactionally aligned base image and creates an active
   coverage generation.
2. A logical decoding slot observes committed row and DDL changes.
3. The capture worker records only complete transactions and advances a proven
   COMMIT-LSN watermark.
4. The DDL hook records the DROP and its dependency manifest inside the
   original transaction.
5. `recover --dry-run` selects the latest unambiguous DROP and builds a
   recovery plan.
6. `recover` recomputes that plan, materializes a shadow table, verifies it,
   swaps it into place, and creates a new coverage generation.

If the slot is lost, WAL continuity cannot be proven, the schema is
unsupported, or a newer same-name relation exists, recovery stops without
modifying the live database.

See [architecture](docs/ARCHITECTURE.md) for the invariants behind this model.

## Storage and retention

Local protection trades storage for fast, table-level recovery. Space usage is
primarily:

- one base image per active protected lifecycle;
- retained WAL-derived row changes;
- temporary peak space while a recovery is materialized.

Use these commands to inspect the current state:

```bash
pg_flashback status
pg_flashback doctor
psql -c "SELECT * FROM flashback_advise('public.orders'::regclass);"
psql -c "SELECT * FROM flashback_retention_status();"
```

`unprotect` first closes the WAL boundary; `cleanup` removes an inactive
lifecycle only when its recovery evidence is no longer needed. Cleanup is
explicit and supports a dry run.

## Physical-backup recovery (deferred)

An experimental physical-backup recovery prototype (for larger tables, via an
existing backup plus archived WAL) used to live here. It has been removed from
the supported tree and its redesign is deferred; the supported product is
local DROP recovery only. See [deferred backup](docs/DEFERRED_BACKUP.md).

## Documentation

- [Quickstart](docs/QUICKSTART.md)
- [Support matrix](docs/SUPPORT.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Testing and development](docs/DEVELOPMENT.md)
- [Deferred backup recovery](docs/DEFERRED_BACKUP.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

## Development

```bash
cargo fmt --all -- --check
cargo clippy --no-default-features --features pg17 -- -D warnings
cargo pgrx test pg17
```

The full PostgreSQL 15–18 matrix, WAL recovery suites, destructive DROP tests,
upgrade tests, package smoke tests, and longer stability tests are documented
in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md). Generated logs and qualification
artifacts belong under `target/`, not in the source tree.

## Security

The extension is installed by a PostgreSQL superuser. Mutating recovery
operations require dedicated roles and use deny-by-default grants. Snapshot and
change data must be treated with the same sensitivity as the protected table.

Report vulnerabilities through GitHub private vulnerability reporting. See
[SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

# pg_flashback

[![CI](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml/badge.svg)](https://github.com/CaghanTU/pg_flashback/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Recover an accidentally dropped PostgreSQL table without restoring the whole
cluster.

```console
$ pg_flashback protect public.orders
Protection enabled for public.orders.

# Later: DROP TABLE public.orders;

$ pg_flashback recover public.orders --dry-run
DROP found for public.orders.
Recovery point: immediately before the DROP
Status: recoverable

$ pg_flashback recover public.orders --yes
Recovery verified.
```

pg_flashback keeps a transactionally aligned base image for each protected
table, captures later committed changes from logical WAL, records the exact
`DROP TABLE` event, and reconstructs the last proven state before that DROP.
The operator does not need to choose an LSN.

## Project status

**Experimental technical preview. Test it in staging; do not treat it as your
only backup.**

The current code is a real, working table-level DROP recovery engine, but it
has an intentionally narrow support contract and a high recovery cost. The
qualified envelope for the public preview is:

- PostgreSQL 15, 16, 17, and 18 on Linux;
- one protected ordinary, permanent, logged table;
- full-table recovery after an ordinary `DROP TABLE`;
- table sizes up to 10 GiB in the measured mixed-row and TOAST-heavy shapes;
- exact-WAL capture with fail-closed coverage and post-recovery verification.

The 10 GiB qualification on commit `7b77476` completed with 115 checks and no
failures. It included concurrent writes, DROP discovery, full logical data
fingerprints, schema/metadata checks, artifact integrity, and successor
coverage.

| 10 GiB data shape | Protect | Recover | Compressed base image |
|---|---:|---:|---:|
| Mixed rows | about 14 minutes | about 102 minutes | about 1.1 GiB (8.9x) |
| TOAST-heavy | about 52 minutes | about 52 minutes | about 4.0 GiB (2.6x) |

These are measurements from one qualification host, not universal performance
promises. PostgreSQL configuration, storage, row width, TOAST behavior, change
rate, and indexes materially change the result.

Not yet claimed:

- 25 GiB or 50 GiB support;
- a final 24-hour soak on this candidate;
- live-table PITR as a public workflow;
- multi-table atomic recovery;
- row-level `DELETE`/`UPDATE` undo. The engine captures those changes and the
  privileged `changes` command can inspect them, but safe partial-row recovery
  is not productized.

See [the support matrix](docs/SUPPORT.md) for the exact accepted and rejected
schema features and [qualification](docs/QUALIFICATION.md) for the measured
envelope and its limitations.

## How this differs from pgBackRest

pgBackRest and pg_flashback both combine a base copy with WAL, but they solve
different recovery problems.

**pgBackRest is physical disaster recovery.** It backs up PostgreSQL data
files for the cluster and archives WAL. To recover one dropped table, the
usual procedure is to restore a backup into a separate PostgreSQL instance,
replay the cluster to just before the DROP, export the table, and import it
back into production. pgBackRest is mature, supports incremental/differential
backups, retention, remote repositories, and large databases.

**pg_flashback is selective operational recovery.** It protects chosen tables,
stores their schema and table-level base image, decodes later row changes, and
rebuilds only the dropped table. It avoids restoring and running a second copy
of the whole cluster, but pays for that granularity with per-table capture,
metadata reconstruction, verification, and substantial temporary disk use.

pg_flashback therefore **does not replace pgBackRest**. A sensible deployment
uses pgBackRest (or another proven backup system) for disaster recovery and,
if its measured cost is acceptable, pg_flashback as an additional fast-path
for a narrow class of accidental table drops.

## The cost, honestly

Normal protection with the `external_zstd` SnapshotStore held roughly
1.1–4.0 GiB of compressed base-image data for the measured 10 GiB tables,
plus retained WAL-derived changes.

Recovery is more expensive. The current path temporarily materializes the
restored table, builds indexes and metadata, creates a successor snapshot, and
verifies the result. After the streaming proof improvement, the measured
10 GiB mixed recovery still used roughly:

- 10 GiB for the reconstructed table;
- about 9.5 GiB for the current `heap_v1` successor snapshot;
- about 9.5 GiB of temporary work at peak;
- the existing compressed base image, retained changes/WAL, indexes, and
  safety headroom.

Plan for the capacity recommendation from `pg_flashback config recommend`,
not just the source table size. Protection and recovery fail closed when the
configured budgets or filesystem space are insufficient.

## Requirements

- PostgreSQL 15–18 on Linux
- Rust 1.85+ and `cargo-pgrx` 0.16.1 when building from source
- `wal_level = logical`
- `shared_preload_libraries = 'pg_flashback'`
- `psql` and `jq` for the operator CLI
- one logical replication slot and capture/maintenance worker capacity per
  configured database
- explicit snapshot, restore-peak, and minimum-free-space budgets

Current security-patched PostgreSQL minors also require `pg_flashback` in the
`output_plugin_libraries` list. Do not overwrite an existing allowlist;
`pg_flashback config recommend` prints a merged recommendation.

`track_commit_timestamp` is not required. DML capture is WAL-only; the
extension does not install capture triggers on user tables.

## Build and install

This preview is currently distributed from source. Initialize pgrx with the
matching PostgreSQL installation and install the extension:

```bash
cargo install --locked cargo-pgrx --version 0.16.1
cargo pgrx init --pg17 /path/to/pg_config
cargo pgrx install --no-default-features --features pg17
sudo install -m 0755 scripts/pg_flashback /usr/local/bin/pg_flashback
```

Replace `pg17` with `pg15`, `pg16`, or `pg18` as appropriate.

Add a minimal configuration for the target database:

```conf
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_worker_processes = 16
max_replication_slots = 8
max_wal_senders = 8

pg_flashback.enabled = on
pg_flashback.target_databases = 'appdb'
pg_flashback.max_workers = 4

# Mandatory admission budgets; size these for your tables and host.
pg_flashback.local_max_snapshot_bytes = '16GB'
pg_flashback.local_max_restore_peak_bytes = '40GB'
pg_flashback.local_min_filesystem_bytes = '45GB'
pg_flashback.local_safety_reserve_bytes = '1GB'
```

The values above are examples, not sizing advice. See the complete
[sample configuration](docs/samples/postgresql.pg_flashback.conf), restart
PostgreSQL, then install the SQL extension:

```bash
export PGDATABASE=appdb
psql -c 'CREATE EXTENSION pg_flashback;'
pg_flashback config recommend
pg_flashback doctor
```

Do not proceed until `doctor` reports that the workers, slot, storage root,
and capacity settings are healthy.

## Protect and recover

Use a dedicated login role granted `flashback_admin` for normal operations;
reserve superuser access for installation and configuration.

```bash
export PGDATABASE=appdb

pg_flashback protect public.orders
pg_flashback status public.orders

# After an accidental DROP:
pg_flashback recover public.orders --dry-run
pg_flashback recover public.orders --yes
```

The dry run identifies the exact DROP, recovery boundary, dependencies, and
coverage status. Execution recomputes the plan, restores a shadow relation,
verifies the live result against an independently derived proof, swaps it into
place, and establishes successor coverage before reporting success.

Useful operator commands:

```text
pg_flashback version
pg_flashback config recommend [TABLE]
pg_flashback doctor [--reconcile] [--all-databases]
pg_flashback protect TABLE
pg_flashback protect-abort OPERATION_ID --yes
pg_flashback list
pg_flashback status [TABLE]
pg_flashback history [TABLE]
pg_flashback changes [TABLE]
pg_flashback recover TABLE [--dry-run] [--latest-drop] [--yes]
pg_flashback maintain TABLE [--dry-run|--yes]
pg_flashback unprotect TABLE --yes
pg_flashback cleanup --tracking-id ID [--dry-run] --yes
```

Global `--json` and `--verbose` flags may appear before or after the command.
The CLI uses normal libpq environment variables and `.pgpass`; it does not
store passwords.

## Snapshot storage

- `heap_v1` is the conservative default. It stores the base image inside
  PostgreSQL as another heap table.
- `external_zstd` is an opt-in local/attached-filesystem backend. It streams
  a compressed artifact outside PGDATA, verifies it before activation, and
  supports resumable/reconciled online protection. It is the backend used for
  the measured 10 GiB protection runs.

`external_zstd` is not an object-store backup and is not a substitute for an
off-host backup. Its root must be outside PGDATA and all tablespaces, owned by
the PostgreSQL server user, mode `0700`, and protected by explicit free-space
and reserve budgets. See [Quickstart](docs/QUICKSTART.md) for configuration.

## Supported and rejected cases

The supported target is an ordinary permanent `LOGGED` table. The proven
contract includes ordinary columns, primary/unique/check constraints, outgoing
foreign keys, plain btree indexes, identity/serial sequences, table owner and
ACL, basic RLS policies, comments, ordinary triggers, quoted identifiers, and
non-`public` schemas.

The compatibility gate rejects unproven structures before protection or
recovery. Examples include partitioned/inherited/foreign/temporary/unlogged
tables, incoming foreign keys, generated columns, exclusion constraints,
rules, publications, column-level ACLs, non-btree/expression/partial indexes,
custom tablespaces, and unsupported `DROP ... CASCADE` dependency graphs.

If WAL continuity is lost, a row exceeds the configured decoder limit, the
DROP identity is ambiguous, or metadata cannot be proven, coverage opens a
durable gap and recovery is refused rather than guessed.

## Architecture in one paragraph

Protection creates a transactionally aligned base image and coverage
generation. A logical output plugin and background worker record complete
committed row/DDL changes and advance a proven COMMIT-LSN watermark. A DDL hook
binds the DROP event to its pre-DROP dependency manifest. Recovery materializes
a shadow table from the base plus its exact WAL prefix, rebuilds metadata,
compares independent expected and actual fingerprints, atomically swaps the
table, and records the result in an append-only operation journal.

Read [Architecture](docs/ARCHITECTURE.md) for the invariants and
[Support](docs/SUPPORT.md) for the precise contract.

## Development

Fast local checks:

```bash
cargo fmt --all -- --check
cargo clippy --no-default-features --features pg17 -- -D warnings
RUST_TEST_THREADS=1 cargo pgrx test pg17
python3 scripts/check_integration_inventory.py
```

The full matrix covers PostgreSQL 15–18, generated-SQL surface checks,
exact-WAL/DROP adversarial suites, package installation, crash/restart paths,
RBAC, and SnapshotStore failpoints. See
[Development](docs/DEVELOPMENT.md) before running destructive or long-lived
qualification scripts.

## License and security

MIT licensed. See [SECURITY.md](SECURITY.md) for private vulnerability
reporting. Snapshot and WAL-derived payloads contain user data and must be
protected with the same controls as the source database.

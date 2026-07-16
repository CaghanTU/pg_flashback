# Large database restore PoC

## Research question

Can pg_flashback recover one table from an existing pgBackRest backup without
first copying the entire PostgreSQL cluster, while keeping PostgreSQL itself in
charge of WAL replay and row correctness?

This PoC does **not** change the extension or claim that restore is trade-off
free. It measures whether an immutable, plain-format pgBackRest backup can be
cloned with filesystem copy-on-write and started as a temporary recovery
instance faster than a traditional pgBackRest restore.

## Host capability gate

The current host provides:

- XFS with `reflink=1` (verified with shared extents)
- pgBackRest 2.53.1 with `repo-hardlink` support
- PostgreSQL 17.3
- no LVM, ZFS, or Btrfs snapshot layer

Therefore the snapshot candidate on this host is an XFS reflink clone of an
immutable pgBackRest backup set. The PoC repository deliberately uses:

```ini
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
compress-type=none
```

The existing `/var/lib/pgbackrest` repository is not used because it has block
incremental and bundling enabled and is not a directly startable cluster tree.

## Compared paths

1. **Classic restore**
   - `pgbackrest restore` copies the full backup into a new data directory.
   - PostgreSQL replays archived WAL to the target time.
   - `pg_dump` extracts only the requested table.
2. **Repository snapshot-direct**
   - XFS reflink clones the backup set's `pg_data` directory.
   - Recovery settings and `recovery.signal` are added to the clone.
   - PostgreSQL replays the same WAL to the same target time.
   - `pg_dump` extracts the same table.

Both paths use PostgreSQL native recovery. No custom WAL redo implementation is
part of this experiment.

## Dataset and failure timeline

- An isolated PG17 cluster is created under `target/large-db-poc/`.
- Random, non-compressible rows are split between a target table (20%) and a
  noise table (80%).
- A full plain-format pgBackRest backup is taken.
- The target table is updated and a sentinel row is committed.
- The PITR target time is recorded.
- The target table is dropped later and WAL is archived.
- Both recovery paths must reconstruct the table at the recorded target.

The committed benchmark sizes are 500 MiB, 1 GiB, and at most 2-3 GiB. A 64 MiB
smoke run is used while developing the harness. Results at these sizes establish
functionality and a scaling curve; they do not by themselves prove 100 GiB
performance.

## Measurements

- backup time and repository size
- classic restore copy time
- reflink clone time
- filesystem blocks allocated by each path
- PostgreSQL WAL recovery time
- target-table extraction time
- total RTO: materialization + recovery + extraction
- recovered row count/fingerprint and sentinel presence

## Acceptance criteria

Correctness is mandatory:

- both paths reach the requested PITR target and promote cleanly;
- the recovered fingerprint matches the pre-DROP fingerprint;
- the sentinel row exists;
- the target table dump completes.

The snapshot path is considered useful when:

- clone-time allocated bytes are materially smaller than cluster size;
- clone time does not scale like a full data copy;
- total RTO is lower than classic restore for the same backup and target;
- repository format and storage trade-offs are reported explicitly.

If these conditions fail, the result is still useful: it closes the
snapshot-direct path on this host before pg_flashback integration work begins.

## Running

```bash
# Fast functional proof
./scripts/run_large_db_restore_poc.sh 64

# Measurement sizes (run sequentially)
./scripts/run_large_db_restore_poc.sh 500
./scripts/run_large_db_restore_poc.sh 1024
./scripts/run_large_db_restore_poc.sh 2048

# Busy-cluster sensitivity: small target table, 50% unrelated row churn
PGFB_POC_TARGET_PERCENT=5 PGFB_POC_CHURN_PERCENT=50 \
  ./scripts/run_large_db_restore_poc.sh 1024
```

Generated clusters, backups, and metrics stay under the ignored `target/`
directory. Successful runs remove bulky working data by default after saving
their result JSON. Set `PGFB_POC_KEEP=1` to preserve a successful run for
inspection.

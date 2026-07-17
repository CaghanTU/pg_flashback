# Development qualification harnesses

These bounded scripts create an isolated temporary PostgreSQL cluster and
write machine-readable summaries to `target/qualification/` by default. Install
the current extension for the selected PostgreSQL build first:

```bash
cargo pgrx install --pg-config /usr/local/pgsql-17/bin/pg_config
```

Run the short development forms:

```bash
./scripts/run_dev_soak.sh
./scripts/run_fault_injection_smoke.sh
./scripts/measure_wal_overhead.sh
```

`run_dev_soak.sh` defaults to 90 seconds. Set `PG_FLASHBACK_SOAK_SECONDS` for
a longer bounded run (for example, `1800`), and
`PG_FLASHBACK_SOAK_TARGET_MIB` (default `2048`) to change its cyclic workload
target. It inserts, updates, and deletes each cycle, so the cluster's retained
table data stays bounded while the workload produces cumulative WAL.

`run_fault_injection_smoke.sh` exercises a postmaster restart and a fast
shutdown/restart recovery path. Missing-slot/discontinuity coverage remains in
the broader WAL E2E suite. `measure_wal_overhead.sh` compares a small
update workload with default replica identity, `REPLICA IDENTITY FULL`, and
tracked capture. Its latency and WAL values are development measurements, not
production capacity claims.

These are development-qualification harnesses only. The exact-RC 24-hour soak
and its release evidence are Milestone 6 work and were not completed overnight.

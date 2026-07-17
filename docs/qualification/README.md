# Development qualification harnesses

These bounded scripts create an isolated temporary PostgreSQL cluster and
write machine-readable summaries to `target/qualification/` by default. Install
the current extension for the selected PostgreSQL build first:

```bash
cargo pgrx install --pg-config /usr/local/pgsql-17/bin/pg_config
```

Run the short development forms:

```bash
HOLD_SECONDS=10 ./scripts/run_capture_maintenance_isolation_slo.sh
PG_FLASHBACK_SOAK_SECONDS=20 PG_FLASHBACK_SOAK_TARGET_MIB=64 ./scripts/run_dev_soak.sh
./scripts/run_fault_injection_smoke.sh
./scripts/measure_wal_overhead.sh
```

`run_capture_maintenance_isolation_slo.sh` is the M3 isolation measurement.
It holds the locked table's lifecycle advisory lock while committing at least
200 individual transactions to the unblocked table. Capture and maintenance
still share one background-worker loop (including its cadence and timeouts);
this test demonstrates capture progress despite a blocked unrelated lifecycle,
not separate workers. It records monotonic commit-ack to first
`flashback.delta_log` visibility at 20 ms polling resolution and reports
p50/p95/p99/max milliseconds. PASS requires p95 below 2000 ms, max below
5000 ms, capture progress during the hold, and bounded slot-lag drain after
the hold. Its latest written PASS/FAIL/PARTIAL report is copied to
`docs/qualification/capture-maintenance-isolation-latest.json`.

`run_dev_soak.sh` is a bounded **development soak**, not a 24-hour or
exact-RC qualification. It defaults to 90 seconds. Set
`PG_FLASHBACK_SOAK_SECONDS` and `PG_FLASHBACK_SOAK_TARGET_MIB` for a bounded
run. It inserts, updates, and deletes each cycle, then must drain capture
within a bounded deadline: decoded INSERT/UPDATE/DELETE event counts must
match the expected counts exactly, every inserted id must appear in
`delta_log`, and `confirmed_flush_lsn` must reach the table's max captured
commit LSN. Global slot lag may remain elevated when filtered non-output WAL
blocks empty peeks from advancing; `lag_near_start` is recorded but is not
the sole PASS gate. The JSON declares
`qualification_kind: "development_soak"`.

`run_fault_injection_smoke.sh` exercises a postmaster restart and a fast
shutdown/restart recovery path. Missing-slot/discontinuity coverage remains in
the broader WAL E2E suite. `measure_wal_overhead.sh` compares a small
update workload with default replica identity, `REPLICA IDENTITY FULL`, and
tracked capture. Its latency and WAL values are development measurements, not
production capacity claims.

These are development-qualification harnesses only. The exact-RC 24-hour soak
and its release evidence are Milestone 6 work and were not completed overnight.
If extension installation fails, record that prerequisite failure rather than
treating an older installed extension as qualification evidence.

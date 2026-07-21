# Development qualification harnesses

These bounded scripts create an isolated temporary PostgreSQL cluster and
write machine-readable summaries to `target/qualification/` by default. Each
artifact records `tested_commit`, source-tree cleanliness, branch/exact tag,
PostgreSQL version, installed extension SHA-256, optional release-helper
SHA-256, configuration summary, and start/end timestamps. Install the current
extension for the selected PostgreSQL build first:

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
200 individual transactions to the unblocked table. Each configured database
has a dedicated capture worker and a dedicated maintenance worker; the harness
asserts both processes are present before applying the maintenance lock. It
records each transaction's monotonic commit acknowledgement and immediately
polls for that exact event's first `flashback.delta_log` visibility at 20 ms
resolution, then reports p50/p95/p99/max milliseconds. PASS requires p95 below
1000 ms, max below 5000 ms, capture progress during the hold, tracked-prefix
catch-up, fixed global-prefix catch-up, and global slot lag below the explicit
target. The JSON keeps `tracked_prefix_caught_up` and
`global_slot_lag_within_target` separate.

`run_dev_soak.sh` is a bounded **development soak**, not a 24-hour or
exact-RC qualification. It defaults to 90 seconds. Set
`PG_FLASHBACK_SOAK_SECONDS` and `PG_FLASHBACK_SOAK_TARGET_MIB` for a bounded
run. It inserts, updates, and deletes each cycle, then must drain capture
within a bounded deadline: decoded INSERT/UPDATE/DELETE event counts must
match the expected counts exactly, every inserted id must appear in
`delta_log`, `confirmed_flush_lsn` must reach the fixed final WAL boundary,
and final global slot lag must return to baseline plus the configured slack.
`lag_near_start=false` can never PASS. The JSON declares
`qualification_kind: "development_soak"`.

`run_fault_injection_smoke.sh` exercises a postmaster restart and a fast
shutdown/restart recovery path. Missing-slot/discontinuity coverage remains in
the broader WAL E2E suite. `measure_wal_overhead.sh` compares a small
update workload with default replica identity, `REPLICA IDENTITY FULL`, and
tracked capture. Its latency and WAL values are development measurements, not
production capacity claims.

Files named `docs/qualification/*-latest.json` are historical development
examples only. They may describe an older commit and are neither updated by
these scripts nor accepted as release evidence. Exact-commit evidence belongs
in immutable CI/qualification-runner artifacts outside the source tree, which
avoids a commit self-reference.

Honest provenance fields (see `scripts/qualification_provenance.sh` and
`scripts/verify_qualification_provenance.sh`):

- `source_commit` / `source_tree` — the tree that was actually tested
- `qualification_artifact_sha256` / `helper_binary_sha256` / `package_sha256`
- `run_started_at` / `run_completed_at` and `tree_clean_at_start`/`end`
- optional later `evidence_summary_commit` that differs only in docs/evidence

Never claim final HEAD was tested if later commits changed executable code,
SQL, tests, packaging or scripts. Do not embed a commit's own hash inside that
same commit.

Set `PG_FLASHBACK_REQUIRE_CLEAN_TREE=1` for a release-gate invocation. This
rejects a dirty checkout before cluster creation. Exact-RC tests must run from
a clean checkout/tag and archive `target/qualification/` externally.

These are development-qualification harnesses only. Exact-candidate release
gates are separate and must use packaged archives from `CANDIDATE_DIR`:

1. `scripts/run_exact_candidate_functional_suite.sh` (Gate A)
2. `scripts/run_exact_rc_chaos_suite.sh` (Gate B; short destructive suite)
3. `scripts/run_exact_rc_24h_stability_soak.sh` (Gate C; ≥86400 active seconds)
4. `scripts/run_exact_candidate_drop_qualification.sh` (Gate D; dedicated
   repeated DROP-to-restore product-claim suite)

Gate D is deliberately not inferred from DML volume in Gate C. By default it
performs 100 consecutive DROP/restore cycles on the same local tracking
lifecycle, then drops and restores an indexed medium relation, a quoted TOAST
relation, and a pgBackRest-backed relation through the packaged helper and
reference-controller production swap. Its machine-readable result reports the
actual DROP count, cumulative rows/logical bytes/physical bytes present at the
destructive boundaries, concurrent commits during the medium restore, and
restore latency distributions. A release must not describe DROP recovery as
stress-qualified unless this exact-candidate gate passes.

`scripts/run_exact_rc_harness_selftest.sh` is accelerated harness regression
only (`qualification_kind=exact_rc_harness_selftest`) and must never emit a
24h release PASS. `scripts/run_development_stability_15m.sh` separately runs
every scheduled stability drill in a 15-minute development-only window. It
emits `development_accelerated_stability`; it cannot satisfy or claim the
exact 24-hour release gate. Supported claim language after Gates B and C on this host
class is the bounded stability soak plus a separate exact-candidate chaos
suite on Linux/aarch64 under Lima on an Apple Silicon host.

If extension installation fails, record that prerequisite failure rather than
treating an older installed extension as qualification evidence.

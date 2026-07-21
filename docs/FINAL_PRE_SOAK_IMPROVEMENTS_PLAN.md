# Final pre-soak improvements plan

Status: implementation planned; final 24-hour qualification intentionally
deferred until this plan is complete.

## 1. Goal

Close the three remaining product-quality areas before spending another full
day on qualification:

1. prove and harden capture/maintenance worker isolation;
2. make installation and operation diagnosable without reading internal
   catalogs;
3. make the qualified local profile convenient for small and medium ordinary
   tables, with DROP recovery as a first-class workflow.

The second physical-backup provider is deliberately not part of this plan.
pgBackRest remains the only qualified backup provider for v0.1.0.

There will be no intermediate 24-hour run. Development regressions, focused
E2Es, accelerated stability runs and clean-host tests must be used while code
is changing. One new exact-candidate 24-hour run starts only after every item
in Section 7 is green and the product commit is frozen.

## 2. Current baseline and audit result

The prior stability process was stopped intentionally before this work. Its
result is interrupted development evidence, not release evidence. Changing
code or packaging after it would invalidate it in any case.

Worker separation is already implemented:

- each admitted database registers one capture worker and one maintenance
  worker;
- WAL consumption and staging flush run in the capture worker;
- checkpoints, partition maintenance and retention run in the maintenance
  worker with bounded lock and statement timeouts;
- the isolation harness has observed 200/200 commits with p95 visibility of
  108 ms while an unrelated lifecycle lock was held.

Therefore this plan must not rewrite the worker architecture. It must audit
and harden the existing implementation and bind fresh evidence to the final
candidate.

The audit also found an admission risk that must be treated as correctness
work: `flashback_track()` checks whether the current database appears in
`target_databases`, while worker registration truncates that list to
`max_workers`. A database beyond that limit can therefore look configured even
though no worker pair was registered. Actual worker startup can also fail when
PostgreSQL has insufficient background-worker slots. Tracking must never
report success when its capture worker is absent.

Existing building blocks should be reused rather than duplicated:

- `flashback_advise(regclass)` already estimates local snapshot, restore peak,
  free-space and write-stall risk;
- `flashback_health()` already projects coverage, gaps, slot lag and actionable
  recovery states;
- `flashback_resolve_target()` already converts a human timestamp to one
  proven COMMIT-LSN prefix;
- `flashback_restore_lsn()` is the qualified mutation primitive;
- capture and maintenance already run in separate processes.

The missing product layer is a consolidated readiness diagnosis, safe worker
admission, discoverable local disaster points and a short operator workflow.

## 3. Milestone W — close worker isolation and admission

Estimated effort: 1–3 working days.

### 3.1 One source of truth for worker admission

Implement an internal worker-readiness projection used by monitoring and
tracking. It must distinguish at least:

- current database absent from `target_databases` / `target_database`;
- current database present but beyond `pg_flashback.max_workers`;
- required capture worker not running;
- required maintenance worker not running;
- both workers running;
- PostgreSQL background-worker capacity insufficient for configured pairs.

Database-list parsing and index selection must not be independently
reimplemented in Rust and SQL with different whitespace, duplicate or ordering
semantics. Define and test one canonical contract. Duplicate configured names
must not consume ambiguous worker indices.

`flashback_track()` and `flashback_track_backup()` must fail closed before
creating a lifecycle if the current database has no admitted, running capture
worker. The error must say whether the problem is the database list,
`max_workers`, `max_worker_processes`, startup/restart delay or a missing
worker. A missing maintenance worker must be visible as an actionable degraded
state; whether initial tracking also rejects it must be decided from the
qualified retention contract and documented explicitly.

### 3.2 Failure independence

Extend the isolation harness to prove all of the following on the exact built
extension:

- a blocked maintenance lifecycle does not block WAL consumption for another
  table;
- terminating the maintenance worker does not terminate or stall capture;
- terminating the capture worker leaves committed changes retained in the
  logical slot and they are consumed exactly once after automatic restart;
- no false `healthy` result is emitted while a required worker is missing;
- after recovery, event counts, COMMIT-LSN order and table fingerprints match;
- slot lag drains to the configured bounded target;
- at least 200 separately committed samples meet p95 below 1 second and maximum
  below 5 seconds on the qualification host;
- a multi-database configuration whose list exceeds `max_workers` is rejected
  for the unserved database instead of silently accepting tracking.

Tests must use process identity plus database identity, not only a worker-name
substring. Automatic PostgreSQL worker restart may be awaited with a bounded
deadline; an infinite retry is forbidden.

### 3.3 Exit criteria

- No maintenance call remains in the capture loop.
- No lifecycle can be created for a database without a running capture worker.
- Worker absence is visible through the supported monitoring surface.
- Isolation, restart/catch-up and multi-database admission regressions pass.
- Existing WAL E2E remains green.

## 4. Milestone O — operational diagnosis and safe preflight

Estimated effort: 2–4 working days.

### 4.1 `flashback_doctor()`

Add one read-only, deny-by-default SQL API named `flashback_doctor()` that
returns one row per check with a stable machine-readable shape:

```text
scope, check_name, status, observed, expected, action
```

`status` is exactly `ok`, `warning` or `error`. At minimum it checks:

- extension preload and effective capture mode;
- `wal_level=logical` for the qualified local profile;
- current database worker admission and live capture/maintenance processes;
- configured worker-pair demand versus `max_workers` and
  `max_worker_processes`;
- logical slot presence, database ownership, WAL status, lag and remaining
  safe WAL budget;
- the three mandatory local capacity budgets and safety reserve;
- active capacity override;
- tracked lifecycle health, open gaps and pending boundaries;
- backup-profile prerequisites only when a backup-profile lifecycle exists.

The function is observational. It must not create slots, edit settings, take a
snapshot, run maintenance or repair coverage. Grant it to `flashback_admin`
and `pg_monitor`, never `PUBLIC`.

### 4.2 Operator wrapper

Add a thin `scripts/pg_flashbackctl` wrapper around supported SQL APIs. It must
use libpq/psql configuration rather than store credentials and must use
`psql -X -v ON_ERROR_STOP=1` with safely quoted variables. Initial commands:

- `doctor` — human-readable checks, nonzero exit on any `error`;
- `advise TABLE` — capacity/write-stall projection and explicit local/backup
  recommendation;
- `track TABLE` — run tracking as the first write in its own autocommit
  transaction and wait with a bounded timeout for an active healthy boundary;
- `status [TABLE]` — supported health output only;
- `disasters TABLE [LOOKBACK]` — list safe recovery coordinates;
- `resolve TABLE TIMESTAMP` — show the resolved LSN and pinned frontier without
  restoring;
- `restore TABLE LSN --yes` — run capacity preflight, show the exact target,
  require explicit confirmation, execute `flashback_restore_lsn()`, then wait
  with a bounded timeout for successor health.

The wrapper must not silently choose a profile, edit `postgresql.conf`, restart
PostgreSQL, create a FULL backup or perform a restore without `--yes`.
Machine-readable JSON output should be supported where practical, but it must
be generated from stable SQL columns rather than scraped aligned psql output.

### 4.3 Documentation consistency

Replace stale statements that still describe worker isolation, backup runtime
integration or local admission as pending. Correct stale `COMMENT ON FUNCTION`
text that says qualified WAL tracking merely attaches triggers. Provide one
short local-profile quick start and keep deep architecture in linked docs.

### 4.4 Exit criteria

- A clean operator can identify every deliberately broken prerequisite from
  `flashback_doctor()` without querying internal catalogs.
- `pg_flashbackctl doctor` has deterministic exit codes.
- All wrapper commands handle quoted schema/table names safely.
- RBAC proves `PUBLIC` cannot call new privileged functions and `pg_monitor`
  remains read-only.
- Documentation and runtime report the same support status.

## 5. Milestone U — small/medium local-profile usability

Estimated effort: 3–5 working days.

### 5.1 Safe DROP/TRUNCATE/ALTER discovery

Add a read-only local-profile API, tentatively
`flashback_disaster_points(text, interval)`, that lists recent committed DDL
disasters and their safe pre-transaction recovery coordinates. It must return
at least:

```text
table_name, event_type, disaster_commit_lsn, disaster_time,
safe_target_lsn, safe_target_time, generation_id, status, reason
```

For a DROP/TRUNCATE/ALTER transaction the safe target is the last complete
admitted COMMIT prefix before the disaster transaction, never an event-level
timestamp inside that transaction. The function must fail closed or return a
non-restorable status when there is a gap, unresolved boundary, ambiguous
generation, missing predecessor commit or frozen watermark. It must not use
the legacy timestamp replay path.

This API is the primary way a user who did not record a timestamp finds the
correct LSN after an accidental DROP. `pg_flashbackctl disasters` consumes it.

### 5.2 One supported local workflow

Qualify this complete operator journey with no direct catalog writes:

1. run `doctor`;
2. run `advise` and receive a clear local-profile decision;
3. track an ordinary LOGGED, non-partitioned table;
4. wait until health is active;
5. perform INSERT/UPDATE/DELETE and repeated DROP scenarios;
6. discover a safe pre-DROP coordinate after the fact;
7. resolve/inspect the target;
8. restore only after explicit confirmation;
9. verify rows, owner, ACL, primary/secondary indexes, sequences and health;
10. repeat tracking/restore/untrack without leaked payloads or slots.

The workflow must cover:

- empty and small tables;
- a bounded medium table that fits the host's configured capacity budget;
- quoted identifiers and TOAST values;
- a table with and without a primary key, with the non-PK performance warning
  visible;
- DROP, TRUNCATE and ALTER fences;
- capacity rejection before lock acquisition;
- lock timeout with no partial mutation;
- interrupted wrapper execution with database correctness preserved.

No arbitrary byte threshold labels a table “small” or “medium”. The local
profile is recommended only when measured snapshot bytes, projected restore
peak, filesystem reserve, expected write stall and supported table shape all
fit the configured budgets. Otherwise the result is `backup` or
`unsupported`, with a reason.

### 5.3 Usability acceptance

- The happy-path local workflow is documented in one place and executable by
  copying commands.
- An accidental DROP can be recovered without the operator having recorded an
  LSN or timestamp before the DROP.
- Every destructive action names the table and exact LSN and requires explicit
  confirmation.
- A failed preflight, timeout or interrupted client leaves the live table and
  coverage metadata consistent.
- No qualified command calls legacy timestamp restore internally.

## 6. Required regression and qualification work

Every implementation commit must run proportional focused tests. Before the
candidate is frozen, run all of these against one clean product commit:

1. `git diff --check`, Rust fmt/clippy, helper fmt/clippy/tests, ShellCheck;
2. PostgreSQL 15/16/17/18 extension suites;
3. WAL E2E and worker isolation/restart E2E;
4. local capacity E2E and new doctor/control-wrapper E2E;
5. exact functional suite, including DROP/TRUNCATE/ALTER and quoted names;
6. exact DROP qualification suite;
7. backup coverage, recovery helper, retained-anchor and advancement E2Es;
8. chaos/fault injection and WAL-overhead measurement;
9. a 15-minute accelerated stability run with every scheduled drill firing;
10. clean-host installation from the newly built candidate archive.

Tests must assert final fingerprints and exact event/restore counts, not only
command exit status. Any generated evidence must name the tested product
commit, source tree and binary/package hashes.

## 7. Freeze gate before the only new 24-hour run

The final stability run may be prepared, but not started by the implementation
workflow. It is
allowed only when all statements below are true:

- Milestones W, O and U satisfy every exit criterion.
- No supported-status contradiction remains in README, release scope,
  checklist, runbooks or function comments.
- The complete short matrix in Section 6 is green on one clean commit.
- The candidate archive and installed extension/helper hashes match that
  commit.
- Clean-host smoke uses that exact archive.
- The 24-hour harness self-test and accelerated schedule pass on that exact
  archive.
- The worktree is clean and the branch tip is pushed normally.
- A freeze JSON and operator-owned start command are generated.

After freeze, any product, SQL, harness, packaging or build-input change
invalidates readiness and requires rebuilding the candidate. Documentation
that changes qualification meaning also invalidates it.

The final 24-hour test must exercise continuous DML plus time-distributed DROP
restores, PostgreSQL restart, worker interruption, maintenance contention,
local restore and bounded capacity. The separate exact-candidate chaos suite
remains required; stability and chaos are complementary evidence.

## 8. Commit sequence

Use reviewable commits; do not create one release-sized commit:

1. `fix: close worker admission and restart visibility gaps`
2. `feat: add read-only operational doctor and control wrapper`
3. `feat: expose safe local disaster recovery points`
4. `test: qualify the supported local operator workflow`
5. `docs: reconcile final pre-soak operating contract`
6. `build: freeze the final stability candidate` only after every short gate
   passes.

A commit may be split further when that makes rollback or review safer. Never
mix generated qualification evidence into a product-fix commit.

## 9. Non-goals and safety rules

- Do not add a second backup provider in this work.
- Do not redesign the already separate workers unless a reproduced bug proves
  it necessary.
- Do not automatically classify by table size alone.
- Do not automatically create backups, edit server configuration or restart
  PostgreSQL.
- Do not weaken fail-closed coverage, generation, gap, WAL-prefix or proof
  validation.
- Do not broaden v0.1.0 to partitions, HA/failover, tablespaces,
  differential/incremental chains or a 100+ GiB performance claim.
- Do not merge to `main`, tag, publish a release or start the 24-hour test as
  part of implementation.
- Use only the configured `CaghanTU <caghan@caghan.dev>` Git identity. No
  automated-tool or co-author attribution may appear in commits, trailers, PR
  text or generated release material.

## 10. Expected duration

Because the worker split already exists, the realistic engineering estimate is
7–12 focused working days rather than a new multi-week architecture rewrite:

| Work | Estimate |
|---|---:|
| Worker audit/admission hardening | 1–3 days |
| Doctor, preflight and control wrapper | 2–4 days |
| Local DROP/usability workflow | 3–5 days |
| Final short qualification and freeze | 1–2 days |

Some work can overlap, but qualification must remain sequential against one
frozen commit. The final 24-hour runtime begins only after this estimate's code
and short-test work is finished.

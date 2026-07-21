# Release checklist

This checklist is a gate, not a retrospective. Do not publish a tag until every
required item is checked against the exact commit being released.

## 1. Scope and version

- [ ] The release commit is clean and reviewed; no generated E2E clusters,
      credentials, dumps or local configuration are tracked.
- [ ] `Cargo.toml`, helper `Cargo.toml`, `pg_flashback.control`,
      `CHANGELOG.md` and the tag agree on the semantic version.
- [ ] `docs/RELEASE_SCOPE.md` describes the actually tested PostgreSQL,
      pgBackRest, repository and table topologies.
- [ ] Architecture claims are scoped independently: the exact 24-hour Gate C is
      a local_delta observer-only stability soak and names its
      Linux/architecture/host class, while x86_64 release artifacts are not
      called qualified until hosted CI and clean-host packaging pass.
- [ ] The exact 24-hour result records 29/29 DROP/restores: 23 hourly, two
      early/late and four after state-changing drills, each discovered via
      `flashback_disaster_points` and verified by fingerprint/contract.
- [ ] Gate C does not call `flashback_consume_wal()`; capture advances only via
      admitted background workers. Manual consume remains a fail-closed harness
      defect.
- [ ] `docs/STORAGE_POLICY.md`, `docs/COVERAGE_MODEL.md`, README and runtime
      behavior agree; no scaffold-only feature is described as enforced.
- [ ] T-01/A remains closed in the release scope: qualified local capture is
      WAL-only, COMMIT LSN is primitive and trigger capture is labeled
      legacy/experimental.
- [ ] README claims and benchmark links are reproducible and do not imply that
      snapshot-direct removes WAL replay cost.

## 2. Source quality

Run from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --locked --no-default-features --features pg17 -- -D warnings
cargo fmt --manifest-path tools/pg_flashback_recovery/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path tools/pg_flashback_recovery/Cargo.toml \
  --all-targets -- -D warnings
cargo test --locked --manifest-path tools/pg_flashback_recovery/Cargo.toml
cargo audit --file Cargo.lock
cargo audit --file tools/pg_flashback_recovery/Cargo.lock
cargo deny check licenses sources
cargo deny --manifest-path tools/pg_flashback_recovery/Cargo.toml \
  check licenses sources
git diff --check
```

- [ ] Both lockfiles are committed and the commands above pass.
- [ ] `cargo audit` reports no vulnerabilities. The only accepted
      maintenance warnings are the two pgrx transitive items documented in
      `THIRD_PARTY_NOTICES.md`; any new warning is reviewed explicitly.
- [ ] `THIRD_PARTY_NOTICES.md` matches direct dependencies and external tools.
- [ ] Every shell script passes `bash -n`; ShellCheck findings are resolved or
      explicitly justified.

## 3. PostgreSQL regression matrix

Run each major against the release commit, removing only generated test data
between runs:

```bash
rm -rf target/test-pgdata && cargo pgrx test pg15
rm -rf target/test-pgdata && cargo pgrx test pg16
rm -rf target/test-pgdata && cargo pgrx test pg17
rm -rf target/test-pgdata && cargo pgrx test pg18
```

- [ ] PostgreSQL 15 regression suite passes for the exact release commit.
- [ ] PostgreSQL 16 regression suite passes for the exact release commit.
- [ ] PostgreSQL 17 regression suite passes for the exact release commit.
- [ ] PostgreSQL 18 regression suite passes for the exact release commit.
- [ ] The real WAL/worker E2E passes: `scripts/run_wal_e2e.sh`.
- [ ] Capture/maintenance isolation SLO passes (≥200 commits, p95 <1s, max <5s):
      `scripts/run_capture_maintenance_isolation_slo.sh`.
- [ ] Worker admission / kill / restart / max_workers E2E passes:
      `scripts/run_worker_admission_isolation_e2e.sh`.
- [ ] Doctor / `pg_flashbackctl` / local DROP discovery workflow E2E passes:
      `scripts/run_operator_workflow_e2e.sh`.
- [ ] Local capacity/write-stall adversarial E2E passes:
      `scripts/run_local_capacity_e2e.sh`.
- [ ] Slot/coverage health action E2E passes (integration
      `slot_health_actions` / WAL E2E health assertions).
- [ ] Retained FULL + continuous WAL production path is exercised:
      `scripts/run_retained_full_wal_poc.sh` and
      `scripts/run_retained_full_adversarial_e2e.sh` (one FULL + continuous WAL;
      no differential/incremental chains).
- [ ] Automatic anchor advancement E2E passes:
      `scripts/run_anchor_advancement_e2e.sh` (discovers operator FULL without
      helper-created backups; predecessor pin/retire/expire; crash resume).
- [ ] Qualification provenance is mechanically honest:
      `scripts/verify_qualification_provenance.sh` (source_commit/tree vs
      docs-only evidence summary; no self-hash churn).
- [ ] Record the exact suite/check counts in the qualification artifact for
      this commit; do not hard-code suite sizes in this checklist.

## 4. Coverage correctness qualification

- [ ] RB-01 through RB-11 have regression or qualification evidence against
      the release commit.
- [ ] Track and maintenance boundaries use the exact lock protocol and reject
      a caller transaction that already performed writes.
- [ ] Concurrent first-track calls serialize on one deterministic pre-identity
      bootstrap key and cannot allocate separate stable tracking identities.
- [ ] Track/re-anchor takes its final relation-lock strength from the outset
      (`SHARE ROW EXCLUSIVE` minimum, stronger when required), and capture
      trigger or replica-identity/stream setup occurs under that same lock.
- [ ] The exact-base scan demonstrably acquires a fresh MVCC snapshot after the
      relation lock: a writer that commits while lock acquisition waits appears
      in the base; old/RR/serializable/imported snapshots are rejected.
- [ ] Generation applicability is half-open and distinct from the inclusive
      capture watermark; at a handoff coordinate only the successor is
      selected.
- [ ] Only `active` and `sealed` generations are admitted. `building`
      (including a pending/unanchored successor), `aborted` and `retired` are
      never selected, while a broken stream still serves targets no later than
      its frozen watermark.
- [ ] WAL backlog crossing a boundary is routed to the correct generation;
      payload carries commit LSN separately from change LSN and legacy trigger
      rows never enter a qualified generation.
- [ ] Sealing freezes ownership and applicability, but backlog already bound to
      the predecessor may advance its watermark monotonically no farther than
      `superseded_before`; retirement is impossible until that drain is proven.
- [ ] Protected DDL metadata is promoted only with its real WAL COMMIT record;
      untrusted logical-message bodies cannot forge DDL or DML history.
- [ ] T-01/A is adversarially tested: trigger mode is unqualified,
      same-microsecond commits and timestamp/LSN inversion cannot admit an
      ambiguous target, and XID is never used as commit order.
- [ ] Restore/query/deleted-row recovery expose and exercise direct COMMIT-LSN
      APIs. Legacy `timestamptz` entry points reject qualified use; the
      fail-closed resolver maps a human time to one proven prefix and per-event
      `commit_time <= target_time` filtering is never accepted as equivalent.
- [ ] Restore, query-as-of and deleted-row recovery pin one immutable
      generation and reject every persistent gap.
- [ ] Retention racing those APIs cannot remove required payload and retires
      only whole sealed generations.
- [ ] Slot loss/recreate/external advance and capture disablement are durable
      and fail closed; qualified WAL decoding never uses the legacy trigger
      row-size skip.
- [ ] Local restore preflight, in-swap base/pending marker, post-commit real
      commit-time and (in WAL mode) COMMIT-LSN resolution, and successor
      activation are tested. The SQL result/NOTICE explicitly reports pending
      coverage, a higher-level workflow waits for healthy activation, and the
      first release rejects a skip-base request.
- [ ] Local swap rollback, crash after commit but before activation, duplicate
      resolver retry and DML committed during the pending window all preserve
      exactly-once activation and successor payload routing.
- [ ] With the worker stopped, committed WAL for the old relation makes restore
      fail before any table change; after worker catch-up, retry proves the
      bounded prefix empty and swaps the OID. The event is owned by the sealed
      predecessor and historical OIDs are not rewritten.
- [ ] A stream break during the very first `building` boundary removes its
      draft payload, leaves an immutable `aborted` tombstone, and permits only
      a new explicit re-anchor generation while the missing interval remains
      rejected.
- [ ] Whole-generation payload retirement leaves durable audit tombstones and
      cannot cascade-delete generation, gap or lineage metadata; interruption
      before/during/after deletion resumes from committed `retiring` intent,
      concurrent retriers serialize, and admission rejects the payload.
- [ ] Policy-B cleanup performs no snapshot heap re-count: content-only drift
      does not wedge retirement, while catalog OID, extension owner/membership,
      physical tuple-layout drift, lifecycle binding and the newer active
      anchor are revalidated immediately before removal.
- [ ] `flashback_admin` has no direct mutating ACL on internal/runtime payload
      and cannot execute restore-guard or trigger attach/detach helpers; newly
      adopted payload is owned by the extension owner with delegated ACLs
      removed.
- [ ] A populated-installation upgrade has measured lock, rewrite, disk/WAL
      and downtime bounds and a maintenance runbook. A real `pg_dump`/restore
      round trip proves that no tracking, payload or coverage state is silently
      transported and that the restored database requires fresh tracking.
- [ ] For the initial `v0.1.0`, the packaged base SQL is tested as the canonical
      fresh-install baseline. For every later release, the extension version is
      bumped and a versioned update script migrates the previous public
      release. Re-running base SQL or drop/create is never an upgrade strategy;
      drop/create would delete extension-owned recovery payload.
- [ ] Capture drain remains within its latency/WAL-lag SLO while maintenance is
      blocked on another table.

## 5. Backup-backed release qualification

Use the exact PostgreSQL and pgBackRest versions named in
`docs/RELEASE_SCOPE.md`:

```bash
scripts/run_recovery_helper_e2e.sh
```

- [ ] The recovery-helper E2E suite passes for the exact release commit,
      including snapshot-direct and classic recovered-data fingerprint
      equality, a real post-DROP extension request and final production swap.
      Record the exact check count in the qualification artifact.
- [ ] The dedicated exact-candidate DROP qualification passes. Evidence records
      the actual attempted/passed DROP count (not DML operations), cumulative
      rows and bytes present at each destructive boundary, repeated same-
      lifecycle restores, indexed medium and quoted/TOAST reconstruction,
      concurrent capture progress, and a pgBackRest-backed DROP through the
      packaged helper and reference-controller production swap.
- [ ] Every backup-profile swap commits a LOGGED pending/unanchored state and
      opens a durable gap; it does not create a local row snapshot, and both
      the result and `flashback_health()` report the required next full backup.
- [ ] Initial backup tracking commits and resolves a durable LOGGED marker,
      then remains at zero active generations until `verify-anchor` activates
      either (a) a completed fresh FULL whose start LSN is strictly after the
      marker, or (b) a retained FULL whose stop is no later than the marker and
      whose contiguous archived WAL is verified through it. An overlapping
      backup and an incomplete archive prefix are rejected.
- [ ] The active backup generation pins one immutable anchor containing
      repository/stanza/label, FULL type, system identifier, timeline,
      manifest reference/SHA-256 and marker/start/stop LSNs; its physical
      boundary is the same anchor's stop LSN, while retained activation
      separately records the marker as `coverage_start_lsn`; cross-lifecycle
      references fail.
- [ ] WAL/backup LSNs are admitted only within their recorded physical
      timeline; a timeline mismatch or promotion freezes coverage and opens a
      gap rather than comparing bare LSNs across timelines.
- [ ] Only a new post-swap completed and verified full backup activates the
      successor at its stop boundary. Its start LSN is strictly after
      the resolved swap commit; failed/partial/differential/incremental or
      overlapping backups do not qualify, and `[swap commit, backup stop)`
      remains rejected.
- [ ] Resolving a backup swap seals the predecessor and leaves the successor
      `building`; zero active generations is preserved while the gap is open,
      with no admission fallback to the predecessor.
- [ ] The backup generation's persisted physical-WAL `valid_through_lsn`
      advances only across a contiguous, revalidated archive range while the
      repository shared lock is held; expire cannot race that proof.
- [ ] Old target, missing WAL, backup/expire race, cancellation, timeout,
      process crash, server-start failure, quota, request/profile conflict,
      corrupt cache and controller failure paths fail closed.
- [ ] No temporary PostgreSQL process, socket, imported table or materialized
      cluster remains after handled failures.
- [ ] The machine-readable E2E summary is archived as release evidence.

## 6. Package verification

- [ ] `cargo pgrx package` succeeds for PostgreSQL 15, 16, 17 and 18.
- [ ] The helper builds in release mode with `--locked`.
- [ ] Release archives contain `LICENSE`, `THIRD_PARTY_NOTICES.md`, relevant
      README/runbook files and no secret-bearing example values.
- [ ] SHA-256 checksums are generated and independently verified.
- [ ] Install each extension package and helper artifact on a clean supported
      Linux host before publishing.

## 7. Publication

- [ ] Create a signed `vX.Y.Z` tag only after CI passes on the release commit.
- [ ] Confirm the tag workflow creates a **draft** GitHub Release and all
      PostgreSQL-major/helper artifacts plus `SHA256SUMS` are present.
- [ ] Review the changelog-derived release notes and limitations one last time.
- [ ] Publish the draft manually; do not let automation silently broaden the
      support contract.
- [ ] After publication, verify the archives/checksums from GitHub and run the
      documented smoke installation.

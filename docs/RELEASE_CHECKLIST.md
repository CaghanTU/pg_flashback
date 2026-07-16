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

- [ ] PostgreSQL 15: 65/65.
- [ ] PostgreSQL 16: 65/65.
- [ ] PostgreSQL 17: 65/65.
- [ ] PostgreSQL 18: 65/65.
- [ ] The real WAL/worker E2E passes: `scripts/run_wal_e2e.sh`.

## 4. Backup-backed release qualification

Use the exact PostgreSQL and pgBackRest versions named in
`docs/RELEASE_SCOPE.md`:

```bash
scripts/run_recovery_helper_e2e.sh
```

- [ ] All 27 checks pass, including snapshot-direct and classic recovered-data
      fingerprint equality, a real post-DROP extension request and final
      production swap.
- [ ] Old target, missing WAL, backup/expire race, cancellation, timeout,
      process crash, server-start failure, quota, request/profile conflict,
      corrupt cache and controller failure paths fail closed.
- [ ] No temporary PostgreSQL process, socket, imported table or materialized
      cluster remains after handled failures.
- [ ] The machine-readable E2E summary is archived as release evidence.

## 5. Package verification

- [ ] `cargo pgrx package` succeeds for PostgreSQL 15, 16, 17 and 18.
- [ ] The helper builds in release mode with `--locked`.
- [ ] Release archives contain `LICENSE`, `THIRD_PARTY_NOTICES.md`, relevant
      README/runbook files and no secret-bearing example values.
- [ ] SHA-256 checksums are generated and independently verified.
- [ ] Install each extension package and helper artifact on a clean supported
      Linux host before publishing.

## 6. Publication

- [ ] Create a signed `vX.Y.Z` tag only after CI passes on the release commit.
- [ ] Confirm the tag workflow creates a **draft** GitHub Release and all
      PostgreSQL-major/helper artifacts plus `SHA256SUMS` are present.
- [ ] Review the changelog-derived release notes and limitations one last time.
- [ ] Publish the draft manually; do not let automation silently broaden the
      support contract.
- [ ] After publication, verify the archives/checksums from GitHub and run the
      documented smoke installation.

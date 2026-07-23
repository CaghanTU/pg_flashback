# Development and testing

Generated build and test output belongs under `target/`. Do not commit
qualification logs, candidate archives, temporary clusters, or freeze files.
Git history and external CI artifacts retain old evidence without turning the
source tree into an archive.

## Toolchain

- Rust 1.85+
- cargo-pgrx 0.16.1
- PostgreSQL development installations for the majors being tested
- `shellcheck`, `jq`, `psql`, and standard Unix tools

```bash
cargo install --locked cargo-pgrx --version 0.16.1
cargo pgrx init --pg17 /path/to/pg_config
```

## Fast checks

```bash
git diff --check
cargo fmt --all -- --check
cargo clippy --no-default-features --features pg17 -- -D warnings
cargo test --manifest-path tools/pg_flashback_recovery/Cargo.toml
shellcheck scripts/pg_flashback scripts/*.sh
```

## PostgreSQL matrix

```bash
cargo pgrx test pg15
cargo pgrx test pg16
cargo pgrx test pg17
cargo pgrx test pg18
```

Do not hard-code the expected test count in documentation. The command exit
status and external run artifact are authoritative.

## Focused integration suites

The `scripts/` directory contains isolated-cluster suites for:

- exact logical-WAL capture;
- worker admission and capture/maintenance isolation;
- DROP recovery and adversarial DROP cases;
- transaction and schema-change matrices;
- unprotect/re-protect/cleanup lifecycle;
- upgrade from 0.1.0 to 0.2.0;
- HA promotion and timeline refusal;
- capacity admission;
- backup-provider experiments;
- clean-host package installation.

The most important local-product commands are:

```bash
./scripts/run_wal_e2e.sh
./scripts/run_exact_candidate_drop_adversarial.sh
./scripts/run_exact_wal_transaction_schema_matrix.sh
./scripts/run_dba_acceptance_regressions.sh
./scripts/run_extension_upgrade_e2e.sh
```

Read each script's header before running it. Most create and remove temporary
databases, slots, clusters, sockets, or installed extension files.

## Long-running tests

Long tests must use an already built package and record its source commit,
source tree, package digest, extension binary digest, PostgreSQL version,
architecture, start/end times, and result outside the tracked source tree.

The stability harness includes normal DML, distributed DROP/recovery drills,
worker interruption, PostgreSQL restart, maintenance contention, and final
fingerprint/coverage checks. A shortened development run is not evidence for a
24-hour claim.

Changing code after a run does not erase the run; it simply means the evidence
belongs to the commit that was actually tested. Development can continue
normally.

## Packages

Build a local package:

```bash
PG_MAJOR=17 ./scripts/build_candidate_archive.sh
```

Artifacts are written under `target/candidate/<commit>/` and include:

- source and binary archives;
- `MANIFEST.json`;
- `SHA256SUMS`;
- an SBOM;
- extension, CLI, and helper binary digests.

Never commit these generated files.

## Documentation rules

- README describes the current user experience.
- `docs/SUPPORT.md` defines supported and rejected behavior.
- `docs/ARCHITECTURE.md` explains invariants, not implementation history.
- Completed plans, old audits, chat transcripts, and run logs stay in Git
  history or external artifacts.
- Experimental features must be labeled and must not broaden the main product
  promise.

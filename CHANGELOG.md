# Changelog

All notable changes to pg_flashback are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

pg_flashback is in active development. This section describes the current
source tree, not a published stable release.

### Added

- One-command local recovery of ordinary PostgreSQL tables after an accidental
  DROP.
- `pg_flashback` operator CLI with `doctor`, `protect`, `list`, `status`,
  `history`, `recover --dry-run`, `unprotect`, and `cleanup`.
- Logical-WAL capture with separate capture and maintenance workers.
- Generation-aware coverage based on complete transaction COMMIT LSNs.
- Event-bound pre-DROP manifests for schema, sequence, ACL, and dependency
  reconstruction.
- Durable recovery planning, execution journal, failure reconciliation, and
  exact successor-generation verification.
- Capacity and write-stall admission checks with actionable health output.
- Multi-database worker admission and CLI status/doctor aggregation.
- Versioned extension upgrade from 0.1.0 to 0.2.0.
- Candidate package provenance, checksums, and SBOM generation.
- `external_zstd` SnapshotStore: a supported opt-in production backend that
  stores the protected base image as a compressed artifact on an external
  filesystem root instead of inside PostgreSQL, with online (non-blocking)
  protect/maintain orchestration, crash/abort reconciliation, and doctor/
  health reporting. `heap_v1` remains the default. `external_zstd` has not
  yet been qualified at 10/25/50 GiB scale or over a 24-hour soak.
- Centralized, version-tolerant `output_plugin_libraries` compatibility
  handling (`scripts/lib/output_plugin_allowlist.sh`,
  `flashback_doctor()`, `pg_flashback config recommend`) for current
  security-patched PostgreSQL minors that added this GUC.

### Correctness and safety

- `flashback_recover_plan` now distinguishes "capture frontier still
  catching up" from "genuinely no DROP" using the durable
  `flashback.capture_streams` watermark instead of the raw replication
  slot, which could appear caught up before the corresponding
  `flashback.delta_log` row was actually committed. A capture stream that is
  missing or broken is now a distinct, immediately-reported hard failure
  (`capture_stream_unavailable`) rather than indefinite catch-up polling.

- Recovery now refuses ambiguous DROP events, unsupported CASCADE dependency
  graphs, same-name identity conflicts, coverage gaps, missing slots, and
  timeline discontinuities.
- Failed and abandoned recovery operations remain durably visible.
- Restore finalization is bound to the exact tracking lifecycle, generation,
  stream, boundary, watermark, and operation.
- Unprotect, re-protect, and cleanup use independent lifecycle identities.
- Original identity/serial sequence names and state are restored after DROP.
- Logical decoding preserves commit ordering and valid JSON for quoted
  identifiers and non-finite numeric values.
- Retention no longer removes active evidence and resumes interrupted cleanup
  safely.
- Public permissions are deny-by-default; sensitive row payloads are excluded
  from monitoring-role access.

[Unreleased]: https://github.com/CaghanTU/pg_flashback/compare/main...HEAD

# Changelog

All notable changes to pg_flashback are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-07-16

First supported open-source release candidate.

### Added

- PostgreSQL 15–18 table-level point-in-time restore, historical query and
  deleted-row recovery using local snapshots plus trigger or logical-WAL
  capture.
- Backup profile for large ordinary tables without an in-database base copy or
  row-delta duplication.
- External `pg-flashback-recovery` executor with pgBackRest full-backup
  selection, real XFS reflink capability probing, classic restore fallback and
  private native PostgreSQL LSN recovery.
- Immutable extension/helper request protocol, checksum-protected custom dump,
  schema/row/OID/owner/ACL validation and a transactional shadow-table swap.
- Durable pre-DDL LSN markers for recovery after `DROP`, `TRUNCATE` and
  `ALTER`, using the WAL insertion position so pre/post schema boundaries are
  strictly ordered.
- Crash reconciliation, cancellation and timeout handling, work quotas,
  repository backup/expire coordination and stable machine-readable errors.
- Reference controller, pgBackRest lock wrapper, operator runbook, explicit
  first-release support contract and a real recovery E2E suite.

### Correctness fixes

- Prevented retention from dropping the active monthly delta partition.
- Made logical slot creation database-aware and fail-closed.
- Escaped decoded identifiers and represented non-finite numeric values as
  valid JSON.
- Preserved logical-decoding commit time/change LSN and deterministic event
  ordering.
- Rejected writable/symlinked helper configuration, symlinked recovery config
  files and non-XFS snapshot-direct roots; added classic-restore disk
  preflight.
- Removed unsupported PostgreSQL 13/14 builds whose tuple layout was not
  safely handled.

[Unreleased]: https://github.com/CaghanTU/pg_flashback/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/CaghanTU/pg_flashback/releases/tag/v0.1.0

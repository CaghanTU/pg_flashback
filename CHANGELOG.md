# Changelog

All notable changes to pg_flashback are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

Draft content for the first intended public release (`v0.1.0`). This section is
not a published release. Do not treat it as shipping until every gate in
[`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md) and
[`docs/RELIABLE_RELEASE_PLAN.md`](docs/RELIABLE_RELEASE_PLAN.md) passes for the
exact release commit.

Platform note: the exact 24-hour candidate gate targets PG17 Linux/aarch64
under Lima on Apple Silicon. Tagged prebuilt x86_64 artifacts have separate
hosted build and clean-host gates and are not described as 24-hour qualified.
Native macOS is unsupported.

### Added

- PostgreSQL 15–18 table-level point-in-time restore, historical query and
  deleted-row recovery using local snapshots plus logical-WAL capture for the
  qualified local profile; trigger capture remains legacy/experimental.
- Backup profile for large ordinary tables without an in-database base copy or
  row-delta duplication, with authenticated repository anchors, post-swap FULL
  re-anchor, contiguous archived-WAL frontiers and durable gap admission.
- External `reconcile-anchors` helper command (plus example systemd timer) that
  discovers newer scheduled pgBackRest FULL backups, advances preferred
  anchors without creating backups, and retires sealed predecessors only after
  their exclusive retention window expires.
- External `pg-flashback-recovery` executor with pgBackRest full-backup
  selection, real XFS reflink capability probing, classic restore fallback and
  private native PostgreSQL LSN recovery.
- Immutable extension/helper request protocol, checksum-protected custom dump,
  schema/row/OID/owner/ACL validation and a transactional shadow-table swap.
- Durable pre-DDL LSN markers for recovery after `DROP`, `TRUNCATE` and
  `ALTER`, using the WAL insertion position so pre/post schema boundaries are
  strictly ordered.
- Crash reconciliation, cancellation and timeout handling, work quotas,
  repository backup/expire coordination, crash-durable expiration leases and
  stable machine-readable errors.
- Reference controller, pgBackRest lock wrapper, operator runbook, explicit
  first-release support contract and a real recovery E2E suite.

### Correctness fixes

- Retained FULL restore admission now uses the advertised coverage lower bound
  (tracking marker), not the earlier physical FULL stop; post-swap /
  `full_reanchor` boundaries reject retained pre-marker FULL reactivation.
- Prevented retention from dropping the active monthly delta partition.
- Made logical slot creation database-aware and fail-closed.
- Escaped decoded identifiers and represented non-finite numeric values as
  valid JSON.
- Preserved identity definitions/sequence state after DROP recovery and kept
  JSON/JSONB/array values structured during WAL replay.
- Preserved logical-decoding commit time/change LSN and deterministic event
  ordering.
- Isolated capture drain from maintenance workers, rejected newer repository
  timelines, detected missing retained anchors, and prevented pgBackRest
  automatic expiration from deleting generation-pinned FULL backups.
- Rejected writable/symlinked helper configuration, symlinked recovery config
  files and non-XFS snapshot-direct roots; added classic-restore disk
  preflight.
- Removed unsupported PostgreSQL 13/14 builds whose tuple layout was not
  safely handled.

[Unreleased]: https://github.com/CaghanTU/pg_flashback/compare/main...HEAD

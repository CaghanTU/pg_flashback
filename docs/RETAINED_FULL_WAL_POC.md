# Retained FULL + continuous WAL

Status: **production-wired for v0.1** (one retained FULL + continuous WAL).
Differential/incremental backup chains and non-pgBackRest providers remain
unsupported.

## Question

Can an existing retained FULL that completed **before** the tracking marker, plus
contiguous archived WAL through the requested target, physically recover the
exact table — including across a production shadow-swap — and can tracking
activate that path without requiring a newly created FULL after the marker?

## Correct recovery invariant

- verified system identifier and timeline/history
- immutable FULL backup/manifest identity
- `backup_stop_lsn <= target_lsn` (start-before-target alone is insufficient)
- continuous verified WAL for backup consistency and from backup stop through target
- no gap in the archive prefix
- physical anchor/dependencies and required WAL remain pinned
- a backup overlapping the target is not eligible merely because its start LSN is earlier

## Production contract

`verify-anchor` discovers repository-derived anchors only:

1. Prefer a fresh FULL with `backup_start_lsn > tracking_marker_lsn` when one
   already exists.
2. Otherwise activate a retained FULL with `backup_stop_lsn <= marker` and
   contiguous archived WAL through the marker (`activation_mode =
   retained_full_plus_wal`).
3. If neither exists, fail closed — do not auto-start a cluster-sized FULL.
   Operators may take an explicit fresh FULL and retry.

Retained activation keeps the physical generation boundary at the FULL stop
(FK-bound to `backup_anchors`) and sets initial `valid_through` from the
verified WAL frontier. Tracked `coverage_start_lsn` is the marker. Fresh
activation still uses the FULL stop as both boundary and initial
`valid_through`.

## Harnesses

| Harness | Role |
|---------|------|
| `scripts/run_retained_full_wal_poc.sh` | Physical A/B/C fingerprints + production retained activation |
| `scripts/run_retained_full_adversarial_e2e.sh` | Activation negatives, pin/expire, forge refusal, restore |
| `tests/sql/integration/retained_full_activation.sql` | SQL consume/eligibility contract |
| `scripts/run_recovery_helper_e2e.sh` | Wrong sysid/timeline and related helper fail-closed paths |

Result JSON from the PoC/adversarial harnesses binds `git_commit` and helper
checksum when the run completes cleanly.

## Limits

- One FULL + continuous WAL only (no diff/incr chains).
- pgBackRest is the only qualified provider.
- Direct external `pgbackrest expire` bypassing the helper remains unsupported;
  health/audit must detect a disappeared pinned anchor.
- Safe advancement to a newer independently verified FULL is operator-driven
  (new verified generation); old pins release only after the successor is active
  and the predecessor is no longer referenced by active/sealed coverage.
- Long replay may recommend a fresher anchor via health without auto-taking a FULL.

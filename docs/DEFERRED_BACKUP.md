# Deferred: physical-backup (pgBackRest) recovery

The supported pg_flashback product is **local_delta / exact-WAL DROP recovery
only**. An experimental prototype that recovered a single table from an
existing pgBackRest physical backup plus archived WAL used to live in this
tree. It has been **removed from the supported tree** and its redesign is
deferred to a separate, later effort.

## Why it was removed

The prototype stored backup-profile generations inside the core
`flashback.coverage_generations` table and drove them from the core WAL
worker. Cleanly separating it into its own extension turned out to require
rebuilding its entire generation lifecycle against companion-owned tables —
work comparable in size to the whole local product. Rather than ship a
half-migrated, non-functional companion, the physical-backup path was pulled
out entirely so the local product stays small, coherent and fully tested.

## What was removed

- the `pg_flashback_backup` companion extension crate;
- the `pg-flashback-recovery` executor (Rust helper) and its pgBackRest and
  XFS-reflink integration;
- the `backup` recovery profile, backup anchors / verified proofs / expire
  leases / restore-request tables, and their RBAC in core;
- the recovery-helper, retained-FULL, anchor-advancement, backup-coverage and
  large-DB restore E2E scripts, and the release-candidate qualification
  harness built around the backup-bundled archive;
- the `pg_flashback.proof_hmac_key_file` GUC and proof-HMAC verification.

A CI check (`scripts/check_core_no_backup_surface.sh`) keeps this surface from
creeping back into the core install SQL, Rust, or packaging scripts.

## Where the last implementation lives

The last fully integrated implementation of the experimental backup subsystem
is preserved in git history at commit **`b6aa654`** (`fix: preserve exact DROP
identity across restore phases`), before the local-core separation began. A
future redesign should start from a fresh, companion-owned generation model
rather than restoring that code as-is.

# Local product support matrix

Primary product: one-command recovery of accidentally dropped small/medium
ordinary permanent LOGGED PostgreSQL tables (`local_delta`, COMMIT-LSN).

Classifications:

- **supported-preserved** — recovered with schema/data contract
- **supported-with-limit** — recovered with documented limits
- **fail-closed-rejected** — refused with actionable error
- **intentionally-unsupported** — out of first-release claim
- **pending-qualification** — code may exist; not yet proven under exact-WAL gates

This matrix is updated as Phase 1–5 cases land. Prefer code+tests over comments.

## Relation topologies

| Case | Class | Notes |
|---|---|---|
| Ordinary permanent LOGGED table (`relkind=r`) | supported-preserved | Product core |
| Partitioned parent / leaf partition | fail-closed-rejected | `flashback_require_supported_local_table` |
| Foreign table | fail-closed-rejected | |
| Materialized view | fail-closed-rejected | |
| TEMP / UNLOGGED | fail-closed-rejected | |
| Classical inheritance parent (has children) | fail-closed-rejected | Track-time; restore-time via manifest TBD |
| Other relkinds | fail-closed-rejected | |

## DROP selection / recovery planning

| Case | Class | Notes |
|---|---|---|
| Latest DROP by COMMIT LSN, restorable | pending-qualification → supported-preserved | Phase 1 selection fix |
| Latest DROP non-restorable/ambiguous | fail-closed-rejected | No older-DROP silent fallback |
| Older DROP via explicit event-id | pending-qualification | |
| Timestamp UX → proven LSN prefix | pending-qualification | Inversion test required |
| Advanced operator LSN | supported-with-limit | Escape hatch; not primary UX |
| Same-name live OID ≠ tracked OID | fail-closed-rejected | Identity conflict |

## CASCADE / dependencies

| Dependency | Report in manifest | Reconstruct | Class |
|---|---|---|---|
| Outgoing FK from target | yes | pending-qualification | |
| Incoming FK to target | yes | pending-qualification | Only if proven |
| Views / matviews on target | yes | pending-qualification | Only if proven |
| User triggers on target | yes | pending-qualification | |
| Owned sequences / identity | yes | supported-with-limit | Sequence checks without `nextval` |
| Inheritance children | yes | fail-closed-rejected | Conservative |
| Unknown / unproven kinds | yes | **never** | fail-closed before swap |

Authoritative manifest: ProcessUtility hook **before** `standard_ProcessUtility`,
same DROP transaction, while OIDs exist. Not WAL-decoder post-DROP catalog.

## HA / timeline

| Case | Class | Notes |
|---|---|---|
| Mutating API on standby (`pg_is_in_recovery`) | fail-closed-rejected | Phase 1 |
| Promote without proven slot continuity | fail-closed + gap/re-anchor | External fencing required |
| Split-brain fencing | intentionally-unsupported | Operator responsibility |
| Failover-slot emulation | intentionally-unsupported | Reject/document |

## Lifecycle

| Case | Class | Notes |
|---|---|---|
| `protect` / `flashback_track` | supported-preserved | |
| Destructive `flashback_untrack` | supported-with-limit | Deprecated vs unprotect/cleanup |
| Two-phase `unprotect` | pending-qualification | Phase 4 |
| `cleanup` by tracking_id | pending-qualification | Phase 4 |
| Re-protect after unprotect | supported-with-limit | New lifecycle only |

## Security / RBAC

| Case | Class | Notes |
|---|---|---|
| `pg_monitor` health/metadata | supported-preserved | |
| `pg_monitor` `old_data`/`new_data` | fail-closed-rejected | Phase 1 revoke |
| Row-change history for owners/sensitive role | pending-qualification | |
| Operation history for operators | pending-qualification | Phase 3 journal |

## Upgrade / packaging

| Case | Class | Notes |
|---|---|---|
| Fresh install 0.1.0 bootstrap | supported-preserved | |
| Versioned extension upgrade | pending-qualification | ADR in Phase 7 |
| Downgrade | intentionally-unsupported until proven | Refuse/document |
| PG major `pg_upgrade` | intentionally-unsupported | Separate decision |
| Candidate MANIFEST/SHA256SUMS | supported-preserved | |
| SBOM / reproducible builds | pending-qualification | Phase 8 |

## Backup-backed / large-DB

| Case | Class | Notes |
|---|---|---|
| pgBackRest helper path | intentionally-unsupported as local product claim | Keep tests; do not broaden claims |
| Second backup provider | intentionally-unsupported | |

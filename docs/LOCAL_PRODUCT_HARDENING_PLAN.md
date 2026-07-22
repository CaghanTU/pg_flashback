# Local product hardening — executable checklist

Branch: `work/v0.1.0-overnight-sanitized`  
Baseline HEAD at plan start: `8a43c994bb6b51472bddaf94bd3985339d0898fc`

This checklist tracks implementation of the local_delta DROP recovery hardening
program. Historical Gate C (`3b392ff`) and short-matrix (`0e65604`) evidence
remain historical only and must not qualify new binaries.

## Fingerprint semantics

| Kind | Meaning |
|---|---|
| Shadow/swap fingerprint | Self-consistency only (materialized shadow vs post-swap relation). |
| Fixture fingerprint | Historical correctness gate in qualification tests. |
| Sequence checks | Must not call `nextval` (no state change). |

## Phase status

| Phase | Title | Status | Notes |
|---|---|---|---|
| 0 | Audit docs | IN_PROGRESS | This file + support matrix |
| 1 | Correctness + security | PENDING | Latest-DROP by COMMIT LSN; ProcessUtility manifest; failpoints; HA; pg_monitor |
| 2 | Recover plan / dry-run | PENDING | No mutating execute grants |
| 3 | Journal + validation + execute grants | PENDING | Immutable header + append-only events |
| 4 | Two-phase unprotect + cleanup | PENDING | Stopping marker protocol |
| 5 | Unified exact-WAL matrix | PENDING | Candidate-archive consolidation |
| 6 | CLI / multi-DB / disk UX | PENDING | Monitoring cache ≠ authority |
| 7 | Upgrade ADR | PENDING | Version not pre-fixed |
| 8 | Package / SBOM | PENDING | Final binary identity |
| 9 | Exact benchmarks | PENDING | Bound to Phase 8 identity |
| 10 | Qualify freeze | PENDING | READY_TO_START; no 24h start |

## Exit criteria (summary)

See the Cursor plan “Local product hardening (final revision)” for full exit
criteria per phase. Do not mark COMPLETE without the listed proofs.

## Audit classifications (Phase 0)

| Item | Classification | Evidence basis |
|---|---|---|
| `flashback_history` row payloads to `pg_monitor` | present; security blocker | `api_track_capture.sql`, `rbac_grants.sql` |
| Destructive `flashback_untrack` | present; insufficient lifecycle | `api_track_capture.sql` |
| `restore_log` LSN path | present but insufficient | success-only insert in `restore_lsn.sql` |
| Recover plan / `--dry-run` | absent | CLI shell selection only |
| Latest-DROP selection | present but insufficient | silent older-restorable risk; generation `LIMIT 1` |
| Pre-DROP dependency manifest | absent | track-time topology only; swap recreates some deps |
| Local cancel mid-`restore_lsn` | insufficient | helper chaos only; no failpoint local path |
| `pg_is_in_recovery` on mutators | absent | |
| Operation journal | absent | |
| Versioned upgrade SQL | absent | Cargo `0.1.0`; tag `v0.4.0` historical mismatch |
| SBOM / SOURCE_DATE_EPOCH | absent | MANIFEST/SHA256SUMS exist |
| Exact WAL matrix (no fake triggers) | insufficient | `run_wal_e2e.sh` + many fake-trigger SQL tests |
| Benchmarks | present but invalid for claims | fake triggers / non-candidate paths |

## Non-negotiables

- No silent fallback from latest non-restorable DROP to an older restorable DROP.
- `plan_token` is identity/stale guard only; RBAC authorizes; execute recomputes.
- Manifest authority is ProcessUtility pre-DROP in the DROP transaction.
- `verified` is worker/finalizer-owned, not CLI-owned.
- Failpoints off in release defaults.
- Do not start the 24-hour Gate C soak from this program; produce READY_TO_START only.

# Retained FULL + continuous WAL PoC

Status: evidence harness for research only. **Not production-wired for v0.1.0.**

## Question

Can an existing retained FULL that completed **before** the tracking marker, plus
contiguous archived WAL through the requested target, physically recover the
exact table — including across a production shadow-swap — without changing the
supported activation contract?

## Correct recovery invariant

- verified system identifier and timeline/history
- immutable FULL backup/manifest identity
- `backup_stop_lsn <= target_lsn` (start-before-target alone is insufficient)
- continuous verified WAL for backup consistency and from backup stop through target
- no gap in the archive prefix
- physical anchor/dependencies and required WAL remain pinned
- a backup overlapping the target is not eligible merely because its start LSN is earlier

## Production contract (unchanged)

Activation still requires:

`marker COMMIT LSN < FULL start LSN < FULL stop LSN`

Scenario B/C success does **not** make older-FULL activation production-ready.

## Latest run

See `target/retained-full-wal-poc/results/<run_id>.json` from
`scripts/run_retained_full_wal_poc.sh` (latest local proof:
`20260717T161100Z-340138`).

| Scenario | Intent | Status |
|----------|--------|--------|
| A | FULL after marker (current contract) | passed |
| B | Retained FULL before marker + WAL | passed |
| C | WAL through production shadow-swap | passed |

### Measured phase timings (ms)

| Scenario | materialize | recovery/replay | extract | helper total | wall |
|----------|-------------|-----------------|---------|--------------|------|
| A | 23 | 327 | 78 | 506 | 629 |
| B | 22 | 219 | 68 | 384 | 514 |
| C | 47 | 648 | 80 | 855 | 988 |

WAL archive footprint observed for scenario B repository view: ~112 MiB
(`117445582` bytes). Fresher-anchor recommendation when replay dominates total
RTO: true (scenario C recovery was ~76% of helper total).

### Assertion coverage

| # | Assertion | Evidence |
|---|-----------|----------|
| 1 | Existing FULL before marker recovers at marker-era target | Scenario B |
| 2 | Missing required WAL fails closed | This harness |
| 3 | Wrong system ID fails closed | `scripts/run_recovery_helper_e2e.sh` |
| 4 | Timeline mismatch fails closed | `scripts/run_recovery_helper_e2e.sh` |
| 5 | Overlapping backup rejected (`stop > target`) | This harness + helper unit tests |
| 6 | Expire cannot remove pinned FULL | This harness |
| 7 | Replay reaches exact requested LSN | Scenarios A/B/C fingerprints |
| 8 | Fingerprint/schema/owner/ACL match | Scenarios A/B/C |
| 9 | WAL through production shadow-swap | Scenario C |
| 10 | Old vs new OID cannot be confused | Scenario C old-OID rejection |
| 11 | Phase timings recorded | Result JSON |
| 12 | Fresher-anchor recommendation when replay dominates | Result JSON |

**Production readiness:** PoC-only. Do not change the v0.1 FULL-after-marker
activation contract based on this evidence alone.

## Production wiring still required (if B/C remain proven)

1. Explicit eligibility policy for pre-marker FULL selection (opt-in, audited).
2. Durable pin of FULL + required WAL range independent of post-marker FULL labels.
3. Activation/proof path that distinguishes physical recoverability from the
   current marker-before-FULL operational contract.
4. Health/action reporting for long replay / fresher-anchor recommendation.
5. Expire coordination that never drops a pinned pre-marker FULL or its WAL prefix.
6. Qualification matrix and RC soak — do not ship on PoC evidence alone.

## Decision gate

- If A/B/C pass on a real pgBackRest repository: keep this document as the design
  recommendation; leave production FULL-after-marker behavior unchanged.
- If B or C cannot be proven: leave production unchanged and record the blocker
  in the result JSON `blockers` array.

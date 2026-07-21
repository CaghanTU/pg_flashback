# v0.1.0 pre-soak release freeze

Date: 2026-07-21

This file closes the qualification inventory before the final 24-hour run. It
exists to prevent a successful long run from being followed by a newly invented
test requirement.

## Closed inventory

The final candidate must pass all of the following **before** Gate C starts:

1. source quality: extension/helper fmt, clippy, helper tests, audit, deny,
   Bash syntax, ShellCheck and whitespace;
2. PostgreSQL 15–18 regression and release-package builds;
3. real WAL worker E2E, local capacity, retained FULL+WAL PoC/adversarial,
   backup coverage, anchor advancement, recovery-helper, capture/maintenance
   SLO, fault and WAL-overhead gates;
4. candidate archive legal/security/operator-content checks plus checksum and
   clean-host installation;
5. exact-candidate Gate A (functional), Gate B (chaos) and Gate D (dedicated
   destructive DROP qualification);
6. qualification harness self-test, provenance/identity checks, clean worktree,
   attribution scan and adequate bounded-work disk headroom.

Gate D, rather than Gate C's two scheduled probes or its DML volume, owns the
DROP recovery stress claim. Its default contract is 100 repeated local
DROP/restores plus indexed-medium, quoted/TOAST and pgBackRest-backed DROP
cases, with actual DROP count, rows/bytes, concurrency and latency recorded.

## The one remaining long test

After the inventory above passes against one clean packaged candidate, the only
remaining technical runtime gate is:

- **Gate C:** at least 86,400 active monotonic seconds of bounded stability on
  the exact same candidate package and installed binary hashes.

Gate C is a duration/stability gate. Destructive faults belong to Gate B and
DROP stress belongs to Gate D; neither is silently redefined after Gate C.

No additional test may be made a v0.1.0 blocker after Gate C starts unless one
of these invalidation conditions occurs:

- a gate fails;
- executable code, SQL, dependencies, packaging or a qualification script
  changes;
- the supported release scope is broadened.

If none occurs, a passing Gate C closes technical qualification for the frozen
scope. Documentation-only evidence recording may follow without rebuilding the
tested binaries, provided provenance verification proves that the later commit
changes documentation/evidence only.

## Known non-test work after Gate C

These are not surprise tests and do not require another 24-hour run unless they
change the frozen candidate:

- restore GitHub Actions billing/spending capacity and obtain hosted x86_64 CI,
  package and clean-host results before making an x86_64 artifact claim;
- review and merge the release candidate;
- create the signed RC/final tag, inspect the draft release, checksums and
  limitations, then publish manually;
- download the published artifacts and repeat checksum plus smoke installation.

## Architecture claim

The declared Gate C host class is PostgreSQL 17 on Linux/aarch64 under Lima on
an Apple Silicon host. PostgreSQL 15–18 regression/package compatibility and
x86_64 release artifacts are separate claims. This does not claim native macOS,
x86_64 24-hour stability, 100+ GiB performance, differential/incremental chains
or non-pgBackRest providers.

## Audit findings closed before freeze

The dedicated DROP gate found and fixed identity/sequence reconstruction and
structured JSON/JSONB/array replay. The pre-soak audit also fixed a retained-
FULL PoC post-success exit failure, corrected stale retained-anchor evidence,
and required legal/security/operator documents in exact candidate archives.


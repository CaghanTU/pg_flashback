# Qualification status

This document records the public preview's measured envelope. It is a concise
product summary, not a dump of local run logs. Raw logs and generated artifacts
remain outside the source tree and are meaningful only for the exact commit
and binary that produced them.

## Candidate identity

- Qualified development commit: `7b774764c9da977397d45ee26a0f48cc754786f3`
- Public-preview note: the Rust/SQL product source is unchanged from that
  candidate; only public documentation, packaging references, and obsolete
  PoC material were cleaned before publication
- Product profile: `local_delta` with `external_zstd` base-image storage
- PostgreSQL integration suite: 152 passed, 0 failed on each of PostgreSQL
  15, 16, 17, and 18
- Scale result: 10 GiB mixed-row and 10 GiB TOAST-heavy tiers passed
- Scale checks: 115 passed, 0 failed, run not interrupted

The scale harness bound each tier to its own recovery operation, DROP event,
successor generation, and expected/actual SHA-256 restore proof. Both tiers
ran with concurrent writes and produced identical pre-DROP and post-recovery
logical data fingerprints.

## Measured 10 GiB results

| Shape | Protect | Recover | Base artifact | Compression |
|---|---:|---:|---:|---:|
| Mixed rows | 832 s | 6,116 s | about 1.1 GiB | about 8.9x |
| TOAST-heavy | 3,094 s | 3,130 s | about 4.0 GiB | about 2.6x |

The restore-proof v2 change reduced measured `recover_execute` temporary
space for the mixed tier from roughly 58.1 GiB to roughly 9.5 GiB. The leaf
proof query fell from roughly 55 GiB to 5.7 GiB. Recovery wall time did not
materially improve; the change is a capacity improvement, not an RTO claim.

## What the result means

It demonstrates that the exact candidate recovered both tested 10 GiB table
shapes without a detected data or supported-metadata mismatch on that host.
It does not prove that every 10 GiB PostgreSQL table is supported or will have
the same runtime. Schema compatibility, row-size limits, change rate, storage,
indexes, TOAST compressibility, and PostgreSQL settings remain material.

Operators must run `pg_flashback config recommend TABLE` and validate the
result in staging on their own workload. The recovery path needs space for a
restored relation, a successor snapshot, temporary proof/index work, retained
WAL/deltas, and safety headroom at the same time.

## Claims deliberately not made

- No 25 GiB or 50 GiB qualification.
- No final 24-hour soak on this candidate.
- No claim of general live-table PITR, multi-table atomic recovery, or
  row-level undo.
- No claim that pg_flashback replaces physical/off-host backup.
- No performance extrapolation from this single qualification host.

The code remains an experimental technical preview until these limits and the
support contract are acceptable for a particular deployment.

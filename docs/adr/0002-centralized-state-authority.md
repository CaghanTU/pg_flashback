# ADR 0002 — Centralized state / progress / lock mutation authority

Status: Accepted
Date: 2026-07-24

## Context

Capture streams, coverage generations, payload retirements, and operation
journal events were mutated from many SQL call sites. That made illegal or
backward transitions, lock-order mistakes, and false-healthy progress advances
hard to reason about. Constraint triggers already encoded the generation graph,
but they were a last line of defense rather than the normal API.

## Decision

1. Add `sql/functions/state_authority.sql` as the only runtime authority for
   lifecycle state transitions and initial row construction (aside from schema bootstrap DDL).
2. Provide narrow primitives (no generic `flashback_set_state`):
   - capture stream transition + progress advance
   - coverage generation transition + watermark advance
   - payload retirement transition
   - typed constructors (`create_capture_stream`, `create_coverage_generation`, `create_retirement_intent`)
   - canonical advisory lock hierarchy helpers (`lock_database_stream`, `lock_lifecycles`)
3. Operation headers (`flashback.operations`) are strictly **immutable** once inserted by `flashback_operation_begin()`. All restore proof, verification status, and exact successor bindings are written as append-only payloads into `flashback.operation_events`.
4. Domain commands take canonical locks, invoke state authority primitives, and apply side effects in a single transaction.
5. Keep existing trigger/CHECK guards unchanged as defense-in-depth.
6. Enforce state authority isolation using `scripts/check_centralized_state_surface.sh` in CI with a multiline lexical scanner and automated selftests.

## Consequences

- Runtime SQL outside `state_authority.sql` must not execute direct `SET state=` or initial state `INSERT`s.
- Internal primitives and constructors are REVOKE-by-default from `PUBLIC`, `flashback_admin`, and `pg_monitor`.
- Operation headers are immutable; journal state projections are derived strictly from `operation_events`.
- Public APIs (`protect`, `recover`, `unprotect`, `cleanup`, `maintain`) remain the official operator surface.
- Progress updates soft-refuse on broken streams or open gaps to prevent false-healthy progress.

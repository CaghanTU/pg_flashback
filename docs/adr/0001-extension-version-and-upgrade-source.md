# ADR 0001 — Extension version and supported upgrade source

Status: Accepted  
Date: 2026-07-23  
Branch: `work/v0.1.0-overnight-sanitized`

## Context

Audit of distributed identities:

| Identity | Observed | Notes |
|---|---|---|
| Cargo / control `default_version` | `0.1.0` (pre-ADR tip) | Source of truth for CREATE EXTENSION |
| Git tag | `v0.4.0` only | Historical packaging tag; **not** equal to extension `default_version` |
| GitHub releases | none beyond tag audit at ADR time | Do not treat tag as upgrade source |
| Schema compatibility | additive catalog for hardening phases 1–6 | Needs versioned upgrade SQL |

The hardening plan forbids pre-fixing `0.1.1`. Upgrade must be chosen from evidence.

## Decision

1. **Next extension version:** `0.2.0`
2. **Supported upgrade source:** `0.1.0` → `0.2.0` only
3. **Unsupported:** upgrading from Git tag `v0.4.0` as if it were control version `0.4.0`; unknown versions; downgrade
4. **Downgrade policy:** refuse / document — operators must restore from backup or reinstall; no `0.2.0` → `0.1.0` SQL
5. **PG major `pg_upgrade`:** out of scope; fail-closed / separate program
6. **Upgrade artifact:** `sql/upgrades/pg_flashback--0.1.0--0.2.0.sql` packaged beside the extension SQL

## Consequences

- Tip Cargo version becomes `0.2.0` after this ADR lands.
- Phase 8 candidate archives bind to `0.2.0` binaries.
- Historical Gate C / short-matrix soaks remain historical and do not qualify the new version.
- Clean-host path: install `0.2.0` OR install `0.1.0` then `ALTER EXTENSION pg_flashback UPDATE TO '0.2.0'`.

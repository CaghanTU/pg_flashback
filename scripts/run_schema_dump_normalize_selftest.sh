#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/schema_dump_normalize.sh
source "$ROOT/scripts/lib/schema_dump_normalize.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat >"$WORK/a.sql" <<'EOF'
\restrict first-random-token
-- Dumped from database version 17
-- Started on 2026-07-29 10:00:00 UTC
CREATE TABLE public.orders (id bigint);
\unrestrict first-random-token
-- Completed on 2026-07-29 10:00:01 UTC
EOF

cat >"$WORK/b.sql" <<'EOF'
\restrict second-random-token
-- Dumped from database version 17
-- Started on 2026-07-29 11:00:00 UTC
CREATE TABLE public.orders (id bigint);
\unrestrict second-random-token
-- Completed on 2026-07-29 11:00:01 UTC
EOF

normalize_schema_dump_in_place "$WORK/a.sql"
normalize_schema_dump_in_place "$WORK/b.sql"

cmp -s "$WORK/a.sql" "$WORK/b.sql" || {
    diff -u "$WORK/a.sql" "$WORK/b.sql" || true
    echo "FAIL: randomized pg_dump wrapper/timestamp lines were not normalized" >&2
    exit 1
}

grep -Fxq 'CREATE TABLE public.orders (id bigint);' "$WORK/a.sql" || {
    echo "FAIL: deterministic schema content was removed" >&2
    exit 1
}

if grep -Eq '^\\(un)?restrict |^-- (Dumped|Started|Completed) ' "$WORK/a.sql"; then
    echo "FAIL: nondeterministic pg_dump content survived normalization" >&2
    exit 1
fi

echo "schema dump normalization self-test: PASS"

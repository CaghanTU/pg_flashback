#!/usr/bin/env bash
# Reject a shadowed second definition of flashback_apply_retention() in the
# generated extension SQL.
#
# pgrx concatenates sql/functions/*.sql in dependency order, so two files that
# define the same signature both reach the install script and the later
# CREATE OR REPLACE silently wins. Retention was in exactly that state: a
# legacy generation-unaware body in api_track_capture.sql was shadowed by the
# canonical coordinator in retention_runtime.sql, which made correctness depend
# on the pgrx `requires` graph rather than on the source being unambiguous.
#
# This check is deliberately narrow. Function overloads (same name, different
# argument lists) are legitimate and must keep passing, so the zero-argument
# signature is matched exactly. A general duplicate-definition lint over every
# signature belongs with the centralization work, not here.
#
# Usage:
#   ./scripts/check_generated_sql_duplicates.sh <generated-extension.sql>
#
# Generate the input with:
#   cargo pgrx schema pg17 --no-default-features --features pg17 -o out.sql
#
# Exit 0 when exactly one definition is present; non-zero otherwise.

set -Eeuo pipefail

GENERATED="${1:-}"

die() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

if [[ -z "$GENERATED" ]]; then
    echo "usage: $0 <generated-extension.sql>" >&2
    exit 2
fi

[[ -f "$GENERATED" ]] || die "generated SQL not found: $GENERATED"

# Zero-argument signature only, with or without schema qualification.
PATTERN='^[[:space:]]*CREATE[[:space:]]+OR[[:space:]]+REPLACE[[:space:]]+FUNCTION[[:space:]]+(public\.)?flashback_apply_retention[[:space:]]*\([[:space:]]*\)'

matches="$(grep -nE "$PATTERN" "$GENERATED" || true)"

if [[ -z "$matches" ]]; then
    count=0
else
    count="$(printf '%s\n' "$matches" | wc -l | tr -d ' ')"
fi

if [[ "$count" -ne 1 ]]; then
    {
        echo "FAIL: expected exactly 1 definition of flashback_apply_retention()," \
             "found $count in $GENERATED"
        if [[ -n "$matches" ]]; then
            echo "definitions:"
            printf '%s\n' "$matches" | sed 's/^/  line /'
        fi
        echo "The canonical body lives in sql/functions/retention_runtime.sql."
    } >&2
    exit 1
fi

ok "flashback_apply_retention() has exactly one definition in $(basename "$GENERATED")"

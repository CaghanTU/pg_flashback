#!/usr/bin/env bash
# A3 regression guard: flashback_build_expected_restore_proof and
# flashback_build_actual_restore_proof must derive their inventory_digest from
# DIFFERENT sources -- expected from schema_def (the pre-swap shadow's
# captured metadata), actual from the live post-swap catalog. The bug this
# guards against: flashback_build_actual_restore_proof once recomputed its
# digest via flashback_canonical_inventory_from_schema_def(p_schema_def) --
# the SAME source the expected side already used -- making verification
# tautological (it could never fail for real catalog drift, only for
# schema_def disagreeing with itself). A live-catalog corruption test alone
# would not have caught a future regression back to that shape if the
# regression happened to still pass every *value*-level assertion by
# coincidence; this checks the SOURCE CODE shape directly.
#
# Usage:
#   ./scripts/check_restore_proof_independence.sh
#
# Exits 0 if the independence property holds in sql/functions/restore_verify.sql;
# non-zero otherwise.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FILE="$ROOT/sql/functions/restore_verify.sql"

die() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

[[ -f "$FILE" ]] || die "not found: $FILE"

extract_body() {
    local fn_name="$1"
    awk -v fn="$fn_name" '
        $0 ~ ("^CREATE OR REPLACE FUNCTION " fn "\\(") { capturing = 1 }
        capturing { print }
        capturing && /^\$\$;/ { exit }
    ' "$FILE"
}

expected_body="$(extract_body 'flashback_build_expected_restore_proof')"
actual_body="$(extract_body 'flashback_build_actual_restore_proof')"

[[ -n "$expected_body" ]] || die "flashback_build_expected_restore_proof not found in $FILE"
[[ -n "$actual_body" ]] || die "flashback_build_actual_restore_proof not found in $FILE"

# --- expected side: must derive from schema_def, never from the live relation ---
if ! grep -q 'flashback_canonical_inventory_from_schema_def' <<<"$expected_body"; then
    die "flashback_build_expected_restore_proof no longer calls" \
        "flashback_canonical_inventory_from_schema_def -- expected proof must be schema_def-sourced"
fi
if grep -q 'flashback_canonical_inventory_from_relation' <<<"$expected_body"; then
    die "flashback_build_expected_restore_proof calls" \
        "flashback_canonical_inventory_from_relation -- expected proof must never read the live catalog" \
        "(that would make it and the actual proof derive from the same live source)"
fi

# --- actual side: must derive from the live relation, never re-derive from schema_def ---
if ! grep -q 'flashback_canonical_inventory_from_relation' <<<"$actual_body"; then
    die "flashback_build_actual_restore_proof no longer calls" \
        "flashback_canonical_inventory_from_relation -- actual proof must be live-catalog-sourced"
fi
if grep -q 'flashback_canonical_inventory_from_schema_def' <<<"$actual_body"; then
    die "flashback_build_actual_restore_proof calls" \
        "flashback_canonical_inventory_from_schema_def -- this is exactly the tautological-verification" \
        "regression: the actual proof would derive from the same source as the expected proof"
fi

# --- the digest itself must be computed from that function's own inventory
# variable, not by re-deriving another canonical_inventory_from_* call inline
# inside the inventory_digest(...) argument ---
if grep -qE "inventory_digest\([[:space:]]*(public\.)?flashback_canonical_inventory_from_" <<<"$actual_body"; then
    die "flashback_build_actual_restore_proof computes inventory_digest directly from a" \
        "flashback_canonical_inventory_from_* call rather than its own already-built inventory variable"
fi

ok "flashback_build_expected_restore_proof is schema_def-sourced only"
ok "flashback_build_actual_restore_proof is live-catalog-sourced only"
ok "expected/actual restore proofs derive from independent sources ($FILE)"

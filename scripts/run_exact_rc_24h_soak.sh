#!/usr/bin/env bash
# Exact-candidate qualification orchestrator.
#
# Gates:
#   A) exact-candidate functional suite (optional via PG_FLASHBACK_RUN_FUNCTIONAL=1)
#   B) exact-candidate chaos suite (PG_FLASHBACK_CHAOS_SUITE=once|off)
#   C) 24h exact-candidate bounded stability soak
#
# Required:
#   CANDIDATE_DIR  directory containing MANIFEST.json + archives
#
# CHAOS_ONLY=1 runs Gate B only (NOT a 24h soak).
# Orchestrator delegates only to packaged functional/chaos/stability suites.
#
# Claim language (when Gate B + Gate C both pass on aarch64/Lima):
#   "24-hour exact-candidate bounded stability soak plus separate exact-candidate
#    chaos suite on Linux/aarch64 under Lima on an Apple Silicon host."

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
[[ -d "$CANDIDATE_DIR" ]] || { echo "FAIL: CANDIDATE_DIR missing: $CANDIDATE_DIR" >&2; exit 2; }
[[ -f "$CANDIDATE_DIR/MANIFEST.json" ]] || { echo "FAIL: MANIFEST.json missing" >&2; exit 2; }

export CANDIDATE_DIR
export PGBACKREST="${PGBACKREST:-/usr/local/bin/pgbackrest}"
export PG_FLASHBACK_REQUIRE_CLEAN_TREE="${PG_FLASHBACK_REQUIRE_CLEAN_TREE:-1}"

CHAOS_SUITE="${PG_FLASHBACK_CHAOS_SUITE:-once}"
CHAOS_ONLY="${PG_FLASHBACK_CHAOS_ONLY:-0}"
RUN_FUNCTIONAL="${PG_FLASHBACK_RUN_FUNCTIONAL:-0}"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$CANDIDATE_DIR/MANIFEST.json")"
PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$CANDIDATE_DIR/MANIFEST.json")"
echo "exact-candidate orchestrator bound to CANDIDATE_DIR=$CANDIDATE_DIR"
echo "source_commit=$SOURCE_COMMIT package_sha256=$PACKAGE_SHA"

if [[ "$CHAOS_ONLY" == "1" ]]; then
    echo "CHAOS_ONLY=1: running Gate B only (NOT a 24h soak)"
    exec "$ROOT/scripts/run_exact_rc_chaos_suite.sh"
fi

if [[ "$RUN_FUNCTIONAL" == "1" ]]; then
    echo "running Gate A: exact-candidate functional suite"
    "$ROOT/scripts/run_exact_candidate_functional_suite.sh"
fi

if [[ "$CHAOS_SUITE" == "once" ]]; then
    echo "running Gate B: exact-candidate chaos suite (once)"
    "$ROOT/scripts/run_exact_rc_chaos_suite.sh"
elif [[ "$CHAOS_SUITE" != "off" ]]; then
    echo "FAIL: PG_FLASHBACK_CHAOS_SUITE must be once|off" >&2
    exit 2
fi

echo "running Gate C: exact-candidate 24h stability soak"
echo "NOTE: this is a bounded stability soak, not continuous chaos."
exec "$ROOT/scripts/run_exact_rc_24h_stability_soak.sh"

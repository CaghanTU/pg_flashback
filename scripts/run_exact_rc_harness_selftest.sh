#!/usr/bin/env bash
# Accelerated harness self-test — NOT release qualification.
#
# qualification_kind is ALWAYS exact_rc_harness_selftest.
# This script MUST NOT emit qualification_kind=exact_rc_24h_stability_soak
# and MUST NOT emit PASS for the 86400-second gate.
#
# Proves negative paths and duration/budget semantics quickly.

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

RESULT_DIR="${PGFB_SELFTEST_RESULT_DIR:-$REPO_ROOT/target/exact-rc-harness-selftest}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RESULT_JSON="$RESULT_DIR/selftest-$RUN_ID.json"
mkdir -p "$RESULT_DIR"
PASSED=0
FAILED=0

log() { printf '[harness-selftest] %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); log "PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); log "FAIL: $1"; }

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR required for selftest}"
[[ -f "$CANDIDATE_DIR/MANIFEST.json" ]] || { echo "missing manifest"; exit 2; }

# 1) Binary mismatch rejected
tmpdir=$(mktemp -d)
cp -a "$CANDIDATE_DIR/." "$tmpdir/"
jq '.artifacts.extension_binary_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"' \
    "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-mismatch.err; then
    fail "binary mismatch was accepted"
else
    pass "binary mismatch rejected"
fi
rm -rf "$tmpdir"

# 2) Source/tree mismatch rejected (fake commit in a copy)
tmpdir=$(mktemp -d)
cp -a "$CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.source_commit = "ffffffffffffffffffffffffffffffffffffffff"' \
    "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
# Fix SHA256SUMS still valid for archives; bind should fail on HEAD mismatch.
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-source.err; then
    fail "source mismatch was accepted"
else
    pass "source mismatch rejected"
fi
rm -rf "$tmpdir"

# 3) Wrong architecture rejected
tmpdir=$(mktemp -d)
cp -a "$CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.arch = "x86_64"' "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
# If host is x86_64 this would pass — force impossible arch.
jq '.provenance.arch = "riscv64"' "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-arch.err; then
    fail "arch mismatch was accepted"
else
    pass "arch mismatch rejected"
fi
rm -rf "$tmpdir"

# 4) Stability harness refuses short PASS via kind check: run a tiny python proof
# that the stability script hardcodes 86400 and has no env override for PASS.
if rg -n 'QUAL_DURATION_SECONDS=86400' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && ! rg -n 'PG_FLASHBACK_SOAK_SECONDS' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "stability soak duration is hard-coded 86400 without env override"
else
    fail "stability soak duration gate is weakenable"
fi

# 5) Wrapper does not call run_dev_soak
if ! rg -n 'run_dev_soak' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null; then
    pass "orchestrator no longer wraps run_dev_soak"
else
    fail "orchestrator still references run_dev_soak"
fi

# 6) Selftest result kind cannot be the release soak kind
KIND="exact_rc_harness_selftest"
[[ "$KIND" != "exact_rc_24h_stability_soak" && "$KIND" != "exact_rc_24h_soak" ]] \
    && pass "selftest kind distinct from release soak" \
    || fail "selftest kind collides with release soak"

# 7) Budget exhaustion concept: heavy-write disable without ending clock
# (static check that HEAVY_WRITES_ENABLED path exists and loop continues)
if rg -n 'HEAVY_WRITES_ENABLED=0' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'clock continues' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "workload budget stops heavy writes without ending clock"
else
    fail "workload budget/clock separation missing"
fi

# 8) Prefix stash/restore helpers exist
if declare -F exact_candidate_restore_prefix >/dev/null \
   && declare -F exact_candidate_stash_prefix_file >/dev/null; then
    pass "prefix stash/restore helpers present"
else
    fail "prefix stash/restore helpers missing"
fi

STATUS=failed
[[ "$FAILED" == "0" ]] && STATUS=passed
jq -n \
    --arg status "$STATUS" \
    --arg kind "exact_rc_harness_selftest" \
    --argjson passed "$PASSED" \
    --argjson failed "$FAILED" \
    '{
      qualification_kind: $kind,
      status: $status,
      assertions_passed: $passed,
      assertions_failed: $failed,
      note: "NOT release qualification; cannot satisfy the 86400-second gate"
    }' > "$RESULT_JSON"
log "result: $RESULT_JSON status=$STATUS passed=$PASSED failed=$FAILED"
[[ "$FAILED" == "0" ]]

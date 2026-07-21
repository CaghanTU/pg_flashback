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

ORIG_CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR required for selftest}"
ORIG_CANDIDATE_DIR="$(cd "$ORIG_CANDIDATE_DIR" && pwd)"
[[ -f "$ORIG_CANDIDATE_DIR/MANIFEST.json" ]] || { echo "missing manifest"; exit 2; }

# 1) Binary mismatch rejected
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
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
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 2) Source/tree mismatch rejected (fake commit in a copy)
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.source_commit = "ffffffffffffffffffffffffffffffffffffffff"' \
    "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-source.err; then
    fail "source mismatch was accepted"
else
    pass "source mismatch rejected"
fi
rm -rf "$tmpdir"
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 3) Wrong architecture rejected
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.arch = "riscv64"' "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-arch.err; then
    fail "arch mismatch was accepted"
else
    pass "arch mismatch rejected"
fi
rm -rf "$tmpdir"
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 4) Stability harness refuses short PASS via kind check: run a tiny python proof
# that the stability script hardcodes 86400 and has no env override for PASS.
if rg -n 'QUAL_DURATION_SECONDS=86400' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && ! rg -n 'PG_FLASHBACK_SOAK_SECONDS' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "stability soak duration is hard-coded 86400 without env override"
else
    fail "stability soak duration gate is weakenable"
fi

# 5) Wrapper does not invoke the development soak harness
if ! rg -n '^[^#]*run_dev_soak\.sh' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null \
   && rg -n 'run_exact_rc_24h_stability_soak' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null; then
    pass "orchestrator delegates to dedicated stability soak"
else
    fail "orchestrator still invokes development soak or lacks stability soak"
fi

# 6) Selftest result kind cannot be the release soak kind
KIND="exact_rc_harness_selftest"
[[ "$KIND" != "exact_rc_24h_stability_soak" && "$KIND" != "exact_rc_24h_soak" ]] \
    && pass "selftest kind distinct from release soak" \
    || fail "selftest kind collides with release soak"

# 7) Budget exhaustion concept: early heavy-write stop, then hard ceiling
if rg -n 'HEAVY_WRITES_ENABLED=0' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'HEAVY_WRITE_STOP_BYTES' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'exceeded hard MAX_WORK_BYTES' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "workload budget soft-stops with headroom and enforces a hard ceiling"
else
    fail "workload budget headroom/hard ceiling missing"
fi

# 8) Prefix stash/restore helpers exist
if declare -F exact_candidate_restore_prefix >/dev/null \
   && declare -F exact_candidate_stash_prefix_file >/dev/null; then
    pass "prefix stash/restore helpers present"
else
    fail "prefix stash/restore helpers missing"
fi

# 9) Accelerated wrapper is development-only and exact mode remains fixed.
if rg -n 'PG_FLASHBACK_STABILITY_MODE=accelerated' "$REPO_ROOT/scripts/run_development_stability_15m.sh" >/dev/null \
   && rg -n 'development_accelerated_stability' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'QUAL_DURATION_SECONDS=86400' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "accelerated drill mode is distinct from fixed 24-hour qualification"
else
    fail "accelerated mode can collide with exact qualification"
fi

# 10) Local restore must wait for source and mutation LSNs before restore.
if [[ "$(rg -c 'wait_for_delta_lsn_after \"public.restore_probe\"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh")" -ge 2 ]] \
   && [[ "$(rg -c 'wait_for_coverage_lsn \"public.restore_probe\"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh")" -ge 2 ]]; then
    pass "local restore waits for captured and covered source/mutation LSNs"
else
    fail "local restore LSN wait discipline missing"
fi

# 11) Early/late DROP drills use distinct tracking identities.
if rg -n 'local table_name="drop_probe_\$\{tag\}"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "DROP drills use distinct relations instead of retracking one lifecycle"
else
    fail "DROP drills can collide on one tracking lifecycle"
fi

# 12) DROP product claims come from a dedicated destructive gate, not Gate C's
# DML counters or its two tiny scheduled probes.
if rg -n 'PGFB_DROP_REPEAT_COUNT:-100' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'perform_local_drop_restore repeated_same_lifecycle' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'record_case backup retained_full_plus_wal' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'cumulative_rows_at_drop' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null; then
    pass "dedicated DROP gate measures repeated local and backup-backed destruction"
else
    fail "dedicated DROP claim gate is missing or under-specified"
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

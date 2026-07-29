#!/usr/bin/env bash
# Harness self-test for run_exact_wal_restart_recovery_adversarial.sh's
# result-JSON integrity -- NOT release qualification.
#
# Runs the real adversarial script end to end against a real candidate and a
# real postgres instance, with PGFB_RESTART_ADV_FORCE_FAIL=A2 forcing a
# deterministic mid-run die() inside case A2 (after A1 has already passed,
# before B ever starts). Then asserts the emitted result JSON has no
# internal contradiction:
#   - A1 is recorded pass:true
#   - A2 is recorded pass:false, kind:"product", with a reason
#   - B is recorded pass:false, kind:"not_run" (never reached)
#   - top-level failed count, status, and exit_code all agree with each other
#   - the script's own process exit code is non-zero
#
# This is what caught the original bug: die() used to just `exit 1` without
# ever touching CASE_RESULTS/FAILED, so a genuine mid-run failure produced
# passed=N, failed=0, status=FAIL, exit_code=1 -- internally contradictory
# (status/exit_code said failure, passed/failed said none). This selftest
# would fail loudly against that old behavior.
#
# Usage: CANDIDATE_DIR=/path/to/candidate ./scripts/run_exact_wal_restart_adversarial_harness_selftest.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RESULT_JSON="$ROOT/target/qualification/exact-wal-restart-adversarial-selftest-$RUN_ID.json"

log() { printf '[restart-adv-harness-selftest] %s\n' "$*"; }
FAILED=0
check() {
    local desc=$1 got=$2 want=$3
    if [[ "$got" == "$want" ]]; then
        log "PASS: $desc"
    else
        log "FAIL: $desc (got=$got want=$want)"
        FAILED=$((FAILED + 1))
    fi
}

set +e
PGFB_RESTART_ADV_FORCE_FAIL=A2 \
    PGFB_RESTART_ADV_RESULT="$RESULT_JSON" \
    CANDIDATE_DIR="$CANDIDATE_DIR" \
    "$ROOT/scripts/run_exact_wal_restart_recovery_adversarial.sh" >"$ROOT/target/qualification/.selftest-run-$RUN_ID.log" 2>&1
SCRIPT_RC=$?
set -e

[[ -f "$RESULT_JSON" ]] || {
    log "FAIL: no result JSON was written at all: $RESULT_JSON"
    cat "$ROOT/target/qualification/.selftest-run-$RUN_ID.log" >&2
    exit 1
}

check "forced run's own process exit code is non-zero" \
    "$([[ "$SCRIPT_RC" -ne 0 ]] && echo nonzero || echo zero)" "nonzero"

JSON_STATUS="$(jq -r '.status' "$RESULT_JSON")"
JSON_EXIT_CODE="$(jq -r '.exit_code' "$RESULT_JSON")"
JSON_FAILED="$(jq -r '.failed' "$RESULT_JSON")"
JSON_PASSED="$(jq -r '.passed' "$RESULT_JSON")"

check "top-level status is FAIL" "$JSON_STATUS" "FAIL"
check "top-level exit_code is non-zero" \
    "$([[ "$JSON_EXIT_CODE" != "0" ]] && echo nonzero || echo zero)" "nonzero"
# The core contradiction this selftest exists to catch: status/exit_code
# say failure but failed==0. Require failed >= 1 whenever status is FAIL.
check "top-level failed count is >= 1 (matches FAIL status)" \
    "$([[ "$JSON_FAILED" -ge 1 ]] && echo ok || echo bad)" "ok"
check "top-level passed count is exactly 1 (A1 only)" "$JSON_PASSED" "1"

A1_PASS="$(jq -r '.cases[] | select(.name=="A1") | .pass' "$RESULT_JSON")"
A2_PASS="$(jq -r '.cases[] | select(.name=="A2") | .pass' "$RESULT_JSON")"
A2_KIND="$(jq -r '.cases[] | select(.name=="A2") | .kind' "$RESULT_JSON")"
A2_REASON="$(jq -r '.cases[] | select(.name=="A2") | .reason' "$RESULT_JSON")"
B_PASS="$(jq -r '.cases[] | select(.name=="B") | .pass' "$RESULT_JSON")"
B_KIND="$(jq -r '.cases[] | select(.name=="B") | .kind' "$RESULT_JSON")"

check "case A1 recorded pass:true (ran and passed before the forced failure)" "$A1_PASS" "true"
check "case A2 recorded pass:false" "$A2_PASS" "false"
check "case A2 recorded kind:product (a product-path die(), not harness setup)" "$A2_KIND" "product"
check "case A2 reason mentions the forced failpoint" \
    "$([[ "$A2_REASON" == *"forced failure"* ]] && echo yes || echo no)" "yes"
check "case B recorded pass:false (never reached)" "$B_PASS" "false"
check "case B recorded kind:not_run (distinguishes it from a product failure)" "$B_KIND" "not_run"

CASE_COUNT="$(jq '.cases | length' "$RESULT_JSON")"
check "exactly 3 cases recorded (A1, A2, B -- every planned case, none silently dropped)" \
    "$CASE_COUNT" "3"

rm -f "$ROOT/target/qualification/.selftest-run-$RUN_ID.log"

if [[ "$FAILED" == "0" ]]; then
    log "result: $RESULT_JSON status=passed"
    exit 0
else
    log "result: $RESULT_JSON status=failed failed_checks=$FAILED"
    exit 1
fi

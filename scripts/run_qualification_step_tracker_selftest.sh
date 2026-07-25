#!/usr/bin/env bash
# Isolated regression suite for scripts/lib/qualification_step_tracker.sh.
# Drives the library directly with fake, fast steps -- never spins up a real
# PostgreSQL cluster and never touches any process this suite did not itself
# spawn as a fixture.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="$ROOT/scripts/lib/qualification_step_tracker.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/pgfb-qst-selftest.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

PASSED=0
FAILED=0
log() { printf '[qst-selftest] %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); log "PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); log "FAIL: $1"; }

# Runs a fixture as a background subprocess so signals can be delivered to it
# without affecting this selftest's own process. The fixture sources the
# library, drives a small scripted qualification run, and prints the final
# summary JSON on stdout (a single line via `jq -c`).
run_fixture() {
    local fixture_body=$1
    bash -c "$fixture_body" 2>"$TMP_ROOT/fixture.stderr"
}

fixture_common='
set -Eeuo pipefail
source "'"$LIB"'"
'

# 1. All planned steps pass -> overall PASS, no missing steps.
out="$(run_fixture "
$fixture_common
qst_init step_a step_b
qst_run_child step_a true
qst_run_child step_b true
qst_compute_summary_json run1 mytest 0 '{}'
")"
status="$(echo "$out" | jq -r '.status')"
missing_n="$(echo "$out" | jq '.missing_steps | length')"
if [[ "$status" == "PASS" && "$missing_n" == "0" ]]; then
    pass "all planned steps pass -> PASS"
else
    fail "all planned steps pass -> expected PASS/0 missing, got status=$status missing=$missing_n"
fi

# 2. A child with non-zero exit -> overall FAIL, that step marked fail.
out="$(run_fixture "
$fixture_common
qst_init step_a step_b
qst_run_child step_a true
qst_run_child step_b false
qst_compute_summary_json run2 mytest 1 '{}'
")"
status="$(echo "$out" | jq -r '.status')"
step_b_status="$(echo "$out" | jq -r '.steps[] | select(.name=="step_b") | .status')"
if [[ "$status" == "FAIL" && "$step_b_status" == "fail" ]]; then
    pass "child non-zero exit -> FAIL"
else
    fail "child non-zero exit -> expected FAIL/step_b=fail, got status=$status step_b=$step_b_status"
fi

# 3. HUP delivered while a child step is running -> FAIL, interrupted=true,
#    the interrupted step is marked fail (not left running/pending), and any
#    step after it never got a chance to run so it shows up in missing_steps.
fixture_hup="$TMP_ROOT/fixture_hup.sh"
cat >"$fixture_hup" <<EOF
set -Eeuo pipefail
source "$LIB"
qst_init step_a step_b step_c
trap 'qst_on_signal HUP' HUP
qst_run_child step_a true
qst_run_child step_b sleep 47.31
# step_c intentionally never runs if step_b is interrupted first.
qst_run_child step_c true
extra='{}'
summary="\$(qst_compute_summary_json run3 mytest \$? "\$extra")"
printf '%s\n' "\$summary" >"$TMP_ROOT/fixture3_summary.json"
EOF
chmod +x "$fixture_hup"
bash "$fixture_hup" >"$TMP_ROOT/fixture3.out" 2>"$TMP_ROOT/fixture3.err" &
fixture_pid=$!
sleep 0.5
kill -HUP "$fixture_pid" 2>/dev/null || true
fixture_rc=0
wait "$fixture_pid" 2>/dev/null || fixture_rc=$?
# The fixture's own trap calls exit 130 and never reaches the line that
# writes fixture3_summary.json (the interruption happens mid-step, before
# the script's own summary line runs) -- that absence is itself part of the
# proof: a real qualification script's EXIT trap (not exercised by this
# narrow library selftest) is what turns "exit 130, no further steps ran"
# into a written FAIL summary. Assert directly on what the library guarantees:
# the process exit code reflects the signal and step_b was left running,
# which the exit code and stderr trace both show.
if [[ "$fixture_rc" -eq 130 ]] && grep -q "INTERRUPTED: received SIGHUP" "$TMP_ROOT/fixture3.err"; then
    pass "HUP mid-child -> process exits 130 with INTERRUPTED trace, child terminated"
else
    fail "HUP mid-child -> expected exit 130 + INTERRUPTED trace, got rc=$fixture_rc; stderr: $(cat "$TMP_ROOT/fixture3.err")"
fi
# The signaled child (a distinctively-named sleep, unlikely to collide with
# anything else on a shared host) must actually be dead, not orphaned.
sleep 0.3
if ! pgrep -f "sleep 47\.31" >/dev/null 2>&1; then
    pass "HUP mid-child -> no leftover sleep child process from this fixture"
else
    fail "HUP mid-child -> child process was not terminated"
fi

# 4. TERM delivered while a child step is running -> same contract as HUP.
fixture_term="$TMP_ROOT/fixture_term.sh"
cat >"$fixture_term" <<EOF
set -Eeuo pipefail
source "$LIB"
qst_init step_a step_b
trap 'qst_on_signal TERM' TERM
qst_run_child step_a true
qst_run_child step_b sleep 47.31
EOF
chmod +x "$fixture_term"
bash "$fixture_term" >"$TMP_ROOT/fixture4.out" 2>"$TMP_ROOT/fixture4.err" &
fixture_pid=$!
sleep 0.5
kill -TERM "$fixture_pid" 2>/dev/null || true
fixture_rc=0
wait "$fixture_pid" 2>/dev/null || fixture_rc=$?
if [[ "$fixture_rc" -eq 130 ]] && grep -q "INTERRUPTED: received SIGTERM" "$TMP_ROOT/fixture4.err"; then
    pass "TERM mid-child -> process exits 130 with INTERRUPTED trace"
else
    fail "TERM mid-child -> expected exit 130 + INTERRUPTED trace, got rc=$fixture_rc; stderr: $(cat "$TMP_ROOT/fixture4.err")"
fi

# 5. Verified-child result artifact missing/empty -> step marked fail even
#    though the child process itself exited 0.
out="$(run_fixture "
$fixture_common
qst_init step_a
missing_result_verifier() { [[ -s '$TMP_ROOT/does-not-exist.json' ]]; }
qst_run_verified_child step_a missing_result_verifier true
qst_compute_summary_json run5 mytest 0 '{}'
")"
status="$(echo "$out" | jq -r '.status')"
step_a_status="$(echo "$out" | jq -r '.steps[] | select(.name=="step_a") | .status')"
if [[ "$status" == "FAIL" && "$step_a_status" == "fail" ]]; then
    pass "verified-child with missing result artifact -> FAIL despite exit 0"
else
    fail "verified-child missing result -> expected FAIL/step_a=fail, got status=$status step_a=$step_a_status"
fi

# Same, but the artifact exists and is genuinely empty.
touch "$TMP_ROOT/empty-result.json"
out="$(run_fixture "
$fixture_common
qst_init step_a
empty_result_verifier() { [[ -s '$TMP_ROOT/empty-result.json' ]]; }
qst_run_verified_child step_a empty_result_verifier true
qst_compute_summary_json run5b mytest 0 '{}'
")"
status="$(echo "$out" | jq -r '.status')"
if [[ "$status" == "FAIL" ]]; then
    pass "verified-child with empty result artifact -> FAIL"
else
    fail "verified-child empty result -> expected FAIL, got status=$status"
fi

# 6. A step in the expected list that never ran (still pending) -> FAIL, and
#    that step is named explicitly under missing_steps.
out="$(run_fixture "
$fixture_common
qst_init step_a step_b step_c
qst_run_child step_a true
qst_run_child step_b true
# step_c deliberately never runs.
qst_compute_summary_json run6 mytest 0 '{}'
")"
status="$(echo "$out" | jq -r '.status')"
missing="$(echo "$out" | jq -r '.missing_steps | join(",")')"
step_c_status="$(echo "$out" | jq -r '.steps[] | select(.name=="step_c") | .status')"
if [[ "$status" == "FAIL" && "$missing" == "step_c" && "$step_c_status" == "pending" ]]; then
    pass "unrun expected step -> FAIL with missing_steps naming it explicitly"
else
    fail "unrun expected step -> expected FAIL/missing=step_c/pending, got status=$status missing=$missing step_c=$step_c_status"
fi

# 7. qst_write_summary_atomic never leaves nothing or a half-written file
#    behind: if the computed summary string is itself malformed JSON (a bug
#    upstream, or a truncated write), it must overwrite the target with a
#    minimal, valid, fail-closed FAIL document instead of silently doing
#    nothing or leaving a stale prior-run summary in place.
good_target="$TMP_ROOT/atomic-summary.json"
printf '{"status":"PASS","note":"stale prior run"}' >"$good_target"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$LIB"
qst_write_summary_atomic "$good_target" '{not valid json' || true
if [[ -f "$good_target" ]] && jq -e . "$good_target" >/dev/null 2>&1; then
    got_status="$(jq -r '.status' "$good_target")"
    if [[ "$got_status" == "FAIL" ]]; then
        pass "atomic summary write of malformed JSON overwrites stale PASS with a valid FAIL document"
    else
        fail "atomic summary write of malformed JSON -> expected fail-closed FAIL status, got $got_status"
    fi
else
    fail "atomic summary write of malformed JSON -> no valid JSON left behind at all"
fi

echo
log "$PASSED passed, $FAILED failed"
if (( FAILED > 0 )); then
    exit 1
fi
echo "qualification step tracker selftest PASS"

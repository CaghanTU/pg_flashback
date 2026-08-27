#!/usr/bin/env bash
# Self-test for the Step 10 scale-qualification harness contract.
#
# Proves, without allocating any scale tier or starting PostgreSQL:
#   1. an interrupted run can never leave a PASS summary
#   2. a tier that never ran is reported under missing_steps, not silently
#      dropped
#   3. a failed tier forces FAIL even when every other tier passed
#   4. the capacity precheck refuses a tier that would violate the
#      emergency reserve (BLOCKED_CAPACITY), and permits one that fits
#   5. the bounded-memory data fingerprint is order-independent, detects a
#      single changed row, a single deleted row, and a duplicated row
#   6. the latency percentile helper is correct on a known sample
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$ROOT/scripts/lib/qualification_step_tracker.sh"
# shellcheck source=scripts/lib/step10_scale_common.sh
source "$ROOT/scripts/lib/step10_scale_common.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
PASSED=0; FAILED=0
pass() { echo "  PASS: $*"; PASSED=$((PASSED+1)); }
fail() { echo "  FAIL: $*" >&2; FAILED=$((FAILED+1)); }

GIB=$((1024*1024*1024))

echo "== 1. interrupted run cannot yield PASS =="
qst_init t1_mixed t10_mixed t50_hybrid
qst_mark_step t1_mixed pass "exit=0"
QST_CURRENT_STEP=t10_mixed
# Simulate exactly what the SIGTERM trap does, without exiting this shell.
# shellcheck disable=SC2034  # read by qst_compute_summary_json
QST_INTERRUPTED=1
# shellcheck disable=SC2034  # read by qst_compute_summary_json
QST_INTERRUPT_SIGNAL=TERM
qst_mark_step "$QST_CURRENT_STEP" fail "interrupted by SIGTERM"
QST_FAILED=$((QST_FAILED+1))
S="$(qst_compute_summary_json step10-selftest step10_scale 130 '{}')"
[[ "$(jq -r '.status' <<<"$S")" == "FAIL" ]] \
    && pass "interrupted run reports FAIL" || fail "interrupted run reported $(jq -r '.status' <<<"$S")"
[[ "$(jq -r '.interrupted' <<<"$S")" == "true" ]] \
    && pass "interrupted flag recorded" || fail "interrupted flag not recorded"
jq -e '.missing_steps | index("t50_hybrid")' <<<"$S" >/dev/null \
    && pass "never-run tier listed under missing_steps" || fail "never-run tier not reported missing"

echo "== 2. a never-run tier alone forces FAIL =="
qst_init t1_mixed t50_hybrid
qst_mark_step t1_mixed pass "exit=0"
S="$(qst_compute_summary_json step10-selftest step10_scale 0 '{}')"
[[ "$(jq -r '.status' <<<"$S")" == "FAIL" ]] \
    && pass "missing tier forces FAIL even with exit 0" || fail "missing tier did not force FAIL"

echo "== 3. one failed tier forces FAIL =="
qst_init t1_mixed t10_mixed
qst_mark_step t1_mixed pass "exit=0"
qst_mark_step t10_mixed fail "tier failed"
QST_FAILED=1
S="$(qst_compute_summary_json step10-selftest step10_scale 0 '{}')"
[[ "$(jq -r '.status' <<<"$S")" == "FAIL" ]] \
    && pass "failed tier forces FAIL" || fail "failed tier did not force FAIL"

echo "== 3b. all-pass run reports PASS (control) =="
qst_init t1_mixed t10_mixed
qst_mark_step t1_mixed pass "exit=0"
qst_mark_step t10_mixed pass "exit=0"
S="$(qst_compute_summary_json step10-selftest step10_scale 0 '{}')"
[[ "$(jq -r '.status' <<<"$S")" == "PASS" ]] \
    && pass "fully completed run reports PASS" || fail "complete run did not report PASS"

echo "== 3c. atomic summary write never leaves a stale PASS =="
echo '{"status":"PASS","stale":true}' > "$WORK/summary.json"
qst_write_summary_atomic "$WORK/summary.json" 'this is not json' || true
[[ "$(jq -r '.status' "$WORK/summary.json")" == "FAIL" ]] \
    && pass "unparseable summary overwrites stale PASS with FAIL" \
    || fail "stale PASS survived a failed summary write"

echo "== 4. capacity precheck =="
# 50 GiB table + 12 GiB indexes against a tiny filesystem must be blocked.
if s10_capacity_precheck t50 $((50*GIB)) $((12*GIB)) /home $((10*1024*GIB)) >"$WORK/cap.json" 2>&1; then
    fail "capacity precheck accepted a tier that violates a 10 TiB reserve"
else
    [[ "$(jq -r '.verdict' "$WORK/cap.json")" == "BLOCKED_CAPACITY" ]] \
        && pass "impossible tier reports BLOCKED_CAPACITY" \
        || fail "blocked tier verdict was $(jq -r '.verdict' "$WORK/cap.json")"
fi
if s10_capacity_precheck t1 $((1*GIB)) $((1*GIB)) /home 0 >"$WORK/cap2.json" 2>&1; then
    pass "a tier that fits is permitted (verdict=$(jq -r '.verdict' "$WORK/cap2.json"))"
else
    fail "capacity precheck rejected a 1 GiB tier with no reserve on a large filesystem"
fi
est="$(s10_estimate_tier_peak_bytes $((50*GIB)) $((12*GIB)))"
(( est > 50*GIB )) && pass "peak estimate exceeds bare table size (est=${est}B)" \
    || fail "peak estimate ${est}B did not exceed table size"

echo "== 5. data fingerprint sensitivity (needs a throwaway PostgreSQL) =="
PG_BIN="${PG_BIN:-}"
if [[ -z "$PG_BIN" ]]; then
    for c in "$HOME"/.pgrx/17.*/pgrx-install/bin /usr/local/pgsql-17/bin /usr/pgsql-17/bin; do
        [[ -x "$c/initdb" && -x "$c/psql" ]] && { PG_BIN="$c"; break; }
    done
fi
if [[ -z "$PG_BIN" ]]; then
    echo "  SKIP: no PostgreSQL bindir found for the fingerprint checks" >&2
else
    SOCK="$(mktemp -d /tmp/s10-selftest-XXXXXX)"
    "$PG_BIN/initdb" -D "$WORK/data" -A trust --locale=C.UTF-8 >/dev/null 2>&1
    {
        echo "port = 29199"
        echo "unix_socket_directories = '$SOCK'"
        echo "listen_addresses = ''"
        echo "fsync = off"
    } >> "$WORK/data/postgresql.conf"
    "$PG_BIN/pg_ctl" -D "$WORK/data" -l "$WORK/pg.log" -w start >/dev/null
    stop_pg() { "$PG_BIN/pg_ctl" -D "$WORK/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$SOCK"; }
    trap 'stop_pg; rm -rf "$WORK"' EXIT
    # shellcheck disable=SC2034  # read by scripts/lib/step10_scale_common.sh
    S10_PSQL=("$PG_BIN/psql" -h "$SOCK" -p 29199 -d postgres)

    s10_q "CREATE TABLE fp(id int primary key, v text);
           INSERT INTO fp SELECT g, 'row-'||g FROM generate_series(1,5000) g;" >/dev/null
    base="$(s10_data_fingerprint fp)"
    base_digest="$(jq -r '.digest' <<<"$base")"
    [[ "$(jq -r '.row_count' <<<"$base")" == "5000" ]] \
        && pass "fingerprint row_count correct" || fail "fingerprint row_count wrong"

    # physical reorder must NOT change the digest
    s10_q "CREATE TABLE fp2(id int primary key, v text);
           INSERT INTO fp2 SELECT g, 'row-'||g FROM generate_series(1,5000) g ORDER BY random();" >/dev/null
    [[ "$(jq -r '.digest' <<<"$(s10_data_fingerprint fp2)")" == "$base_digest" ]] \
        && pass "digest is insertion-order independent" || fail "digest changed under reordering"

    # one changed row must change it
    s10_q "UPDATE fp SET v='tampered' WHERE id=2500;" >/dev/null
    [[ "$(jq -r '.digest' <<<"$(s10_data_fingerprint fp)")" != "$base_digest" ]] \
        && pass "digest detects a single changed row" || fail "digest missed a changed row"
    s10_q "UPDATE fp SET v='row-2500' WHERE id=2500;" >/dev/null

    # one deleted row must change it
    s10_q "DELETE FROM fp WHERE id=1234;" >/dev/null
    d_del="$(s10_data_fingerprint fp)"
    [[ "$(jq -r '.digest' <<<"$d_del")" != "$base_digest" ]] \
        && pass "digest detects a deleted row" || fail "digest missed a deleted row"
    [[ "$(jq -r '.row_count' <<<"$d_del")" == "4999" ]] \
        && pass "row_count follows the delete" || fail "row_count did not follow the delete"

    # a duplicated row must change it (XOR alone would cancel a pair;
    # the per-bucket ordered digest and the count must still catch it)
    s10_q "CREATE TABLE fp3(id int, v text);
           INSERT INTO fp3 SELECT g, 'row-'||g FROM generate_series(1,5000) g;" >/dev/null
    d3a="$(jq -r '.digest' <<<"$(s10_data_fingerprint fp3)")"
    s10_q "INSERT INTO fp3 VALUES (77,'row-77');" >/dev/null
    d3b="$(s10_data_fingerprint fp3)"
    [[ "$(jq -r '.digest' <<<"$d3b")" != "$d3a" ]] \
        && pass "digest detects a duplicated row" || fail "digest missed a duplicated row"

    # two rows swapping values (XOR-cancelling shape) must still be caught
    s10_q "CREATE TABLE fp4(id int primary key, v text);
           INSERT INTO fp4 SELECT g, 'row-'||g FROM generate_series(1,5000) g;" >/dev/null
    d4a="$(jq -r '.digest' <<<"$(s10_data_fingerprint fp4)")"
    s10_q "UPDATE fp4 SET v='row-4001' WHERE id=4000;
           UPDATE fp4 SET v='row-4000' WHERE id=4001;" >/dev/null
    [[ "$(jq -r '.digest' <<<"$(s10_data_fingerprint fp4)")" != "$d4a" ]] \
        && pass "digest detects a value swap between two rows" || fail "digest missed a value swap"

    stop_pg
    trap 'rm -rf "$WORK"' EXIT
fi

echo "== 6. percentile helper =="
printf '%s\n' 1 2 3 4 5 6 7 8 9 10 > "$WORK/lat"
[[ "$(s10_percentile "$WORK/lat" 50)" == "5" ]] && pass "p50 correct" || fail "p50 wrong: $(s10_percentile "$WORK/lat" 50)"
[[ "$(s10_percentile "$WORK/lat" 100)" == "10" ]] && pass "max correct" || fail "max wrong"
[[ "$(s10_percentile "$WORK/lat" 90)" == "9" ]] && pass "p90 correct" || fail "p90 wrong: $(s10_percentile "$WORK/lat" 90)"
: > "$WORK/empty"
[[ "$(s10_percentile "$WORK/empty" 95)" == "0" ]] && pass "empty sample yields 0" || fail "empty sample wrong"

echo ""
echo "step10 harness selftest: passed=$PASSED failed=$FAILED"
[[ "$FAILED" == 0 ]] || exit 1
echo "step10 harness selftest: PASS"

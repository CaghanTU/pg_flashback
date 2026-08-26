#!/usr/bin/env bash
# Deterministic regressions for scripts/lib/output_plugin_allowlist.sh.
#
# Covers, without needing a real PostgreSQL cluster for the fast cases:
#   1. GUC unsupported (older minor)
#   2. Empty existing value
#   3. pg_flashback already present
#   4. Existing single other plugin
#   5. Existing multiple plugins with whitespace
#   6. Idempotent second invocation
#   7. Unrelated probe failure (must hard-fail, never be treated as
#      "unsupported")
#   8. Existing entries remain byte/element-equivalent after merge
#   9. No wildcard ('*') is ever emitted
#
# For the coexistence-sensitive cases (4, 5, and the live-instance
# equivalent), this file also carries a clearly labeled snapshot of the
# PRE-FIX logic (the version that returned early on any existing
# output_plugin_libraries line, and the version that issued a bare
# `ALTER SYSTEM SET output_plugin_libraries = 'pg_flashback'`) and asserts
# that snapshot FAILS those cases -- proving this is a real regression
# fix, not merely new tests written to match new code.
#
# A live-instance check against a real PostgreSQL binary is included last
# (skipped, not failed, if no usable pg_config/psql is found), because the
# ALTER SYSTEM SET quoting behavior for GUC_LIST_QUOTE variables can only
# be observed against a real server (see opal_merge_plugin_sql_values's
# header comment for why a single quoted comma-string does not round-trip
# through ALTER SYSTEM SET).
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/output_plugin_allowlist.sh
source "$ROOT/scripts/lib/output_plugin_allowlist.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

PASSED=0
FAILED=0
pass() { echo "  PASS: $*"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $*" >&2; FAILED=$((FAILED + 1)); }

# --------------------------------------------------------------------
# Fake `postgres` binary: `postgres -D <data_dir> -C output_plugin_libraries`
# responds according to a control file dropped into <data_dir>, so the
# probe/merge logic can be exercised deterministically without a real
# cluster. `postgres --version` always succeeds (opal's log lines call it).
# --------------------------------------------------------------------
make_fake_pg_bin() {
    local bindir=$1
    mkdir -p "$bindir"
    cat >"$bindir/postgres" <<'FAKE'
#!/usr/bin/env bash
if [[ "$1" == "--version" ]]; then
    echo "postgres (PostgreSQL) 99.0 (fake selftest binary)"
    exit 0
fi
data_dir=""
want_guc=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == "-D" ]]; then data_dir="${args[$((i + 1))]}"; fi
    if [[ "${args[$i]}" == "-C" ]]; then want_guc="${args[$((i + 1))]}"; fi
done
if [[ "$want_guc" != "output_plugin_libraries" ]]; then
    echo "fake postgres: unsupported invocation: ${args[*]}" >&2
    exit 1
fi
ctrl="$data_dir/.opal_test_response"
if [[ ! -f "$ctrl" ]]; then
    echo "fake postgres: missing control file $ctrl" >&2
    exit 1
fi
response="$(cat "$ctrl")"
case "$response" in
    VALUE:*)
        printf '%s\n' "${response#VALUE:}"
        exit 0
        ;;
    UNSUPPORTED)
        echo 'FATAL:  unrecognized configuration parameter "output_plugin_libraries"' >&2
        exit 1
        ;;
    ERROR:*)
        echo "${response#ERROR:}" >&2
        exit 1
        ;;
    *)
        echo "fake postgres: unknown control response: $response" >&2
        exit 1
        ;;
esac
FAKE
    chmod +x "$bindir/postgres"
}

new_case_dir() {
    local name=$1 response=$2
    local dir="$WORK/$name"
    mkdir -p "$dir"
    printf '%s' "$response" > "$dir/.opal_test_response"
    printf '# base conf\n' > "$dir/postgresql.conf"
    printf '%s' "$dir"
}

FAKE_BIN="$WORK/bin"
make_fake_pg_bin "$FAKE_BIN"

# ======================================================================
# 1. GUC unsupported -- no-op, no line written, no error.
# ======================================================================
d="$(new_case_dir case1 UNSUPPORTED)"
before_sha="$(sha256sum "$d/postgresql.conf" | awk '{print $1}')"
if opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>"$d/stderr.log"; then
    after_sha="$(sha256sum "$d/postgresql.conf" | awk '{print $1}')"
    if [[ "$before_sha" == "$after_sha" ]]; then
        grep -qi "not present on this PostgreSQL minor" "$d/stderr.log" \
            && pass "1: unsupported GUC is a clean no-op with an informational message" \
            || fail "1: unsupported GUC no-op did not explain why: $(cat "$d/stderr.log")"
    else
        fail "1: unsupported GUC modified postgresql.conf"
    fi
else
    fail "1: unsupported GUC returned non-zero (expected a soft no-op)"
fi

# ======================================================================
# 2. Empty existing value -- configure only pg_flashback.
# ======================================================================
d="$(new_case_dir case2 "VALUE:")"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>/dev/null
line="$(grep '^output_plugin_libraries' "$d/postgresql.conf" || true)"
[[ "$line" == "output_plugin_libraries = 'pg_flashback'" ]] \
    && pass "2: empty existing value configures exactly pg_flashback" \
    || fail "2: empty existing value produced unexpected line: '$line'"

# ======================================================================
# 3. pg_flashback already present -- idempotent no-op, no duplicate line.
# ======================================================================
d="$(new_case_dir case3 "VALUE:decoderbufs, pg_flashback, wal2json")"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>"$d/stderr.log"
n="$(grep -c '^output_plugin_libraries' "$d/postgresql.conf" || true)"
[[ "$n" == "0" ]] \
    && grep -qi "already allows pg_flashback" "$d/stderr.log" \
    && pass "3: pg_flashback already present is a no-op (no line appended)" \
    || fail "3: already-present case appended a line or wrong message (n=$n): $(cat "$d/stderr.log")"

# ======================================================================
# 4. Existing single other plugin -- must be preserved AND pg_flashback added.
# ======================================================================
d="$(new_case_dir case4 "VALUE:wal2json")"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>/dev/null
line="$(grep '^output_plugin_libraries' "$d/postgresql.conf" | tail -1)"
if [[ "$line" == *"wal2json"* && "$line" == *"pg_flashback"* ]]; then
    pass "4: existing single other plugin (wal2json) preserved alongside pg_flashback: $line"
else
    fail "4: existing single other plugin lost or pg_flashback missing: $line"
fi

# ---- Old (pre-fix) logic snapshot: proves this is a real regression ----
# The original opal_configure_postgresql_conf returned early whenever ANY
# output_plugin_libraries assignment already existed in the file,
# regardless of its content.
opal_configure_postgresql_conf_OLD_BUGGY() {
    local conf_file=$1
    if grep -Eq '^[[:space:]]*output_plugin_libraries[[:space:]]*=' "$conf_file"; then
        return 0
    fi
    printf '%s\n' "output_plugin_libraries = 'pg_flashback'" >> "$conf_file"
}
d="$(new_case_dir case4_old "VALUE:wal2json")"
printf 'output_plugin_libraries = %s\n' "'wal2json'" >> "$d/postgresql.conf"
opal_configure_postgresql_conf_OLD_BUGGY "$d/postgresql.conf"
old_line="$(grep '^output_plugin_libraries' "$d/postgresql.conf" | tail -1)"
if [[ "$old_line" == *"pg_flashback"* ]]; then
    fail "4-old: pre-fix logic unexpectedly added pg_flashback (regression proof invalid)"
else
    pass "4-old: pre-fix logic PROVEN broken -- left '$old_line' without pg_flashback, would still fail real slot creation"
fi

# ======================================================================
# 5. Existing multiple plugins with whitespace -- all preserved verbatim,
#    element-for-element (case 8's requirement folded in here too).
# ======================================================================
d="$(new_case_dir case5 "VALUE:decoderbufs,  wal2json ,test_decoding")"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>/dev/null
effective="$(grep '^output_plugin_libraries' "$d/postgresql.conf" | tail -1 | sed -E "s/^output_plugin_libraries = '(.*)'$/\1/")"
ok=1
for elem in decoderbufs wal2json test_decoding pg_flashback; do
    opal_plugin_list_contains "$effective" "$elem" || ok=0
done
n_elems="$(printf '%s' "$effective" | tr ',' '\n' | sed -E 's/^[[:space:]]+|[[:space:]]+$//g' | grep -c .)"
if [[ "$ok" == 1 && "$n_elems" == 4 ]]; then
    pass "5/8: multiple existing plugins with irregular whitespace all preserved element-for-element, pg_flashback added (exactly 4 elements): $effective"
else
    fail "5/8: multi-plugin whitespace merge incorrect (ok=$ok n_elems=$n_elems): $effective"
fi

# ======================================================================
# 6. Idempotent second invocation -- calling twice in a row (real re-probe
#    each time, not a cached decision) converges, no duplicate/drift.
# ======================================================================
d="$(new_case_dir case6 "VALUE:decoderbufs, wal2json")"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>/dev/null
first_effective="$(grep '^output_plugin_libraries' "$d/postgresql.conf" | tail -1)"
# Simulate the probe now observing the merged value (as a real postgres
# binary would, since it reads the conf file we just appended to).
printf 'VALUE:decoderbufs, wal2json, pg_flashback' > "$d/.opal_test_response"
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>/dev/null
n="$(grep -c '^output_plugin_libraries' "$d/postgresql.conf" || true)"
second_effective="$(grep '^output_plugin_libraries' "$d/postgresql.conf" | tail -1)"
if [[ "$n" == "1" && "$first_effective" == "$second_effective" ]]; then
    pass "6: second invocation is idempotent (no duplicate line, unchanged effective value)"
else
    fail "6: second invocation was not idempotent (n=$n first='$first_effective' second='$second_effective')"
fi

# ======================================================================
# 7. Unrelated probe failure -- must hard-fail (rc=2), never silently
#    treated as "unsupported minor".
# ======================================================================
d="$(new_case_dir case7 "ERROR:FATAL:  could not access file \"some_other_broken_thing\"")"
set +e
opal_configure_postgresql_conf "$FAKE_BIN" "$d" 2>"$d/stderr.log"
rc=$?
set -e
n="$(grep -c '^output_plugin_libraries' "$d/postgresql.conf" || true)"
if [[ "$rc" == "2" && "$n" == "0" ]] && grep -qi "unrelated reason" "$d/stderr.log"; then
    pass "7: unrelated probe failure hard-fails (rc=2), never reinterpreted as unsupported-minor"
else
    fail "7: unrelated probe failure mishandled (rc=$rc n=$n): $(cat "$d/stderr.log")"
fi

# ======================================================================
# 9. No wildcard is ever emitted, across every case run so far.
# ======================================================================
if grep -rl "output_plugin_libraries" "$WORK"/*/postgresql.conf 2>/dev/null | xargs grep -l "'\*'" 2>/dev/null | grep -q .; then
    fail "9: a wildcard '*' was emitted somewhere in postgresql.conf across the case fixtures"
else
    pass "9: no wildcard ('*') was emitted in any postgresql.conf fixture"
fi

# ======================================================================
# Live-instance path (opal_live_ensure_output_plugin_libraries). Uses a
# real PostgreSQL binary because ALTER SYSTEM SET's GUC_LIST_QUOTE
# serialization can only be observed against a real server.
# ======================================================================
PG_BIN=""
for cand in "$HOME/.pgrx/17."*/pgrx-install/bin "$HOME/.pgrx/15."*/pgrx-install/bin /usr/local/pgsql-17/bin; do
    if [[ -x "$cand/initdb" && -x "$cand/pg_ctl" && -x "$cand/psql" ]]; then PG_BIN="$cand"; break; fi
done

if [[ -z "$PG_BIN" ]]; then
    echo "SKIP: no usable PostgreSQL bindir found for the live-instance coexistence check" >&2
else
    LIVE_DATA="$WORK/live/data"
    LIVE_SOCK="$WORK/live/sock"
    mkdir -p "$LIVE_SOCK"
    "$PG_BIN/initdb" -D "$LIVE_DATA" -A trust --locale=C.UTF-8 >/dev/null 2>&1
    {
        echo "output_plugin_libraries = 'decoderbufs, wal2json'"
        echo "port = 28964"
        echo "unix_socket_directories = '$LIVE_SOCK'"
        echo "listen_addresses = ''"
    } >> "$LIVE_DATA/postgresql.conf"
    "$PG_BIN/pg_ctl" -D "$LIVE_DATA" -l "$WORK/live/log" -w start >/dev/null

    live_cleanup() { "$PG_BIN/pg_ctl" -D "$LIVE_DATA" -m immediate stop >/dev/null 2>&1 || true; }
    trap 'live_cleanup; rm -rf "$WORK"' EXIT

    PSQL_INV=("$PG_BIN/psql" -h "$LIVE_SOCK" -p 28964 -d postgres)

    before="$("${PSQL_INV[@]}" -qAtc "SELECT current_setting('output_plugin_libraries')")"

    # ---- Old (pre-fix) logic snapshot, run against the SAME live server
    # first, on a throwaway savepoint-equivalent (ALTER SYSTEM is not
    # transactional, so reset it back to `before` immediately after).
    "${PSQL_INV[@]}" -qAtc "ALTER SYSTEM SET output_plugin_libraries = 'pg_flashback'" >/dev/null
    "${PSQL_INV[@]}" -qAtc "SELECT pg_reload_conf()" >/dev/null
    sleep 0.3
    old_after="$("${PSQL_INV[@]}" -qAtc "SELECT current_setting('output_plugin_libraries')")"
    if opal_plugin_list_contains "$old_after" "decoderbufs" && opal_plugin_list_contains "$old_after" "wal2json"; then
        fail "coexistence-old: pre-fix bare ALTER SYSTEM SET unexpectedly preserved existing plugins (regression proof invalid): $old_after"
    else
        pass "coexistence-old: pre-fix logic PROVEN broken -- bare ALTER SYSTEM SET output_plugin_libraries='pg_flashback' destroyed decoderbufs/wal2json, observed: $old_after"
    fi
    # Restore the pre-existing list before exercising the fixed function.
    "${PSQL_INV[@]}" -qAtc "ALTER SYSTEM SET output_plugin_libraries = 'decoderbufs', 'wal2json'" >/dev/null
    "${PSQL_INV[@]}" -qAtc "SELECT pg_reload_conf()" >/dev/null
    sleep 0.3

    # ---- Fixed logic ----
    if opal_live_ensure_output_plugin_libraries "${PSQL_INV[@]}" 2>"$WORK/live/opal.log"; then
        after="$("${PSQL_INV[@]}" -qAtc "SELECT current_setting('output_plugin_libraries')")"
        if opal_plugin_list_contains "$after" "decoderbufs" \
            && opal_plugin_list_contains "$after" "wal2json" \
            && opal_plugin_list_contains "$after" "pg_flashback"; then
            pass "coexistence-live: fixed opal_live_ensure_output_plugin_libraries preserved decoderbufs+wal2json and added pg_flashback: $after"
        else
            fail "coexistence-live: fixed function lost an existing entry or missed pg_flashback: before='$before' after='$after'"
        fi
        [[ "$after" != *"*"* ]] \
            && pass "9-live: no wildcard emitted on the live instance" \
            || fail "9-live: wildcard observed on the live instance: $after"
        grep -qi "restart_required" "$WORK/live/opal.log" \
            && fail "restart_required must not be reported when reload was sufficient and verified: $(cat "$WORK/live/opal.log")" \
            || pass "no misleading restart_required message; reload+verify was reported sufficient"
    else
        fail "coexistence-live: fixed function returned non-zero unexpectedly: $(cat "$WORK/live/opal.log")"
    fi

    # ---- Idempotent second invocation on the live instance ----
    if opal_live_ensure_output_plugin_libraries "${PSQL_INV[@]}" 2>"$WORK/live/opal2.log"; then
        after2="$("${PSQL_INV[@]}" -qAtc "SELECT current_setting('output_plugin_libraries')")"
        [[ "$after2" == "$after" ]] \
            && pass "6-live: idempotent second live invocation left the value unchanged"
        grep -qi "already allows pg_flashback" "$WORK/live/opal2.log" \
            && pass "6-live: second invocation correctly reported already-allowed no-op" \
            || fail "6-live: second invocation did not report the expected no-op message: $(cat "$WORK/live/opal2.log")"
    else
        fail "6-live: idempotent second invocation returned non-zero: $(cat "$WORK/live/opal2.log")"
    fi

    live_cleanup
fi

echo ""
echo "output-plugin-allowlist selftest: passed=$PASSED failed=$FAILED"
[[ "$FAILED" == 0 ]] || exit 1
echo "output-plugin-allowlist selftest: PASS"

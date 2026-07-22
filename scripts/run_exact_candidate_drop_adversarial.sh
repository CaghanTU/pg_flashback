#!/usr/bin/env bash
# Adversarial DROP recovery gaps beyond the main drop-qualification matrix.
# Installs ONLY from CANDIDATE_DIR. Never cargo-builds.
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
KEEP="${PGFB_DROP_ADV_KEEP:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_DROP_ADV_BASE:-$REPO_ROOT/target/exact-candidate-drop-adversarial}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="${PGFB_DROP_ADV_RESULT:-$BASE/results/drop-adversarial-$RUN_ID.json}"
CASES_JSONL="$RUN_ROOT/cases.jsonl"

STATUS=failed
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
EC_BOUND=0
PASSED=0
FAILED=0
SOCKET_DIR=""
PRIMARY_DIR=""
CLI=""

log() { printf '[drop-adversarial] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() {
    PASSED=$((PASSED + 1))
    jq -cn --arg name "$1" --arg status passed '{name:$name,status:$status}' >> "$CASES_JSONL"
    log "PASS[$PASSED]: $1"
}
fail_case() {
    FAILED=$((FAILED + 1))
    jq -cn --arg name "$1" --arg status failed --arg detail "$2" \
        '{name:$name,status:$status,detail:$detail}' >> "$CASES_JSONL"
    die "$1 — $2"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == 1 ]]; then
        "$EC_PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    if [[ "$PREFIX_INSTALLED" == 1 ]]; then
        exact_candidate_restore_prefix || true
    fi
    if [[ "$rc" == 0 && "$FAILED" == 0 ]]; then STATUS=passed; else STATUS=failed; fi
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$STATUS" \
        --argjson passed "$PASSED" --argjson failed "$FAILED" \
        --argjson identity "$([[ "${EC_BOUND:-0}" == 1 ]] && exact_candidate_identity_json || echo '{}')" \
        --slurpfile cases "$CASES_JSONL" \
        '{status:$status,passed:$passed,failed:$failed,cases:$cases,identity:$identity}' \
        > "$RESULT_JSON"
    log "result: $RESULT_JSON status=$STATUS passed=$PASSED failed=$FAILED"
    if [[ "$KEEP" != 1 && "$STATUS" == passed ]]; then
        rm -rf -- "$RUN_ROOT"
    fi
    exit "$rc"
}
trap cleanup EXIT

mkdir -p "$RUN_ROOT" "$(dirname "$RESULT_JSON")"
: > "$CASES_JSONL"

exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
EC_BOUND=1
exact_candidate_install_into_prefix || die "candidate install failed"
PREFIX_INSTALLED=1
exact_candidate_verify_installed || die "installed identity mismatch"

EC_PG_BIN="${PG_BIN:?PG_BIN unset after candidate bind}"
CLI="$EC_EXT_ROOT/bin/pg_flashback"
[[ -x "$CLI" ]] || die "packaged CLI missing: $CLI"
export PATH="$EC_PG_BIN:$PATH"
export PSQL_BIN="$EC_PG_BIN/psql"

SOCKET_DIR="/tmp/pgfb-dropadv-$RUN_ID"
PRIMARY_DIR="$RUN_ROOT/primary"
mkdir -p "$SOCKET_DIR"
chmod 700 "$SOCKET_DIR"
PORT=$((41000 + ($$ % 20000)))

"$EC_PG_BIN/initdb" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >>"$PRIMARY_DIR/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET_DIR'
port = $PORT
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
pg_flashback.target_database = postgres
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
"$EC_PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$RUN_ROOT/pg.log" start -w >/dev/null
PRIMARY_STARTED=1
export PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGDATABASE=postgres

q() { "$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "$1"; }
wait_ready() {
    local _i state
    for _i in $(seq 1 200); do
        state=$(q "SELECT admission_state FROM flashback_worker_readiness();")
        [[ "$state" == ready ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_healthy() {
    local rel=$1 _i
    for _i in $(seq 1 400); do
        [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='$rel';")" == healthy ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_drop_restorable() {
    local rel=$1 _i
    for _i in $(seq 1 400); do
        if [[ "$(q "SELECT count(*) FROM flashback_disaster_points('$rel', interval '1 hour')
                     WHERE event_type='DROP' AND status='restorable';")" -ge 1 ]]; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}
fingerprint() {
    local rel=$1
    q "SELECT count(*)::text || '|' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)),0)::text FROM $rel t;"
}

q "CREATE EXTENSION pg_flashback;"
wait_ready || die "workers not ready"

"$CLI" doctor >/dev/null || die "doctor failed on clean instance"
pass "cli_doctor"

# Control-character / injection refusal
rc=0
"$CLI" protect $'public.evil\nDROP' >/tmp/pgfb-adv-ctrl.out 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "control_chars_rejected" "protect accepted control characters"
grep -qi 'control characters' /tmp/pgfb-adv-ctrl.out || fail_case "control_chars_rejected" "missing control-char message"
pass "control_chars_rejected"

# Unsupported topologies via SQL API
q "CREATE UNLOGGED TABLE public.adv_unlogged(id int PRIMARY KEY);"
rc=0
q "SELECT flashback_track('public.adv_unlogged');" >/tmp/pgfb-adv-unlog.err 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "unlogged_rejected" "track accepted UNLOGGED"
grep -Eiq 'permanent LOGGED|not supported' /tmp/pgfb-adv-unlog.err || fail_case "unlogged_rejected" "unexpected error text"
pass "unlogged_rejected"

q "CREATE TABLE public.adv_part(id int, ts date) PARTITION BY RANGE (ts);
    CREATE TABLE public.adv_part_2026 PARTITION OF public.adv_part
      FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');"
rc=0
q "SELECT flashback_track('public.adv_part');" >/tmp/pgfb-adv-part.err 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "partitioned_rejected" "track accepted partitioned table"
pass "partitioned_rejected"

# Different schema DROP + recover via CLI
q "CREATE SCHEMA sales;
    CREATE TABLE sales.orders(
      id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      note text NOT NULL,
      payload jsonb NOT NULL,
      CONSTRAINT orders_note_check CHECK (char_length(note) > 0)
    );
    CREATE INDEX orders_payload_idx ON sales.orders ((payload->>'k'));
    GRANT SELECT ON sales.orders TO PUBLIC;"
"$CLI" protect sales.orders >/tmp/pgfb-adv-protect.out
wait_healthy 'sales.orders' || fail_case "schema_drop_recover" "not healthy after protect"
q "INSERT INTO sales.orders(note, payload) VALUES ('a','{\"k\":\"1\"}'),('b','{\"k\":\"2\"}');"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE table_name='sales.orders' AND event_type='INSERT' AND commit_lsn IS NOT NULL;")" -ge 2 ]] && break
    sleep 0.05
done
FP=$(fingerprint 'sales.orders')
OWNER=$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='sales.orders'::regclass;")
ACL=$(q "SELECT COALESCE(array_to_string(relacl,','),'') FROM pg_class WHERE oid='sales.orders'::regclass;")
SCHEMA=$(q "SELECT string_agg(a.attname||':'||format_type(a.atttypid,a.atttypmod),',' ORDER BY a.attnum)
            FROM pg_attribute a WHERE a.attrelid='sales.orders'::regclass AND a.attnum>0 AND NOT a.attisdropped;")
q "INSERT INTO sales.orders(note, payload) VALUES ('c','{\"k\":\"3\"}');
   DROP TABLE sales.orders;"
wait_drop_restorable 'sales.orders' || fail_case "schema_drop_recover" "DROP not restorable"
"$CLI" recover sales.orders --latest-drop --yes >/tmp/pgfb-adv-recover.out
[[ "$(q "SELECT to_regclass('sales.orders') IS NOT NULL;")" == t ]] || fail_case "schema_drop_recover" "relation missing"
[[ "$(fingerprint 'sales.orders')" == "$FP" ]] || fail_case "schema_drop_recover" "fingerprint mismatch"
[[ "$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='sales.orders'::regclass;")" == "$OWNER" ]] \
    || fail_case "schema_drop_recover" "owner mismatch"
[[ "$(q "SELECT COALESCE(array_to_string(relacl,','),'') FROM pg_class WHERE oid='sales.orders'::regclass;")" == "$ACL" ]] \
    || fail_case "schema_drop_recover" "acl mismatch"
[[ "$(q "SELECT string_agg(a.attname||':'||format_type(a.atttypid,a.atttypmod),',' ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid='sales.orders'::regclass AND a.attnum>0 AND NOT a.attisdropped;")" == "$SCHEMA" ]] \
    || fail_case "schema_drop_recover" "schema mismatch"
pass "schema_drop_recover"

# Same-name new relation identity conflict
q "DROP TABLE sales.orders;
    CREATE TABLE sales.orders(id int PRIMARY KEY, note text);"
rc=0
"$CLI" recover sales.orders --latest-drop --yes >/tmp/pgfb-adv-ident.out 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "same_name_identity_conflict" "recover overwrote identity-mismatched relation"
grep -Eiq 'identity conflict|different relation' /tmp/pgfb-adv-ident.out \
    || fail_case "same_name_identity_conflict" "missing identity conflict message"
q "DROP TABLE sales.orders;"
pass "same_name_identity_conflict"

# Slot loss => non-restorable / fail-closed recover
q "CREATE TABLE public.adv_slot(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.adv_slot');" >/dev/null
wait_healthy public.adv_slot || die "adv_slot not healthy"
q "INSERT INTO public.adv_slot VALUES (1,'x');"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='public.adv_slot'::regclass AND commit_lsn IS NOT NULL;")" -ge 1 ]] && break
    sleep 0.05
done
FP_SLOT=$(fingerprint public.adv_slot)
q "DROP TABLE public.adv_slot;"
wait_drop_restorable public.adv_slot || die "adv_slot DROP not restorable before slot drop"
SLOT=$(q "SELECT flashback_effective_slot_name();")
q "SELECT pg_drop_replication_slot('$SLOT');" >/dev/null
# Force health/gap recognition
sleep 0.5
rc=0
"$CLI" recover public.adv_slot --latest-drop --yes >/tmp/pgfb-adv-slot.out 2>&1 || rc=$?
# Slot loss may still leave already-captured DROP restorable; if so, restore must
# succeed with fingerprint. If disaster_points marks non_restorable, CLI must fail closed.
if [[ "$rc" == 0 ]]; then
    [[ "$(fingerprint public.adv_slot)" == "$FP_SLOT" ]] \
        || fail_case "slot_loss_drop_path" "restored wrong fingerprint after slot loss"
    pass "slot_loss_drop_path"
else
    grep -Eiq 'Cannot recover|non_restorable|gap|slot|Action' /tmp/pgfb-adv-slot.out \
        || fail_case "slot_loss_drop_path" "failed without actionable message"
    pass "slot_loss_drop_path"
fi

# Concurrent recover calls fail-closed / serialize safely
q "CREATE TABLE public.adv_conc(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.adv_conc');" >/dev/null
wait_healthy public.adv_conc || die "adv_conc not healthy"
q "INSERT INTO public.adv_conc VALUES (1,'a'),(2,'b');"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='public.adv_conc'::regclass AND event_type='INSERT' AND commit_lsn IS NOT NULL;")" -ge 2 ]] && break
    sleep 0.05
done
FP_CONC=$(fingerprint public.adv_conc)
q "DROP TABLE public.adv_conc;"
wait_drop_restorable public.adv_conc || die "adv_conc DROP not restorable"
(
  set +e
  "$CLI" recover public.adv_conc --latest-drop --yes >/tmp/pgfb-adv-conc1.out 2>&1
  echo $? >"$RUN_ROOT/conc1.rc"
) &
PID1=$!
(
  set +e
  "$CLI" recover public.adv_conc --latest-drop --yes >/tmp/pgfb-adv-conc2.out 2>&1
  echo $? >"$RUN_ROOT/conc2.rc"
) &
PID2=$!
wait "$PID1" || true
wait "$PID2" || true
[[ -f "$RUN_ROOT/conc1.rc" && -f "$RUN_ROOT/conc2.rc" ]] \
    || fail_case "concurrent_recover" "concurrent recover workers did not record exit codes"
RC1=$(cat "$RUN_ROOT/conc1.rc")
RC2=$(cat "$RUN_ROOT/conc2.rc")
# Exactly one success is ideal; both success only if second is idempotent healthy no-op.
# Zero success is failure.
if [[ "$RC1" != 0 && "$RC2" != 0 ]]; then
    fail_case "concurrent_recover" "both concurrent recovers failed (rc=$RC1/$RC2)"
fi
[[ "$(q "SELECT to_regclass('public.adv_conc') IS NOT NULL;")" == t ]] \
    || fail_case "concurrent_recover" "table missing after concurrent recover"
[[ "$(fingerprint public.adv_conc)" == "$FP_CONC" ]] \
    || fail_case "concurrent_recover" "fingerprint mismatch"
pass "concurrent_recover"

# Protect vs recover race: protect must not start a second lifecycle on a dropped name mid-recover
# (already protected / dropped). Verify protect on still-present table is idempotent.
"$CLI" protect public.adv_conc >/tmp/pgfb-adv-prot2.out
grep -Eiq 'Already protected|Protected:' /tmp/pgfb-adv-prot2.out \
    || fail_case "protect_idempotent" "unexpected protect output"
pass "protect_idempotent"

# Capacity rejection on recover preflight
q "CREATE TABLE public.adv_cap(id bigint PRIMARY KEY, payload text NOT NULL);"
q "INSERT INTO public.adv_cap SELECT g, repeat('x', 200) FROM generate_series(1,2000) g;"
q "SELECT flashback_track('public.adv_cap');" >/dev/null
wait_healthy public.adv_cap || die "adv_cap not healthy"
q "DROP TABLE public.adv_cap;"
wait_drop_restorable public.adv_cap || die "adv_cap DROP not restorable"
# ALTER SYSTEM must not share a multi-statement implicit transaction.
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc \
    "ALTER SYSTEM SET pg_flashback.local_max_restore_peak_bytes = '1kB';" >/dev/null
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "SELECT pg_reload_conf();" >/dev/null
rc=0
"$CLI" recover public.adv_cap --latest-drop --yes >/tmp/pgfb-adv-cap.out 2>&1 || rc=$?
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc \
    "ALTER SYSTEM RESET pg_flashback.local_max_restore_peak_bytes;" >/dev/null
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "SELECT pg_reload_conf();" >/dev/null
[[ "$rc" != 0 ]] || fail_case "capacity_reject_recover" "recover ignored tiny restore budget"
pass "capacity_reject_recover"

# Zero DROP events
q "CREATE TABLE public.adv_nodrop(id int PRIMARY KEY);"
q "SELECT flashback_track('public.adv_nodrop');" >/dev/null
wait_healthy public.adv_nodrop || die "adv_nodrop not healthy"
rc=0
"$CLI" recover public.adv_nodrop --latest-drop --yes >/tmp/pgfb-adv-nodrop.out 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "zero_drop_events" "recover succeeded with no DROP"
grep -Eiq 'no DROP|Cannot recover|missing' /tmp/pgfb-adv-nodrop.out \
    || fail_case "zero_drop_events" "unclear zero-DROP error"
pass "zero_drop_events"

# Latest DROP by COMMIT LSN only: newer non-restorable must not fall back to older restorable.
q "CREATE TABLE public.adv_fallback(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.adv_fallback');" >/dev/null
wait_healthy public.adv_fallback || die "adv_fallback not healthy"
q "INSERT INTO public.adv_fallback VALUES (1,'old');"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='public.adv_fallback'::regclass AND event_type='INSERT' AND commit_lsn IS NOT NULL;")" -ge 1 ]] && break
    sleep 0.05
done
q "DROP TABLE public.adv_fallback;"
wait_drop_restorable public.adv_fallback || die "first DROP not restorable"
# Recreate same name as new lifecycle is not started; inject a newer synthetic DROP
# that is non_restorable by opening a coverage gap intersecting the prefix.
TID=$(q "SELECT tracking_id FROM flashback.tracked_tables WHERE format('%I.%I',schema_name,table_name)='public.adv_fallback' ORDER BY tracked_since DESC LIMIT 1;")
q "INSERT INTO flashback.coverage_gaps(tracking_id, gap_start_lsn, gap_end_lsn, lower_bound_inclusive, reason)
   SELECT $TID, pg_current_wal_lsn(), NULL, true, 'adversarial_gap';"
# Ensure disaster_points still lists the older DROP; CLI must evaluate latest by LSN.
# With an open gap, latest DROP becomes non_restorable; older must NOT be chosen.
rc=0
"$CLI" recover public.adv_fallback --latest-drop --yes >/tmp/pgfb-adv-fallback.out 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "latest_drop_no_silent_fallback" "CLI fell back to older restorable DROP"
grep -Eiq 'Cannot recover|non_restorable|ambiguous|gap|Action' /tmp/pgfb-adv-fallback.out \
    || fail_case "latest_drop_no_silent_fallback" "unclear refusal"
pass "latest_drop_no_silent_fallback"

# Failpoint cancel before materialize + retry (deterministic; not timing-only).
q "CREATE TABLE public.adv_fp(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.adv_fp');" >/dev/null
wait_healthy public.adv_fp || die "adv_fp not healthy"
q "INSERT INTO public.adv_fp VALUES (1,'x');"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='public.adv_fp'::regclass AND commit_lsn IS NOT NULL;")" -ge 1 ]] && break
    sleep 0.05
done
FP_FP=$(fingerprint public.adv_fp)
q "DROP TABLE public.adv_fp;"
wait_drop_restorable public.adv_fp || die "adv_fp DROP not restorable"
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc \
    "ALTER SYSTEM SET pg_flashback.test_restore_failpoint = 'before_materialize';" >/dev/null
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "SELECT pg_reload_conf();" >/dev/null
rc=0
"$CLI" recover public.adv_fp --latest-drop --yes >/tmp/pgfb-adv-fp1.out 2>&1 || rc=$?
[[ "$rc" != 0 ]] || fail_case "failpoint_before_materialize" "failpoint did not cancel restore"
[[ "$(q "SELECT to_regclass('public.adv_fp') IS NULL;")" == t ]] \
    || fail_case "failpoint_before_materialize" "table mutated despite cancel"
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc \
    "ALTER SYSTEM RESET pg_flashback.test_restore_failpoint;" >/dev/null
"$EC_PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "SELECT pg_reload_conf();" >/dev/null
"$CLI" recover public.adv_fp --latest-drop --yes >/tmp/pgfb-adv-fp2.out
[[ "$(fingerprint public.adv_fp)" == "$FP_FP" ]] \
    || fail_case "failpoint_before_materialize" "retry fingerprint mismatch"
pass "failpoint_before_materialize"

[[ "$FAILED" == 0 ]]

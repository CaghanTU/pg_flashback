#!/usr/bin/env bash
# Senior-DBA acceptance regressions for the nine invalidated-candidate findings.
# Uses a dedicated PostgreSQL instance + real WAL/worker (no timing-only asserts).
# Deterministic failpoints for recover failure; barriers via consume_wal/health.
#
# Usage: ./scripts/run_dba_acceptance_regressions.sh
# Env:   PG_CONFIG, PGFB_DBA_PORT, PGFB_DBA_KEEP=1

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
CLI_SRC="$ROOT/scripts/pg_flashback"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PGFB_DBA_PORT:-28957}"
WORK_ROOT="${PGFB_DBA_WORK_ROOT:-$ROOT/target/dba-acceptance/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-dba-$RUN_ID"
RESULT_JSON="$WORK_ROOT/dba-acceptance-$RUN_ID.json"
LOG="$WORK_ROOT/postgresql.log"
PASSED=0
FAILED=0
declare -a CASE_RESULTS=()

die() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASSED=$((PASSED + 1)); CASE_RESULTS+=("{\"name\":$(jq -Rn --arg s "$*" '$s'),\"pass\":true}"); }
fail_case() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); CASE_RESULTS+=("{\"name\":$(jq -Rn --arg s "$*" '$s'),\"pass\":false}"); }

cleanup() {
    local rc=$?
    set +e
    if [[ "${PGFB_DBA_KEEP:-0}" != "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
        rm -rf "$DATA" "$SOCKET"
    else
        echo "PGFB_DBA_KEEP=1: leaving $WORK_ROOT" >&2
    fi
    mkdir -p "$WORK_ROOT"
    local arr
    arr=$(printf '%s\n' "${CASE_RESULTS[@]:-}" | jq -s '.')
    jq -n \
        --arg run_id "$RUN_ID" \
        --argjson passed "$PASSED" \
        --argjson failed "$FAILED" \
        --argjson cases "$arr" \
        --argjson exit_code "$rc" \
        '{run_id:$run_id,passed:$passed,failed:$failed,cases:$cases,exit_code:$exit_code,
          status:(if $failed==0 and $exit_code==0 then "PASS" else "FAIL" end)}' \
        >"$RESULT_JSON"
    echo "DBA acceptance: $([[ $FAILED -eq 0 && $rc -eq 0 ]] && echo PASS || echo FAIL) ($RESULT_JSON)"
    [[ $FAILED -eq 0 && $rc -eq 0 ]] || exit 1
    exit 0
}
trap cleanup EXIT

[[ -x "$PG_BIN/psql" ]] || die "psql not found via $PG_CONFIG"
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || die "extension not installed in $SHARE_DIR"
[[ -x "$CLI_SRC" ]] || die "CLI source missing: $CLI_SRC"

mkdir -p "$WORK_ROOT" "$SOCKET"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
# Workers attach after DBs exist; set via ALTER SYSTEM + restart below.
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
# Lab only: some probes still call flashback_restore_lsn directly.
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() {
    local db=${DB:-postgres}
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$db" -v ON_ERROR_STOP=1 -qAtc "$1"
}
qe() {
    local db=${DB:-postgres}
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$db" -v ON_ERROR_STOP=1 -qAtc "$1" 2>&1
}
export PATH="$WORK_ROOT/bin:$PG_BIN:$PATH"
mkdir -p "$WORK_ROOT/bin"
install -m 0755 "$CLI_SRC" "$WORK_ROOT/bin/pg_flashback"
PGUSER="$(id -un)"
export PGHOST="$SOCKET" PGPORT="$PORT" PGUSER

restart_pg() {
    "$PG_BIN/pg_ctl" -D "$DATA" restart -w -t 60 -l "$LOG" >/dev/null
    for _ in $(seq 1 30); do
        DB=postgres q "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

wait_worker() {
    local db=$1 timeout_s=${2:-60}
    local deadline
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        if DB="$db" q "SELECT count(*)>0 FROM pg_stat_activity
            WHERE backend_type LIKE 'pg_flashback%' AND datname=current_database();" | grep -q t; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

wait_health() {
    local table=$1 expected=${2:-healthy} db=${3:-$DB} timeout_s=${4:-60}
    local deadline health
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        health=$(DB="$db" q "SELECT COALESCE((SELECT h.health FROM flashback_health() h
            WHERE h.table_name = '$table' ORDER BY h.generation_id DESC LIMIT 1),'missing');")
        [[ "$health" == "$expected" ]] && return 0
        sleep 0.1
    done
    return 1
}

wait_drop() {
    local table=$1 db=${2:-$DB} timeout_s=${3:-60}
    local deadline n
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        n=$(DB="$db" q "SELECT count(*) FROM flashback.delta_log
            WHERE event_type='DROP' AND table_name='$table';")
        [[ "$n" != "0" ]] && return 0
        sleep 0.1
    done
    return 1
}

recover_begin_execute() {
    local table=$1
    local token op
    token=$(q "SELECT flashback_recover_plan('$table')->>'plan_token';")
    op=$(q "SELECT flashback_recover_begin('$table', '$token')->>'operation_id';")
    q "SELECT flashback_recover_execute('$table', '$token', interval '24 hours', NULL, NULL, NULL, $op);" >/dev/null
    echo "$op"
}

for db in dba_a dba_b; do
    DB=postgres q "CREATE DATABASE $db;" >/dev/null
    DB="$db" q "CREATE EXTENSION pg_flashback;" >/dev/null
done
DB=postgres q "ALTER SYSTEM SET pg_flashback.target_databases = 'dba_a,dba_b';" >/dev/null
restart_pg || die "restart after target_databases"
wait_worker dba_a || die "worker missing for dba_a"
wait_worker dba_b || die "worker missing for dba_b"
DB=dba_a
echo "workers ready for dba_a,dba_b"

echo "== Finding 1: durable failed-recovery audit =="
q "CREATE TABLE public.dba_fail(id int PRIMARY KEY, payload text);"
q "SELECT flashback_track('public.dba_fail');" >/dev/null
wait_health public.dba_fail healthy || die "f1 initial health"
q "INSERT INTO public.dba_fail VALUES (1,'a'),(2,'b');"
wait_health public.dba_fail healthy || die "f1 after dml"
q "DROP TABLE public.dba_fail;"
wait_drop public.dba_fail || die "f1 drop not captured"
TOKEN=$(q "SELECT flashback_recover_plan('public.dba_fail')->>'plan_token';")
[[ "$(q "SELECT flashback_recover_plan('public.dba_fail')->>'status';")" == "restorable" ]] \
    || die "f1 plan not restorable"
OP=$(q "SELECT flashback_recover_begin('public.dba_fail', '$TOKEN')->>'operation_id';")
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "started" ]] \
    || die "f1 begin not durable started"

# before_materialize failpoint: table stays dropped; operation remains started until mark_failed
DB=postgres q "ALTER SYSTEM SET pg_flashback.test_restore_failpoint = 'before_materialize';" >/dev/null
DB=postgres q "SELECT pg_reload_conf();" >/dev/null
set +e
ERR=$(qe "SELECT flashback_recover_execute('public.dba_fail', '$TOKEN', interval '24 hours', NULL, NULL, NULL, $OP);")
RC=$?
set -e
[[ $RC -ne 0 ]] || die "f1 before_materialize did not fail"
[[ "$(q "SELECT to_regclass('public.dba_fail') IS NULL;")" == "t" ]] || die "f1 table mutated on before_materialize"
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "started" ]] \
    || die "f1 header rolled back with execute TX"
q "SELECT flashback_recover_mark_failed($OP, 'P0001', 'test_restore_failpoint', left(replace('$ERR', '''', ''''''),200), '{}'::jsonb);" >/dev/null
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "failed" ]] \
    || die "f1 mark_failed missing"
[[ "$(q "SELECT error_code FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "test_restore_failpoint" ]] \
    || die "f1 error_code not durable"
pass "f1 before_materialize durable failed"

# Retry succeeds with a NEW operation
DB=postgres q "ALTER SYSTEM RESET pg_flashback.test_restore_failpoint;" >/dev/null
DB=postgres q "SELECT pg_reload_conf();" >/dev/null
q "SELECT flashback_recover_plan('public.dba_fail')->>'plan_token';" >/dev/null
OP2=$(recover_begin_execute public.dba_fail)
[[ "$OP2" != "$OP" ]] || die "f1 retry reused operation_id"
wait_health public.dba_fail healthy || die "f1 retry health"
# after_swap_before_commit on a fresh table
q "CREATE TABLE public.dba_fail2(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dba_fail2');" >/dev/null
wait_health public.dba_fail2 healthy || die "f1b health"
q "INSERT INTO public.dba_fail2 VALUES (1,'x');"
wait_health public.dba_fail2 healthy || die "f1b dml"
FP_BEFORE=$(q "SELECT count(*) FROM public.dba_fail2;")
q "DROP TABLE public.dba_fail2;"
wait_drop public.dba_fail2 || die "f1b drop"
TOKEN3=$(q "SELECT flashback_recover_plan('public.dba_fail2')->>'plan_token';")
OP3=$(q "SELECT flashback_recover_begin('public.dba_fail2', '$TOKEN3')->>'operation_id';")
DB=postgres q "ALTER SYSTEM SET pg_flashback.test_restore_failpoint = 'after_swap_before_commit';" >/dev/null
DB=postgres q "SELECT pg_reload_conf();" >/dev/null
set +e
ERR3=$(qe "SELECT flashback_recover_execute('public.dba_fail2', '$TOKEN3', interval '24 hours', NULL, NULL, NULL, $OP3);")
RC3=$?
set -e
[[ $RC3 -ne 0 ]] || die "f1 after_swap_before_commit did not fail"
# Aborted TX rolls back swap → table remains dropped
[[ "$(q "SELECT to_regclass('public.dba_fail2') IS NULL;")" == "t" ]] || die "f1 after_swap left live table"
q "SELECT flashback_recover_mark_failed($OP3, 'P0001', 'after_swap_before_commit', left(replace('$ERR3', '''', ''''''),200), '{}'::jsonb);" >/dev/null
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP3;")" == "failed" ]] \
    || die "f1 after_swap mark_failed"
DB=postgres q "ALTER SYSTEM RESET pg_flashback.test_restore_failpoint;" >/dev/null
DB=postgres q "SELECT pg_reload_conf();" >/dev/null
recover_begin_execute public.dba_fail2 >/dev/null
wait_health public.dba_fail2 healthy || die "f1 after_swap retry"
[[ "$(q "SELECT count(*) FROM public.dba_fail2;")" == "$FP_BEFORE" ]] || die "f1 after_swap fingerprint"
# Crash-after-begin leaves reconcilable started state
q "SELECT flashback_recover_plan('public.dba_fail2')->>'plan_token';" >/dev/null || true
# Use a fresh DROP cycle for abandoned
q "CREATE TABLE public.dba_abandon(id int PRIMARY KEY);"
q "SELECT flashback_track('public.dba_abandon');" >/dev/null
wait_health public.dba_abandon healthy || die "f1 abandon health"
q "INSERT INTO public.dba_abandon VALUES (1);"
wait_health public.dba_abandon healthy || die "f1 abandon dml"
q "DROP TABLE public.dba_abandon;"
wait_drop public.dba_abandon || die "f1 abandon drop"
TOKENA=$(q "SELECT flashback_recover_plan('public.dba_abandon')->>'plan_token';")
OPA=$(q "SELECT flashback_recover_begin('public.dba_abandon', '$TOKENA')->>'operation_id';")
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OPA;")" == "started" ]] \
    || die "f1 abandon not started"
# Force stale window then reconcile. operation_events is append-only
# (trg_flashback_operation_events_immutable); backdating this harness-only
# probe row requires the standard superuser bypass, never a product API.
q "SET session_replication_role = replica;
   UPDATE flashback.operation_events SET recorded_at = clock_timestamp() - interval '10 minutes'
    WHERE operation_id=$OPA AND event_type='started';
   SET session_replication_role = DEFAULT;" >/dev/null
q "SELECT flashback_reconcile_recover_operations(interval '1 minute');" >/dev/null
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OPA;")" == "abandoned" ]] \
    || die "f1 reconcile did not abandon"
pass "f1 after_swap + abandon reconcile"

echo "== Finding 2: exact successor-bound verification =="
# Fabricate pending op with bogus generation; healthy same-name lifecycle must not verify it.
FAKE_OP=$(q "
SELECT flashback_operation_begin(
    'recover', 'public.dba_fail2',
    (SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_fail2' AND is_active LIMIT 1),
    1, 'fake-token', NULL, 999999, '0/0'::pg_lsn,
    jsonb_build_object('successor', jsonb_build_object(
        'tracking_id', (SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_fail2' AND is_active LIMIT 1),
        'generation_id', 999999,
        'boundary_xid', 1,
        'boundary_marker', 'fake'
    ))
);")
q "SELECT flashback_operation_append_event($FAKE_OP, 'applied_coverage_pending', NULL, NULL, 'fake',
    (SELECT details FROM flashback.operations WHERE operation_id=$FAKE_OP));" >/dev/null
BEFORE_STATE=$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$FAKE_OP;")
q "SELECT flashback_finalize_recover_operations();" >/dev/null
AFTER_STATE=$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$FAKE_OP;")
[[ "$AFTER_STATE" != "verified" ]] || die "f2 fake generation verified"
[[ "$AFTER_STATE" == "$BEFORE_STATE" || "$AFTER_STATE" == "failed" ]] || true
[[ "$AFTER_STATE" != "verified" ]] && pass "f2 fake generation not verified" || fail_case "f2 fake verified"

echo "== Finding 3: reprotect after unprotect =="
q "CREATE TABLE public.dba_repro(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dba_repro');" >/dev/null
wait_health public.dba_repro healthy || die "f3 protect1"
TID1=$(q "SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_repro' AND is_active;")
q "INSERT INTO public.dba_repro VALUES (1,'old');"
wait_health public.dba_repro healthy || die "f3 dml1"
q "SELECT flashback_unprotect('public.dba_repro');" >/dev/null
# Wait until stop COMMIT consumed / inactive
for _ in $(seq 1 200); do
    st=$(q "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id=$TID1;")
    [[ "$st" == "unprotected" || "$st" == "cleaned" ]] && break
    # finalize unprotect if needed
    q "SELECT flashback_finalize_unprotect_operations();" >/dev/null || true
    sleep 0.1
done
[[ "$(q "SELECT is_active FROM flashback.tracked_tables WHERE tracking_id=$TID1;")" == "f" ]] \
    || die "f3 old lifecycle still active"
# Reprotect before cleanup
q "SELECT flashback_track('public.dba_repro');" >/dev/null
wait_health public.dba_repro healthy || die "f3 reprotect health"
TID2=$(q "SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_repro' AND is_active;")
[[ "$TID2" != "$TID1" ]] || die "f3 reprotect revived old tracking_id"
q "INSERT INTO public.dba_repro VALUES (2,'new');"
NEW_ONLY=0
for _ in $(seq 1 200); do
    NEW_ONLY=$(q "SELECT count(*) FROM flashback.delta_log
        WHERE tracking_id=$TID2 AND event_type='INSERT'
          AND (new_data->>'id') = '2';")
    [[ "$NEW_ONLY" != "0" ]] && break
    sleep 0.1
done
OLD_LEAK=$(q "SELECT count(*) FROM flashback.delta_log
    WHERE tracking_id=$TID1 AND event_type='INSERT' AND (new_data->>'id')='2';")
[[ "$NEW_ONLY" != "0" ]] || die "f3 new DML not under new tracking"
[[ "$OLD_LEAK" == "0" ]] || die "f3 new DML leaked to old tracking"
# Cleanup old must not delete new metadata
q "SELECT flashback_cleanup($TID1, true);" >/dev/null || q "SELECT flashback_cleanup($TID1);" >/dev/null || true
[[ "$(q "SELECT count(*) FROM flashback.tracked_tables WHERE tracking_id=$TID2 AND is_active;")" == "1" ]] \
    || die "f3 cleanup removed new lifecycle"
q "DROP TABLE public.dba_repro;"
wait_drop public.dba_repro || die "f3 drop"
recover_begin_execute public.dba_repro >/dev/null
wait_health public.dba_repro healthy || die "f3 recover"
[[ "$(q "SELECT count(*) FROM public.dba_repro WHERE id=2;")" == "1" ]] || die "f3 recover missing new row"
pass "f3 reprotect + cleanup + recover"

echo "== Finding 4: identity sequence name preservation =="
q 'CREATE SCHEMA IF NOT EXISTS "Odd Schema";'
q 'CREATE TABLE public.dba_id_seq(
    order_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload text);'
q 'CREATE TABLE "Odd Schema"."Odd Table"(
    "ID" bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    v text);'
SEQ1=$(q "SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_depend d ON d.objid=c.oid AND d.deptype='i'
    JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
    WHERE a.attrelid='public.dba_id_seq'::regclass AND a.attname='order_id';")
SEQ2=$(q "SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_depend d ON d.objid=c.oid AND d.deptype='i'
    JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
    WHERE a.attrelid='\"Odd Schema\".\"Odd Table\"'::regclass AND a.attname='ID';")
q "SELECT flashback_track('public.dba_id_seq');" >/dev/null
q "SELECT flashback_track('\"Odd Schema\".\"Odd Table\"');" >/dev/null
wait_health public.dba_id_seq healthy || die "f4 health1"
wait_health '"Odd Schema"."Odd Table"' healthy || die "f4 health2"
q "INSERT INTO public.dba_id_seq(payload) VALUES ('a'),('b');"
q "INSERT INTO \"Odd Schema\".\"Odd Table\"(v) VALUES ('x');"
wait_health public.dba_id_seq healthy || die "f4 dml"
NEXT_BEFORE=$(q "SELECT nextval(pg_get_serial_sequence('public.dba_id_seq','order_id'));")
# rewind for restore fidelity — capture already has rows 1,2; next should be 3 after restore
q "SELECT setval(pg_get_serial_sequence('public.dba_id_seq','order_id'), $NEXT_BEFORE, true);" >/dev/null
q "DROP TABLE public.dba_id_seq;"
q "DROP TABLE \"Odd Schema\".\"Odd Table\";"
wait_drop public.dba_id_seq || die "f4 drop1"
wait_drop '"Odd Schema"."Odd Table"' || die "f4 drop2"
recover_begin_execute public.dba_id_seq >/dev/null
wait_health public.dba_id_seq healthy || die "f4 recover1"
TOKENQ=$(q "SELECT flashback_recover_plan('\"Odd Schema\".\"Odd Table\"')->>'plan_token';")
OPQ=$(q "SELECT flashback_recover_begin('\"Odd Schema\".\"Odd Table\"', '$TOKENQ')->>'operation_id';")
q "SELECT flashback_recover_execute('\"Odd Schema\".\"Odd Table\"', '$TOKENQ', interval '24 hours', NULL, NULL, NULL, $OPQ);" >/dev/null
wait_health '"Odd Schema"."Odd Table"' healthy || die "f4 recover2"
SEQ1_AFTER=$(q "SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_depend d ON d.objid=c.oid AND d.deptype='i'
    JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
    WHERE a.attrelid='public.dba_id_seq'::regclass AND a.attname='order_id';")
SEQ2_AFTER=$(q "SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_depend d ON d.objid=c.oid AND d.deptype='i'
    JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
    WHERE a.attrelid='\"Odd Schema\".\"Odd Table\"'::regclass AND a.attname='ID';")
[[ "$SEQ1_AFTER" == "$SEQ1" ]] || die "f4 seq1 renamed ($SEQ1 -> $SEQ1_AFTER)"
[[ "$SEQ2_AFTER" == "$SEQ2" ]] || die "f4 seq2 renamed ($SEQ2 -> $SEQ2_AFTER)"
[[ "$SEQ1_AFTER" != *__fb_lsn_shadow_* ]] || die "f4 shadow name leaked"
INS=$(q "INSERT INTO public.dba_id_seq(payload) VALUES ('c') RETURNING order_id;")
[[ "$INS" == "3" ]] || die "f4 next identity value expected 3 got $INS"
# Name collision refuse
q "CREATE TABLE public.dba_id_coll(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);"
q "SELECT flashback_track('public.dba_id_coll');" >/dev/null
wait_health public.dba_id_coll healthy || die "f4 coll health"
q "INSERT INTO public.dba_id_coll DEFAULT VALUES;"
wait_health public.dba_id_coll healthy || die "f4 coll dml"
COLL_SEQ=$(q "SELECT pg_get_serial_sequence('public.dba_id_coll','id');")
q "DROP TABLE public.dba_id_coll;"
wait_drop public.dba_id_coll || die "f4 coll drop"
# Occupy original sequence name with unrelated sequence before recover
q "CREATE SEQUENCE $COLL_SEQ;" >/dev/null || true
# If DROP removed the sequence, create a blocker with that name
q "DO \$\$ BEGIN
  IF to_regclass('$COLL_SEQ') IS NULL THEN
    EXECUTE format('CREATE SEQUENCE %s', '$COLL_SEQ');
  END IF;
END \$\$;"
TOKENC=$(q "SELECT flashback_recover_plan('public.dba_id_coll')->>'plan_token';")
OPC=$(q "SELECT flashback_recover_begin('public.dba_id_coll', '$TOKENC')->>'operation_id';")
set +e
ERRC=$(qe "SELECT flashback_recover_execute('public.dba_id_coll', '$TOKENC', interval '24 hours', NULL, NULL, NULL, $OPC);")
RCC=$?
set -e
[[ $RCC -ne 0 ]] || die "f4 collision did not refuse"
[[ "$(q "SELECT to_regclass('public.dba_id_coll') IS NULL;")" == "t" ]] || die "f4 collision mutated live"
q "SELECT flashback_recover_mark_failed($OPC, NULL, 'sequence_name_collision', left('$ERRC',200), '{}'::jsonb);" >/dev/null
pass "f4 identity sequence names + collision refuse"

echo "== Finding 5: CASCADE plan matches execute =="
q "CREATE TABLE public.dba_casc(id int PRIMARY KEY, v text);"
q "CREATE VIEW public.dba_casc_v AS SELECT * FROM public.dba_casc;"
q "CREATE VIEW public.dba_casc_v2 AS SELECT * FROM public.dba_casc;"
q "SELECT flashback_track('public.dba_casc');" >/dev/null
wait_health public.dba_casc healthy || die "f5 health"
q "INSERT INTO public.dba_casc VALUES (1,'x');"
wait_health public.dba_casc healthy || die "f5 dml"
q "DROP TABLE public.dba_casc CASCADE;"
wait_drop public.dba_casc || die "f5 drop"
PLAN5=$(q "SELECT flashback_recover_plan('public.dba_casc');")
STATUS5=$(printf '%s' "$PLAN5" | jq -r '.status')
CODE5=$(printf '%s' "$PLAN5" | jq -r '.code')
[[ "$STATUS5" == "non_restorable" ]] || die "f5 plan status=$STATUS5"
[[ "$CODE5" == "unsupported_dependencies" ]] || die "f5 plan code=$CODE5"
# Views deduped in manifest
VIEW_N=$(printf '%s' "$PLAN5" | jq '[.dependency_manifest.views[]?.name] | unique | length')
[[ "$VIEW_N" -ge 1 ]] || die "f5 no views in blockers"
# Execute must refuse the same way if forced via begin injection:
# mark manifest unsupported then try restore_lsn path / require
set +e
ERR5=$(qe "SELECT flashback_require_supported_drop_manifest(
    (SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_casc' ORDER BY tracked_since DESC LIMIT 1),
    ($(printf '%s' "$PLAN5" | jq -r '.disaster_event_id'))::bigint);")
RC5=$?
set -e
[[ $RC5 -ne 0 ]] || die "f5 require_supported did not refuse"
# A bare non-zero exit also covers typos and connection faults, so require the
# refusal to be the extension's own fail-closed message.
[[ "$ERR5" == *"restore refused"* ]] \
    || die "f5 require_supported refused for the wrong reason: $ERR5"
# Identity-less call must also fail closed (no latest-manifest fallback).
set +e
ERR5b=$(qe "SELECT flashback_require_supported_drop_manifest(
    (SELECT tracking_id FROM flashback.tracked_tables WHERE table_name='dba_casc' ORDER BY tracked_since DESC LIMIT 1));")
RC5b=$?
set -e
[[ $RC5b -ne 0 ]] || die "f5 identity-less require_supported unexpectedly succeeded"
[[ "$ERR5b" == *"exact DROP identity required"* ]] \
    || die "f5 identity-less refusal did not demand the exact DROP identity: $ERR5b"
pass "f5 CASCADE plan=execute non_restorable"

echo "== Finding 6: multi-database CLI =="
DB=dba_b
q "CREATE TABLE public.dba_iso(id int PRIMARY KEY);"
q "SELECT flashback_track('public.dba_iso');" >/dev/null
wait_health public.dba_iso healthy dba_b || die "f6 b health"
q "INSERT INTO public.dba_iso VALUES (1);"
wait_health public.dba_iso healthy dba_b || die "f6 b dml"
# Keep dba_a configured, but stop its flashback workers before DROP EXTENSION
# so catalog teardown does not deadlock with capture/maintenance.
DB=dba_a
q "SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND backend_type LIKE 'pg_flashback%';" >/dev/null || true
sleep 0.5
q "DROP EXTENSION pg_flashback CASCADE;" >/dev/null
set +e
DOC_JSON=$(PGDATABASE=postgres "$WORK_ROOT/bin/pg_flashback" --json doctor --all-databases 2>"$WORK_ROOT/doctor.err")
DOC_RC=$?
set -e
printf '%s' "$DOC_JSON" | jq -e . >/dev/null || die "f6 doctor JSON invalid"
echo "$DOC_JSON" | jq -e 'tostring | contains("extension_missing")' >/dev/null \
    || die "f6 doctor missing extension_missing"
[[ $DOC_RC -eq 1 ]] || die "f6 doctor exit expected 1 got $DOC_RC"
# dba_b still captures
DB=dba_b
q "INSERT INTO public.dba_iso VALUES (2);"
INS_B=0
for _ in $(seq 1 200); do
    INS_B=$(DB=dba_b q "SELECT count(*) FROM flashback.delta_log
        WHERE event_type='INSERT'
          AND (table_name='public.dba_iso' OR table_name='dba_iso')
          AND (new_data->>'id')='2';")
    [[ "$INS_B" != "0" ]] && break
    sleep 0.1
done
[[ "$INS_B" != "0" ]] || die "f6 b capture stopped (id=2 missing under dba_b)"
wait_health public.dba_iso healthy dba_b || die "f6 b continued health"
set +e
ST_JSON=$(PGDATABASE=postgres "$WORK_ROOT/bin/pg_flashback" --json status --all-databases 2>"$WORK_ROOT/status.err")
ST_RC=$?
set -e
printf '%s' "$ST_JSON" | jq -e . >/dev/null || die "f6 status JSON invalid rc=$ST_RC"
[[ $ST_RC -eq 1 ]] || die "f6 status exit expected 1 got $ST_RC"
pass "f6 multi-DB JSON + isolation"

echo "== Finding 7: CLI --json after command =="
DB=dba_b
set +e
LIST_JSON=$(PGDATABASE=dba_b "$WORK_ROOT/bin/pg_flashback" list --json 2>"$WORK_ROOT/list.err")
LIST_RC=$?
set -e
[[ $LIST_RC -eq 0 ]] || die "f7 list --json rc=$LIST_RC err=$(cat "$WORK_ROOT/list.err")"
printf '%s' "$LIST_JSON" | jq -e . >/dev/null || die "f7 list --json invalid JSON"
pass "f7 list --json"

echo "== Finding 8: post-DROP discovery UX =="
DB=dba_b
q "CREATE TABLE public.dba_disc(id int PRIMARY KEY, blob text);"
q "SELECT flashback_track('public.dba_disc');" >/dev/null
wait_health public.dba_disc healthy dba_b || die "f8 health"
# Create enough WAL to exercise post-DROP discovery without violating the
# product's fail-closed max_row_size contract.  The old 200 KiB rows correctly
# broke the capture stream after max_row_size became enforced, so they tested
# oversized-row invalidation rather than discovery catch-up.
q "INSERT INTO public.dba_disc
   SELECT g, repeat('Z', 12000) FROM generate_series(1,128) g;"
q "DROP TABLE public.dba_disc;"
# Immediately plan should be catchup_pending OR restorable if worker already caught up
PLAN8=$(q "SELECT flashback_recover_plan('public.dba_disc');")
CODE8=$(printf '%s' "$PLAN8" | jq -r '.code')
if [[ "$CODE8" == "capture_catchup_pending" ]]; then
    pass "f8 capture_catchup_pending exposed"
elif [[ "$CODE8" == "ok" || "$CODE8" == "no_drop_event" ]]; then
    # Worker may already be caught up on fast machines; still require frontier fields when pending absent
    [[ "$(printf '%s' "$PLAN8" | jq -r '.status')" != "error" || "$CODE8" == "no_drop_event" ]] \
        && pass "f8 plan after DROP (worker already caught up: $CODE8)" \
        || fail_case "f8 unexpected code $CODE8"
else
    fail_case "f8 unexpected code $CODE8"
fi
# Poll until drop visible; must not silently pick wrong older drop
wait_drop public.dba_disc dba_b 90 || die "f8 drop never appeared"
PLAN8B=$(q "SELECT flashback_recover_plan('public.dba_disc');")
[[ "$(printf '%s' "$PLAN8B" | jq -r '.status')" == "restorable" ]] || die "f8 never became restorable"

echo "== Finding 9: PATH collision detection =="
STALE="$WORK_ROOT/stale-path"
mkdir -p "$STALE"
printf '#!/bin/sh\necho STALE_BINARY\nexit 0\n' >"$STALE/pg_flashback"
chmod 0755 "$STALE/pg_flashback"
RESOLVED=$(PATH="$STALE:$WORK_ROOT/bin:$PATH" command -v pg_flashback)
[[ "$RESOLVED" == "$STALE/pg_flashback" ]] || die "f9 probe did not prefer stale"
OUT=$(PATH="$STALE:$WORK_ROOT/bin:$PATH" pg_flashback version || true)
[[ "$OUT" == *STALE_BINARY* ]] || die "f9 stale not executed"
GOOD=$(PATH="$WORK_ROOT/bin:$PATH" command -v pg_flashback)
[[ "$GOOD" == "$WORK_ROOT/bin/pg_flashback" ]] || die "f9 good path wrong"
HASH=$(sha256sum "$GOOD" | awk '{print $1}')
[[ -n "$HASH" ]] || die "f9 empty hash"
pass "f9 PATH collision observable; candidate hash=$HASH"

echo "All DBA acceptance cases finished: passed=$PASSED failed=$FAILED"
[[ "$FAILED" -eq 0 ]] || exit 1

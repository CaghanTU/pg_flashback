#!/usr/bin/env bash
# Exact-candidate packaged functional suite for the supported local_delta path.
# Installs only from CANDIDATE_DIR and never cargo-builds.
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
KEEP="${PGFB_FUNC_KEEP:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_FUNC_BASE:-$REPO_ROOT/target/exact-candidate-functional}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="${PGFB_FUNC_RESULT:-$BASE/results/exact-candidate-functional-$RUN_ID.json}"
SOCKET="/tmp/pgfb-func-$RUN_ID"
DATA="$RUN_ROOT/data"
PORT=$((33000 + ($$ % 20000)))
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
EC_BOUND=0
RUN_COMPLETE=0
PASSED=0
declare -A CASE_PASS

log() { printf '[candidate-functional] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); CASE_PASS["$1"]=true; log "PASS[$PASSED]: $1"; }

write_result() {
    local rc=$1 status=failed
    [[ "$rc" == 0 && "$RUN_COMPLETE" == 1 ]] && status=passed
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$status" \
        --argjson passed "$PASSED" \
        --argjson identity "$([[ "$EC_BOUND" == 1 ]] && exact_candidate_identity_json || echo '{}')" \
        --argjson dml "${CASE_PASS[local_dml]:-false}" \
        --argjson drop "${CASE_PASS[local_drop_restore]:-false}" \
        --argjson trunc "${CASE_PASS[local_truncate_restore]:-false}" \
        --argjson alter "${CASE_PASS[local_alter_fail_closed]:-false}" \
        --argjson toast "${CASE_PASS[local_quoted_toast]:-false}" \
        '{
          qualification_kind:"exact_candidate_local_functional",
          status:$status,
          assertions_passed:$passed,
          identity:$identity,
          cases:{
            local_dml:$dml,
            local_drop_restore:$drop,
            local_truncate_restore:$trunc,
            local_alter_fail_closed:$alter,
            local_quoted_toast:$toast
          }
        }' >"$RESULT_JSON"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    [[ "$PRIMARY_STARTED" == 1 ]] && "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    [[ "$PREFIX_INSTALLED" == 1 ]] && exact_candidate_restore_prefix >/dev/null 2>&1
    write_result "$rc"
    rm -rf "$SOCKET"
    if [[ "$RUN_COMPLETE" == 1 && "$KEEP" != 1 ]]; then rm -rf "$RUN_ROOT"; fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$RUN_ROOT" "$SOCKET"
chmod 700 "$SOCKET"
exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
EC_BOUND=1
exact_candidate_install_into_prefix || die "candidate prefix install failed"
PREFIX_INSTALLED=1
exact_candidate_verify_installed || die "installed identity mismatch"
CLI="$EC_EXT_ROOT/bin/pg_flashback"
[[ -x "$CLI" ]] || die "packaged CLI missing"

"$PG_BIN/initdb" -D "$DATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = $PORT
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$RUN_ROOT/postgresql.log" start -w >/dev/null
PRIMARY_STARTED=1
export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE=postgres
q() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "$1"; }
wait_ready() {
    for _ in $(seq 1 300); do
        [[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_healthy() {
    local rel=$1
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='$rel';")" == healthy ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_events() {
    local rel=$1 type=$2 count=$3
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT count(*) FROM flashback.delta_log
                 WHERE table_name='$rel' AND event_type='$type' AND commit_lsn IS NOT NULL;")" -ge "$count" ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_drop() {
    local rel=$1
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT count(*) FROM flashback_disaster_points('$rel', interval '1 hour')
                 WHERE event_type='DROP' AND status='restorable';")" -ge 1 ]] && return 0
        sleep 0.05
    done
    return 1
}
fp() {
    local rel=$1
    q "SELECT count(*)::text || '|' ||
              COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
       FROM $rel AS t;"
}

q "CREATE EXTENSION pg_flashback;"
wait_ready || die "workers not ready"
"$CLI" doctor >/dev/null

q "CREATE TABLE public.func_dml(id int PRIMARY KEY, v text NOT NULL);"
"$CLI" protect public.func_dml >/dev/null
wait_healthy public.func_dml || die "func_dml not healthy"
q "INSERT INTO public.func_dml VALUES (1,'a'),(2,'b');
   UPDATE public.func_dml SET v='bb' WHERE id=2;
   DELETE FROM public.func_dml WHERE id=1;"
wait_events public.func_dml DELETE 1 || die "DML events not captured"
[[ "$(q "TABLE public.func_dml;")" == "2|bb" ]] || die "live DML state wrong"
pass local_dml

q "CREATE ROLE func_reader;
   CREATE TABLE public.func_drop(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text NOT NULL);
   CREATE INDEX func_drop_v_idx ON public.func_drop(v);
   GRANT SELECT ON public.func_drop TO func_reader;"
"$CLI" protect public.func_drop >/dev/null
wait_healthy public.func_drop || die "func_drop not healthy"
q "INSERT INTO public.func_drop(v) SELECT 'v-'||g FROM generate_series(1,50) g;"
wait_events public.func_drop INSERT 50 || die "DROP fixture not captured"
DROP_FP="$(fp public.func_drop)"
q "DROP TABLE public.func_drop;"
wait_drop public.func_drop || die "DROP not restorable"
"$CLI" recover public.func_drop --latest-drop --yes >/dev/null
[[ "$(fp public.func_drop)" == "$DROP_FP" ]] || die "DROP fingerprint mismatch"
[[ "$(q "SELECT has_table_privilege('func_reader','public.func_drop','SELECT');")" == t ]] || die "DROP ACL mismatch"
pass local_drop_restore

q "CREATE TABLE public.func_trunc(id int PRIMARY KEY, v text NOT NULL);"
"$CLI" protect public.func_trunc >/dev/null
wait_healthy public.func_trunc || die "func_trunc not healthy"
q "INSERT INTO public.func_trunc SELECT g,'v-'||g FROM generate_series(1,25) g;"
wait_events public.func_trunc INSERT 25 || die "TRUNCATE fixture not captured"
TRUNC_FP="$(fp public.func_trunc)"
TARGET_LSN="$(q "SELECT max(commit_lsn) FROM flashback.delta_log WHERE table_name='public.func_trunc' AND event_type='INSERT';")"
q "TRUNCATE public.func_trunc;"
wait_events public.func_trunc TRUNCATE 1 || die "TRUNCATE event not captured"
"$CLI" recover public.func_trunc --lsn "$TARGET_LSN" --yes >/dev/null
[[ "$(fp public.func_trunc)" == "$TRUNC_FP" ]] || die "TRUNCATE recovery mismatch"
pass local_truncate_restore

q "CREATE TABLE public.func_alter(id int PRIMARY KEY, v text NOT NULL);"
"$CLI" protect public.func_alter >/dev/null
wait_healthy public.func_alter || die "func_alter not healthy"
q "ALTER TABLE public.func_alter ADD COLUMN note text DEFAULT 'n';"
wait_events public.func_alter ALTER 1 || die "ALTER epoch event not captured"
[[ "$(q "SELECT count(*) FROM flashback.schema_versions
          WHERE tracking_id=(SELECT tracking_id FROM flashback.tracked_tables
          WHERE table_name='func_alter' AND is_active);")" -ge 2 ]] || die "ALTER schema epoch missing"
pass local_alter_fail_closed

q "CREATE SCHEMA odd;
   CREATE TABLE odd.\"quoted table\"(\"id col\" int PRIMARY KEY, payload text NOT NULL);"
"$CLI" protect 'odd."quoted table"' >/dev/null
wait_healthy 'odd."quoted table"' || die "quoted table not healthy"
q "INSERT INTO odd.\"quoted table\" VALUES (1, repeat('toast-',20000));"
wait_events 'odd."quoted table"' INSERT 1 || die "quoted TOAST row not captured"
TOAST_FP="$(fp 'odd."quoted table"')"
q "DROP TABLE odd.\"quoted table\";"
wait_drop 'odd."quoted table"' || die "quoted DROP not restorable"
"$CLI" recover 'odd."quoted table"' --latest-drop --yes >/dev/null
[[ "$(fp 'odd."quoted table"')" == "$TOAST_FP" ]] || die "quoted TOAST fingerprint mismatch"
pass local_quoted_toast

exact_candidate_verify_end_state || die "candidate identity changed"
RUN_COMPLETE=1
log "PASS: $PASSED local functional cases"

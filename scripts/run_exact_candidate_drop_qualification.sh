#!/usr/bin/env bash
# Exact-candidate DROP qualification.
#
# This is the product-claim gate for DROP recovery. It is intentionally
# separate from the 24-hour stability soak: the soak proves elapsed stability,
# while this suite repeatedly destroys real relations and proves reconstruction
# through both supported recovery profiles.
#
# Installs ONLY from CANDIDATE_DIR archives. Never cargo-builds.

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PGBACKREST="${PGBACKREST:-/usr/local/bin/pgbackrest}"
KEEP="${PGFB_DROP_KEEP:-1}"

REPEAT_COUNT="${PGFB_DROP_REPEAT_COUNT:-100}"
REPEAT_ROWS="${PGFB_DROP_REPEAT_ROWS:-256}"
MEDIUM_ROWS="${PGFB_DROP_MEDIUM_ROWS:-50000}"
TOAST_ROWS="${PGFB_DROP_TOAST_ROWS:-2000}"
BACKUP_ROWS="${PGFB_DROP_BACKUP_ROWS:-20000}"
BACKUP_WAL_ROWS="${PGFB_DROP_BACKUP_WAL_ROWS:-1000}"
MIN_FREE_BYTES="${PGFB_DROP_MIN_FREE_BYTES:-4294967296}"
MAX_WORK_BYTES="${PGFB_DROP_MAX_WORK_BYTES:-4294967296}"

for numeric in REPEAT_COUNT REPEAT_ROWS MEDIUM_ROWS TOAST_ROWS BACKUP_ROWS BACKUP_WAL_ROWS MIN_FREE_BYTES MAX_WORK_BYTES; do
    [[ "${!numeric}" =~ ^[0-9]+$ ]] || {
        echo "FAIL: $numeric must be a non-negative integer" >&2
        exit 2
    }
done
(( REPEAT_COUNT >= 10 && REPEAT_COUNT <= 500 )) || {
    echo "FAIL: PGFB_DROP_REPEAT_COUNT must be between 10 and 500" >&2
    exit 2
}
(( REPEAT_ROWS >= 32 && MEDIUM_ROWS >= 10000 && TOAST_ROWS >= 100 && BACKUP_ROWS >= 5000 )) || {
    echo "FAIL: DROP qualification row counts are below their meaningful minimums" >&2
    exit 2
}

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_DROP_BASE:-$REPO_ROOT/target/exact-candidate-drop}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="${PGFB_DROP_RESULT:-$BASE/results/exact-candidate-drop-$RUN_ID.json}"
CASES_JSONL="$RUN_ROOT/drop-cases.jsonl"
PROGRESS_LOG="$RUN_ROOT/progress.log"

STATUS=failed
RUN_COMPLETE=0
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
EC_BOUND=0
DROP_ATTEMPTED=0
DROP_PASSED=0
CUMULATIVE_ROWS_DROPPED=0
CUMULATIVE_LOGICAL_BYTES_DROPPED=0
CUMULATIVE_PHYSICAL_BYTES_DROPPED=0
PEAK_WORK_BYTES=0
START_FREE_BYTES=0
START_MONO_NS="$(exact_candidate_monotonic_now_ns)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BG_WRITER_PID=""
BG_WRITER_STOP=""
SOCKET_DIR=""
HELPER_SOCKET_DIR=""
PRIMARY_DIR=""

log() { printf '[exact-candidate-drop] %s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$PROGRESS_LOG"; }
die() { log "FAIL: $*"; exit 1; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }
monotonic_ms() { python3 - <<'PY'
import time
print(time.monotonic_ns() // 1_000_000)
PY
}

du_bytes() {
    local path=$1 bytes
    [[ -e "$path" ]] || { echo 0; return; }
    for _ in 1 2 3; do
        if bytes="$(du -sb "$path" 2>/dev/null | awk '{print $1}')"; then
            printf '%s\n' "$bytes"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

enforce_resource_bounds() {
    local free work
    free="$(exact_candidate_free_bytes "$RUN_ROOT")" || die "cannot measure filesystem free bytes"
    work="$(du_bytes "$RUN_ROOT")" || die "cannot measure qualification work bytes"
    (( work > PEAK_WORK_BYTES )) && PEAK_WORK_BYTES=$work
    (( free >= MIN_FREE_BYTES )) || die "free bytes $free below reserve $MIN_FREE_BYTES"
    (( work <= MAX_WORK_BYTES )) || die "work bytes $work exceed cap $MAX_WORK_BYTES"
}

record_case() {
    local profile=$1 scenario=$2 iteration=$3 rows=$4 logical_bytes=$5 physical_bytes=$6 restore_ms=$7
    local helper_durations=${8:-null}
    jq -cn \
        --arg profile "$profile" --arg scenario "$scenario" \
        --argjson iteration "$iteration" --argjson rows "$rows" \
        --argjson logical_bytes "$logical_bytes" --argjson physical_bytes "$physical_bytes" \
        --argjson restore_ms "$restore_ms" --argjson helper_durations "$helper_durations" \
        '{profile:$profile, scenario:$scenario, iteration:$iteration,
          rows_at_drop:$rows, logical_tuple_bytes_at_drop:$logical_bytes,
          physical_relation_bytes_at_drop:$physical_bytes,
          restore_wall_ms:$restore_ms, helper_durations:$helper_durations,
          relation_absent_after_drop:true, row_fingerprint_verified:true,
          schema_verified:true, owner_verified:true, acl_verified:true}' >> "$CASES_JSONL"
}

write_result() {
    local rc=$1 end_ns elapsed identity cases summary
    end_ns="$(exact_candidate_monotonic_now_ns)"
    elapsed=$(( (end_ns - START_MONO_NS) / 1000000 ))
    if [[ "$rc" == 0 && "$RUN_COMPLETE" == 1 && "$DROP_ATTEMPTED" == "$DROP_PASSED" ]]; then
        STATUS=passed
    else
        STATUS=failed
    fi
    if [[ "${EC_BOUND:-0}" == 1 ]]; then
        identity="$(exact_candidate_identity_json)"
    else
        identity='{}'
    fi
    cases="$(jq -s '.' "$CASES_JSONL" 2>/dev/null || echo '[]')"
    summary="$(jq -n --argjson cases "$cases" '
      def stats:
        sort as $s
        | if length == 0 then null else {
            min:$s[0],
            p50:$s[((length * 0.50 | ceil) - 1)],
            p95:$s[((length * 0.95 | ceil) - 1)],
            p99:$s[((length * 0.99 | ceil) - 1)],
            max:$s[-1]
          } end;
      {
        local_restore_wall_ms:([$cases[] | select(.profile == "local_delta") | .restore_wall_ms] | stats),
        backup_restore_wall_ms:([$cases[] | select(.profile == "backup") | .restore_wall_ms] | stats)
      }')"
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$STATUS" --arg started "$STARTED_AT" \
        --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson elapsed_ms "$elapsed" --argjson exit_code "$rc" \
        --argjson attempted "$DROP_ATTEMPTED" --argjson passed "$DROP_PASSED" \
        --argjson rows "$CUMULATIVE_ROWS_DROPPED" \
        --argjson logical "$CUMULATIVE_LOGICAL_BYTES_DROPPED" \
        --argjson physical "$CUMULATIVE_PHYSICAL_BYTES_DROPPED" \
        --argjson peak_work "$PEAK_WORK_BYTES" --argjson max_work "$MAX_WORK_BYTES" \
        --argjson start_free "$START_FREE_BYTES" --argjson min_free "$MIN_FREE_BYTES" \
        --argjson concurrent_commits "${CONCURRENT_COMMITS:-0}" \
        --argjson identity "$identity" --argjson cases "$cases" --argjson timings "$summary" \
        '{
          qualification_kind:"exact_candidate_drop_qualification",
          status:$status, started_at_utc:$started, finished_at_utc:$finished,
          elapsed_wall_ms:$elapsed_ms, exit_code:$exit_code,
          drop_summary:{attempted:$attempted, passed:$passed,
            cumulative_rows_at_drop:$rows,
            cumulative_logical_tuple_bytes_at_drop:$logical,
            cumulative_physical_relation_bytes_at_drop:$physical},
          concurrency:{commits_during_medium_drop_restore:$concurrent_commits},
          resources:{peak_work_bytes:$peak_work, max_work_bytes:$max_work,
            start_free_bytes:$start_free, min_free_bytes:$min_free},
          timing_summary:$timings, cases:$cases, identity:$identity,
          claim:"Repeated exact-candidate DROP-to-restore qualification across local_delta and pgBackRest-backed profiles."
        }' > "$RESULT_JSON"
    log "result written: $RESULT_JSON status=$STATUS drops=$DROP_PASSED/$DROP_ATTEMPTED"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM ERR
    set +e
    if [[ -n "$BG_WRITER_PID" ]]; then
        : > "$BG_WRITER_STOP" 2>/dev/null || true
        kill "$BG_WRITER_PID" 2>/dev/null || true
        wait "$BG_WRITER_PID" 2>/dev/null || true
    fi
    if [[ "$PRIMARY_STARTED" == 1 ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_DIR" "$HELPER_SOCKET_DIR" 2>/dev/null || true
    if [[ "$PREFIX_INSTALLED" == 1 ]]; then
        exact_candidate_restore_prefix || rc=1
        PREFIX_INSTALLED=0
    fi
    if [[ "${EC_BOUND:-0}" == 1 ]]; then
        exact_candidate_verify_end_state || rc=1
    fi
    write_result "$rc"
    if [[ "$KEEP" != 1 && "$STATUS" == passed ]]; then
        rm -rf -- "$RUN_ROOT" "$EC_EXTRACT_DIR" "$EC_STASH_DIR"
    else
        log "artifacts kept at $RUN_ROOT"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'rc=$?; log "FAIL: command rc=$rc line=${BASH_LINENO[0]} command=$BASH_COMMAND"; exit "$rc"' ERR

mkdir -p "$RUN_ROOT" "$BASE/results" "$RUN_ROOT/log"
: > "$CASES_JSONL"
: > "$PROGRESS_LOG"

require_executable "$PGBACKREST"
require_executable "$(command -v jq)"
require_executable "$(command -v python3)"
START_FREE_BYTES="$(exact_candidate_free_bytes "$REPO_ROOT")"
(( START_FREE_BYTES >= MIN_FREE_BYTES + MAX_WORK_BYTES )) \
    || die "insufficient free bytes: have $START_FREE_BYTES need $((MIN_FREE_BYTES + MAX_WORK_BYTES))"

EC_STASH_DIR="$RUN_ROOT/prefix-stash"
EC_EXTRACT_DIR="$RUN_ROOT/extract"
exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate identity bind failed"
exact_candidate_install_into_prefix || die "candidate install failed"
PREFIX_INSTALLED=1
HELPER="$EC_HELPER_BIN"
CONTROLLER="$EC_HELPER_ROOT/bin/pg_flashback_backup_restore.sh"
require_executable "$HELPER"
require_executable "$CONTROLLER"

STANZA=drop_qualification
DB_NAME=dropdb
PORT_BASE=$((39000 + ($$ % 15000)))
PRIMARY_PORT=$PORT_BASE
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-drop-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-droph-$RUN_ID"
PRIMARY_DIR="$RUN_ROOT/primary"
REPO_DIR="$RUN_ROOT/repo"
WORK_ROOT="$RUN_ROOT/helper-work"
LOG_DIR="$RUN_ROOT/log"
PGBACKREST_CONFIG="$RUN_ROOT/pgbackrest.conf"
PROOF_HMAC_KEY_FILE="$RUN_ROOT/proof-hmac.key"
HELPER_CONFIG="$RUN_ROOT/helper.json"
EXPIRE_LOCK="$RUN_ROOT/expire.lock"

mkdir -p "$REPO_DIR" "$WORK_ROOT" "$LOG_DIR" "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
chmod 700 "$WORK_ROOT" "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "$PROOF_HMAC_KEY_FILE"
chmod 600 "$PROOF_HMAC_KEY_FILE"
: > "$EXPIRE_LOCK"

pgbr() { "$PGBACKREST" --config="$PGBACKREST_CONFIG" --stanza="$STANZA" "$@"; }
q() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" -c "$1"; }
fingerprint_of() {
    local rel=$1
    q "SELECT count(*)::text || '|' ||
              COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
       FROM $rel AS t;"
}
schema_signature() {
    local rel=$1
    q "WITH r AS (SELECT '$rel'::regclass::oid AS oid),
       cols AS (
         SELECT string_agg(format('%s:%s:%s:%s:%s:%s', a.attname,
                    format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,
                    a.attgenerated,COALESCE(pg_get_expr(d.adbin,d.adrelid),'')), ',' ORDER BY a.attnum) AS v
         FROM r JOIN pg_attribute a ON a.attrelid=r.oid
         LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
         WHERE a.attnum>0 AND NOT a.attisdropped),
       cons AS (
         SELECT string_agg(c.conname || ':' || pg_get_constraintdef(c.oid,true), ',' ORDER BY c.conname) AS v
         FROM r JOIN pg_constraint c ON c.conrelid=r.oid),
       idx AS (
         SELECT string_agg(ic.relname || ':' || pg_get_indexdef(i.indexrelid), ',' ORDER BY ic.relname) AS v
         FROM r JOIN pg_index i ON i.indrelid=r.oid JOIN pg_class ic ON ic.oid=i.indexrelid)
       SELECT md5(COALESCE(cols.v,'') || '|' || COALESCE(cons.v,'') || '|' || COALESCE(idx.v,''))
       FROM cols,cons,idx;"
}
owner_of() { q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='$1'::regclass;"; }
acl_of() { q "SELECT COALESCE(array_to_string(relacl, ','), '') FROM pg_class WHERE oid='$1'::regclass;"; }
row_count_of() { q "SELECT count(*) FROM $1;"; }
logical_bytes_of() { q "SELECT COALESCE(sum(pg_column_size(t)),0)::bigint FROM $1 t;"; }
physical_bytes_of() { q "SELECT pg_total_relation_size('$1'::regclass)::bigint;"; }
tracking_id_of() {
    q "SELECT tracking_id FROM flashback.tracked_tables
       WHERE rel_oid='$1'::regclass AND is_active ORDER BY tracking_id DESC LIMIT 1;"
}
max_event_id() { q "SELECT COALESCE(max(event_id),0) FROM flashback.delta_log WHERE tracking_id=$1;"; }

wait_healthy() {
    local rel=$1 _i health
    for _i in $(seq 1 400); do
        health="$(q "SELECT health FROM flashback_health() WHERE table_name='$rel';")"
        [[ "$health" == healthy ]] && return 0
        q "SELECT flashback_consume_wal(8192);" >/dev/null || true
        sleep 0.05
    done
    return 1
}

wait_capture_ready() {
    local _i state
    for _i in $(seq 1 400); do
        state="$(q "SELECT admission_state FROM flashback_worker_readiness();")"
        [[ "$state" == ready || "$state" == maintenance_missing ]] && return 0
        sleep 0.05
    done
    return 1
}

wait_event_covered() {
    local tracking_id=$1 event_type=$2 after_event_id=$3 attempts=${4:-400}
    local _i lsn covered
    for _i in $(seq 1 "$attempts"); do
        q "SELECT flashback_consume_wal(8192);" >/dev/null || true
        lsn="$(q "SELECT commit_lsn::text FROM flashback.delta_log
                  WHERE tracking_id=$tracking_id AND event_type='$event_type'
                    AND event_id>$after_event_id AND commit_lsn IS NOT NULL
                  ORDER BY event_id DESC LIMIT 1;")"
        if [[ -n "$lsn" ]]; then
            covered="$(q "SELECT EXISTS (
                         SELECT 1 FROM flashback.coverage_generations
                         WHERE tracking_id=$tracking_id
                           AND state IN ('active','sealed')
                           AND boundary_lsn <= '$lsn'::pg_lsn
                           AND (superseded_before_lsn IS NULL OR '$lsn'::pg_lsn < superseded_before_lsn)
                           AND valid_through_lsn >= '$lsn'::pg_lsn);" 2>/dev/null || echo f)"
            if [[ "$covered" == t ]]; then
                printf '%s\n' "$lsn"
                return 0
            fi
        fi
        sleep 0.05
    done
    return 1
}

restore_lsn_retry() {
    local rel=$1 lsn=$2 _i err=""
    for _i in $(seq 1 400); do
        q "SELECT flashback_consume_wal(8192);" >/dev/null || true
        if err="$(q "SELECT flashback_restore_lsn('$rel', '$lsn'::pg_lsn);" 2>&1)"; then
            return 0
        fi
        sleep 0.05
    done
    log "restore_lsn last error for $rel@$lsn: $err"
    return 1
}

perform_local_drop_restore() {
    local scenario=$1 iteration=$2 rel=$3 tracking_id=$4 target_lsn=$5 expected_fp=$6
    local expected_schema=$7 expected_owner=$8 expected_acl=$9 rows=${10} logical=${11} physical=${12}
    local before_drop start_ms end_ms restore_ms
    before_drop="$(max_event_id "$tracking_id")"
    q "DROP TABLE $rel;" >/dev/null
    DROP_ATTEMPTED=$((DROP_ATTEMPTED + 1))
    CUMULATIVE_ROWS_DROPPED=$((CUMULATIVE_ROWS_DROPPED + rows))
    CUMULATIVE_LOGICAL_BYTES_DROPPED=$((CUMULATIVE_LOGICAL_BYTES_DROPPED + logical))
    CUMULATIVE_PHYSICAL_BYTES_DROPPED=$((CUMULATIVE_PHYSICAL_BYTES_DROPPED + physical))
    [[ "$(q "SELECT to_regclass('$rel') IS NULL;")" == t ]] || die "$scenario DROP did not remove $rel"
    wait_event_covered "$tracking_id" DROP "$before_drop" >/dev/null \
        || die "$scenario DROP commit was not captured and covered"
    start_ms="$(monotonic_ms)"
    restore_lsn_retry "$rel" "$target_lsn" || die "$scenario restore failed"
    end_ms="$(monotonic_ms)"
    restore_ms=$((end_ms - start_ms))
    [[ "$(q "SELECT to_regclass('$rel') IS NOT NULL;")" == t ]] || die "$scenario restored relation missing"
    [[ "$(fingerprint_of "$rel")" == "$expected_fp" ]] || die "$scenario row fingerprint mismatch"
    [[ "$(schema_signature "$rel")" == "$expected_schema" ]] || die "$scenario schema signature mismatch"
    [[ "$(owner_of "$rel")" == "$expected_owner" ]] || die "$scenario owner mismatch"
    [[ "$(acl_of "$rel")" == "$expected_acl" ]] || die "$scenario ACL mismatch"
    if [[ "$scenario" == medium_indexed ]]; then
        next_identity="$(q "BEGIN;
          INSERT INTO public.drop_medium(external_key,category,payload)
          VALUES ('post-restore-identity',1,'probe') RETURNING id;
          ROLLBACK;")"
        (( next_identity > rows )) \
            || die "$scenario identity sequence did not advance beyond recovered max (next=$next_identity rows=$rows)"
    fi
    wait_healthy "$rel" || die "$scenario successor coverage did not become healthy"
    DROP_PASSED=$((DROP_PASSED + 1))
    record_case local_delta "$scenario" "$iteration" "$rows" "$logical" "$physical" "$restore_ms"
}

force_archive() { q "SELECT pg_switch_wal();" >/dev/null; sleep 2; }
backup_info() {
    local field=$1
    pgbr info --output=json | jq -r --arg field "$field" '
      .[0].backup | sort_by(.timestamp.stop) | last
      | if $field == "label" then .label elif $field == "stop" then .lsn.stop else empty end'
}
write_helper_config() {
    jq -n \
        --arg profile drop_backup --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$PGBACKREST_CONFIG" --arg pg_bin_dir "$PG_BIN" \
        --arg repository_path "$REPO_DIR" --arg stanza "$STANZA" \
        --arg work_root "$WORK_ROOT" --arg socket_root "$HELPER_SOCKET_DIR" \
        --arg expire_lock "$EXPIRE_LOCK" --arg controller_host "$SOCKET_DIR" \
        --arg controller_database "$DB_NAME" --arg recovery_user "$(id -un)" \
        --arg proof_hmac_key_file "$PROOF_HMAC_KEY_FILE" \
        --argjson controller_port "$PRIMARY_PORT" --argjson recovery_port "$HELPER_PORT" \
        '{profile:$profile, pgbackrest_bin:$pgbackrest, pgbackrest_config:$pgbackrest_config,
          pg_bin_dir:$pg_bin_dir, cp_bin:"/usr/bin/cp", repository_path:$repository_path,
          repository_key:1, stanza:$stanza, work_root:$work_root, socket_root:$socket_root,
          recovery_port:$recovery_port, recovery_user:$recovery_user,
          snapshot_provider:"xfs_reflink", expire_lock_path:$expire_lock,
          max_work_bytes:2147483648, max_work_root_bytes:3221225472, min_free_bytes:1,
          artifact_ttl_seconds:86400, max_retained_artifacts:16,
          max_retained_artifact_bytes:2147483648, command_timeout_seconds:180,
          recovery_timeout_seconds:300, proof_hmac_key_file:$proof_hmac_key_file,
          controller:{host:$controller_host,port:$controller_port,database:$controller_database,user:$recovery_user}}' \
        > "$HELPER_CONFIG"
    chmod 600 "$HELPER_CONFIG"
}

"$PG_BIN/initdb" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust > "$LOG_DIR/initdb.log"
cat > "$PGBACKREST_CONFIG" <<EOF
[global]
repo1-path=$REPO_DIR
repo1-retention-full=99
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
start-fast=y
compress-type=none
archive-async=n
spool-path=$RUN_ROOT/spool
log-path=$LOG_DIR
log-level-console=info
log-level-file=detail

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF
chmod 600 "$PGBACKREST_CONFIG"

cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
wal_level = logical
archive_mode = on
archive_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --stanza=$STANZA archive-push %p'
archive_timeout = 1
max_wal_senders = 10
max_replication_slots = 10
shared_preload_libraries = 'pg_flashback'
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.target_databases = '$DB_NAME'
pg_flashback.worker_interval_ms = 25
pg_flashback.local_max_snapshot_bytes = 2GB
pg_flashback.local_max_restore_peak_bytes = 4GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 32MB
pg_flashback.allow_unaudited_restore = on
EOF

"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
pgbr stanza-create
q "CREATE EXTENSION pg_flashback;"
wait_capture_ready || die "capture worker not ready before first track"
q "CREATE ROLE drop_owner LOGIN; CREATE ROLE drop_reader LOGIN;"
write_helper_config
enforce_resource_bounds

# Case 1: repeatedly destroy and reconstruct the same tracked lifecycle. This
# catches post-restore OID/generation bugs that distinct one-shot tables miss.
log "starting repeated local DROP matrix: $REPEAT_COUNT iterations"
q "CREATE TABLE public.drop_repeat(id bigint PRIMARY KEY, cycle integer NOT NULL, payload text NOT NULL);"
q "ALTER TABLE public.drop_repeat OWNER TO drop_owner; GRANT SELECT ON public.drop_repeat TO drop_reader;"
q "INSERT INTO public.drop_repeat
   SELECT g,0,repeat(md5(g::text),4) FROM generate_series(1,$REPEAT_ROWS) g;"
q "SELECT flashback_track('public.drop_repeat');" >/dev/null
wait_healthy public.drop_repeat || die "drop_repeat initial coverage not healthy"
REPEAT_TRACKING_ID="$(tracking_id_of public.drop_repeat)"
for iteration in $(seq 1 "$REPEAT_COUNT"); do
    before="$(max_event_id "$REPEAT_TRACKING_ID")"
    q "UPDATE public.drop_repeat SET cycle=$iteration WHERE id=1;" >/dev/null
    target_lsn="$(wait_event_covered "$REPEAT_TRACKING_ID" UPDATE "$before")" \
        || die "repeat iteration $iteration target not covered"
    fp="$(fingerprint_of public.drop_repeat)"
    schema="$(schema_signature public.drop_repeat)"
    owner="$(owner_of public.drop_repeat)"
    acl="$(acl_of public.drop_repeat)"
    rows="$(row_count_of public.drop_repeat)"
    logical="$(logical_bytes_of public.drop_repeat)"
    physical="$(physical_bytes_of public.drop_repeat)"
    perform_local_drop_restore repeated_same_lifecycle "$iteration" public.drop_repeat \
        "$REPEAT_TRACKING_ID" "$target_lsn" "$fp" "$schema" "$owner" "$acl" \
        "$rows" "$logical" "$physical"
    if (( iteration % 10 == 0 )); then
        log "repeated DROP progress $iteration/$REPEAT_COUNT"
        enforce_resource_bounds
    fi
done

# A separately tracked writer must continue committing while a much larger
# relation is dropped and reconstructed.
q "CREATE TABLE public.drop_concurrent(id bigserial PRIMARY KEY, marker text NOT NULL);"
q "SELECT flashback_track('public.drop_concurrent');" >/dev/null
wait_healthy public.drop_concurrent || die "concurrent writer coverage not healthy"

log "building medium local DROP relation: $MEDIUM_ROWS rows"
q "CREATE TABLE public.drop_medium(
     id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     external_key text NOT NULL UNIQUE,
     category integer NOT NULL CHECK (category BETWEEN 0 AND 31),
     payload text NOT NULL,
     created_at timestamptz NOT NULL DEFAULT clock_timestamp());"
q "CREATE INDEX drop_medium_payload_prefix_idx ON public.drop_medium (payload);"
q "ALTER TABLE public.drop_medium OWNER TO drop_owner; GRANT SELECT ON public.drop_medium TO drop_reader;"
q "INSERT INTO public.drop_medium(external_key,category,payload)
   SELECT 'key-'||g, g%32,
          (SELECT string_agg(md5(g::text||':'||s::text),'' ORDER BY s)
           FROM generate_series(1,32) s)
   FROM generate_series(1,$MEDIUM_ROWS) g;"
q "SELECT flashback_track('public.drop_medium');" >/dev/null
wait_healthy public.drop_medium || die "drop_medium initial coverage not healthy"
MEDIUM_TRACKING_ID="$(tracking_id_of public.drop_medium)"
before="$(max_event_id "$MEDIUM_TRACKING_ID")"
q "UPDATE public.drop_medium SET category=(category+1)%32 WHERE id=1;" >/dev/null
target_lsn="$(wait_event_covered "$MEDIUM_TRACKING_ID" UPDATE "$before")" \
    || die "medium target not covered"
fp="$(fingerprint_of public.drop_medium)"
schema="$(schema_signature public.drop_medium)"
owner="$(owner_of public.drop_medium)"
acl="$(acl_of public.drop_medium)"
rows="$(row_count_of public.drop_medium)"
logical="$(logical_bytes_of public.drop_medium)"
physical="$(physical_bytes_of public.drop_medium)"
BG_WRITER_STOP="$RUN_ROOT/stop-concurrent-writer"
rm -f "$BG_WRITER_STOP"
(
    while [[ ! -e "$BG_WRITER_STOP" ]]; do
        "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" \
            -c "INSERT INTO public.drop_concurrent(marker) VALUES (clock_timestamp()::text);" >/dev/null
        sleep 0.05
    done
) &
BG_WRITER_PID=$!
concurrent_before="$(row_count_of public.drop_concurrent)"
perform_local_drop_restore medium_indexed 1 public.drop_medium "$MEDIUM_TRACKING_ID" \
    "$target_lsn" "$fp" "$schema" "$owner" "$acl" "$rows" "$logical" "$physical"
: > "$BG_WRITER_STOP"
wait "$BG_WRITER_PID"
BG_WRITER_PID=""
concurrent_after="$(row_count_of public.drop_concurrent)"
CONCURRENT_COMMITS=$((concurrent_after - concurrent_before))
(( CONCURRENT_COMMITS >= 5 )) || die "only $CONCURRENT_COMMITS concurrent commits during medium restore"
wait_healthy public.drop_concurrent || die "concurrent writer coverage not healthy after medium restore"
enforce_resource_bounds

# Quoted identifier plus out-of-line, poorly compressible TOAST payload.
log "building quoted/TOAST DROP relation: $TOAST_ROWS rows"
q 'CREATE TABLE public."Drop Weird"(id bigint PRIMARY KEY, blob text NOT NULL, meta jsonb NOT NULL);'
q "CREATE INDEX \"Drop Weird Bucket\" ON public.\"Drop Weird\" (id);"
q 'ALTER TABLE public."Drop Weird" OWNER TO drop_owner; GRANT SELECT ON public."Drop Weird" TO drop_reader;'
q "INSERT INTO public.\"Drop Weird\"
   SELECT g,
          (SELECT string_agg(md5(g::text||':'||s::text),'' ORDER BY s)
           FROM generate_series(1,128) s),
          jsonb_build_object('bucket',g%17,'marker','base')
   FROM generate_series(1,$TOAST_ROWS) g;"
q 'SELECT flashback_track('\''public."Drop Weird"'\'');' >/dev/null
wait_healthy 'public."Drop Weird"' || die "quoted/TOAST initial coverage not healthy"
TOAST_TRACKING_ID="$(tracking_id_of 'public."Drop Weird"')"
before="$(max_event_id "$TOAST_TRACKING_ID")"
q 'UPDATE public."Drop Weird" SET meta=meta||'\''{"marker":"target"}'\''::jsonb WHERE id=1;' >/dev/null
target_lsn="$(wait_event_covered "$TOAST_TRACKING_ID" UPDATE "$before")" \
    || die "quoted/TOAST target not covered"
fp="$(fingerprint_of 'public."Drop Weird"')"
schema="$(schema_signature 'public."Drop Weird"')"
owner="$(owner_of 'public."Drop Weird"')"
acl="$(acl_of 'public."Drop Weird"')"
rows="$(row_count_of 'public."Drop Weird"')"
logical="$(logical_bytes_of 'public."Drop Weird"')"
physical="$(physical_bytes_of 'public."Drop Weird"')"
perform_local_drop_restore quoted_toast 1 'public."Drop Weird"' "$TOAST_TRACKING_ID" \
    "$target_lsn" "$fp" "$schema" "$owner" "$acl" "$rows" "$logical" "$physical"
enforce_resource_bounds

# Backup profile: retained FULL + continuous WAL, real DROP, packaged helper,
# checksum-verified import and extension-side production shadow swap.
log "building backup DROP relation: $BACKUP_ROWS base + $BACKUP_WAL_ROWS WAL rows"
q "CREATE TABLE public.backup_drop_medium(
     id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL);"
q "CREATE INDEX backup_drop_medium_marker_idx ON public.backup_drop_medium(marker);"
q "ALTER TABLE public.backup_drop_medium OWNER TO drop_owner;
   GRANT SELECT ON public.backup_drop_medium TO drop_reader;"
q "INSERT INTO public.backup_drop_medium(marker,payload)
   SELECT 'base', decode((SELECT string_agg(md5(g::text||':'||s::text),'' ORDER BY s)
                          FROM generate_series(1,32) s),'hex')
   FROM generate_series(1,$BACKUP_ROWS) g;"
q "CHECKPOINT;"
pgbr backup --type=full --no-expire-auto >/dev/null
log "backup anchor $(backup_info label) stop=$(backup_info stop)"
q "SELECT flashback_track_backup('public.backup_drop_medium','drop_backup');" >/dev/null
for _ in $(seq 1 400); do
    BACKUP_TRACKING_ID="$(q "SELECT tracking_id FROM flashback.tracked_tables
                              WHERE table_name='backup_drop_medium' AND is_active;")"
    marker="$(q "SELECT details->>'tracking_marker_lsn' FROM flashback.coverage_generations
                  WHERE tracking_id=${BACKUP_TRACKING_ID:-0} AND recovery_profile='backup'
                    AND state='building' ORDER BY generation_no DESC LIMIT 1;" 2>/dev/null || true)"
    [[ -n "$BACKUP_TRACKING_ID" && -n "$marker" ]] && break
    q "SELECT flashback_consume_wal(8192);" >/dev/null || true
    sleep 0.05
done
[[ -n "${BACKUP_TRACKING_ID:-}" && -n "${marker:-}" ]] || die "backup tracking marker unresolved"
q "INSERT INTO public.backup_drop_medium(marker,payload)
   SELECT 'wal',decode(repeat(md5(('wal-'||g)::text),16),'hex')
   FROM generate_series(1,$BACKUP_WAL_ROWS) g;" >/dev/null
target_lsn="$(q "SELECT pg_current_wal_lsn()::text;")"
force_archive
verify_request="$RUN_ROOT/verify-backup.json"
jq -n --arg request_id drop-qualification-anchor --argjson tracking_id "$BACKUP_TRACKING_ID" \
    '{request_id:$request_id,tracking_id:$tracking_id}' > "$verify_request"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$verify_request" \
    > "$RUN_ROOT/verify-backup.result.json"
[[ "$(jq -r .status "$RUN_ROOT/verify-backup.result.json")" == verified ]] \
    || die "backup anchor verification failed"
fp="$(fingerprint_of public.backup_drop_medium)"
schema="$(schema_signature public.backup_drop_medium)"
owner="$(owner_of public.backup_drop_medium)"
acl="$(acl_of public.backup_drop_medium)"
rows="$(row_count_of public.backup_drop_medium)"
logical="$(logical_bytes_of public.backup_drop_medium)"
physical="$(physical_bytes_of public.backup_drop_medium)"
q "DROP TABLE public.backup_drop_medium;" >/dev/null
DROP_ATTEMPTED=$((DROP_ATTEMPTED + 1))
CUMULATIVE_ROWS_DROPPED=$((CUMULATIVE_ROWS_DROPPED + rows))
CUMULATIVE_LOGICAL_BYTES_DROPPED=$((CUMULATIVE_LOGICAL_BYTES_DROPPED + logical))
CUMULATIVE_PHYSICAL_BYTES_DROPPED=$((CUMULATIVE_PHYSICAL_BYTES_DROPPED + physical))
[[ "$(q "SELECT to_regclass('public.backup_drop_medium') IS NULL;")" == t ]] \
    || die "backup DROP did not remove relation"
force_archive
controller_start="$(monotonic_ms)"
PGHOST="$SOCKET_DIR" PGPORT="$PRIMARY_PORT" PGUSER="$(id -un)" \
    "$CONTROLLER" --config "$HELPER_CONFIG" --dbname "$DB_NAME" \
      --table public.backup_drop_medium --target-lsn "$target_lsn" --helper "$HELPER" \
      > "$RUN_ROOT/controller-backup-drop.result.json"
controller_end="$(monotonic_ms)"
controller_ms=$((controller_end - controller_start))
[[ "$(jq -r .status "$RUN_ROOT/controller-backup-drop.result.json")" == completed ]] \
    || die "backup DROP controller did not complete"
request_id="$(jq -r .request_id "$RUN_ROOT/controller-backup-drop.result.json")"
helper_result="$WORK_ROOT/$request_id/result.json"
[[ -f "$helper_result" ]] || die "backup DROP helper result missing"
helper_durations="$(jq -c '.durations' "$helper_result")"
[[ "$(fingerprint_of public.backup_drop_medium)" == "$fp" ]] || die "backup DROP row fingerprint mismatch"
[[ "$(schema_signature public.backup_drop_medium)" == "$schema" ]] || die "backup DROP schema mismatch"
[[ "$(owner_of public.backup_drop_medium)" == "$owner" ]] || die "backup DROP owner mismatch"
[[ "$(acl_of public.backup_drop_medium)" == "$acl" ]] || die "backup DROP ACL mismatch"
for _ in $(seq 1 400); do
    post_health="$(q "SELECT health FROM flashback_health()
                      WHERE table_name='public.backup_drop_medium';")"
    [[ "$post_health" == backup_reanchor_required ]] && break
    q "SELECT flashback_consume_wal(8192);" >/dev/null || true
    sleep 0.05
done
[[ "${post_health:-}" == backup_reanchor_required ]] \
    || die "backup DROP post-swap health was ${post_health:-missing}, expected backup_reanchor_required"
DROP_PASSED=$((DROP_PASSED + 1))
record_case backup retained_full_plus_wal 1 "$rows" "$logical" "$physical" "$controller_ms" "$helper_durations"
enforce_resource_bounds

[[ "$DROP_PASSED" == "$DROP_ATTEMPTED" ]] || die "only $DROP_PASSED/$DROP_ATTEMPTED DROP cases passed"
[[ "$DROP_PASSED" -ge $((REPEAT_COUNT + 3)) ]] \
    || die "DROP count $DROP_PASSED below expected $((REPEAT_COUNT + 3))"
exact_candidate_verify_installed || die "candidate binary hash mismatch at end"
RUN_COMPLETE=1
log "DROP qualification passed: $DROP_PASSED destructive DROP-to-restore cases"
exit 0

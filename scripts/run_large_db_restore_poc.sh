#!/usr/bin/env bash
# Compare classic pgBackRest restore with an XFS reflink clone of a
# plain-format pgBackRest backup. All PostgreSQL clusters, ports, sockets and
# repository paths are isolated under target/large-db-poc.

set -Eeuo pipefail

SIZE_MB="${1:-64}"
TARGET_PERCENT="${PGFB_POC_TARGET_PERCENT:-20}"
CHURN_PERCENT="${PGFB_POC_CHURN_PERCENT:-0}"
KEEP="${PGFB_POC_KEEP:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
POC_BASE="${PGFB_POC_BASE:-$REPO_ROOT/target/large-db-poc}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SIZE_MB}mb-$$"
RUN_ROOT="$POC_BASE/runs/$RUN_ID"
RESULT_DIR="$POC_BASE/results"

PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
PGBACKREST="${PGFB_POC_PGBACKREST:-/usr/local/bin/pgbackrest}"
STANZA="large_db_poc"
DB_NAME="pocdb"
PRIMARY_PORT="${PGFB_POC_PRIMARY_PORT:-28917}"
CLASSIC_PORT="${PGFB_POC_CLASSIC_PORT:-28918}"
SNAPSHOT_PORT="${PGFB_POC_SNAPSHOT_PORT:-28919}"

PRIMARY_DIR="$RUN_ROOT/primary"
CLASSIC_DIR="$RUN_ROOT/classic"
SNAPSHOT_DIR="$RUN_ROOT/snapshot"
REPO_DIR="$RUN_ROOT/repo"
# Unix socket paths are limited to roughly 107 bytes on Linux. RUN_ROOT is
# intentionally descriptive and can exceed that once `.s.PGSQL.<port>` is
# appended, so sockets use a short, unique directory under /tmp.
SOCKET_DIR="${PGFB_POC_SOCKET_DIR:-/tmp/pgfb-poc-$RUN_ID}"
SPOOL_DIR="$RUN_ROOT/spool"
LOG_DIR="$RUN_ROOT/log"
DUMP_DIR="$RUN_ROOT/dump"
PGBACKREST_CONFIG="$RUN_ROOT/pgbackrest.conf"
RESULT_JSON="$RESULT_DIR/$RUN_ID.json"

INITDB="$PG_BIN/initdb"
PG_CTL="$PG_BIN/pg_ctl"
PSQL="$PG_BIN/psql"
PG_DUMP="$PG_BIN/pg_dump"

PRIMARY_STARTED=0
CLASSIC_STARTED=0
SNAPSHOT_STARTED=0
RUN_COMPLETE=0

log() {
    printf '[large-db-poc] %s %s\n' "$(date +%H:%M:%S)" "$*"
}

die() {
    log "ERROR: $*"
    exit 1
}

require_executable() {
    [[ -x "$1" ]] || die "required executable not found: $1"
}

now_ns() {
    date +%s%N
}

elapsed_ms() {
    printf '%s\n' "$(( ($2 - $1) / 1000000 ))"
}

fs_used_bytes() {
    df -B1 --output=used "$RUN_ROOT" | awk 'NR == 2 {gsub(/[[:space:]]/, "", $1); print $1}'
}

dir_apparent_bytes() {
    du -sb "$1" | awk '{print $1}'
}

stop_cluster() {
    local dir="$1"
    if [[ -s "$dir/postmaster.pid" ]]; then
        "$PG_CTL" -D "$dir" stop -m fast -w -t 60 > /dev/null 2>&1 || true
    fi
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    stop_cluster "$SNAPSHOT_DIR"
    stop_cluster "$CLASSIC_DIR"
    stop_cluster "$PRIMARY_DIR"
    rmdir "$SOCKET_DIR" > /dev/null 2>&1 || true

    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" ]]; then
        rm -rf -- "$RUN_ROOT"
        log "bulky run data removed; result kept at $RESULT_JSON"
    elif [[ "$KEEP" == "1" || "$rc" != "0" ]]; then
        log "run artifacts kept at $RUN_ROOT"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

pgbr() {
    "$PGBACKREST" --config="$PGBACKREST_CONFIG" --stanza="$STANZA" "$@"
}

primary_sql() {
    "$PSQL" -X -v ON_ERROR_STOP=1 -qAt \
        -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" -c "$1"
}

recovery_sql() {
    local port="$1"
    local sql="$2"
    "$PSQL" -X -v ON_ERROR_STOP=1 -qAt \
        -h "$SOCKET_DIR" -p "$port" -d "$DB_NAME" -c "$sql"
}

load_rows() {
    local table="$1"
    local remaining="$2"
    local batch=100000
    local take
    while (( remaining > 0 )); do
        take=$batch
        (( remaining < batch )) && take=$remaining
        # 57 md5 hex blocks decode to 912 bytes. The value stays below the
        # per-row TOAST threshold, so PostgreSQL stores the payload inline
        # instead of making this a compression benchmark.
        primary_sql "INSERT INTO $table(marker, payload) SELECT 'base', decode(repeat(md5(random()::text || g::text), 57), 'hex') FROM generate_series(1, $take) AS g;" > /dev/null
        remaining=$((remaining - take))
        log "$table: $remaining rows remaining"
    done
}

churn_noise_rows() {
    local total="$1"
    local first=1
    local batch=100000
    local last
    while (( first <= total )); do
        last=$((first + batch - 1))
        (( last > total )) && last=$total
        primary_sql "UPDATE noise_table SET marker='churned', payload=decode(repeat(md5(random()::text || id::text), 57), 'hex') WHERE id BETWEEN $first AND $last;" > /dev/null
        log "noise churn: $last / $total rows committed"
        first=$((last + 1))
    done
}

wait_for_promotion() {
    local port="$1"
    local name="$2"
    local value=""
    for _ in $(seq 1 600); do
        value=$(recovery_sql "$port" "SELECT pg_is_in_recovery();" 2>/dev/null || true)
        if [[ "$value" == "f" ]]; then
            return 0
        fi
        sleep 0.2
    done
    die "$name did not promote within 120 seconds"
}

fingerprint() {
    local port="$1"
    recovery_sql "$port" "
        SELECT count(*)::text || '|' ||
               COALESCE(sum(id), 0)::text || '|' ||
               count(*) FILTER (WHERE marker = 'sentinel')::text || '|' ||
               COALESCE(bit_xor(hashtextextended(encode(payload, 'hex'), 0)), 0)::text
        FROM public.target_table;"
}

primary_fingerprint() {
    primary_sql "
        SELECT count(*)::text || '|' ||
               COALESCE(sum(id), 0)::text || '|' ||
               count(*) FILTER (WHERE marker = 'sentinel')::text || '|' ||
               COALESCE(bit_xor(hashtextextended(encode(payload, 'hex'), 0)), 0)::text
        FROM public.target_table;"
}

start_recovery_cluster() {
    local dir="$1"
    local port="$2"
    local log_file="$3"
    "$PG_CTL" -D "$dir" -l "$log_file" \
        -o "-p $port -k $SOCKET_DIR -c archive_mode=off" \
        start -w -t 60 > /dev/null
}

write_result() {
    jq -n \
        --arg run_id "$RUN_ID" \
        --arg status "ok" \
        --arg pg_version "$($PG_BIN/postgres --version)" \
        --arg pgbackrest_version "$($PGBACKREST version)" \
        --arg backup_label "$BACKUP_LABEL" \
        --arg target_time "$TARGET_TIME" \
        --arg expected_fingerprint "$EXPECTED_FINGERPRINT" \
        --arg classic_fingerprint "$CLASSIC_FINGERPRINT" \
        --arg snapshot_fingerprint "$SNAPSHOT_FINGERPRINT" \
        --argjson requested_size_mb "$SIZE_MB" \
        --argjson target_percent "$TARGET_PERCENT" \
        --argjson churn_percent "$CHURN_PERCENT" \
        --argjson churn_rows "$CHURN_ROWS" \
        --argjson database_bytes "$DATABASE_BYTES" \
        --argjson target_table_bytes "$TARGET_TABLE_BYTES" \
        --argjson backup_repo_bytes "$BACKUP_REPO_BYTES" \
        --argjson final_repo_bytes "$FINAL_REPO_BYTES" \
        --argjson backup_ms "$BACKUP_MS" \
        --argjson classic_restore_ms "$CLASSIC_RESTORE_MS" \
        --argjson classic_recovery_ms "$CLASSIC_RECOVERY_MS" \
        --argjson classic_extract_ms "$CLASSIC_EXTRACT_MS" \
        --argjson classic_total_rto_ms "$CLASSIC_TOTAL_RTO_MS" \
        --argjson classic_allocated_bytes "$CLASSIC_ALLOCATED_BYTES" \
        --argjson classic_recovery_allocated_bytes "$CLASSIC_RECOVERY_ALLOCATED_BYTES" \
        --argjson classic_total_allocated_bytes "$CLASSIC_TOTAL_ALLOCATED_BYTES" \
        --argjson classic_dump_bytes "$CLASSIC_DUMP_BYTES" \
        --argjson snapshot_clone_ms "$SNAPSHOT_CLONE_MS" \
        --argjson snapshot_recovery_ms "$SNAPSHOT_RECOVERY_MS" \
        --argjson snapshot_extract_ms "$SNAPSHOT_EXTRACT_MS" \
        --argjson snapshot_total_rto_ms "$SNAPSHOT_TOTAL_RTO_MS" \
        --argjson snapshot_clone_allocated_bytes "$SNAPSHOT_CLONE_ALLOCATED_BYTES" \
        --argjson snapshot_recovery_allocated_bytes "$SNAPSHOT_RECOVERY_ALLOCATED_BYTES" \
        --argjson snapshot_total_allocated_bytes "$SNAPSHOT_TOTAL_ALLOCATED_BYTES" \
        --argjson snapshot_dump_bytes "$SNAPSHOT_DUMP_BYTES" \
        '{
            run_id: $run_id,
            status: $status,
            requested_size_mb: $requested_size_mb,
            target_percent: $target_percent,
            churn_percent: $churn_percent,
            churn_rows: $churn_rows,
            versions: {postgres: $pg_version, pgbackrest: $pgbackrest_version},
            backup: {
                label: $backup_label,
                target_time: $target_time,
                duration_ms: $backup_ms,
                repository_bytes_after_backup: $backup_repo_bytes,
                repository_bytes_after_wal: $final_repo_bytes
            },
            dataset: {database_bytes: $database_bytes, target_table_bytes: $target_table_bytes},
            correctness: {expected: $expected_fingerprint, classic: $classic_fingerprint, snapshot: $snapshot_fingerprint},
            classic: {
                restore_ms: $classic_restore_ms,
                recovery_ms: $classic_recovery_ms,
                extract_ms: $classic_extract_ms,
                total_rto_ms: $classic_total_rto_ms,
                restore_allocated_bytes: $classic_allocated_bytes,
                recovery_allocated_bytes: $classic_recovery_allocated_bytes,
                total_allocated_bytes: $classic_total_allocated_bytes,
                dump_bytes: $classic_dump_bytes
            },
            snapshot_direct: {
                clone_ms: $snapshot_clone_ms,
                recovery_ms: $snapshot_recovery_ms,
                extract_ms: $snapshot_extract_ms,
                total_rto_ms: $snapshot_total_rto_ms,
                clone_allocated_bytes: $snapshot_clone_allocated_bytes,
                recovery_allocated_bytes: $snapshot_recovery_allocated_bytes,
                total_allocated_bytes: $snapshot_total_allocated_bytes,
                dump_bytes: $snapshot_dump_bytes
            }
        }' > "$RESULT_JSON"
}

[[ "$SIZE_MB" =~ ^[0-9]+$ ]] || die "size must be an integer MiB value"
(( SIZE_MB >= 32 )) || die "size must be at least 32 MiB"
[[ "$TARGET_PERCENT" =~ ^[0-9]+$ ]] || die "target percent must be an integer"
(( TARGET_PERCENT >= 1 && TARGET_PERCENT <= 90 )) || die "target percent must be between 1 and 90"
[[ "$CHURN_PERCENT" =~ ^[0-9]+$ ]] || die "churn percent must be an integer"
(( CHURN_PERCENT >= 0 && CHURN_PERCENT <= 100 )) || die "churn percent must be between 0 and 100"

require_executable "$INITDB"
require_executable "$PG_CTL"
require_executable "$PSQL"
require_executable "$PG_DUMP"
require_executable "$PGBACKREST"
require_executable "$(command -v jq)"
require_executable "$(command -v cp)"

[[ "$RUN_ROOT" != *"'"* && "$RUN_ROOT" != *$'\n'* ]] || die "PoC path may not contain quotes or newlines"

mkdir -p "$RUN_ROOT" "$RESULT_DIR" "$REPO_DIR" "$SOCKET_DIR" "$SPOOL_DIR" "$LOG_DIR" "$DUMP_DIR"
chmod 700 "$SOCKET_DIR"

log "run=$RUN_ID requested_size=${SIZE_MB}MiB target_share=${TARGET_PERCENT}% churn=${CHURN_PERCENT}%"

"$INITDB" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust > "$LOG_DIR/initdb.log"

cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
listen_addresses = ''
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
wal_level = replica
archive_mode = on
archive_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --stanza=$STANZA archive-push %p'
archive_timeout = '2s'
full_page_writes = on
fsync = on
synchronous_commit = on
shared_buffers = '128MB'
max_wal_size = '2GB'
log_min_messages = warning
EOF

cat > "$PGBACKREST_CONFIG" <<EOF
[global]
repo1-path=$REPO_DIR
repo1-retention-full=1
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
compress-type=none
archive-async=n
spool-path=$SPOOL_DIR
log-path=$LOG_DIR
log-level-file=detail
log-level-console=warn
start-fast=y
process-max=2

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF

log "starting isolated primary with archiving disabled during data load"
"$PG_CTL" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" \
    -o "-c archive_mode=off" start -w -t 60 > /dev/null
PRIMARY_STARTED=1

"$PSQL" -X -v ON_ERROR_STOP=1 -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d postgres \
    -c "CREATE DATABASE $DB_NAME;"
primary_sql "CREATE TABLE target_table(id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL); CREATE TABLE noise_table(id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL);" > /dev/null

TOTAL_ROWS=$(( SIZE_MB * 1024 * 1024 / 1000 ))
TARGET_ROWS=$(( TOTAL_ROWS * TARGET_PERCENT / 100 ))
NOISE_ROWS=$(( TOTAL_ROWS - TARGET_ROWS ))
(( TARGET_ROWS > 1000 )) || TARGET_ROWS=1001
(( NOISE_ROWS > 1000 )) || NOISE_ROWS=1001

load_rows target_table "$TARGET_ROWS"
load_rows noise_table "$NOISE_ROWS"
primary_sql "VACUUM (ANALYZE) target_table;" > /dev/null
primary_sql "VACUUM (ANALYZE) noise_table;" > /dev/null
primary_sql "CHECKPOINT;" > /dev/null

DATABASE_BYTES=$(primary_sql "SELECT pg_database_size(current_database());")
TARGET_TABLE_BYTES=$(primary_sql "SELECT pg_total_relation_size('public.target_table');")
log "actual database=$(numfmt --to=iec-i --suffix=B "$DATABASE_BYTES") target_table=$(numfmt --to=iec-i --suffix=B "$TARGET_TABLE_BYTES")"

stop_cluster "$PRIMARY_DIR"
PRIMARY_STARTED=0

log "restarting primary with WAL archiving enabled"
"$PG_CTL" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 > /dev/null
PRIMARY_STARTED=1
pgbr stanza-create
pgbr check

log "taking plain-format full backup"
t0=$(now_ns)
pgbr backup --type=full
t1=$(now_ns)
BACKUP_MS=$(elapsed_ms "$t0" "$t1")
BACKUP_LABEL=$(pgbr info --output=json | jq -r '.[0].backup | sort_by(.timestamp.stop) | last.label')
[[ -n "$BACKUP_LABEL" && "$BACKUP_LABEL" != "null" ]] || die "could not determine backup label"

BACKUP_DATA_DIR="$REPO_DIR/backup/$STANZA/$BACKUP_LABEL/pg_data"
[[ -d "$BACKUP_DATA_DIR" ]] || die "plain backup data directory not found: $BACKUP_DATA_DIR"
[[ -f "$BACKUP_DATA_DIR/PG_VERSION" ]] || die "backup is not a directly startable pg_data tree"
BACKUP_REPO_BYTES=$(dir_apparent_bytes "$REPO_DIR")

log "creating post-backup state and DROP timeline"
CHURN_ROWS=$(( NOISE_ROWS * CHURN_PERCENT / 100 ))
if (( CHURN_ROWS > 0 )); then
    log "generating unrelated post-backup WAL by updating $CHURN_ROWS noise rows"
    churn_noise_rows "$CHURN_ROWS"
fi
UPDATE_LIMIT=$TARGET_ROWS
(( UPDATE_LIMIT > 1000 )) && UPDATE_LIMIT=1000
primary_sql "UPDATE target_table SET marker='updated', payload=decode(repeat(md5(random()::text || id::text), 57), 'hex') WHERE id <= $UPDATE_LIMIT;" > /dev/null
primary_sql "INSERT INTO target_table(marker, payload) VALUES ('sentinel', decode(repeat(md5(random()::text), 57), 'hex'));" > /dev/null
TARGET_TIME=$(primary_sql "SELECT clock_timestamp();")
EXPECTED_FINGERPRINT=$(primary_fingerprint)
sleep 2
primary_sql "DROP TABLE target_table; SELECT pg_switch_wal();" > /dev/null
sleep 3
pgbr check

stop_cluster "$PRIMARY_DIR"
PRIMARY_STARTED=0
FINAL_REPO_BYTES=$(dir_apparent_bytes "$REPO_DIR")

log "classic path: full pgBackRest restore"
classic_fs_before=$(fs_used_bytes)
t0=$(now_ns)
pgbr --pg1-path="$CLASSIC_DIR" --set="$BACKUP_LABEL" \
    --type=time --target="$TARGET_TIME" --target-action=promote restore
t1=$(now_ns)
CLASSIC_RESTORE_MS=$(elapsed_ms "$t0" "$t1")
sync
classic_fs_after_restore=$(fs_used_bytes)
CLASSIC_ALLOCATED_BYTES=$((classic_fs_after_restore - classic_fs_before))
(( CLASSIC_ALLOCATED_BYTES >= 0 )) || CLASSIC_ALLOCATED_BYTES=0

t0=$(now_ns)
start_recovery_cluster "$CLASSIC_DIR" "$CLASSIC_PORT" "$LOG_DIR/classic.log"
CLASSIC_STARTED=1
wait_for_promotion "$CLASSIC_PORT" "classic restore"
t1=$(now_ns)
CLASSIC_RECOVERY_MS=$(elapsed_ms "$t0" "$t1")
sync
classic_fs_after_recovery=$(fs_used_bytes)
CLASSIC_RECOVERY_ALLOCATED_BYTES=$((classic_fs_after_recovery - classic_fs_after_restore))
(( CLASSIC_RECOVERY_ALLOCATED_BYTES >= 0 )) || CLASSIC_RECOVERY_ALLOCATED_BYTES=0
CLASSIC_TOTAL_ALLOCATED_BYTES=$((CLASSIC_ALLOCATED_BYTES + CLASSIC_RECOVERY_ALLOCATED_BYTES))

CLASSIC_FINGERPRINT=$(fingerprint "$CLASSIC_PORT")
[[ "$CLASSIC_FINGERPRINT" == "$EXPECTED_FINGERPRINT" ]] || die "classic fingerprint mismatch: expected=$EXPECTED_FINGERPRINT actual=$CLASSIC_FINGERPRINT"

t0=$(now_ns)
"$PG_DUMP" -Fc --no-owner --no-acl -h "$SOCKET_DIR" -p "$CLASSIC_PORT" -d "$DB_NAME" \
    -t public.target_table -f "$DUMP_DIR/classic.dump"
t1=$(now_ns)
CLASSIC_EXTRACT_MS=$(elapsed_ms "$t0" "$t1")
CLASSIC_DUMP_BYTES=$(stat -c %s "$DUMP_DIR/classic.dump")
CLASSIC_TOTAL_RTO_MS=$((CLASSIC_RESTORE_MS + CLASSIC_RECOVERY_MS + CLASSIC_EXTRACT_MS))
stop_cluster "$CLASSIC_DIR"
CLASSIC_STARTED=0

log "snapshot path: XFS reflink clone of repository backup"
snapshot_fs_before=$(fs_used_bytes)
t0=$(now_ns)
mkdir -p "$SNAPSHOT_DIR"
cp -a --reflink=always "$BACKUP_DATA_DIR/." "$SNAPSHOT_DIR/"
chmod 700 "$SNAPSHOT_DIR"
rm -f "$SNAPSHOT_DIR/postmaster.pid" "$SNAPSHOT_DIR/standby.signal"
touch "$SNAPSHOT_DIR/recovery.signal"
cat >> "$SNAPSHOT_DIR/postgresql.auto.conf" <<EOF
restore_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --pg1-path=$SNAPSHOT_DIR --stanza=$STANZA archive-get %f "%p"'
recovery_target_time = '$TARGET_TIME'
recovery_target_action = 'promote'
EOF
t1=$(now_ns)
SNAPSHOT_CLONE_MS=$(elapsed_ms "$t0" "$t1")
sync
snapshot_fs_after_clone=$(fs_used_bytes)
SNAPSHOT_CLONE_ALLOCATED_BYTES=$((snapshot_fs_after_clone - snapshot_fs_before))
(( SNAPSHOT_CLONE_ALLOCATED_BYTES >= 0 )) || SNAPSHOT_CLONE_ALLOCATED_BYTES=0

t0=$(now_ns)
start_recovery_cluster "$SNAPSHOT_DIR" "$SNAPSHOT_PORT" "$LOG_DIR/snapshot.log"
SNAPSHOT_STARTED=1
wait_for_promotion "$SNAPSHOT_PORT" "snapshot-direct restore"
t1=$(now_ns)
SNAPSHOT_RECOVERY_MS=$(elapsed_ms "$t0" "$t1")
sync
snapshot_fs_after_recovery=$(fs_used_bytes)
SNAPSHOT_RECOVERY_ALLOCATED_BYTES=$((snapshot_fs_after_recovery - snapshot_fs_after_clone))
(( SNAPSHOT_RECOVERY_ALLOCATED_BYTES >= 0 )) || SNAPSHOT_RECOVERY_ALLOCATED_BYTES=0
SNAPSHOT_TOTAL_ALLOCATED_BYTES=$((SNAPSHOT_CLONE_ALLOCATED_BYTES + SNAPSHOT_RECOVERY_ALLOCATED_BYTES))

SNAPSHOT_FINGERPRINT=$(fingerprint "$SNAPSHOT_PORT")
[[ "$SNAPSHOT_FINGERPRINT" == "$EXPECTED_FINGERPRINT" ]] || die "snapshot fingerprint mismatch: expected=$EXPECTED_FINGERPRINT actual=$SNAPSHOT_FINGERPRINT"

t0=$(now_ns)
"$PG_DUMP" -Fc --no-owner --no-acl -h "$SOCKET_DIR" -p "$SNAPSHOT_PORT" -d "$DB_NAME" \
    -t public.target_table -f "$DUMP_DIR/snapshot.dump"
t1=$(now_ns)
SNAPSHOT_EXTRACT_MS=$(elapsed_ms "$t0" "$t1")
SNAPSHOT_DUMP_BYTES=$(stat -c %s "$DUMP_DIR/snapshot.dump")
SNAPSHOT_TOTAL_RTO_MS=$((SNAPSHOT_CLONE_MS + SNAPSHOT_RECOVERY_MS + SNAPSHOT_EXTRACT_MS))

write_result
RUN_COMPLETE=1

log "classic total RTO: ${CLASSIC_TOTAL_RTO_MS}ms (restore=$CLASSIC_RESTORE_MS recovery=$CLASSIC_RECOVERY_MS extract=$CLASSIC_EXTRACT_MS)"
log "snapshot total RTO: ${SNAPSHOT_TOTAL_RTO_MS}ms (clone=$SNAPSHOT_CLONE_MS recovery=$SNAPSHOT_RECOVERY_MS extract=$SNAPSHOT_EXTRACT_MS)"
log "classic total allocated: $(numfmt --to=iec-i --suffix=B "$CLASSIC_TOTAL_ALLOCATED_BYTES")"
log "snapshot total allocated: $(numfmt --to=iec-i --suffix=B "$SNAPSHOT_TOTAL_ALLOCATED_BYTES") (clone=$(numfmt --to=iec-i --suffix=B "$SNAPSHOT_CLONE_ALLOCATED_BYTES"))"
log "result: $RESULT_JSON"

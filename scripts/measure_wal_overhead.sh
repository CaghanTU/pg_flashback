#!/usr/bin/env bash
# Compare a small update workload with default, RI FULL, and tracked capture.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
# shellcheck source=scripts/qualification_provenance.sh
source "$ROOT/scripts/qualification_provenance.sh"
qualification_provenance_init "$ROOT" "$PG_CONFIG"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_WAL_OVERHEAD_PORT:-28957}"
ROWS="${PG_FLASHBACK_WAL_OVERHEAD_ROWS:-500}"
WORK_ROOT="${PG_FLASHBACK_WAL_OVERHEAD_WORK_ROOT:-$ROOT/target/wal-overhead/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-wal-overhead-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_WAL_OVERHEAD_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/wal-overhead-$RUN_ID.json"
DB=postgres
CLEANUP_OK=false

require_file() {
    [[ -f "$1" ]] || { echo "FAIL: required file not found: $1" >&2; exit 2; }
}

cleanup() {
    local rc=$?
    set +e
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    if ! "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
        rm -rf "$DATA" "$SOCKET"
        [[ ! -e "$DATA" && ! -e "$SOCKET" ]] && CLEANUP_OK=true
    fi
    [[ "$CLEANUP_OK" == true ]] || rc=1
    exit "$rc"
}
trap cleanup EXIT

require_file "$PG_CONFIG"
require_file "$SHARE_DIR/extension/pg_flashback.control"
[[ "$ROWS" =~ ^[0-9]+$ && "$ROWS" -gt 0 ]] || {
    echo "FAIL: PG_FLASHBACK_WAL_OVERHEAD_ROWS must be a positive integer" >&2; exit 2; }
mkdir -p "$WORK_ROOT" "$SOCKET" "$RESULT_DIR"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" \
    -o "-p $PORT -k $SOCKET" start -w >/dev/null
q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }

q "CREATE EXTENSION pg_flashback;
   CREATE TABLE public.wal_default(id integer PRIMARY KEY, payload text NOT NULL);
   CREATE TABLE public.wal_full(LIKE public.wal_default INCLUDING ALL);
   CREATE TABLE public.wal_tracked(LIKE public.wal_default INCLUDING ALL);
   INSERT INTO public.wal_default SELECT g, repeat(md5(g::text), 128) FROM generate_series(1, $ROWS) AS g;
   INSERT INTO public.wal_full SELECT * FROM public.wal_default;
   INSERT INTO public.wal_tracked SELECT * FROM public.wal_default;
   ALTER TABLE public.wal_full REPLICA IDENTITY FULL;"
q "SELECT flashback_track('public.wal_tracked');" >/dev/null
for _ in $(seq 1 200); do
    [[ "$(q "SELECT health FROM flashback_health()
             WHERE table_name = 'public.wal_tracked';")" == healthy ]] && break
    sleep 0.1
done
[[ "$(q "SELECT health FROM flashback_health()
         WHERE table_name = 'public.wal_tracked';")" == healthy ]] || {
    echo "FAIL: tracked coverage did not become healthy" >&2; exit 1; }

measure_update() {
    local table=$1 start end started elapsed
    q "CHECKPOINT;" >/dev/null
    start="$(q "SELECT pg_current_wal_insert_lsn();")"
    started="$(date +%s%N)"
    q "UPDATE public.$table SET payload = reverse(payload);" >/dev/null
    elapsed=$(( ($(date +%s%N) - started) / 1000000 ))
    end="$(q "SELECT pg_current_wal_insert_lsn();")"
    printf '%s|%s\n' "$(q "SELECT pg_wal_lsn_diff('$end', '$start')::bigint;")" "$elapsed"
}

IFS='|' read -r DEFAULT_WAL DEFAULT_MS <<<"$(measure_update wal_default)"
IFS='|' read -r FULL_WAL FULL_MS <<<"$(measure_update wal_full)"
IFS='|' read -r TRACKED_WAL TRACKED_MS <<<"$(measure_update wal_tracked)"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE rel_oid = 'public.wal_tracked'::regclass AND event_type='UPDATE';")" == "$ROWS" ]] && break
    sleep 0.1
done
CAPTURED="$(q "SELECT count(*) FROM flashback.delta_log
               WHERE rel_oid = 'public.wal_tracked'::regclass AND event_type='UPDATE';")"
[[ "$CAPTURED" == "$ROWS" ]] || { echo "FAIL: tracked updates were not fully captured" >&2; exit 1; }

FULL_RATIO="$(awk -v n="$FULL_WAL" -v d="$DEFAULT_WAL" 'BEGIN {printf "%.3f", d ? n/d : 0}')"
TRACKED_RATIO="$(awk -v n="$TRACKED_WAL" -v d="$DEFAULT_WAL" 'BEGIN {printf "%.3f", d ? n/d : 0}')"
cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
$(qualification_provenance_json "$(date -u +%Y-%m-%dT%H:%M:%SZ)"),
  "config": {"worker_interval_ms": 25, "rows": $ROWS},
  "status": "PASS",
  "rows": $ROWS,
  "measurements": {
    "default_replica_identity": {"wal_bytes": $DEFAULT_WAL, "rough_txn_latency_ms": $DEFAULT_MS},
    "replica_identity_full": {"wal_bytes": $FULL_WAL, "rough_txn_latency_ms": $FULL_MS, "wal_ratio_to_default": $FULL_RATIO},
    "tracked_local_delta": {"wal_bytes": $TRACKED_WAL, "rough_txn_latency_ms": $TRACKED_MS, "wal_ratio_to_default": $TRACKED_RATIO, "captured_update_rows": $CAPTURED}
  },
  "limitations": "Single-node development sample. WAL includes checkpoint/full-page-image effects and does not establish production throughput or retention capacity."
}
EOF
echo "WAL overhead measurement: PASS ($RESULT_JSON)"

#!/usr/bin/env bash
# Exact-candidate WAL overhead measurement (Phase 9).
# Requires CANDIDATE_DIR; never cargo-builds in claim runs.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required for claim runs}"
ROWS="${PG_FLASHBACK_WAL_OVERHEAD_ROWS:-500}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK_ROOT="${PG_FLASHBACK_WAL_OVERHEAD_WORK_ROOT:-$ROOT/target/wal-overhead/$RUN_ID}"
RESULT_DIR="${PG_FLASHBACK_WAL_OVERHEAD_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/exact-wal-overhead-$RUN_ID.json"
mkdir -p "$WORK_ROOT" "$RESULT_DIR"

log() { printf '[exact-wal-overhead] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

exact_candidate_bind_dir "$CANDIDATE_DIR" || die "bind failed"
IDENTITY="$(exact_candidate_identity_json)"
PG_BIN="${PG_BIN:-/usr/local/pgsql-${EC_PG_MAJOR}/bin}"
exact_candidate_install_into_prefix || die "install failed"
trap 'exact_candidate_restore_prefix || true; cleanup || true' EXIT

DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-walov-$RUN_ID"
mkdir -p "$SOCKET"
cleanup() {
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
}

"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/output_plugin_allowlist.sh"
opal_configure_postgresql_conf "$PG_BIN" "$DATA"
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = 28957
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF

"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/pg.log" start -w
PSQL=("$PG_BIN/psql" -h "$SOCKET" -p 28957 -d postgres -v ON_ERROR_STOP=on -qAt)
"${PSQL[@]}" -c "CREATE EXTENSION pg_flashback;"

wal_bytes() {
    "${PSQL[@]}" -c "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0');"
}

run_workload() {
    local label=$1
    "${PSQL[@]}" <<SQL
DROP TABLE IF EXISTS public.wal_ov CASCADE;
CREATE TABLE public.wal_ov (id int PRIMARY KEY, v text);
INSERT INTO public.wal_ov SELECT g, 'seed' FROM generate_series(1,$ROWS) g;
SQL
    local before after delta h
    case "$label" in
        default) ;;
        ri_full) "${PSQL[@]}" -c "ALTER TABLE public.wal_ov REPLICA IDENTITY FULL;" ;;
        tracked)
            "${PSQL[@]}" -c "SELECT flashback_track('public.wal_ov');"
            for _ in $(seq 1 120); do
                h=$("${PSQL[@]}" -c "SELECT health FROM flashback_health() WHERE table_name='public.wal_ov' LIMIT 1;")
                [[ "$h" == "healthy" ]] && break
                sleep 0.2
            done
            ;;
    esac
    before=$(wal_bytes)
    "${PSQL[@]}" -c "UPDATE public.wal_ov SET v = v || 'u' WHERE id % 2 = 0;"
    after=$(wal_bytes)
    delta=$((after - before))
    printf '%s\n' "$delta"
}

D_DEFAULT=$(run_workload default | tail -n1 | tr -dc '0-9')
D_FULL=$(run_workload ri_full | tail -n1 | tr -dc '0-9')
D_TRACKED=$(run_workload tracked | tail -n1 | tr -dc '0-9')
[[ -n "$D_DEFAULT" && -n "$D_FULL" && -n "$D_TRACKED" ]] || die "workload did not return WAL byte deltas"
printf '%s' "$IDENTITY" | jq -e . >/dev/null || die "candidate identity JSON invalid"

jq -n \
  --arg identity "$IDENTITY" \
  --argjson rows "$ROWS" \
  --argjson default_bytes "$D_DEFAULT" \
  --argjson ri_full_bytes "$D_FULL" \
  --argjson tracked_bytes "$D_TRACKED" \
  '{
     qualification_kind: "exact_wal_overhead",
     status: "passed",
     identity: ($identity | fromjson),
     rows: $rows,
     wal_bytes_delta: {
       default: $default_bytes,
       replica_identity_full: $ri_full_bytes,
       local_delta_tracked: $tracked_bytes
     },
     note: "Bound to Phase 8 candidate identity; estimates only from this evidence"
   }' > "$RESULT_JSON"

log "result: $RESULT_JSON"
cat "$RESULT_JSON"

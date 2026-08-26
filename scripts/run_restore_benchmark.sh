#!/usr/bin/env bash
# Exact-candidate WAL-only restore benchmark (Phase 9).
#
# Hard requirements:
#   CANDIDATE_DIR  — Phase 8 final package identity (MANIFEST.json)
#   Never cargo-builds inside claim runs.
#   Never installs fake capture triggers.
#
# Usage:
#   CANDIDATE_DIR=/path/to/candidate ./scripts/run_restore_benchmark.sh
#
# Scales: PG_FLASHBACK_BENCH_SCALES="1000 10000" (default)

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required for claim runs}"
SCALES="${PG_FLASHBACK_BENCH_SCALES:-1000 10000}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RESULT_DIR="${PG_FLASHBACK_BENCH_RESULT_DIR:-$REPO_ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/exact-wal-restore-benchmark-$RUN_ID.json"
WORK="${PG_FLASHBACK_BENCH_WORK:-$REPO_ROOT/target/exact-wal-bench/$RUN_ID}"
mkdir -p "$RESULT_DIR" "$WORK"

log() { printf '[exact-wal-bench] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

# Detect the legacy fake-trigger helper definition, not this checker line.
if grep -Eq 'CREATE[[:space:]]+OR[[:space:]]+REPLACE[[:space:]]+FUNCTION[[:space:]]+_fb_bench_capture_trigger' "$0"; then
    die "benchmark harness embeds forbidden fake-trigger technique"
fi

exact_candidate_bind_dir "$CANDIDATE_DIR" || die "bind candidate failed"
IDENTITY="$(exact_candidate_identity_json)"
PG_BIN="${PG_BIN:-/usr/local/pgsql-${EC_PG_MAJOR}/bin}"
[[ -x "$PG_BIN/psql" ]] || die "PG_BIN missing psql: $PG_BIN"

exact_candidate_install_into_prefix || die "install candidate failed"
trap 'exact_candidate_restore_prefix || true; cleanup || true' EXIT

DATA="$WORK/data"
SOCKET="/tmp/pgfb-bench-$RUN_ID"
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
port = 28971
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.target_database = postgres
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF

"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/pg.log" start -w
PSQL=("$PG_BIN/psql" -h "$SOCKET" -p 28971 -d postgres -v ON_ERROR_STOP=on -qAt)
"${PSQL[@]}" -c "CREATE EXTENSION pg_flashback;"

RESULTS='[]'
CORRECTNESS=1
for n in $SCALES; do
    log "scale=$n"
    # Unique relation per scale so multi-scale claims do not fight pending
    # successor generations from a prior recover on the same name.
    rel="public.bench_t_${n}"
    "${PSQL[@]}" <<SQL
DROP TABLE IF EXISTS ${rel} CASCADE;
CREATE TABLE ${rel} (id int PRIMARY KEY, payload text);
SELECT flashback_track('${rel}');
SQL
    h=""
    for _ in $(seq 1 120); do
        h=$("${PSQL[@]}" -c "SELECT health FROM flashback_health() WHERE table_name='${rel}' LIMIT 1;")
        [[ "$h" == "healthy" ]] && break
        sleep 0.25
    done
    [[ "$h" == "healthy" ]] || die "not healthy before load"

    "${PSQL[@]}" -c "INSERT INTO ${rel} SELECT g, 'x' FROM generate_series(1,$n) g;"
    for _ in $(seq 1 120); do
        h=$("${PSQL[@]}" -c "SELECT health FROM flashback_health() WHERE table_name='${rel}' LIMIT 1;")
        [[ "$h" == "healthy" ]] && break
        sleep 0.25
    done

    FP=$("${PSQL[@]}" -c "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM ${rel};")
    "${PSQL[@]}" -c "DROP TABLE ${rel};"
    st=""
    for _ in $(seq 1 180); do
        st=$("${PSQL[@]}" -c "SELECT status FROM flashback_disaster_points('${rel}', interval '1 hour') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        [[ "$st" == "restorable" ]] && break
        sleep 0.25
    done

    START_NS=$(date +%s%N)
    TOKEN=$("${PSQL[@]}" -c "SELECT flashback_recover_plan('${rel}')->>'plan_token';")
    OP=$("${PSQL[@]}" -c "SELECT flashback_recover_begin('${rel}', '$TOKEN')->>'operation_id';")
    "${PSQL[@]}" -c "SELECT flashback_recover_execute('${rel}', '$TOKEN', interval '24 hours', NULL, NULL, NULL, $OP);" >/dev/null
    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    FP2=$("${PSQL[@]}" -c "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM ${rel};")
    ok=true
    if [[ "$FP" != "$FP2" ]]; then
        ok=false
        CORRECTNESS=0
    fi
    RPS="unknown"
    if [[ "$ELAPSED_MS" -gt 0 ]]; then
        RPS=$(awk -v n="$n" -v ms="$ELAPSED_MS" 'BEGIN { printf "%.1f", n / (ms/1000.0) }')
    fi
    RESULTS=$(jq -n --argjson acc "$RESULTS" --argjson n "$n" --argjson ms "$ELAPSED_MS" \
        --arg rps "$RPS" --argjson ok "$ok" \
        '$acc + [{rows:$n, elapsed_ms:$ms, rows_per_sec:$rps, fingerprint_ok:$ok}]')
    log "scale=$n elapsed_ms=$ELAPSED_MS rps=$RPS fp_ok=$ok"
done

STATUS=failed
[[ "$CORRECTNESS" == 1 ]] && STATUS=passed
jq -n \
  --arg status "$STATUS" \
  --argjson identity "$IDENTITY" \
  --argjson results "$RESULTS" \
  --argjson correctness_hard_gate "$CORRECTNESS" \
  '{
     qualification_kind: "exact_wal_restore_benchmark",
     status: $status,
     identity: $identity,
     correctness_hard_gate: ($correctness_hard_gate == 1),
     results: $results,
     duration_estimate_note: "duration_estimate remains unknown in plans unless evidence from this report is attached",
     note: "Bound to Phase 8 candidate identity; no cargo build in claim run"
   }' > "$RESULT_JSON"

log "result: $RESULT_JSON"
[[ "$CORRECTNESS" == 1 ]] || exit 1

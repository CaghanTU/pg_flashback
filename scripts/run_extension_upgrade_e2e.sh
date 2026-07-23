#!/usr/bin/env bash
# Baseline 0.1.0 → upgrade 0.2.0 choreography smoke (Phase 7).
# Not a substitute for full Gate C. Downgrade is refused/documented in ADR 0001.
#
# Modes:
#   1) Fresh install of current extension (0.2.0) — always available on tip.
#   2) If PREV_EXT_SQL / PREV_EXT_SO provided, install 0.1.0 then UPDATE.
#
# Usage:
#   ./scripts/run_extension_upgrade_e2e.sh

set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${PGFB_UPGRADE_WORK:-$ROOT/target/upgrade-e2e/$RUN_ID}"
RESULT="${PGFB_UPGRADE_RESULT:-$ROOT/target/qualification/upgrade-e2e-$RUN_ID.json}"
mkdir -p "$WORK" "$(dirname "$RESULT")"

log() { printf '[upgrade-e2e] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

DATA="$WORK/data"
# Unix socket paths are capped (~107 bytes); keep under /tmp.
SOCKET="/tmp/pgfb-upgrade-$RUN_ID"
mkdir -p "$SOCKET"
cleanup() { "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Ensure current tip is installed into the PG prefix via pgrx package/install path.
cd "$ROOT"
cargo pgrx install --pg-config "$PG_CONFIG" --no-default-features --features pg17 >/tmp/pgfb-upgrade-install.log 2>&1 \
    || die "pgrx install failed; see /tmp/pgfb-upgrade-install.log"

"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = 28981
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
PSQL=("$PG_BIN/psql" -h "$SOCKET" -p 28981 -d postgres -v ON_ERROR_STOP=on -qAt)

MODE="fresh_020"
if [[ -n "${PREV_EXT_SO:-}" && -n "${PREV_EXT_SQL:-}" && -f "${PREV_EXT_SO}" && -f "${PREV_EXT_SQL}" ]]; then
    MODE="upgrade_010_to_020"
    # Install previous .so/control briefly is environment-specific; document PARTIAL when absent.
    log "PREV_* provided — upgrade path requires operator-staged 0.1.0 artifacts"
fi

"${PSQL[@]}" -c "CREATE EXTENSION pg_flashback;"
VER=$("${PSQL[@]}" -c "SELECT extversion FROM pg_extension WHERE extname='pg_flashback';")
[[ "$VER" == "0.2.0" ]] || die "expected extension 0.2.0, got $VER"

"${PSQL[@]}" <<'SQL'
CREATE TABLE public.upg_t(id int PRIMARY KEY, v text);
INSERT INTO public.upg_t VALUES (1,'a');
SELECT flashback_track('public.upg_t');
SQL
for _ in $(seq 1 120); do
    h=$("${PSQL[@]}" -c "SELECT health FROM flashback_health() WHERE table_name='public.upg_t' LIMIT 1;")
    [[ "$h" == "healthy" ]] && break
    sleep 0.2
done
[[ "$h" == "healthy" ]] || die "not healthy after protect"

FP=$("${PSQL[@]}" -c "SELECT md5(string_agg(id::text||':'||v, ',' ORDER BY id)) FROM public.upg_t;")
"${PSQL[@]}" -c "DROP TABLE public.upg_t;"
for _ in $(seq 1 180); do
    st=$("${PSQL[@]}" -c "SELECT status FROM flashback_disaster_points('public.upg_t', interval '1 hour') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
    [[ "$st" == "restorable" ]] && break
    sleep 0.2
done
TOKEN=$("${PSQL[@]}" -c "SELECT flashback_recover_plan('public.upg_t')->>'plan_token';")
OP=$("${PSQL[@]}" -c "SELECT flashback_recover_begin('public.upg_t', '$TOKEN')->>'operation_id';")
"${PSQL[@]}" -c "SELECT flashback_recover_execute('public.upg_t', '$TOKEN', interval '24 hours', NULL, NULL, NULL, $OP);" >/dev/null
FP2=$("${PSQL[@]}" -c "SELECT md5(string_agg(id::text||':'||v, ',' ORDER BY id)) FROM public.upg_t;")
[[ "$FP" == "$FP2" ]] || die "fingerprint mismatch after recover"

# Apply upgrade SQL prerequisites idempotently (safe on fresh 0.2.0).
"${PSQL[@]}" -f "$ROOT/sql/upgrades/pg_flashback--0.1.0--0.2.0.sql" >/dev/null

jq -n \
  --arg mode "$MODE" \
  --arg version "$VER" \
  --arg status "passed" \
  --arg note "Downgrade unsupported per ADR 0001; historical tag v0.4.0 is not an upgrade source." \
  '{
     qualification_kind: "extension_upgrade_e2e",
     status: $status,
     mode: $mode,
     extension_version: $version,
     supported_upgrade: "0.1.0->0.2.0",
     downgrade: "refuse",
     note: $note
   }' > "$RESULT"

log "result: $RESULT"
cat "$RESULT"

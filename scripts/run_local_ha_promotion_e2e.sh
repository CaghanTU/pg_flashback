#!/usr/bin/env bash
# Primary/standby fail-closed guards for mutating local APIs.
# Builds a physical standby, proves protect/recover refuse on standby,
# documents fencing requirements. Does not emulate failover slots.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_BIN="${PG_BIN:-/usr/local/pgsql-17/bin}"
PORT_PRIMARY="${PG_FLASHBACK_HA_PRIMARY_PORT:-28971}"
PORT_STANDBY="${PG_FLASHBACK_HA_STANDBY_PORT:-28972}"
WORK="${PG_FLASHBACK_HA_WORK:-$ROOT/target/local-ha-e2e/$$}"
PRIMARY="$WORK/primary"
STANDBY="$WORK/standby"
SOCK="$WORK/sock"
mkdir -p "$SOCK" "$WORK"

cleanup() {
    "$PG_BIN/pg_ctl" -D "$PRIMARY" stop -m immediate -w >/dev/null 2>&1 || true
    "$PG_BIN/pg_ctl" -D "$STANDBY" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

log() { printf '[local-ha-e2e] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

command -v "$PG_BIN/initdb" >/dev/null || die "PG_BIN=$PG_BIN missing initdb"
test -f /usr/local/pgsql-17/share/extension/pg_flashback.control \
    || die "pg_flashback not installed into prefix"

"$PG_BIN/initdb" -D "$PRIMARY" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$PRIMARY/postgresql.conf" <<EOF
port = $PORT_PRIMARY
unix_socket_directories = '$SOCK'
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
wal_log_hints = on
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
echo "local replication trust" >>"$PRIMARY/pg_hba.conf"
echo "host replication all 127.0.0.1/32 trust" >>"$PRIMARY/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >>"$PRIMARY/pg_hba.conf"

"$PG_BIN/pg_ctl" -D "$PRIMARY" -l "$WORK/primary.log" start -w
PSQL_P="$PG_BIN/psql -h $SOCK -p $PORT_PRIMARY -v ON_ERROR_STOP=1 -qAt"

$PSQL_P -d postgres -c "CREATE EXTENSION pg_flashback;"
$PSQL_P -d postgres -c "SELECT pg_create_physical_replication_slot('ha_standby_slot', true);" >/dev/null

"$PG_BIN/pg_basebackup" -h "$SOCK" -p "$PORT_PRIMARY" -D "$STANDBY" -Fp -Xs -P -R \
    --slot=ha_standby_slot >/dev/null
cat >>"$STANDBY/postgresql.conf" <<EOF
port = $PORT_STANDBY
unix_socket_directories = '$SOCK'
primary_slot_name = 'ha_standby_slot'
EOF

"$PG_BIN/pg_ctl" -D "$STANDBY" -l "$WORK/standby.log" start -w
PSQL_S="$PG_BIN/psql -h $SOCK -p $PORT_STANDBY -v ON_ERROR_STOP=1 -qAt"

[[ "$($PSQL_S -d postgres -c 'SELECT pg_is_in_recovery();')" == t ]] \
    || die "standby not in recovery"

$PSQL_P -d postgres -c "CREATE TABLE public.ha_orders(id int PRIMARY KEY, v text);
                         SELECT flashback_track('public.ha_orders');" >/dev/null

# Standby mutating APIs must fail closed.
rc=0
$PSQL_S -d postgres -c "SELECT flashback_track('public.ha_orders');" \
    >"$WORK/standby_track.err" 2>&1 || rc=$?
[[ "$rc" != 0 ]] || die "flashback_track succeeded on standby"
grep -Eiq 'standby|pg_is_in_recovery|read.only' "$WORK/standby_track.err" \
    || die "standby track error missing recovery hint"

rc=0
$PSQL_S -d postgres -c "SELECT flashback_restore_lsn('public.ha_orders', '0/1');" \
    >"$WORK/standby_restore.err" 2>&1 || rc=$?
[[ "$rc" != 0 ]] || die "flashback_restore_lsn succeeded on standby"

log "PASS: standby protect/recover refuse (fencing remains operator-owned)"
log "NOTE: After promote, if logical slot/epoch continuity is unproven, open a durable gap and require re-anchor; never advertise old-primary history as valid on the new timeline."
echo "local-ha-e2e: PASS"

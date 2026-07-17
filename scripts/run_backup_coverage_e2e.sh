#!/usr/bin/env bash
# Real-transaction backup coverage lifecycle smoke.
# Verifies: dedicated-txn track_backup, worker marker COMMIT resolve,
# verified proof activation after marker, prepare admission, and legacy/raw
# coverage rejection. Full helper swap E2E remains scripts/run_recovery_helper_e2e.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_BIN="${PG_BIN:-/usr/local/pgsql-17/bin}"
PSQL="${PSQL:-$PG_BIN/psql}"
PORT="${PG_FLASHBACK_BACKUP_E2E_PORT:-28827}"
DATA="${PG_FLASHBACK_BACKUP_E2E_DATA:-$ROOT/target/backup-coverage-e2e-pgdata}"
SOCKET="${PG_FLASHBACK_BACKUP_E2E_SOCKET:-$ROOT/target/backup-coverage-e2e-socket}"
DB=postgres

mkdir -p "$SOCKET"
cleanup() {
  "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
  rm -rf "$DATA" "$SOCKET"
}
trap cleanup EXIT

rm -rf "$DATA"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$DATA/postgresql.log" \
  -o "-p $PORT -k $SOCKET" \
  start -w >/dev/null

q() { "$PSQL" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }

q "CREATE EXTENSION pg_flashback;"
q "CREATE TABLE public.backup_cov(id int PRIMARY KEY, note text NOT NULL);"
q "INSERT INTO public.backup_cov VALUES (1, 'a');"

# Dedicated transaction: track alone.
TRACK=$(q "SELECT flashback_track_backup('public.backup_cov', 'e2e_helper');")
test "$TRACK" = "t"
STATE=$(q "SELECT state FROM flashback.coverage_generations WHERE recovery_profile='backup';")
test "$STATE" = "building"
TRACKING_ID=$(q "SELECT tracking_id FROM flashback.tracked_tables
                 WHERE table_name = 'backup_cov' AND is_active;")

# Wait for worker/consume to resolve the marker COMMIT into details.
RESOLVED=0
for _ in $(seq 1 100); do
  RESOLVED=$(q "SELECT count(*) FROM flashback.coverage_generations
                 WHERE recovery_profile='backup' AND state='building'
                   AND details ? 'tracking_marker_lsn';")
  [[ "$RESOLVED" == "1" ]] && break
  q "SELECT flashback_consume_wal(4096);" >/dev/null || true
  sleep 0.1
done
test "$RESOLVED" = "1"

MARKER=$(q "SELECT details->>'tracking_marker_lsn'
            FROM flashback.coverage_generations
            WHERE recovery_profile='backup' AND state='building';")
SYSID=$(q "SELECT system_identifier FROM pg_control_system();")
TIMELINE=$(q "SELECT timeline_id FROM pg_control_checkpoint();")
START=$(q "SELECT ('$MARKER'::pg_lsn + 1)::text;")
STOP=$(q "SELECT ('$MARKER'::pg_lsn + 50)::text;")

# Raw activate remains closed.
if q "SELECT flashback_activate_backup_anchor(
        'public.backup_cov', 'repo', 'stanza', 'rawF',
        $SYSID, $TIMELINE, 'manifest-raw', repeat('aa', 32),
        '$START'::pg_lsn, '$STOP'::pg_lsn);" 2>/dev/null; then
  echo "FAIL: raw activate_backup_anchor was accepted"
  exit 1
fi

# Reject overlapping start via proof consume.
if q "SELECT flashback_consume_verified_backup_proof(
        flashback_install_verified_backup_proof(
          'e2e-overlap', $TRACKING_ID, 'e2e_helper', 'repo', 'stanza', 'overlapF',
          $SYSID, $TIMELINE, 'manifest-overlap', repeat('aa', 32),
          '$MARKER'::pg_lsn, '$STOP'::pg_lsn
        ));" 2>/dev/null; then
  echo "FAIL: overlapping FULL start was accepted"
  exit 1
fi

GEN=$(q "SELECT flashback_consume_verified_backup_proof(
        flashback_install_verified_backup_proof(
          'e2e-activate', $TRACKING_ID, 'e2e_helper', 'repo', 'stanza', '20260717E2EF',
          $SYSID, $TIMELINE, 'manifest-e2e', repeat('ab', 32),
          '$START'::pg_lsn, '$STOP'::pg_lsn
        ));")
ACTIVE=$(q "SELECT state || ':' || valid_through_lsn::text
            FROM flashback.coverage_generations WHERE generation_id = $GEN;")
test "$ACTIVE" = "active:$STOP"

# Legacy assertion remains rejected.
if q "SELECT flashback_set_backup_coverage('public.backup_cov', '$START'::pg_lsn, '$STOP'::pg_lsn);" 2>/dev/null; then
  echo "FAIL: legacy set_backup_coverage was accepted"
  exit 1
fi

# One-time proof reuse is rejected.
if q "SELECT flashback_consume_verified_backup_proof(
        (SELECT proof_id FROM flashback.verified_backup_proofs
         WHERE verification_request_id = 'e2e-activate'));" 2>/dev/null; then
  echo "FAIL: consumed proof was reusable"
  exit 1
fi

PREP=$(q "SELECT flashback_prepare_backup_restore('public.backup_cov', '$STOP'::pg_lsn)->>'generation_id';")
test "$PREP" = "$GEN"

echo "ok: backup coverage track/resolve/proof-activate/prepare"

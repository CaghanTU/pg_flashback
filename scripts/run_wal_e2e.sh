#!/bin/bash
# ShellCheck cannot see that cleanup is entered through the EXIT trap.
# shellcheck disable=SC2317,SC2329
# pg_flashback — WAL mode + background worker end-to-end verification.
#
# The pgrx test harness runs every test inside ONE transaction, and logical
# decoding only ever sees committed transactions — so the real WAL pipeline
# (slot → worker → flashback_consume_wal → delta_log → restore) can only be
# verified with separate sessions/transactions against a live instance.
# This script does exactly that, including:
#   * per-database slot creation via flashback_track()
#   * background worker consumption (real commit_time + LSN stamping)
#   * large-transaction consumption stays linear (commit map is not rescanned
#     once per decoded row)
#   * poison-value safety (quoted table name, NaN numeric)
#   * generation activation + COMMIT-LSN target admission
#   * concurrent first-track serialization through one bootstrap/stable-lock
#     handoff (including first slot creation)
#   * a writer committing while track waits is present in the exact base,
#     proving the scan snapshot is acquired after the relation lock
#   * fail-closed timestamp-to-prefix resolution
#   * LSN query, deleted-row recovery and full restore
#   * REPLICA IDENTITY lifecycle (track sets FULL, restore keeps it,
#     untrack restores the original)
#   * cross-database coverage warning for a database no worker serves
#
# Requires: a PostgreSQL instance with pg_flashback in shared_preload_libraries
# and wal_level=logical. Defaults target the cargo-pgrx dev instance.
# The instance is RESTARTED (target_databases is a postmaster GUC).
#
# Cleanup is guaranteed: an EXIT trap restores the saved GUCs, restarts the
# instance and drops the test databases/slots on success, failure and
# interrupt alike. Set WAL_E2E_KEEP=1 to skip cleanup for debugging.
#
# Usage: ./scripts/run_wal_e2e.sh [port] [socket_dir] [pgdata] [bindir]

set -euo pipefail

PORT="${1:-28817}"
SOCKDIR="${2:-$HOME/.pgrx}"
PGDATA="${3:-$HOME/.pgrx/data-17}"
BINDIR="${4:-}"

if [[ -z "$BINDIR" ]]; then
    for cand in /usr/local/pgsql-17/bin "$HOME/.pgrx/17."*/pgrx-install/bin; do
        if [[ -x "$cand/psql" && -x "$cand/pg_ctl" ]]; then BINDIR="$cand"; break; fi
    done
fi
[[ -n "$BINDIR" ]] || { echo "FAIL: psql/pg_ctl bulunamadı; bindir argümanı verin"; exit 1; }

PSQL="$BINDIR/psql -h $SOCKDIR -p $PORT -v ON_ERROR_STOP=on"
DB="wal_e2e"
UNCOV_DB="wal_e2e_uncov"

OLD_TARGETS=""
OLD_MODE=""
OLD_ENABLED=""
GUCS_MODIFIED=0
CLEANED=0
RETENTION_PAUSE_PID=""
RETENTION_PIN_PID=""
RETENTION_BEGIN_PID=""
RETENTION_BLOCKER_PID=""
RETENTION_RESUME_PID=""
STOPPED_WORKER_PID=""
CONSUME_LOCK_PID=""

q()  { $PSQL -d "$DB" -qAtc "$1"; }
qp() { $PSQL -d postgres -qAtc "$1"; }

stop_idle_worker() {
    local pid=""
    for _ in $(seq 1 100); do
        pid=$(q "SELECT pid FROM pg_stat_activity
            WHERE backend_type='pg_flashback delta worker'
              AND datname=current_database()
              AND wait_event_type='Extension'
            LIMIT 1")
        if [[ -n "$pid" ]]; then
            kill -STOP "$pid"
            sleep 0.05
            if [[ "$(q "SELECT count(*) FROM pg_locks
                         WHERE pid=$pid AND locktype='advisory' AND granted")" == "0" ]]; then
                echo "$pid"
                return 0
            fi
            kill -CONT "$pid" > /dev/null 2>&1 || true
        fi
        sleep 0.05
    done
    return 1
}

assert_eq() { # desc expected actual
    if [[ "$2" != "$3" ]]; then echo "FAIL: $1 (beklenen=$2, bulunan=$3)"; exit 1; fi
    echo "  ok: $1 = $3"
}

restart_pg() {
    "$BINDIR/pg_ctl" -D "$PGDATA" restart -w -t 60 -l "$SOCKDIR/17.log" > /dev/null 2>&1 \
        || "$BINDIR/pg_ctl" -D "$PGDATA" start -w -t 60 -l "$SOCKDIR/17.log" > /dev/null 2>&1 \
        || true
    for _ in $(seq 1 30); do
        qp "SELECT 1" > /dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

# Idempotent cleanup — runs on EXIT (success, failure and signal alike).
# Order matters: GUCs first + restart (detaches workers from the test DBs),
# then slots (they block DROP DATABASE), then the databases.
cleanup() {
    local rc=$?
    [[ "$CLEANED" == "1" ]] && exit "$rc"
    CLEANED=1
    if [[ "${WAL_E2E_KEEP:-0}" == "1" ]]; then
        echo "*** WAL_E2E_KEEP=1: ortam inceleme için bırakıldı (GUC'lar test değerlerinde!) ***"
        exit "$rc"
    fi
    set +e
    [[ -n "$STOPPED_WORKER_PID" ]] && kill -CONT "$STOPPED_WORKER_PID" > /dev/null 2>&1
    [[ -n "$CONSUME_LOCK_PID" ]] && kill "$CONSUME_LOCK_PID" > /dev/null 2>&1
    [[ -n "$RETENTION_RESUME_PID" ]] && kill "$RETENTION_RESUME_PID" > /dev/null 2>&1
    [[ -n "$RETENTION_BLOCKER_PID" ]] && kill "$RETENTION_BLOCKER_PID" > /dev/null 2>&1
    [[ -n "$RETENTION_BEGIN_PID" ]] && kill "$RETENTION_BEGIN_PID" > /dev/null 2>&1
    [[ -n "$RETENTION_PIN_PID" ]] && kill "$RETENTION_PIN_PID" > /dev/null 2>&1
    [[ -n "$RETENTION_PAUSE_PID" ]] && kill "$RETENTION_PAUSE_PID" > /dev/null 2>&1
    q "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
       WHERE application_name LIKE 'pgfb_%' AND pid <> pg_backend_pid()" > /dev/null 2>&1
    echo "━━━ Temizlik: GUC'ları geri al, restart, test DB/slot'larını düşür ━━━"
    if [[ "$GUCS_MODIFIED" == "1" ]]; then
        # Guard: a previous ABORTED run may have left our own test DB name in
        # target_databases; "restoring" that poisoned value would strand the
        # workers on a database this cleanup is about to drop.
        if [[ -n "$OLD_TARGETS" && "$OLD_TARGETS" != "$DB" && "$OLD_TARGETS" != "$UNCOV_DB" ]]; then
            qp "ALTER SYSTEM SET pg_flashback.target_databases = '$OLD_TARGETS'" > /dev/null 2>&1
        else
            qp "ALTER SYSTEM RESET pg_flashback.target_databases" > /dev/null 2>&1
        fi
        if [[ -n "$OLD_MODE" ]]; then
            qp "ALTER SYSTEM SET pg_flashback.capture_mode = '$OLD_MODE'" > /dev/null 2>&1
        else
            qp "ALTER SYSTEM RESET pg_flashback.capture_mode" > /dev/null 2>&1
        fi
        if [[ -n "$OLD_ENABLED" ]]; then
            qp "ALTER SYSTEM SET pg_flashback.enabled = '$OLD_ENABLED'" > /dev/null 2>&1
        else
            qp "ALTER SYSTEM RESET pg_flashback.enabled" > /dev/null 2>&1
        fi
        restart_pg || echo "  uyarı: instance yeniden başlatılamadı — elle kontrol edin"
    fi
    qp "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name IN ('pg_flashback_${DB}', 'pg_flashback_${UNCOV_DB}')" > /dev/null 2>&1
    qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" > /dev/null 2>&1
    qp "DROP DATABASE IF EXISTS $UNCOV_DB WITH (FORCE)" > /dev/null 2>&1
    qp "DROP ROLE IF EXISTS wal_e2e_app" > /dev/null 2>&1
    rm -f /tmp/pg_flashback_track_race_1.out \
        /tmp/pg_flashback_track_race_2.out \
        /tmp/pg_flashback_boundary_writer.out \
        /tmp/pg_flashback_consume_lock.out \
        /tmp/pg_flashback_slot_loss_lock.out \
        /tmp/pg_flashback_retention_pause.out \
        /tmp/pg_flashback_retention_pin.out \
        /tmp/pg_flashback_retention_begin.out \
        /tmp/pg_flashback_retention_blocker.out \
        /tmp/pg_flashback_retention_resume.out \
        /tmp/pg_flashback_retention_admit.out \
        /tmp/pg_flashback_enabled_gap_reject.out \
        /tmp/pg_flashback_mode_ddl_reject.out
    echo "  ok: temizlik tamamlandı (target_databases=$(qp "SELECT current_setting('pg_flashback.target_databases', true)" 2>/dev/null))"
    echo ""
    if [[ "$rc" == "0" ]]; then
        echo "╔══════════════════════════════════════════╗"
        echo "║   WAL E2E: TÜM KONTROLLER BAŞARILI ✔     ║"
        echo "╚══════════════════════════════════════════╝"
    else
        echo "╔══════════════════════════════════════════╗"
        echo "║   WAL E2E: BAŞARISIZ ✘ (ortam geri alındı)║"
        echo "╚══════════════════════════════════════════╝"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'echo "FAIL: hata (satır $LINENO)"' ERR

echo "━━━ 0. Ortam hazırlığı (GUC kaydet, DB + slot temizle, restart) ━━━"
OLD_TARGETS=$(qp "SELECT current_setting('pg_flashback.target_databases', true)")
OLD_MODE=$(qp "SELECT current_setting('pg_flashback.capture_mode', true)")
OLD_ENABLED=$(qp "SELECT current_setting('pg_flashback.enabled', true)")

qp "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
    WHERE slot_name IN ('pg_flashback_${DB}', 'pg_flashback_${UNCOV_DB}')" > /dev/null || true
qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" > /dev/null
qp "DROP DATABASE IF EXISTS $UNCOV_DB WITH (FORCE)" > /dev/null
qp "DROP ROLE IF EXISTS wal_e2e_app" > /dev/null
qp "CREATE ROLE wal_e2e_app LOGIN" > /dev/null
qp "CREATE DATABASE $DB" > /dev/null
GUCS_MODIFIED=1
qp "ALTER SYSTEM SET pg_flashback.target_databases = '$DB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.capture_mode = 'wal'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.enabled = 'on'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.local_max_snapshot_bytes = '8GB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.local_max_restore_peak_bytes = '16GB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.local_min_filesystem_bytes = '64MB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.local_safety_reserve_bytes = '16MB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.local_boundary_write_stall_ms = 60000" > /dev/null
restart_pg || { echo "FAIL: PostgreSQL yeniden başlatılamadı"; exit 1; }
echo "  ok: instance yeniden başladı (target_databases=$DB, capture_mode=wal)"

echo "━━━ 1. Extension + track (per-DB slot, ayrı transaction'lar) ━━━"
q "CREATE EXTENSION pg_flashback" > /dev/null
q "CREATE TABLE orders (id serial PRIMARY KEY, customer text, amount numeric(10,2))" > /dev/null
q "CREATE TABLE filtered_relevant(id integer PRIMARY KEY, payload text NOT NULL)" > /dev/null
q "CREATE TABLE track_race (id integer PRIMARY KEY, payload text)" > /dev/null
q "CREATE TABLE track_boundary (id integer PRIMARY KEY, payload text);
   INSERT INTO track_boundary VALUES (1, 'old')" > /dev/null
q "CREATE TABLE ddl_probe (id integer PRIMARY KEY, payload text);
   ALTER TABLE ddl_probe OWNER TO wal_e2e_app" > /dev/null
q "CREATE TABLE wal_batch_probe (id int PRIMARY KEY, version int NOT NULL DEFAULT 0, payload text NOT NULL);
   INSERT INTO wal_batch_probe
   SELECT g, 0, string_agg(md5(g::text || ':' || s::text), '')
   FROM generate_series(1,2000) AS g
   CROSS JOIN generate_series(1,32) AS s
   GROUP BY g" > /dev/null
q "CREATE TABLE wal_amp_default (LIKE wal_batch_probe INCLUDING ALL);
   CREATE TABLE wal_amp_full (LIKE wal_batch_probe INCLUDING ALL);
   INSERT INTO wal_amp_default SELECT * FROM wal_batch_probe;
   INSERT INTO wal_amp_full SELECT * FROM wal_batch_probe;
   ALTER TABLE wal_amp_full REPLICA IDENTITY FULL;
   CREATE OR REPLACE FUNCTION wal_amp_update(p_table regclass)
   RETURNS TABLE(source_xid bigint, wal_bytes numeric)
   LANGUAGE plpgsql
   AS \$fn\$
   DECLARE
       v_start pg_lsn;
   BEGIN
       source_xid := txid_current()::bigint;
       v_start := pg_current_wal_insert_lsn();
       EXECUTE format(
           'UPDATE %s AS t SET version=version+1, '
           'payload=(SELECT string_agg(md5(t.id::text || '':'' || '
           't.version::text || '':'' || s::text), '''') '
           'FROM generate_series(1,32) AS s)',
           p_table
       );
       wal_bytes := pg_wal_lsn_diff(pg_current_wal_insert_lsn(), v_start);
       RETURN NEXT;
   END;
   \$fn\$" > /dev/null
# The first two sessions also race to create the database's logical slot. The
# database/bootstrap/stable lock handoff must let exactly one lifecycle commit;
# the loser fails cleanly after observing that binding.
$PSQL -d "$DB" -qc "SELECT flashback_track('track_race')" \
    > /tmp/pg_flashback_track_race_1.out 2>&1 &
TRACK_RACE_PID_1=$!
$PSQL -d "$DB" -qc "SELECT flashback_track('track_race')" \
    > /tmp/pg_flashback_track_race_2.out 2>&1 &
TRACK_RACE_PID_2=$!
if wait "$TRACK_RACE_PID_1"; then TRACK_RACE_RC_1=0; else TRACK_RACE_RC_1=$?; fi
if wait "$TRACK_RACE_PID_2"; then TRACK_RACE_RC_2=0; else TRACK_RACE_RC_2=$?; fi
if [[ "$TRACK_RACE_RC_1" == "$TRACK_RACE_RC_2" ]]; then
    echo "FAIL: concurrent first-track tam olarak bir başarı üretmedi (rc1=$TRACK_RACE_RC_1 rc2=$TRACK_RACE_RC_2)"
    exit 1
fi
assert_eq "concurrent first-track tek current binding bıraktı" "1" \
    "$(q "SELECT count(*) FROM flashback.tracked_tables WHERE table_name='track_race'")"
assert_eq "concurrent first-track tek lifecycle bıraktı" "1" \
    "$(q "SELECT count(*) FROM flashback.tracking_lifecycles WHERE initial_table_name='track_race'")"
assert_eq "concurrent first-track tek generation bıraktı" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg
           JOIN flashback.tracked_tables tt USING (tracking_id)
           WHERE tt.table_name='track_race'")"
assert_eq "concurrent first-track tek base snapshot bıraktı" "1" \
    "$(q "SELECT count(*) FROM flashback.snapshots s
           JOIN flashback.tracked_tables tt USING (tracking_id)
           WHERE tt.table_name='track_race'")"
for _ in $(seq 1 100); do
    [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.track_race'")" == "healthy" ]] && break
    sleep 0.1
done
q "SELECT flashback_untrack('track_race'); DROP TABLE track_race" > /dev/null

# A writer owns RowExclusiveLock before track starts. Track must wait, then its
# volatile READ COMMITTED CTAS must acquire a new post-lock snapshot containing
# the writer's committed value. Reusing the outer SELECT snapshot would capture
# 'old' here and silently open RB-01/RB-07 again.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_boundary_writer.out 2>&1 <<'SQL' &
BEGIN;
UPDATE track_boundary SET payload = 'committed-before-base' WHERE id = 1;
SELECT pg_sleep(3);
COMMIT;
SQL
BOUNDARY_WRITER_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_locks
               WHERE relation='track_boundary'::regclass
                 AND mode='RowExclusiveLock' AND granted")" -ge 1 ]] && break
    sleep 0.1
done
assert_eq "boundary writer relation lock aldı" "1" \
    "$(q "SELECT (count(*) >= 1)::int FROM pg_locks
           WHERE relation='track_boundary'::regclass
             AND mode='RowExclusiveLock' AND granted")"
q "SELECT flashback_track('track_boundary')" > /dev/null
if ! wait "$BOUNDARY_WRITER_PID"; then
    echo "FAIL: boundary writer transaction başarısız"
    exit 1
fi
BOUNDARY_BASE=$(q "SELECT base_snapshot_table FROM flashback.tracked_tables
                    WHERE table_name='track_boundary'")
assert_eq "post-lock exact base writer commit'ini içeriyor" "committed-before-base" \
    "$(q "SELECT payload FROM $BOUNDARY_BASE WHERE id=1")"
for _ in $(seq 1 100); do
    [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.track_boundary'")" == "healthy" ]] && break
    sleep 0.1
done
q "SELECT flashback_untrack('track_boundary'); DROP TABLE track_boundary" > /dev/null

q "SELECT flashback_track('orders')" > /dev/null
q "SELECT flashback_track('filtered_relevant')" > /dev/null
q "SELECT flashback_track('wal_batch_probe')" > /dev/null
q "SELECT flashback_track('ddl_probe')" > /dev/null
$PSQL -d "$DB" -q <<'SQL'
CREATE TABLE "we""ird" (id int PRIMARY KEY, amount numeric);
SELECT flashback_track('"we""ird"');
SQL

assert_eq "slot bu veritabanında" "$DB" \
    "$(q "SELECT database FROM pg_replication_slots WHERE slot_name = 'pg_flashback_${DB}'")"
assert_eq "WAL track REPLICA IDENTITY FULL yaptı" "f" \
    "$(q "SELECT relreplident FROM pg_class WHERE oid = 'orders'::regclass")"

for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback_health() WHERE health='healthy'")" == "5" ]] && break
    sleep 0.1
done
assert_eq "beş WAL generation aktif ve sağlıklı" "5" \
    "$(q "SELECT count(*) FROM flashback_health() WHERE health='healthy'")"
assert_eq "building generation kalmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations WHERE state='building'")"

echo "━━━ 1a. Filtered WAL prefix slot'u bounded sürede ilerletiyor ━━━"
SLOT_NAME="pg_flashback_${DB}"
# A backup generation legitimately remains building after its tracking marker
# is resolved while the controller waits for a later FULL backup.  That wait
# must not pin unrelated filtered WAL in the database-wide logical slot.
q "CREATE TABLE backup_wait(id integer PRIMARY KEY, payload text NOT NULL)" > /dev/null
q "SELECT flashback_track_backup('backup_wait', 'wal_e2e_profile')" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.coverage_generations cg
               JOIN flashback.tracked_tables tt USING (tracking_id)
               WHERE tt.table_name='backup_wait'
                 AND cg.state='building'
                 AND cg.state_reason='marker_commit_observed'
                 AND cg.details ? 'tracking_marker_lsn'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "backup FULL beklerken marker COMMIT LSN çözüldü" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg
           JOIN flashback.tracked_tables tt USING (tracking_id)
           WHERE tt.table_name='backup_wait'
             AND cg.state='building'
             AND cg.state_reason='marker_commit_observed'
             AND cg.details ? 'tracking_marker_lsn'")"
FILTERED_DELTA_BEFORE=$(q "SELECT count(*) FROM flashback.delta_log")
FILTERED_START_FLUSH=$(q "SELECT confirmed_flush_lsn FROM pg_replication_slots
                           WHERE slot_name='$SLOT_NAME'")
q "CREATE TABLE untracked_wal_noise(id integer PRIMARY KEY, payload text NOT NULL)" > /dev/null
for chunk in $(seq 1 16); do
    q "INSERT INTO untracked_wal_noise
       SELECT (($chunk - 1) * 128) + g,
              string_agg(md5(($chunk::text || ':' || g::text || ':' || s::text ||
                              ':' || random()::text)), '' ORDER BY s)
       FROM generate_series(1, 128) AS g
       CROSS JOIN generate_series(1, 64) AS s
       GROUP BY g" > /dev/null
done
FILTERED_TARGET_LSN=$(q "SELECT pg_current_wal_flush_lsn()")
FILTERED_GENERATED_BYTES=$(q "SELECT pg_wal_lsn_diff(
    '$FILTERED_TARGET_LSN'::pg_lsn, '$FILTERED_START_FLUSH'::pg_lsn)::bigint")
for _ in $(seq 1 200); do
    q "SELECT flashback_consume_wal(4096)" > /dev/null
    FILTERED_CONFIRMED=$(q "SELECT confirmed_flush_lsn FROM pg_replication_slots
                             WHERE slot_name='$SLOT_NAME'")
    [[ "$(q "SELECT '$FILTERED_CONFIRMED'::pg_lsn >=
                       '$FILTERED_TARGET_LSN'::pg_lsn")" == "t" ]] && break
    sleep 0.02
done
assert_eq "yalnız filtered WAL sonrası confirmed_flush sabit hedefe ulaştı" "t" \
    "$(q "SELECT confirmed_flush_lsn >= '$FILTERED_TARGET_LSN'::pg_lsn
           FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'")"
assert_eq "empty-prefix consume delta_log satırı üretmedi" "$FILTERED_DELTA_BEFORE" \
    "$(q "SELECT count(*) FROM flashback.delta_log")"
assert_eq "empty-prefix gerçek slot konumunu stream metadata'sına kaydetti" "t" \
    "$(q "SELECT cs.confirmed_flush_lsn = rs.confirmed_flush_lsn
           FROM flashback.capture_streams cs
           JOIN pg_replication_slots rs ON rs.slot_name = cs.slot_name
           WHERE cs.state='active'")"
q "SELECT flashback_ensure_active_wal_stream()" > /dev/null
assert_eq "bounded empty-prefix kendi slot ilerlemesini external saymadı" "0" \
    "$(q "SELECT count(*) FROM flashback.capture_streams
           WHERE state='broken'
             AND invalidation_reason='replication_slot_advanced_externally'")"
assert_eq "building backup generation filtered WAL ilerlemesini engellemedi" "building" \
    "$(q "SELECT cg.state FROM flashback.coverage_generations cg
           JOIN flashback.tracked_tables tt USING (tracking_id)
           WHERE tt.table_name='backup_wait'")"
echo "  ok: filtered_bytes=$FILTERED_GENERATED_BYTES target=$FILTERED_TARGET_LSN confirmed=$FILTERED_CONFIRMED"

q "INSERT INTO filtered_relevant VALUES (1, 'after-filtered-prefix')" > /dev/null
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
               WHERE rel_oid='filtered_relevant'::regclass
                 AND event_type='INSERT'
                 AND new_data->>'payload'='after-filtered-prefix'")" == "1" ]] && break
    q "SELECT flashback_consume_wal(4096)" > /dev/null
    sleep 0.02
done
assert_eq "filtered prefix arkasındaki relevant commit kaybolmadı" "1" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE rel_oid='filtered_relevant'::regclass
             AND event_type='INSERT'
             AND new_data->>'payload'='after-filtered-prefix'")"
q "SELECT flashback_untrack('filtered_relevant'); DROP TABLE filtered_relevant" > /dev/null

# pg_logical_emit_message() is PUBLIC. A non-admin may send a payload that
# looks exactly like a DELETE event, but the decoder must ignore the body and
# the worker must not admit its COMMIT into the trusted table-state ledger.
MALICIOUS_OUTPUT=$(q "SET ROLE wal_e2e_app;
    SELECT txid_current()::text;
    SELECT pg_logical_emit_message(
        true, 'pg_flashback',
        jsonb_build_object(
            'op', 'DELETE', 'schema', 'public', 'table', 'orders',
            'oid', 'orders'::regclass::oid,
            'old', jsonb_build_object('id', 999999, 'customer', 'forged')
        )::text
    )::text")
mapfile -t MALICIOUS_LINES <<< "$MALICIOUS_OUTPUT"
MALICIOUS_XID="${MALICIOUS_LINES[0]}"
MALICIOUS_LSN="${MALICIOUS_LINES[1]}"
for _ in $(seq 1 100); do
    [[ "$(q "SELECT confirmed_flush_lsn >= '$MALICIOUS_LSN'::pg_lsn
               FROM pg_replication_slots
               WHERE slot_name='pg_flashback_${DB}'")" == "t" ]] && break
    sleep 0.1
done
assert_eq "PUBLIC logical-message gövdesi delta geçmişini zehirleyemedi" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE source_xid=$MALICIOUS_XID")"
assert_eq "sahte marker trusted commit ledger'ına girmedi" "0" \
    "$(q "SELECT count(*) FROM flashback.capture_commits WHERE source_xid=$MALICIOUS_XID")"

WORKER_DB=""
for _ in $(seq 1 20); do
    WORKER_DB=$(qp "SELECT datname FROM pg_stat_activity
                    WHERE backend_type = 'pg_flashback delta worker' AND datname = '$DB'")
    [[ "$WORKER_DB" == "$DB" ]] && break
    sleep 0.5
done
assert_eq "worker bu veritabanına bağlı" "$DB" "$WORKER_DB"
assert_eq "maintenance ayrı worker process'inde" "1" \
    "$(qp "SELECT count(*) FROM pg_stat_activity
           WHERE backend_type = 'pg_flashback maintenance worker'
             AND datname = '$DB'")"

echo "━━━ 1b. Lifecycle lock çakışması slot ilerlemesini rollback ediyor ━━━"
q "CREATE TABLE consume_lock_probe(id integer PRIMARY KEY, payload text NOT NULL)" > /dev/null
q "SELECT flashback_track('consume_lock_probe')" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT health FROM flashback_health()
               WHERE table_name='public.consume_lock_probe'")" == "healthy" ]] && break
    sleep 0.1
done
CONSUME_LOCK_TRACKING_ID=$(q "SELECT tracking_id FROM flashback.tracked_tables
                               WHERE table_name='consume_lock_probe' AND is_active")
PGAPPNAME=pgfb_consume_rollback $PSQL -d "$DB" -qAt \
    > /tmp/pg_flashback_consume_lock.out 2>&1 <<SQL &
BEGIN;
SELECT pg_advisory_xact_lock(358944::integer, hashint8($CONSUME_LOCK_TRACKING_ID));
SELECT pg_sleep(5);
COMMIT;
SQL
CONSUME_LOCK_PID=$!
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM pg_locks l
               JOIN pg_stat_activity a USING (pid)
               WHERE a.application_name='pgfb_consume_rollback'
                 AND l.locktype='advisory' AND l.classid=358944
                 AND l.granted")" == "1" ]] && break
    sleep 0.05
done
assert_eq "test lifecycle lock'u tutuluyor" "1" \
    "$(q "SELECT count(*) FROM pg_locks l
           JOIN pg_stat_activity a USING (pid)
           WHERE a.application_name='pgfb_consume_rollback'
             AND l.locktype='advisory' AND l.classid=358944
             AND l.granted")"
CONSUME_LOCK_XID=$(q "INSERT INTO consume_lock_probe VALUES (1, 'must-retry');
                       SELECT txid_current()::text")
CONSUME_LOCK_TARGET=$(q "SELECT pg_current_wal_flush_lsn()")
sleep 1
assert_eq "busy lifecycle sırasında decoded olay commit edilmedi" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE source_xid=$CONSUME_LOCK_XID")"
assert_eq "busy lifecycle sırasında slot hedef commit'i geçmedi" "t" \
    "$(q "SELECT confirmed_flush_lsn < '$CONSUME_LOCK_TARGET'::pg_lsn
           FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'")"
wait "$CONSUME_LOCK_PID"
CONSUME_LOCK_PID=""
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
               WHERE source_xid=$CONSUME_LOCK_XID")" == "1" ]] && break
    sleep 0.1
done
assert_eq "lock bırakılınca aynı olay kayıpsız retry edildi" "1" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE source_xid=$CONSUME_LOCK_XID")"
q "SELECT flashback_untrack('consume_lock_probe'); DROP TABLE consume_lock_probe" > /dev/null

echo "━━━ 2. Commit edilen DML'in worker tarafından tüketilmesi ━━━"
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.snapshots WHERE rel_oid='wal_batch_probe'::regclass")" -ge 1 ]] && break
    sleep 0.1
done

# Measure the WAL cost that the release contract must disclose.  CHECKPOINT
# before each identical workload makes full-page-image conditions comparable.
q "CHECKPOINT" > /dev/null
IFS='|' read -r AMP_DEFAULT_XID AMP_DEFAULT_WAL <<< "$(q "SELECT * FROM wal_amp_update('wal_amp_default')")"

q "CHECKPOINT" > /dev/null
IFS='|' read -r AMP_FULL_XID AMP_FULL_WAL <<< "$(q "SELECT * FROM wal_amp_update('wal_amp_full')")"

q "CHECKPOINT" > /dev/null
AMP_TOTAL_START=$(q "SELECT pg_current_wal_insert_lsn()")

# Regression for the decoded-commit lookup.  Without a materialized commit
# map, PostgreSQL scans the whole decoded batch for every row (O(n^2)); this
# exact 2,000-row/1 KiB transaction took ~27 seconds on the qualification host.
BATCH_STARTED_NS=$(date +%s%N)
IFS='|' read -r AMP_TRACKED_XID AMP_TRACKED_APP_WAL <<< \
    "$(q "SELECT * FROM wal_amp_update('wal_batch_probe')")"
BATCH_CAPTURED=0
for _ in $(seq 1 100); do
    BATCH_CAPTURED=$(q "SELECT count(*) FROM flashback.delta_log
                         WHERE rel_oid='wal_batch_probe'::regclass AND event_type='UPDATE'")
    [[ "$BATCH_CAPTURED" == "2000" ]] && break
    sleep 0.1
done
assert_eq "2.000 satırlık WAL transaction 10 saniyede tüketildi" "2000" "$BATCH_CAPTURED"
assert_eq "izlenmeyen DEFAULT tablo decoder ledger'ına girmedi" "0" \
    "$(q "SELECT count(*) FROM flashback.capture_commits WHERE source_xid=$AMP_DEFAULT_XID")"
assert_eq "izlenmeyen RI FULL tablo decoder ledger'ına girmedi" "0" \
    "$(q "SELECT count(*) FROM flashback.capture_commits WHERE source_xid=$AMP_FULL_XID")"
BATCH_ELAPSED_MS=$((($(date +%s%N) - BATCH_STARTED_NS) / 1000000))
AMP_TOTAL_END=$(q "SELECT pg_current_wal_insert_lsn()")
AMP_TRACKED_TOTAL_WAL=$(q "SELECT pg_wal_lsn_diff('$AMP_TOTAL_END', '$AMP_TOTAL_START')::bigint")
AMP_RI_RATIO=$(awk -v full="$AMP_FULL_WAL" -v base="$AMP_DEFAULT_WAL" \
    'BEGIN { if (base == 0) print "n/a"; else printf "%.2f", full / base }')
AMP_TOTAL_RATIO=$(awk -v total="$AMP_TRACKED_TOTAL_WAL" -v base="$AMP_DEFAULT_WAL" \
    'BEGIN { if (base == 0) print "n/a"; else printf "%.2f", total / base }')
if ! awk -v full="$AMP_FULL_WAL" -v base="$AMP_DEFAULT_WAL" \
    'BEGIN { exit !(full > base) }'; then
    echo "FAIL: REPLICA IDENTITY FULL WAL maliyeti baseline'dan büyük ölçülmedi"
    exit 1
fi
echo "  ok: WAL amplification — default=${AMP_DEFAULT_WAL} B, RI_FULL=${AMP_FULL_WAL} B (${AMP_RI_RATIO}x), tracked-total=${AMP_TRACKED_TOTAL_WAL} B (${AMP_TOTAL_RATIO}x), tracked-xid=${AMP_TRACKED_XID}, tracked-app-wal=${AMP_TRACKED_APP_WAL} B"
echo "  ok: büyük transaction capture süresi = ${BATCH_ELAPSED_MS}ms"

q "INSERT INTO orders (customer, amount) SELECT 'cust_'||g, g*1.5 FROM generate_series(1,1000) g" > /dev/null
q "UPDATE orders SET amount = amount + 100 WHERE id <= 10" > /dev/null
q "INSERT INTO orders (customer, amount) VALUES ('yeni_musteri', 999.99)" > /dev/null
$PSQL -d "$DB" -qc 'INSERT INTO "we""ird" VALUES (1, '"'"'NaN'"'"'), (2, 42.5)' > /dev/null
sleep 2

T_MID=$(q "SELECT clock_timestamp()")
sleep 1

q "DELETE FROM orders WHERE id <= 500" > /dev/null
sleep 2

assert_eq "INSERT olay sayısı" "1003" "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='INSERT'")"
assert_eq "orders UPDATE olay sayısı" "10"   "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='orders'::regclass AND event_type='UPDATE'")"
assert_eq "orders DELETE olay sayısı" "500"  "$(q "SELECT count(*) FROM flashback.delta_log WHERE rel_oid='orders'::regclass AND event_type='DELETE'")"
assert_eq "LSN'siz olay sayısı" "0"   "$(q "SELECT count(*) FROM flashback.delta_log WHERE lsn IS NULL")"
assert_eq "COMMIT-LSN'siz qualified olay sayısı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE tracking_id IS NOT NULL AND commit_lsn IS NULL")"
assert_eq "generation/stream bağsız qualified olay sayısı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE tracking_id IS NOT NULL AND (generation_id IS NULL OR stream_id IS NULL)")"
assert_eq "NaN kayıpsız yakalandı" "NaN" \
    "$(q "SELECT new_data->>'amount' FROM flashback.delta_log WHERE table_name LIKE '%ird%' AND new_data->>'id' = '1'")"

# The table owner has no direct access to internal routines or the protected
# pending table. The hook temporarily switches only its internal SPI call to
# the extension owner and restores the caller before the DDL returns.
DDL_XID=$(q "SET ROLE wal_e2e_app;
    SELECT txid_current()::text;
    ALTER TABLE public.ddl_probe SET (autovacuum_enabled = true)")
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
               WHERE source_xid=$DDL_XID AND event_type='ALTER'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "normal tablo sahibi DDL'i korumalı pending hattından yakalandı" "1" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE source_xid=$DDL_XID AND event_type='ALTER'")"
assert_eq "işlenen DDL pending payload bırakmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.pending_wal_events WHERE source_xid=$DDL_XID")"
assert_eq "DDL schema version gerçek COMMIT-LSN ile damgalandı" "1" \
    "$(q "SELECT count(*) FROM flashback.schema_versions
           WHERE rel_oid='ddl_probe'::regclass
             AND source_xid=$DDL_XID AND commit_lsn IS NOT NULL")"

echo "━━━ 3. Gerçek commit zamanı damgası (PITR doğruluğu) ━━━"
# T_MID'den önce commit olan UPDATE'ler T_MID'den önce, sonra commit olan
# DELETE'ler T_MID'den sonra damgalanmış olmalı — tüketim anı değil.
assert_eq "UPDATE'ler T_MID'den önce damgalı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='UPDATE' AND committed_at >= '$T_MID'")"
assert_eq "DELETE'ler T_MID'den sonra damgalı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='DELETE' AND committed_at <= '$T_MID'")"

echo "━━━ 4. Timestamp yalnız kanıtlanmış WAL prefix'ine çözülüyor ━━━"
RESOLVED_LSN=$(q "SELECT resolved_lsn FROM flashback_resolve_target('orders', '$T_MID')")
[[ -n "$RESOLVED_LSN" ]] || { echo "FAIL: timestamp resolver LSN döndürmedi"; exit 1; }
assert_eq "resolver sonucu canonical admission'dan geçiyor" "1" \
    "$(q "SELECT count(*) FROM flashback_admit_lsn_target('orders', '$RESOLVED_LSN')")"
assert_eq "query_lsn felaket öncesi 1001 satır görüyor" "1001" \
    "$(q "SELECT count(*) FROM flashback_query_lsn('orders', '$RESOLVED_LSN') AS t(id int, customer text, amount numeric(10,2))")"
FILTER_RC=0
$PSQL -d "$DB" -qc "SELECT * FROM flashback_query_lsn('orders', '$RESOLVED_LSN', 'true OR pg_sleep(1) IS NULL') AS t(id int, customer text, amount numeric(10,2))" \
    > /tmp/pg_flashback_filter_reject.out 2>&1 || FILTER_RC=$?
[[ "$FILTER_RC" != "0" ]] || { echo "FAIL: SECURITY DEFINER filter_clause reddedilmedi"; exit 1; }
grep -q "filter_clause is not supported" /tmp/pg_flashback_filter_reject.out
echo "  ok: SECURITY DEFINER serbest predicate fail-closed"

echo "━━━ 4a. Silinen satırları non-destructive geri getir ━━━"
assert_eq "recover_deleted_lsn 500 satır döndürdü" "500" \
    "$(q "SELECT flashback_recover_deleted_lsn('orders', '$RESOLVED_LSN')")"
assert_eq "recover sonrası satır sayısı" "1001" "$(q "SELECT count(*) FROM orders")"
q "DELETE FROM orders WHERE id <= 500" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM orders")" == "501" ]] && break
    sleep 0.1
done

echo "━━━ 4b. Felaket-öncesi COMMIT LSN'e full restore ━━━"
# Freeze the worker only while it is idle and owns no advisory key, then commit
# one old-OID UPDATE before the restore swaps the physical table. On
# resume the decoder must attach that buffered WAL to the immutable predecessor
# generation, not discard it because tracked_tables now points at the new OID.
STOPPED_WORKER_PID=$(stop_idle_worker)
[[ -n "$STOPPED_WORKER_PID" ]] || { echo "FAIL: backlog testi worker PID bulamadı"; exit 1; }
BUFFERED_OLD_OID_XID=$(q "BEGIN;
    SELECT txid_current()::text;
    UPDATE orders SET amount=amount+7 WHERE id=1001;
    COMMIT")

# Slot advancement cannot commit inside the restore transaction. With the
# worker deliberately stopped, restore must fail without swapping either table
# instead of looping over or discarding the same logical prefix.
RESTORE_BLOCKED_RC=0
$PSQL -d "$DB" -qc "SELECT flashback_restore_lsn(
       ARRAY['orders', '\"we\"\"ird\"'], '$RESOLVED_LSN'
   )" > /tmp/pg_flashback_restore_backlog.out 2>&1 || RESTORE_BLOCKED_RC=$?
[[ "$RESTORE_BLOCKED_RC" != "0" ]] || {
    echo "FAIL: restore buffered relation WAL varken fail-closed davranmadı"; exit 1; }
grep -Eq "logical slot is .* behind|committed WAL .* is still pending" \
    /tmp/pg_flashback_restore_backlog.out
assert_eq "reddedilen restore canlı tabloyu değiştirmedi" "501" \
    "$(q "SELECT count(*) FROM orders")"

kill -CONT "$STOPPED_WORKER_PID"
STOPPED_WORKER_PID=""
for _ in $(seq 1 200); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE source_xid=$BUFFERED_OLD_OID_XID")" == "1" ]] && break
    sleep 0.05
done
assert_eq "worker retry öncesi buffered eski-OID WAL'ı tüketti" "1" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE source_xid=$BUFFERED_OLD_OID_XID")"

q "SELECT flashback_restore_lsn(
       ARRAY['orders', '\"we\"\"ird\"'], '$RESOLVED_LSN'
   )" > /dev/null

assert_eq "restore sonrası satır sayısı" "1001" "$(q "SELECT count(*) FROM orders")"
assert_eq "yeni_musteri kurtarıldı" "1" "$(q "SELECT count(*) FROM orders WHERE customer='yeni_musteri'")"
assert_eq "güncellenmiş tutarlar korundu" "115.00" "$(q "SELECT max(amount) FROM orders WHERE id <= 10")"
# WAL modunda restore capture trigger'ı GERİ TAKMAMALI (staging'i kimse boşaltmaz)
assert_eq "restore sonrası capture trigger yok" "0" \
    "$(q "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'orders'::regclass AND tgname LIKE 'flashback_capture_%'")"
assert_eq "restore REPLICA IDENTITY FULL korudu" "f" \
    "$(q "SELECT relreplident FROM pg_class WHERE oid = 'orders'::regclass")"
assert_eq "multi-table LSN restore ikinci tabloyu da korudu" "2" \
    "$(q 'SELECT count(*) FROM "we""ird"')"

for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='active'")" == "1" \
       && "$(q "SELECT count(*) FROM flashback.delta_log WHERE source_xid=$BUFFERED_OLD_OID_XID")" == "1" \
       && "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='building'")" == "0" ]] && break
    sleep 0.1
done
assert_eq "post-restore successor aktif" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='active' AND cg.generation_no=2")"
assert_eq "post-restore pending generation kalmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='building'")"
assert_eq "restore öncesi buffered eski-OID WAL predecessor generation'a bağlandı" "1" \
    "$(q "SELECT count(*)
           FROM flashback.delta_log d
           JOIN flashback.coverage_generations cg
             ON cg.generation_id=d.generation_id
            AND cg.tracking_id=d.tracking_id
           JOIN flashback.tracked_tables tt
             ON tt.tracking_id=d.tracking_id
           WHERE d.source_xid=$BUFFERED_OLD_OID_XID
             AND d.event_type='UPDATE'
             AND cg.state='sealed'
             AND d.rel_oid=cg.rel_oid_at_boundary
             AND d.rel_oid<>tt.rel_oid")"

LEGACY_RC=0
$PSQL -d "$DB" -qc "SELECT flashback_restore('orders', '$T_MID')" > /tmp/pg_flashback_legacy_reject.out 2>&1 || LEGACY_RC=$?
[[ "$LEGACY_RC" != "0" ]] || { echo "FAIL: legacy timestamp restore qualified generation'da reddedilmedi"; exit 1; }
grep -q "disabled for correctness-qualified WAL coverage" /tmp/pg_flashback_legacy_reject.out
echo "  ok: legacy timestamp restore fail-closed"

echo "━━━ 4c. Generation retention: durable intent, kill, idempotent resume ━━━"
RETIRE_GENERATION=$(q "SELECT cg.generation_id
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='orders'
      AND cg.state='sealed'
      AND cg.valid_through_lsn >= cg.superseded_before_lsn
    ORDER BY cg.generation_no
    LIMIT 1")
[[ -n "$RETIRE_GENERATION" ]] || { echo "FAIL: retire edilebilir sealed orders generation bulunamadı"; exit 1; }
IFS='|' read -r RETIRE_SNAPSHOT RETIRE_DELTA_ROWS RETIRE_SCHEMA_ROWS <<< "$(q "
    SELECT snap.snapshot_table,
           (SELECT count(*) FROM flashback.delta_log d
            WHERE d.generation_id=cg.generation_id),
           (SELECT count(*) FROM flashback.schema_versions sv
            WHERE sv.generation_id=cg.generation_id)
    FROM flashback.coverage_generations cg
    JOIN flashback.snapshots snap
      ON snap.snapshot_id=cg.boundary_snapshot_id
     AND snap.tracking_id=cg.tracking_id
    WHERE cg.generation_id=$RETIRE_GENERATION")"
RETIRE_ACTIVE_TARGET=$(q "SELECT cg.boundary_lsn
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='orders' AND cg.state='active'")

# Keep the background worker's maintenance branch paused with an unrelated
# lifecycle-class advisory key. Manual retention calls use the real orders key
# and remain unblocked; this makes the crash window deterministic.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_retention_pause.out 2>&1 <<'SQL' &
SET application_name = 'pgfb_retention_pause';
SELECT pg_advisory_lock(358944::integer, 2147483000::integer);
SELECT pg_sleep(60);
SQL
RETENTION_PAUSE_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_locks l
               JOIN pg_stat_activity a ON a.pid=l.pid
               WHERE l.locktype='advisory' AND l.classid=358944
                 AND l.objid=2147483000 AND l.granted
                 AND a.application_name='pgfb_retention_pause'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "worker maintenance retention testi boyunca duraklatıldı" "1" \
    "$(q "SELECT count(*) FROM pg_locks l
           JOIN pg_stat_activity a ON a.pid=l.pid
           WHERE l.locktype='advisory' AND l.classid=358944
             AND l.objid=2147483000 AND l.granted
             AND a.application_name='pgfb_retention_pause'")"

q "UPDATE flashback.tracked_tables
      SET retention_interval=interval '0 seconds'
    WHERE table_name='orders'" > /dev/null

# Admission holds the stable lifecycle pin for its whole transaction. A
# retirement intent must wait behind that pin and must not become visible
# while a query/recover/restore caller can still materialize the generation.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_retention_pin.out 2>&1 <<SQL &
SET application_name = 'pgfb_retention_pin';
BEGIN;
SELECT count(*) FROM flashback_admit_lsn_target('orders', '$RESOLVED_LSN');
SELECT pg_sleep(60);
COMMIT;
SQL
RETENTION_PIN_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_locks l
               JOIN pg_stat_activity a ON a.pid=l.pid
               WHERE l.locktype='advisory' AND l.classid=358944
                 AND l.granted AND a.application_name='pgfb_retention_pin'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "admission transaction generation payload'ını pinledi" "1" \
    "$(q "SELECT count(*) FROM pg_locks l
           JOIN pg_stat_activity a ON a.pid=l.pid
           WHERE l.locktype='advisory' AND l.classid=358944
             AND l.granted AND a.application_name='pgfb_retention_pin'")"

PGAPPNAME=pgfb_retention_begin $PSQL -d "$DB" -qc \
    "SELECT flashback_begin_generation_retirement($RETIRE_GENERATION)" \
    > /tmp/pg_flashback_retention_begin.out 2>&1 &
RETENTION_BEGIN_PID=$!
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM pg_stat_activity
               WHERE application_name='pgfb_retention_begin'
                 AND wait_event_type='Lock'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "retirement pinned admission bitene kadar bekledi" "1" \
    "$(q "SELECT count(*) FROM pg_stat_activity
           WHERE application_name='pgfb_retention_begin'
             AND wait_event_type='Lock'")"
assert_eq "pin açıkken retirement intent görünmedi" "0" \
    "$(q "SELECT count(*) FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION")"

kill "$RETENTION_PIN_PID" > /dev/null 2>&1 || true
# Closing the psql client does not reliably interrupt a backend currently in
# pg_sleep(): PostgreSQL may not observe the socket EOF until the sleep timer
# fires. Terminate the server backend explicitly so the lifecycle advisory
# lock is released immediately and the waiting retirement call can resume.
RETENTION_PIN_BACKEND=$(q "SELECT pid FROM pg_stat_activity
    WHERE application_name='pgfb_retention_pin' LIMIT 1")
if [[ -n "$RETENTION_PIN_BACKEND" ]]; then
    q "SELECT pg_terminate_backend($RETENTION_PIN_BACKEND)" > /dev/null
fi
wait "$RETENTION_PIN_PID" 2>/dev/null || true
RETENTION_PIN_PID=""
set +e
wait "$RETENTION_BEGIN_PID"
RETENTION_BEGIN_RC=$?
set -e
if [[ "$RETENTION_BEGIN_RC" != "0" ]]; then
    echo "FAIL: retirement begin backend exited with rc=$RETENTION_BEGIN_RC"
    sed -n '1,120p' /tmp/pg_flashback_retention_begin.out >&2 || true
    exit 1
fi
RETENTION_BEGIN_PID=""
assert_eq "pin bırakılınca durable intent commit edildi" "retiring" \
    "$(q "SELECT state FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "intent sonrası generation henüz sealed" "sealed" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "intent sonrası snapshot fiziksel olarak duruyor" "t" \
    "$(q "SELECT to_regclass('$RETIRE_SNAPSHOT') IS NOT NULL")"

RETIRE_ADMIT_RC=0
$PSQL -d "$DB" -qc "SELECT * FROM flashback_admit_lsn_target('orders', '$RESOLVED_LSN')" \
    > /tmp/pg_flashback_retention_admit.out 2>&1 || RETIRE_ADMIT_RC=$?
[[ "$RETIRE_ADMIT_RC" != "0" ]] || { echo "FAIL: retiring generation admission'a açık kaldı"; exit 1; }
grep -q "owned by 0 eligible generations" /tmp/pg_flashback_retention_admit.out
assert_eq "retiring predecessor kapanırken active successor kullanılabilir" "1" \
    "$(q "SELECT count(*) FROM flashback_admit_lsn_target('orders', '$RETIRE_ACTIVE_TARGET')")"

# Block the snapshot DROP, start the resume transaction, then terminate that
# backend. PostgreSQL must roll back every destructive step while the already
# committed retirement intent remains available for the next cycle.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_retention_blocker.out 2>&1 <<SQL &
SET application_name = 'pgfb_retention_snapshot_blocker';
BEGIN;
LOCK TABLE $RETIRE_SNAPSHOT IN ACCESS SHARE MODE;
SELECT pg_sleep(60);
COMMIT;
SQL
RETENTION_BLOCKER_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_stat_activity
               WHERE application_name='pgfb_retention_snapshot_blocker'")" == "1" ]] && break
    sleep 0.1
done

PGAPPNAME=pgfb_retention_resume $PSQL -d "$DB" -qc "SELECT flashback_apply_retention()" \
    > /tmp/pg_flashback_retention_resume.out 2>&1 &
RETENTION_RESUME_PID=$!
RETENTION_RESUME_BACKEND=""
for _ in $(seq 1 100); do
    RETENTION_RESUME_BACKEND=$(q "SELECT pid FROM pg_stat_activity
        WHERE application_name='pgfb_retention_resume'
          AND wait_event_type='Lock'")
    [[ -n "$RETENTION_RESUME_BACKEND" ]] && break
    sleep 0.1
done
[[ -n "$RETENTION_RESUME_BACKEND" ]] || { echo "FAIL: retention resume snapshot lock'unda beklemedi"; exit 1; }
q "SELECT pg_terminate_backend($RETENTION_RESUME_BACKEND)" > /dev/null
if wait "$RETENTION_RESUME_PID"; then
    echo "FAIL: terminate edilen retention resume backend başarı döndürdü"
    exit 1
fi
RETENTION_RESUME_PID=""
kill "$RETENTION_BLOCKER_PID" > /dev/null 2>&1 || true
RETENTION_BLOCKER_BACKEND=$(q "SELECT pid FROM pg_stat_activity
    WHERE application_name='pgfb_retention_snapshot_blocker' LIMIT 1")
if [[ -n "$RETENTION_BLOCKER_BACKEND" ]]; then
    q "SELECT pg_terminate_backend($RETENTION_BLOCKER_BACKEND)" > /dev/null
fi
wait "$RETENTION_BLOCKER_PID" 2>/dev/null || true
RETENTION_BLOCKER_PID=""

assert_eq "kesinti sonrası intent resumable kaldı" "retiring" \
    "$(q "SELECT state FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "kesinti snapshot DROP'unu rollback etti" "t" \
    "$(q "SELECT to_regclass('$RETIRE_SNAPSHOT') IS NOT NULL")"
assert_eq "kesinti delta payload'ını rollback etti" "$RETIRE_DELTA_ROWS" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "kesinti schema payload'ını rollback etti" "$RETIRE_SCHEMA_ROWS" \
    "$(q "SELECT count(*) FROM flashback.schema_versions
           WHERE generation_id=$RETIRE_GENERATION")"

# Policy B: cleanup verifies identity/ownership/need, NOT content. Drift in a
# payload we are about to discard must not wedge retention (the old full
# COUNT(*) verification turned any stray row into unbounded disk growth).
# The intent-time row count stays as immutable forensic evidence.
SNAPSHOT_EVIDENCE_ROWS=$(q "SELECT snapshot_row_count
    FROM flashback.generation_payload_retirements
    WHERE generation_id=$RETIRE_GENERATION")
q "INSERT INTO $RETIRE_SNAPSHOT (id, customer, amount)
   VALUES (2000000, 'content_drift_probe', 0)" > /dev/null

q "SELECT flashback_apply_retention()" > /dev/null
assert_eq "içerik kayması cleanup'ı bloke etmedi (forensik kanıt intent-anı değerinde)" \
    "$SNAPSHOT_EVIDENCE_ROWS" \
    "$(q "SELECT snapshot_row_count FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "resume audit'i removed yaptı" "removed" \
    "$(q "SELECT state FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "resume generation audit'ini retired yaptı" "retired" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "resume snapshot payload'ını kaldırdı" "f" \
    "$(q "SELECT to_regclass('$RETIRE_SNAPSHOT') IS NOT NULL")"
assert_eq "resume delta payload'ını bütünüyle kaldırdı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log
           WHERE generation_id=$RETIRE_GENERATION")"
assert_eq "resume schema payload'ını bütünüyle kaldırdı" "0" \
    "$(q "SELECT count(*) FROM flashback.schema_versions
           WHERE generation_id=$RETIRE_GENERATION")"
q "SELECT flashback_apply_retention()" > /dev/null
assert_eq "tekrar cleanup immutable audit'i çoğaltmadı" "1" \
    "$(q "SELECT count(*) FROM flashback.generation_payload_retirements
           WHERE generation_id=$RETIRE_GENERATION AND state='removed'")"

q "UPDATE flashback.tracked_tables
      SET retention_interval=interval '7 days'
    WHERE table_name='orders'" > /dev/null

kill "$RETENTION_PAUSE_PID" > /dev/null 2>&1 || true
RETENTION_PAUSE_BACKEND=$(q "SELECT pid FROM pg_stat_activity
    WHERE application_name='pgfb_retention_pause' LIMIT 1")
if [[ -n "$RETENTION_PAUSE_BACKEND" ]]; then
    q "SELECT pg_terminate_backend($RETENTION_PAUSE_BACKEND)" > /dev/null
fi
wait "$RETENTION_PAUSE_PID" 2>/dev/null || true
RETENTION_PAUSE_PID=""

echo "━━━ 4d. Slot kaybı coverage'ı donduruyor; explicit re-anchor yeni epoch açıyor ━━━"
OLD_STREAM_ID=$(q "SELECT cg.stream_id
                    FROM flashback.coverage_generations cg
                    JOIN flashback.tracked_tables tt USING (tracking_id)
                    WHERE tt.table_name='orders' AND cg.state='active'")
OLD_GENERATION_ID=$(q "SELECT cg.generation_id
                        FROM flashback.coverage_generations cg
                        JOIN flashback.tracked_tables tt USING (tracking_id)
                        WHERE tt.table_name='orders' AND cg.state='active'")
OLD_FRONTIER=$(q "SELECT valid_through_lsn
                   FROM flashback.coverage_generations
                   WHERE generation_id=$OLD_GENERATION_ID")

# Hold the database-stream key between worker transactions. This makes the slot
# deterministically inactive without terminating the registered worker (a
# graceful termination is not guaranteed to auto-restart and made this test
# flaky). Releasing the lock lets that same worker persist the missing-slot
# incident.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_slot_loss_lock.out 2>&1 <<'SQL' &
SET application_name = 'pgfb_slot_loss_holder';
SELECT pg_advisory_lock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SELECT pg_sleep(3);
SELECT pg_advisory_unlock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SQL
SLOT_LOSS_LOCK_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_locks l
               JOIN pg_stat_activity a ON a.pid=l.pid
               WHERE l.locktype='advisory' AND l.classid=358945
                 AND l.granted
                 AND a.application_name='pgfb_slot_loss_holder'")" == "1" \
       && "$(q "SELECT active_pid IS NULL FROM pg_replication_slots
                 WHERE slot_name='pg_flashback_${DB}'")" == "t" ]] && break
    sleep 0.1
done
SLOT_DROPPED=0
if q "SELECT pg_drop_replication_slot('pg_flashback_${DB}')" > /dev/null 2>&1; then
    SLOT_DROPPED=1
fi
if ! wait "$SLOT_LOSS_LOCK_PID"; then
    echo "FAIL: slot-loss stream lock holder başarısız"
    exit 1
fi
assert_eq "logical slot düşürüldü" "1" "$SLOT_DROPPED"

for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.capture_streams
               WHERE stream_id=$OLD_STREAM_ID AND state='broken'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "eski stream missing-slot ile kırıldı" "replication_slot_missing" \
    "$(q "SELECT invalidation_reason FROM flashback.capture_streams
           WHERE stream_id=$OLD_STREAM_ID")"
assert_eq "health slot kaybını slot_lost gösteriyor" "slot_lost" \
    "$(q "SELECT health FROM flashback_health()
           WHERE table_name='public.orders'")"
assert_eq "kırık stream'in kanıtlanmış eski hedefi hâlâ okunabilir" "1" \
    "$(q "SELECT count(*) FROM flashback_admit_lsn_target('orders', '$OLD_FRONTIER')")"

# This committed update is intentionally outside every logical slot.  The
# exact LSN sampled immediately afterward must remain a rejected interval even
# after a later re-anchor.
q "UPDATE orders SET amount=amount+7 WHERE id=1001" > /dev/null
MISSING_INTERVAL_LSN=$(q "SELECT pg_current_wal_insert_lsn()")

q "SELECT pg_create_logical_replication_slot('pg_flashback_${DB}', 'pg_flashback')" > /dev/null
REANCHOR_GENERATION=$(q "SELECT flashback_reanchor('orders')")
[[ -n "$REANCHOR_GENERATION" ]] || { echo "FAIL: flashback_reanchor generation döndürmedi"; exit 1; }

for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.coverage_generations
               WHERE generation_id=$REANCHOR_GENERATION AND state='active'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "re-anchor successor gerçek COMMIT-LSN ile aktif" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations
           WHERE generation_id=$REANCHOR_GENERATION
             AND state='active' AND boundary_lsn IS NOT NULL")"
assert_eq "re-anchor yeni stream epoch kullandı" "1" \
    "$(q "SELECT count(*)
           FROM flashback.coverage_generations
           WHERE generation_id=$REANCHOR_GENERATION
             AND stream_id <> $OLD_STREAM_ID")"
assert_eq "eski generation watermark'ı gap üzerinden ileri uydurulmadı" "$OLD_FRONTIER" \
    "$(q "SELECT valid_through_lsn FROM flashback.coverage_generations
           WHERE generation_id=$OLD_GENERATION_ID")"
assert_eq "kalıcı gap yeni generation ile kapatıldı ama audit kaldı" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_gaps
           WHERE source_generation_id=$OLD_GENERATION_ID
             AND reanchored_by_generation_id=$REANCHOR_GENERATION
             AND gap_end_lsn IS NOT NULL")"

GAP_RC=0
$PSQL -d "$DB" -qc "SELECT * FROM flashback_admit_lsn_target('orders', '$MISSING_INTERVAL_LSN')" \
    > /tmp/pg_flashback_gap_reject.out 2>&1 || GAP_RC=$?
[[ "$GAP_RC" != "0" ]] || { echo "FAIL: slot kaybı aralığındaki LSN kabul edildi"; exit 1; }
grep -q "owned by 0 eligible generations" /tmp/pg_flashback_gap_reject.out
echo "  ok: slot kaybı ile re-anchor arasındaki kalıcı boşluk reddedildi"

q "UPDATE orders SET amount=amount+1 WHERE id=1001" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log d
               WHERE d.generation_id=$REANCHOR_GENERATION
                 AND d.event_type='UPDATE'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "re-anchor sonrası DML yeni generation'a bağlandı" "1" \
    "$(q "SELECT count(*) FROM flashback.delta_log d
           WHERE d.generation_id=$REANCHOR_GENERATION
             AND d.event_type='UPDATE'")"

echo "━━━ 4e. Harici slot ilerletme sessiz devam etmiyor ━━━"
q "CREATE TABLE external_advance_probe (id int PRIMARY KEY)" > /dev/null
EXTERNAL_OLD_STREAM=$(q "SELECT stream_id FROM flashback.capture_streams
                          WHERE state='active'")
EXTERNAL_OLD_GENERATION=$REANCHOR_GENERATION
EXTERNAL_OLD_FRONTIER=$(q "SELECT valid_through_lsn
                            FROM flashback.coverage_generations
                            WHERE generation_id=$EXTERNAL_OLD_GENERATION")

# Hold the worker's database-stream advisory key while another connection
# commits a user-table change and consumes it directly from the slot.  When
# the lock is released, pg_flashback must notice confirmed_flush_lsn drift.
$PSQL -d "$DB" -qAt > /tmp/pg_flashback_external_lock.out 2>&1 <<'SQL' &
SELECT pg_advisory_lock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SELECT pg_sleep(5);
SELECT pg_advisory_unlock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SQL
LOCK_HOLDER_PID=$!
for _ in $(seq 1 50); do
    [[ "$(q "SELECT count(*) FROM pg_locks
               WHERE locktype='advisory' AND classid=358945 AND granted")" -ge 1 ]] && break
    sleep 0.1
done
assert_eq "external-advance testi worker stream kilidini tuttu" "1" \
    "$(q "SELECT CASE WHEN count(*) >= 1 THEN 1 ELSE 0 END FROM pg_locks
           WHERE locktype='advisory' AND classid=358945 AND granted")"

q "INSERT INTO external_advance_probe VALUES (1)" > /dev/null
EXTERNAL_MISSING_LSN=$(q "SELECT pg_current_wal_insert_lsn()")
EXTERNAL_PROBE_OID=$(q "SELECT 'external_advance_probe'::regclass::oid")
EXTERNAL_ROWS=$(q "SELECT count(*) FROM pg_logical_slot_get_changes(
                     'pg_flashback_${DB}', NULL, NULL,
                     'tracked_oids', '$EXTERNAL_PROBE_OID')")
[[ "$EXTERNAL_ROWS" -ge 2 ]] || {
    echo "FAIL: harici consumer beklenen event+COMMIT çıktısını tüketmedi (satır=$EXTERNAL_ROWS)"
    exit 1
}
wait "$LOCK_HOLDER_PID"

for _ in $(seq 1 100); do
    [[ "$(q "SELECT invalidation_reason FROM flashback.capture_streams
               WHERE stream_id=$EXTERNAL_OLD_STREAM")" == "replication_slot_advanced_externally" ]] && break
    sleep 0.1
done
assert_eq "harici confirmed_flush ilerlemesi stream'i kırdı" \
    "replication_slot_advanced_externally" \
    "$(q "SELECT invalidation_reason FROM flashback.capture_streams
           WHERE stream_id=$EXTERNAL_OLD_STREAM")"
EXTERNAL_HEALTH="$(q "SELECT health FROM flashback_health()
                        WHERE table_name='public.orders'")"
# External slot consumers typically leave the slot missing/invalid; slot_lost
# is the stronger fail-closed signal (recreate slot + reanchor). Older builds
# projected reanchor_recommended for the same break.
[[ "$EXTERNAL_HEALTH" == "slot_lost" || "$EXTERNAL_HEALTH" == "reanchor_recommended" ]] \
    || { echo "FAIL: harici ilerleme sonrası health beklenen slot_lost|reanchor_recommended, bulunan=$EXTERNAL_HEALTH"; exit 1; }
echo "  ok: harici ilerleme sonrası health $EXTERNAL_HEALTH"

EXTERNAL_REANCHOR_GENERATION=$(q "SELECT flashback_reanchor('orders')")
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.coverage_generations
               WHERE generation_id=$EXTERNAL_REANCHOR_GENERATION
                 AND state='active'")" == "1" ]] && break
    sleep 0.1
done
assert_eq "external-advance sonrası re-anchor aktif" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations
           WHERE generation_id=$EXTERNAL_REANCHOR_GENERATION
             AND state='active'")"
assert_eq "external-advance eski watermark'ı ileri uydurmadı" "$EXTERNAL_OLD_FRONTIER" \
    "$(q "SELECT valid_through_lsn FROM flashback.coverage_generations
           WHERE generation_id=$EXTERNAL_OLD_GENERATION")"

EXTERNAL_GAP_RC=0
$PSQL -d "$DB" -qc "SELECT * FROM flashback_admit_lsn_target('orders', '$EXTERNAL_MISSING_LSN')" \
    > /tmp/pg_flashback_external_gap_reject.out 2>&1 || EXTERNAL_GAP_RC=$?
[[ "$EXTERNAL_GAP_RC" != "0" ]] || { echo "FAIL: harici slot ilerletme aralığı kabul edildi"; exit 1; }
grep -q "owned by 0 eligible generations" /tmp/pg_flashback_external_gap_reject.out
echo "  ok: harici tüketici aralığı kalıcı gap olarak reddedildi"

echo "━━━ 4f. untrack orijinal REPLICA IDENTITY'yi geri getiriyor ━━━"
$PSQL -d "$DB" -qc 'SELECT flashback_untrack('"'"'"we""ird"'"'"')' > /dev/null
assert_eq "untrack sonrası replica identity DEFAULT" "d" \
    "$(q "SELECT relreplident FROM pg_class WHERE relname = 'we\"ird'")"

echo "━━━ 4g. enabled/capture_mode SIGHUP geçişleri durable gap açıyor ━━━"
CONFIG_DISABLED_STREAM=$(q "SELECT cg.stream_id
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='orders' AND cg.state='active'")
CONFIG_DISABLED_GENERATION=$(q "SELECT cg.generation_id
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='orders' AND cg.state='active'")
qp "ALTER SYSTEM SET pg_flashback.enabled = 'off'" > /dev/null
qp "SELECT pg_reload_conf()" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT invalidation_reason FROM flashback.capture_streams
               WHERE stream_id=$CONFIG_DISABLED_STREAM")" == "capture_disabled" ]] && break
    sleep 0.1
done
assert_eq "enabled=off worker idling öncesi stream'i kırdı" "capture_disabled" \
    "$(q "SELECT invalidation_reason FROM flashback.capture_streams
           WHERE stream_id=$CONFIG_DISABLED_STREAM")"
assert_eq "enabled=off orders için tek LOGGED gap açtı" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_gaps
           WHERE source_generation_id=$CONFIG_DISABLED_GENERATION
             AND reason='capture_disabled'")"

q "UPDATE orders SET amount=amount+3 WHERE id=1001" > /dev/null
CONFIG_DISABLED_LSN=$(q "SELECT pg_current_wal_insert_lsn()")
qp "ALTER SYSTEM SET pg_flashback.enabled = 'on'" > /dev/null
qp "SELECT pg_reload_conf()" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.capture_streams
               WHERE state='active' AND stream_id <> $CONFIG_DISABLED_STREAM")" == "1" ]] && break
    sleep 0.1
done
CONFIG_ENABLED_REANCHOR=$(q "SELECT flashback_reanchor('orders')")
for _ in $(seq 1 100); do
    [[ "$(q "SELECT state FROM flashback.coverage_generations
               WHERE generation_id=$CONFIG_ENABLED_REANCHOR")" == "active" ]] && break
    sleep 0.1
done
assert_eq "enabled=on sonrası explicit re-anchor yeni epoch açtı" "active" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$CONFIG_ENABLED_REANCHOR")"
CONFIG_DISABLED_GAP_RC=0
$PSQL -d "$DB" -qc "SELECT * FROM flashback_admit_lsn_target('orders', '$CONFIG_DISABLED_LSN')" \
    > /tmp/pg_flashback_enabled_gap_reject.out 2>&1 || CONFIG_DISABLED_GAP_RC=$?
[[ "$CONFIG_DISABLED_GAP_RC" != "0" ]] || { echo "FAIL: enabled=off aralığındaki LSN kabul edildi"; exit 1; }
grep -q "owned by 0 eligible generations" /tmp/pg_flashback_enabled_gap_reject.out
echo "  ok: enabled=off ile re-anchor arasındaki boşluk kalıcı reddedildi"

CONFIG_MODE_STREAM=$(q "SELECT stream_id FROM flashback.capture_streams WHERE state='active'")
CONFIG_MODE_GENERATION=$CONFIG_ENABLED_REANCHOR
qp "ALTER SYSTEM SET pg_flashback.capture_mode = 'trigger'" > /dev/null
qp "SELECT pg_reload_conf()" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT invalidation_reason FROM flashback.capture_streams
               WHERE stream_id=$CONFIG_MODE_STREAM")" == "capture_mode_changed" ]] && break
    sleep 0.1
done
assert_eq "capture_mode=trigger stream'i senkron kırdı" "capture_mode_changed" \
    "$(q "SELECT invalidation_reason FROM flashback.capture_streams
           WHERE stream_id=$CONFIG_MODE_STREAM")"
assert_eq "capture_mode değişimi orders için tek LOGGED gap açtı" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_gaps
           WHERE source_generation_id=$CONFIG_MODE_GENERATION
             AND reason='capture_mode_changed'")"

CONFIG_DDL_RC=0
$PSQL -d "$DB" -qc "ALTER TABLE orders SET (autovacuum_enabled = false)" \
    > /tmp/pg_flashback_mode_ddl_reject.out 2>&1 || CONFIG_DDL_RC=$?
[[ "$CONFIG_DDL_RC" != "0" ]] || { echo "FAIL: broken stream üzerinde DDL kabul edildi"; exit 1; }
# Depending on whether the caller's session-local mode is reconciled before
# the durable stream-state check, the fail-closed guard reports either the
# stream state or the disabled/no-active-epoch reason. Both are the required
# invariant: no DDL may commit while the qualified WAL stream is broken.
grep -Eq "DDL capture refused because (WAL stream|capture configuration is disabled)" \
    /tmp/pg_flashback_mode_ddl_reject.out
echo "  ok: broken qualified stream üzerinde DDL fail-closed"

qp "ALTER SYSTEM SET pg_flashback.capture_mode = 'wal'" > /dev/null
qp "SELECT pg_reload_conf()" > /dev/null
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.capture_streams
               WHERE state='active' AND stream_id <> $CONFIG_MODE_STREAM")" == "1" ]] && break
    sleep 0.1
done
CONFIG_MODE_REANCHOR=$(q "SELECT flashback_reanchor('orders')")
for _ in $(seq 1 100); do
    [[ "$(q "SELECT state FROM flashback.coverage_generations
               WHERE generation_id=$CONFIG_MODE_REANCHOR")" == "active" ]] && break
    sleep 0.1
done
assert_eq "WAL moduna dönüş explicit re-anchor olmadan coverage uydurmadı" "active" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$CONFIG_MODE_REANCHOR")"

echo "━━━ 4h. İlk boundary'den önce slot kaybı taslağı abort ediyor; re-anchor kurtarıyor ━━━"
q "CREATE TABLE initial_abort_probe (id integer PRIMARY KEY, note text);
   INSERT INTO initial_abort_probe VALUES (1, 'base')" > /dev/null

STOPPED_WORKER_PID=$(stop_idle_worker)
[[ -n "$STOPPED_WORKER_PID" ]] || { echo "FAIL: initial-abort worker PID bulunamadı"; exit 1; }

q "SELECT flashback_track('initial_abort_probe')" > /dev/null
INITIAL_ABORT_GENERATION=$(q "SELECT cg.generation_id
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='initial_abort_probe' AND cg.state='building'")
[[ -n "$INITIAL_ABORT_GENERATION" ]] || { echo "FAIL: initial-abort building generation oluşmadı"; exit 1; }
q "SELECT pg_drop_replication_slot('pg_flashback_${DB}')" > /dev/null
kill -CONT "$STOPPED_WORKER_PID"
STOPPED_WORKER_PID=""

for _ in $(seq 1 100); do
    [[ "$(q "SELECT state FROM flashback.coverage_generations
               WHERE generation_id=$INITIAL_ABORT_GENERATION")" == "aborted" ]] && break
    sleep 0.1
done
assert_eq "boundary COMMIT görülmeyen generation audit tombstone oldu" "aborted" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$INITIAL_ABORT_GENERATION")"
assert_eq "aborted generation fiziksel snapshot bırakmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.snapshots s
           JOIN flashback.coverage_generations cg
             ON cg.boundary_snapshot_id=s.snapshot_id
            AND cg.tracking_id=s.tracking_id
           WHERE cg.generation_id=$INITIAL_ABORT_GENERATION
             AND (s.payload_state<>'missing' OR to_regclass(s.snapshot_table) IS NOT NULL)")"

q "SELECT pg_create_logical_replication_slot('pg_flashback_${DB}', 'pg_flashback')" > /dev/null
INITIAL_RECOVERY_GENERATION=$(q "SELECT flashback_reanchor('initial_abort_probe')")
for _ in $(seq 1 100); do
    [[ "$(q "SELECT state FROM flashback.coverage_generations
               WHERE generation_id=$INITIAL_RECOVERY_GENERATION")" == "active" ]] && break
    sleep 0.1
done
assert_eq "aborted ilk boundary yeni exact re-anchor ile kurtarıldı" "active" \
    "$(q "SELECT state FROM flashback.coverage_generations
           WHERE generation_id=$INITIAL_RECOVERY_GENERATION")"
assert_eq "yeni generation aborted tombstone'u lineage olarak korudu" "$INITIAL_ABORT_GENERATION" \
    "$(q "SELECT parent_generation_id FROM flashback.coverage_generations
           WHERE generation_id=$INITIAL_RECOVERY_GENERATION")"

echo "━━━ 5. Kapsam dışı veritabanı fail-closed ━━━"
qp "CREATE DATABASE $UNCOV_DB" > /dev/null
$PSQL -d "$UNCOV_DB" -qc "CREATE EXTENSION pg_flashback" -c "CREATE TABLE t (id int PRIMARY KEY)" > /dev/null
UNCOV_RC=0
$PSQL -d "$UNCOV_DB" -qc "SELECT flashback_track('t')" > /tmp/pg_flashback_uncov.out 2>&1 || UNCOV_RC=$?
[[ "$UNCOV_RC" != "0" ]] || { echo "FAIL: kapsam dışı DB tracking'i reddedilmedi"; exit 1; }
grep -q "not covered by a background worker" /tmp/pg_flashback_uncov.out
echo "  ok: kapsam dışı DB tracking'i kalıcı metadata oluşturmadan reddedildi"

exit 0

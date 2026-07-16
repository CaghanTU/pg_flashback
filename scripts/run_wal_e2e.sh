#!/bin/bash
# ShellCheck cannot see that cleanup is entered through the EXIT trap.
# shellcheck disable=SC2317
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
GUCS_MODIFIED=0
CLEANED=0

q()  { $PSQL -d "$DB" -qAtc "$1"; }
qp() { $PSQL -d postgres -qAtc "$1"; }

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
        /tmp/pg_flashback_slot_loss_lock.out
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
restart_pg || { echo "FAIL: PostgreSQL yeniden başlatılamadı"; exit 1; }
echo "  ok: instance yeniden başladı (target_databases=$DB, capture_mode=wal)"

echo "━━━ 1. Extension + track (per-DB slot, ayrı transaction'lar) ━━━"
q "CREATE EXTENSION pg_flashback" > /dev/null
q "CREATE TABLE orders (id serial PRIMARY KEY, customer text, amount numeric(10,2))" > /dev/null
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
    [[ "$(q "SELECT count(*) FROM flashback_health() WHERE health='healthy'")" == "4" ]] && break
    sleep 0.1
done
assert_eq "dört WAL generation aktif ve sağlıklı" "4" \
    "$(q "SELECT count(*) FROM flashback_health() WHERE health='healthy'")"
assert_eq "building generation kalmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations WHERE state='building'")"

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
echo "  ok: WAL amplification — default=${AMP_DEFAULT_WAL} B, RI_FULL=${AMP_FULL_WAL} B (${AMP_RI_RATIO}x), tracked-total=${AMP_TRACKED_TOTAL_WAL} B (${AMP_TOTAL_RATIO}x)"
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
       && "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='building'")" == "0" ]] && break
    sleep 0.1
done
assert_eq "post-restore successor aktif" "1" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='active' AND cg.generation_no=2")"
assert_eq "post-restore pending generation kalmadı" "0" \
    "$(q "SELECT count(*) FROM flashback.coverage_generations cg JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='orders' AND cg.state='building'")"

LEGACY_RC=0
$PSQL -d "$DB" -qc "SELECT flashback_restore('orders', '$T_MID')" > /tmp/pg_flashback_legacy_reject.out 2>&1 || LEGACY_RC=$?
[[ "$LEGACY_RC" != "0" ]] || { echo "FAIL: legacy timestamp restore qualified generation'da reddedilmedi"; exit 1; }
grep -q "disabled for correctness-qualified WAL coverage" /tmp/pg_flashback_legacy_reject.out
echo "  ok: legacy timestamp restore fail-closed"

echo "━━━ 4c. Slot kaybı coverage'ı donduruyor; explicit re-anchor yeni epoch açıyor ━━━"
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
assert_eq "health slot kaybını degraded gösteriyor" "degraded" \
    "$(q "SELECT health FROM flashback_health()
           WHERE table_name='public.orders'")"
assert_eq "kırık stream'in kanıtlanmış eski hedefi hâlâ okunabilir" "1" \
    "$(q "SELECT count(*) FROM flashback_admit_lsn_target('orders', '$RESOLVED_LSN')")"

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

echo "━━━ 4d. Harici slot ilerletme sessiz devam etmiyor ━━━"
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
assert_eq "harici ilerleme sonrası health degraded" "degraded" \
    "$(q "SELECT health FROM flashback_health()
           WHERE table_name='public.orders'")"

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

echo "━━━ 4e. untrack orijinal REPLICA IDENTITY'yi geri getiriyor ━━━"
$PSQL -d "$DB" -qc 'SELECT flashback_untrack('"'"'"we""ird"'"'"')' > /dev/null
assert_eq "untrack sonrası replica identity DEFAULT" "d" \
    "$(q "SELECT relreplident FROM pg_class WHERE relname = 'we\"ird'")"

echo "━━━ 5. Kapsam dışı veritabanı fail-closed ━━━"
qp "CREATE DATABASE $UNCOV_DB" > /dev/null
$PSQL -d "$UNCOV_DB" -qc "CREATE EXTENSION pg_flashback" -c "CREATE TABLE t (id int PRIMARY KEY)" > /dev/null
UNCOV_RC=0
$PSQL -d "$UNCOV_DB" -qc "SELECT flashback_track('t')" > /tmp/pg_flashback_uncov.out 2>&1 || UNCOV_RC=$?
[[ "$UNCOV_RC" != "0" ]] || { echo "FAIL: kapsam dışı DB tracking'i reddedilmedi"; exit 1; }
grep -q "not covered by a background worker" /tmp/pg_flashback_uncov.out
echo "  ok: kapsam dışı DB tracking'i kalıcı metadata oluşturmadan reddedildi"

exit 0

#!/bin/bash
# pg_flashback — WAL mode + background worker end-to-end verification.
#
# The pgrx test harness runs every test inside ONE transaction, and logical
# decoding only ever sees committed transactions — so the real WAL pipeline
# (slot → worker → flashback_consume_wal → delta_log → restore) can only be
# verified with separate sessions/transactions against a live instance.
# This script does exactly that, including:
#   * per-database slot creation via flashback_track()
#   * background worker consumption (real commit_time + LSN stamping)
#   * poison-value safety (quoted table name, NaN numeric)
#   * PITR correctness of a restore to a point between committed transactions
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
qp "CREATE DATABASE $DB" > /dev/null
GUCS_MODIFIED=1
qp "ALTER SYSTEM SET pg_flashback.target_databases = '$DB'" > /dev/null
qp "ALTER SYSTEM SET pg_flashback.capture_mode = 'wal'" > /dev/null
restart_pg || { echo "FAIL: PostgreSQL yeniden başlatılamadı"; exit 1; }
echo "  ok: instance yeniden başladı (target_databases=$DB, capture_mode=wal)"

echo "━━━ 1. Extension + track (per-DB slot, ayrı transaction'lar) ━━━"
q "CREATE EXTENSION pg_flashback" > /dev/null
q "CREATE TABLE orders (id serial PRIMARY KEY, customer text, amount numeric(10,2))" > /dev/null
q "SELECT flashback_track('orders')" > /dev/null
$PSQL -d "$DB" -q <<'SQL'
CREATE TABLE "we""ird" (id int PRIMARY KEY, amount numeric);
SELECT flashback_track('"we""ird"');
SQL

assert_eq "slot bu veritabanında" "$DB" \
    "$(q "SELECT database FROM pg_replication_slots WHERE slot_name = 'pg_flashback_${DB}'")"
assert_eq "WAL track REPLICA IDENTITY FULL yaptı" "f" \
    "$(q "SELECT relreplident FROM pg_class WHERE oid = 'orders'::regclass")"

WORKER_DB=""
for _ in $(seq 1 20); do
    WORKER_DB=$(qp "SELECT datname FROM pg_stat_activity
                    WHERE backend_type = 'pg_flashback delta worker' AND datname = '$DB'")
    [[ "$WORKER_DB" == "$DB" ]] && break
    sleep 0.5
done
assert_eq "worker bu veritabanına bağlı" "$DB" "$WORKER_DB"

echo "━━━ 2. Commit edilen DML'in worker tarafından tüketilmesi ━━━"
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
assert_eq "UPDATE olay sayısı" "10"   "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='UPDATE'")"
assert_eq "DELETE olay sayısı" "500"  "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='DELETE'")"
assert_eq "LSN'siz olay sayısı" "0"   "$(q "SELECT count(*) FROM flashback.delta_log WHERE lsn IS NULL")"
assert_eq "NaN kayıpsız yakalandı" "NaN" \
    "$(q "SELECT new_data->>'amount' FROM flashback.delta_log WHERE table_name LIKE '%ird%' AND new_data->>'id' = '1'")"

echo "━━━ 3. Gerçek commit zamanı damgası (PITR doğruluğu) ━━━"
# T_MID'den önce commit olan UPDATE'ler T_MID'den önce, sonra commit olan
# DELETE'ler T_MID'den sonra damgalanmış olmalı — tüketim anı değil.
assert_eq "UPDATE'ler T_MID'den önce damgalı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='UPDATE' AND committed_at >= '$T_MID'")"
assert_eq "DELETE'ler T_MID'den sonra damgalı" "0" \
    "$(q "SELECT count(*) FROM flashback.delta_log WHERE event_type='DELETE' AND committed_at <= '$T_MID'")"

echo "━━━ 4. Felaket-öncesine restore ━━━"
q "SELECT flashback_restore('orders', '$T_MID')" > /dev/null
assert_eq "restore sonrası satır sayısı" "1001" "$(q "SELECT count(*) FROM orders")"
assert_eq "yeni_musteri kurtarıldı" "1" "$(q "SELECT count(*) FROM orders WHERE customer='yeni_musteri'")"
assert_eq "güncellenmiş tutarlar korundu" "115.00" "$(q "SELECT max(amount) FROM orders WHERE id <= 10")"
# WAL modunda restore capture trigger'ı GERİ TAKMAMALI (staging'i kimse boşaltmaz)
assert_eq "restore sonrası capture trigger yok" "0" \
    "$(q "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'orders'::regclass AND tgname LIKE 'flashback_capture_%'")"
assert_eq "restore REPLICA IDENTITY FULL korudu" "f" \
    "$(q "SELECT relreplident FROM pg_class WHERE oid = 'orders'::regclass")"

echo "━━━ 4b. untrack orijinal REPLICA IDENTITY'yi geri getiriyor ━━━"
$PSQL -d "$DB" -qc 'SELECT flashback_untrack('"'"'"we""ird"'"'"')' > /dev/null
assert_eq "untrack sonrası replica identity DEFAULT" "d" \
    "$(q "SELECT relreplident FROM pg_class WHERE relname = 'we\"ird'")"

echo "━━━ 5. Kapsam dışı veritabanı uyarısı ━━━"
qp "CREATE DATABASE $UNCOV_DB" > /dev/null
WARN_OUT=$($PSQL -d "$UNCOV_DB" -qc "CREATE EXTENSION pg_flashback" \
    -c "CREATE TABLE t (id int PRIMARY KEY)" \
    -c "SELECT flashback_track('t')" 2>&1 | grep -c "NOT covered" || true)
assert_eq "kapsam dışı DB'de uyarı basıldı" "1" "$WARN_OUT"

exit 0

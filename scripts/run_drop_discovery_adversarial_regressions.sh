#!/usr/bin/env bash
# Adversarial regressions for the immediate-DROP discovery race fixed in
# flashback_recover_plan (durable flashback.capture_streams frontier instead
# of the raw, non-transactionally-advanced pg_replication_slots row).
#
# Drives the real CLI (pg_flashback recover --dry-run / recover --yes), never
# flashback_consume_wal() directly, never a fixed sleep, never internal
# catalog mutation -- the CLI's own bounded discovery polling
# (PG_FLASHBACK_DISCOVERY_TIMEOUT_S) is the only wait mechanism exercised.
#
# Usage: ./scripts/run_drop_discovery_adversarial_regressions.sh
# Env:   PG_CONFIG, PGFB_DDA_PORT, PGFB_DDA_KEEP=1
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
CLI_SRC="$ROOT/scripts/pg_flashback"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PGFB_DDA_PORT:-28958}"
WORK_ROOT="${PGFB_DDA_WORK_ROOT:-$ROOT/target/drop-discovery-adversarial/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-dda-$RUN_ID"
LOG="$WORK_ROOT/postgresql.log"
PASSED=0
FAILED=0

die() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASSED=$((PASSED + 1)); }
fail_case() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }

cleanup() {
    local rc=$?
    set +e
    if [[ "${PGFB_DDA_KEEP:-0}" != "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
        rm -rf "$DATA" "$SOCKET"
    else
        echo "PGFB_DDA_KEEP=1: leaving $WORK_ROOT" >&2
    fi
    echo "drop-discovery adversarial: $([[ $FAILED -eq 0 && $rc -eq 0 ]] && echo PASS || echo FAIL) (passed=$PASSED failed=$FAILED)"
    [[ $FAILED -eq 0 && $rc -eq 0 ]] || exit 1
    exit 0
}
trap cleanup EXIT

[[ -x "$PG_BIN/psql" ]] || die "psql not found via $PG_CONFIG"
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || die "extension not installed in $SHARE_DIR"
[[ -x "$CLI_SRC" ]] || die "CLI source missing: $CLI_SRC"

mkdir -p "$WORK_ROOT" "$SOCKET"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/output_plugin_allowlist.sh"
opal_configure_postgresql_conf "$PG_BIN" "$DATA"
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 -qAtc "$1"; }
mkdir -p "$WORK_ROOT/bin"
install -m 0755 "$CLI_SRC" "$WORK_ROOT/bin/pg_flashback"
export PATH="$WORK_ROOT/bin:$PG_BIN:$PATH"
PGFB_DDA_USER="$(id -un)"
export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE=postgres PGUSER="$PGFB_DDA_USER"

q "CREATE EXTENSION pg_flashback;" >/dev/null

now_ms() { date +%s%3N; }
echo "== Scenario 3: no DROP ever happened -> bounded, honest missing result =="
q "CREATE TABLE public.dda_t3(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dda_t3');" >/dev/null
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t3');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s3 lifecycle never became healthy"
q "INSERT INTO public.dda_t3 VALUES (1,'a');" >/dev/null

# A live pg_flashback instance's own maintenance cycle produces a small,
# continuous, unrelated WAL trickle (independently confirmed: it stops the
# moment pg_flashback.enabled=off), which routinely keeps the durable
# frontier a little behind pg_current_wal_lsn() -- flashback_consume_wal
# deliberately only advances its frontier for an empty/irrelevant prefix
# once >=64KiB has accumulated, to avoid writing a WAL record on every idle
# cycle. So "genuinely caught up" is not instantaneous on a live cluster.
# What must hold regardless: a bounded discovery-timeout CLI call never
# hangs past its own bound, never invents a DROP, and never reports
# anything other than the honest "still pending" or "genuinely absent" code.
t0=$(now_ms)
set +e
OUT3A=$(PG_FLASHBACK_DISCOVERY_TIMEOUT_S=3 pg_flashback --json recover public.dda_t3 --dry-run --latest-drop 2>"$WORK_ROOT/s3a.err")
RC3A=$?
set -e
t1=$(now_ms)
elapsed_ms=$((t1 - t0))
printf '%s' "$OUT3A" | jq -e . >/dev/null 2>&1 || die "s3a invalid JSON: $OUT3A"
[[ $RC3A -eq 1 ]] || die "s3a expected exit 1 got $RC3A: $OUT3A"
code3a=$(printf '%s' "$OUT3A" | jq -r '.code')
[[ "$code3a" == "no_drop_event" || "$code3a" == "capture_catchup_pending" ]] \
    || die "s3a unexpected code (must be an honest pending/absent code, never invented): $OUT3A"
(( elapsed_ms < 5000 )) || die "s3a took ${elapsed_ms}ms with a 3s discovery timeout; CLI did not respect its own bound"
pass "s3 bounded by its own discovery timeout in ${elapsed_ms}ms, reported honest code=$code3a (never invented a DROP)"

echo "== Scenario 1: immediate DROP -> recover --dry-run waits and succeeds (no sleep) =="
q "CREATE TABLE public.dda_t1(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dda_t1');" >/dev/null
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t1');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s1 lifecycle never became healthy"
q "INSERT INTO public.dda_t1 VALUES (1,'a'),(2,'b');" >/dev/null
t0=$(now_ms)
q "DROP TABLE public.dda_t1;"
set +e
OUT1=$(pg_flashback --json recover public.dda_t1 --dry-run --latest-drop 2>"$WORK_ROOT/s1.err")
RC1=$?
set -e
t1=$(now_ms)
elapsed_ms=$((t1 - t0))
printf '%s' "$OUT1" | jq -e . >/dev/null 2>&1 || die "s1 invalid JSON: $OUT1 ($(cat "$WORK_ROOT/s1.err"))"
[[ $RC1 -eq 0 ]] || die "s1 dry-run failed rc=$RC1: $OUT1"
[[ "$(printf '%s' "$OUT1" | jq -r '.status')" == "ok" ]] || die "s1 unexpected status: $OUT1"
[[ "$(printf '%s' "$OUT1" | jq -r '.code')" == "dry_run" ]] || die "s1 unexpected code: $OUT1"
pass "s1 immediate dry-run succeeded in ${elapsed_ms}ms (JSON stable, no sleep in test)"

echo "== Scenario 2: immediate DROP -> real recover succeeds =="
q "CREATE TABLE public.dda_t2(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dda_t2');" >/dev/null
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t2');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s2 lifecycle never became healthy"
q "INSERT INTO public.dda_t2 VALUES (1,'a'),(2,'b'),(3,'c');" >/dev/null
q "DROP TABLE public.dda_t2;"
set +e
OUT2=$(pg_flashback --json recover public.dda_t2 --yes --latest-drop 2>"$WORK_ROOT/s2.err")
RC2=$?
set -e
[[ $RC2 -eq 0 ]] || die "s2 recover failed rc=$RC2: $OUT2 ($(cat "$WORK_ROOT/s2.err"))"
[[ "$(printf '%s' "$OUT2" | jq -r '.status')" == "ok" ]] || die "s2 unexpected status: $OUT2"
cnt=$(q "SELECT count(*) FROM public.dda_t2;")
[[ "$cnt" == "3" ]] || die "s2 row count after recover: $cnt"
pass "s2 immediate real recover restored 3 rows with no sleep"

echo "== Scenario 4: capture stream lost while a caller would otherwise poll -> hard fail =="
q "CREATE TABLE public.dda_t4(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dda_t4');" >/dev/null
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t4');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s4 lifecycle never became healthy"
q "INSERT INTO public.dda_t4 VALUES (1,'a');" >/dev/null
# `healthy` is not a post-DML drain barrier: crash-safe WAL consumption
# persists decoded payload and acknowledges the slot in two transactions.
# Wait for both the row event and the cleared acknowledgement intent before
# freezing the worker, otherwise the DROP hook can correctly refuse an
# unrelated still-pending commit and this slot-loss scenario tests nothing.
for _ in $(seq 1 600); do
    captured=$(q "SELECT count(*) FROM flashback.delta_log
        WHERE table_name='public.dda_t4' AND event_type='INSERT'
          AND (new_data->>'id')='1';")
    ack_pending=$(q "SELECT count(*) FROM flashback.capture_streams
        WHERE state='active' AND details ? 'safe_slot_advance_upto_lsn';")
    [[ "$captured" == "1" && "$ack_pending" == "0" ]] && break
    sleep 0.1
done
[[ "$captured" == "1" && "$ack_pending" == "0" ]] \
    || die "s4 precondition never reached a durable drained INSERT"
slot=$(q "SELECT slot_name FROM pg_replication_slots WHERE plugin='pg_flashback';")
[[ -n "$slot" ]] || die "s4 no pg_flashback slot found"
# The DROP's ProcessUtility hook itself needs the slot to exist (it peeks a
# pre-DROP manifest), so the slot cannot be dropped before the table DROP.
# Freeze the capture worker (SIGSTOP, same technique as
# scripts/run_wal_e2e.sh's stop_idle_worker) while it is idle between
# cycles -- confirmed by it not holding the advisory admission lock -- so it
# cannot race the table DROP with a decode cycle of its own. Then DROP the
# table (slot still exists and is valid, hook succeeds), drop the
# replication slot while the worker is still frozen (deterministic: zero
# chance the worker gets there first), and only then resume the worker so
# its next cycle is the one that discovers the missing slot.
worker_pid=""
for _ in $(seq 1 300); do
    worker_pid=$(q "SELECT pid FROM pg_stat_activity
        WHERE backend_type='pg_flashback delta worker'
          AND datname=current_database()
          AND wait_event_type='Extension'
        LIMIT 1;")
    if [[ -n "$worker_pid" ]]; then
        kill -STOP "$worker_pid"
        sleep 0.05
        idle=$(q "SELECT count(*) FROM pg_locks
            WHERE pid=$worker_pid AND locktype='advisory' AND granted;")
        [[ "$idle" == "0" ]] && break
        kill -CONT "$worker_pid" >/dev/null 2>&1 || true
        worker_pid=""
    fi
    sleep 0.05
done
[[ -n "$worker_pid" ]] || die "s4 could not freeze capture worker between cycles"
q "DROP TABLE public.dda_t4;"
dropped_slot=0
for _ in $(seq 1 100); do
    if q "SELECT pg_drop_replication_slot('$slot');" >/dev/null 2>"$WORK_ROOT/s4-drop-slot.err"; then
        dropped_slot=1
        break
    fi
    sleep 0.1
done
kill -CONT "$worker_pid" >/dev/null 2>&1 || true
[[ "$dropped_slot" == 1 ]] || die "s4 could not drop slot '$slot': $(cat "$WORK_ROOT/s4-drop-slot.err")"
# Give the worker's next cycle a chance to observe the missing slot and mark
# the capture stream broken (flashback_ensure_active_wal_stream's fail-closed
# path), the same durable evidence flashback_recover_plan now consults. With
# multiple pinned lifecycles from earlier scenarios sharing this stream, the
# mark-broken transaction can occasionally queue behind the maintenance
# worker's own lifecycle locking; bounded but generous.
for _ in $(seq 1 600); do
    st=$(q "SELECT state FROM flashback.capture_streams ORDER BY epoch_no DESC LIMIT 1;")
    [[ "$st" != "active" ]] && break
    sleep 0.1
done
[[ "$st" != "active" ]] || die "s4 capture stream never left active state after slot drop"
t0=$(now_ms)
set +e
OUT4=$(PG_FLASHBACK_DISCOVERY_TIMEOUT_S=25 pg_flashback --json recover public.dda_t4 --dry-run --latest-drop 2>"$WORK_ROOT/s4.err")
RC4=$?
set -e
t1=$(now_ms)
elapsed_ms=$((t1 - t0))
printf '%s' "$OUT4" | jq -e . >/dev/null 2>&1 || die "s4 invalid JSON: $OUT4"
[[ $RC4 -eq 1 ]] || die "s4 expected exit 1 got $RC4: $OUT4"
[[ "$(printf '%s' "$OUT4" | jq -r '.code')" == "capture_stream_unavailable" ]] || die "s4 unexpected code: $OUT4"
(( elapsed_ms < 20000 )) || die "s4 took ${elapsed_ms}ms; capture_stream_unavailable must be a hard fail, not polled"
pass "s4 broken capture stream returned capture_stream_unavailable as a hard fail in ${elapsed_ms}ms"

echo "== Scenario 5: two historical DROPs of the same lifecycle stay ambiguity-safe (latest wins) =="
q "CREATE TABLE public.dda_t5(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.dda_t5');" >/dev/null
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t5');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s5 first lifecycle never became healthy"
q "INSERT INTO public.dda_t5 VALUES (1,'first');" >/dev/null
q "DROP TABLE public.dda_t5;"
set +e
OUT5A=$(pg_flashback --json recover public.dda_t5 --yes --latest-drop 2>"$WORK_ROOT/s5a.err")
RC5A=$?
set -e
[[ $RC5A -eq 0 ]] || die "s5 first recover failed: $OUT5A ($(cat "$WORK_ROOT/s5a.err"))"
# Recover keeps the same lifecycle tracking the restored table (no re-track
# needed or valid); wait for successor coverage to become healthy again
# before creating the second, later DROP of the same qualified name.
for _ in $(seq 1 200); do
    h=$(q "SELECT flashback_lifecycle_health('public.dda_t5');")
    [[ "$h" == "healthy" ]] && break
    sleep 0.05
done
[[ "$h" == "healthy" ]] || die "s5 lifecycle never became healthy again after first recover"
q "UPDATE public.dda_t5 SET v='second' WHERE id=1;" >/dev/null
q "INSERT INTO public.dda_t5 VALUES (2,'second-new');" >/dev/null
q "DROP TABLE public.dda_t5;"
set +e
OUT5B=$(pg_flashback --json recover public.dda_t5 --dry-run --latest-drop 2>"$WORK_ROOT/s5b.err")
RC5B=$?
set -e
printf '%s' "$OUT5B" | jq -e . >/dev/null 2>&1 || die "s5 second dry-run invalid JSON: $OUT5B"
[[ $RC5B -eq 0 ]] || die "s5 second dry-run failed rc=$RC5B: $OUT5B ($(cat "$WORK_ROOT/s5b.err"))"
[[ "$(printf '%s' "$OUT5B" | jq -r '.data.plan.selection')" == "latest_drop" ]] || die "s5 unexpected selection: $OUT5B"
set +e
OUT5C=$(pg_flashback --json recover public.dda_t5 --yes --latest-drop 2>"$WORK_ROOT/s5c.err")
RC5C=$?
set -e
[[ $RC5C -eq 0 ]] || die "s5 second real recover failed: $OUT5C ($(cat "$WORK_ROOT/s5c.err"))"
cnt5=$(q "SELECT count(*) FROM public.dda_t5;")
[[ "$cnt5" == "2" ]] || die "s5 recovered the wrong generation's rows (count=$cnt5, expected 2 from the second lifecycle)"
pass "s5 two same-name historical DROPs resolved to the latest generation, not an ambiguous/wrong one"

echo ""
echo "drop-discovery adversarial: passed=$PASSED failed=$FAILED"

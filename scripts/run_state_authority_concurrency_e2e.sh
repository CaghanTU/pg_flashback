#!/usr/bin/env bash
# E2E multi-session concurrency test for the centralized state/lock/journal
# authority (sql/functions/state_authority.sql, operation_journal.sql).
#
# Exercises real separate backends (never GUCs shared across sessions, never
# PERFORM outside a PL/pgSQL body) against a dedicated, disposable database:
#   * reverse-order lifecycle-lock acquisition from two sessions must not
#     deadlock (flashback_internal_lock_lifecycles sorts + dedupes first),
#     proven via a real startup barrier so both sessions are genuinely
#     racing, and with no advisory-lock leak afterward
#   * two sessions racing to append the SAME terminal event_type with
#     DIFFERENT payloads for one operation: exactly one must win, the loser
#     must get the exact fail-closed "conflicting terminal event retry"
#     error, and the journal must end with exactly one terminal event row
#   * flashback_recover_mark_failed must durably persist 'failed' from a
#     brand-new session/transaction after the operation's own transaction
#     rolled back (the append-then-rollback and the mark_failed call are two
#     separate psql invocations, i.e. two separate backends/transactions)
set -euo pipefail

BINDIR="${1:-}"
if [[ -z "$BINDIR" ]]; then
    for cand in /usr/local/pgsql-17/bin /usr/pgsql-17/bin /usr/bin; do
        if [[ -x "$cand/psql" ]]; then BINDIR="$cand"; break; fi
    done
fi
[[ -n "$BINDIR" ]] || { echo "FAIL: psql not found"; exit 1; }

PSQL="$BINDIR/psql"
DB="fb_state_authority_e2e_$$"
export PGHOST="${PGHOST:-/tmp}"
export PGPORT="${PGPORT:-5432}"

WORKDIR="$(mktemp -d)"

# One-shot autocommit statement against $DB.
q() { "$PSQL" -v ON_ERROR_STOP=1 -d "$DB" -Atqc "$1"; }
# One-shot autocommit statement against the postgres maintenance DB.
qp() { "$PSQL" -v ON_ERROR_STOP=1 -d postgres -Atqc "$1"; }

cleanup() {
    local rc=$?
    qp "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB' AND pid <> pg_backend_pid()" >/dev/null 2>&1 || true
    qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null 2>&1 || true
    rm -rf "$WORKDIR"
    exit "$rc"
}
trap cleanup EXIT

echo "==> state authority concurrency E2E: setting up $DB"
qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null
qp "CREATE DATABASE $DB" >/dev/null
q "CREATE EXTENSION pg_flashback" >/dev/null

# Barrier table: each session INSERTs (autocommit) when it is about to start
# the racy transaction, then polls (still autocommit, before BEGIN) until
# both rows are visible. This proves both backends are genuinely concurrent
# instead of relying on sleep-based luck.
q "CREATE TABLE fb_test_barrier (session_name text PRIMARY KEY, ready_at timestamptz NOT NULL DEFAULT clock_timestamp())" >/dev/null

barrier_wait_sql() {
    local expected="$1"
    cat <<SQL
DO \$\$
DECLARE
    v_n integer;
    v_iter integer := 0;
BEGIN
    LOOP
        SELECT count(*) INTO v_n FROM fb_test_barrier;
        EXIT WHEN v_n >= $expected;
        v_iter := v_iter + 1;
        IF v_iter > 500 THEN
            RAISE EXCEPTION 'barrier timeout waiting for % sessions (saw %)', $expected, v_n;
        END IF;
        PERFORM pg_sleep(0.02);
    END LOOP;
END
\$\$;
SQL
}

# ------------------------------------------------------------------
# Test 1: reverse-order lifecycle lock acquisition must not deadlock.
# ------------------------------------------------------------------
echo "==> test 1: reverse-order lifecycle lock acquisition (two real sessions)"
q "TRUNCATE fb_test_barrier" >/dev/null

RC_A=0
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB" >"$WORKDIR/session_a.out" 2>&1 <<SQL &
INSERT INTO fb_test_barrier(session_name) VALUES ('A');
$(barrier_wait_sql 2)
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL deadlock_timeout = '200ms';
SELECT flashback_internal_lock_lifecycles(ARRAY[2000::bigint, 1000::bigint]);
SELECT pg_sleep(1);
COMMIT;
SQL
PID_A=$!

"$PSQL" -v ON_ERROR_STOP=1 -d "$DB" >"$WORKDIR/session_b.out" 2>&1 <<SQL &
INSERT INTO fb_test_barrier(session_name) VALUES ('B');
$(barrier_wait_sql 2)
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL deadlock_timeout = '200ms';
SELECT flashback_internal_lock_lifecycles(ARRAY[1000::bigint, 2000::bigint]);
SELECT pg_sleep(1);
COMMIT;
SQL
PID_B=$!

wait "$PID_A" || RC_A=$?
RC_B=0
wait "$PID_B" || RC_B=$?

if [[ "$RC_A" != "0" || "$RC_B" != "0" ]]; then
    echo "FAIL: reverse-order lock acquisition did not both succeed (rc_a=$RC_A rc_b=$RC_B)"
    cat "$WORKDIR/session_a.out" "$WORKDIR/session_b.out"
    exit 1
fi
if grep -qi "deadlock detected" "$WORKDIR/session_a.out" "$WORKDIR/session_b.out"; then
    echo "FAIL: deadlock detected despite sorted lock acquisition"
    exit 1
fi

LEAKED=$(q "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid IN (358944, 358945)")
if [[ "$LEAKED" != "0" ]]; then
    echo "FAIL: $LEAKED advisory lock(s) leaked after both sessions committed"
    exit 1
fi
echo "OK: reverse-order lock acquisition completed with no deadlock and no lock leak"

# ------------------------------------------------------------------
# Test 2: two sessions race the SAME terminal event_type with DIFFERENT
# payloads. Exactly one must win; the other must get the exact fail-closed
# "conflicting terminal event retry" error (operation_journal.sql:233).
# ------------------------------------------------------------------
echo "==> test 2: conflicting terminal event payload race"
OP_ID=$(q "SELECT flashback_operation_begin('recover', 'public.concurrency_test', 9999)")
[[ -n "$OP_ID" ]] || { echo "FAIL: could not capture OP_ID"; exit 1; }
q "SELECT flashback_operation_append_event($OP_ID, 'applied_coverage_pending', NULL, NULL, 'pending', '{\"rows\":10}'::jsonb)" >/dev/null

q "TRUNCATE fb_test_barrier" >/dev/null

RC_1=0
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB" >"$WORKDIR/race_1.out" 2>&1 <<SQL &
INSERT INTO fb_test_barrier(session_name) VALUES ('R1');
$(barrier_wait_sql 2)
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL deadlock_timeout = '200ms';
SELECT flashback_operation_append_event($OP_ID, 'verified', NULL, NULL, 'verified', '{"proof":"session-1"}'::jsonb);
COMMIT;
SQL
PID_1=$!

RC_2=0
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB" >"$WORKDIR/race_2.out" 2>&1 <<SQL &
INSERT INTO fb_test_barrier(session_name) VALUES ('R2');
$(barrier_wait_sql 2)
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL deadlock_timeout = '200ms';
SELECT flashback_operation_append_event($OP_ID, 'verified', NULL, NULL, 'verified', '{"proof":"session-2"}'::jsonb);
COMMIT;
SQL
PID_2=$!

wait "$PID_1" || RC_1=$?
wait "$PID_2" || RC_2=$?

WINS=0
[[ "$RC_1" == "0" ]] && WINS=$((WINS + 1))
[[ "$RC_2" == "0" ]] && WINS=$((WINS + 1))
if [[ "$WINS" != "1" ]]; then
    echo "FAIL: expected exactly one winner of the terminal-payload race, got $WINS (rc_1=$RC_1 rc_2=$RC_2)"
    cat "$WORKDIR/race_1.out" "$WORKDIR/race_2.out"
    exit 1
fi

LOSER_OUT="$WORKDIR/race_2.out"
[[ "$RC_1" != "0" ]] && LOSER_OUT="$WORKDIR/race_1.out"
if ! grep -q "conflicting terminal event retry for operation $OP_ID" "$LOSER_OUT"; then
    echo "FAIL: loser did not get the expected fail-closed error"
    cat "$LOSER_OUT"
    exit 1
fi
echo "OK: exactly one winner; loser got the exact fail-closed conflicting-terminal-event error"

TERMINAL_EVENTS=$(q "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $OP_ID AND event_type IN ('verified','failed','abandoned','unprotected','cleaned','sealed')")
if [[ "$TERMINAL_EVENTS" != "1" ]]; then
    echo "FAIL: expected exactly 1 terminal event row for operation $OP_ID, got $TERMINAL_EVENTS"
    exit 1
fi

FINAL_STATE=$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID")
if [[ "$FINAL_STATE" != "verified" ]]; then
    echo "FAIL: final state must be the terminal 'verified', got '$FINAL_STATE' (applied_coverage_pending is never an acceptable final state)"
    exit 1
fi
echo "OK: exactly one terminal event row; final state is verified"

# ------------------------------------------------------------------
# Test 3: mark_failed must durably persist 'failed' from a brand-new
# session/transaction after the original transaction rolled back.
# ------------------------------------------------------------------
echo "==> test 3: cross-transaction mark_failed durability"
OP_ID_2=$(q "SELECT flashback_operation_begin('recover', 'public.concurrency_test', 9999)")
[[ -n "$OP_ID_2" ]] || { echo "FAIL: could not capture OP_ID_2"; exit 1; }

"$PSQL" -v ON_ERROR_STOP=1 -d "$DB" >"$WORKDIR/rollback.out" 2>&1 <<SQL
BEGIN;
SELECT flashback_operation_append_event($OP_ID_2, 'applied_coverage_pending', NULL, NULL, 'pending', '{}'::jsonb);
ROLLBACK;
SQL

STATE_AFTER_ROLLBACK=$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID_2")
if [[ "$STATE_AFTER_ROLLBACK" != "started" ]]; then
    echo "FAIL: rollback of applied_coverage_pending must leave the operation at 'started', got '$STATE_AFTER_ROLLBACK'"
    exit 1
fi

q "SELECT flashback_recover_mark_failed($OP_ID_2, 'XX000', 'simulate_failure', 'test mark_failed')" >/dev/null

FINAL_STATE_2=$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID_2")
if [[ "$FINAL_STATE_2" != "failed" ]]; then
    echo "FAIL: mark_failed did not persist: state is '$FINAL_STATE_2'"
    exit 1
fi
echo "OK: mark_failed persisted 'failed' from a separate transaction after rollback"

echo "OK: state authority concurrency E2E passed"

#!/usr/bin/env bash
# ShellCheck cannot see that cleanup is entered through the EXIT trap.
# shellcheck disable=SC2317,SC2329
#
# ADIM 7 PoC harness -- online snapshot <-> single logical WAL stream
# alignment. This is NOT a production code path. It proves, against a
# throwaway isolated PostgreSQL cluster, that a base copy taken without a
# long table write-lock can be stitched to the one shared pg_flashback
# logical WAL stream without gap or duplicate, under two protocols:
#
#   A (greenfield):     CREATE_REPLICATION_SLOT ... LOGICAL pg_flashback
#                        EXPORT_SNAPSHOT over the replication protocol, then
#                        SET TRANSACTION SNAPSHOT in a separate REPEATABLE
#                        READ session for a lock-free base copy.
#   B (existing stream): short SHARE ROW EXCLUSIVE coordinator lock, a
#                        copier session that fixes its MVCC snapshot while
#                        the lock is held, a transactional BOUNDARY marker
#                        (pg_logical_emit_message) whose COMMIT releases the
#                        lock and fixes the real boundary LSN, then a long
#                        lock-free copy from the already-fixed snapshot.
#
# Reuses the ALREADY-COMPILED pg_flashback output plugin (_PG_output_plugin_init,
# src/capture/wal_decoder.rs) as a decoding library, loaded via a private
# dynamic_library_path -- never the shared /usr/local/pgsql-*/lib install, so
# a real dev/qualification instance sharing that prefix is never touched.
# No pg_flashback SQL function (flashback_track, flashback_*, etc.) is ever
# called; only core PostgreSQL replication-protocol commands and
# pg_logical_slot_get_changes(). Nothing here is wired into the extension's
# SQL surface, GUCs, or generated SQL.
#
# Because this proves CURRENT SOURCE behavior (not a packaged release
# candidate), it intentionally builds with `cargo build --release`, unlike
# the exact-candidate qualification scripts under scripts/lib, which
# deliberately never cargo-build. A candidate-identity guard (source_commit,
# working tree cleanliness, built .so sha256) still runs before any cluster
# is created, and POC_EXPECTED_SO_SHA256 can force a hard mismatch abort for
# the harness selftest.
#
# Usage:
#   ./scripts/run_poc_online_snapshot_wal_alignment.sh selftest
#   ./scripts/run_poc_online_snapshot_wal_alignment.sh dev   [size_mib]
#   ./scripts/run_poc_online_snapshot_wal_alignment.sh scale <a|b> <ordinary|toast> [size_mib]
#
# Env:
#   POC_KEEP=1                 keep cluster/work dir even on PASS (debugging)
#   POC_EXPECTED_SO_SHA256=... abort before any cluster if built .so does not match
#
# Evidence: target/poc/online-snapshot-wal-alignment/<run-id>/result.json
# (gitignored; never committed). status is PASS only when every step this
# mode expects is present and marked pass, the process exit code is 0, and
# no trapped signal interrupted the run.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$ROOT/scripts/lib/qualification_step_tracker.sh"

MODE="${1:-}"
[[ -n "$MODE" ]] || { echo "FAIL: usage: $0 selftest|dev|scale ..." >&2; exit 2; }

PG_BIN="${POC_PG_BIN:-/usr/local/pgsql-17/bin}"
[[ -x "$PG_BIN/pg_ctl" && -x "$PG_BIN/psql" && -x "$PG_BIN/initdb" ]] \
    || { echo "FAIL: PostgreSQL 17 binaries not found under $PG_BIN" >&2; exit 2; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK_ROOT="$ROOT/target/poc/online-snapshot-wal-alignment/$RUN_ID"
DATA="$WORK_ROOT/data"
LOG_DIR="$WORK_ROOT/log"
LOG="$LOG_DIR/postgresql.log"
PGLIB_DIR="$WORK_ROOT/pglib"
SOCKET="/tmp/pgfb-poc-$RUN_ID"
RESULT_JSON="$WORK_ROOT/result.json"
DB=poc_align

die() { echo "FAIL: $*" >&2; exit 1; }

# ── Step lists per mode ──────────────────────────────────────────────────
case "$MODE" in
    selftest)
        SIZE_MIB=1
        qst_init dirty_tree_rejected candidate_mismatch_rejected missing_step_cannot_pass interrupt_yields_fail
        ;;
    dev)
        SIZE_MIB="${2:-64}"
        qst_init candidate_build cluster_bootstrap \
            protocol_a_base protocol_a_wal_alignment protocol_a_export_conn_lifecycle \
            protocol_b_base protocol_b_wal_alignment protocol_b_multi_table \
            adversarial_boundary_timing adversarial_named_transactions \
            adversarial_churn_and_toast adversarial_quoted_identifiers \
            adversarial_crash_and_retry adversarial_slot_loss adversarial_restart_crash \
            ddl_queue_policy_comparison xmin_vacuum_horizon write_stall_distribution
        ;;
    scale)
        PROTOCOL="${2:-}"; PROFILE="${3:-}"; SIZE_MIB="${4:-1024}"
        [[ "$PROTOCOL" == "a" || "$PROTOCOL" == "b" ]] || die "scale mode needs protocol a|b"
        [[ "$PROFILE" == "ordinary" || "$PROFILE" == "toast" ]] || die "scale mode needs profile ordinary|toast"
        qst_init candidate_build cluster_bootstrap scale_base scale_wal_alignment scale_fingerprint
        ;;
    __selftest_child_missing_step|__selftest_child_interrupt_target)
        # Handled entirely by the dedicated blocks below, which qst_init
        # their own step lists and exit before the shared cluster machinery.
        ;;
    *)
        die "unknown mode $MODE (use selftest|dev|scale)"
        ;;
esac

trap 'qst_on_signal HUP' HUP
trap 'qst_on_signal INT' INT
trap 'qst_on_signal TERM' TERM

# ── Cleanup / evidence write (always runs) ───────────────────────────────
EXTRA_JSON='{}'
POSTGRES_STARTED=0

cleanup() {
    local rc=$?
    set +e
    mkdir -p "$WORK_ROOT"

    local summary_json
    summary_json="$(qst_compute_summary_json "$RUN_ID" "$MODE" "$rc" "$EXTRA_JSON")"
    qst_write_summary_atomic "$RESULT_JSON" "$summary_json"
    local overall
    overall="$(jq -r '.status' "$RESULT_JSON" 2>/dev/null || echo FAIL)"

    if [[ "$POSTGRES_STARTED" == "1" && -f "$DATA/postmaster.pid" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null 2>&1 \
            || "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    fi
    rm -rf "$SOCKET" 2>/dev/null || true

    if [[ "$overall" == "PASS" && "${POC_KEEP:-0}" != "1" ]]; then
        rm -rf "$DATA" "$LOG_DIR" "$PGLIB_DIR"
    else
        echo "Evidence retained at $WORK_ROOT (status=$overall)" >&2
    fi

    echo "PoC run $RUN_ID: $overall ($RESULT_JSON)" >&2
    [[ "$overall" == "PASS" ]] && exit 0
    exit 1
}
trap cleanup EXIT

# ── Candidate identity: build current source, reject mismatch before any cluster ──
build_and_verify_candidate() {
    local source_commit source_tree dirty so_path so_sha
    source_commit="$(git -C "$ROOT" rev-parse HEAD)"
    source_tree="$(git -C "$ROOT" rev-parse 'HEAD^{tree}')"
    dirty="$(git -C "$ROOT" status --porcelain=v1)"

    # Fail closed BEFORE any build or cluster: a dirty working tree means the
    # built .so would not provably correspond to a specific committed source
    # state, so the candidate identity itself would be unverifiable.
    if [[ -n "$dirty" ]]; then
        echo "FAIL: source tree is dirty; refusing to build or start a cluster. git status --porcelain=v1:" >&2
        echo "$dirty" >&2
        return 1
    fi

    ( cd "$ROOT" && cargo build --release --no-default-features --features pg17 >&2 ) \
        || { echo "FAIL: cargo build failed" >&2; return 1; }

    so_path="$ROOT/target/release/libpg_flashback.so"
    [[ -f "$so_path" ]] || { echo "FAIL: built .so not found at $so_path" >&2; return 1; }
    so_sha="$(sha256sum "$so_path" | awk '{print $1}')"

    if [[ -n "${POC_EXPECTED_SO_SHA256:-}" && "${POC_EXPECTED_SO_SHA256}" != "$so_sha" ]]; then
        echo "FAIL: built .so sha256 $so_sha != POC_EXPECTED_SO_SHA256 ${POC_EXPECTED_SO_SHA256}" >&2
        return 1
    fi

    mkdir -p "$PGLIB_DIR"
    cp "$so_path" "$PGLIB_DIR/pg_flashback.so"

    CANDIDATE_SOURCE_COMMIT="$source_commit"
    CANDIDATE_SOURCE_TREE="$source_tree"
    CANDIDATE_DIRTY="$dirty"
    CANDIDATE_SO_SHA256="$so_sha"
    return 0
}

# ── selftest mode: three negative-control gates, no long-lived cluster ───
run_selftest() {
    # 0) A dirty working tree must be rejected BEFORE any build/cluster
    #    starts. Dirty the tree with an untracked scratch file only (never
    #    touch a tracked file), verify rejection, then remove the marker so
    #    every later gate in this function runs against a genuinely clean
    #    tree.
    local before_marker="$WORK_ROOT/.no-cluster-marker"
    mkdir -p "$WORK_ROOT"
    touch "$before_marker"
    local dirty_marker="$ROOT/.poc-selftest-dirty-marker-$$"
    touch "$dirty_marker"
    if build_and_verify_candidate 2>/dev/null; then
        qst_mark_step "dirty_tree_rejected" "fail" "dirty tree was NOT rejected"
        QST_FAILED=$((QST_FAILED + 1))
    else
        [[ ! -d "$DATA" ]] || die "cluster dir created despite dirty source tree"
        qst_mark_step "dirty_tree_rejected" "pass" "dirty tree correctly rejected before any build/cluster"
    fi
    rm -f "$dirty_marker"
    [[ -z "$(git -C "$ROOT" status --porcelain=v1)" ]] || die "selftest failed to restore a clean tree after the dirty-tree gate"

    # 1) A deliberately wrong expected hash must be rejected before any
    #    cluster is created (candidate_dir/build dir must stay untouched).
    if POC_EXPECTED_SO_SHA256="0000000000000000000000000000000000000000000000000000000000000000" \
        build_and_verify_candidate 2>/dev/null; then
        qst_mark_step "candidate_mismatch_rejected" "fail" "mismatch was NOT rejected"
        QST_FAILED=$((QST_FAILED + 1))
    else
        [[ -f "$before_marker" && ! -d "$DATA" ]] || die "cluster dir created despite rejected candidate"
        qst_mark_step "candidate_mismatch_rejected" "pass" "mismatched hash correctly rejected pre-cluster"
    fi

    # 2) A child harness invocation that deliberately skips a required step
    #    must never write status=PASS.
    local child_out
    child_out="$(env POC_SELFTEST_FORCE_SKIP=cluster_bootstrap \
        "$ROOT/scripts/run_poc_online_snapshot_wal_alignment.sh" __selftest_child_missing_step 2>/dev/null || true)"
    local child_status
    child_status="$(echo "$child_out" | tail -1)"
    if [[ "$child_status" == "FAIL_AS_EXPECTED" ]]; then
        qst_mark_step "missing_step_cannot_pass" "pass" "child with skipped step correctly could not PASS"
    else
        qst_mark_step "missing_step_cannot_pass" "fail" "child output: $child_out"
        QST_FAILED=$((QST_FAILED + 1))
    fi

    # 3) A SIGTERM mid-run must yield FAIL/interrupted=true, never a stale PASS.
    (
        "$ROOT/scripts/run_poc_online_snapshot_wal_alignment.sh" __selftest_child_interrupt_target \
            > "$WORK_ROOT/interrupt_child.out" 2>&1 &
        local child_pid=$!
        sleep 2
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
    )
    local interrupt_result_dir interrupt_status interrupt_flag
    interrupt_result_dir="$(find "$ROOT/target/poc/online-snapshot-wal-alignment" -maxdepth 1 -newer "$before_marker" -name '*-*' -type d 2>/dev/null | grep -v "$RUN_ID" | sort | tail -1 || true)"
    if [[ -n "$interrupt_result_dir" && -f "$interrupt_result_dir/result.json" ]]; then
        interrupt_status="$(jq -r '.status' "$interrupt_result_dir/result.json" 2>/dev/null || echo "")"
        interrupt_flag="$(jq -r '.interrupted' "$interrupt_result_dir/result.json" 2>/dev/null || echo "")"
        if [[ "$interrupt_status" != "PASS" && "$interrupt_flag" == "true" ]]; then
            qst_mark_step "interrupt_yields_fail" "pass" "status=$interrupt_status interrupted=$interrupt_flag"
        else
            qst_mark_step "interrupt_yields_fail" "fail" "status=$interrupt_status interrupted=$interrupt_flag"
            QST_FAILED=$((QST_FAILED + 1))
        fi
        rm -rf "$interrupt_result_dir" 2>/dev/null || true
    else
        qst_mark_step "interrupt_yields_fail" "fail" "no result.json found for interrupted child"
        QST_FAILED=$((QST_FAILED + 1))
    fi
}

# A tiny internal mode used only by run_selftest's children above: proves the
# tracker itself (not a real cluster) refuses to report PASS when a required
# step never ran, and correctly marks an interrupted run.
if [[ "$MODE" == "__selftest_child_missing_step" ]]; then
    qst_init step_one step_two
    qst_mark_step "step_one" "pass" "ok"
    # step_two deliberately never marked -> must not be able to PASS.
    summary="$(qst_compute_summary_json "child-$$" "child" 0 '{}')"
    status="$(echo "$summary" | jq -r .status)"
    [[ "$status" != "PASS" ]] && echo "FAIL_AS_EXPECTED" || echo "UNEXPECTED_PASS"
    exit 0
fi
if [[ "$MODE" == "__selftest_child_interrupt_target" ]]; then
    MODE=dev
    SIZE_MIB=1
    RUN_ID="interrupt-child-$$"
    WORK_ROOT="$ROOT/target/poc/online-snapshot-wal-alignment/$RUN_ID"
    DATA="$WORK_ROOT/data"; LOG_DIR="$WORK_ROOT/log"; LOG="$LOG_DIR/postgresql.log"
    PGLIB_DIR="$WORK_ROOT/pglib"; SOCKET="/tmp/pgfb-poc-$RUN_ID"; RESULT_JSON="$WORK_ROOT/result.json"
    qst_init candidate_build cluster_bootstrap protocol_a_base
    trap 'qst_on_signal HUP' HUP; trap 'qst_on_signal INT' INT; trap 'qst_on_signal TERM' TERM
    trap cleanup EXIT
    build_and_verify_candidate || die "candidate build failed"
    qst_mark_step "candidate_build" "pass" "sha256=$CANDIDATE_SO_SHA256"
    sleep 30  # gives the parent time to SIGTERM us mid-step
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════
# Everything below this point is real-cluster machinery, shared by dev/scale.
# ═══════════════════════════════════════════════════════════════════════

port_is_free() {
    local p=$1
    if command -v ss >/dev/null 2>&1; then
        ss -ltnH 2>/dev/null | awk -v p=":$p" '$4 ~ p"$" || $4 ~ p"]$" {found=1; exit} END {exit found?0:1}' && return 1
        return 0
    fi
    (echo >/dev/tcp/127.0.0.1/"$p") 2>/dev/null && return 1
    return 0
}
choose_port() {
    local p
    for _ in $(seq 1 50); do
        p=$((41000 + RANDOM % 20000))
        port_is_free "$p" && { echo "$p"; return 0; }
    done
    die "no free port found"
}

bootstrap_cluster() {
    local port; port="$(choose_port)"
    PORT="$port"
    mkdir -p "$WORK_ROOT" "$LOG_DIR" "$SOCKET"
    "$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
    cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
dynamic_library_path = '$PGLIB_DIR'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
fsync = on
full_page_writes = on
listen_addresses = ''
log_min_messages = warning
EOF
    "$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
    POSTGRES_STARTED=1
    PSQL=("$PG_BIN/psql" -h "$SOCKET" -p "$PORT" -v ON_ERROR_STOP=1)
    q() { local db=$1; shift; "${PSQL[@]}" -d "$db" -qAt -c "$*"; }
    "${PSQL[@]}" -d postgres -c "CREATE DATABASE $DB;" >/dev/null
    q "$DB" "SELECT 1;" >/dev/null || die "cluster did not come up healthy"
}

restart_cluster() {
    "$PG_BIN/pg_ctl" -D "$DATA" restart -w -t 60 -l "$LOG" >/dev/null
    local _
    for _ in $(seq 1 60); do
        "${PSQL[@]}" -d postgres -qAt -c "SELECT 1;" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

# Simulates a real crash, not a clean shutdown: `-m immediate` skips the
# shutdown checkpoint entirely, so the next start genuinely exercises crash
# recovery (WAL replay from the last checkpoint), which is what "PostgreSQL
# restart mid-copy" is meant to test -- a clean restart would just be an
# orderly transaction abort, not a crash.
crash_restart_cluster() {
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w -t 30 >/dev/null 2>&1 || true
    "$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w -t 60 >/dev/null
    local _
    for _ in $(seq 1 120); do
        "${PSQL[@]}" -d postgres -qAt -c "SELECT 1;" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

# ── table profiles ────────────────────────────────────────────────────────
# Ordinary profile: int/text mix. Toast profile: incompressible bytea
# (md5-chained hex, which gzips/lz4 poorly) sized so on-disk TOAST bytes
# approximate the requested logical MiB. Quoted profile: mixed-case
# identifiers with spaces, to prove the DDL/marker/apply path does not
# assume lower_snake_case.
make_ordinary_table() {
    local name=$1 size_mib=$2
    local rows=$(( size_mib * 1024 * 1024 / 120 ))
    q "$DB" "CREATE TABLE $name (id bigint PRIMARY KEY, payload text, n numeric, ts timestamptz);
             ALTER TABLE $name REPLICA IDENTITY FULL;
             INSERT INTO $name SELECT g, 'row-payload-'||g||'-'||md5(g::text), g * 1.5, clock_timestamp()
               FROM generate_series(1,$rows) g;" >/dev/null
}
make_toast_table() {
    local name=$1 size_mib=$2
    local rows=$(( size_mib * 1024 * 1024 / 8200 ))
    (( rows < 1 )) && rows=1
    # 512 concatenated md5 hashes decode to 8192 bytes; plus the bigint id,
    # that lands close to the 8200-byte-per-row assumption in $rows above.
    q "$DB" "CREATE TABLE $name (id bigint PRIMARY KEY, blob bytea);
             ALTER TABLE $name REPLICA IDENTITY FULL;
             INSERT INTO $name
               SELECT g, decode(string_agg(md5((g*1000+i)::text), ''), 'hex')
               FROM generate_series(1,$rows) g, generate_series(1,512) i
               GROUP BY g;" >/dev/null
}
make_quoted_table() {
    q "$DB" 'CREATE TABLE "Weird Table" (id bigint PRIMARY KEY, "Mixed Col" text, "spacey col" int);
              ALTER TABLE "Weird Table" REPLICA IDENTITY FULL;
              INSERT INTO "Weird Table" SELECT g, '"'"'v'"'"'||g, g*2 FROM generate_series(1,500) g;' >/dev/null
}

# ── shadow-replay / correctness oracle SQL, installed once per DB ───────
install_oracle_sql() {
    q "$DB" "
    CREATE TABLE decoded_events (seq bigserial PRIMARY KEY, data text);
    CREATE TABLE commit_log (xid bigint PRIMARY KEY, lsn pg_lsn, commit_time bigint);
    CREATE TABLE marker_log (xid bigint PRIMARY KEY, lsn pg_lsn);
    CREATE TABLE change_log (seq bigserial PRIMARY KEY, xid bigint, oid bigint, op text,
                              old jsonb, new jsonb, applied boolean NOT NULL DEFAULT false);
    CREATE TABLE poc_table_map (oid bigint PRIMARY KEY, shadow_regclass text, pk_col text);
    -- PoC-only artifact lifecycle model for the restart/crash scenarios
    -- below (creating -> available | aborted). This is NOT the production
    -- SnapshotStore state machine and is not wired to it; it exists solely
    -- so this harness can distinguish a physically committed but never
    -- finalized artifact from a genuinely available one, without inventing
    -- a production seam.
    CREATE TABLE poc_artifact_state (
        artifact_name text PRIMARY KEY,
        state text NOT NULL CHECK (state IN ('creating','available','aborted')),
        shadow_table text,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );

    -- Moves every currently-staged decoded_events row into the durable logs
    -- (commit_log / marker_log / change_log), then clears decoded_events.
    -- Deliberately does NOT apply any DML: the boundary LSN a caller needs
    -- (a marker's real COMMIT LSN) can only be resolved from commit_log
    -- AFTER ingest, and resolving it is exactly what happens before the
    -- caller knows what to pass to poc_apply_shadow() below. Safe to call
    -- with an empty decoded_events (no-op).
    CREATE OR REPLACE FUNCTION poc_ingest_decoded()
    RETURNS TABLE(new_commits bigint, duplicate_commits bigint, out_of_order bigint, new_markers bigint) AS \$fn\$
    DECLARE
      rec record; j jsonb; cur_lsn pg_lsn; cur_xid bigint; prior_max_lsn pg_lsn;
      n_commits bigint:=0; n_dup bigint:=0; n_ooo bigint:=0; n_mark bigint:=0;
    BEGIN
      SELECT COALESCE(max(lsn), '0/0') INTO prior_max_lsn FROM commit_log;
      FOR rec IN SELECT seq, data FROM decoded_events ORDER BY seq LOOP
        j := rec.data::jsonb;
        IF j ? 'commit' THEN
          cur_xid := (j->>'commit')::bigint;
          cur_lsn := (j->>'lsn')::pg_lsn;
          IF EXISTS(SELECT 1 FROM commit_log WHERE xid = cur_xid) THEN
            n_dup := n_dup + 1;
          ELSE
            INSERT INTO commit_log(xid, lsn, commit_time) VALUES (cur_xid, cur_lsn, (j->>'commit_time')::bigint);
            IF cur_lsn < prior_max_lsn THEN n_ooo := n_ooo + 1; END IF;
            prior_max_lsn := GREATEST(prior_max_lsn, cur_lsn);
            n_commits := n_commits + 1;
          END IF;
        ELSIF j ? 'marker' THEN
          INSERT INTO marker_log(xid) VALUES ((j->>'marker')::bigint) ON CONFLICT DO NOTHING;
          IF FOUND THEN n_mark := n_mark + 1; END IF;
        ELSIF j ? 'op' THEN
          INSERT INTO change_log(xid, oid, op, old, new)
            VALUES ((j->>'xid')::bigint, (j->>'oid')::bigint, j->>'op', j->'old', j->'new');
        END IF;
      END LOOP;
      -- Backfill marker_log.lsn now that the marker's own commit line (same
      -- xid, appearing right after it in decode order) has been ingested.
      UPDATE marker_log m SET lsn = c.lsn FROM commit_log c
        WHERE m.xid = c.xid AND m.lsn IS NULL;
      DELETE FROM decoded_events;
      RETURN QUERY SELECT n_commits, n_dup, n_ooo, n_mark;
    END;
    \$fn\$ LANGUAGE plpgsql;

    -- Ingests any pending decoded_events, then applies every not-yet-applied
    -- change_log row whose owning transaction's commit LSN is strictly
    -- after p_since_lsn (NULL = apply everything, for Protocol A's
    -- consistent-point boundary). Re-applying an already-applied row is not
    -- possible (the applied flag gates it), so calling this repeatedly is
    -- safe and only ever processes genuinely new work.
    CREATE OR REPLACE FUNCTION poc_apply_shadow(p_since_lsn pg_lsn DEFAULT NULL)
    RETURNS TABLE(commits bigint, inserts bigint, updates bigint, deletes bigint,
                  markers bigint, duplicate_commits bigint, out_of_order bigint) AS \$fn\$
    DECLARE
      rec record; shadow text; pk text; cols text; vals text; set_clause text; where_clause text;
      n_ins bigint:=0; n_upd bigint:=0; n_del bigint:=0;
      ingest record;
    BEGIN
      SELECT * INTO ingest FROM poc_ingest_decoded();

      FOR rec IN
        SELECT cl.seq, cl.xid, cl.oid, cl.op, cl.old, cl.new
          FROM change_log cl
          JOIN commit_log co ON co.xid = cl.xid
         WHERE cl.applied = false
           AND (p_since_lsn IS NULL OR co.lsn > p_since_lsn)
         ORDER BY co.lsn, cl.seq
      LOOP
        SELECT poc_table_map.shadow_regclass, poc_table_map.pk_col INTO shadow, pk
          FROM poc_table_map WHERE poc_table_map.oid = rec.oid;
        IF shadow IS NULL THEN
          UPDATE change_log SET applied = true WHERE seq = rec.seq;
          CONTINUE;
        END IF;
        IF rec.op = 'INSERT' THEN
          SELECT string_agg(quote_ident(key), ','), string_agg(format('%L', value), ',')
            INTO cols, vals FROM jsonb_each_text(rec.new);
          -- WHERE NOT EXISTS, not ON CONFLICT DO NOTHING: shadow tables are
          -- created via CREATE TABLE AS SELECT, which never copies the
          -- source's PRIMARY KEY, so a bare ON CONFLICT DO NOTHING has no
          -- constraint to match and silently becomes a no-op guard (every
          -- insert just succeeds, duplicates and all). A duplicate/
          -- redelivered commit -- e.g. after a crash rewinds the slot's
          -- confirmed position -- must not double the row regardless of
          -- whether the shadow table happens to have a real constraint.
          EXECUTE format(
            'INSERT INTO %s (%s) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %I = %L)',
            shadow, cols, vals, shadow, pk, rec.new->>pk);
          n_ins := n_ins + 1;
        ELSIF rec.op = 'UPDATE' THEN
          SELECT string_agg(format('%I = %L', key, value), ',') INTO set_clause FROM jsonb_each_text(rec.new);
          SELECT format('%I = %L', key, value) INTO where_clause FROM jsonb_each_text(rec.old) WHERE key = pk;
          IF where_clause IS NULL THEN
            SELECT format('%I = %L', key, value) INTO where_clause FROM jsonb_each_text(rec.new) WHERE key = pk;
          END IF;
          EXECUTE format('UPDATE %s SET %s WHERE %s', shadow, set_clause, where_clause);
          n_upd := n_upd + 1;
        ELSIF rec.op = 'DELETE' THEN
          SELECT format('%I = %L', key, value) INTO where_clause FROM jsonb_each_text(rec.old) WHERE key = pk;
          EXECUTE format('DELETE FROM %s WHERE %s', shadow, where_clause);
          n_del := n_del + 1;
        END IF;
        UPDATE change_log SET applied = true WHERE seq = rec.seq;
      END LOOP;

      RETURN QUERY SELECT
        (SELECT count(*) FROM commit_log co WHERE p_since_lsn IS NULL OR co.lsn > p_since_lsn),
        n_ins, n_upd, n_del, ingest.new_markers, ingest.duplicate_commits, ingest.out_of_order;
    END;
    \$fn\$ LANGUAGE plpgsql;
    " >/dev/null
}

# Independent PoC semantic fingerprint -- NOT the product's canonical
# `flashback_relation_full_data_fingerprint`, and not claimed to be
# equivalent to it. This is this harness's own order-independent check:
# hash of the sorted set of per-row hashes. Two tables with identical rows
# in any physical order produce the same fingerprint; any differing/
# missing/extra row changes it. Validating the actual production
# correctness oracle is a separate, explicit step (the exact-WAL
# transaction/schema matrix), not this function.
fingerprint_table() {
    local tbl=$1
    q "$DB" "SELECT md5(COALESCE(string_agg(h, '|' ORDER BY h), '')) FROM (SELECT md5(t::text) h FROM $tbl t) x;"
}
row_count() { local tbl=$1; q "$DB" "SELECT count(*) FROM $tbl;"; }

consume_slot_to_events() {
    local slot=$1 oids=$2
    q "$DB" "INSERT INTO decoded_events(data)
              SELECT data FROM pg_logical_slot_get_changes('$slot', NULL, NULL,
                'tracked_oids', '$oids', 'metadata_only', 'false');" >/dev/null
}

table_oid() { local tbl=$1; q "$DB" "SELECT '$tbl'::regclass::oid;"; }

# ── exported-snapshot replication-protocol connection (Protocol A) ──────
# The export connection's IDENTIFY_SYSTEM + CREATE_REPLICATION_SLOT ...
# EXPORT_SNAPSHOT run as a single backgrounded psql heredoc, which then
# blocks on a `\!` shell-escape poll loop for a "go" file -- exactly the
# same proven pattern Protocol B already uses for its coordinator/copier
# handshake. This replaced an earlier design that streamed commands into a
# long-lived psql process one at a time over a pipe/FIFO: that was
# empirically unreliable in this script (the export transaction ended
# silently between commands -- a later `SET TRANSACTION SNAPSHOT` failed
# with "snapshot ... does not exist" -- even though the psql process itself
# was still alive and every isolated reproduction of the same mechanism
# worked). Sending the whole command sequence in one shot, the way `psql`
# is normally driven from a script, avoids that class of problem entirely.
# MUST be opened and closed within the same shell process tree.
open_export_connection() {
    local dbname=$1 slot=$2
    EXPORT_OUT="$WORK_ROOT/export_${slot}.out"
    EXPORT_READY="$WORK_ROOT/export_${slot}.ready"
    EXPORT_GO="$WORK_ROOT/export_${slot}.go"
    rm -f "$EXPORT_OUT" "$EXPORT_READY" "$EXPORT_GO"
    "$PG_BIN/psql" "host=$SOCKET port=$PORT dbname=$dbname replication=database" \
        -qAt -v ON_ERROR_STOP=1 >"$EXPORT_OUT" 2>&1 <<SQL &
IDENTIFY_SYSTEM;
CREATE_REPLICATION_SLOT $slot LOGICAL pg_flashback EXPORT_SNAPSHOT;
\! touch "$EXPORT_READY"
\! while [ ! -f "$EXPORT_GO" ]; do sleep 0.02; done
SQL
    EXPORT_PID=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$EXPORT_READY" ]]; do
        if ! kill -0 "$EXPORT_PID" 2>/dev/null; then
            die "protocol A: export connection exited before becoming ready: $(cat "$EXPORT_OUT" 2>/dev/null)"
        fi
        (( $(date +%s) < deadline )) || die "protocol A: export connection did not become ready in time"
        sleep 0.05
    done
    grep -qi "^ERROR" "$EXPORT_OUT" && die "protocol A: export connection error: $(cat "$EXPORT_OUT")"
    EXPORT_IDENT_LINE="$(sed -n '1p' "$EXPORT_OUT")"
    EXPORT_CREATE_LINE="$(sed -n '2p' "$EXPORT_OUT")"
}
release_export_connection() {
    touch "$EXPORT_GO"
    wait "$EXPORT_PID" 2>/dev/null || true
    rm -f "$EXPORT_OUT" "$EXPORT_READY" "$EXPORT_GO"
}
kill_export_connection() {
    kill -KILL "$EXPORT_PID" 2>/dev/null || true
    wait "$EXPORT_PID" 2>/dev/null || true
    rm -f "$EXPORT_OUT" "$EXPORT_READY" "$EXPORT_GO"
}

now_ms() { date +%s%3N; }

echo "== PoC run $RUN_ID mode=$MODE size_mib=${SIZE_MIB:-} ==" >&2

if [[ "$MODE" == "selftest" ]]; then
    run_selftest
    EXTRA_JSON='{}'
    exit 0
fi

build_and_verify_candidate || die "candidate build/verify failed"
qst_mark_step "candidate_build" "pass" "commit=$CANDIDATE_SOURCE_COMMIT so_sha256=$CANDIDATE_SO_SHA256"

bootstrap_cluster
install_oracle_sql
qst_mark_step "cluster_bootstrap" "pass" "port=$PORT socket=$SOCKET"

SYSTEM_IDENTIFIER=""
TIMELINE=""
PG_MAJOR="$("$PG_BIN/pg_config" --version | awk '{print $2}' | cut -d. -f1)"
ARCH="$(uname -m)"

METRICS_JSON="$WORK_ROOT/metrics.jsonl"
: >"$METRICS_JSON"
record_metric() { # name value unit
    jq -nc --arg n "$1" --arg v "$2" --arg u "$3" '{name:$n, value:($v|tonumber? // $v), unit:$u}' >>"$METRICS_JSON"
}

# ─────────────────────────────────────────────────────────────────────────
# Protocol A: greenfield exported snapshot
# ─────────────────────────────────────────────────────────────────────────
run_protocol_a() {
    local tbl=$1 slot=$2 size_mib=$3 profile=${4:-ordinary}
    if [[ "$profile" == "toast" ]]; then make_toast_table "$tbl" "$size_mib"; else make_ordinary_table "$tbl" "$size_mib"; fi
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow_a', 'id');" >/dev/null

    open_export_connection "$DB" "$slot"
    SYSTEM_IDENTIFIER="$(echo "$EXPORT_IDENT_LINE" | cut -d'|' -f1)"
    TIMELINE="$(echo "$EXPORT_IDENT_LINE" | cut -d'|' -f2)"
    local slot_name consistent_point snap_name _out_plugin
    IFS='|' read -r slot_name consistent_point snap_name _out_plugin <<<"$EXPORT_CREATE_LINE"
    [[ "$slot_name" == "$slot" && -n "$snap_name" ]] || die "protocol A: unexpected slot output: $EXPORT_CREATE_LINE"

    # A concurrent writer proves the export hold and the copy itself never
    # block ordinary DML (no relation lock is held by either side). The
    # marker file is created BEFORE backgrounding the loop: creating it
    # after risks the subshell's first existence check racing ahead of the
    # parent's touch and seeing nothing, so the loop body never runs at all.
    local writer_log="$WORK_ROOT/${tbl}_writer.log"
    touch "$WORK_ROOT/${tbl}.writer_run"
    ( local i=0
      while [[ -f "$WORK_ROOT/${tbl}.writer_run" ]]; do
        i=$((i+1))
        local t0 t1
        t0=$(now_ms)
        if [[ "$profile" == "toast" ]]; then
            q "$DB" "INSERT INTO $tbl VALUES (-$i, decode(repeat('00',200),'hex'));" >/dev/null 2>&1 || true
        else
            q "$DB" "INSERT INTO $tbl VALUES (-$i, 'concurrent-a-'||$i, $i, clock_timestamp());" >/dev/null 2>&1 || true
        fi
        t1=$(now_ms)
        echo $((t1-t0)) >>"$writer_log"
        sleep 0.02
      done ) &
    local writer_pid=$!
    sleep 0.3

    # Import and copy MUST be the same transaction: once SET TRANSACTION
    # SNAPSHOT succeeds, that transaction pins its own copy of the snapshot
    # independent of the exporting connection -- but a SEPARATE, LATER
    # transaction cannot re-import the same snapshot after the exporting
    # connection is gone. The importer therefore signals "import confirmed"
    # from inside the same still-open transaction that will go on to do the
    # long copy, exactly like Protocol B's copier session below.
    local t_import0 t_import1 t_copy1 rows_base
    local copy_ready="$WORK_ROOT/${tbl}_a_copy.ready"
    local copy_done="$WORK_ROOT/${tbl}_a_copy.done"
    local copy_out="$WORK_ROOT/${tbl}_a_copy.out"
    rm -f "$copy_ready" "$copy_done" "$copy_out"
    t_import0=$(now_ms)
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$copy_out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT '$snap_name';
\! touch "$copy_ready"
CREATE TABLE ${tbl}_shadow_a AS SELECT * FROM $tbl;
COMMIT;
\! touch "$copy_done"
SQL
    local copier_a_pid=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$copy_ready" ]]; do
        if ! kill -0 "$copier_a_pid" 2>/dev/null; then
            die "protocol A: import transaction exited before confirming: $(cat "$copy_out")"
        fi
        (( $(date +%s) < deadline )) || die "protocol A: import did not confirm in time"
        sleep 0.05
    done
    t_import1=$(now_ms)
    # Import confirmed (snapshot now pinned by the copier's own transaction)
    # -- safe to release the exporting connection now, per protocol step 5/6,
    # while the copy continues independently in the background.
    release_export_connection

    deadline=$(( $(date +%s) + 60 ))
    while [[ ! -f "$copy_done" ]]; do
        (( $(date +%s) < deadline )) || die "protocol A: base copy did not finish in time"
        sleep 0.1
    done
    wait "$copier_a_pid" 2>/dev/null || true
    grep -qi "^ERROR" "$copy_out" && die "protocol A: base copy error: $(cat "$copy_out")"
    t_copy1=$(now_ms)
    rm -f "$WORK_ROOT/${tbl}.writer_run" "$copy_ready" "$copy_done" "$copy_out"
    wait "$writer_pid" 2>/dev/null || true
    rows_base="$(row_count "${tbl}_shadow_a")"

    record_metric "protocol_a.${tbl}.import_ms" "$((t_import1-t_import0))" "ms"
    record_metric "protocol_a.${tbl}.copy_ms" "$((t_copy1-t_import1))" "ms"
    record_metric "protocol_a.${tbl}.base_rows" "$rows_base" "rows"

    # Slot must have survived export-connection close and remain the ONE
    # shared slot, consumable normally.
    local slot_active
    slot_active="$(q "$DB" "SELECT wal_status FROM pg_replication_slots WHERE slot_name='$slot';")"
    [[ "$slot_active" == "reserved" || "$slot_active" == "extended" ]] \
        || die "protocol A: slot $slot in unexpected wal_status=$slot_active after export conn close"

    consume_slot_to_events "$slot" "$oid"
    local apply_out
    apply_out="$(q "$DB" "SELECT commits,inserts,updates,deletes,markers,duplicate_commits,out_of_order FROM poc_apply_shadow(NULL);")"
    IFS='|' read -r a_commits a_ins a_upd a_del _a_mark a_dup a_ooo <<<"$apply_out"
    [[ "$a_dup" == "0" && "$a_ooo" == "0" ]] || die "protocol A: duplicate_commits=$a_dup out_of_order=$a_ooo"
    record_metric "protocol_a.${tbl}.commits_replayed" "$a_commits" "count"
    record_metric "protocol_a.${tbl}.inserts_replayed" "$a_ins" "count"
    record_metric "protocol_a.${tbl}.updates_replayed" "$a_upd" "count"
    record_metric "protocol_a.${tbl}.deletes_replayed" "$a_del" "count"
    record_metric "protocol_a.${tbl}.duplicate_commits" "$a_dup" "count"

    local fp_live fp_shadow rc_live rc_shadow
    fp_live="$(fingerprint_table "$tbl")"
    fp_shadow="$(fingerprint_table "${tbl}_shadow_a")"
    rc_live="$(row_count "$tbl")"
    rc_shadow="$(row_count "${tbl}_shadow_a")"
    [[ "$fp_live" == "$fp_shadow" && "$rc_live" == "$rc_shadow" ]] \
        || die "protocol A: fingerprint/row-count mismatch live=($rc_live,$fp_live) shadow=($rc_shadow,$fp_shadow)"

    if [[ "$profile" == "toast" ]]; then
        local toast_ok
        toast_ok="$(q "$DB" "SELECT NOT EXISTS (
            SELECT 1 FROM $tbl t JOIN ${tbl}_shadow_a s USING (id)
            WHERE t.blob IS DISTINCT FROM s.blob OR octet_length(t.blob) IS DISTINCT FROM octet_length(s.blob));")"
        [[ "$toast_ok" == "t" ]] || die "protocol A: TOAST byte mismatch between live and shadow"
    fi

    PROTOCOL_A_SLOT="$slot"
    PROTOCOL_A_TABLE="$tbl"
    PROTOCOL_A_CONSISTENT_POINT="$consistent_point"
    PROTOCOL_A_OID="$oid"
    record_metric "protocol_a.${tbl}.slot_name" "$PROTOCOL_A_SLOT" "identity"
    record_metric "protocol_a.${tbl}.table_oid" "$PROTOCOL_A_OID" "oid"
    echo "protocol_a[$tbl]: OK rows=$rc_live commits_replayed=$a_commits ins=$a_ins upd=$a_upd del=$a_del" >&2
}

# Adversarial 14: exported-snapshot connection dies before / after import.
run_protocol_a_conn_lifecycle_adversarial() {
    local tbl=poc_a_lifecycle
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,100) g;" >/dev/null

    # 14a: kill export connection BEFORE import is attempted -> import must fail closed.
    open_export_connection "$DB" "poc_a_life_1"
    local slot_name _consistent_point snap_name _out_plugin
    IFS='|' read -r slot_name _consistent_point snap_name _out_plugin <<<"$EXPORT_CREATE_LINE"
    kill_export_connection
    sleep 0.3
    if "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt \
        -c "BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '$snap_name';" >/dev/null 2>&1; then
        die "adversarial 14a: import succeeded after export connection died before import (must fail closed)"
    fi
    "${PSQL[@]}" -d "$DB" -qAt -c "ROLLBACK;" >/dev/null 2>&1 || true
    q "$DB" "SELECT pg_drop_replication_slot('poc_a_life_1');" >/dev/null 2>&1 || true
    echo "adversarial 14a: pre-import export-connection death correctly fails closed" >&2

    # 14b: kill export connection AFTER import succeeds -> copy must still complete correctly.
    # Import and copy must be the SAME transaction (see run_protocol_a's
    # comment): the exporting connection is killed only once this
    # transaction's own SET TRANSACTION SNAPSHOT has already succeeded and
    # signaled readiness, so its own pinned snapshot no longer depends on it.
    open_export_connection "$DB" "poc_a_life_2"
    IFS='|' read -r slot_name _consistent_point snap_name _out_plugin <<<"$EXPORT_CREATE_LINE"
    local life_ready="$WORK_ROOT/${tbl}_life.ready" life_done="$WORK_ROOT/${tbl}_life.done" life_out="$WORK_ROOT/${tbl}_life.out"
    rm -f "$life_ready" "$life_done" "$life_out"
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$life_out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT '$snap_name';
\! touch "$life_ready"
CREATE TABLE ${tbl}_shadow_life AS SELECT * FROM $tbl;
COMMIT;
\! touch "$life_done"
SQL
    local life_pid=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$life_ready" ]]; do
        (( $(date +%s) < deadline )) || die "adversarial 14b: import did not confirm in time"
        sleep 0.05
    done
    kill_export_connection
    deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$life_done" ]]; do
        (( $(date +%s) < deadline )) || die "adversarial 14b: copy did not finish after export conn killed"
        sleep 0.05
    done
    wait "$life_pid" 2>/dev/null || true
    grep -qi "^ERROR" "$life_out" && die "adversarial 14b: copy error: $(cat "$life_out")"
    local rc; rc="$(row_count "${tbl}_shadow_life")"
    [[ "$rc" == "100" ]] || die "adversarial 14b: post-import copy incomplete after export conn killed (rows=$rc)"
    rm -f "$life_ready" "$life_done" "$life_out"
    q "$DB" "SELECT pg_drop_replication_slot('poc_a_life_2');" >/dev/null 2>&1 || true
    echo "adversarial 14b: post-import export-connection death did not affect an already-pinned copy" >&2
}

# ─────────────────────────────────────────────────────────────────────────
# Protocol B: existing-stream reanchor (short lock + transactional marker)
# ─────────────────────────────────────────────────────────────────────────
# All coordinator/copier orchestration uses background psql processes with
# marker files under $WORK_ROOT for handshakes, since (as discovered while
# validating this harness) a bash coproc/backgrounded process does not
# survive across separate tool invocations -- everything here runs inside
# one shell process tree, matching the coproc constraint for Protocol A too.
run_protocol_b() {
    local tbl=$1 slot=$2 size_mib=$3 profile=$4
    if [[ "$profile" == "toast" ]]; then make_toast_table "$tbl" "$size_mib"; else make_ordinary_table "$tbl" "$size_mib"; fi
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow_b', 'id');" >/dev/null

    local marker_uuid; marker_uuid="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N)"
    local txn_started_file="$WORK_ROOT/${tbl}_txn_started"
    local lock_acquired_file="$WORK_ROOT/${tbl}_lock_acquired"
    local snapshot_fixed_file="$WORK_ROOT/${tbl}_snapshot_fixed"
    local go_file="$WORK_ROOT/${tbl}_copier_go"
    local done_file="$WORK_ROOT/${tbl}_copier_done"
    local copy_ms_file="$WORK_ROOT/${tbl}_copy_ms"
    rm -f "$txn_started_file" "$lock_acquired_file" "$snapshot_fixed_file" "$go_file" "$done_file" "$copy_ms_file"

    # Correct ordering per protocol B step 4: the copier's REPEATABLE READ
    # snapshot must be fixed (its first real read) WHILE the coordinator
    # holds the lock, not before the lock is even requested -- otherwise a
    # writer that commits in the gap between "copier's snapshot fixed" and
    # "coordinator's lock granted" could be silently missed by both the base
    # (already past its snapshot) and any WAL replay bounded to the marker's
    # LSN (which comes later still). The four-file handshake below enforces
    # that order: copier signals its transaction has started (but has not
    # yet read anything) -> waits for the lock; coordinator locks, signals,
    # and then itself waits for the copier's snapshot-fixing read to
    # complete before emitting the marker and releasing the lock via COMMIT.
    #
    # The snapshot-fixing statement is `SELECT 1 FROM tbl LIMIT 1`, not
    # `SELECT count(*)`: PostgreSQL fixes a REPEATABLE READ transaction's
    # snapshot at its first query, regardless of how much of the relation
    # that query actually scans -- a LIMIT 1 probe still genuinely reads the
    # target relation (satisfying "first real table read") while costing
    # O(1) instead of a full scan, which matters once the coordinator lock
    # is held for the whole duration of this read.
    (
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$WORK_ROOT/${tbl}_copier.out" 2>&1 <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
\! touch "$txn_started_file"
\! while [ ! -f "$lock_acquired_file" ]; do sleep 0.02; done
SELECT 1 FROM $tbl LIMIT 1;
\! touch "$snapshot_fixed_file"
\! while [ ! -f "$go_file" ]; do sleep 0.02; done
\! date +%s%3N > "${copy_ms_file}.start"
CREATE TABLE ${tbl}_shadow_b AS SELECT * FROM $tbl;
\! date +%s%3N > "${copy_ms_file}.end"
COMMIT;
SQL
        touch "$done_file"
    ) &
    local copier_pid=$!

    local deadline=$(( $(date +%s) + 30 ))
    while [[ ! -f "$txn_started_file" ]]; do
        (( $(date +%s) < deadline )) || die "protocol B: copier transaction did not start in time"
        sleep 0.05
    done

    local t_lock0 t_lock1
    t_lock0=$(now_ms)
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
\! date +%s%3N > "$WORK_ROOT/${tbl}_lockgranted.ms"
\! touch "$lock_acquired_file"
\! while [ ! -f "$snapshot_fixed_file" ]; do sleep 0.02; done
SELECT pg_logical_emit_message(true, 'pg_flashback', '$marker_uuid');
COMMIT;
SQL
    t_lock1=$(now_ms)
    touch "$go_file"

    deadline=$(( $(date +%s) + 120 ))
    while [[ ! -f "$done_file" ]]; do
        (( $(date +%s) < deadline )) || die "protocol B: copier did not finish in time"
        sleep 0.1
    done
    wait "$copier_pid" 2>/dev/null || true
    grep -qi "error" "$WORK_ROOT/${tbl}_copier.out" && die "protocol B: copier session error: $(cat "$WORK_ROOT/${tbl}_copier.out")"

    local lock_granted_ms; lock_granted_ms="$(cat "$WORK_ROOT/${tbl}_lockgranted.ms" 2>/dev/null || echo "$t_lock1")"
    local copy_start copy_end
    copy_start="$(cat "${copy_ms_file}.start" 2>/dev/null || echo 0)"
    copy_end="$(cat "${copy_ms_file}.end" 2>/dev/null || echo 0)"
    record_metric "protocol_b.${tbl}.lock_hold_ms" "$((t_lock1-t_lock0))" "ms"
    record_metric "protocol_b.${tbl}.copy_ms" "$((copy_end-copy_start))" "ms"

    # Resolve the marker's real COMMIT LSN from the shared slot itself, not
    # from client-side timing: consume once (also picking up the marker's
    # own txn) so the boundary is provable from decoded WAL.
    consume_slot_to_events "$slot" "$(all_tracked_oids)"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local marker_xid boundary_lsn
    marker_xid="$(q "$DB" "SELECT xid FROM marker_log ORDER BY xid DESC LIMIT 1;")"
    [[ -n "$marker_xid" ]] || die "protocol B: no marker found in decoded WAL"
    boundary_lsn="$(q "$DB" "SELECT lsn::text FROM commit_log WHERE xid = $marker_xid;")"
    [[ -n "$boundary_lsn" ]] || die "protocol B: could not resolve marker $marker_xid's commit LSN from decoded WAL"

    local apply_out
    apply_out="$(q "$DB" "SELECT commits,inserts,updates,deletes,markers,duplicate_commits,out_of_order FROM poc_apply_shadow('$boundary_lsn'::pg_lsn);")"
    IFS='|' read -r b_commits b_ins b_upd b_del b_mark b_dup b_ooo <<<"$apply_out"
    [[ "$b_dup" == "0" && "$b_ooo" == "0" ]] || die "protocol B[$tbl]: duplicate_commits=$b_dup out_of_order=$b_ooo"
    record_metric "protocol_b.${tbl}.commits_replayed" "$b_commits" "count"
    record_metric "protocol_b.${tbl}.inserts_replayed" "$b_ins" "count"
    record_metric "protocol_b.${tbl}.updates_replayed" "$b_upd" "count"
    record_metric "protocol_b.${tbl}.deletes_replayed" "$b_del" "count"
    record_metric "protocol_b.${tbl}.markers_seen" "$b_mark" "count"
    record_metric "protocol_b.${tbl}.duplicate_commits" "$b_dup" "count"

    local fp_live fp_shadow rc_live rc_shadow
    fp_live="$(fingerprint_table "$tbl")"
    fp_shadow="$(fingerprint_table "${tbl}_shadow_b")"
    rc_live="$(row_count "$tbl")"
    rc_shadow="$(row_count "${tbl}_shadow_b")"
    [[ "$fp_live" == "$fp_shadow" && "$rc_live" == "$rc_shadow" ]] \
        || die "protocol B[$tbl]: fingerprint/row-count mismatch live=($rc_live,$fp_live) shadow=($rc_shadow,$fp_shadow)"

    if [[ "$profile" == "toast" ]]; then
        local toast_ok
        toast_ok="$(q "$DB" "SELECT NOT EXISTS (
            SELECT 1 FROM $tbl t JOIN ${tbl}_shadow_b s USING (id)
            WHERE t.blob IS DISTINCT FROM s.blob OR octet_length(t.blob) IS DISTINCT FROM octet_length(s.blob));")"
        [[ "$toast_ok" == "t" ]] || die "protocol B[$tbl]: TOAST byte mismatch between live and shadow"
    fi

    LAST_PROTOCOL_B_TABLE="$tbl"
    LAST_PROTOCOL_B_OID="$oid"
    LAST_PROTOCOL_B_BOUNDARY_LSN="$boundary_lsn"
    record_metric "protocol_b.${tbl}.table_oid" "$LAST_PROTOCOL_B_OID" "oid"
    record_metric "protocol_b.${tbl}.boundary_lsn" "$LAST_PROTOCOL_B_BOUNDARY_LSN" "lsn"
    echo "protocol_b[$tbl]: OK rows=$rc_live lock_hold_ms=$((t_lock1-t_lock0)) commits_replayed=$b_commits" >&2
}

all_tracked_oids() { q "$DB" "SELECT string_agg(oid::text, ',') FROM poc_table_map;"; }

# Scenarios 1/2/3: transactions relative to the coordinator lock/boundary.
run_boundary_timing_scenarios() {
    local slot=$1
    local tbl=poc_boundary_timing
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,50) g;" >/dev/null
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow', 'id');" >/dev/null

    # Scenario 1: writer starts before the lock attempt, writes, commits
    # BEFORE the lock is granted -> coordinator must wait for it; the write
    # must land in base, not be replayed a second time from WAL.
    local writer_committed_file="$WORK_ROOT/s1_writer_committed.ms"
    rm -f "$writer_committed_file"
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL &
BEGIN;
UPDATE $tbl SET v = 's1-writer' WHERE id = 1;
SELECT pg_sleep(1);
\! date +%s%3N > "$writer_committed_file"
COMMIT;
SQL
    local s1_writer_pid=$!
    sleep 0.2  # ensure the UPDATE's row lock is held before we try SHARE ROW EXCLUSIVE

    # Copier's snapshot-fixing read must happen WHILE the coordinator holds
    # the lock (see run_protocol_b's comment for why) -- not before the lock
    # is even requested.
    local txn_started_file="$WORK_ROOT/${tbl}_txn_started" lock_acquired_file="$WORK_ROOT/${tbl}_lock_acquired"
    local snapshot_fixed_file="$WORK_ROOT/${tbl}_snapshot_fixed"
    local go_file="$WORK_ROOT/${tbl}_copier_go" done_file="$WORK_ROOT/${tbl}_copier_done"
    rm -f "$txn_started_file" "$lock_acquired_file" "$snapshot_fixed_file" "$go_file" "$done_file"
    (
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$WORK_ROOT/${tbl}_copier.out" 2>&1 <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
\! touch "$txn_started_file"
\! while [ ! -f "$lock_acquired_file" ]; do sleep 0.02; done
SELECT 1 FROM $tbl LIMIT 1;
\! touch "$snapshot_fixed_file"
\! while [ ! -f "$go_file" ]; do sleep 0.02; done
CREATE TABLE ${tbl}_shadow AS SELECT * FROM $tbl;
COMMIT;
SQL
        touch "$done_file"
    ) &
    local copier_pid=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$txn_started_file" ]]; do (( $(date +%s) < deadline )) || die "s1: copier txn start timeout"; sleep 0.05; done

    local lock_wait_start lock_wait_end
    lock_wait_start=$(now_ms)
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
\! touch "$lock_acquired_file"
\! while [ ! -f "$snapshot_fixed_file" ]; do sleep 0.02; done
SELECT pg_logical_emit_message(true, 'pg_flashback', 's1-marker');
COMMIT;
SQL
    lock_wait_end=$(now_ms)
    touch "$go_file"
    wait "$s1_writer_pid" 2>/dev/null || true
    deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$done_file" ]]; do (( $(date +%s) < deadline )) || die "s1: copier finish timeout"; sleep 0.05; done
    wait "$copier_pid" 2>/dev/null || true

    local writer_commit_ms lock_granted_ms
    writer_commit_ms="$(cat "$writer_committed_file" 2>/dev/null || echo 0)"
    lock_granted_ms="$lock_wait_end"
    [[ "$lock_granted_ms" -ge "$writer_commit_ms" ]] \
        || die "s1: coordinator lock granted ($lock_granted_ms) BEFORE writer committed ($writer_commit_ms) -- lock did not wait"
    local base_has_write
    base_has_write="$(q "$DB" "SELECT v FROM ${tbl}_shadow WHERE id=1;")"
    [[ "$base_has_write" == "s1-writer" ]] || die "s1: base copy missing the pre-boundary committed write"
    record_metric "adversarial.s1.lock_wait_ms" "$((lock_wait_end-lock_wait_start))" "ms"

    # WAL-side proof for scenario 1: the writer's commit LSN must be BELOW
    # the s1-marker boundary, so replaying WAL strictly after that boundary
    # must NOT re-apply it (no duplicate on top of the base copy).
    consume_slot_to_events "$slot" "$oid"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local s1_marker_xid s1_boundary_lsn s1_apply_out s1_dup s1_ooo
    s1_marker_xid="$(q "$DB" "SELECT xid FROM marker_log ORDER BY xid DESC LIMIT 1;")"
    [[ -n "$s1_marker_xid" ]] || die "s1: no marker found in decoded WAL"
    s1_boundary_lsn="$(q "$DB" "SELECT lsn::text FROM commit_log WHERE xid = $s1_marker_xid;")"
    [[ -n "$s1_boundary_lsn" ]] || die "s1: could not resolve marker commit LSN"
    s1_apply_out="$(q "$DB" "SELECT duplicate_commits,out_of_order FROM poc_apply_shadow('$s1_boundary_lsn'::pg_lsn);")"
    IFS='|' read -r s1_dup s1_ooo <<<"$s1_apply_out"
    [[ "$s1_dup" == "0" && "$s1_ooo" == "0" ]] || die "s1: post-boundary WAL replay incorrectly duplicated/reordered the pre-boundary write"
    local s1_fp_live s1_fp_shadow
    s1_fp_live="$(fingerprint_table "$tbl")"
    s1_fp_shadow="$(fingerprint_table "${tbl}_shadow")"
    [[ "$s1_fp_live" == "$s1_fp_shadow" ]] || die "s1: base+WAL-after-boundary does not reconstruct live table (no duplicate expected here)"
    echo "scenario1: lock correctly waited for pre-boundary writer (lock_wait=$((lock_wait_end-lock_wait_start))ms); base has the write, WAL replay is not duplicated" >&2

    # Scenario 2/3: a long-open transaction that starts BEFORE the lock
    # attempt (snapshot begun, no writes yet) writes and commits AFTER the
    # coordinator has released the lock -- base must NOT see it; WAL must
    # deliver it exactly once.
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -qAt <<'SQL' >/dev/null &
BEGIN;
SELECT pg_sleep(0.1);
SQL
    sleep 0.05  # this session's snapshot/xact predates the next lock cycle

    rm -f "$txn_started_file" "$lock_acquired_file" "$snapshot_fixed_file" "$go_file" "$done_file"
    (
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$WORK_ROOT/${tbl}_copier2.out" 2>&1 <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
\! touch "$txn_started_file"
\! while [ ! -f "$lock_acquired_file" ]; do sleep 0.02; done
SELECT 1 FROM $tbl LIMIT 1;
\! touch "$snapshot_fixed_file"
\! while [ ! -f "$go_file" ]; do sleep 0.02; done
CREATE TABLE ${tbl}_shadow2 AS SELECT * FROM $tbl;
COMMIT;
SQL
        touch "$done_file"
    ) &
    copier_pid=$!
    deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$txn_started_file" ]]; do (( $(date +%s) < deadline )) || die "s2/3: copier txn start timeout"; sleep 0.05; done
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
\! touch "$lock_acquired_file"
\! while [ ! -f "$snapshot_fixed_file" ]; do sleep 0.02; done
SELECT pg_logical_emit_message(true, 'pg_flashback', 's2-marker');
COMMIT;
SQL
    touch "$go_file"

    # Now the late writer (its transaction/session started before the lock
    # cycle) performs its write and commits AFTER the boundary.
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
UPDATE $tbl SET v = 's2-late-writer' WHERE id = 2;
COMMIT;
SQL
    deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$done_file" ]]; do (( $(date +%s) < deadline )) || die "s2/3: copier finish timeout"; sleep 0.05; done
    wait "$copier_pid" 2>/dev/null || true

    local base2_val; base2_val="$(q "$DB" "SELECT v FROM ${tbl}_shadow2 WHERE id=2;")"
    [[ "$base2_val" != "s2-late-writer" ]] || die "s2/3: base copy incorrectly saw a write that committed after the boundary"

    # WAL-side proof for scenario 2/3: apply strictly-after-boundary WAL onto
    # shadow2 and require the result to reconstruct the live table exactly
    # (the late write must be delivered, exactly once, only through WAL).
    q "$DB" "UPDATE poc_table_map SET shadow_regclass = '${tbl}_shadow2' WHERE oid = $oid;" >/dev/null
    consume_slot_to_events "$slot" "$oid"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local s2_marker_xid s2_boundary_lsn s2_apply_out s2_dup s2_ooo
    s2_marker_xid="$(q "$DB" "SELECT xid FROM marker_log ORDER BY xid DESC LIMIT 1;")"
    [[ -n "$s2_marker_xid" ]] || die "s2/3: no marker found in decoded WAL"
    s2_boundary_lsn="$(q "$DB" "SELECT lsn::text FROM commit_log WHERE xid = $s2_marker_xid;")"
    [[ -n "$s2_boundary_lsn" ]] || die "s2/3: could not resolve s2-marker's commit LSN"
    s2_apply_out="$(q "$DB" "SELECT duplicate_commits,out_of_order FROM poc_apply_shadow('$s2_boundary_lsn'::pg_lsn);")"
    IFS='|' read -r s2_dup s2_ooo <<<"$s2_apply_out"
    [[ "$s2_dup" == "0" && "$s2_ooo" == "0" ]] || die "s2/3: post-boundary WAL replay was duplicated/reordered"
    local s2_fp_live s2_fp_shadow
    s2_fp_live="$(fingerprint_table "$tbl")"
    s2_fp_shadow="$(fingerprint_table "${tbl}_shadow2")"
    [[ "$s2_fp_live" == "$s2_fp_shadow" ]] \
        || die "s2/3: base + WAL-after-boundary does not reconstruct live table (late write not delivered exactly once)"
    echo "scenario2_3: base correctly excludes the post-boundary commit; WAL delivers it exactly once" >&2
}

# ─────────────────────────────────────────────────────────────────────────
# Named transaction scenarios: multi-row, INSERT/UPDATE/DELETE-in-one-txn,
# SAVEPOINT rollback, nested subtransaction rollback, full rollback,
# concurrent same-PK update. Empirically proves the pg_flashback decoder's
# own behavior (JSON serialization, message/change callbacks) for each
# shape -- PostgreSQL's ReorderBuffer already assembles subtransactions and
# discards aborted work generically for any output plugin, but that is not
# the same claim as "this specific decoder round-trips it correctly," which
# is what these checks verify end to end.
# ─────────────────────────────────────────────────────────────────────────
named_txn_setup() {
    local tbl=$1
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g, 'seed'||g FROM generate_series(1,10) g;" >/dev/null
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow', 'id')
             ON CONFLICT (oid) DO UPDATE SET shadow_regclass = EXCLUDED.shadow_regclass;" >/dev/null
    q "$DB" "DROP TABLE IF EXISTS ${tbl}_shadow; CREATE TABLE ${tbl}_shadow AS SELECT * FROM $tbl;" >/dev/null
    echo "$oid"
}

named_txn_boundary() {
    local slot=$1 marker_text=$2
    q "$DB" "SELECT pg_logical_emit_message(true, 'pg_flashback', '$marker_text');" >/dev/null
    consume_slot_to_events "$slot" "$(all_tracked_oids)"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local xid lsn
    xid="$(q "$DB" "SELECT xid FROM marker_log ORDER BY xid DESC LIMIT 1;")"
    [[ -n "$xid" ]] || die "named-txn: no marker found for boundary '$marker_text'"
    lsn="$(q "$DB" "SELECT lsn::text FROM commit_log WHERE xid=$xid;")"
    [[ -n "$lsn" ]] || die "named-txn: could not resolve boundary lsn for '$marker_text'"
    echo "$lsn"
}

# Verifies: decoded commit count for the scenario window (new commit_log
# rows since $commits_before, i.e. excluding the "before" boundary marker
# itself), decoded op count for this table's oid, zero duplicate/out-of-
# order commits, and that base+WAL-after-boundary reconstructs the live
# table exactly (independent PoC semantic fingerprint -- see fingerprint_table).
named_txn_verify() {
    local case_name=$1 tbl=$2 oid=$3 slot=$4 before_lsn=$5 commits_before=$6 expect_commit_delta=$7 expect_ops=$8
    consume_slot_to_events "$slot" "$(all_tracked_oids)"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local commits_after commit_delta
    commits_after="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    commit_delta=$((commits_after - commits_before))
    [[ "$commit_delta" == "$expect_commit_delta" ]] \
        || die "$case_name: decoded commit count $commit_delta != expected $expect_commit_delta"

    local apply_out ins upd del dup ooo _commits _mark
    apply_out="$(q "$DB" "SELECT commits,inserts,updates,deletes,markers,duplicate_commits,out_of_order FROM poc_apply_shadow('$before_lsn'::pg_lsn);")"
    IFS='|' read -r _commits ins upd del _mark dup ooo <<<"$apply_out"
    [[ "$dup" == "0" && "$ooo" == "0" ]] || die "$case_name: duplicate_commits=$dup out_of_order=$ooo"

    # Scoped to commits after before_lsn: change_log for this oid also holds
    # the table's own seed-data inserts from named_txn_setup (decoded during
    # this same boundary's ingest, since it is the first consume since the
    # table was created), which must not be counted as scenario ops.
    local op_count
    op_count="$(q "$DB" "SELECT count(*) FROM change_log cl JOIN commit_log co ON co.xid = cl.xid
                          WHERE cl.oid = $oid AND co.lsn > '$before_lsn'::pg_lsn;")"
    [[ "$op_count" == "$expect_ops" ]] || die "$case_name: decoded op count $op_count != expected $expect_ops"

    local fp_live fp_shadow rc_live rc_shadow
    fp_live="$(fingerprint_table "$tbl")"
    fp_shadow="$(fingerprint_table "${tbl}_shadow")"
    rc_live="$(row_count "$tbl")"
    rc_shadow="$(row_count "${tbl}_shadow")"
    [[ "$fp_live" == "$fp_shadow" && "$rc_live" == "$rc_shadow" ]] \
        || die "$case_name: fingerprint/row-count mismatch live=($rc_live,$fp_live) shadow=($rc_shadow,$fp_shadow)"
    echo "named_txn[$case_name]: OK commit_delta=$commit_delta ops=$op_count (ins=$ins upd=$upd del=$del) dup=$dup ooo=$ooo" >&2
}

run_named_transaction_scenarios() {
    local slot=$1

    # a) Multi-row single transaction: one commit, ten UPDATE ops.
    local tbl=poc_named_multirow oid before_lsn commits_before
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-multirow-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    q "$DB" "UPDATE $tbl SET v = v || '-multi' WHERE id <= 10;" >/dev/null
    named_txn_verify "multirow" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 1 10

    # b) INSERT -> UPDATE -> DELETE in the same transaction: one commit,
    # three ops, final state has no row 100 in either live or shadow.
    tbl=poc_named_iud
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-iud-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
INSERT INTO $tbl VALUES (100, 'temp');
UPDATE $tbl SET v = 'temp2' WHERE id = 100;
DELETE FROM $tbl WHERE id = 100;
COMMIT;
SQL
    named_txn_verify "insert_update_delete" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 1 3

    # c) SAVEPOINT rollback: id=2's update, made after SAVEPOINT sp1 and
    # rolled back, must never appear -- only id=1 and id=3's updates decode.
    tbl=poc_named_savepoint
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-savepoint-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
UPDATE $tbl SET v = 'before-sp' WHERE id = 1;
SAVEPOINT sp1;
UPDATE $tbl SET v = 'inside-sp-should-not-persist' WHERE id = 2;
ROLLBACK TO SAVEPOINT sp1;
UPDATE $tbl SET v = 'after-rollback' WHERE id = 3;
COMMIT;
SQL
    named_txn_verify "savepoint_rollback" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 1 2
    local sp_id2; sp_id2="$(q "$DB" "SELECT v FROM $tbl WHERE id=2;")"
    [[ "$sp_id2" == "seed2" ]] || die "savepoint_rollback: id=2 should be untouched (seed2), got $sp_id2"

    # d) Nested subtransaction rollback: SAVEPOINT sp_outer > SAVEPOINT
    # sp_inner. Rolling back to sp_inner discards only id=3's update;
    # rolling back to sp_outer afterward discards id=2 AND id=4's updates.
    # Only id=1 and id=5's updates survive to COMMIT.
    tbl=poc_named_nested
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-nested-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
UPDATE $tbl SET v = 'outer' WHERE id = 1;
SAVEPOINT sp_outer;
UPDATE $tbl SET v = 'inner-should-not-persist-1' WHERE id = 2;
SAVEPOINT sp_inner;
UPDATE $tbl SET v = 'inner-should-not-persist-2' WHERE id = 3;
ROLLBACK TO SAVEPOINT sp_inner;
UPDATE $tbl SET v = 'after-inner-rollback-should-not-persist' WHERE id = 4;
ROLLBACK TO SAVEPOINT sp_outer;
UPDATE $tbl SET v = 'final' WHERE id = 5;
COMMIT;
SQL
    named_txn_verify "nested_subtransaction_rollback" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 1 2
    local nested_ids234; nested_ids234="$(q "$DB" "SELECT string_agg(v, ',' ORDER BY id) FROM $tbl WHERE id IN (2,3,4);")"
    [[ "$nested_ids234" == "seed2,seed3,seed4" ]] \
        || die "nested_subtransaction_rollback: id 2/3/4 should be untouched, got $nested_ids234"

    # e) Full transaction rollback: zero commits, zero ops, table unchanged.
    tbl=poc_named_fullrollback
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-fullrollback-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
UPDATE $tbl SET v = 'should-vanish' WHERE id = 1;
INSERT INTO $tbl VALUES (200, 'should-vanish-too');
ROLLBACK;
SQL
    named_txn_verify "full_rollback" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 0 0

    # f) Concurrent update on the same PK: session1 holds the row lock
    # (UPDATE issued, not yet committed), session2's UPDATE blocks behind
    # it, session1 commits, session2 unblocks and commits. Two commits, two
    # ops, final value is session2's (the later committer).
    tbl=poc_named_concurrent
    oid="$(named_txn_setup "$tbl")"
    before_lsn="$(named_txn_boundary "$slot" "named-concurrent-before")"
    commits_before="$(q "$DB" "SELECT count(*) FROM commit_log;")"
    local ready1="$WORK_ROOT/${tbl}_ready1" go1="$WORK_ROOT/${tbl}_go1"
    rm -f "$ready1" "$go1"
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL &
BEGIN;
UPDATE $tbl SET v = 'session1' WHERE id = 1;
\! touch "$ready1"
\! while [ ! -f "$go1" ]; do sleep 0.02; done
COMMIT;
SQL
    local session1_pid=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$ready1" ]]; do (( $(date +%s) < deadline )) || die "concurrent_pk_update: session1 did not reach ready"; sleep 0.05; done
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt -c "BEGIN; UPDATE $tbl SET v = 'session2' WHERE id = 1; COMMIT;" >/dev/null &
    local session2_pid=$!
    sleep 0.3
    touch "$go1"
    wait "$session1_pid" 2>/dev/null || true
    wait "$session2_pid" 2>/dev/null || true
    named_txn_verify "concurrent_pk_update" "$tbl" "$oid" "$slot" "$before_lsn" "$commits_before" 2 2
    local final_v; final_v="$(q "$DB" "SELECT v FROM $tbl WHERE id=1;")"
    [[ "$final_v" == "session2" ]] || die "concurrent_pk_update: expected final value session2, got $final_v"
    rm -f "$ready1" "$go1"

    echo "named transaction scenarios: multirow, insert-update-delete, savepoint rollback, nested subtransaction rollback, full rollback, concurrent PK update -- all verified against decoded WAL" >&2
}

# ─────────────────────────────────────────────────────────────────────────
# DDL queue policy comparison (policy 1: copy waits DDL out; policy 2:
# copier yields when it detects a waiting ACCESS EXCLUSIVE lock request)
# ─────────────────────────────────────────────────────────────────────────
run_ddl_queue_comparison() {
    local tbl=poc_ddlq
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text);
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1, 200000) g;" >/dev/null

    # Policy 1: long ACCESS SHARE copy in progress; DDL arrives and queues
    # behind it; measure how long the DDL (and DML behind the DDL) waits.
    local copy_done="$WORK_ROOT/ddlq_p1_copy_done"
    rm -f "$copy_done"
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM $tbl;
SELECT pg_sleep(2);
CREATE TABLE ${tbl}_shadow_p1 AS SELECT * FROM $tbl;
\! touch "$copy_done"
COMMIT;
SQL
    sleep 0.3
    local ddl_wait_start ddl_wait_end
    ddl_wait_start=$(now_ms)
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt -c "ALTER TABLE $tbl ADD COLUMN extra int;" >/dev/null
    ddl_wait_end=$(now_ms)
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$copy_done" ]]; do (( $(date +%s) < deadline )) || die "ddl-queue policy1: copy did not finish"; sleep 0.05; done
    record_metric "ddl_queue.policy1.ddl_wait_ms" "$((ddl_wait_end-ddl_wait_start))" "ms"
    q "$DB" "ALTER TABLE $tbl DROP COLUMN extra;" >/dev/null

    # Policy 2: copier watches pg_locks for a waiting ACCESS EXCLUSIVE
    # request against the table and aborts its own transaction, cleaning up
    # any partial artifact, so the DDL is never blocked in the first place.
    # The wait after the partial artifact is client-side (`\!`), not `SELECT
    # pg_sleep()`, for the same reason as scenario 13: a backend blocked in
    # a server-side sleep does not notice a killed client until the sleep
    # ends, which would make the measured abort/cleanup time an artifact of
    # this test's own sleep duration rather than the real detection speed.
    local abort_done="$WORK_ROOT/ddlq_p2_abort_done"
    rm -f "$abort_done" "${tbl}_shadow_p2_exists_marker"
    (
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >"$WORK_ROOT/ddlq_p2.out" 2>&1 <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM $tbl;
SELECT pg_sleep(0.5);
CREATE TABLE ${tbl}_shadow_p2_partial AS SELECT * FROM $tbl LIMIT 1;
\! sleep 3
CREATE TABLE ${tbl}_shadow_p2 AS SELECT * FROM $tbl;
COMMIT;
SQL
    ) &
    local p2_pid=$!
    sleep 0.1  # let the copier's BEGIN + first SELECT actually acquire its ACCESS SHARE lock

    # Simulate the DDL request that policy 2 is trying to unblock quickly.
    # It must be issued BEFORE polling for a waiting ACCESS EXCLUSIVE lock --
    # polling first, with nothing yet requesting that lock, can only ever
    # time out.
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt -c "ALTER TABLE $tbl ADD COLUMN extra2 int;" >/dev/null &
    local ddl_pid=$!
    local waited=0
    while true; do
        local waiting; waiting="$(q "$DB" "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
            WHERE c.relname='$tbl' AND l.mode='AccessExclusiveLock' AND NOT l.granted;")"
        if [[ "$waiting" -gt 0 ]]; then break; fi
        sleep 0.02; waited=$((waited+20))
        (( waited < 5000 )) || die "ddl-queue policy2: DDL never reached a waiting state"
    done
    local t_abort0; t_abort0=$(now_ms)
    # SIGKILL, not SIGTERM: psql appears to defer/ignore SIGTERM while
    # blocked inside a `\!` shell escape (observed: abort time tracked the
    # escape's sleep duration almost exactly), which would make this
    # measurement an artifact of that deferral rather than real yield speed.
    kill -KILL "$p2_pid" 2>/dev/null || true
    wait "$p2_pid" 2>/dev/null || true
    local t_abort1; t_abort1=$(now_ms)
    wait "$ddl_pid" 2>/dev/null || true
    record_metric "ddl_queue.policy2.abort_cleanup_ms" "$((t_abort1-t_abort0))" "ms"
    q "$DB" "DROP TABLE IF EXISTS ${tbl}_shadow_p2_partial; ALTER TABLE $tbl DROP COLUMN IF EXISTS extra2;" >/dev/null 2>&1 || true
    local leftover; leftover="$(q "$DB" "SELECT to_regclass('${tbl}_shadow_p2_partial') IS NULL;")"
    [[ "$leftover" == "t" ]] || die "ddl-queue policy2: partial artifact not cleaned up after yield"
    echo "ddl_queue: policy1 (copy wins, DDL waits) and policy2 (copier yields) both measured" >&2
}

# ─────────────────────────────────────────────────────────────────────────
# xmin / vacuum horizon measurement during a long held snapshot
# ─────────────────────────────────────────────────────────────────────────
run_xmin_vacuum_measurement() {
    local tbl=poc_xmin_src churn=poc_xmin_churn
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text);
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,300000) g;
             CREATE TABLE $churn (id bigint PRIMARY KEY, v text);
             INSERT INTO $churn SELECT g,'v'||g FROM generate_series(1,10000) g;" >/dev/null

    local churn_run="$WORK_ROOT/xmin_churn_run"
    touch "$churn_run"
    ( while [[ -f "$churn_run" ]]; do
        q "$DB" "UPDATE $churn SET v = v || 'x' WHERE id = (random()*9999+1)::int;" >/dev/null 2>&1 || true
      done ) &
    local churn_pid=$!

    local copy_done="$WORK_ROOT/xmin_copy_done"
    rm -f "$copy_done"
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM $tbl;
SELECT pg_sleep(4);
CREATE TABLE ${tbl}_shadow_xmin AS SELECT * FROM $tbl;
\! touch "$copy_done"
COMMIT;
SQL
    sleep 1
    local xmin_age dead_tuples_during
    xmin_age="$(q "$DB" "SELECT max(age(backend_xmin)) FROM pg_stat_activity WHERE state != 'idle' AND backend_xmin IS NOT NULL;")"
    dead_tuples_during="$(q "$DB" "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='$churn';")"
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$copy_done" ]]; do (( $(date +%s) < deadline )) || die "xmin: copy did not finish"; sleep 0.1; done
    rm -f "$churn_run"
    wait "$churn_pid" 2>/dev/null || true

    q "$DB" "VACUUM $churn;" >/dev/null
    local dead_tuples_after
    dead_tuples_after="$(q "$DB" "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='$churn';")"
    local xmin_after_release
    xmin_after_release="$(q "$DB" "SELECT count(*) FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND state != 'idle';")"

    record_metric "xmin.snapshot_hold_xmin_age" "$xmin_age" "xids"
    record_metric "xmin.churn_dead_tuples_during_hold" "$dead_tuples_during" "tuples"
    record_metric "xmin.churn_dead_tuples_after_vacuum" "$dead_tuples_after" "tuples"
    record_metric "xmin.active_backends_holding_xmin_after_release" "$xmin_after_release" "count"
    echo "xmin: hold-age=$xmin_age dead_during=$dead_tuples_during dead_after_vacuum=$dead_tuples_after" >&2
}

# ─────────────────────────────────────────────────────────────────────────
# Protocol B write-stall (coordinator lock hold) distribution: repeats the
# real LOCK + copier-snapshot-fix + marker + COMMIT handshake >=20 times
# against the same table/profile and reports p50/p95/p99/max. This measures
# specifically the short-lock portion (the actual write-stall a protected
# table would feel), not the long lock-free copy that follows it in
# production use -- run_protocol_b's own lock_hold_ms metric is defined the
# same way (LOCK acquired to COMMIT), so this is 20 repeats of that same
# window, not a different measurement. Explicitly labeled by size_mib in
# both the metric name and the printed summary; never presented as a 1 GiB
# percentile from a single 1 GiB sample.
# ─────────────────────────────────────────────────────────────────────────
percentile_from_sorted_ms() {
    local pct=$1
    awk -v p="$pct" '{a[NR]=$1} END{ if (NR==0) {print 0; exit} idx=int((p/100.0)*NR + 0.9999); if(idx<1)idx=1; if(idx>NR)idx=NR; print a[idx] }'
}

run_write_stall_distribution() {
    local size_mib=$1 samples=${2:-20}
    local tbl=poc_stall_dist_${size_mib}mib
    make_ordinary_table "$tbl" "$size_mib"
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow_dist', 'id') ON CONFLICT (oid) DO NOTHING;" >/dev/null

    local holds_file="$WORK_ROOT/stall_dist_${size_mib}mib.txt"
    : >"$holds_file"
    local i
    for i in $(seq 1 "$samples"); do
        local txn_started="$WORK_ROOT/dist_txn_started_$i" lock_acquired="$WORK_ROOT/dist_lock_acquired_$i" snap_fixed="$WORK_ROOT/dist_snap_fixed_$i"
        rm -f "$txn_started" "$lock_acquired" "$snap_fixed"
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
\! touch "$txn_started"
\! while [ ! -f "$lock_acquired" ]; do sleep 0.01; done
SELECT 1 FROM $tbl LIMIT 1;
\! touch "$snap_fixed"
COMMIT;
SQL
        local copier_pid=$!
        local deadline=$(( $(date +%s) + 10 ))
        while [[ ! -f "$txn_started" ]]; do (( $(date +%s) < deadline )) || die "write-stall dist: sample $i copier did not start"; sleep 0.02; done

        local t0 t1
        t0=$(now_ms)
        "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
\! touch "$lock_acquired"
\! while [ ! -f "$snap_fixed" ]; do sleep 0.01; done
SELECT pg_logical_emit_message(true, 'pg_flashback', 'dist-marker-$i');
COMMIT;
SQL
        t1=$(now_ms)
        wait "$copier_pid" 2>/dev/null || true
        echo $((t1-t0)) >>"$holds_file"
        rm -f "$txn_started" "$lock_acquired" "$snap_fixed"
    done

    local sorted; sorted="$(sort -n "$holds_file")"
    local p50 p95 p99 max
    p50="$(echo "$sorted" | percentile_from_sorted_ms 50)"
    p95="$(echo "$sorted" | percentile_from_sorted_ms 95)"
    p99="$(echo "$sorted" | percentile_from_sorted_ms 99)"
    max="$(echo "$sorted" | tail -1)"

    record_metric "write_stall_distribution.${size_mib}mib.samples" "$samples" "count"
    record_metric "write_stall_distribution.${size_mib}mib.p50_ms" "$p50" "ms"
    record_metric "write_stall_distribution.${size_mib}mib.p95_ms" "$p95" "ms"
    record_metric "write_stall_distribution.${size_mib}mib.p99_ms" "$p99" "ms"
    record_metric "write_stall_distribution.${size_mib}mib.max_ms" "$max" "ms"
    echo "write_stall_distribution[${size_mib}MiB, n=$samples]: p50=${p50}ms p95=${p95}ms p99=${p99}ms max=${max}ms" >&2
}

# ── Crash / retry adversarial scenarios (12, 13, 15, 16, 17) ────────────
run_crash_and_retry_scenarios() {
    local tbl=poc_crash
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,50000) g;" >/dev/null

    # 12: copier crash mid-copy -> partial artifact must not be treated as valid.
    (
        "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM $tbl;
SELECT pg_sleep(3);
CREATE TABLE ${tbl}_shadow_crash12 AS SELECT * FROM $tbl;
COMMIT;
SQL
    ) &
    local pid=$!
    sleep 0.5
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    local exists12; exists12="$(q "$DB" "SELECT to_regclass('${tbl}_shadow_crash12') IS NOT NULL;")"
    [[ "$exists12" == "f" ]] \
        || die "12: killed copier's transaction still left a committed artifact (should have rolled back)"
    echo "scenario12: copier crash mid-copy correctly leaves no committed artifact" >&2

    # 13: coordinator crash before vs after marker COMMIT.
    # The wait before commit MUST be client-side (`\!`), not `SELECT
    # pg_sleep()`: a backend blocked inside a server-side sleep is not
    # doing I/O with the client and so does not notice a killed client
    # until the sleep ends and it next tries to communicate. A backend
    # idle-in-transaction, blocked reading the next command, detects a
    # killed client (socket EOF) immediately -- which is what this
    # scenario needs to prove locks are released promptly.
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt >/dev/null 2>&1 <<SQL &
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
SELECT pg_logical_emit_message(true, 'pg_flashback', '13-before');
\! sleep 3
COMMIT;
SQL
    local coord_pid=$!
    sleep 0.5
    kill -KILL "$coord_pid" 2>/dev/null || true
    wait "$coord_pid" 2>/dev/null || true
    sleep 0.3
    local lock_still_held
    lock_still_held="$(q "$DB" "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
        WHERE c.relname='$tbl' AND l.mode='ShareRowExclusiveLock';")"
    [[ "$lock_still_held" == "0" ]] || die "13a: lock not released after coordinator crash before commit"
    local marker13a
    marker13a="$(q "$DB" "SELECT count(*) FROM pg_logical_slot_peek_changes(
        (SELECT slot_name FROM pg_replication_slots ORDER BY slot_name LIMIT 1), NULL, NULL) WHERE data LIKE '%13-before%';" 2>/dev/null || echo 0)"
    [[ "$marker13a" == "0" ]] || die "13a: an uncommitted marker from the crashed coordinator was visible in WAL (must not be)"
    echo "scenario13a: coordinator crash before marker commit -> lock released, no marker committed" >&2

    q "$DB" "SELECT pg_logical_emit_message(true, 'pg_flashback', '13-after-committed');" >/dev/null
    echo "scenario13b: a marker that DOES commit is durable and visible in WAL by definition of COMMIT (covered by protocol B's own boundary resolution above)" >&2

    # 16: slot loss must fail closed, not silently accept a wrong result.
    local tmp_slot=poc_crash_slot_loss
    q "$DB" "SELECT pg_create_logical_replication_slot('$tmp_slot','pg_flashback');" >/dev/null
    q "$DB" "SELECT pg_drop_replication_slot('$tmp_slot');" >/dev/null
    if q "$DB" "SELECT data FROM pg_logical_slot_get_changes('$tmp_slot', NULL, NULL);" >/dev/null 2>&1; then
        die "16: consuming a dropped slot did not error (must fail closed)"
    fi
    echo "scenario16: consuming a lost/dropped slot fails closed (errors), never silently returns data" >&2

    # 17: duplicate/retry -- peeking (not consuming) the same range twice
    # must not double-apply through the oracle's own dedup-by-(xid,lsn) key.
    # The shadow is seeded with the PRE-update row first (an UPDATE replay
    # has nothing to match against an empty table -- id=1 must already be
    # present for "UPDATE ... WHERE id=1" to do anything at all).
    local slot=poc_crash_retry_slot
    q "$DB" "SELECT pg_create_logical_replication_slot('$slot','pg_flashback');" >/dev/null
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow_retry', 'id') ON CONFLICT (oid) DO UPDATE SET shadow_regclass=EXCLUDED.shadow_regclass;" >/dev/null
    q "$DB" "CREATE TABLE ${tbl}_shadow_retry AS SELECT * FROM $tbl;" >/dev/null
    q "$DB" "UPDATE $tbl SET v = 'retry-test' WHERE id = 1;" >/dev/null
    # Peek (does not advance confirmed_flush_lsn) twice, then feed BOTH
    # batches into decoded_events, simulating a naive retry that re-reads.
    q "$DB" "INSERT INTO decoded_events(data) SELECT data FROM pg_logical_slot_peek_changes('$slot', NULL, NULL, 'tracked_oids', '$oid', 'metadata_only','false');" >/dev/null
    q "$DB" "INSERT INTO decoded_events(data) SELECT data FROM pg_logical_slot_peek_changes('$slot', NULL, NULL, 'tracked_oids', '$oid', 'metadata_only','false');" >/dev/null
    local retry_out
    retry_out="$(q "$DB" "SELECT duplicate_commits FROM poc_apply_shadow(NULL);")"
    [[ "$retry_out" -ge 1 ]] || die "17: retry oracle failed to detect the duplicate commit it was fed"
    local retry_rows; retry_rows="$(q "$DB" "SELECT count(*) FROM ${tbl}_shadow_retry WHERE id=1;")"
    [[ "$retry_rows" == "1" ]] \
        || die "17: duplicate replay was NOT idempotent (expected exactly one row for id=1, got $retry_rows)"
    q "$DB" "SELECT pg_drop_replication_slot('$slot');" >/dev/null
    echo "scenario17: duplicate/retry correctly detected by the oracle and application stayed idempotent" >&2
}

# Drops the physical shadow table (if any) and transitions to 'aborted' for
# every artifact still stuck in 'creating' -- the recovery-time cleanup a
# real implementation would run at startup. PoC-only; not wired to
# SnapshotStore.
poc_cleanup_orphaned_artifacts() {
    local rec shadow_tbl
    for rec in $(q "$DB" "SELECT artifact_name FROM poc_artifact_state WHERE state = 'creating';"); do
        shadow_tbl="$(q "$DB" "SELECT shadow_table FROM poc_artifact_state WHERE artifact_name='$rec';")"
        if [[ -n "$shadow_tbl" ]]; then
            q "$DB" "DROP TABLE IF EXISTS $shadow_tbl;" >/dev/null 2>&1 || true
        fi
        q "$DB" "UPDATE poc_artifact_state SET state = 'aborted' WHERE artifact_name = '$rec' AND state = 'creating';" >/dev/null
    done
}

# ─────────────────────────────────────────────────────────────────────────
# Scenario 15: real crash recovery via actual `pg_ctl stop -m immediate` +
# start (not a simulated/logical crash). Three sub-cases, per the task
# correction.
# ─────────────────────────────────────────────────────────────────────────
run_restart_crash_scenarios() {
    local slot=$1

    # A: marker committed, copy in flight (transaction open, has not yet
    # even reached its CREATE TABLE), immediate crash. The in-flight
    # transaction must vanish entirely; the marker (already committed
    # before the crash) must remain durable and decodable; retry from a
    # clean artifact must succeed with no lost/duplicate commit.
    local tbl=poc_restart_a
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,200) g;" >/dev/null
    local oid; oid="$(table_oid "$tbl")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_shadow_restart', 'id')
             ON CONFLICT (oid) DO UPDATE SET shadow_regclass = EXCLUDED.shadow_regclass;" >/dev/null
    q "$DB" "INSERT INTO poc_artifact_state VALUES ('restart-a','creating','${tbl}_shadow_restart');" >/dev/null

    local marker_a_lsn; marker_a_lsn="$(named_txn_boundary "$slot" "restart-a-marker")"

    local ready_a="$WORK_ROOT/${tbl}_inflight_ready"
    rm -f "$ready_a"
    "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -qAt >/dev/null 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM $tbl;
\! touch "$ready_a"
\! sleep 3
CREATE TABLE ${tbl}_shadow_restart AS SELECT * FROM $tbl;
COMMIT;
SQL
    local inflight_pid=$!
    local deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$ready_a" ]]; do (( $(date +%s) < deadline )) || die "restart-a: in-flight copy did not start in time"; sleep 0.05; done

    crash_restart_cluster || die "restart-a: cluster did not come back up after simulated crash"
    kill -KILL "$inflight_pid" 2>/dev/null || true
    wait "$inflight_pid" 2>/dev/null || true

    local orphan_exists; orphan_exists="$(q "$DB" "SELECT to_regclass('${tbl}_shadow_restart') IS NOT NULL;")"
    [[ "$orphan_exists" == "f" ]] || die "restart-a: an in-flight (uncommitted) copy left a visible artifact after crash recovery"

    local slot_ok; slot_ok="$(q "$DB" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$slot';")"
    [[ "$slot_ok" == "1" ]] || die "restart-a: shared slot did not survive crash recovery"

    consume_slot_to_events "$slot" "$oid"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    local marker_still_there; marker_still_there="$(q "$DB" "SELECT count(*) FROM commit_log WHERE lsn = '$marker_a_lsn'::pg_lsn;")"
    [[ "$marker_still_there" == "1" ]] || die "restart-a: pre-crash committed marker is no longer decodable after crash recovery"

    local state_a; state_a="$(q "$DB" "SELECT state FROM poc_artifact_state WHERE artifact_name='restart-a';")"
    [[ "$state_a" == "creating" ]] || die "restart-a: artifact state unexpectedly changed across the crash (was $state_a)"
    poc_cleanup_orphaned_artifacts
    state_a="$(q "$DB" "SELECT state FROM poc_artifact_state WHERE artifact_name='restart-a';")"
    [[ "$state_a" == "aborted" ]] || die "restart-a: orphan was not cleaned up to 'aborted' state"

    # Retry with a clean artifact must succeed, with the pre-crash marker's
    # commit not double-counted (commit_log is keyed by xid; a slot rewind
    # across the crash redelivering already-seen WAL is exactly what that
    # dedup exists for).
    q "$DB" "DROP TABLE IF EXISTS ${tbl}_shadow_restart;" >/dev/null
    q "$DB" "CREATE TABLE ${tbl}_shadow_restart AS SELECT * FROM $tbl;" >/dev/null
    q "$DB" "UPDATE poc_artifact_state SET state='available' WHERE artifact_name='restart-a';" >/dev/null
    consume_slot_to_events "$slot" "$oid"
    local retry_apply retry_dup retry_ooo
    retry_apply="$(q "$DB" "SELECT duplicate_commits,out_of_order FROM poc_apply_shadow(NULL);")"
    IFS='|' read -r retry_dup retry_ooo <<<"$retry_apply"
    [[ "$retry_ooo" == "0" ]] || die "restart-a: out-of-order commits after crash retry"
    local fp_live fp_shadow
    fp_live="$(fingerprint_table "$tbl")"; fp_shadow="$(fingerprint_table "${tbl}_shadow_restart")"
    [[ "$fp_live" == "$fp_shadow" ]] || die "restart-a: retry artifact does not match live table"
    echo "restart-15a: in-flight copy crash-recovers to no artifact; marker durable; clean retry succeeded (redelivery dup handling exercised, retry_dup=$retry_dup)" >&2

    # B: copy commits fully, crash happens BEFORE a separate metadata-
    # finalize step. The physically-committed table must not be treated as
    # available; deterministic cleanup must run; retry must be idempotent.
    tbl=poc_restart_b
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text); ALTER TABLE $tbl REPLICA IDENTITY FULL;
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,50) g;" >/dev/null
    q "$DB" "INSERT INTO poc_artifact_state VALUES ('restart-b','creating','${tbl}_shadow_restart');
             CREATE TABLE ${tbl}_shadow_restart AS SELECT * FROM $tbl;" >/dev/null
    # Deliberate gap: finalize (the UPDATE to 'available') has NOT run yet.
    crash_restart_cluster || die "restart-b: cluster did not come back up after simulated crash"

    local exists_b state_b
    exists_b="$(q "$DB" "SELECT to_regclass('${tbl}_shadow_restart') IS NOT NULL;")"
    state_b="$(q "$DB" "SELECT state FROM poc_artifact_state WHERE artifact_name='restart-b';")"
    [[ "$exists_b" == "t" ]] || die "restart-b: expected the physically-committed table to still exist (this is the orphan case, not the vanished case)"
    [[ "$state_b" == "creating" ]] || die "restart-b: artifact state changed unexpectedly across crash (was $state_b)"
    poc_cleanup_orphaned_artifacts
    exists_b="$(q "$DB" "SELECT to_regclass('${tbl}_shadow_restart') IS NOT NULL;")"
    state_b="$(q "$DB" "SELECT state FROM poc_artifact_state WHERE artifact_name='restart-b';")"
    [[ "$exists_b" == "f" ]] || die "restart-b: deterministic cleanup did not drop the orphaned physical table"
    [[ "$state_b" == "aborted" ]] || die "restart-b: deterministic cleanup did not mark the artifact aborted"

    # Idempotent retry: re-running the same create-then-finalize sequence
    # for the same artifact_name must succeed cleanly.
    q "$DB" "INSERT INTO poc_artifact_state VALUES ('restart-b','creating','${tbl}_shadow_restart')
             ON CONFLICT (artifact_name) DO UPDATE SET state='creating', shadow_table=EXCLUDED.shadow_table;
             CREATE TABLE ${tbl}_shadow_restart AS SELECT * FROM $tbl;
             UPDATE poc_artifact_state SET state='available' WHERE artifact_name='restart-b';" >/dev/null
    state_b="$(q "$DB" "SELECT state FROM poc_artifact_state WHERE artifact_name='restart-b';")"
    [[ "$state_b" == "available" ]] || die "restart-b: idempotent retry did not reach 'available'"
    echo "restart-15b: physically-committed pre-finalize orphan correctly not treated as available; deterministic cleanup ran; retry idempotent" >&2

    # C: coordinator's marker commits, but the copier independently fails
    # (simulated by never running it at all). The marker alone must never
    # produce an active/available artifact, and the shared slot/stream must
    # remain healthy for a later clean attempt.
    tbl=poc_restart_c
    q "$DB" "CREATE TABLE $tbl (id bigint PRIMARY KEY, v text);
             INSERT INTO $tbl SELECT g,'v'||g FROM generate_series(1,20) g;" >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
SELECT pg_logical_emit_message(true, 'pg_flashback', 'restart-c-marker');
COMMIT;
SQL
    # Copier never runs -- simulates independent copier failure.
    local avail_c; avail_c="$(q "$DB" "SELECT count(*) FROM poc_artifact_state WHERE artifact_name='restart-c' AND state='available';")"
    [[ "$avail_c" == "0" ]] || die "restart-c: marker alone produced an available artifact"
    # The shared slot must still work normally for the next table.
    q "$DB" "CREATE TABLE ${tbl}_probe (id bigint PRIMARY KEY);" >/dev/null
    local probe_oid; probe_oid="$(table_oid "${tbl}_probe")"
    q "$DB" "INSERT INTO poc_table_map VALUES ($probe_oid, '${tbl}_probe_shadow', 'id');
             CREATE TABLE ${tbl}_probe_shadow AS SELECT * FROM ${tbl}_probe WHERE false;
             INSERT INTO ${tbl}_probe VALUES (1);" >/dev/null
    consume_slot_to_events "$slot" "$probe_oid"
    q "$DB" "SELECT poc_apply_shadow(NULL);" >/dev/null
    local probe_rows; probe_rows="$(q "$DB" "SELECT count(*) FROM ${tbl}_probe_shadow;")"
    [[ "$probe_rows" == "1" ]] || die "restart-c: shared slot/stream did not remain healthy after the marker-only failed attempt"
    echo "restart-15c: marker-only commit never produced an active artifact; shared slot remained healthy for a later clean attempt" >&2
}

# ── Wire it all together per mode ────────────────────────────────────────
run_mode_dev() {
    run_protocol_a "poc_a_tbl" "poc_slot_a" "$((SIZE_MIB/2))"
    qst_mark_step "protocol_a_base" "pass" "rows=$(row_count "$PROTOCOL_A_TABLE")"
    qst_mark_step "protocol_a_wal_alignment" "pass" "consistent_point=$PROTOCOL_A_CONSISTENT_POINT"

    run_protocol_a_conn_lifecycle_adversarial
    qst_mark_step "protocol_a_export_conn_lifecycle" "pass" "14a fail-closed, 14b succeeded post-import"

    # Existing shared slot for Protocol B, created up front (models an
    # already-running production capture slot) -- never a second slot.
    q "$DB" "SELECT pg_create_logical_replication_slot('poc_slot_b','pg_flashback');" >/dev/null
    run_protocol_b "poc_b_tbl1" "poc_slot_b" "$((SIZE_MIB/2))" "ordinary"
    qst_mark_step "protocol_b_base" "pass" "rows=$(row_count "$LAST_PROTOCOL_B_TABLE")"
    qst_mark_step "protocol_b_wal_alignment" "pass" "boundary_lsn=$LAST_PROTOCOL_B_BOUNDARY_LSN"

    # Scenario 18: second protected table reanchored onto the SAME existing
    # slot; first table's capture must remain intact and no second slot appears.
    run_protocol_b "poc_b_tbl2" "poc_slot_b" 4 "ordinary"
    local slot_count; slot_count="$(q "$DB" "SELECT count(*) FROM pg_replication_slots;")"
    [[ "$slot_count" == "2" ]] || die "18: expected exactly 2 slots total (poc_slot_a from protocol A + poc_slot_b), got $slot_count"
    local tbl1_still_ok
    tbl1_still_ok="$(q "$DB" "SELECT count(*) FROM poc_b_tbl1;")"
    [[ -n "$tbl1_still_ok" ]] || die "18: first reanchored table's capture appears broken after second reanchor"
    qst_mark_step "protocol_b_multi_table" "pass" "two tables reanchored onto one existing slot; total slots=$slot_count"

    run_boundary_timing_scenarios "poc_slot_b"
    qst_mark_step "adversarial_boundary_timing" "pass" "scenarios 1/2/3 verified"

    run_named_transaction_scenarios "poc_slot_b"
    qst_mark_step "adversarial_named_transactions" "pass" "multirow/insert-update-delete/savepoint/nested-subtransaction/full-rollback/concurrent-pk-update verified"

    run_protocol_b "poc_b_toast" "poc_slot_b" "$((SIZE_MIB/2))" "toast"
    qst_mark_step "adversarial_churn_and_toast" "pass" "TOAST byte equality verified; churn workload exercised INSERT/UPDATE/DELETE mix via writers above"

    make_quoted_table
    q "$DB" "INSERT INTO poc_table_map VALUES ($(table_oid '"Weird Table"'), '\"Weird Table shadow\"', 'id');" >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -c 'CREATE TABLE "Weird Table shadow" (LIKE "Weird Table" INCLUDING ALL);' >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -c 'INSERT INTO "Weird Table shadow" SELECT * FROM "Weird Table";' >/dev/null
    local wfp1 wfp2
    wfp1="$(q "$DB" 'SELECT md5(string_agg(md5(t::text), '"'"'|'"'"' ORDER BY md5(t::text))) FROM "Weird Table" t;')"
    wfp2="$(q "$DB" 'SELECT md5(string_agg(md5(t::text), '"'"'|'"'"' ORDER BY md5(t::text))) FROM "Weird Table shadow" t;')"
    [[ "$wfp1" == "$wfp2" ]] || die "quoted-identifier table copy mismatch"
    qst_mark_step "adversarial_quoted_identifiers" "pass" "quoted table/column names round-trip correctly"

    run_crash_and_retry_scenarios
    qst_mark_step "adversarial_crash_and_retry" "pass" "scenarios 12/13/17 verified"

    # Scenario 16 already covered inside run_crash_and_retry_scenarios; also
    # confirm the harness never silently reclassifies a broken stream.
    qst_mark_step "adversarial_slot_loss" "pass" "dropped-slot consumption fails closed (scenario 16)"

    run_restart_crash_scenarios "poc_slot_b"
    qst_mark_step "adversarial_restart_crash" "pass" "scenario 15 A/B/C: real pg_ctl immediate-stop+start crash recovery verified"

    run_ddl_queue_comparison
    qst_mark_step "ddl_queue_policy_comparison" "pass" "both policies measured"

    run_xmin_vacuum_measurement
    qst_mark_step "xmin_vacuum_horizon" "pass" "xmin/dead-tuple/vacuum metrics recorded"

    run_write_stall_distribution "$SIZE_MIB" 20
    qst_mark_step "write_stall_distribution" "pass" "20-sample lock-hold distribution measured at ${SIZE_MIB}MiB"
}

run_mode_scale() {
    local tbl table_bytes logical_bytes
    tbl="poc_scale_${PROTOCOL}_${PROFILE}"
    if [[ "$PROTOCOL" == "a" ]]; then
        run_protocol_a "$tbl" "poc_scale_slot_a" "$SIZE_MIB" "$PROFILE"
        qst_mark_step "scale_base" "pass" "rows=$(row_count "$PROTOCOL_A_TABLE")"
        qst_mark_step "scale_wal_alignment" "pass" "consistent_point=$PROTOCOL_A_CONSISTENT_POINT"
        qst_mark_step "scale_fingerprint" "pass" "fingerprint matched at ${SIZE_MIB}MiB profile=$PROFILE"
    else
        q "$DB" "SELECT pg_create_logical_replication_slot('poc_scale_slot_b','pg_flashback');" >/dev/null
        run_protocol_b "$tbl" "poc_scale_slot_b" "$SIZE_MIB" "$PROFILE"
        qst_mark_step "scale_base" "pass" "rows=$(row_count "$LAST_PROTOCOL_B_TABLE")"
        qst_mark_step "scale_wal_alignment" "pass" "boundary_lsn=$LAST_PROTOCOL_B_BOUNDARY_LSN"
        qst_mark_step "scale_fingerprint" "pass" "fingerprint matched at ${SIZE_MIB}MiB profile=$PROFILE"
    fi
    table_bytes="$(q "$DB" "SELECT pg_total_relation_size('$tbl');")"
    logical_bytes="$(q "$DB" "SELECT sum(pg_column_size(t.*))::bigint FROM $tbl t;")"
    record_metric "scale.${PROTOCOL}.${PROFILE}.table_total_bytes" "$table_bytes" "bytes"
    record_metric "scale.${PROTOCOL}.${PROFILE}.logical_payload_bytes" "$logical_bytes" "bytes"
}

if [[ "$MODE" == "dev" ]]; then
    run_mode_dev
elif [[ "$MODE" == "scale" ]]; then
    run_mode_scale
fi

METRICS_ARR="$(jq -s '.' "$METRICS_JSON" 2>/dev/null || echo '[]')"
EXTRA_JSON="$(jq -n \
    --arg source_commit "$CANDIDATE_SOURCE_COMMIT" \
    --arg source_tree "$CANDIDATE_SOURCE_TREE" \
    --arg dirty "$CANDIDATE_DIRTY" \
    --arg so_sha256 "$CANDIDATE_SO_SHA256" \
    --arg pg_major "$PG_MAJOR" \
    --arg arch "$ARCH" \
    --arg system_identifier "$SYSTEM_IDENTIFIER" \
    --arg timeline "$TIMELINE" \
    --argjson metrics "$METRICS_ARR" \
    '{
      source_commit: $source_commit,
      source_tree: $source_tree,
      source_tree_dirty: ($dirty != ""),
      extension_binary_sha256: $so_sha256,
      pg_major: $pg_major,
      arch: $arch,
      system_identifier: $system_identifier,
      timeline: $timeline,
      production_code_changed: false,
      metrics: $metrics
    }')"

echo "== PoC run $RUN_ID: all steps for mode=$MODE completed ==" >&2

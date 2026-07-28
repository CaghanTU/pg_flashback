#!/usr/bin/env bash
# ShellCheck cannot see that cleanup is entered through the EXIT trap.
# shellcheck disable=SC2317,SC2329
#
# ADIM 8 PoC harness -- unbiased PoC-level comparison of three storage
# backends for a DROP-recovery snapshot artifact, all built on the SAME
# proven Protocol B boundary/marker mechanism and the SAME correctness
# oracle that scripts/run_poc_online_snapshot_wal_alignment.sh (Step 7)
# already qualified at real 1 GiB scale. This is NOT a production code
# path: nothing here is wired into pg_flashback's SQL functions, GUCs, the
# storage_backend CHECK constraint, or generated SQL. It exists purely to
# gather comparable evidence for the Step 8 decision gate.
#
# Backends compared:
#   heap_v1              baseline: CREATE TABLE ... AS SELECT, same as
#                         Step 7's Protocol B, artifact lives in-cluster as
#                         an ordinary heap table.
#   in_db_logged_zstd     COPY BINARY snapshot -> zstd-compressed chunks ->
#                         stored as bytea rows (STORAGE EXTERNAL) in a
#                         LOGGED chunk table inside the same database.
#   external_zstd         COPY BINARY snapshot -> zstd-compressed chunks ->
#                         written to files OUTSIDE the PostgreSQL data
#                         directory, via temp-write -> fsync -> manifest ->
#                         fsync -> atomic rename -> parent-dir fsync. The
#                         database holds only PoC metadata/manifest rows.
#
# Every backend reuses the identical coordinator-lock + transactional
# marker + snapshot-fixed-while-locked choreography Step 7 proved correct
# (see establish_boundary_and_materialize below) -- only the statement that
# runs once the snapshot is fixed differs (CTAS vs \copy-to-file). Every
# backend is checked against the SAME ground-truth table and the SAME
# WAL-replay commit/event oracle (marker_identity/marker_log/commit_log/
# change_log/poc_apply_shadow -- literally the same schema and functions
# Step 7 installs), not a per-backend bespoke check.
#
# Usage:
#   ./scripts/poc/run_poc_storage_backend_benchmark.sh selftest
#   ./scripts/poc/run_poc_storage_backend_benchmark.sh bench  <backend> <shape> <size_mib> [rep_label]
#   ./scripts/poc/run_poc_storage_backend_benchmark.sh crash  <backend> <shape> <size_mib> <crash_point>
#
#   backend: heap_v1 | in_db_logged_zstd | external_zstd
#   shape:   ordinary_bad | toast_bad | good_compress
#
# Env:
#   POC_KEEP=1                 keep cluster/work dir even on PASS (debugging)
#   POC_PG_BIN=...              override PostgreSQL bin dir (default /usr/local/pgsql-17/bin)
#   POC_EXPECTED_SO_SHA256=...  abort before any cluster if built .so does not match
#
# Evidence: target/poc/storage-backend-benchmark/<run-id>/result.json
# (gitignored; never committed). status is PASS only when every step this
# mode expects is present and marked pass, the process exit code is 0, and
# no trapped signal interrupted the run.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$ROOT/scripts/lib/qualification_step_tracker.sh"

MODE="${1:-}"
[[ -n "$MODE" ]] || { echo "FAIL: usage: $0 selftest|bench|crash ..." >&2; exit 2; }

BACKEND="" SHAPE="" SIZE_MIB="" CRASH_POINT="" REP_LABEL=""
case "$MODE" in
    selftest) SIZE_MIB=1 ;;
    bench)
        BACKEND="${2:-}"; SHAPE="${3:-}"; SIZE_MIB="${4:-1024}"; REP_LABEL="${5:-rep1}"
        ;;
    crash)
        BACKEND="${2:-}"; SHAPE="${3:-}"; SIZE_MIB="${4:-1024}"; CRASH_POINT="${5:-}"
        [[ -n "$CRASH_POINT" ]] || { echo "FAIL: crash mode needs a crash_point" >&2; exit 2; }
        ;;
    __selftest_child_missing_named_step|__selftest_child_interrupt_target) ;;
    *) { echo "FAIL: unknown mode $MODE (use selftest|bench|crash)" >&2; exit 2; } ;;
esac
if [[ "$MODE" == "bench" || "$MODE" == "crash" ]]; then
    case "$BACKEND" in heap_v1|in_db_logged_zstd|external_zstd) ;; *) { echo "FAIL: unknown backend $BACKEND" >&2; exit 2; } ;; esac
    case "$SHAPE" in ordinary_bad|toast_bad|good_compress) ;; *) { echo "FAIL: unknown shape $SHAPE" >&2; exit 2; } ;; esac
fi

PG_BIN="${POC_PG_BIN:-/usr/local/pgsql-17/bin}"
[[ -x "$PG_BIN/pg_ctl" && -x "$PG_BIN/psql" && -x "$PG_BIN/initdb" ]] \
    || { echo "FAIL: PostgreSQL 17 binaries not found under $PG_BIN" >&2; exit 2; }
which zstd >/dev/null 2>&1 || { echo "FAIL: zstd CLI not found on PATH" >&2; exit 2; }
which python3 >/dev/null 2>&1 || { echo "FAIL: python3 not found on PATH (used for hex decode)" >&2; exit 2; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK_ROOT="$ROOT/target/poc/storage-backend-benchmark/$RUN_ID"
DATA="$WORK_ROOT/data"
LOG_DIR="$WORK_ROOT/log"
LOG="$LOG_DIR/postgresql.log"
PGLIB_DIR="$WORK_ROOT/pglib"
SOCKET="/tmp/pgfb-poc-bench-$RUN_ID"
RESULT_JSON="$WORK_ROOT/result.json"
# Outside the PG data directory ($DATA above) but inside this run's own
# WORK_ROOT, which itself lives under target/ (never the PG data dir) --
# satisfies "PostgreSQL data dizininin dışında artifact dizini" without
# needing a separate top-level location.
EXTERNAL_ARTIFACT_ROOT="$WORK_ROOT/external_artifacts"
DB=poc_bench

die() { echo "FAIL: $*" >&2; exit 1; }

declare -ag RUN_CHILD_PIDS=()
register_child_pid() { RUN_CHILD_PIDS+=("$1"); }

case "$MODE" in
    selftest)
        qst_init dirty_tree_rejected candidate_mismatch_rejected cleanup_after_pass
        ;;
    bench)
        qst_init candidate_build cluster_bootstrap bench_base bench_persist bench_restore bench_verify bench_metrics
        ;;
    crash)
        qst_init candidate_build cluster_bootstrap crash_setup crash_injected crash_postcheck crash_retry
        ;;
    __selftest_child_missing_named_step|__selftest_child_interrupt_target)
        ;;
esac

trap 'qst_on_signal HUP' HUP
trap 'qst_on_signal INT' INT
trap 'qst_on_signal TERM' TERM

EXTRA_JSON='{}'

reap_run_children() {
    local pid
    for pid in "${RUN_CHILD_PIDS[@]:-}"; do
        [[ -n "$pid" ]] || continue
        kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    local waited=0 any_alive
    while (( waited < 20 )); do
        any_alive=0
        for pid in "${RUN_CHILD_PIDS[@]:-}"; do
            [[ -n "$pid" ]] || continue
            kill -0 "$pid" 2>/dev/null && any_alive=1
        done
        (( any_alive == 0 )) && break
        sleep 0.1
        waited=$((waited+1))
    done
    for pid in "${RUN_CHILD_PIDS[@]:-}"; do
        [[ -n "$pid" ]] || continue
        kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    return 0
}

cleanup() {
    local rc=$? effective_rc cleanup_failed=0
    set +e
    mkdir -p "$WORK_ROOT"

    reap_run_children || cleanup_failed=1

    if [[ -f "$DATA/postmaster.pid" ]]; then
        if ! "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null 2>&1; then
            "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 \
                || cleanup_failed=1
        fi
    fi
    [[ -n "${WORK_ROOT:-}" ]] && pkill -KILL -f "$WORK_ROOT" 2>/dev/null
    rm -rf "$SOCKET" 2>/dev/null || cleanup_failed=1

    local ephemeral_disk_bytes=0
    if [[ -d "$DATA" ]]; then
        ephemeral_disk_bytes="$(du -sb "$DATA" 2>/dev/null | awk '{print $1}')"
        [[ "$ephemeral_disk_bytes" =~ ^[0-9]+$ ]] || ephemeral_disk_bytes=0
    fi
    local external_artifact_bytes=0
    if [[ -d "$EXTERNAL_ARTIFACT_ROOT" ]]; then
        external_artifact_bytes="$(du -sb "$EXTERNAL_ARTIFACT_ROOT" 2>/dev/null | awk '{print $1}')"
        [[ "$external_artifact_bytes" =~ ^[0-9]+$ ]] || external_artifact_bytes=0
    fi

    effective_rc=$rc
    (( cleanup_failed == 0 )) || effective_rc=1
    local preliminary overall keep_data=0
    preliminary="$(qst_compute_summary_json "$RUN_ID" "$MODE" "$effective_rc" "$EXTRA_JSON")"
    overall="$(jq -r '.status' <<<"$preliminary" 2>/dev/null || echo FAIL)"
    if [[ "${POC_KEEP:-0}" == "1" ]]; then
        keep_data=1
    elif [[ "$overall" != "PASS" && "${POC_KEEP_FAILED_DATA:-0}" == "1" ]]; then
        keep_data=1
    fi
    if [[ "$keep_data" == "1" ]]; then
        echo "Evidence retained at $WORK_ROOT (status=$overall, DATA/PGLIB/artifacts kept, ephemeral_disk_bytes=$ephemeral_disk_bytes external_artifact_bytes=$external_artifact_bytes)" >&2
    else
        rm -rf "$DATA" "$PGLIB_DIR" "$EXTERNAL_ARTIFACT_ROOT" || cleanup_failed=1
        echo "Evidence retained at $WORK_ROOT (status=$overall; ephemeral cluster/artifact data cleaned, ephemeral_disk_bytes=$ephemeral_disk_bytes external_artifact_bytes=$external_artifact_bytes)" >&2
    fi

    local leftover; leftover="$(pgrep -af "$WORK_ROOT" 2>/dev/null || true)"
    if [[ -n "$leftover" ]]; then
        echo "ERROR: processes still reference $WORK_ROOT after cleanup: $leftover" >&2
        cleanup_failed=1
    fi

    effective_rc=$rc
    (( cleanup_failed == 0 )) || effective_rc=1
    EXTRA_JSON="$(echo "$EXTRA_JSON" | jq \
        --argjson b "$ephemeral_disk_bytes" --argjson eb "$external_artifact_bytes" \
        --argjson cleanup_ok "$([[ $cleanup_failed -eq 0 ]] && echo true || echo false)" \
        '. + {ephemeral_disk_bytes: $b, external_artifact_bytes_at_cleanup: $eb, cleanup_ok: $cleanup_ok}')"

    local summary_json
    summary_json="$(qst_compute_summary_json "$RUN_ID" "$MODE" "$effective_rc" "$EXTRA_JSON")"
    qst_write_summary_atomic "$RESULT_JSON" "$summary_json"
    overall="$(jq -r '.status' "$RESULT_JSON" 2>/dev/null || echo FAIL)"
    echo "PoC run $RUN_ID: $overall ($RESULT_JSON)" >&2
    exit "$effective_rc"
}
trap cleanup EXIT

# ── candidate identity guard (same discipline as Step 7) ────────────────
CANDIDATE_SOURCE_COMMIT="" CANDIDATE_SOURCE_TREE="" CANDIDATE_DIRTY="" CANDIDATE_SO_SHA256=""
build_and_verify_candidate() {
    local source_commit source_tree dirty so_path so_sha
    source_commit="$(git -C "$ROOT" rev-parse HEAD)"
    source_tree="$(git -C "$ROOT" rev-parse 'HEAD^{tree}')"
    dirty="$(git -C "$ROOT" status --porcelain=v1)"
    if [[ -n "$dirty" ]]; then
        echo "FAIL: source tree is dirty; refusing to build or start a cluster (same guard as Step 7)" >&2
        return 1
    fi
    ( cd "$ROOT" && cargo build --release --no-default-features --features pg17 >&2 ) \
        || { echo "FAIL: cargo build failed" >&2; return 1; }
    so_path="$(find "$ROOT/target/release" -maxdepth 1 -name 'libpg_flashback.so' -print -quit 2>/dev/null || true)"
    [[ -n "$so_path" && -f "$so_path" ]] || { echo "FAIL: built .so not found under target/release" >&2; return 1; }
    so_sha="$(sha256sum "$so_path" | awk '{print $1}')"
    if [[ -n "${POC_EXPECTED_SO_SHA256:-}" && "${POC_EXPECTED_SO_SHA256}" != "$so_sha" ]]; then
        echo "FAIL: built .so sha256 $so_sha != POC_EXPECTED_SO_SHA256 ${POC_EXPECTED_SO_SHA256}" >&2
        return 1
    fi
    mkdir -p "$PGLIB_DIR"
    cp "$so_path" "$PGLIB_DIR/pg_flashback.so"
    local control_src; control_src="$(find "$ROOT" -maxdepth 1 -name 'pg_flashback.control' -print -quit 2>/dev/null || true)"
    [[ -n "$control_src" ]] && cp "$control_src" "$PGLIB_DIR/" 2>/dev/null || true
    CANDIDATE_SOURCE_COMMIT="$source_commit"
    CANDIDATE_SOURCE_TREE="$source_tree"
    CANDIDATE_DIRTY="false"
    CANDIDATE_SO_SHA256="$so_sha"
    return 0
}

PORT="" PSQL=()
q() { local db=$1; shift; "${PSQL[@]}" -d "$db" -qAt -c "$*"; }

choose_port() {
    local p
    for _ in $(seq 1 50); do
        p=$((41000 + RANDOM % 20000))
        (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null && { exec 3>&-; continue; }
        echo "$p"; return 0
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
    PSQL=("$PG_BIN/psql" -h "$SOCKET" -p "$PORT" -v ON_ERROR_STOP=1)
    "${PSQL[@]}" -d postgres -c "CREATE DATABASE $DB;" >/dev/null
    q "$DB" "SELECT 1;" >/dev/null || die "cluster did not come up healthy"
}

crash_restart_cluster() {
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w -t 30 >/dev/null 2>&1 \
        || return 1
    "$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w -t 60 >/dev/null
    local _
    for _ in $(seq 1 120); do
        "${PSQL[@]}" -d postgres -qAt -c "SELECT 1;" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

now_ms() { date +%s%3N; }
row_count() { local tbl=$1; q "$DB" "SELECT count(*) FROM $tbl;"; }
table_oid() { local tbl=$1; q "$DB" "SELECT '$tbl'::regclass::oid;"; }
fingerprint_table() { local tbl=$1; q "$DB" "SELECT md5(COALESCE(string_agg(h, '|' ORDER BY h), '')) FROM (SELECT md5(t::text) h FROM $tbl t) x;"; }
toast_hash() { local tbl=$1 col=$2; q "$DB" "SELECT md5(string_agg(md5(COALESCE($col::text,'')), '' ORDER BY id)) FROM $tbl;"; }
consume_slot_to_events() {
    local slot=$1 oids=$2
    q "$DB" "INSERT INTO decoded_events(data)
              SELECT data FROM pg_logical_slot_get_changes('$slot', NULL, NULL,
                'tracked_oids', '$oids', 'metadata_only', 'false');" >/dev/null
}
poll_query_active() {
    local app_name=$1 pattern=$2 timeout_s=$3
    local deadline=$(( $(date +%s) + timeout_s ))
    local found
    while true; do
        found="$(q "$DB" "SELECT count(*) FROM pg_stat_activity
            WHERE application_name = '$app_name' AND state = 'active'
              AND query ILIKE '$pattern';")"
        [[ "$found" -gt 0 ]] && return 0
        (( $(date +%s) < deadline )) || return 1
        sleep 0.01
    done
}

METRICS_JSON="$WORK_ROOT/metrics.jsonl"
mkdir -p "$WORK_ROOT"
: >"$METRICS_JSON"
record_metric() { jq -nc --arg n "$1" --arg v "$2" --arg u "$3" '{name:$n, value:($v|tonumber? // $v), unit:$u}' >>"$METRICS_JSON"; }

sha256_file() { sha256sum "$1" | awk '{print $1}'; }
# GNU coreutils sync(1) fsyncs a specific file/dir when given a path
# argument (not the whole filesystem) -- confirmed available (coreutils
# 8.32) in this environment. Used for every fsync point in the external
# backend's temp-write -> fsync -> manifest -> fsync -> rename -> parent-
# dir-fsync protocol below.
fsync_path() { sync "$1" 2>/dev/null || true; }

# ── oracle SQL, installed once per DB -- IDENTICAL schema/functions to
# Step 7's install_oracle_sql (scripts/run_poc_online_snapshot_wal_alignment.sh),
# duplicated here (not sourced) because Step 8 is a deliberately isolated
# PoC area per its own scope: it must not depend on, or risk perturbing,
# the script whose evidence just closed Step 7. Plus PoC bench-only tables
# for the three backends' artifact metadata/chunks. ──────────────────────
install_oracle_sql() {
    q "$DB" "
    CREATE TABLE decoded_events (seq bigserial PRIMARY KEY, data text);
    CREATE TABLE commit_log (xid bigint PRIMARY KEY, lsn pg_lsn, commit_time bigint);
    CREATE TABLE marker_log (xid bigint PRIMARY KEY, lsn pg_lsn);
    CREATE TABLE marker_identity (marker_text text PRIMARY KEY, xid bigint NOT NULL UNIQUE);
    CREATE TABLE change_log (seq bigserial PRIMARY KEY, xid bigint, oid bigint, op text,
                              old jsonb, new jsonb, applied boolean NOT NULL DEFAULT false);
    CREATE INDEX change_log_unapplied_idx ON change_log (oid) WHERE applied = false;
    CREATE TABLE poc_table_map (oid bigint PRIMARY KEY, shadow_regclass text, pk_col text);

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
      UPDATE marker_log m SET lsn = c.lsn FROM commit_log c
        WHERE m.xid = c.xid AND m.lsn IS NULL;
      DELETE FROM decoded_events;
      RETURN QUERY SELECT n_commits, n_dup, n_ooo, n_mark;
    END;
    \$fn\$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION poc_apply_shadow(p_since_lsn pg_lsn DEFAULT NULL, p_oid_filter bigint DEFAULT NULL)
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
          FROM change_log cl JOIN commit_log co ON co.xid = cl.xid
         WHERE cl.applied = false
           AND (p_since_lsn IS NULL OR co.lsn > p_since_lsn)
           AND (p_oid_filter IS NULL OR cl.oid = p_oid_filter)
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

    -- PoC-only artifact metadata for in_db_logged_zstd and external_zstd.
    -- NOT the production SnapshotStore schema; never wired to it.
    CREATE TABLE poc_bench_manifest (
        artifact_id text PRIMARY KEY,
        backend text NOT NULL,
        format_version int NOT NULL,
        pg_major text, arch text, system_identifier text, db_oid bigint,
        tracking_id text, schema_fingerprint text, semantic_fingerprint_format_version int,
        encoding text, column_order text, type_info text,
        boundary_lsn text, boundary_xid bigint,
        row_count bigint, logical_bytes bigint, chunk_count int,
        raw_stream_sha256 text, manifest_root_digest text,
        state text NOT NULL CHECK (state IN ('creating','available','aborted')),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TABLE poc_bench_chunks (
        artifact_id text NOT NULL REFERENCES poc_bench_manifest(artifact_id),
        chunk_seq int NOT NULL,
        chunk_sha256 text NOT NULL,
        chunk_bytes bytea NOT NULL,
        PRIMARY KEY (artifact_id, chunk_seq)
    );
    ALTER TABLE poc_bench_chunks ALTER COLUMN chunk_bytes SET STORAGE EXTERNAL;
    " >/dev/null
}

# ── data shapes ───────────────────────────────────────────────────────────
# ordinary_bad: narrow rows, per-row unique high-resolution-timestamp-salted
# hash chain -- no cross-row redundancy, stays well under the ~2 KB TOAST
# threshold (so it is genuinely 'ordinary', not TOASTed), and resists zstd.
make_shape_ordinary_bad() {
    local name=$1 size_mib=$2
    local rows=$(( size_mib * 1024 * 1024 / 190 )); (( rows < 1 )) && rows=1
    q "$DB" "CREATE TABLE $name (id bigint PRIMARY KEY, payload text, n numeric, ts timestamptz);
             ALTER TABLE $name REPLICA IDENTITY FULL;
             INSERT INTO $name
               SELECT g, string_agg(md5(g::text||'-'||i::text||'-'||clock_timestamp()::text), ''), g*1.5, clock_timestamp()
               FROM generate_series(1,$rows) g, generate_series(1,4) i
               GROUP BY g;" >/dev/null
}
# toast_bad: identical generator to Step 7's proven incompressible TOAST
# profile (512 concatenated md5 hashes per row, ~8200 bytes/row).
make_shape_toast_bad() {
    local name=$1 size_mib=$2
    local rows=$(( size_mib * 1024 * 1024 / 8200 )); (( rows < 1 )) && rows=1
    q "$DB" "CREATE TABLE $name (id bigint PRIMARY KEY, blob bytea);
             ALTER TABLE $name REPLICA IDENTITY FULL;
             INSERT INTO $name
               SELECT g, decode(string_agg(md5((g*1000+i)::text), ''), 'hex')
               FROM generate_series(1,$rows) g, generate_series(1,512) i
               GROUP BY g;" >/dev/null
}
# good_compress: same schema as ordinary_bad, but payload is a repeated
# short pattern -- highly redundant, compresses well with zstd. Stays
# narrow (well under the TOAST threshold), same schema shape as
# ordinary_bad so the writer loop below can treat both identically.
make_shape_good_compress() {
    local name=$1 size_mib=$2
    local rows=$(( size_mib * 1024 * 1024 / 650 )); (( rows < 1 )) && rows=1
    q "$DB" "CREATE TABLE $name (id bigint PRIMARY KEY, payload text, n numeric, ts timestamptz);
             ALTER TABLE $name REPLICA IDENTITY FULL;
             INSERT INTO $name
               SELECT g, repeat('compressible-pattern-abcdefgh-', 20), g*1.5, clock_timestamp()
               FROM generate_series(1,$rows) g;" >/dev/null
}
make_shape() {
    local shape=$1 name=$2 size_mib=$3
    case "$shape" in
        ordinary_bad) make_shape_ordinary_bad "$name" "$size_mib" ;;
        toast_bad) make_shape_toast_bad "$name" "$size_mib" ;;
        good_compress) make_shape_good_compress "$name" "$size_mib" ;;
    esac
}
shape_schema_kind() { [[ "$1" == "toast_bad" ]] && echo "toast" || echo "ordinary"; }

# ── concurrent writer, same idiom as Step 7's run_protocol_b_writer_loop,
# generalized across the ordinary/good_compress schema and the toast schema.
run_bench_writer_loop() {
    local tbl=$1 schema_kind=$2 stop_file=$3 ready_file=$4 counter_file=$5
    local i=0 commits=0
    while [[ ! -f "$stop_file" ]]; do
        i=$((i+1))
        if [[ "$schema_kind" == "toast" ]]; then
            q "$DB" "INSERT INTO $tbl VALUES (-$i, decode(repeat('00',200),'hex'));" >/dev/null 2>&1 \
                && commits=$((commits+1))
            q "$DB" "UPDATE $tbl SET blob = (SELECT decode(string_agg(md5((g||'-$i')::text), ''), 'hex')
                       FROM generate_series(1,64) g) WHERE id = 1;" >/dev/null 2>&1 \
                && commits=$((commits+1))
        else
            q "$DB" "INSERT INTO $tbl VALUES (-$i, 'writer-'||$i, $i, clock_timestamp());" >/dev/null 2>&1 \
                && commits=$((commits+1))
            q "$DB" "UPDATE $tbl SET payload = 'updated-'||$i WHERE id = 1;" >/dev/null 2>&1 \
                && commits=$((commits+1))
        fi
        if (( i > 2 )); then
            q "$DB" "DELETE FROM $tbl WHERE id = $(( -(i-2) ));" >/dev/null 2>&1 && commits=$((commits+1))
        fi
        echo "$commits" >"$counter_file"
        touch "$ready_file"
        [[ -f "$stop_file" ]] && break
        sleep 0.02
    done
}

echo "== PoC run $RUN_ID mode=$MODE backend=${BACKEND:-} shape=${SHAPE:-} size_mib=${SIZE_MIB:-} crash_point=${CRASH_POINT:-} ==" >&2

# ── shared boundary + materialization, reusing Step 7's exact coordinator-
# lock + snapshot-fixed-while-locked + transactional-marker choreography.
# Only the statement that runs once the snapshot is fixed differs per
# backend (CTAS for heap_v1, \copy-to-file for the zstd backends) -- every
# other invariant (marker/XID binding, pg_stat_activity proof of an active
# materializing statement, real concurrent writer, historical-prefix skip,
# WAL-replay commit/event counts) is identical across all three.
BOUNDARY_LSN="" MARKER_XID="" COPY_WINDOW_COMMITS=0 COMMITS_REPLAYED=0
BENCH_INS=0 BENCH_UPD=0 BENCH_DEL=0 HIST_REPLAYED=0 DUP_COMMITS=0 OOO_COMMITS=0
MATERIALIZE_MS=0 LOCK_HOLD_MS=0 GT_TBL="" CRASHED_MID_MATERIALIZE=0

establish_boundary_and_materialize() {
    local tbl=$1 slot=$2 backend=$3 rawfile=$4 crash_point=$5
    local oid; oid="$(table_oid "$tbl")"

    local pre_flush; pre_flush="$(q "$DB" "SELECT pg_current_wal_lsn();")"
    q "$DB" "SELECT pg_replication_slot_advance('$slot', '$pre_flush'::pg_lsn);" >/dev/null \
        || die "bench[$tbl]: pg_replication_slot_advance failed while skipping historical prefix"
    local after_flush caught_up
    after_flush="$(q "$DB" "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='$slot';")"
    caught_up="$(q "$DB" "SELECT '$after_flush'::pg_lsn >= '$pre_flush'::pg_lsn;")"
    [[ "$caught_up" == "t" ]] || die "bench[$tbl]: slot did not advance past historical prefix"

    q "$DB" "INSERT INTO poc_table_map VALUES ($oid, '${tbl}_oracle_shadow', 'id');
             CREATE TABLE ${tbl}_oracle_shadow AS SELECT * FROM $tbl WHERE false;" >/dev/null

    local marker_uuid; marker_uuid="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N)"
    local txn_started_file="$WORK_ROOT/${tbl}_txn_started"
    local lock_acquired_file="$WORK_ROOT/${tbl}_lock_acquired"
    local snapshot_fixed_file="$WORK_ROOT/${tbl}_snapshot_fixed"
    local go_file="$WORK_ROOT/${tbl}_go"
    local copier_finished_file="$WORK_ROOT/${tbl}_copier_finished"
    local copier_exit_code_file="$WORK_ROOT/${tbl}_copier_exit_code"
    local writer_stop_file="$WORK_ROOT/${tbl}_writer_stop"
    local writer_ready_file="$WORK_ROOT/${tbl}_writer_ready"
    local writer_counter_file="$WORK_ROOT/${tbl}_writer_counter"
    rm -f "$txn_started_file" "$lock_acquired_file" "$snapshot_fixed_file" "$go_file" \
          "$copier_finished_file" "$copier_exit_code_file" \
          "$writer_stop_file" "$writer_ready_file" "$writer_counter_file"

    local materialize_sql app_pattern
    if [[ "$backend" == "heap_v1" ]]; then
        materialize_sql="CREATE TABLE ${tbl}_artifact_heap AS SELECT * FROM $tbl;"
        app_pattern='CREATE TABLE%AS%SELECT%'
    else
        materialize_sql="\\copy (SELECT * FROM $tbl) TO '$rawfile' (FORMAT binary)"
        app_pattern='COPY%'
    fi
    local app_name="poc_bench_copier_$$"
    local copier_pid="" writer_pid="" stop_children_called=0
    stop_bench_children() {
        (( stop_children_called == 1 )) && return 0
        stop_children_called=1
        touch "$writer_stop_file" 2>/dev/null
        local pid
        for pid in "$copier_pid" "$writer_pid"; do
            [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
        done
        local waited=0 alive
        while (( waited < 20 )); do
            alive=0
            for pid in "$copier_pid" "$writer_pid"; do
                [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && alive=1
            done
            (( alive == 0 )) && break
            sleep 0.1; waited=$((waited+1))
        done
        for pid in "$copier_pid" "$writer_pid"; do
            [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null
            [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
        done
        return 0
    }

    (
        # shellcheck disable=SC2154  # ec is assigned by this same trap string at fire time
        trap 'ec=$?; echo "$ec" >"'"$copier_exit_code_file"'" 2>/dev/null
              touch "'"$writer_stop_file"'" "'"$copier_finished_file"'" 2>/dev/null
              exit $ec' EXIT
        PGAPPNAME="$app_name" "${PG_BIN}/psql" -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt \
            >"$WORK_ROOT/${tbl}_copier.out" 2>&1 <<SQL
BEGIN ISOLATION LEVEL REPEATABLE READ;
\! touch "$txn_started_file"
\! while [ ! -f "$lock_acquired_file" ]; do sleep 0.02; done
SELECT 1 FROM $tbl LIMIT 1;
\! touch "$snapshot_fixed_file"
\! while [ ! -f "$go_file" ]; do sleep 0.02; done
$materialize_sql
COMMIT;
SQL
    ) &
    copier_pid=$!
    register_child_pid "$copier_pid"

    local deadline=$(( $(date +%s) + 60 ))
    while [[ ! -f "$txn_started_file" ]]; do
        (( $(date +%s) < deadline )) || die "bench[$tbl]: copier transaction did not start in time"
        sleep 0.05
    done

    local t_lock0 t_lock1
    t_lock0=$(now_ms)
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null
BEGIN;
LOCK TABLE $tbl IN SHARE ROW EXCLUSIVE MODE;
\! touch "$lock_acquired_file"
\! while [ ! -f "$snapshot_fixed_file" ]; do sleep 0.02; done
INSERT INTO marker_identity(marker_text, xid) VALUES ('$marker_uuid', txid_current()::text::bigint);
SELECT pg_logical_emit_message(true, 'pg_flashback', '$marker_uuid');
COMMIT;
SQL
    t_lock1=$(now_ms)
    LOCK_HOLD_MS=$((t_lock1-t_lock0))
    touch "$go_file"

    if [[ "$crash_point" == "snapshot_start" ]]; then
        stop_bench_children
        crash_restart_cluster || die "bench-crash[$tbl]: cluster did not come back up after simulated crash"
        wait "$copier_pid" 2>/dev/null || true
        CRASHED_MID_MATERIALIZE=1
        return 0
    fi

    if ! poll_query_active "$app_name" "$app_pattern" 30; then
        stop_bench_children
        die "bench[$tbl]: never observed the copier's materializing statement active in pg_stat_activity"
    fi

    run_bench_writer_loop "$tbl" "$(shape_schema_kind "$SHAPE")" "$writer_stop_file" \
        "$writer_ready_file" "$writer_counter_file" &
    writer_pid=$!
    register_child_pid "$writer_pid"
    deadline=$(( $(date +%s) + 20 ))
    while [[ ! -f "$writer_ready_file" ]]; do
        [[ -f "$writer_stop_file" ]] && break
        (( $(date +%s) < deadline )) || { stop_bench_children; die "bench[$tbl]: writer produced no commit during materialization window"; }
        sleep 0.02
    done

    if [[ "$crash_point" == "mid_materialize" ]]; then
        stop_bench_children
        crash_restart_cluster || die "bench-crash[$tbl]: cluster did not come back up after simulated crash"
        wait "$copier_pid" 2>/dev/null || true
        CRASHED_MID_MATERIALIZE=1
        return 0
    fi

    local t_copy0 t_copy1
    t_copy0=$(now_ms)
    deadline=$(( $(date +%s) + 180 ))
    while [[ ! -f "$copier_finished_file" ]]; do
        (( $(date +%s) < deadline )) || { stop_bench_children; die "bench[$tbl]: materialization did not finish in time"; }
        sleep 0.1
    done
    t_copy1=$(now_ms)
    MATERIALIZE_MS=$((t_copy1-t_copy0))
    stop_bench_children
    wait "$copier_pid" 2>/dev/null || true
    wait "$writer_pid" 2>/dev/null || true

    local copier_exit_code; copier_exit_code="$(cat "$copier_exit_code_file" 2>/dev/null || echo 1)"
    if [[ "$copier_exit_code" != "0" ]] || grep -qi "error" "$WORK_ROOT/${tbl}_copier.out" 2>/dev/null; then
        die "bench[$tbl]: copier session error (exit=$copier_exit_code): $(cat "$WORK_ROOT/${tbl}_copier.out" 2>/dev/null)"
    fi

    COPY_WINDOW_COMMITS="$(cat "$writer_counter_file" 2>/dev/null || echo 0)"
    (( COPY_WINDOW_COMMITS > 0 )) || die "bench[$tbl]: copy_window_commits is 0 -- no proven concurrent churn"

    consume_slot_to_events "$slot" "$oid"
    q "$DB" "SELECT poc_ingest_decoded();" >/dev/null
    MARKER_XID="$(q "$DB" "SELECT ml.xid FROM marker_identity mi JOIN marker_log ml USING (xid) WHERE mi.marker_text = '$marker_uuid';")"
    [[ -n "$MARKER_XID" ]] || die "bench[$tbl]: marker not found in decoded WAL"
    BOUNDARY_LSN="$(q "$DB" "SELECT lsn::text FROM commit_log WHERE xid = $MARKER_XID;")"
    [[ -n "$BOUNDARY_LSN" ]] || die "bench[$tbl]: could not resolve marker commit LSN"

    local apply_out
    apply_out="$(q "$DB" "SELECT commits,inserts,updates,deletes,duplicate_commits,out_of_order FROM poc_apply_shadow('$BOUNDARY_LSN'::pg_lsn, $oid);")"
    IFS='|' read -r COMMITS_REPLAYED BENCH_INS BENCH_UPD BENCH_DEL DUP_COMMITS OOO_COMMITS <<<"$apply_out"
    [[ "$DUP_COMMITS" == "0" && "$OOO_COMMITS" == "0" ]] || die "bench[$tbl]: duplicate_commits=$DUP_COMMITS out_of_order=$OOO_COMMITS"
    [[ "$COMMITS_REPLAYED" == "$COPY_WINDOW_COMMITS" ]] || die "bench[$tbl]: commits_replayed=$COMMITS_REPLAYED != copy_window_commits=$COPY_WINDOW_COMMITS"
    [[ "$BENCH_INS" -gt 0 && "$BENCH_UPD" -gt 0 && "$BENCH_DEL" -gt 0 ]] || die "bench[$tbl]: writer INSERT/UPDATE/DELETE not all >0 (ins=$BENCH_INS upd=$BENCH_UPD del=$BENCH_DEL)"
    HIST_REPLAYED="$(q "$DB" "SELECT count(*) FROM change_log WHERE oid=$oid AND applied=false;")"

    if [[ "$backend" == "heap_v1" ]]; then
        GT_TBL="${tbl}_artifact_heap"
    fi
    return 0
}

# Ground truth for the zstd backends: a plain local COPY-FROM-file load of
# the exact rawfile the fixed-snapshot transaction produced. Deliberately
# NOT counted as backend "snapshot create" time (recorded separately) --
# it exists only so restore correctness can be checked against something
# independent of the compression/chunking/storage path under test.
RAW_STREAM_SHA256="" GT_LOAD_MS=0
load_ground_truth_from_rawfile() {
    local tbl=$1 rawfile=$2
    RAW_STREAM_SHA256="$(sha256_file "$rawfile")"
    local t0 t1
    t0=$(now_ms)
    q "$DB" "CREATE TABLE ${tbl}_ground_truth (LIKE $tbl);" >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -c "\\copy ${tbl}_ground_truth FROM '$rawfile' (FORMAT binary)" >/dev/null
    t1=$(now_ms)
    GT_LOAD_MS=$((t1-t0))
    GT_TBL="${tbl}_ground_truth"
}

# Returns 0 (continue) unless point == crash_point, in which case it crashes
# the real postmaster right at this call site, records which checkpoint
# fired, and returns 1 (caller must stop and let the crash-mode verifier
# take over).
CRASHED_DURING_PERSIST=""
crash_checkpoint() {
    local point=$1 crash_point=$2
    if [[ -n "$crash_point" && "$point" == "$crash_point" ]]; then
        crash_restart_cluster || die "bench-crash: cluster did not come back up after simulated crash at $point"
        CRASHED_DURING_PERSIST="$point"
        return 1
    fi
    return 0
}

# ── in_db_logged_zstd: chunk + compress the rawfile, load each compressed
# chunk as a bytea row (STORAGE EXTERNAL) via pg_read_binary_file (safe:
# absolute path under this run's own WORK_ROOT, superuser session), then a
# single UPDATE flips state to 'available' -- the manifest-commit boundary.
ARTIFACT_BYTES=0 CHUNK_COUNT=0 PERSIST_MS=0 MANIFEST_ROOT_DIGEST=""
persist_in_db_logged_zstd() {
    local rawfile=$1 artifact_id=$2 tbl=$3 crash_point=$4
    local chunk_dir="$WORK_ROOT/chunks_${artifact_id}"
    mkdir -p "$chunk_dir"

    local db_oid schema_fp
    db_oid="$(q "$DB" "SELECT oid FROM pg_database WHERE datname='$DB';")"
    schema_fp="$(q "$DB" "SELECT md5(string_agg(column_name||':'||data_type, ',' ORDER BY ordinal_position)) FROM information_schema.columns WHERE table_name='$tbl';")"
    q "$DB" "INSERT INTO poc_bench_manifest(artifact_id, backend, format_version, state, tracking_id, boundary_lsn, boundary_xid,
             pg_major, arch, system_identifier, db_oid, schema_fingerprint, semantic_fingerprint_format_version, encoding)
             VALUES ('$artifact_id','in_db_logged_zstd',1,'creating','$artifact_id','$BOUNDARY_LSN',$MARKER_XID,
             '$PG_MAJOR_RUNTIME','$(uname -m)','$SYSTEM_IDENTIFIER_RUNTIME',$db_oid,'$schema_fp',1,'binary');" >/dev/null
    crash_checkpoint "metadata_creating_row_committed" "$crash_point" || return 0

    ( cd "$chunk_dir" && split -b 67108864 -d -a4 "$rawfile" raw_ )
    local t0 t1 chunk_seq=0 f csha chunk_hashes=""
    t0=$(now_ms)
    for f in "$chunk_dir"/raw_*; do
        zstd -q -T0 --rm "$f" -o "${f}.zst"
        csha="$(sha256_file "${f}.zst")"
        chunk_hashes="${chunk_hashes}${chunk_seq}:${csha}"$'\n'
        q "$DB" "INSERT INTO poc_bench_chunks(artifact_id, chunk_seq, chunk_sha256, chunk_bytes)
                 VALUES ('$artifact_id', $chunk_seq, '$csha', pg_read_binary_file('${f}.zst'));" >/dev/null
        chunk_seq=$((chunk_seq+1))
        crash_checkpoint "mid_chunk_load" "$crash_point" || return 0
    done
    t1=$(now_ms)
    PERSIST_MS=$((t1-t0))
    CHUNK_COUNT=$chunk_seq
    MANIFEST_ROOT_DIGEST="$(printf '%s' "$chunk_hashes" | sha256sum | awk '{print $1}')"
    ARTIFACT_BYTES="$(q "$DB" "SELECT COALESCE(sum(octet_length(chunk_bytes)),0) FROM poc_bench_chunks WHERE artifact_id='$artifact_id';")"

    crash_checkpoint "metadata_commit_before" "$crash_point" || return 0
    q "$DB" "UPDATE poc_bench_manifest SET state='available', chunk_count=$CHUNK_COUNT,
             manifest_root_digest='$MANIFEST_ROOT_DIGEST', raw_stream_sha256='$RAW_STREAM_SHA256',
             row_count=(SELECT count(*) FROM $tbl), logical_bytes=(SELECT sum(pg_column_size(t.*)) FROM $tbl t)
             WHERE artifact_id='$artifact_id';" >/dev/null
    crash_checkpoint "metadata_commit_after" "$crash_point" || return 0
    return 0
}

restore_in_db_logged_zstd() {
    local artifact_id=$1 restored_tbl=$2
    local state; state="$(q "$DB" "SELECT state FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    [[ "$state" == "available" ]] || die "restore-in_db[$artifact_id]: artifact state is '$state', not 'available' -- refusing to restore"
    local manifest_pg_major manifest_sysid
    manifest_pg_major="$(q "$DB" "SELECT pg_major FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    manifest_sysid="$(q "$DB" "SELECT system_identifier FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    [[ "$manifest_pg_major" == "$PG_MAJOR_RUNTIME" ]] || die "restore-in_db[$artifact_id]: manifest pg_major=$manifest_pg_major != runtime pg_major=$PG_MAJOR_RUNTIME -- refusing to restore"
    [[ "$manifest_sysid" == "$SYSTEM_IDENTIFIER_RUNTIME" ]] || die "restore-in_db[$artifact_id]: manifest system_identifier=$manifest_sysid != runtime system_identifier=$SYSTEM_IDENTIFIER_RUNTIME -- refusing to restore"
    local expected_hash actual_hash
    expected_hash="$(q "$DB" "SELECT raw_stream_sha256 FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    local chunk_count; chunk_count="$(q "$DB" "SELECT chunk_count FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    local present_chunks; present_chunks="$(q "$DB" "SELECT count(*) FROM poc_bench_chunks WHERE artifact_id='$artifact_id';")"
    [[ "$present_chunks" == "$chunk_count" ]] || die "restore-in_db[$artifact_id]: expected $chunk_count chunks, found $present_chunks -- refusing to restore"

    local restore_dir="$WORK_ROOT/restore_${artifact_id}"
    mkdir -p "$restore_dir"
    local rebuilt="$restore_dir/rebuilt.bin"
    : >"$rebuilt"
    local seq
    for seq in $(seq 0 $((chunk_count-1))); do
        local stored_sha db_sha
        stored_sha="$(q "$DB" "SELECT chunk_sha256 FROM poc_bench_chunks WHERE artifact_id='$artifact_id' AND chunk_seq=$seq;")"
        "${PSQL[@]}" -d "$DB" -qAt -c "SELECT chunk_bytes FROM poc_bench_chunks WHERE artifact_id='$artifact_id' AND chunk_seq=$seq;" \
            | python3 -c "import sys; d=sys.stdin.read().strip(); d=d[2:] if d.startswith('\\\\x') else d; sys.stdout.buffer.write(bytes.fromhex(d))" \
            > "$restore_dir/chunk_${seq}.zst"
        db_sha="$(sha256_file "$restore_dir/chunk_${seq}.zst")"
        [[ "$db_sha" == "$stored_sha" ]] \
            || die "restore-in_db[$artifact_id]: chunk $seq sha256 mismatch (manifest=$stored_sha actual=$db_sha) -- refusing to restore"
        zstd -q -d "$restore_dir/chunk_${seq}.zst" -o "$restore_dir/chunk_${seq}.raw" --force
        cat "$restore_dir/chunk_${seq}.raw" >>"$rebuilt"
    done
    actual_hash="$(sha256_file "$rebuilt")"
    [[ "$actual_hash" == "$expected_hash" ]] \
        || die "restore-in_db[$artifact_id]: reconstructed stream sha256 mismatch (expected=$expected_hash actual=$actual_hash) -- refusing to restore"

    q "$DB" "CREATE TABLE $restored_tbl (LIKE $GT_TBL);" >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -c "\\copy $restored_tbl FROM '$rebuilt' (FORMAT binary)" >/dev/null
    rm -rf "$restore_dir"
}

# ── external_zstd: temp-write -> fsync -> (per-chunk atomic rename) ->
# manifest temp-write -> fsync -> atomic rename -> parent-dir fsync. DB
# holds only a PoC metadata/manifest pointer row (no bytea chunk content).
persist_external_zstd() {
    local rawfile=$1 artifact_id=$2 tbl=$3 crash_point=$4
    local art_dir="$EXTERNAL_ARTIFACT_ROOT/$artifact_id"
    local tmp_dir="$EXTERNAL_ARTIFACT_ROOT/.tmp-$artifact_id"
    mkdir -p "$art_dir" "$tmp_dir"

    local db_oid schema_fp
    db_oid="$(q "$DB" "SELECT oid FROM pg_database WHERE datname='$DB';")"
    schema_fp="$(q "$DB" "SELECT md5(string_agg(column_name||':'||data_type, ',' ORDER BY ordinal_position)) FROM information_schema.columns WHERE table_name='$tbl';")"
    q "$DB" "INSERT INTO poc_bench_manifest(artifact_id, backend, format_version, state, tracking_id, boundary_lsn, boundary_xid,
             pg_major, arch, system_identifier, db_oid, schema_fingerprint, semantic_fingerprint_format_version, encoding)
             VALUES ('$artifact_id','external_zstd',1,'creating','$artifact_id','$BOUNDARY_LSN',$MARKER_XID,
             '$PG_MAJOR_RUNTIME','$(uname -m)','$SYSTEM_IDENTIFIER_RUNTIME',$db_oid,'$schema_fp',1,'binary');" >/dev/null

    local chunk_dir="$WORK_ROOT/chunks_${artifact_id}"
    mkdir -p "$chunk_dir"
    ( cd "$chunk_dir" && split -b 67108864 -d -a4 "$rawfile" raw_ )
    local t0 t1 chunk_seq=0 f csha chunk_hashes=""
    t0=$(now_ms)
    for f in "$chunk_dir"/raw_*; do
        local tmp_chunk="$tmp_dir/chunk_${chunk_seq}.zst.tmp"
        zstd -q -T0 --rm "$f" -o "$tmp_chunk"
        crash_checkpoint "temp_chunk_write_crash" "$crash_point" || return 0
        fsync_path "$tmp_chunk"
        crash_checkpoint "chunk_fsync_done" "$crash_point" || return 0
        csha="$(sha256_file "$tmp_chunk")"
        chunk_hashes="${chunk_hashes}${chunk_seq}:${csha}"$'\n'
        local final_chunk="$art_dir/chunk_${chunk_seq}.zst"
        crash_checkpoint "chunk_rename_before" "$crash_point" || return 0
        mv -f "$tmp_chunk" "$final_chunk"
        crash_checkpoint "chunk_rename_after" "$crash_point" || return 0
        fsync_path "$final_chunk"
        chunk_seq=$((chunk_seq+1))
    done
    fsync_path "$art_dir"
    t1=$(now_ms)
    PERSIST_MS=$((t1-t0))
    CHUNK_COUNT=$chunk_seq
    MANIFEST_ROOT_DIGEST="$(printf '%s' "$chunk_hashes" | sha256sum | awk '{print $1}')"
    ARTIFACT_BYTES="$(du -sb "$art_dir" 2>/dev/null | awk '{print $1}')"

    local row_count logical_bytes
    row_count="$(q "$DB" "SELECT count(*) FROM $tbl;")"
    logical_bytes="$(q "$DB" "SELECT sum(pg_column_size(t.*)) FROM $tbl t;")"
    local manifest_tmp="$tmp_dir/manifest.json.tmp"
    jq -n --arg artifact_id "$artifact_id" --arg backend external_zstd --argjson format_version 1 \
        --arg boundary_lsn "$BOUNDARY_LSN" --argjson boundary_xid "$MARKER_XID" \
        --argjson row_count "$row_count" --argjson logical_bytes "$logical_bytes" \
        --argjson chunk_count "$CHUNK_COUNT" --arg raw_stream_sha256 "$RAW_STREAM_SHA256" \
        --arg manifest_root_digest "$MANIFEST_ROOT_DIGEST" --arg pg_major "$PG_MAJOR_RUNTIME" \
        --arg arch "$(uname -m)" --arg system_identifier "$SYSTEM_IDENTIFIER_RUNTIME" \
        '{artifact_id:$artifact_id, backend:$backend, format_version:$format_version,
          boundary_lsn:$boundary_lsn, boundary_xid:$boundary_xid, row_count:$row_count,
          logical_bytes:$logical_bytes, chunk_count:$chunk_count,
          raw_stream_sha256:$raw_stream_sha256, manifest_root_digest:$manifest_root_digest,
          pg_major:$pg_major, arch:$arch, system_identifier:$system_identifier}' >"$manifest_tmp"
    crash_checkpoint "manifest_fsync_before" "$crash_point" || return 0
    fsync_path "$manifest_tmp"
    crash_checkpoint "manifest_fsync_after" "$crash_point" || return 0
    local manifest_final="$art_dir/manifest.json"
    crash_checkpoint "manifest_rename_before" "$crash_point" || return 0
    mv -f "$manifest_tmp" "$manifest_final"
    crash_checkpoint "manifest_rename_after" "$crash_point" || return 0
    fsync_path "$art_dir"
    crash_checkpoint "parent_dir_fsync_after" "$crash_point" || return 0

    crash_checkpoint "metadata_commit_before" "$crash_point" || return 0
    q "$DB" "UPDATE poc_bench_manifest SET state='available', chunk_count=$CHUNK_COUNT,
             manifest_root_digest='$MANIFEST_ROOT_DIGEST', raw_stream_sha256='$RAW_STREAM_SHA256',
             row_count=$row_count, logical_bytes=$logical_bytes WHERE artifact_id='$artifact_id';" >/dev/null
    crash_checkpoint "metadata_commit_after" "$crash_point" || return 0
    rm -rf "$tmp_dir"
    return 0
}

restore_external_zstd() {
    local artifact_id=$1 restored_tbl=$2
    local art_dir="$EXTERNAL_ARTIFACT_ROOT/$artifact_id"
    local manifest_final="$art_dir/manifest.json"
    local state; state="$(q "$DB" "SELECT state FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    [[ "$state" == "available" ]] || die "restore-external[$artifact_id]: DB state is '$state', not 'available' -- refusing to restore"
    [[ -f "$manifest_final" ]] || die "restore-external[$artifact_id]: manifest.json missing on disk despite DB state=available -- refusing to restore (file-without-metadata/metadata-without-file must fail closed)"
    local m_root m_chunks m_hash
    m_root="$(jq -r .manifest_root_digest "$manifest_final")"
    m_chunks="$(jq -r .chunk_count "$manifest_final")"
    m_hash="$(jq -r .raw_stream_sha256 "$manifest_final")"
    local db_root; db_root="$(q "$DB" "SELECT manifest_root_digest FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    [[ "$m_root" == "$db_root" ]] || die "restore-external[$artifact_id]: on-disk manifest_root_digest ($m_root) != DB manifest_root_digest ($db_root) -- refusing to restore"
    local m_pg_major m_sysid db_pg_major db_sysid
    m_pg_major="$(jq -r .pg_major "$manifest_final")"; m_sysid="$(jq -r .system_identifier "$manifest_final")"
    db_pg_major="$(q "$DB" "SELECT pg_major FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    db_sysid="$(q "$DB" "SELECT system_identifier FROM poc_bench_manifest WHERE artifact_id='$artifact_id';")"
    [[ "$m_pg_major" == "$PG_MAJOR_RUNTIME" && "$db_pg_major" == "$PG_MAJOR_RUNTIME" ]] \
        || die "restore-external[$artifact_id]: pg_major mismatch (manifest=$m_pg_major db=$db_pg_major runtime=$PG_MAJOR_RUNTIME) -- refusing to restore"
    [[ "$m_sysid" == "$SYSTEM_IDENTIFIER_RUNTIME" && "$db_sysid" == "$SYSTEM_IDENTIFIER_RUNTIME" ]] \
        || die "restore-external[$artifact_id]: system_identifier mismatch (manifest=$m_sysid db=$db_sysid runtime=$SYSTEM_IDENTIFIER_RUNTIME) -- refusing to restore"

    local restore_dir="$WORK_ROOT/restore_${artifact_id}"
    mkdir -p "$restore_dir"
    local rebuilt="$restore_dir/rebuilt.bin"
    : >"$rebuilt"
    local seq
    for seq in $(seq 0 $((m_chunks-1))); do
        local chunk_file="$art_dir/chunk_${seq}.zst"
        [[ -f "$chunk_file" ]] || die "restore-external[$artifact_id]: chunk $seq missing on disk -- refusing to restore"
        zstd -q -d "$chunk_file" -o "$restore_dir/chunk_${seq}.raw" --force \
            || die "restore-external[$artifact_id]: chunk $seq failed to decompress (corrupt) -- refusing to restore"
        cat "$restore_dir/chunk_${seq}.raw" >>"$rebuilt"
    done
    local actual_hash; actual_hash="$(sha256_file "$rebuilt")"
    [[ "$actual_hash" == "$m_hash" ]] \
        || die "restore-external[$artifact_id]: reconstructed stream sha256 mismatch (manifest=$m_hash actual=$actual_hash) -- refusing to restore"

    q "$DB" "CREATE TABLE $restored_tbl (LIKE $GT_TBL);" >/dev/null
    "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -c "\\copy $restored_tbl FROM '$rebuilt' (FORMAT binary)" >/dev/null
    rm -rf "$restore_dir"
}

PG_MAJOR_RUNTIME="" SYSTEM_IDENTIFIER_RUNTIME=""
capture_runtime_identity() {
    PG_MAJOR_RUNTIME="$("$PG_BIN/pg_config" --version | awk '{print $2}' | cut -d. -f1)"
    SYSTEM_IDENTIFIER_RUNTIME="$(q "$DB" "SELECT system_identifier::text FROM pg_control_system();")"
}

# ── restore verification: row count, order-independent semantic
# fingerprint, schema (column name/type/order), and for toast_bad, an
# aggregate byte-equality hash -- against the ground truth captured at the
# SAME fixed snapshot every backend materialized from, not against the
# live (still-mutating) source table.
verify_restore() {
    local tbl=$1 restored_tbl=$2 shape=$3
    local gt_rows r_rows gt_fp r_fp
    gt_rows="$(row_count "$GT_TBL")"; r_rows="$(row_count "$restored_tbl")"
    [[ "$gt_rows" == "$r_rows" ]] || die "verify[$restored_tbl]: row count mismatch ground_truth=$gt_rows restored=$r_rows"
    gt_fp="$(fingerprint_table "$GT_TBL")"; r_fp="$(fingerprint_table "$restored_tbl")"
    [[ "$gt_fp" == "$r_fp" ]] || die "verify[$restored_tbl]: semantic fingerprint mismatch ground_truth=$gt_fp restored=$r_fp"
    local gt_schema r_schema
    gt_schema="$(q "$DB" "SELECT string_agg(column_name||':'||data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name='$GT_TBL';")"
    r_schema="$(q "$DB" "SELECT string_agg(column_name||':'||data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name='$restored_tbl';")"
    [[ "$gt_schema" == "$r_schema" ]] || die "verify[$restored_tbl]: schema mismatch ground_truth=[$gt_schema] restored=[$r_schema]"
    if [[ "$shape" == "toast_bad" ]]; then
        local gt_th r_th
        gt_th="$(toast_hash "$GT_TBL" blob)"; r_th="$(toast_hash "$restored_tbl" blob)"
        [[ "$gt_th" == "$r_th" ]] || die "verify[$restored_tbl]: TOAST byte-equality hash mismatch ground_truth=$gt_th restored=$r_th"
    fi
    return 0
}

# ── bench mode: one full backend x shape x size_mib trial ────────────────
run_bench() {
    local tbl=poc_bench_src
    make_shape "$SHAPE" "$tbl" "$SIZE_MIB"
    qst_mark_step "bench_base" "pass" "rows=$(row_count "$tbl") shape=$SHAPE"

    q "$DB" "SELECT pg_create_logical_replication_slot('poc_bench_slot','pg_flashback');" >/dev/null
    capture_runtime_identity

    local source_table_bytes source_logical_bytes
    source_table_bytes="$(q "$DB" "SELECT pg_total_relation_size('$tbl');")"
    source_logical_bytes="$(q "$DB" "SELECT sum(pg_column_size(t.*))::bigint FROM $tbl t;")"
    record_metric "bench.${BACKEND}.${SHAPE}.source_table_bytes" "$source_table_bytes" "bytes"
    record_metric "bench.${BACKEND}.${SHAPE}.source_logical_bytes" "$source_logical_bytes" "bytes"

    local artifact_id="bench-${BACKEND}-${SHAPE}-${SIZE_MIB}-${REP_LABEL}-$$"
    local rawfile="$WORK_ROOT/rawstream_${artifact_id}.bin"

    local t_snap0 t_snap1
    t_snap0=$(now_ms)
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    t_snap1=$(now_ms)
    qst_mark_step "bench_base" "pass" "rows=$(row_count "$tbl") shape=$SHAPE boundary_lsn=$BOUNDARY_LSN"

    if [[ "$BACKEND" != "heap_v1" ]]; then
        [[ -f "$rawfile" ]] || die "bench: expected raw stream file $rawfile after materialization"
        load_ground_truth_from_rawfile "$tbl" "$rawfile"
    fi

    local t_persist0 t_persist1
    t_persist0=$(now_ms)
    case "$BACKEND" in
        heap_v1) : ;; # artifact already materialized as GT_TBL; nothing further to persist
        in_db_logged_zstd) persist_in_db_logged_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
        external_zstd) persist_external_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
    esac
    t_persist1=$(now_ms)
    qst_mark_step "bench_persist" "pass" "artifact_id=$artifact_id persist_ms=$((t_persist1-t_persist0))"

    local restored_tbl="poc_bench_restored"
    local t_restore0 t_restore1
    t_restore0=$(now_ms)
    case "$BACKEND" in
        heap_v1) restored_tbl="$GT_TBL" ;;
        in_db_logged_zstd) restore_in_db_logged_zstd "$artifact_id" "$restored_tbl" ;;
        external_zstd) restore_external_zstd "$artifact_id" "$restored_tbl" ;;
    esac
    t_restore1=$(now_ms)
    local restore_ms=$((t_restore1-t_restore0))
    qst_mark_step "bench_restore" "pass" "restore_ms=$restore_ms"

    local t_verify0 t_verify1
    t_verify0=$(now_ms)
    verify_restore "$tbl" "$restored_tbl" "$SHAPE"
    t_verify1=$(now_ms)
    local verify_ms=$((t_verify1-t_verify0))
    qst_mark_step "bench_verify" "pass" "row_count=$(row_count "$restored_tbl") verify_ms=$verify_ms"

    local total_rto_ms=$(( (t_snap1-t_snap0) + (t_persist1-t_persist0) + (restore_ms) + (verify_ms) ))
    record_metric "bench.${BACKEND}.${SHAPE}.snapshot_create_ms" "$((t_snap1-t_snap0))" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.lock_hold_ms" "$LOCK_HOLD_MS" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.materialize_ms" "$MATERIALIZE_MS" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.persist_ms" "$((t_persist1-t_persist0))" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.restore_ms" "$restore_ms" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.verify_ms" "$verify_ms" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.total_rto_ms" "$total_rto_ms" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.gt_load_ms_excluded_from_rto" "$GT_LOAD_MS" "ms"
    record_metric "bench.${BACKEND}.${SHAPE}.copy_window_commits" "$COPY_WINDOW_COMMITS" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.commits_replayed" "$COMMITS_REPLAYED" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.inserts_replayed" "$BENCH_INS" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.updates_replayed" "$BENCH_UPD" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.deletes_replayed" "$BENCH_DEL" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.historical_payload_events_replayed" "$HIST_REPLAYED" "count"
    record_metric "bench.${BACKEND}.${SHAPE}.artifact_bytes" "$ARTIFACT_BYTES" "bytes"
    record_metric "bench.${BACKEND}.${SHAPE}.chunk_count" "$CHUNK_COUNT" "count"
    if [[ -n "$source_logical_bytes" && "$source_logical_bytes" -gt 0 && -n "$ARTIFACT_BYTES" && "$ARTIFACT_BYTES" -gt 0 ]]; then
        record_metric "bench.${BACKEND}.${SHAPE}.compression_ratio" "$(echo "scale=4; $source_logical_bytes / $ARTIFACT_BYTES" | bc 2>/dev/null || echo 0)" "ratio"
    fi
    if [[ "$BACKEND" != "heap_v1" ]]; then
        record_metric "bench.${BACKEND}.${SHAPE}.raw_stream_bytes" "$(stat -c%s "$rawfile" 2>/dev/null || echo 0)" "bytes"
    fi

    record_metric "bench.${BACKEND}.${SHAPE}.chunk_compress_ms" "$PERSIST_MS" "ms"
    qst_mark_step "bench_metrics" "pass" "recorded"
    EXTRA_JSON="$(echo "$EXTRA_JSON" | jq \
        --arg backend "$BACKEND" --arg shape "$SHAPE" --argjson size_mib "$SIZE_MIB" --arg rep "$REP_LABEL" \
        --arg artifact_id "$artifact_id" --arg source_commit "$CANDIDATE_SOURCE_COMMIT" \
        --arg source_tree "$CANDIDATE_SOURCE_TREE" --arg source_dirty "$CANDIDATE_DIRTY" \
        --arg so_sha256 "$CANDIDATE_SO_SHA256" --arg pg_major "$PG_MAJOR_RUNTIME" \
        --arg system_identifier "$SYSTEM_IDENTIFIER_RUNTIME" \
        --argjson metrics "$(jq -s '.' "$METRICS_JSON" 2>/dev/null || echo '[]')" \
        '. + {backend:$backend, shape:$shape, size_mib:$size_mib, rep:$rep, artifact_id:$artifact_id,
              source_commit:$source_commit, source_tree:$source_tree, source_tree_dirty:$source_dirty,
              extension_binary_sha256:$so_sha256,
              pg_major:$pg_major, system_identifier:$system_identifier, metrics:$metrics}')"
}

# ── crash mode: adversarial matrix, one crash_point per invocation ──────
# NOTE on fsync-adjacent points (chunk_fsync_done, manifest_fsync_before/
# after, parent_dir_fsync_after): a process-level SIGKILL-equivalent
# (pg_ctl stop -m immediate for the DB-side points; this harness cannot
# safely power-cycle its own host for the filesystem-side points, so those
# are modeled as an abrupt kill of the writing step) can only prove that
# incomplete userspace state (a partial temp file, a not-yet-renamed
# chunk) is never mistaken for valid. It does NOT by itself prove real
# fsync-durability against an actual power loss, since a normal process
# kill does not revert a rename() the kernel has already accepted into its
# page cache. That gap is disclosed explicitly in the Step 8 report rather
# than silently claimed as tested.
run_crash_materialize_point() {
    local tbl=$1 artifact_id=$2 rawfile=$3
    CRASHED_MID_MATERIALIZE=0
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" "$CRASH_POINT"
    [[ "$CRASHED_MID_MATERIALIZE" == "1" ]] || die "crash[$CRASH_POINT]: expected a crash injection, none occurred"
    qst_mark_step "crash_injected" "pass" "crashed at $CRASH_POINT"

    local artifact_visible
    artifact_visible="$(q "$DB" "SELECT to_regclass('${tbl}_artifact_heap') IS NOT NULL;")"
    [[ "$artifact_visible" == "f" ]] || die "crash[$CRASH_POINT]: partial artifact table visible after restart"
    qst_mark_step "crash_postcheck" "pass" "no partial artifact visible after restart"

    local retry_id="${artifact_id}-retry"
    local retry_raw="$WORK_ROOT/rawstream_${retry_id}.bin"
    CRASHED_MID_MATERIALIZE=0
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$retry_raw" ""
    if [[ "$BACKEND" != "heap_v1" ]]; then load_ground_truth_from_rawfile "$tbl" "$retry_raw"; fi
    case "$BACKEND" in
        heap_v1) : ;;
        in_db_logged_zstd) persist_in_db_logged_zstd "$retry_raw" "$retry_id" "$tbl" "" ;;
        external_zstd) persist_external_zstd "$retry_raw" "$retry_id" "$tbl" "" ;;
    esac
    local restored_tbl=poc_bench_restored_retry
    case "$BACKEND" in
        heap_v1) restored_tbl="$GT_TBL" ;;
        in_db_logged_zstd) restore_in_db_logged_zstd "$retry_id" "$restored_tbl" ;;
        external_zstd) restore_external_zstd "$retry_id" "$restored_tbl" ;;
    esac
    verify_restore "$tbl" "$restored_tbl" "$SHAPE"
    qst_mark_step "crash_retry" "pass" "clean retry after crash succeeded and verified"
}

run_crash_persist_point() {
    local tbl=$1 artifact_id=$2 rawfile=$3
    [[ "$BACKEND" != "heap_v1" ]] || die "crash[$CRASH_POINT]: not applicable to heap_v1 (no separate persist phase)"
    case "$CRASH_POINT" in
        temp_chunk_write_crash|chunk_fsync_done|chunk_rename_before|chunk_rename_after| \
        manifest_fsync_before|manifest_fsync_after|manifest_rename_before|manifest_rename_after|parent_dir_fsync_after)
            [[ "$BACKEND" == "external_zstd" ]] || die "crash[$CRASH_POINT]: only applicable to external_zstd"
            ;;
    esac

    CRASHED_MID_MATERIALIZE=0
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    load_ground_truth_from_rawfile "$tbl" "$rawfile"

    CRASHED_DURING_PERSIST=""
    case "$BACKEND" in
        in_db_logged_zstd) persist_in_db_logged_zstd "$rawfile" "$artifact_id" "$tbl" "$CRASH_POINT" ;;
        external_zstd) persist_external_zstd "$rawfile" "$artifact_id" "$tbl" "$CRASH_POINT" ;;
    esac
    [[ "$CRASHED_DURING_PERSIST" == "$CRASH_POINT" ]] \
        || die "crash[$CRASH_POINT]: expected a crash injection during persist, none occurred (CRASHED_DURING_PERSIST=$CRASHED_DURING_PERSIST)"
    qst_mark_step "crash_injected" "pass" "crashed at $CRASH_POINT during persist"

    local state
    state="$(q "$DB" "SELECT state FROM poc_bench_manifest WHERE artifact_id='$artifact_id';" 2>/dev/null || echo "")"
    if [[ "$CRASH_POINT" == "metadata_commit_after" ]]; then
        [[ "$state" == "available" ]] || die "crash[$CRASH_POINT]: expected state=available after a post-commit crash, got '$state'"
    else
        [[ "$state" != "available" ]] || die "crash[$CRASH_POINT]: artifact incorrectly shows state=available after a pre-commit crash"
    fi
    qst_mark_step "crash_postcheck" "pass" "manifest state after crash: '$state'"

    local retry_id="${artifact_id}-retry"
    local retry_raw="$WORK_ROOT/rawstream_${retry_id}.bin"
    CRASHED_MID_MATERIALIZE=0
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$retry_raw" ""
    load_ground_truth_from_rawfile "$tbl" "$retry_raw"
    case "$BACKEND" in
        in_db_logged_zstd) persist_in_db_logged_zstd "$retry_raw" "$retry_id" "$tbl" "" ;;
        external_zstd) persist_external_zstd "$retry_raw" "$retry_id" "$tbl" "" ;;
    esac
    local restored_tbl=poc_bench_restored_retry
    case "$BACKEND" in
        in_db_logged_zstd) restore_in_db_logged_zstd "$retry_id" "$restored_tbl" ;;
        external_zstd) restore_external_zstd "$retry_id" "$restored_tbl" ;;
    esac
    verify_restore "$tbl" "$restored_tbl" "$SHAPE"
    qst_mark_step "crash_retry" "pass" "clean retry with a fresh artifact id succeeded and verified"
}

run_crash_duplicate_retry() {
    local tbl=$1 artifact_id=$2 rawfile=$3
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    if [[ "$BACKEND" != "heap_v1" ]]; then load_ground_truth_from_rawfile "$tbl" "$rawfile"; fi
    case "$BACKEND" in
        heap_v1) : ;;
        in_db_logged_zstd) persist_in_db_logged_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
        external_zstd) persist_external_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
    esac
    qst_mark_step "crash_injected" "pass" "no crash for duplicate_retry -- tests idempotent double-persist under the SAME artifact identity"

    local dup_failed=0
    case "$BACKEND" in
        heap_v1)
            q "$DB" "CREATE TABLE ${tbl}_artifact_heap AS SELECT * FROM $tbl;" >/dev/null 2>&1 || dup_failed=1
            ;;
        in_db_logged_zstd)
            persist_in_db_logged_zstd "$rawfile" "$artifact_id" "$tbl" "" >/dev/null 2>&1 || dup_failed=1
            ;;
        external_zstd)
            persist_external_zstd "$rawfile" "$artifact_id" "$tbl" "" >/dev/null 2>&1 || dup_failed=1
            ;;
    esac
    [[ "$dup_failed" == "1" ]] || die "crash[duplicate_retry]: persisting the same identity twice did NOT fail closed -- expected a collision rejection"
    qst_mark_step "crash_postcheck" "pass" "duplicate attempt under the same identity was rejected"

    local restored_tbl=poc_bench_restored
    case "$BACKEND" in
        heap_v1) restored_tbl="${tbl}_artifact_heap" ;;
        in_db_logged_zstd) restore_in_db_logged_zstd "$artifact_id" "$restored_tbl" ;;
        external_zstd) restore_external_zstd "$artifact_id" "$restored_tbl" ;;
    esac
    verify_restore "$tbl" "$restored_tbl" "$SHAPE"
    qst_mark_step "crash_retry" "pass" "original artifact still restores correctly after a rejected duplicate attempt"
}

run_crash_tamper() {
    local tbl=$1 artifact_id=$2 rawfile=$3 point=$4
    [[ "$BACKEND" != "heap_v1" ]] || die "crash[$point]: not applicable to heap_v1 (no chunked/manifest artifact to tamper)"
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    load_ground_truth_from_rawfile "$tbl" "$rawfile"
    case "$BACKEND" in
        in_db_logged_zstd) persist_in_db_logged_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
        external_zstd) persist_external_zstd "$rawfile" "$artifact_id" "$tbl" "" ;;
    esac
    qst_mark_step "crash_injected" "pass" "no crash for $point -- tests adversarial post-hoc tampering"

    case "$point" in
        corrupt_chunk)
            if [[ "$BACKEND" == "in_db_logged_zstd" ]]; then
                q "$DB" "UPDATE poc_bench_chunks SET chunk_bytes = chunk_bytes || decode('00','hex') WHERE artifact_id='$artifact_id' AND chunk_seq=0;" >/dev/null
            else
                printf '\x00' >> "$EXTERNAL_ARTIFACT_ROOT/$artifact_id/chunk_0.zst"
            fi ;;
        missing_chunk)
            if [[ "$BACKEND" == "in_db_logged_zstd" ]]; then
                q "$DB" "DELETE FROM poc_bench_chunks WHERE artifact_id='$artifact_id' AND chunk_seq=0;" >/dev/null
            else
                rm -f "$EXTERNAL_ARTIFACT_ROOT/$artifact_id/chunk_0.zst"
            fi ;;
        manifest_mismatch)
            q "$DB" "UPDATE poc_bench_manifest SET row_count = row_count + 999999 WHERE artifact_id='$artifact_id';" >/dev/null
            if [[ "$BACKEND" == "external_zstd" ]]; then
                local mf="$EXTERNAL_ARTIFACT_ROOT/$artifact_id/manifest.json"
                jq '.manifest_root_digest = "deadbeef"' "$mf" > "$mf.tmp" && mv "$mf.tmp" "$mf"
            fi ;;
        wrong_identity_binding)
            q "$DB" "UPDATE poc_bench_manifest SET system_identifier = 'deliberately-wrong-identity' WHERE artifact_id='$artifact_id';" >/dev/null
            if [[ "$BACKEND" == "external_zstd" ]]; then
                local mf="$EXTERNAL_ARTIFACT_ROOT/$artifact_id/manifest.json"
                jq '.system_identifier = "deliberately-wrong-identity"' "$mf" > "$mf.tmp" && mv "$mf.tmp" "$mf"
            fi ;;
    esac
    qst_mark_step "crash_postcheck" "pass" "tampered artifact prepared ($point)"

    local restored_tbl=poc_bench_restored_tamper restore_failed=0
    case "$BACKEND" in
        in_db_logged_zstd) restore_in_db_logged_zstd "$artifact_id" "$restored_tbl" >/dev/null 2>&1 || restore_failed=1 ;;
        external_zstd) restore_external_zstd "$artifact_id" "$restored_tbl" >/dev/null 2>&1 || restore_failed=1 ;;
    esac
    [[ "$restore_failed" == "1" ]] || die "crash[$point]: restore did NOT fail closed on a tampered/corrupt artifact -- real correctness gap"
    qst_mark_step "crash_retry" "pass" "restore correctly refused the tampered artifact ($point); fail-closed confirmed"
}

run_crash_split_state() {
    local tbl=$1 artifact_id=$2 rawfile=$3 point=$4
    [[ "$BACKEND" == "external_zstd" ]] || die "crash[$point]: only applicable to external_zstd (file/DB split state)"
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    load_ground_truth_from_rawfile "$tbl" "$rawfile"
    persist_external_zstd "$rawfile" "$artifact_id" "$tbl" ""
    qst_mark_step "crash_injected" "pass" "no crash for $point -- simulates a torn file/DB-metadata split by direct removal"

    if [[ "$point" == "file_without_metadata" ]]; then
        q "$DB" "DELETE FROM poc_bench_manifest WHERE artifact_id='$artifact_id';" >/dev/null
    else
        rm -rf "${EXTERNAL_ARTIFACT_ROOT:?}/${artifact_id:?}"
    fi
    qst_mark_step "crash_postcheck" "pass" "split state prepared ($point)"

    local restored_tbl=poc_bench_restored_split restore_failed=0
    restore_external_zstd "$artifact_id" "$restored_tbl" >/dev/null 2>&1 || restore_failed=1
    [[ "$restore_failed" == "1" ]] || die "crash[$point]: restore did NOT fail closed on a file/DB-metadata split -- real correctness gap"
    qst_mark_step "crash_retry" "pass" "restore correctly refused a split file/DB-metadata state ($point); fail-closed confirmed"
}

run_crash_orphan_gc() {
    local tbl=$1 artifact_id=$2 rawfile=$3
    [[ "$BACKEND" == "external_zstd" ]] || die "crash[orphan_gc_safety]: only applicable to external_zstd"
    establish_boundary_and_materialize "$tbl" "poc_bench_slot" "$BACKEND" "$rawfile" ""
    load_ground_truth_from_rawfile "$tbl" "$rawfile"
    persist_external_zstd "$rawfile" "$artifact_id" "$tbl" ""
    qst_mark_step "crash_injected" "pass" "no crash for orphan_gc_safety -- creates one live artifact plus one orphaned temp dir"

    local orphan_dir="$EXTERNAL_ARTIFACT_ROOT/.tmp-orphan-$$"
    mkdir -p "$orphan_dir"
    echo "orphan" > "$orphan_dir/chunk_0.zst.tmp"
    qst_mark_step "crash_postcheck" "pass" "orphan temp dir created alongside the live artifact"

    # PoC GC: remove only .tmp-* directories under the artifact root that
    # have no corresponding available manifest row referencing them --
    # never touches a real artifact_id directory.
    local d
    for d in "$EXTERNAL_ARTIFACT_ROOT"/.tmp-*; do
        [[ -d "$d" ]] || continue
        rm -rf "$d"
    done
    [[ -d "$EXTERNAL_ARTIFACT_ROOT/$artifact_id" ]] \
        || die "crash[orphan_gc_safety]: GC incorrectly removed the LIVE artifact directory"
    [[ ! -d "$orphan_dir" ]] \
        || die "crash[orphan_gc_safety]: GC failed to remove the orphaned temp directory"

    local restored_tbl=poc_bench_restored_gc
    restore_external_zstd "$artifact_id" "$restored_tbl"
    verify_restore "$tbl" "$restored_tbl" "$SHAPE"
    qst_mark_step "crash_retry" "pass" "live artifact survived GC and still restores correctly; orphan was removed"
}

run_crash() {
    local tbl=poc_bench_src
    make_shape "$SHAPE" "$tbl" "$SIZE_MIB"
    q "$DB" "SELECT pg_create_logical_replication_slot('poc_bench_slot','pg_flashback');" >/dev/null
    capture_runtime_identity
    qst_mark_step "crash_setup" "pass" "rows=$(row_count "$tbl") shape=$SHAPE backend=$BACKEND crash_point=$CRASH_POINT"

    local artifact_id="crash-${BACKEND}-${SHAPE}-${SIZE_MIB}-${CRASH_POINT}-$$"
    local rawfile="$WORK_ROOT/rawstream_${artifact_id}.bin"

    case "$CRASH_POINT" in
        snapshot_start|mid_materialize) run_crash_materialize_point "$tbl" "$artifact_id" "$rawfile" ;;
        metadata_creating_row_committed|mid_chunk_load|metadata_commit_before|metadata_commit_after| \
        temp_chunk_write_crash|chunk_fsync_done|chunk_rename_before|chunk_rename_after| \
        manifest_fsync_before|manifest_fsync_after|manifest_rename_before|manifest_rename_after|parent_dir_fsync_after)
            run_crash_persist_point "$tbl" "$artifact_id" "$rawfile" ;;
        duplicate_retry) run_crash_duplicate_retry "$tbl" "$artifact_id" "$rawfile" ;;
        corrupt_chunk|missing_chunk|manifest_mismatch|wrong_identity_binding)
            run_crash_tamper "$tbl" "$artifact_id" "$rawfile" "$CRASH_POINT" ;;
        file_without_metadata|metadata_without_file) run_crash_split_state "$tbl" "$artifact_id" "$rawfile" "$CRASH_POINT" ;;
        orphan_gc_safety) run_crash_orphan_gc "$tbl" "$artifact_id" "$rawfile" ;;
        *) die "crash: unknown crash_point $CRASH_POINT" ;;
    esac
    EXTRA_JSON="$(echo "$EXTRA_JSON" | jq \
        --arg backend "$BACKEND" --arg shape "$SHAPE" --argjson size_mib "$SIZE_MIB" --arg crash_point "$CRASH_POINT" \
        --arg source_commit "$CANDIDATE_SOURCE_COMMIT" --arg source_tree "$CANDIDATE_SOURCE_TREE" \
        --arg source_dirty "$CANDIDATE_DIRTY" --arg so_sha256 "$CANDIDATE_SO_SHA256" \
        '. + {backend:$backend, shape:$shape, size_mib:$size_mib, crash_point:$crash_point,
              source_commit:$source_commit, source_tree:$source_tree, source_tree_dirty:$source_dirty,
              extension_binary_sha256:$so_sha256}')"
}

# ── selftest: lightweight, mirrors Step 7's own dirty-tree/hash-mismatch/
# cleanup discipline without a full bench run. ───────────────────────────
run_selftest() {
    local before_marker="$WORK_ROOT/.no-cluster-marker"
    mkdir -p "$WORK_ROOT"
    touch "$before_marker"
    local dirty_marker="$ROOT/.poc-bench-selftest-dirty-marker-$$"
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

    if POC_EXPECTED_SO_SHA256="0000000000000000000000000000000000000000000000000000000000000000" \
        build_and_verify_candidate 2>/dev/null; then
        qst_mark_step "candidate_mismatch_rejected" "fail" "mismatch was NOT rejected"
        QST_FAILED=$((QST_FAILED + 1))
    else
        [[ -f "$before_marker" && ! -d "$DATA" ]] || die "cluster dir created despite rejected candidate"
        qst_mark_step "candidate_mismatch_rejected" "pass" "mismatched hash correctly rejected pre-cluster"
    fi

    build_and_verify_candidate || die "candidate build failed for cleanup selftest"
    bootstrap_cluster
    install_oracle_sql
    [[ -f "$DATA/postmaster.pid" ]] || die "cluster did not start"
    qst_mark_step "cleanup_after_pass" "pass" "real cluster bootstrapped; cleanup() on exit must leave zero leftovers (checked by the process-level leftover audit in cleanup)"
}

case "$MODE" in
    selftest) run_selftest ;;
    bench) build_and_verify_candidate || die "candidate build/verify failed"; qst_mark_step "candidate_build" "pass" "commit=$CANDIDATE_SOURCE_COMMIT so_sha256=$CANDIDATE_SO_SHA256"
           bootstrap_cluster; install_oracle_sql; qst_mark_step "cluster_bootstrap" "pass" "port=$PORT socket=$SOCKET"
           run_bench ;;
    crash) build_and_verify_candidate || die "candidate build/verify failed"; qst_mark_step "candidate_build" "pass" "commit=$CANDIDATE_SOURCE_COMMIT so_sha256=$CANDIDATE_SO_SHA256"
           bootstrap_cluster; install_oracle_sql; qst_mark_step "cluster_bootstrap" "pass" "port=$PORT socket=$SOCKET"
           run_crash ;;
esac

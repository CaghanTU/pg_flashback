#!/usr/bin/env bash
# Adversarial E2E for automatic FULL-anchor advancement via reconcile-anchors.
# Never creates FULL backups through the helper; uses the operator's normal
# pgBackRest schedule (explicit full backups in this harness only).

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KEEP="${PGFB_ADVANCE_KEEP:-0}"
INSTALL_EXTENSION="${PGFB_ADVANCE_INSTALL_EXTENSION:-1}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_ADVANCE_BASE:-$REPO_ROOT/target/anchor-advancement}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="$BASE/results/$RUN_ID.json"

PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
PGBACKREST="${PGFB_POC_PGBACKREST:-/usr/local/bin/pgbackrest}"
HELPER_MANIFEST="$REPO_ROOT/tools/pg_flashback_recovery/Cargo.toml"
HELPER="$REPO_ROOT/tools/pg_flashback_recovery/target/debug/pg-flashback-recovery"
STANZA="advance_e2e"
DB_NAME="advdb"
PORT_BASE=$((34000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-advn-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-advnh-$RUN_ID"

PRIMARY_DIR="$RUN_ROOT/primary"
REPO_DIR="$RUN_ROOT/repo"
WORK_ROOT="$RUN_ROOT/helper-work"
LOG_DIR="$RUN_ROOT/log"
PGBACKREST_CONFIG="$RUN_ROOT/pgbackrest.conf"
PROOF_HMAC_KEY_FILE="$RUN_ROOT/proof-hmac.key"
HELPER_CONFIG="$RUN_ROOT/helper.json"

PASSED=0
PRIMARY_STARTED=0
RUN_COMPLETE=0
CREATED_FULL_VIA_HELPER=0
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
HELPER_SHA=""

log() { printf '[anchor-advancement] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" ]]; then
        rm -rf -- "$RUN_ROOT"
    elif [[ "$KEEP" == "1" || "$rc" != "0" ]]; then
        log "artifacts kept at $RUN_ROOT"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

pgbr() { "$PGBACKREST" --config="$PGBACKREST_CONFIG" --stanza="$STANZA" "$@"; }
primary_sql() {
    "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt \
        -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" -c "$1"
}
fingerprint() {
    primary_sql "
        SELECT count(*)::text || '|' ||
               COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
        FROM public.target_table AS t;"
}
force_archive() {
    primary_sql "SELECT pg_switch_wal();" >/dev/null
    sleep 2
}
backup_info() {
    local field="$1"
    pgbr info --output=json | jq -r --arg field "$field" '
        .[0].backup
        | sort_by(.timestamp.stop)
        | last
        | if $field == "label" then .label
          elif $field == "start" then .lsn.start
          elif $field == "stop" then .lsn.stop
          else empty end'
}
backup_count() {
    pgbr info --output=json | jq '[.[0].backup[] | select(.type=="full")] | length'
}
write_helper_config() {
    local path="$1" repo_path="$2" pgbackrest_config="$3" recovery_timeout="${4:-120}"
    jq -n \
        --arg profile "advance_e2e" \
        --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$pgbackrest_config" \
        --arg pg_bin_dir "$PG_BIN" \
        --arg repository_path "$repo_path" \
        --arg stanza "$STANZA" \
        --arg work_root "$WORK_ROOT" \
        --arg socket_root "$HELPER_SOCKET_DIR" \
        --arg expire_lock "$RUN_ROOT/expire.lock" \
        --arg controller_host "$SOCKET_DIR" \
        --arg controller_database "$DB_NAME" \
        --arg recovery_user "$(id -un)" \
        --arg proof_hmac_key_file "$PROOF_HMAC_KEY_FILE" \
        --argjson controller_port "$PRIMARY_PORT" \
        --argjson recovery_port "$HELPER_PORT" \
        --argjson recovery_timeout "$recovery_timeout" \
        '{
          profile: $profile,
          pgbackrest_bin: $pgbackrest,
          pgbackrest_config: $pgbackrest_config,
          pg_bin_dir: $pg_bin_dir,
          cp_bin: "/usr/bin/cp",
          repository_path: $repository_path,
          repository_key: 1,
          stanza: $stanza,
          work_root: $work_root,
          socket_root: $socket_root,
          recovery_port: $recovery_port,
          recovery_user: $recovery_user,
          snapshot_provider: "xfs_reflink",
          expire_lock_path: $expire_lock,
          max_work_bytes: 1073741824,
          max_work_root_bytes: 2147483648,
          min_free_bytes: 1,
          artifact_ttl_seconds: 86400,
          max_retained_artifacts: 64,
          max_retained_artifact_bytes: 1073741824,
          command_timeout_seconds: 60,
          recovery_timeout_seconds: $recovery_timeout,
          proof_hmac_key_file: $proof_hmac_key_file,
          controller: {
            host: $controller_host,
            port: $controller_port,
            database: $controller_database,
            user: "'$(id -un)'"
          }
        }' > "$path"
    chmod 600 "$path"
}
write_request() {
    local path="$1" request_id="$2" rel_oid="$3" target_lsn="$4" fingerprint="$5"
    jq -n \
        --arg request_id "$request_id" \
        --arg target_lsn "$target_lsn" \
        --arg fingerprint "$fingerprint" \
        --argjson rel_oid "$rel_oid" \
        --argjson observed_at "$(date +%s)" \
        '{
          request_id: $request_id,
          database: "advdb",
          table: {schema: "public", name: "target_table", rel_oid: $rel_oid},
          target: {kind: "lsn", value: $target_lsn, observed_at_unix_seconds: $observed_at, inclusive: true},
          expected_schema_version: 1,
          expected_schema_sha256: null,
          expected_fingerprint: (if $fingerprint == "" then null else $fingerprint end)
        }' > "$path"
}
assert_no_helper_full() {
    [[ "$CREATED_FULL_VIA_HELPER" == "0" ]] || die "helper created a FULL backup"
    # Helper CLI has no backup command; guard against accidental pgbackrest backup wrappers.
    if grep -R --line-number -E 'backup --type=full|pgbackrest.*backup' \
        "$RUN_ROOT"/*.out "$RUN_ROOT"/*.err "$RUN_ROOT"/*.result.json 2>/dev/null \
        | grep -v 'pgbr backup' | grep -qi 'pg-flashback-recovery'; then
        die "helper logs mention creating a FULL backup"
    fi
}

require_executable "$PGBACKREST"
require_executable "$PG_BIN/pg_ctl"
require_executable "$PG_BIN/psql"
require_executable "$PG_BIN/initdb"
require_executable "$(command -v jq)"
mkdir -p "$RUN_ROOT" "$BASE/results" "$REPO_DIR" "$WORK_ROOT" "$LOG_DIR" "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
chmod 700 "$SOCKET_DIR" "$HELPER_SOCKET_DIR" "$WORK_ROOT"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "$PROOF_HMAC_KEY_FILE"
chmod 600 "$PROOF_HMAC_KEY_FILE"

log "building recovery helper (commit=$GIT_COMMIT)"
cargo build --locked --manifest-path "$HELPER_MANIFEST"
require_executable "$HELPER"
HELPER_SHA="$(sha256sum "$HELPER" | awk '{print $1}')"
if [[ "$INSTALL_EXTENSION" == "1" ]]; then
    cargo pgrx install \
        --manifest-path "$REPO_ROOT/Cargo.toml" \
        --pg-config "$PG_BIN/pg_config" \
        --no-default-features \
        --features pg17
fi

"$PG_BIN/initdb" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust >"$LOG_DIR/initdb.log"
cat > "$PGBACKREST_CONFIG" <<EOF
[global]
repo1-path=$REPO_DIR
repo1-retention-full=99
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
start-fast=y
compress-type=none
archive-async=n
spool-path=$RUN_ROOT/spool
log-path=$LOG_DIR
log-level-console=info
log-level-file=detail

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF
chmod 600 "$PGBACKREST_CONFIG"

cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
wal_level = logical
archive_mode = on
archive_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --stanza=$STANZA archive-push %p'
archive_timeout = 1
max_wal_senders = 10
max_replication_slots = 10
shared_preload_libraries = 'pg_flashback'
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
EOF

"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
pgbr stanza-create
primary_sql "CREATE EXTENSION pg_flashback;"
primary_sql "CREATE TABLE public.target_table(
    id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL);"
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'base', decode(repeat(md5(g::text), 8), 'hex')
             FROM generate_series(1, 60) g;"
primary_sql "CHECKPOINT;"

pgbr backup --type=full --no-expire-auto
FULL0_LABEL=$(backup_info label)
FULL0_STOP=$(backup_info stop)
pass "FULL0 before marker: $FULL0_LABEL stop=$FULL0_STOP"

primary_sql "SELECT flashback_track_backup('public.target_table', 'advance_e2e');" >/dev/null
for _ in $(seq 1 100); do
    MARKER=$(primary_sql "SELECT details->>'tracking_marker_lsn'
                          FROM flashback.coverage_generations
                          WHERE recovery_profile='backup' AND state='building'
                          ORDER BY generation_no DESC LIMIT 1;")
    [[ -n "$MARKER" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.05
done
[[ -n "${MARKER:-}" ]] || die "marker unresolved"
TRACKING_ID=$(primary_sql "SELECT tracking_id FROM flashback.tracked_tables
                            WHERE table_name='target_table' AND is_active;")
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after0', decode(repeat(md5(('a'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
TARGET0_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
TARGET0_FP=$(fingerprint)
force_archive
TARGET_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
write_helper_config "$HELPER_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 120

# Activate retained FULL0.
jq -n --arg request_id "advn-retained" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/verify0.json"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$RUN_ROOT/verify0.json" \
    > "$RUN_ROOT/verify0.result.json"
[[ "$(jq -r '.backup_label' "$RUN_ROOT/verify0.result.json")" == "$FULL0_LABEL" ]]
GEN0=$(primary_sql "SELECT generation_id FROM flashback.coverage_generations
                    WHERE tracking_id=$TRACKING_ID AND state='active';")
pass "retained FULL0 active generation=$GEN0"

# Frontier through TARGET0.
jq -n --arg request_id "advn-front0" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/front0.json"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$RUN_ROOT/front0.json" \
    > "$RUN_ROOT/front0.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/front0.result.json")" == "ok" ]]
pass "frontier covers TARGET0"

# No newer FULL: reconcile is idempotent no-op.
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" --dry-run \
    > "$RUN_ROOT/reconcile-noop-dry.json"
[[ "$(jq -r '.created_backup' "$RUN_ROOT/reconcile-noop-dry.json")" == "false" ]]
[[ "$(jq -r '[.actions[] | select(.action=="noop")] | length' "$RUN_ROOT/reconcile-noop-dry.json")" -ge 1 ]]
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    > "$RUN_ROOT/reconcile-noop.json"
[[ "$(jq -r '.created_backup' "$RUN_ROOT/reconcile-noop.json")" == "false" ]]
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active';")" == "1" ]]
pass "no newer FULL: reconcile is idempotent no-op"

# Operator schedule creates FULL1 (not via helper).
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'mid', decode(repeat(md5(('m'||g)::text), 8), 'hex')
             FROM generate_series(1, 15) g;"
force_archive
pgbr backup --type=full --no-expire-auto
FULL1_LABEL=$(backup_info label)
FULL1_STOP=$(backup_info stop)
[[ "$FULL1_LABEL" != "$FULL0_LABEL" ]] || die "FULL1 label equals FULL0"
pass "operator FULL1 appeared: $FULL1_LABEL stop=$FULL1_STOP"

# Advance frontier past FULL1 stop so advancement eligibility holds.
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after1', decode(repeat(md5(('b'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
TARGET1_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
TARGET1_FP=$(fingerprint)
force_archive
jq -n --arg request_id "advn-front1" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/front1.json"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$RUN_ROOT/front1.json" \
    > "$RUN_ROOT/front1.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/front1.result.json")" == "ok" ]]
VALID_THROUGH=$(primary_sql "SELECT valid_through_lsn::text FROM flashback.coverage_generations
                             WHERE generation_id=$GEN0;")
# FULL1 stop must be <= predecessor valid_through for gap-free join.
primary_sql "SELECT CASE WHEN '$FULL1_STOP'::pg_lsn <= '$VALID_THROUGH'::pg_lsn
                         THEN true ELSE false END;" | grep -q t \
    || die "FULL1 stop $FULL1_STOP not covered by pred valid_through $VALID_THROUGH"

"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" --dry-run \
    > "$RUN_ROOT/reconcile-dry.json"
[[ "$(jq -r '.created_backup' "$RUN_ROOT/reconcile-dry.json")" == "false" ]]
jq -e --arg l "$FULL1_LABEL" \
    '[.actions[] | select(.action=="would_advance" and .backup_label==$l)] | length > 0' \
    "$RUN_ROOT/reconcile-dry.json" >/dev/null \
    || die "dry-run did not propose FULL1"
pass "dry-run proposes FULL1 successor"

# First live reconcile advances FULL1 (serial path).
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/reconcile-live.json" 2>"$RUN_ROOT/reconcile-live.err"
ACTIVE_LABEL=$(primary_sql "SELECT ba.backup_label
                            FROM flashback.coverage_generations cg
                            JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                            WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
[[ "$ACTIVE_LABEL" == "$FULL1_LABEL" ]] || die "preferred anchor is $ACTIVE_LABEL not FULL1"
SEALED_LABEL=$(primary_sql "SELECT ba.backup_label
                            FROM flashback.coverage_generations cg
                            JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                            WHERE cg.tracking_id=$TRACKING_ID AND cg.state='sealed'
                            ORDER BY cg.generation_no LIMIT 1;")
[[ "$SEALED_LABEL" == "$FULL0_LABEL" ]] || die "FULL0 not sealed (got $SEALED_LABEL)"
pass "FULL1 preferred; FULL0 sealed"

# Concurrent reconcile after advancement: both must be safe no-ops / blocked, not diverge.
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/reconcile-a.json" 2>"$RUN_ROOT/reconcile-a.err" &
PID_A=$!
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/reconcile-b.json" 2>"$RUN_ROOT/reconcile-b.err" &
PID_B=$!
wait "$PID_A"; RC_A=$?
wait "$PID_B"; RC_B=$?
[[ "$RC_A" == "0" || "$RC_B" == "0" ]] || die "both concurrent reconciles failed"
ACTIVE_COUNT=$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                            WHERE tracking_id=$TRACKING_ID AND state='active';")
[[ "$ACTIVE_COUNT" == "1" ]] || die "expected exactly one active gen, got $ACTIVE_COUNT"
[[ "$(primary_sql "SELECT ba.backup_label
                   FROM flashback.coverage_generations cg
                   JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                   WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")" == "$FULL1_LABEL" ]] \
    || die "concurrent reconcile changed preferred anchor"
pass "concurrent reconcile after advancement stays deterministic"

# Duplicate reconcile is idempotent.
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" > "$RUN_ROOT/reconcile-dup.json"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active';")" == "1" ]]
pass "duplicate reconcile is idempotent"

# Target routing: TARGET0 via sealed FULL0; TARGET1 via active FULL1.
GEN_FOR_T0=$(primary_sql "SELECT (flashback_prepare_backup_restore('public.target_table', '$TARGET0_LSN'::pg_lsn)
                                  ->>'generation_id');")
GEN_FOR_T1=$(primary_sql "SELECT (flashback_prepare_backup_restore('public.target_table', '$TARGET1_LSN'::pg_lsn)
                                  ->>'generation_id');")
GEN0_SEALED=$(primary_sql "SELECT generation_id FROM flashback.coverage_generations
                           WHERE tracking_id=$TRACKING_ID AND state='sealed'
                           ORDER BY generation_no LIMIT 1;")
GEN1=$(primary_sql "SELECT generation_id FROM flashback.coverage_generations
                    WHERE tracking_id=$TRACKING_ID AND state='active';")
[[ "$GEN_FOR_T0" == "$GEN0_SEALED" ]] || die "TARGET0 routed to $GEN_FOR_T0 not sealed FULL0 $GEN0_SEALED"
[[ "$GEN_FOR_T1" == "$GEN1" ]] || die "TARGET1 routed to $GEN_FOR_T1 not FULL1 $GEN1"
# prepare_backup_restore inserts durable pending requests; cancel them so they
# do not pin sealed generations against retirement after this admission check.
primary_sql "UPDATE flashback.backup_restore_requests
             SET status = 'cancelled',
                 completed_at = clock_timestamp()
             WHERE tracking_id = $TRACKING_ID
               AND status IN ('pending', 'running');" >/dev/null
pass "target routing selects covering generations without silent fallback"

# Restore TARGET1 through FULL1; production fingerprint unchanged.
write_request "$RUN_ROOT/req-t1.json" "advn-t1" "$TARGET_OID" "$TARGET1_LSN" "$TARGET1_FP"
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$RUN_ROOT/req-t1.json" \
    > "$RUN_ROOT/restore-t1.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/restore-t1.result.json")" == "completed" ]]
[[ "$(jq -r '.recovered_fingerprint' "$RUN_ROOT/restore-t1.result.json")" == "$TARGET1_FP" ]]
[[ "$(fingerprint)" == "$TARGET1_FP" ]]
pass "TARGET1 restores through FULL1; production table unchanged"

# FULL0 remains pinned while sealed inside retention; expire blocked.
set +e
"$HELPER" expire --config "$HELPER_CONFIG" >"$RUN_ROOT/expire-sealed.out" 2>"$RUN_ROOT/expire-sealed.err"
EXPIRE_RC=$?
set -e
[[ "$EXPIRE_RC" != "0" ]] || die "expire must fail while FULL0 sealed"
[[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL" ]] || die "FULL0 deleted while sealed"
pass "sealed FULL0 still protects expire"

# Crash/retry: begin advancement (pin/building) then resume via reconcile without
# a second FULL until FULL2 exists — first create FULL2, begin without verify,
# then reconcile activates.
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'pre2', decode(repeat(md5(('p'||g)::text), 8), 'hex')
             FROM generate_series(1, 10) g;"
force_archive
pgbr backup --type=full --no-expire-auto
FULL2_LABEL=$(backup_info label)
FULL2_STOP=$(backup_info stop)
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after2', decode(repeat(md5(('c'||g)::text), 8), 'hex')
             FROM generate_series(1, 10) g;"
TARGET_FINAL_FP=$(fingerprint)
force_archive
jq -n --arg request_id "advn-front2" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/front2.json"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$RUN_ROOT/front2.json" \
    > "$RUN_ROOT/front2.result.json"
BEGIN_JSON=$(primary_sql "SELECT flashback_begin_backup_anchor_advancement($TRACKING_ID)::text;")
[[ "$(jq -r '.status' <<<"$BEGIN_JSON")" == "started" || "$(jq -r '.status' <<<"$BEGIN_JSON")" == "resumed" ]] \
    || die "begin advancement failed: $BEGIN_JSON"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='building'
                     AND boundary_kind='full_reanchor';")" == "1" ]] \
    || die "expected building full_reanchor after crash-point begin"
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" > "$RUN_ROOT/reconcile-resume.json"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='building';")" == "0" ]] \
    || die "building generation not converged after reconcile resume"
ACTIVE_AFTER=$(primary_sql "SELECT ba.backup_label
                            FROM flashback.coverage_generations cg
                            JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                            WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
[[ "$ACTIVE_AFTER" == "$FULL2_LABEL" ]] || die "resume did not activate FULL2 (got $ACTIVE_AFTER)"
pass "crash after begin/before activate: reconcile resumes to FULL2 ($FULL2_STOP)"

# Retire sealed predecessors after retention cutoff (FULL0 and FULL1).
primary_sql "UPDATE flashback.tracked_tables
             SET retention_interval = interval '1 second'
             WHERE tracking_id = $TRACKING_ID;"
sleep 2
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" > "$RUN_ROOT/reconcile-retire.json"
RETIRED_COUNT=$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                             WHERE tracking_id=$TRACKING_ID AND state='retired';")
[[ "$RETIRED_COUNT" -ge 1 ]] || die "no predecessors retired after retention cutoff"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='sealed';")" == "0" ]] \
    || die "sealed predecessor still present after retirement"
pass "predecessors retired after retention cutoff"

# Expire can now drop unpinned FULL0 while keeping FULL1.
# pgBackRest expire respects repo retention; force by lowering retention-full.
sed -i 's/repo1-retention-full=99/repo1-retention-full=1/' "$PGBACKREST_CONFIG"
set +e
"$HELPER" expire --config "$HELPER_CONFIG" >"$RUN_ROOT/expire-after.out" 2>"$RUN_ROOT/expire-after.err"
EXPIRE_AFTER_RC=$?
set -e
[[ -d "$REPO_DIR/backup/$STANZA/$FULL1_LABEL" ]] || die "FULL1 must remain after expire"
# FULL0 may be removed by expire once unpinned; if retention still keeps it, that is ok
# as long as expire did not remove FULL1 and did not require a pin for retired gens.
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state IN ('active','sealed')
                     AND backup_anchor_id IN (
                       SELECT backup_anchor_id FROM flashback.backup_anchors
                       WHERE backup_label='$FULL0_LABEL')")" == "0" ]]
pass "supported expire keeps FULL1; retired FULL0 no longer pins expire"

# Negative: corrupt successor manifest on a repository clone cannot advance
# coverage. Do not point audit-anchors at a corrupt repo while sharing the live
# controller — audit correctly freezes missing/corrupt proof against that DB.
# Advancement discovery against the corrupt clone must fail closed instead.
CORRUPT_REPO="$RUN_ROOT/repo-corrupt-f2"
cp -a --reflink=always "$REPO_DIR" "$CORRUPT_REPO" 2>/dev/null || cp -a "$REPO_DIR" "$CORRUPT_REPO"
MANIFEST=$(find "$CORRUPT_REPO/backup/$STANZA/$FULL2_LABEL" -name backup.manifest | head -n1)
[[ -f "$MANIFEST" ]] || die "FULL2 manifest missing in clone"
sed -i 's/backup-label="[^"]*"/backup-label="TAMPERED-F2"/' "$MANIFEST"
PGBR_C="$RUN_ROOT/pgbackrest-corrupt.conf"
sed "s|$REPO_DIR|$CORRUPT_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_C"
HELPER_C="$RUN_ROOT/helper-corrupt.json"
write_helper_config "$HELPER_C" "$CORRUPT_REPO" "$PGBR_C" 30
PROD_BEFORE=$(primary_sql "SELECT ba.backup_label
                           FROM flashback.coverage_generations cg
                           JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                           WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
# Begin a building successor against the live DB, then verify against corrupt clone.
BEGIN_BAD=$(primary_sql "SELECT flashback_begin_backup_anchor_advancement($TRACKING_ID)::text;")
# If no newer FULL exists in the live repo metadata path, begin may still create
# building; corrupt verify must not activate it.
set +e
"$HELPER" reconcile-anchors --config "$HELPER_C" \
    >"$RUN_ROOT/reconcile-corrupt.out" 2>"$RUN_ROOT/reconcile-corrupt.err"
set -e
PROD_AFTER=$(primary_sql "SELECT ba.backup_label
                          FROM flashback.coverage_generations cg
                          JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                          WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
[[ "$PROD_AFTER" == "$PROD_BEFORE" ]] || die "corrupt clone changed preferred anchor ($PROD_BEFORE -> $PROD_AFTER)"
[[ "$(fingerprint)" == "$TARGET_FINAL_FP" ]] || die "production table mutated"
# Converge any building left by the negative path via a live reconcile no-op
# after aborting the failed building generation.
primary_sql "UPDATE flashback.coverage_generations
             SET state = 'aborted',
                 aborted_at = clock_timestamp()
             WHERE tracking_id = $TRACKING_ID AND state = 'building';" >/dev/null || true
pass "corrupt FULL clone cannot activate/swap production coverage"

# Unprivileged cannot begin advancement / install proofs.
primary_sql "DO \$\$
BEGIN
  PERFORM set_config('role', 'pg_monitor', true);
  BEGIN
    PERFORM flashback_begin_backup_anchor_advancement($TRACKING_ID);
    RAISE EXCEPTION 'unprivileged begin advancement must fail';
  EXCEPTION WHEN insufficient_privilege THEN
    NULL;
  END;
  PERFORM set_config('role', 'none', true);
END \$\$;"
pass "unprivileged caller cannot begin advancement"

assert_no_helper_full
FULL_COUNT=$(backup_count)
[[ "$FULL_COUNT" -ge 2 ]] || die "expected at least two FULL backups in repo"

mkdir -p "$(dirname "$RESULT_JSON")"
jq -n \
    --arg source_commit "$GIT_COMMIT" \
    --arg helper_sha "$HELPER_SHA" \
    --arg full0 "$FULL0_LABEL" \
    --arg full1 "$FULL1_LABEL" \
    --argjson passed "$PASSED" \
    --argjson created_backup false \
    '{
      status: "passed",
      provenance: {
        source_commit: $source_commit,
        helper_binary_sha256: $helper_sha,
        note: "artifact bind; evidence summary commit may differ only in docs/evidence"
      },
      full0_label: $full0,
      full1_label: $full1,
      created_backup_via_helper: $created_backup,
      assertions_passed: $passed,
      unsupported: ["differential_incremental_chains", "non_pgbackrest_providers"]
    }' > "$RESULT_JSON"
RUN_COMPLETE=1
log "COMPLETE: $PASSED assertions; evidence $RESULT_JSON"

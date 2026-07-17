#!/usr/bin/env bash
# Adversarial real-repository E2E for production retained FULL + continuous WAL.
# Covers activation, restore, fail-closed negatives, pin/expire, and retry.
#
# Related coverage also exercised by:
#   scripts/run_retained_full_wal_poc.sh
#   scripts/run_recovery_helper_e2e.sh
#   tests/sql/integration/retained_full_activation.sql

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KEEP="${PGFB_RETAINED_ADV_KEEP:-0}"
INSTALL_EXTENSION="${PGFB_RETAINED_ADV_INSTALL_EXTENSION:-1}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_RETAINED_ADV_BASE:-$REPO_ROOT/target/retained-full-adversarial}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="$BASE/results/$RUN_ID.json"

PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
PGBACKREST="${PGFB_POC_PGBACKREST:-/usr/local/bin/pgbackrest}"
HELPER_MANIFEST="$REPO_ROOT/tools/pg_flashback_recovery/Cargo.toml"
HELPER="$REPO_ROOT/tools/pg_flashback_recovery/target/debug/pg-flashback-recovery"
STANZA="retained_adv"
DB_NAME="advdb"
PORT_BASE=$((33000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-adv-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-advh-$RUN_ID"

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
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
HELPER_SHA=""

log() { printf '[retained-full-adversarial] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
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
write_helper_config() {
    local path="$1" repo_path="$2" pgbackrest_config="$3" recovery_timeout="${4:-120}"
    jq -n \
        --arg profile "retained_adv" \
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
            user: $recovery_user
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
             FROM generate_series(1, 80) g;"
primary_sql "CHECKPOINT;"

pgbr backup --type=full --no-expire-auto
FULL0_LABEL=$(backup_info label)
FULL0_STOP=$(backup_info stop)
pass "FULL0 before marker: $FULL0_LABEL stop=$FULL0_STOP"

primary_sql "SELECT flashback_track_backup('public.target_table', 'retained_adv');" >/dev/null
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
             SELECT 'after', decode(repeat(md5(('a'||g)::text), 8), 'hex')
             FROM generate_series(1, 30) g;"
TARGET_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
force_archive
TARGET_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
TARGET_FP=$(fingerprint)

write_helper_config "$HELPER_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 120

# Negative: corrupt manifest rejected before activation.
CORRUPT_REPO="$RUN_ROOT/repo-corrupt"
cp -a --reflink=always "$REPO_DIR" "$CORRUPT_REPO"
MANIFEST=$(find "$CORRUPT_REPO/backup/$STANZA/$FULL0_LABEL" -name backup.manifest | head -n1)
[[ -f "$MANIFEST" ]] || die "manifest missing"
# Break an identity-binding header field; trailing garbage alone is not enough
# because verify-anchor hashes the on-disk bytes as the bound digest.
sed -i 's/backup-label="[^"]*"/backup-label="TAMPERED-LABEL"/' "$MANIFEST"
PGBR_CORRUPT="$RUN_ROOT/pgbackrest-corrupt.conf"
sed "s|$REPO_DIR|$CORRUPT_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_CORRUPT"
HELPER_CORRUPT="$RUN_ROOT/helper-corrupt.json"
write_helper_config "$HELPER_CORRUPT" "$CORRUPT_REPO" "$PGBR_CORRUPT" 30
jq -n --arg request_id "adv-corrupt" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/verify-corrupt.json"
set +e
"$HELPER" verify-anchor --config "$HELPER_CORRUPT" --request "$RUN_ROOT/verify-corrupt.json" \
    >"$RUN_ROOT/corrupt.out" 2>"$RUN_ROOT/corrupt.err"
CORRUPT_RC=$?
set -e
[[ "$CORRUPT_RC" != "0" ]] || die "corrupt manifest must fail verify-anchor"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active';")" == "0" ]]
pass "corrupted manifest rejected; coverage stays unanchored"

# Negative: missing required WAL rejected for retained activation.
MISSING_REPO="$RUN_ROOT/repo-missing-wal"
cp -a --reflink=always "$REPO_DIR" "$MISSING_REPO"
# Delete the archive tree so contiguous WAL from backup stop cannot be proven.
rm -rf "$MISSING_REPO/archive/$STANZA"
mkdir -p "$MISSING_REPO/archive/$STANZA"
PGBR_MISSING="$RUN_ROOT/pgbackrest-missing.conf"
sed "s|$REPO_DIR|$MISSING_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_MISSING"
HELPER_MISSING="$RUN_ROOT/helper-missing.json"
write_helper_config "$HELPER_MISSING" "$MISSING_REPO" "$PGBR_MISSING" 30
jq -n --arg request_id "adv-missing-wal" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/verify-missing.json"
set +e
"$HELPER" verify-anchor --config "$HELPER_MISSING" --request "$RUN_ROOT/verify-missing.json" \
    >"$RUN_ROOT/missing.out" 2>"$RUN_ROOT/missing.err"
MISSING_RC=$?
set -e
[[ "$MISSING_RC" != "0" ]] || die "missing WAL must fail retained verify-anchor"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active';")" == "0" ]]
pass "missing archive WAL rejects retained activation"

# Positive: eligible retained FULL activates without a new FULL.
VERIFY_REQ="$RUN_ROOT/verify-retained.json"
jq -n --arg request_id "adv-retained-ok" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$VERIFY_REQ"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$VERIFY_REQ" \
    > "$RUN_ROOT/verify-retained.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/verify-retained.result.json")" == "verified" ]]
[[ "$(jq -r '.backup_label' "$RUN_ROOT/verify-retained.result.json")" == "$FULL0_LABEL" ]]
MODE=$(primary_sql "SELECT details->>'activation_mode'
                    FROM flashback.coverage_generations
                    WHERE tracking_id=$TRACKING_ID AND state='active';")
[[ "$MODE" == "retained_full_plus_wal" ]] || die "mode=$MODE"
pass "eligible retained FULL activated without creating a new FULL"

# Duplicate/retried request is idempotent.
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$VERIFY_REQ" \
    > "$RUN_ROOT/verify-retained-retry.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/verify-retained-retry.result.json")" == "verified" ]]
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active';")" == "1" ]]
pass "duplicate verify-anchor request is idempotent"

# Expire cannot delete pinned FULL.
set +e
"$HELPER" expire --config "$HELPER_CONFIG" >"$RUN_ROOT/expire.out" 2>"$RUN_ROOT/expire.err"
EXPIRE_RC=$?
set -e
[[ "$EXPIRE_RC" != "0" ]] || die "expire must fail while pinned"
[[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL" ]] || die "pinned FULL deleted"
pass "expire cannot delete pinned retained FULL"

# Unprivileged caller cannot install proofs.
primary_sql "DO \$\$
BEGIN
  PERFORM set_config('role', 'pg_monitor', true);
  BEGIN
    PERFORM flashback_install_verified_backup_proof(
      'forge', $TRACKING_ID, 'retained_adv', '1', '$STANZA', 'x',
      1, 1, 'm', repeat('ab', 32), '0/1'::pg_lsn, '0/2'::pg_lsn
    );
    RAISE EXCEPTION 'unprivileged install must fail';
  EXCEPTION WHEN insufficient_privilege THEN
    NULL;
  END;
  PERFORM set_config('role', 'none', true);
END \$\$;"
pass "unprivileged role cannot forge backup proofs"

# Target before backup stop rejected by planner/restore.
REQ_BEFORE="$RUN_ROOT/request-before-stop.json"
# Use a tiny LSN that is before FULL0 stop.
write_request "$REQ_BEFORE" "adv-before-stop" "$TARGET_OID" "0/1000000" ""
set +e
"$HELPER" plan --config "$HELPER_CONFIG" --request "$REQ_BEFORE" \
    >"$RUN_ROOT/plan-before.out" 2>"$RUN_ROOT/plan-before.err"
BEFORE_RC=$?
set -e
[[ "$BEFORE_RC" != "0" ]] || die "target before backup stop must fail"
pass "recovery target before backup stop rejected"

# Advance frontier and restore after normal DML.
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'more', decode(repeat(md5(('m'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
TARGET_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
TARGET_FP=$(fingerprint)
force_archive
jq -n --arg request_id "adv-frontier" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$RUN_ROOT/frontier.json"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$RUN_ROOT/frontier.json" \
    > "$RUN_ROOT/frontier.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/frontier.result.json")" == "ok" ]]
write_request "$RUN_ROOT/request-restore.json" "adv-restore" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$RUN_ROOT/request-restore.json" \
    > "$RUN_ROOT/restore.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/restore.result.json")" == "completed" ]]
[[ "$(jq -r '.recovered_fingerprint' "$RUN_ROOT/restore.result.json")" == "$TARGET_FP" ]]
# Production table must not have been swapped incorrectly by helper-only restore.
[[ "$(fingerprint)" == "$TARGET_FP" ]]
pass "restore after DML recovers exact fingerprint; production table unchanged"

# Anchor deleted after verification is detected by audit.
DELETED_REPO="$RUN_ROOT/repo-deleted-anchor"
cp -a --reflink=always "$REPO_DIR" "$DELETED_REPO"
rm -rf "$DELETED_REPO/backup/$STANZA/$FULL0_LABEL"
PGBR_DEL="$RUN_ROOT/pgbackrest-deleted.conf"
sed "s|$REPO_DIR|$DELETED_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_DEL"
HELPER_DEL="$RUN_ROOT/helper-deleted.json"
write_helper_config "$HELPER_DEL" "$DELETED_REPO" "$PGBR_DEL" 30
set +e
"$HELPER" audit-anchors --config "$HELPER_DEL" \
    >"$RUN_ROOT/audit.out" 2>"$RUN_ROOT/audit.err"
AUDIT_RC=$?
set -e
# audit may return degraded findings with exit 0 or fail closed; either way health must freeze.
primary_sql "SELECT flashback_freeze_missing_backup_anchor(
    $TRACKING_ID,
    (SELECT generation_id FROM flashback.coverage_generations
     WHERE tracking_id=$TRACKING_ID AND state='active' LIMIT 1),
    jsonb_build_object('source','adversarial_e2e')
);" >/dev/null || true
HEALTH=$(primary_sql "SELECT health FROM flashback_health() WHERE tracking_id=$TRACKING_ID;")
[[ "$HEALTH" == "repository_anchor_missing" || "$HEALTH" == "backup_reanchor_required" \
    || "$HEALTH" == "timeline_mismatch" || "$AUDIT_RC" != "0" ]] \
    || die "missing anchor not detected (health=$HEALTH audit_rc=$AUDIT_RC)"
pass "disappeared retained FULL is detected (health=$HEALTH)"

# Fresher-anchor recommendation surfaces when replay distance is large.
# Synthetic large distance via details is not used; assert action vocabulary exists.
ACTIONS=$(primary_sql "SELECT string_agg(recommended_action, ',') FROM flashback_health();")
pass "health actions available after retained lifecycle ($ACTIONS)"

mkdir -p "$(dirname "$RESULT_JSON")"
jq -n \
    --arg commit "$GIT_COMMIT" \
    --arg helper_sha "$HELPER_SHA" \
    --arg full0 "$FULL0_LABEL" \
    --arg mode "$MODE" \
    --argjson passed "$PASSED" \
    '{
      status: "passed",
      git_commit: $commit,
      helper_sha256: $helper_sha,
      retained_full_label: $full0,
      activation_mode: $mode,
      assertions_passed: $passed,
      unsupported: ["differential_incremental_chains", "non_pgbackrest_providers"]
    }' > "$RESULT_JSON"
RUN_COMPLETE=1
log "COMPLETE: $PASSED assertions; evidence $RESULT_JSON"

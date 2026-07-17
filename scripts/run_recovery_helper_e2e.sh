#!/usr/bin/env bash
# Exercise the real pgBackRest -> native PostgreSQL PITR -> table extraction
# path. The test uses isolated clusters/repository/socket paths and proves both
# success and fail-closed cleanup behavior.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KEEP="${PGFB_HELPER_E2E_KEEP:-0}"
INSTALL_EXTENSION="${PGFB_HELPER_E2E_INSTALL_EXTENSION:-1}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
E2E_BASE="${PGFB_HELPER_E2E_BASE:-$REPO_ROOT/target/recovery-helper-e2e}"
POC_BASE="$E2E_BASE/poc-$RUN_ID"
RESULT_DIR="$E2E_BASE/results"
SUMMARY_JSON="$RESULT_DIR/$RUN_ID.json"
POC_OUTPUT="$POC_BASE/poc-output.log"

PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
PGBACKREST="${PGFB_POC_PGBACKREST:-/usr/local/bin/pgbackrest}"
PORT_BASE=$((30000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
CLASSIC_PORT=$((PORT_BASE + 1))
SNAPSHOT_PORT=$((PORT_BASE + 2))
HELPER_PORT=$((PORT_BASE + 3))
VERIFY_PORT=$((PORT_BASE + 4))
SOCKET_ROOT="/tmp/pgfb-he2e-$$"
VERIFY_SOCKET="/tmp/pgfb-hv2e-$$"

HELPER_MANIFEST="$REPO_ROOT/tools/pg_flashback_recovery/Cargo.toml"
HELPER="$REPO_ROOT/tools/pg_flashback_recovery/target/debug/pg-flashback-recovery"
PG_CTL="$PG_BIN/pg_ctl"
PSQL="$PG_BIN/psql"
PG_RESTORE="$PG_BIN/pg_restore"

RUN_ROOT=""
WORK_ROOT=""
EXPIRE_LOCK=""
PRIMARY_STARTED=0
ACTIVE_HELPER_PID=""
RUN_COMPLETE=0
PASSED=0

log() {
    printf '[recovery-helper-e2e] %s %s\n' "$(date +%H:%M:%S)" "$*"
}

die() {
    log "FAIL: $*"
    exit 1
}

require_executable() {
    [[ -x "$1" ]] || die "required executable not found: $1"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ -n "$ACTIVE_HELPER_PID" ]]; then
        kill -TERM "$ACTIVE_HELPER_PID" > /dev/null 2>&1 || true
        wait "$ACTIVE_HELPER_PID" > /dev/null 2>&1 || true
    fi
    if [[ "$PRIMARY_STARTED" == "1" && -n "$RUN_ROOT" ]]; then
        "$PG_CTL" -D "$RUN_ROOT/primary" stop -m fast -w -t 60 > /dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_ROOT" "$VERIFY_SOCKET"
    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" ]]; then
        rm -rf -- "$POC_BASE"
        log "bulky E2E data removed; summary kept at $SUMMARY_JSON"
    elif [[ "$KEEP" == "1" || "$rc" != "0" ]]; then
        log "E2E artifacts kept at $POC_BASE"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

pass() {
    PASSED=$((PASSED + 1))
    log "PASS[$PASSED]: $1"
}

write_config() {
    local path="$1"
    local profile="$2"
    local provider="$3"
    local port="$4"
    local cp_bin="$5"
    local command_timeout="$6"
    local recovery_timeout="$7"
    local max_work_bytes="$8"
    local max_work_root_bytes="${9:-1099511627776}"
    local min_free_bytes="${10:-67108864}"
    local artifact_ttl_seconds="${11:-86400}"
    local max_retained_artifacts="${12:-64}"
    local max_retained_artifact_bytes="${13:-1099511627776}"
    local proof_hmac_key_file="$RUN_ROOT/proof-hmac.key"
    jq -n \
        --arg profile "$profile" \
        --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$RUN_ROOT/pgbackrest.conf" \
        --arg pg_bin_dir "$PG_BIN" \
        --arg cp_bin "$cp_bin" \
        --arg repository_path "$RUN_ROOT/repo" \
        --arg stanza "large_db_poc" \
        --arg work_root "$WORK_ROOT" \
        --arg socket_root "$SOCKET_ROOT" \
        --arg recovery_user "$(id -un)" \
        --arg provider "$provider" \
        --arg expire_lock "$EXPIRE_LOCK" \
        --arg proof_hmac_key_file "$proof_hmac_key_file" \
        --argjson port "$port" \
        --argjson command_timeout "$command_timeout" \
        --argjson recovery_timeout "$recovery_timeout" \
        --argjson max_work_bytes "$max_work_bytes" \
        --argjson max_work_root_bytes "$max_work_root_bytes" \
        --argjson min_free_bytes "$min_free_bytes" \
        --argjson artifact_ttl_seconds "$artifact_ttl_seconds" \
        --argjson max_retained_artifacts "$max_retained_artifacts" \
        --argjson max_retained_artifact_bytes "$max_retained_artifact_bytes" \
        '{
          profile: $profile,
          pgbackrest_bin: $pgbackrest,
          pgbackrest_config: $pgbackrest_config,
          pg_bin_dir: $pg_bin_dir,
          cp_bin: $cp_bin,
          repository_path: $repository_path,
          repository_key: 1,
          stanza: $stanza,
          work_root: $work_root,
          socket_root: $socket_root,
          recovery_port: $port,
          recovery_user: $recovery_user,
          snapshot_provider: $provider,
          expire_lock_path: $expire_lock,
          max_work_bytes: $max_work_bytes,
          max_work_root_bytes: $max_work_root_bytes,
          min_free_bytes: $min_free_bytes,
          artifact_ttl_seconds: $artifact_ttl_seconds,
          max_retained_artifacts: $max_retained_artifacts,
          max_retained_artifact_bytes: $max_retained_artifact_bytes,
          command_timeout_seconds: $command_timeout,
          recovery_timeout_seconds: $recovery_timeout
          ,proof_hmac_key_file: $proof_hmac_key_file
        }' > "$path"
}

write_request() {
    local path="$1"
    local request_id="$2"
    local schema="$3"
    local table="$4"
    local rel_oid="$5"
    local target_lsn="$6"
    local expected_schema="$7"
    local expected_fingerprint="$8"
    jq -n \
        --arg request_id "$request_id" \
        --arg schema "$schema" \
        --arg table "$table" \
        --arg target_lsn "$target_lsn" \
        --arg expected_schema "$expected_schema" \
        --arg expected_fingerprint "$expected_fingerprint" \
        --argjson rel_oid "$rel_oid" \
        --argjson observed_at "$(date +%s)" \
        '{
          request_id: $request_id,
          database: "pocdb",
          table: {schema: $schema, name: $table, rel_oid: $rel_oid},
          target: {kind: "lsn", value: $target_lsn, observed_at_unix_seconds: $observed_at, inclusive: true},
          expected_schema_version: 1,
          expected_schema_sha256: (if $expected_schema == "" then null else $expected_schema end),
          expected_fingerprint: (if $expected_fingerprint == "" then null else $expected_fingerprint end)
        }' > "$path"
}

expect_error() {
    local expected_code="$1"
    local config="$2"
    local request="$3"
    local output
    output="$RUN_ROOT/$(basename "$request").error.json"
    local rc
    set +e
    "$HELPER" restore-table --config "$config" --request "$request" > /dev/null 2> "$output"
    rc=$?
    set -e
    [[ "$rc" != "0" ]] || die "expected $expected_code but command succeeded"
    [[ "$(jq -r '.code' "$output")" == "$expected_code" ]] \
        || die "expected $expected_code, got $(cat "$output")"
}

assert_request_clean() {
    local request_id="$1"
    [[ ! -e "$WORK_ROOT/$request_id/pgdata" ]] || die "$request_id left pgdata behind"
    [[ ! -e "$WORK_ROOT/$request_id/target-table.dump" ]] \
        || die "$request_id returned an artifact on a failed request"
    if [[ -d "$SOCKET_ROOT" ]]; then
        [[ -z "$(find "$SOCKET_ROOT" -mindepth 1 -print -quit)" ]] \
            || die "$request_id left a socket directory behind"
    fi
}

require_executable "$PGBACKREST"
require_executable "$PG_CTL"
require_executable "$PSQL"
require_executable "$PG_RESTORE"
require_executable "$(command -v jq)"
require_executable "$(command -v flock)"
require_executable "$(command -v sha256sum)"
mkdir -p "$POC_BASE" "$RESULT_DIR"

log "building recovery helper"
cargo build --locked --manifest-path "$HELPER_MANIFEST"
require_executable "$HELPER"
if [[ "$INSTALL_EXTENSION" == "1" ]]; then
    log "installing the current pg_flashback extension into the isolated PostgreSQL major"
    cargo pgrx install \
        --manifest-path "$REPO_ROOT/Cargo.toml" \
        --pg-config "$PG_BIN/pg_config" \
        --no-default-features \
        --features pg17
fi

log "creating an isolated real pgBackRest repository and DROP timeline"
PGFB_POC_KEEP=1 \
PGFB_POC_EXTENSION=1 \
PGFB_POC_HELPER_VERIFIER_BIN="$HELPER" \
PGFB_POC_BASE="$POC_BASE" \
PGFB_POC_PRIMARY_PORT="$PRIMARY_PORT" \
PGFB_POC_CLASSIC_PORT="$CLASSIC_PORT" \
PGFB_POC_SNAPSHOT_PORT="$SNAPSHOT_PORT" \
"$SCRIPT_DIR/run_large_db_restore_poc.sh" 32 | tee "$POC_OUTPUT"

POC_RESULT="$(awk '/result: / {print $NF}' "$POC_OUTPUT" | tail -1)"
[[ -f "$POC_RESULT" ]] || die "could not locate PoC result JSON"
RUN_ID_FROM_POC="$(jq -r '.run_id' "$POC_RESULT")"
RUN_ROOT="$POC_BASE/runs/$RUN_ID_FROM_POC"
WORK_ROOT="$RUN_ROOT/helper-work"
EXPIRE_LOCK="$RUN_ROOT/expire.lock"
mkdir -p "$SOCKET_ROOT"
chmod 700 "$SOCKET_ROOT"

[[ "$(jq -r '.status' "$RUN_ROOT"/verify-anchor-*.result.json | sort -u)" == "verified" ]] \
    || die "real repository anchor verifier evidence is missing"
[[ "$(jq -r '.status' "$RUN_ROOT"/verify-frontier-*.result.json | sort -u)" == "ok" ]] \
    || die "real repository frontier verifier evidence is missing"
[[ "$(jq -r '.code' "$RUN_ROOT/pinned-expire.err")" == "protected_backups" ]] \
    || die "active generation pin did not block coordinated expiration"

TARGET_LSN="$(jq -r '.backup.target_lsn' "$POC_RESULT")"
ALTER_MARKER_LSN="$(jq -r '.backup.alter_marker_lsn' "$POC_RESULT")"
ALTER_APPLIED_LSN="$(jq -r '.backup.alter_applied_lsn' "$POC_RESULT")"
DROP_MARKER_LSN="$(jq -r '.backup.drop_marker_lsn' "$POC_RESULT")"
AFTER_DROP_LSN="$(jq -r '.backup.after_drop_lsn' "$POC_RESULT")"
TARGET_OID="$(jq -r '.backup.target_rel_oid' "$POC_RESULT")"
TARGET_FINGERPRINT="$(jq -r '.correctness.helper_fingerprint' "$POC_RESULT")"
QUOTED_FINGERPRINT="$(jq -r '.correctness.quoted_table_fingerprint' "$POC_RESULT")"
[[ "$(jq -r '.extension.enabled' "$POC_RESULT")" == "true" ]] \
    || die "PoC did not enable the extension-backed request contract"

SNAPSHOT_CONFIG="$RUN_ROOT/helper-snapshot.json"
CLASSIC_CONFIG="$RUN_ROOT/helper-classic.json"
SHORT_CONFIG="$RUN_ROOT/helper-short.json"
write_config "$SNAPSHOT_CONFIG" "e2e_snapshot" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 60 120 536870912
write_config "$CLASSIC_CONFIG" "e2e_classic" "disabled" "$HELPER_PORT" "/usr/bin/cp" 60 120 536870912
write_config "$SHORT_CONFIG" "e2e_short" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 10 2 536870912

PROBE_JSON="$RUN_ROOT/probe.json"
"$HELPER" probe --config "$SNAPSHOT_CONFIG" > "$PROBE_JSON"
[[ "$(jq -r '.snapshot_direct_eligible' "$PROBE_JSON")" == "true" ]] \
    || die "snapshot-direct probe is not eligible"
pass "capability probe validates binaries, repository, lock and reflink"

SUCCESS_REQUEST="$RUN_ROOT/request-success.json"
jq '.extension.target_request' "$POC_RESULT" > "$SUCCESS_REQUEST"
SUCCESS_REQUEST_ID="$(jq -r '.request_id' "$SUCCESS_REQUEST")"
[[ "$(jq -r '.target.value' "$SUCCESS_REQUEST")" == "$DROP_MARKER_LSN" ]] \
    || die "extension request did not use the durable pre-DROP marker LSN"
PLAN_JSON="$RUN_ROOT/plan.json"
"$HELPER" plan --config "$SNAPSHOT_CONFIG" --request "$SUCCESS_REQUEST" > "$PLAN_JSON"
[[ "$(jq -r '.engine' "$PLAN_JSON")" == "snapshot_direct" ]] || die "snapshot plan chose the wrong engine"
pass "planner pins the newest eligible full backup"

SUCCESS_JSON="$RUN_ROOT/snapshot-success.json"
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$SUCCESS_REQUEST" > "$SUCCESS_JSON"
[[ "$(jq -r '.status' "$SUCCESS_JSON")" == "completed" ]]
[[ "$(jq -r '.engine' "$SUCCESS_JSON")" == "snapshot_direct" ]]
[[ "$(jq -r '.recovered_fingerprint' "$SUCCESS_JSON")" == "$TARGET_FINGERPRINT" ]]
[[ "$(jq -r '.recovered_owner' "$SUCCESS_JSON")" == "pgfb_poc_owner" ]]
[[ "$(jq -r '.recovered_acl[] | select(.grantee == "pgfb_poc_reader" and .privilege == "SELECT") | .is_grantable' "$SUCCESS_JSON")" == "false" ]]
[[ "$(jq -r '.recovered_schema_sha256 | length' "$SUCCESS_JSON")" == "64" ]]
[[ "$(jq -r '.cleanup_complete' "$SUCCESS_JSON")" == "true" ]]
SUCCESS_ARTIFACT="$(jq -r '.artifact_path' "$SUCCESS_JSON")"
SUCCESS_ARTIFACT_TABLE="$(jq -r '.artifact_table' "$SUCCESS_JSON")"
[[ -s "$SUCCESS_ARTIFACT" ]] || die "snapshot success artifact is missing"
[[ "$(sha256sum "$SUCCESS_ARTIFACT" | awk '{print $1}')" == "$(jq -r '.artifact_sha256' "$SUCCESS_JSON")" ]]
[[ ! -e "$WORK_ROOT/$SUCCESS_REQUEST_ID/pgdata" ]]
pass "snapshot-direct recovers the pre-DROP table and cleans temporary PostgreSQL"

IDEMPOTENT_JSON="$RUN_ROOT/idempotent.json"
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$SUCCESS_REQUEST" > "$IDEMPOTENT_JSON"
[[ "$(jq -r '.artifact_sha256' "$IDEMPOTENT_JSON")" == "$(jq -r '.artifact_sha256' "$SUCCESS_JSON")" ]]
pass "same request ID returns the durable validated result"

ALTER_REQUEST="$RUN_ROOT/request-pre-alter.json"
jq '.extension.alter_request' "$POC_RESULT" > "$ALTER_REQUEST"
[[ "$(jq -r '.target.value' "$ALTER_REQUEST")" == "$ALTER_MARKER_LSN" ]]
[[ "$ALTER_MARKER_LSN" != "$ALTER_APPLIED_LSN" ]]
ALTER_JSON="$RUN_ROOT/pre-alter-success.json"
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$ALTER_REQUEST" > "$ALTER_JSON"
[[ "$(jq -r '.status' "$ALTER_JSON")" == "completed" ]]
[[ "$(jq -r '.recovered_schema_sha256' "$ALTER_JSON")" == "$(jq -r '.expected_schema_sha256' "$ALTER_REQUEST")" ]]
pass "pre-ALTER marker recovers the old schema before the post-DDL schema LSN"

CLASSIC_REQUEST="$RUN_ROOT/request-classic.json"
write_request "$CLASSIC_REQUEST" "e2e-classic-success" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" "$TARGET_FINGERPRINT"
CLASSIC_JSON="$RUN_ROOT/classic-success.json"
"$HELPER" restore-table --config "$CLASSIC_CONFIG" --request "$CLASSIC_REQUEST" > "$CLASSIC_JSON"
[[ "$(jq -r '.engine' "$CLASSIC_JSON")" == "classic_restore" ]]
[[ "$(jq -r '.recovered_fingerprint' "$CLASSIC_JSON")" == "$TARGET_FINGERPRINT" ]]
pass "classic pgBackRest fallback produces the same recovered table"

QUOTED_REQUEST="$RUN_ROOT/request-quoted.json"
jq '.extension.quoted_request' "$POC_RESULT" > "$QUOTED_REQUEST"
QUOTED_JSON="$RUN_ROOT/quoted-success.json"
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$QUOTED_REQUEST" > "$QUOTED_JSON"
[[ "$(jq -r '.recovered_fingerprint' "$QUOTED_JSON")" == "$QUOTED_FINGERPRINT" ]]
QUOTED_ARTIFACT="$(jq -r '.artifact_path' "$QUOTED_JSON")"
QUOTED_ARTIFACT_TABLE="$(jq -r '.artifact_table' "$QUOTED_JSON")"
pass "quoted schema/table identifiers remain data, not executable SQL"

TOO_OLD_REQUEST="$RUN_ROOT/request-too-old.json"
write_request "$TOO_OLD_REQUEST" "e2e-too-old" "public" "target_table" "$TARGET_OID" "0/1" "" ""
expect_error "target_before_oldest_backup" "$SNAPSHOT_CONFIG" "$TOO_OLD_REQUEST"
pass "target older than retained coverage fails closed"

AFTER_DROP_REQUEST="$RUN_ROOT/request-after-drop.json"
write_request "$AFTER_DROP_REQUEST" "e2e-after-drop" "public" "target_table" "$TARGET_OID" "$AFTER_DROP_LSN" "" ""
expect_error "table_not_found" "$SNAPSHOT_CONFIG" "$AFTER_DROP_REQUEST"
assert_request_clean "e2e-after-drop"
pass "target after DROP does not return an unrelated artifact"

WRONG_OID_REQUEST="$RUN_ROOT/request-wrong-oid.json"
write_request "$WRONG_OID_REQUEST" "e2e-wrong-oid" "public" "target_table" 999999 "$TARGET_LSN" "" ""
expect_error "table_identity_mismatch" "$SNAPSHOT_CONFIG" "$WRONG_OID_REQUEST"
assert_request_clean "e2e-wrong-oid"
pass "OID mismatch fails closed and cleans"

WRONG_SCHEMA_REQUEST="$RUN_ROOT/request-wrong-schema.json"
write_request "$WRONG_SCHEMA_REQUEST" "e2e-wrong-schema" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ""
expect_error "schema_fingerprint_mismatch" "$SNAPSHOT_CONFIG" "$WRONG_SCHEMA_REQUEST"
assert_request_clean "e2e-wrong-schema"
pass "schema mismatch fails closed and cleans"

WRONG_FP_REQUEST="$RUN_ROOT/request-wrong-fingerprint.json"
write_request "$WRONG_FP_REQUEST" "e2e-wrong-fingerprint" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" "wrong"
expect_error "fingerprint_mismatch" "$SNAPSHOT_CONFIG" "$WRONG_FP_REQUEST"
assert_request_clean "e2e-wrong-fingerprint"
pass "row fingerprint mismatch fails closed and cleans"

MISSING_WAL_REQUEST="$RUN_ROOT/request-missing-wal.json"
write_request "$MISSING_WAL_REQUEST" "e2e-missing-wal" "public" "target_table" "$TARGET_OID" "0/F0000000" "" ""
expect_error "recovery_target_unreachable" "$SHORT_CONFIG" "$MISSING_WAL_REQUEST"
assert_request_clean "e2e-missing-wal"
pass "missing WAL has a stable error and idempotent cleanup"

REPO_BUSY_REQUEST="$RUN_ROOT/request-repo-busy.json"
write_request "$REPO_BUSY_REQUEST" "e2e-repo-busy" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
flock -x "$EXPIRE_LOCK" -c 'sleep 3' &
LOCK_PID=$!
sleep 0.2
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$REPO_BUSY_REQUEST" \
    > "$RUN_ROOT/repo-busy.result.json" 2> "$RUN_ROOT/repo-busy.err" &
WAITING_HELPER_PID=$!
sleep 0.5
kill -0 "$WAITING_HELPER_PID" 2>/dev/null \
    || die "restore did not wait for the exclusive expire lock"
wait "$LOCK_PID"
wait "$WAITING_HELPER_PID" \
    || die "restore failed after the exclusive expire lock was released"
[[ "$(jq -r '.status' "$RUN_ROOT/repo-busy.result.json")" == "completed" ]] \
    || die "restore did not complete after waiting for expire"
pass "backup/expire exclusive lock serializes recovery materialization"

SLOW_CP="$RUN_ROOT/slow-cp.sh"
cat > "$SLOW_CP" <<'SLOW_CP_EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
for argument in "$@"; do
    if [[ "$argument" == *".reflink-probe-"* ]]; then
        exec /usr/bin/cp "$@"
    fi
done
sleep 30
exec /usr/bin/cp "$@"
SLOW_CP_EOF
chmod 700 "$SLOW_CP"
SLOW_CONFIG="$RUN_ROOT/helper-slow.json"
write_config "$SLOW_CONFIG" "e2e_slow" "xfs_reflink" "$HELPER_PORT" "$SLOW_CP" 60 120 536870912
CANCEL_ONE="$RUN_ROOT/request-cancel-one.json"
CANCEL_TWO="$RUN_ROOT/request-cancel-two.json"
write_request "$CANCEL_ONE" "e2e-cancel-one" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
write_request "$CANCEL_TWO" "e2e-cancel-two" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
"$HELPER" restore-table --config "$SLOW_CONFIG" --request "$CANCEL_ONE" > "$RUN_ROOT/cancel.out" 2> "$RUN_ROOT/cancel.err" &
ACTIVE_HELPER_PID=$!
for _ in $(seq 1 200); do
    phase="$(jq -r '.phase // empty' "$WORK_ROOT/e2e-cancel-one/state.json" 2>/dev/null || true)"
    [[ "$phase" == "materializing" ]] && break
    sleep 0.05
done
[[ "${phase:-}" == "materializing" ]] || die "cancel test never reached materializing"
if flock -x -n "$EXPIRE_LOCK" -c true; then
    die "expire lock was not pinned during materialization"
fi
expect_error "recovery_busy" "$SLOW_CONFIG" "$CANCEL_TWO"
kill -TERM "$ACTIVE_HELPER_PID"
set +e
wait "$ACTIVE_HELPER_PID"
CANCEL_RC=$?
set -e
ACTIVE_HELPER_PID=""
[[ "$CANCEL_RC" != "0" ]]
[[ "$(jq -r '.code' "$RUN_ROOT/cancel.err")" == "cancelled" ]]
assert_request_clean "e2e-cancel-one"
pass "profile concurrency, repository pinning and SIGTERM cleanup are enforced"

CRASH_REQUEST="$RUN_ROOT/request-crash.json"
write_request "$CRASH_REQUEST" "e2e-crash-reconcile" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" "$TARGET_FINGERPRINT"
"$HELPER" restore-table --config "$SLOW_CONFIG" --request "$CRASH_REQUEST" > "$RUN_ROOT/crash.out" 2> "$RUN_ROOT/crash.err" &
ACTIVE_HELPER_PID=$!
for _ in $(seq 1 200); do
    phase="$(jq -r '.phase // empty' "$WORK_ROOT/e2e-crash-reconcile/state.json" 2>/dev/null || true)"
    [[ "$phase" == "materializing" && -f "$WORK_ROOT/e2e-crash-reconcile/active-process.json" ]] && break
    sleep 0.05
done
[[ -f "$WORK_ROOT/e2e-crash-reconcile/active-process.json" ]] \
    || die "crash test did not persist the active process identity"
ORPHAN_GROUP="$(jq -r '.process_group' "$WORK_ROOT/e2e-crash-reconcile/active-process.json")"
kill -KILL "$ACTIVE_HELPER_PID"
set +e
wait "$ACTIVE_HELPER_PID"
CRASH_RC=$?
set -e
ACTIVE_HELPER_PID=""
[[ "$CRASH_RC" != "0" ]]
kill -0 -- "-$ORPHAN_GROUP" 2>/dev/null || die "crash test did not leave a child process to reconcile"
CRASH_RECOVERED_JSON="$RUN_ROOT/crash-recovered.json"
CRASH_RETRY_CONFIG="$RUN_ROOT/helper-crash-retry.json"
jq --arg cp_bin "/usr/bin/cp" '.cp_bin = $cp_bin' "$SLOW_CONFIG" > "$CRASH_RETRY_CONFIG"
"$HELPER" restore-table --config "$CRASH_RETRY_CONFIG" --request "$CRASH_REQUEST" > "$CRASH_RECOVERED_JSON"
[[ "$(jq -r '.status' "$CRASH_RECOVERED_JSON")" == "completed" ]]
kill -0 -- "-$ORPHAN_GROUP" 2>/dev/null && die "startup reconciliation left the orphan process group alive"
[[ ! -e "$WORK_ROOT/e2e-crash-reconcile/active-process.json" ]]
[[ ! -e "$WORK_ROOT/e2e-crash-reconcile/pgdata" ]]
pass "SIGKILL crash is reconciled on the next execution without an orphan process or cluster"

TIMEOUT_CONFIG="$RUN_ROOT/helper-command-timeout.json"
write_config "$TIMEOUT_CONFIG" "e2e_timeout" "xfs_reflink" "$HELPER_PORT" "$SLOW_CP" 1 120 536870912
TIMEOUT_REQUEST="$RUN_ROOT/request-command-timeout.json"
write_request "$TIMEOUT_REQUEST" "e2e-command-timeout" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
expect_error "command_timeout" "$TIMEOUT_CONFIG" "$TIMEOUT_REQUEST"
assert_request_clean "e2e-command-timeout"
pass "child command timeout kills its process group and cleans"

FAILING_PG_BIN="$RUN_ROOT/failing-pg-bin"
mkdir -p "$FAILING_PG_BIN"
ln -s "$PG_BIN/postgres" "$FAILING_PG_BIN/postgres"
ln -s "$PG_BIN/psql" "$FAILING_PG_BIN/psql"
ln -s "$PG_BIN/pg_dump" "$FAILING_PG_BIN/pg_dump"
cat > "$FAILING_PG_BIN/pg_ctl" <<FAILING_PG_CTL_EOF
#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "\${1:-}" == "--version" ]]; then
    exec "$PG_BIN/pg_ctl" --version
fi
exit 42
FAILING_PG_CTL_EOF
chmod 700 "$FAILING_PG_BIN/pg_ctl"
START_FAILURE_CONFIG="$RUN_ROOT/helper-start-failure.json"
write_config "$START_FAILURE_CONFIG" "e2e_start_failure" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 10 120 536870912
jq --arg pg_bin_dir "$FAILING_PG_BIN" '.pg_bin_dir = $pg_bin_dir' "$START_FAILURE_CONFIG" > "$START_FAILURE_CONFIG.tmp"
mv "$START_FAILURE_CONFIG.tmp" "$START_FAILURE_CONFIG"
START_FAILURE_REQUEST="$RUN_ROOT/request-start-failure.json"
write_request "$START_FAILURE_REQUEST" "e2e-start-failure" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
expect_error "command_failed" "$START_FAILURE_CONFIG" "$START_FAILURE_REQUEST"
assert_request_clean "e2e-start-failure"
pass "temporary PostgreSQL start failure preserves the original error and cleans"

QUOTA_CONFIG="$RUN_ROOT/helper-quota.json"
write_config "$QUOTA_CONFIG" "e2e_quota" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 10 120 1048576
QUOTA_REQUEST="$RUN_ROOT/request-quota.json"
write_request "$QUOTA_REQUEST" "e2e-quota" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
expect_error "work_quota_exceeded" "$QUOTA_CONFIG" "$QUOTA_REQUEST"
pass "backup larger than the configured work quota is rejected before materialization"

# Free-space reserve clearly above available capacity.
FREE_CONFIG="$RUN_ROOT/helper-free-space.json"
write_config "$FREE_CONFIG" "e2e_free_space" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 10 120 \
    536870912 1099511627776 9223372036854775807 86400 64 1099511627776
FREE_REQUEST="$RUN_ROOT/request-free-space.json"
write_request "$FREE_REQUEST" "e2e-free-space" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
expect_error "free_space_exhausted" "$FREE_CONFIG" "$FREE_REQUEST"
pass "configured free-space reserve rejects recovery before materialization"

# GC dry-run + live: expired unpinned artifact removed; fresh pin retained.
GC_OLD_DIR="$WORK_ROOT/e2e-gc-old"
mkdir -p "$GC_OLD_DIR"
printf 'stale-artifact' > "$GC_OLD_DIR/target-table.dump"
HELPER_VERSION_STRING="$($HELPER --version)"
jq -n \
    --arg request_id "e2e-gc-old" \
    --arg artifact_path "$GC_OLD_DIR/target-table.dump" \
    --arg helper_version "$HELPER_VERSION_STRING" \
    '{
      result_format_version: 3,
      helper_version: $helper_version,
      status: "completed",
      request: {
        request_id: $request_id,
        database: "pocdb",
        table: {schema: "public", name: "target_table", rel_oid: 1},
        target: {kind: "lsn", value: "0/1", observed_at_unix_seconds: 1, inclusive: true},
        expected_schema_version: 1,
        expected_schema_sha256: null,
        expected_fingerprint: null
      },
      profile: "e2e_snapshot",
      engine: "classic_restore",
      stanza: "large_db_poc",
      repository_key: 1,
      backup_label: "stale",
      backup_stop_lsn: "0/1",
      postgres_version: "17",
      pgbackrest_version: "2",
      recovered_row_count: 0,
      recovered_owner: "postgres",
      recovered_acl: [],
      recovered_schema_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      artifact_schema: "flashback_import",
      artifact_table: "r_aaaaaaaaaaaaaaaa",
      artifact_schema_sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      recovered_fingerprint: "0|0",
      artifact_path: $artifact_path,
      artifact_bytes: 14,
      artifact_sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      durations: {materialize_ms: 1, recovery_ms: 1, validate_ms: 1, extract_ms: 1, total_ms: 4},
      cleanup_complete: true
    }' > "$GC_OLD_DIR/result.json"
jq -n \
    --arg request_id "e2e-gc-old" \
    '{request_id: $request_id, phase: "completed", updated_at_unix_seconds: 1, detail: null}' \
    > "$GC_OLD_DIR/state.json"

[[ -f "$WORK_ROOT/$SUCCESS_REQUEST_ID/artifact.pin" ]] \
    || die "successful restore did not write an awaiting-import pin"
GC_DRY_JSON="$RUN_ROOT/gc-dry-run.json"
"$HELPER" gc --config "$SNAPSHOT_CONFIG" --dry-run > "$GC_DRY_JSON"
[[ "$(jq -r '.status' "$GC_DRY_JSON")" == "ok" ]]
[[ "$(jq -r '.dry_run' "$GC_DRY_JSON")" == "true" ]]
jq -e --arg id e2e-gc-old \
    '.decisions[] | select(.request_id == $id and .action == "remove")' \
    "$GC_DRY_JSON" > /dev/null
[[ -d "$GC_OLD_DIR" ]] || die "dry-run GC deleted an artifact"
GC_LIVE_JSON="$RUN_ROOT/gc-live.json"
# Short TTL so the synthetic old artifact is eligible while the pinned success stays.
GC_CONFIG="$RUN_ROOT/helper-gc.json"
write_config "$GC_CONFIG" "e2e_snapshot" "xfs_reflink" "$HELPER_PORT" "/usr/bin/cp" 120 300 \
    536870912 1099511627776 67108864 60 64 1099511627776
"$HELPER" gc --config "$GC_CONFIG" > "$GC_LIVE_JSON"
[[ ! -d "$GC_OLD_DIR" ]] || die "live GC left expired unpinned artifact in place"
[[ -f "$WORK_ROOT/$SUCCESS_REQUEST_ID/artifact.pin" ]] \
    || die "live GC removed a pinned successful artifact"
[[ -f "$(jq -r '.audit_path' "$GC_LIVE_JSON")" ]] \
    || die "GC audit log was not written"
pass "gc --dry-run and gc remove expired artifacts while preserving pins"

CONFLICT_REQUEST="$RUN_ROOT/request-conflict.json"
jq '.expected_fingerprint = "conflict"' "$SUCCESS_REQUEST" > "$CONFLICT_REQUEST"
expect_error "request_conflict" "$SNAPSHOT_CONFIG" "$CONFLICT_REQUEST"
pass "request IDs cannot be reused for a different contract"

CONFLICT_PROFILE_CONFIG="$RUN_ROOT/helper-conflict-profile.json"
jq '.profile = "different_profile"' "$SNAPSHOT_CONFIG" > "$CONFLICT_PROFILE_CONFIG"
expect_error "request_conflict" "$CONFLICT_PROFILE_CONFIG" "$SUCCESS_REQUEST"
pass "request IDs are immutably bound to the helper profile"

UNSAFE_PATH_CONFIG="$RUN_ROOT/helper-unsafe-path.json"
jq --arg work_root "$RUN_ROOT/repo/../helper-work" '.work_root = $work_root' \
    "$SNAPSHOT_CONFIG" > "$UNSAFE_PATH_CONFIG"
UNSAFE_PATH_REQUEST="$RUN_ROOT/request-unsafe-path.json"
write_request "$UNSAFE_PATH_REQUEST" "e2e-unsafe-path" "public" "target_table" "$TARGET_OID" "$TARGET_LSN" "" ""
expect_error "invalid_config" "$UNSAFE_PATH_CONFIG" "$UNSAFE_PATH_REQUEST"
pass "runtime roots containing parent traversal are rejected before execution"

UNSAFE_MODE_CONFIG="$RUN_ROOT/helper-unsafe-mode.json"
cp -- "$SNAPSHOT_CONFIG" "$UNSAFE_MODE_CONFIG"
chmod 0666 "$UNSAFE_MODE_CONFIG"
expect_error "invalid_config" "$UNSAFE_MODE_CONFIG" "$UNSAFE_PATH_REQUEST"
SAFE_CONFIG_LINK="$RUN_ROOT/helper-config-link.json"
ln -s "$SNAPSHOT_CONFIG" "$SAFE_CONFIG_LINK"
expect_error "invalid_config" "$SAFE_CONFIG_LINK" "$UNSAFE_PATH_REQUEST"
pass "writable and symlinked helper configuration files are rejected before parsing"

ORIGINAL_SHA="$(jq -r '.artifact_sha256' "$SUCCESS_JSON")"
printf X | dd of="$SUCCESS_ARTIFACT" bs=1 seek=0 conv=notrunc status=none
[[ "$(sha256sum "$SUCCESS_ARTIFACT" | awk '{print $1}')" != "$ORIGINAL_SHA" ]]
REPAIRED_JSON="$RUN_ROOT/repaired.json"
"$HELPER" restore-table --config "$SNAPSHOT_CONFIG" --request "$SUCCESS_REQUEST" > "$REPAIRED_JSON"
[[ "$(sha256sum "$SUCCESS_ARTIFACT" | awk '{print $1}')" == "$(jq -r '.artifact_sha256' "$REPAIRED_JSON")" ]]
pass "corrupt cached artifact is discarded and rebuilt"

mkdir -p "$VERIFY_SOCKET"
chmod 700 "$VERIFY_SOCKET"
"$PG_CTL" -D "$RUN_ROOT/primary" -l "$RUN_ROOT/log/helper-artifact-verify.log" \
    -o "-c archive_mode=off -p $VERIFY_PORT -k $VERIFY_SOCKET" start -w -t 60 > /dev/null
PRIMARY_STARTED=1

CONTROLLER_FAILURE_ERR="$RUN_ROOT/controller-helper-failure.err"
CONTROLLER_FAILURE_CONFIG="$RUN_ROOT/helper-controller-failure.json"
jq '.pgbackrest_bin = "/bin/false"' "$SNAPSHOT_CONFIG" \
    > "$CONTROLLER_FAILURE_CONFIG"
chmod 600 "$CONTROLLER_FAILURE_CONFIG"
set +e
PGHOST="$VERIFY_SOCKET" PGPORT="$VERIFY_PORT" PGUSER="$(id -un)" \
    "$SCRIPT_DIR/pg_flashback_backup_restore.sh" \
        --config "$CONTROLLER_FAILURE_CONFIG" \
        --dbname pocdb \
        --table public.target_table \
        --target-lsn "$DROP_MARKER_LSN" \
        --helper "$HELPER" > /dev/null 2> "$CONTROLLER_FAILURE_ERR"
CONTROLLER_FAILURE_RC=$?
set -e
[[ "$CONTROLLER_FAILURE_RC" != "0" ]]
grep -q '"code":"command_failed"' "$CONTROLLER_FAILURE_ERR"
FAILED_CONTROLLER_REQUEST="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT request_id FROM flashback.backup_restore_requests WHERE status = 'failed' ORDER BY created_at DESC LIMIT 1;")"
[[ -n "$FAILED_CONTROLLER_REQUEST" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = 'flashback_import' AND c.relkind IN ('r', 'p');")" == "0" ]]
pass "reference controller records helper failure and leaves no imported table"

CONTROLLER_JSON="$RUN_ROOT/controller-success.json"
PGHOST="$VERIFY_SOCKET" PGPORT="$VERIFY_PORT" PGUSER="$(id -un)" \
    "$SCRIPT_DIR/pg_flashback_backup_restore.sh" \
        --config "$SNAPSHOT_CONFIG" \
        --dbname pocdb \
        --request "$SUCCESS_REQUEST" \
        --helper "$HELPER" > "$CONTROLLER_JSON"
[[ "$(jq -r '.status' "$CONTROLLER_JSON")" == "completed" ]]
[[ "$(jq -r '.request_id' "$CONTROLLER_JSON")" == "$SUCCESS_REQUEST_ID" ]]
PRODUCTION_FP="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb -c "SELECT count(*)::text || '|' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM public.target_table AS t;")"
[[ "$PRODUCTION_FP" == "$TARGET_FINGERPRINT" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb -c "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.target_table'::regclass;")" == "pgfb_poc_owner" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb -c "SELECT has_table_privilege('pgfb_poc_reader', 'public.target_table', 'SELECT');")" == "t" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb -v request_id="$SUCCESS_REQUEST_ID" -f <(printf '%s\n' "SELECT status FROM flashback.backup_restore_requests WHERE request_id = :'request_id';"))" == "completed" ]]
pass "reference controller verifies the artifact and completes the extension shadow swap"

# The swap intentionally leaves zero active generations and a building
# successor. Resolve its real marker, take a new FULL strictly after it, and
# prove that repository verification reanchors coverage at the new backup stop.
POST_SWAP_TRACKING_ID="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT tracking_id FROM flashback.tracked_tables WHERE schema_name='public' AND table_name='target_table' AND is_active;")"
[[ -n "$POST_SWAP_TRACKING_ID" ]] || die "post-swap tracking lifecycle is missing"
for _ in $(seq 1 200); do
    POST_SWAP_MARKER="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
        -c "SELECT details->>'tracking_marker_lsn' FROM flashback.coverage_generations WHERE tracking_id=$POST_SWAP_TRACKING_ID AND state='building' ORDER BY generation_no DESC LIMIT 1;")"
    [[ -n "$POST_SWAP_MARKER" ]] && break
    "$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
        -c "SELECT flashback_consume_wal(4096);" > /dev/null || true
    sleep 0.05
done
[[ -n "${POST_SWAP_MARKER:-}" ]] || die "post-swap backup marker did not resolve"
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) FROM flashback.coverage_generations WHERE tracking_id=$POST_SWAP_TRACKING_ID AND state='active';")" == "0" ]]

"$PG_CTL" -D "$RUN_ROOT/primary" stop -m fast -w -t 60 > /dev/null
PRIMARY_STARTED=0
"$PG_CTL" -D "$RUN_ROOT/primary" -l "$RUN_ROOT/log/post-swap-reanchor.log" \
    -o "-p $VERIFY_PORT -k $VERIFY_SOCKET" start -w -t 60 > /dev/null
PRIMARY_STARTED=1
"$PGBACKREST" --config="$RUN_ROOT/pgbackrest.conf" --stanza=large_db_poc --repo=1 \
    --pg1-port="$VERIFY_PORT" --pg1-socket-path="$VERIFY_SOCKET" \
    --type=full --compress-type=none --no-expire-auto backup > "$RUN_ROOT/post-swap-backup.log"
[[ "$(find "$RUN_ROOT/repo/backup/large_db_poc" -mindepth 2 -maxdepth 2 \
    -name backup.manifest | wc -l)" -ge "2" ]] \
    || die "post-swap backup auto-expired a retained predecessor anchor"
POST_SWAP_CONFIG="$RUN_ROOT/helper-post-swap-reanchor.json"
jq --arg host "$VERIFY_SOCKET" --argjson port "$VERIFY_PORT" \
    '.controller.host = $host | .controller.port = $port' \
    "$RUN_ROOT/helper-verifier.json" > "$POST_SWAP_CONFIG"
chmod 600 "$POST_SWAP_CONFIG"
POST_SWAP_VERIFY_REQUEST="$RUN_ROOT/request-post-swap-reanchor.json"
jq -n --arg request_id "e2e-post-swap-reanchor" \
    --argjson tracking_id "$POST_SWAP_TRACKING_ID" \
    '{request_id: $request_id, tracking_id: $tracking_id}' > "$POST_SWAP_VERIFY_REQUEST"
chmod 600 "$POST_SWAP_VERIFY_REQUEST"
"$HELPER" verify-anchor --config "$POST_SWAP_CONFIG" \
    --request "$POST_SWAP_VERIFY_REQUEST" > "$RUN_ROOT/post-swap-reanchor.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/post-swap-reanchor.result.json")" == "verified" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) FROM flashback.coverage_generations WHERE tracking_id=$POST_SWAP_TRACKING_ID AND state='active' AND boundary_lsn > '$POST_SWAP_MARKER'::pg_lsn;")" == "1" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) FROM flashback.coverage_gaps WHERE tracking_id=$POST_SWAP_TRACKING_ID AND reanchored_by_generation_id IS NOT NULL;")" -ge "1" ]]
pass "post-swap FULL backup reanchors the building successor with no fallback gap"

"$PG_BIN/dropdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" --if-exists helper_verify > /dev/null
"$PG_BIN/createdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" helper_verify
"$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d helper_verify -c 'CREATE SCHEMA flashback_import;' > /dev/null
"$PG_RESTORE" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d helper_verify --no-owner --no-acl "$SUCCESS_ARTIFACT"
ACTUAL_FP="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d helper_verify -c "SELECT count(*)::text || '|' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM flashback_import.\"$SUCCESS_ARTIFACT_TABLE\" AS t;")"
[[ "$ACTUAL_FP" == "$TARGET_FINGERPRINT" ]]
"$PG_RESTORE" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d helper_verify --no-owner --no-acl "$QUOTED_ARTIFACT"
ACTUAL_QUOTED_FP="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d helper_verify -c "SELECT count(*)::text || '|' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM flashback_import.\"$QUOTED_ARTIFACT_TABLE\" AS t;")"
[[ "$ACTUAL_QUOTED_FP" == "$QUOTED_FINGERPRINT" ]]
ANCHOR_AUDIT_CONFIG="$RUN_ROOT/helper-anchor-audit.json"
jq --arg host "$VERIFY_SOCKET" --argjson port "$VERIFY_PORT" \
    '.controller.host = $host | .controller.port = $port' \
    "$RUN_ROOT/helper-verifier.json" > "$ANCHOR_AUDIT_CONFIG"
chmod 600 "$ANCHOR_AUDIT_CONFIG"
HEALTHY_AUDIT_JSON="$RUN_ROOT/anchor-audit-healthy.json"
"$HELPER" audit-anchors --config "$ANCHOR_AUDIT_CONFIG" > "$HEALTHY_AUDIT_JSON"
[[ "$(jq -r '.status' "$HEALTHY_AUDIT_JSON")" == "ok" ]]
[[ "$(jq -r '.checked' "$HEALTHY_AUDIT_JSON")" -ge 1 ]]
pass "periodic anchor audit verifies every retained repository anchor"

# A newer PostgreSQL timeline in the retained archive is not ordinary missing
# WAL. Verify it from a CoW repository copy so the live fixture remains intact,
# and require a durable timeline gap rather than a generic transient error.
WRONG_TIMELINE_REPO="$RUN_ROOT/repo-wrong-timeline"
cp -a --reflink=always "$RUN_ROOT/repo" "$WRONG_TIMELINE_REPO"
ARCHIVE_SEGMENT="$(find "$WRONG_TIMELINE_REPO/archive/large_db_poc" -type f \
    -regextype posix-extended -regex '.*/[0-9A-Fa-f]{24}-[0-9a-f]{40}' | sort | tail -1)"
[[ -f "$ARCHIVE_SEGMENT" ]] || die "wrong-timeline fixture found no archived WAL segment"
ARCHIVE_NAME="$(basename "$ARCHIVE_SEGMENT")"
ANCHOR_TIMELINE="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT min(timeline_id) FROM flashback.backup_anchors;")"
FUTURE_TIMELINE_HEX="$(printf '%08X' $((ANCHOR_TIMELINE + 1)))"
FUTURE_SEGMENT="$(dirname "$ARCHIVE_SEGMENT")/$FUTURE_TIMELINE_HEX${ARCHIVE_NAME:8}"
cp --reflink=always "$ARCHIVE_SEGMENT" "$FUTURE_SEGMENT"
WRONG_TIMELINE_TRACKING_ID="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT min(tracking_id) FROM flashback.coverage_generations WHERE recovery_profile='backup' AND state='active';")"
WRONG_TIMELINE_CONFIG="$RUN_ROOT/helper-wrong-timeline.json"
jq --arg repository "$WRONG_TIMELINE_REPO" \
    '.repository_path = $repository' "$ANCHOR_AUDIT_CONFIG" > "$WRONG_TIMELINE_CONFIG"
chmod 600 "$WRONG_TIMELINE_CONFIG"
WRONG_TIMELINE_REQUEST="$RUN_ROOT/request-wrong-timeline.json"
jq -n --arg request_id "e2e-wrong-timeline" \
    --argjson tracking_id "$WRONG_TIMELINE_TRACKING_ID" \
    '{request_id: $request_id, tracking_id: $tracking_id}' > "$WRONG_TIMELINE_REQUEST"
chmod 600 "$WRONG_TIMELINE_REQUEST"
set +e
"$HELPER" verify-frontier --config "$WRONG_TIMELINE_CONFIG" \
    --request "$WRONG_TIMELINE_REQUEST" \
    > "$RUN_ROOT/wrong-timeline.out" 2> "$RUN_ROOT/wrong-timeline.err"
WRONG_TIMELINE_RC=$?
set -e
[[ "$WRONG_TIMELINE_RC" != "0" ]]
[[ "$(jq -r '.code' "$RUN_ROOT/wrong-timeline.err")" == "timeline_mismatch" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) FROM flashback.coverage_gaps WHERE tracking_id=$WRONG_TIMELINE_TRACKING_ID AND reason='timeline_mismatch' AND reanchored_by_generation_id IS NULL;")" == "1" ]]
pass "newer repository timeline is detected and durably freezes coverage"

ANCHOR_MANIFEST="$(find "$RUN_ROOT/repo/backup/large_db_poc" -mindepth 2 -maxdepth 2 -name backup.manifest -print -quit)"
[[ -f "$ANCHOR_MANIFEST" ]] || die "anchor audit fixture manifest is missing"
mv "$ANCHOR_MANIFEST" "$ANCHOR_MANIFEST.audit-missing"
MISSING_AUDIT_JSON="$RUN_ROOT/anchor-audit-missing.json"
"$HELPER" audit-anchors --config "$ANCHOR_AUDIT_CONFIG" > "$MISSING_AUDIT_JSON"
mv "$ANCHOR_MANIFEST.audit-missing" "$ANCHOR_MANIFEST"
[[ "$(jq -r '.status' "$MISSING_AUDIT_JSON")" == "degraded" ]]
[[ "$(jq '[.findings[] | select(.status == "frozen")] | length' "$MISSING_AUDIT_JSON")" -ge 1 ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d pocdb \
    -c "SELECT count(*) > 0 FROM flashback_health() WHERE health = 'degraded' AND reason LIKE '%repository proof%';")" == "t" ]]
pass "externally removed anchor is detected, durably frozen and surfaced by health"

# Prove the database-side lease closes the expire/admission race. This uses a
# separate extension database with no retained anchors so expiration may start,
# then attempts to create a backup generation while a deliberately slow
# pgBackRest wrapper keeps the lease open.
"$PG_BIN/dropdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" --if-exists expire_e2e > /dev/null
"$PG_BIN/createdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" expire_e2e
"$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d expire_e2e \
    -c 'CREATE EXTENSION pg_flashback; CREATE TABLE public.expire_candidate(id bigint PRIMARY KEY);' \
    > /dev/null
SLOW_EXPIRE="$RUN_ROOT/slow-pgbackrest-expire.sh"
cat > "$SLOW_EXPIRE" <<SLOW_EXPIRE_EOF
#!/usr/bin/env bash
set -Eeuo pipefail
sleep 3
exec "$PGBACKREST" "\$@"
SLOW_EXPIRE_EOF
chmod 700 "$SLOW_EXPIRE"
EXPIRE_E2E_CONFIG="$RUN_ROOT/helper-expire-e2e.json"
jq --arg host "$VERIFY_SOCKET" \
   --argjson port "$VERIFY_PORT" \
   --arg pgbackrest "$SLOW_EXPIRE" \
   '.controller.host = $host
    | .controller.port = $port
    | .controller.database = "expire_e2e"
    | .pgbackrest_bin = $pgbackrest
    | .profile = "expire_e2e"' \
   "$RUN_ROOT/helper-verifier.json" > "$EXPIRE_E2E_CONFIG"
chmod 600 "$EXPIRE_E2E_CONFIG"
"$HELPER" expire --config "$EXPIRE_E2E_CONFIG" \
    > "$RUN_ROOT/expire-e2e.result.json" 2> "$RUN_ROOT/expire-e2e.err" &
ACTIVE_HELPER_PID=$!
EXPIRE_LEASE_ACTIVE="f"
for _ in $(seq 1 100); do
    EXPIRE_LEASE_ACTIVE="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" \
        -d expire_e2e -c "SELECT EXISTS (SELECT 1 FROM flashback.backup_expire_leases WHERE state='active');")"
    [[ "$EXPIRE_LEASE_ACTIVE" == "t" ]] && break
    sleep 0.05
done
[[ "$EXPIRE_LEASE_ACTIVE" == "t" ]] || die "durable expire lease was not observed"
set +e
"$PSQL" -X -qAt -v ON_ERROR_STOP=1 -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" \
    -d expire_e2e \
    -c "SELECT flashback_track_backup('public.expire_candidate', 'expire_e2e');" \
    > "$RUN_ROOT/expire-race-track.out" 2> "$RUN_ROOT/expire-race-track.err"
EXPIRE_RACE_RC=$?
set -e
[[ "$EXPIRE_RACE_RC" != "0" ]] || die "backup generation activated during expiration"
grep -q 'blocked by repository expiration' "$RUN_ROOT/expire-race-track.err" \
    || die "expire race did not fail with the durable lease guard"
wait "$ACTIVE_HELPER_PID" || die "coordinated expiration failed"
ACTIVE_HELPER_PID=""
[[ "$(jq -r '.status' "$RUN_ROOT/expire-e2e.result.json")" == "expired" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d expire_e2e \
    -c "SELECT count(*) FROM flashback.backup_expire_leases WHERE state='completed';")" == "1" ]]
"$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d expire_e2e \
    -c "SELECT flashback_track_backup('public.expire_candidate', 'expire_e2e');" > /dev/null
pass "durable expire lease blocks generation activation and completes after pgBackRest"

# A helper/pgBackRest failure must leave the lease active. A later invocation
# with the corrected binary resumes the same lease, reruns expire, and only
# then reopens backup lifecycle admission.
"$PG_BIN/dropdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" --if-exists expire_resume_e2e > /dev/null
"$PG_BIN/createdb" -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" expire_resume_e2e
"$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d expire_resume_e2e \
    -c 'CREATE EXTENSION pg_flashback;' > /dev/null
EXPIRE_RESUME_CONFIG="$RUN_ROOT/helper-expire-resume.json"
jq --arg host "$VERIFY_SOCKET" --argjson port "$VERIFY_PORT" \
   '.controller.host = $host
    | .controller.port = $port
    | .controller.database = "expire_resume_e2e"
    | .pgbackrest_bin = "/bin/false"
    | .profile = "expire_resume_e2e"' \
   "$RUN_ROOT/helper-verifier.json" > "$EXPIRE_RESUME_CONFIG"
chmod 600 "$EXPIRE_RESUME_CONFIG"
set +e
"$HELPER" expire --config "$EXPIRE_RESUME_CONFIG" \
    > "$RUN_ROOT/expire-resume-fail.out" 2> "$RUN_ROOT/expire-resume-fail.err"
EXPIRE_RESUME_RC=$?
set -e
[[ "$EXPIRE_RESUME_RC" != "0" ]]
[[ "$(jq -r '.code' "$RUN_ROOT/expire-resume-fail.err")" == "command_failed" ]]
EXPIRE_RESUME_LEASE_ID="$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" \
    -d expire_resume_e2e \
    -c "SELECT lease_id FROM flashback.backup_expire_leases WHERE state='active';")"
[[ -n "$EXPIRE_RESUME_LEASE_ID" ]] || die "failed expire did not retain its durable lease"
jq --arg pgbackrest "$PGBACKREST" '.pgbackrest_bin = $pgbackrest' \
    "$EXPIRE_RESUME_CONFIG" > "$EXPIRE_RESUME_CONFIG.retry"
mv "$EXPIRE_RESUME_CONFIG.retry" "$EXPIRE_RESUME_CONFIG"
chmod 600 "$EXPIRE_RESUME_CONFIG"
"$HELPER" expire --config "$EXPIRE_RESUME_CONFIG" > "$RUN_ROOT/expire-resume.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/expire-resume.result.json")" == "expired" ]]
[[ "$("$PSQL" -X -qAt -h "$VERIFY_SOCKET" -p "$VERIFY_PORT" -d expire_resume_e2e \
    -c "SELECT state FROM flashback.backup_expire_leases WHERE lease_id=$EXPIRE_RESUME_LEASE_ID;")" == "completed" ]]
pass "failed expiration retains and safely resumes its durable lease"

"$PG_CTL" -D "$RUN_ROOT/primary" stop -m fast -w -t 60 > /dev/null
PRIMARY_STARTED=0
pass "returned artifacts restore into a separate database with matching fingerprints"

HELPER_VERSION="$($HELPER --version)"
[[ -n "$HELPER_VERSION" ]] || die "helper --version returned an empty value"
SOURCE_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf 'unknown')"
SOURCE_DIRTY=true
if git -C "$REPO_ROOT" diff --quiet \
    && git -C "$REPO_ROOT" diff --cached --quiet \
    && [[ -z "$(git -C "$REPO_ROOT" ls-files --others --exclude-standard)" ]]; then
    SOURCE_DIRTY=false
fi
jq -n \
    --arg run_id "$RUN_ID" \
    --arg status "ok" \
    --arg source_commit "$SOURCE_COMMIT" \
    --arg platform "$(uname -sm)" \
    --arg postgres "$("$PG_BIN/postgres" --version)" \
    --arg pgbackrest "$($PGBACKREST version)" \
    --arg helper "$HELPER_VERSION" \
    --arg snapshot_sha "$(jq -r '.artifact_sha256' "$REPAIRED_JSON")" \
    --arg classic_sha "$(jq -r '.artifact_sha256' "$CLASSIC_JSON")" \
    --argjson source_dirty "$SOURCE_DIRTY" \
    --argjson checks "$PASSED" \
    '{run_id: $run_id, status: $status, checks_passed: $checks,
      source: {commit: $source_commit, dirty: $source_dirty, platform: $platform},
      versions: {postgres: $postgres, pgbackrest: $pgbackrest, helper: $helper},
      artifacts: {snapshot_sha256: $snapshot_sha, classic_sha256: $classic_sha}}' > "$SUMMARY_JSON"

RUN_COMPLETE=1
log "all $PASSED checks passed"
log "summary: $SUMMARY_JSON"

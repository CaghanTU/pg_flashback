#!/usr/bin/env bash
# Reference controller for the backup-backed restore protocol. It keeps the
# recovery helper isolated from production and performs the extension-side
# import/swap through libpq tools.

set -Eeuo pipefail
umask 077

usage() {
    cat >&2 <<'EOF'
Usage:
  pg_flashback_backup_restore.sh --config FILE --dbname DB \
      --table SCHEMA.TABLE --target-lsn LSN [--helper BIN]
  pg_flashback_backup_restore.sh --config FILE --dbname DB \
      --request CLAIMED_REQUEST.json [--helper BIN]

Connection settings other than the database name use the standard PGHOST,
PGPORT, PGUSER, PGPASSWORD and PGSERVICE environment variables.
EOF
    exit 64
}

CONFIG=""
DBNAME=""
TABLE=""
TARGET_LSN=""
REQUEST_FILE=""
HELPER="${PG_FLASHBACK_RECOVERY_BIN:-pg-flashback-recovery}"

while (($# > 0)); do
    case "$1" in
        --config) CONFIG="${2:-}"; shift 2 ;;
        --dbname) DBNAME="${2:-}"; shift 2 ;;
        --table) TABLE="${2:-}"; shift 2 ;;
        --target-lsn) TARGET_LSN="${2:-}"; shift 2 ;;
        --request) REQUEST_FILE="${2:-}"; shift 2 ;;
        --helper) HELPER="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; usage ;;
    esac
done

[[ -n "$CONFIG" && -f "$CONFIG" && -n "$DBNAME" ]] || usage
if [[ -n "$REQUEST_FILE" ]]; then
    [[ -f "$REQUEST_FILE" && -z "$TABLE" && -z "$TARGET_LSN" ]] || usage
else
    [[ -n "$TABLE" && -n "$TARGET_LSN" ]] || usage
fi
command -v "$HELPER" > /dev/null 2>&1 || {
    printf 'recovery helper is not executable: %s\n' "$HELPER" >&2
    exit 69
}
command -v jq > /dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 69; }
command -v sha256sum > /dev/null 2>&1 || { printf 'sha256sum is required\n' >&2; exit 69; }

PG_BIN_DIR="$(jq -er '.pg_bin_dir | select(type == "string" and length > 0)' "$CONFIG")"
PSQL="$PG_BIN_DIR/psql"
PG_RESTORE="$PG_BIN_DIR/pg_restore"
[[ -x "$PSQL" && -x "$PG_RESTORE" ]] || {
    printf 'psql/pg_restore are not executable under %s\n' "$PG_BIN_DIR" >&2
    exit 69
}

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgfb-controller.XXXXXXXX")"
REQUEST_JSON="$TMP_DIR/request.json"
RESULT_JSON="$TMP_DIR/result.json"
HELPER_ERROR="$TMP_DIR/helper-error.json"
REQUEST_ID=""
ARTIFACT_SCHEMA=""
ARTIFACT_TABLE=""
IMPORTED=0
ACCEPTED=0
COMPLETED=0

psql_file() {
    "$PSQL" -X -qAt -v ON_ERROR_STOP=1 --dbname "$DBNAME" "$@"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$rc" != "0" && "$IMPORTED" == "1" && "$ACCEPTED" == "0" \
          && "$ARTIFACT_SCHEMA" == "flashback_import" \
          && "$ARTIFACT_TABLE" =~ ^r_[0-9a-f]{16}$ ]]; then
        printf '%s\n' \
            "SELECT format('DROP TABLE IF EXISTS %I.%I', :'artifact_schema', :'artifact_table') \\gexec" \
            | psql_file \
                --set=artifact_schema="$ARTIFACT_SCHEMA" \
                --set=artifact_table="$ARTIFACT_TABLE" > /dev/null 2>&1 || true
    fi
    if [[ "$rc" != "0" && -n "$REQUEST_ID" && "$ACCEPTED" == "0" ]]; then
        printf '%s\n' \
            "SELECT flashback_fail_backup_restore(:'request_id', :'message', false);" \
            | psql_file \
                --set=request_id="$REQUEST_ID" \
                --set=message="backup restore controller failed (exit $rc)" > /dev/null 2>&1 || true
    fi
    if [[ "$rc" != "0" && "$ACCEPTED" == "1" && "$COMPLETED" == "0" ]]; then
        printf 'request %s remains artifact_ready for inspection/retry; imported table is %s.%s\n' \
            "$REQUEST_ID" "$ARTIFACT_SCHEMA" "$ARTIFACT_TABLE" >&2
    fi
    rm -rf -- "$TMP_DIR"
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n "$REQUEST_FILE" ]]; then
    cp -- "$REQUEST_FILE" "$REQUEST_JSON"
else
    printf '%s\n' \
        "SELECT flashback_prepare_backup_restore(:'target_table', :'target_lsn'::pg_lsn)::text;" \
        | psql_file --set=target_table="$TABLE" --set=target_lsn="$TARGET_LSN" \
        > "$REQUEST_JSON"
    REQUEST_ID="$(jq -er '.request_id' "$REQUEST_JSON")"
    printf '%s\n' "SELECT flashback_claim_backup_restore(:'request_id')::text;" \
        | psql_file --set=request_id="$REQUEST_ID" > "$REQUEST_JSON.claimed"
    cmp -s "$REQUEST_JSON" "$REQUEST_JSON.claimed" || {
        printf 'claimed request differs from the prepared immutable request\n' >&2
        exit 65
    }
fi

REQUEST_ID="$(jq -er '.request_id' "$REQUEST_JSON")"
[[ "$(jq -er '.database' "$REQUEST_JSON")" == "$DBNAME" ]] || {
    printf 'request database does not match --dbname\n' >&2
    exit 65
}

if ! "$HELPER" restore-table --config "$CONFIG" --request "$REQUEST_JSON" \
    > "$RESULT_JSON" 2> "$HELPER_ERROR"; then
    cat "$HELPER_ERROR" >&2
    exit 70
fi

jq -e --arg request_id "$REQUEST_ID" '
    .status == "completed"
    and .cleanup_complete == true
    and .result_format_version >= 3
    and .request.request_id == $request_id
    and .artifact_schema == "flashback_import"
    and (.artifact_table | test("^r_[0-9a-f]{16}$"))
    and (.artifact_sha256 | test("^[0-9a-f]{64}$"))
' "$RESULT_JSON" > /dev/null || {
    printf 'helper result failed controller validation\n' >&2
    exit 65
}

ARTIFACT_SCHEMA="$(jq -er '.artifact_schema' "$RESULT_JSON")"
ARTIFACT_TABLE="$(jq -er '.artifact_table' "$RESULT_JSON")"
ARTIFACT_PATH="$(jq -er '.artifact_path' "$RESULT_JSON")"
EXPECTED_SHA="$(jq -er '.artifact_sha256' "$RESULT_JSON")"
[[ -f "$ARTIFACT_PATH" ]] || { printf 'artifact is missing\n' >&2; exit 66; }
ACTUAL_SHA="$(sha256sum -- "$ARTIFACT_PATH" | awk '{print $1}')"
[[ "$ACTUAL_SHA" == "$EXPECTED_SHA" ]] || {
    printf 'artifact checksum mismatch\n' >&2
    exit 65
}

EXISTING="$(printf '%s\n' \
    "SELECT COALESCE(to_regclass(format('%I.%I', :'artifact_schema', :'artifact_table'))::text, '');" \
    | psql_file --set=artifact_schema="$ARTIFACT_SCHEMA" --set=artifact_table="$ARTIFACT_TABLE")"
[[ -z "$EXISTING" ]] || {
    printf 'artifact table already exists: %s\n' "$EXISTING" >&2
    exit 73
}

"$PG_RESTORE" --exit-on-error --single-transaction --no-owner --no-acl \
    --dbname "$DBNAME" "$ARTIFACT_PATH"
IMPORTED=1

RESULT_COMPACT="$(jq -c . "$RESULT_JSON")"
printf '%s\n' \
    "SELECT flashback_accept_backup_restore(:'request_id', :'result_json'::jsonb);" \
    | psql_file --set=request_id="$REQUEST_ID" --set=result_json="$RESULT_COMPACT" > /dev/null
ACCEPTED=1

RESTORED_OID="$(printf '%s\n' \
    "SELECT flashback_finalize_backup_restore(:'request_id');" \
    | psql_file --set=request_id="$REQUEST_ID")"
COMPLETED=1

jq -n \
    --arg request_id "$REQUEST_ID" \
    --arg restored_oid "$RESTORED_OID" \
    --arg artifact_sha256 "$EXPECTED_SHA" \
    '{status: "completed", request_id: $request_id,
      restored_oid: ($restored_oid | tonumber), artifact_sha256: $artifact_sha256}'

#!/usr/bin/env bash
# Deterministic real-process crash/retry qualification for external_zstd.
set -euo pipefail

BINDIR="${1:-/usr/local/pgsql-17/bin}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT_DIR="${PGFB_EXTZSTD_FAILPOINT_OUT:-$SCRIPT_DIR/../target/qualification/external-zstd-failpoints}"
mkdir -p "$OUT_DIR"

phases=(
    copier_after_staging_create
    copier_during_compression
    copier_after_staged_fsync_before_commit
    copier_after_copy_commit
    copier_after_commit_receipt
    finalizer_after_artifact_seal
    finalizer_after_manifest_fsync
    finalizer_after_publish_rename
    finalizer_after_db_available
    finalizer_after_activation
    restore_during_decode
    retire_after_delete
)

started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
results='[]'
for index in "${!phases[@]}"; do
    phase=${phases[$index]}
    log="$OUT_DIR/$phase.log"
    echo "[external-zstd-failpoints] $((index + 1))/${#phases[@]} $phase"
    if PGFB_RUN_ID=$((41000 + index)) \
       PGFB_EXTZSTD_FAILPOINT_CASE="$phase" \
       "$SCRIPT_DIR/run_external_zstd_artifact_e2e.sh" "$BINDIR" >"$log" 2>&1; then
        grep -q "EXTERNAL_ZSTD_FAILPOINT=PASS case=$phase" "$log" || {
            echo "FAIL: $phase exited zero without its terminal assertion" >&2
            tail -n 80 "$log" >&2
            exit 1
        }
        results=$(jq -c --arg phase "$phase" --arg log "$log" \
            '. + [{phase:$phase,status:"passed",log:$log}]' <<<"$results")
    else
        echo "FAIL: external_zstd failpoint $phase" >&2
        tail -n 100 "$log" >&2
        exit 1
    fi
done

finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n \
    --arg started_at "$started" \
    --arg finished_at "$finished" \
    --arg source_commit "$(git -C "$SCRIPT_DIR/.." rev-parse HEAD)" \
    --argjson results "$results" \
    '{status:"passed",started_at:$started_at,finished_at:$finished_at,source_commit:$source_commit,results:$results}' \
    >"$OUT_DIR/result.json"
echo "EXTERNAL_ZSTD_FAILPOINT_MATRIX=PASS count=${#phases[@]} evidence=$OUT_DIR/result.json"

#!/usr/bin/env bash
# Complete short external_zstd crash, isolation, and namespace qualification.
set -euo pipefail

BINDIR="${1:-/usr/local/pgsql-17/bin}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT_DIR="${PGFB_EXTZSTD_ADVERSARIAL_OUT:-$SCRIPT_DIR/../target/qualification/external-zstd-adversarial}"
mkdir -p "$OUT_DIR"
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

PGFB_EXTZSTD_FAILPOINT_OUT="$OUT_DIR/failpoints" \
    "$SCRIPT_DIR/run_external_zstd_failpoint_matrix.sh" "$BINDIR" \
    >"$OUT_DIR/failpoints.log" 2>&1

PGFB_RUN_ID=44001 \
PGFB_EXTZSTD_ISOLATION=1 \
PGFB_EXTZSTD_PAUSE_CASE=copier_during_compression \
    "$SCRIPT_DIR/run_external_zstd_artifact_e2e.sh" "$BINDIR" \
    >"$OUT_DIR/isolation.log" 2>&1
grep -q '^EXTERNAL_ZSTD_ISOLATION=PASS ' "$OUT_DIR/isolation.log"

PGFB_RUN_ID=44002 \
PGFB_EXTZSTD_MULTIDB=1 \
    "$SCRIPT_DIR/run_external_zstd_artifact_e2e.sh" "$BINDIR" \
    >"$OUT_DIR/multidb.log" 2>&1
grep -q '^EXTERNAL_ZSTD_MULTIDB=PASS ' "$OUT_DIR/multidb.log"

finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n \
    --arg source_commit "$(git -C "$SCRIPT_DIR/.." rev-parse HEAD)" \
    --arg started_at "$started" \
    --arg finished_at "$finished" \
    --arg isolation "$(grep '^EXTERNAL_ZSTD_ISOLATION=PASS ' "$OUT_DIR/isolation.log")" \
    --arg multidb "$(grep '^EXTERNAL_ZSTD_MULTIDB=PASS ' "$OUT_DIR/multidb.log")" \
    --slurpfile failpoints "$OUT_DIR/failpoints/result.json" \
    '{status:"passed",source_commit:$source_commit,started_at:$started_at,finished_at:$finished_at,failpoints:$failpoints[0],isolation:$isolation,multidb:$multidb}' \
    >"$OUT_DIR/result.json"

echo "EXTERNAL_ZSTD_ADVERSARIAL=PASS evidence=$OUT_DIR/result.json"

#!/usr/bin/env bash
# Phase 5 — Unified exact-WAL candidate-archive matrix.
#
# Consolidates accrued exact-WAL DROP / transaction / schema contracts under
# one archive identity. Forbidden techniques:
#   - fake capture triggers writing directly to delta_log
#   - manual consume masking as product evidence
#   - timestamp wall-clock order as proof of COMMIT LSN selection
#
# Required:
#   CANDIDATE_DIR   directory with MANIFEST.json + archives (exact candidate)
#
# Optional:
#   PGFB_MATRIX_SKIP_ADVERSARIAL=1
#   PGFB_MATRIX_SKIP_DROP=1
#   PGFB_MATRIX_SKIP_FUNCTIONAL=1
#
# This is NOT a 24h soak.

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_MATRIX_BASE:-$REPO_ROOT/target/exact-wal-matrix}"
RESULT_JSON="${PGFB_MATRIX_RESULT:-$BASE/results/exact-wal-matrix-$RUN_ID.json}"
mkdir -p "$(dirname "$RESULT_JSON")"

log() { printf '[exact-wal-matrix] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

# Refuse to claim product evidence if this harness itself embeds forbidden patterns.
forbid_scan() {
    local f
    for f in \
        "$REPO_ROOT/scripts/run_exact_wal_transaction_schema_matrix.sh" \
        "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" \
        "$REPO_ROOT/scripts/run_exact_candidate_drop_adversarial.sh" \
        "$REPO_ROOT/scripts/run_exact_candidate_functional_suite.sh"
    do
        [[ -f "$f" ]] || continue
        if grep -Eq '_fb_bench_capture_trigger|INSERT INTO flashback\.delta_log|ATTACH.*TRIGGER.*delta_log' "$f"; then
            die "forbidden fake-trigger / direct-delta technique in $f"
        fi
    done
}

declare -A CASE_STATUS
record() {
    local name=$1 status=$2
    CASE_STATUS["$name"]=$status
    log "$status: $name"
}

forbid_scan
exact_candidate_bind_dir "$CANDIDATE_DIR" || die "failed to bind candidate identity from $CANDIDATE_DIR"
IDENTITY_JSON="$(exact_candidate_identity_json)"

run_child() {
    local name=$1
    shift
    local rc=0
    log "running $name: $*"
    if "$@"; then
        record "$name" "passed"
    else
        rc=$?
        record "$name" "failed"
        return "$rc"
    fi
}

OVERALL=0

if [[ "${PGFB_MATRIX_SKIP_FUNCTIONAL:-0}" != 1 ]]; then
    run_child functional_suite \
        env CANDIDATE_DIR="$CANDIDATE_DIR" \
        "$REPO_ROOT/scripts/run_exact_candidate_functional_suite.sh" || OVERALL=1
else
    record functional_suite skipped
fi

if [[ "${PGFB_MATRIX_SKIP_DROP:-0}" != 1 ]]; then
    run_child drop_qualification \
        env CANDIDATE_DIR="$CANDIDATE_DIR" \
        "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" || OVERALL=1
else
    record drop_qualification skipped
fi

if [[ "${PGFB_MATRIX_SKIP_ADVERSARIAL:-0}" != 1 ]]; then
    run_child drop_adversarial \
        env CANDIDATE_DIR="$CANDIDATE_DIR" \
        "$REPO_ROOT/scripts/run_exact_candidate_drop_adversarial.sh" || OVERALL=1
else
    record drop_adversarial skipped
fi

# Classification ledger (product contract). Values reflect code+suite coverage.
CLASSIFICATION_JSON='{
  "latest_drop_commit_lsn": "supported-preserved",
  "latest_drop_no_silent_fallback": "supported-preserved",
  "ambiguous_coverage_generation": "fail-closed-rejected",
  "timestamp_ux_lsn_wins": "pending-qualification",
  "explicit_disaster_event_id": "supported-with-limit",
  "advanced_lsn": "supported-with-limit",
  "identity_conflict_same_name": "fail-closed-rejected",
  "pre_drop_dependency_manifest": "supported-with-limit",
  "unknown_dependency_fail_closed": "fail-closed-rejected",
  "multi_statement_transaction_dml": "supported-preserved",
  "truncate_alter_disasters": "supported-with-limit",
  "cascade_unproven_classes": "fail-closed-rejected",
  "standby_mutators_refused": "fail-closed-rejected",
  "operation_journal_verified_by_worker": "supported-preserved",
  "two_phase_unprotect": "supported-with-limit",
  "forbidden_fake_triggers": "intentionally-unsupported"
}'

CASES_JSON="$(
  jq -n \
    --arg functional "${CASE_STATUS[functional_suite]:-missing}" \
    --arg dropq "${CASE_STATUS[drop_qualification]:-missing}" \
    --arg adv "${CASE_STATUS[drop_adversarial]:-missing}" \
    '{
      functional_suite:$functional,
      drop_qualification:$dropq,
      drop_adversarial:$adv
    }'
)"

STATUS=failed
[[ "$OVERALL" == 0 ]] && STATUS=passed

jq -n \
  --arg status "$STATUS" \
  --argjson exit_code "$OVERALL" \
  --argjson identity "$IDENTITY_JSON" \
  --argjson cases "$CASES_JSON" \
  --argjson classification "$CLASSIFICATION_JSON" \
  '{
     qualification_kind: "exact_wal_transaction_schema_matrix",
     status: $status,
     exit_code: $exit_code,
     identity: $identity,
     suite_results: $cases,
     classification: $classification,
     forbidden_techniques_absent: true,
     note: "Historical soaks do not qualify this tip; matrix binds to CANDIDATE_DIR identity only."
   }' > "$RESULT_JSON"

log "result: $RESULT_JSON"
exit "$OVERALL"

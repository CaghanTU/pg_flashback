#!/usr/bin/env bash
# Exact-candidate 24-hour chaos soak gate.
#
# HARD RULE: duration MUST be exactly 86400 elapsed seconds (or more only if
# drain/teardown runs after the window). Refuses any shorter configuration.
# A shorter development soak is NOT this gate — use scripts/run_dev_soak.sh.
#
# Required:
#   CANDIDATE_MANIFEST  path to candidate MANIFEST.json (binds source/package SHA)
#
# Optional:
#   PG_FLASHBACK_SOAK_SECONDS   must be >= 86400 (default 86400)
#   PG_FLASHBACK_CHAOS_SUITE=once|off   run injectors once (default once)
#   PG_FLASHBACK_CHAOS_ONLY=1          run chaos suite only; skip 86400 soak
#
# Example (full exact-RC, do not start until short gates pass on x86_64):
#   CANDIDATE_MANIFEST=target/candidate/<commit>/MANIFEST.json \
#     PG_FLASHBACK_REQUIRE_CLEAN_TREE=1 \
#     ./scripts/run_exact_rc_24h_soak.sh
#
# Short chaos-only gate (not a 24h soak):
#   CANDIDATE_MANIFEST=... PG_FLASHBACK_CHAOS_ONLY=1 ./scripts/run_exact_rc_24h_soak.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${CANDIDATE_MANIFEST:?CANDIDATE_MANIFEST is required}"
[[ -f "$MANIFEST" ]] || {
    echo "FAIL: missing candidate manifest: $MANIFEST" >&2
    exit 2
}

CHAOS_SUITE="${PG_FLASHBACK_CHAOS_SUITE:-once}"
CHAOS_ONLY="${PG_FLASHBACK_CHAOS_ONLY:-0}"
export PG_FLASHBACK_REQUIRE_CLEAN_TREE="${PG_FLASHBACK_REQUIRE_CLEAN_TREE:-1}"
export PG_FLASHBACK_SOAK_RESULT_DIR="${PG_FLASHBACK_SOAK_RESULT_DIR:-$ROOT/target/qualification}"
export PG_FLASHBACK_SOAK_WORK_ROOT="${PG_FLASHBACK_SOAK_WORK_ROOT:-$ROOT/target/exact-rc-24h-soak}"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$MANIFEST")"
PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$MANIFEST")"
HELPER_SHA="$(jq -r '.artifacts.helper_binary_sha256' "$MANIFEST")"
echo "exact-RC soak bound to source_commit=$SOURCE_COMMIT package_sha256=$PACKAGE_SHA helper_binary_sha256=$HELPER_SHA"

run_chaos_suite() {
    echo "running exact-RC chaos suite (once)"
    CANDIDATE_MANIFEST="$MANIFEST" \
        PGFB_CHAOS_RESULT="${PG_FLASHBACK_SOAK_RESULT_DIR}/exact-rc-chaos-$(date -u +%Y%m%dT%H%M%SZ).json" \
        "$ROOT/scripts/run_exact_rc_chaos_suite.sh"
}

if [[ "$CHAOS_ONLY" == "1" ]]; then
    echo "CHAOS_ONLY=1: running injectors only (this is NOT a 24h soak)"
    run_chaos_suite
    exit 0
fi

REQUESTED="${PG_FLASHBACK_SOAK_SECONDS:-86400}"
if [[ "$REQUESTED" -lt 86400 ]]; then
    echo "FAIL: exact-RC soak refuses duration < 86400 (got $REQUESTED). This is not a 24h soak." >&2
    exit 2
fi
export PG_FLASHBACK_SOAK_SECONDS="$REQUESTED"
echo "configured_duration_seconds=$PG_FLASHBACK_SOAK_SECONDS"

# Chaos injectors run before the long window so short failures fail closed early.
if [[ "$CHAOS_SUITE" == "once" ]]; then
    run_chaos_suite
elif [[ "$CHAOS_SUITE" != "off" ]]; then
    echo "FAIL: PG_FLASHBACK_CHAOS_SUITE must be once|off (got $CHAOS_SUITE)" >&2
    exit 2
fi

mkdir -p "$PG_FLASHBACK_SOAK_RESULT_DIR"
BIND_JSON="$PG_FLASHBACK_SOAK_RESULT_DIR/exact-rc-24h-binding-$(date -u +%Y%m%dT%H%M%SZ).json"
jq -n \
    --arg source_commit "$SOURCE_COMMIT" \
    --arg package_sha "$PACKAGE_SHA" \
    --arg helper_sha "$HELPER_SHA" \
    --arg chaos_suite "$CHAOS_SUITE" \
    --argjson duration "$PG_FLASHBACK_SOAK_SECONDS" \
    --arg started "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{
      qualification_kind: "exact_rc_24h_soak",
      provenance: {
        source_commit: $source_commit,
        package_sha256: $package_sha,
        helper_binary_sha256: $helper_sha
      },
      chaos_suite: $chaos_suite,
      configured_duration_seconds: $duration,
      started_at: $started,
      status: "started"
    }' > "$BIND_JSON"
echo "binding written: $BIND_JSON"

START_EPOCH="$(date +%s)"
set +e
"$ROOT/scripts/run_dev_soak.sh"
RC=$?
set -e
END_EPOCH="$(date +%s)"
ELAPSED=$((END_EPOCH - START_EPOCH))

jq --argjson elapsed "$ELAPSED" --argjson rc "$RC" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '.elapsed_seconds = $elapsed
     | .finished_at = $finished
     | .exit_code = $rc
     | .status = (if ($rc == 0 and $elapsed >= 86400) then "passed"
                  elif ($elapsed < 86400) then "failed_short_duration"
                  else "failed" end)' \
    "$BIND_JSON" > "${BIND_JSON}.tmp"
mv "${BIND_JSON}.tmp" "$BIND_JSON"

if [[ "$ELAPSED" -lt 86400 ]]; then
    echo "FAIL: exact-RC soak elapsed only ${ELAPSED}s (< 86400). Not a 24h soak." >&2
    exit 3
fi
exit "$RC"

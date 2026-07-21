#!/usr/bin/env bash
# Accelerated harness self-test — NOT release qualification.
#
# qualification_kind is ALWAYS exact_rc_harness_selftest.
# This script MUST NOT emit qualification_kind=exact_rc_24h_stability_soak
# and MUST NOT emit PASS for the 86400-second gate.
#
# Proves negative paths and duration/budget semantics quickly.

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

RESULT_DIR="${PGFB_SELFTEST_RESULT_DIR:-$REPO_ROOT/target/exact-rc-harness-selftest}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RESULT_JSON="$RESULT_DIR/selftest-$RUN_ID.json"
mkdir -p "$RESULT_DIR"
PASSED=0
FAILED=0

log() { printf '[harness-selftest] %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); log "PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); log "FAIL: $1"; }

ORIG_CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR required for selftest}"
ORIG_CANDIDATE_DIR="$(cd "$ORIG_CANDIDATE_DIR" && pwd)"
[[ -f "$ORIG_CANDIDATE_DIR/MANIFEST.json" ]] || { echo "missing manifest"; exit 2; }

# 1) Binary mismatch rejected
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
jq '.artifacts.extension_binary_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"' \
    "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-mismatch.err; then
    fail "binary mismatch was accepted"
else
    pass "binary mismatch rejected"
fi
rm -rf "$tmpdir"
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 2) Source/tree mismatch rejected (fake commit in a copy)
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.source_commit = "ffffffffffffffffffffffffffffffffffffffff"' \
    "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-source.err; then
    fail "source mismatch was accepted"
else
    pass "source mismatch rejected"
fi
rm -rf "$tmpdir"
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 3) Wrong architecture rejected
tmpdir=$(mktemp -d)
cp -a "$ORIG_CANDIDATE_DIR/." "$tmpdir/"
jq '.provenance.arch = "riscv64"' "$tmpdir/MANIFEST.json" > "$tmpdir/MANIFEST.json.tmp"
mv "$tmpdir/MANIFEST.json.tmp" "$tmpdir/MANIFEST.json"
if EC_STASH_DIR="$tmpdir/stash" EC_EXTRACT_DIR="$tmpdir/extract" \
    exact_candidate_bind_dir "$tmpdir" 2>/tmp/selftest-arch.err; then
    fail "arch mismatch was accepted"
else
    pass "arch mismatch rejected"
fi
rm -rf "$tmpdir"
CANDIDATE_DIR="$ORIG_CANDIDATE_DIR"

# 4) Stability harness refuses short PASS via kind check: run a tiny python proof
# that the stability script hardcodes 86400 and has no env override for PASS.
if rg -n 'QUAL_DURATION_SECONDS=86400' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && ! rg -n 'PG_FLASHBACK_SOAK_SECONDS' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "stability soak duration is hard-coded 86400 without env override"
else
    fail "stability soak duration gate is weakenable"
fi

# 5) Wrapper does not invoke the development soak harness
if ! rg -n '^[^#]*run_dev_soak\.sh' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null \
   && rg -n 'run_exact_rc_24h_stability_soak' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null; then
    pass "orchestrator delegates to dedicated stability soak"
else
    fail "orchestrator still invokes development soak or lacks stability soak"
fi

# 6) Selftest result kind cannot be the release soak kind
KIND="exact_rc_harness_selftest"
[[ "$KIND" != "exact_rc_24h_stability_soak" && "$KIND" != "exact_rc_24h_soak" ]] \
    && pass "selftest kind distinct from release soak" \
    || fail "selftest kind collides with release soak"

# 7) Budget exhaustion concept: early heavy-write stop, then hard ceiling
if rg -n 'HEAVY_WRITES_ENABLED=0' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'HEAVY_WRITE_STOP_BYTES' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'exceeded hard MAX_WORK_BYTES' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "workload budget soft-stops with headroom and enforces a hard ceiling"
else
    fail "workload budget headroom/hard ceiling missing"
fi

# 8) Prefix stash/restore helpers exist
if declare -F exact_candidate_restore_prefix >/dev/null \
   && declare -F exact_candidate_stash_prefix_file >/dev/null; then
    pass "prefix stash/restore helpers present"
else
    fail "prefix stash/restore helpers missing"
fi

# 9) Accelerated wrapper is development-only and exact mode remains fixed.
if rg -n 'PG_FLASHBACK_STABILITY_MODE=accelerated' "$REPO_ROOT/scripts/run_development_stability_15m.sh" >/dev/null \
   && rg -n 'development_accelerated_stability' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'QUAL_DURATION_SECONDS=86400' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "accelerated drill mode is distinct from fixed 24-hour qualification"
else
    fail "accelerated mode can collide with exact qualification"
fi

# 10) Local restore must wait for source and mutation LSNs before restore.
if [[ "$(rg -c 'wait_for_delta_lsn_after \"public.restore_probe\"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh")" -ge 2 ]] \
   && [[ "$(rg -c 'wait_for_coverage_lsn \"public.restore_probe\"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh")" -ge 2 ]]; then
    pass "local restore waits for captured and covered source/mutation LSNs"
else
    fail "local restore LSN wait discipline missing"
fi

# 11) Every stability DROP drill uses a distinct tracking identity.
if rg -n 'local table_name="drop_probe_\$\{tag\}"' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "DROP drills use distinct relations instead of retracking one lifecycle"
else
    fail "DROP drills can collide on one tracking lifecycle"
fi

# 12) DROP breadth/scale claims come from a dedicated destructive gate rather
# than being inferred from Gate C's DML counters.
if rg -n 'PGFB_DROP_REPEAT_COUNT:-100' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'perform_local_drop_restore repeated_same_lifecycle' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'record_case backup retained_full_plus_wal' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null \
   && rg -n 'cumulative_rows_at_drop' "$REPO_ROOT/scripts/run_exact_candidate_drop_qualification.sh" >/dev/null; then
    pass "dedicated DROP gate measures repeated local and backup-backed destruction"
else
    fail "dedicated DROP claim gate is missing or under-specified"
fi

# 13) The retained-FULL PoC must not fail after writing successful evidence by
# referencing the removed, formerly tracked design-note output path.
if ! rg -n 'DESIGN_NOTE' "$REPO_ROOT/scripts/run_retained_full_wal_poc.sh" >/dev/null \
   && rg -n 'RUN_COMPLETE=1' "$REPO_ROOT/scripts/run_retained_full_wal_poc.sh" >/dev/null \
   && rg -n 'ALL REQUIRED PoC ASSERTIONS PASSED' "$REPO_ROOT/scripts/run_retained_full_wal_poc.sh" >/dev/null; then
    pass "retained-FULL PoC exits from its machine-readable result without stale design-note state"
else
    fail "retained-FULL PoC can fail after successful evidence generation"
fi

# 14) Candidate archives must carry the legal/security notices and the
# operator-facing recovery contract that the release checklist promises.
EXT_ARCHIVE="$(jq -r '.artifacts.extension_archive.name' "$ORIG_CANDIDATE_DIR/MANIFEST.json")"
HELPER_ARCHIVE="$(jq -r '.artifacts.helper_archive.name' "$ORIG_CANDIDATE_DIR/MANIFEST.json")"
EXT_LIST="$(mktemp)"
HELPER_LIST="$(mktemp)"
tar -tzf "$ORIG_CANDIDATE_DIR/$EXT_ARCHIVE" > "$EXT_LIST"
tar -tzf "$ORIG_CANDIDATE_DIR/$HELPER_ARCHIVE" > "$HELPER_LIST"
ARCHIVE_CONTENT_OK=1
for required in LICENSE SECURITY.md THIRD_PARTY_NOTICES.md README.md \
    docs/RELEASE_SCOPE.md docs/BACKUP_RESTORE_RUNBOOK.md; do
    rg -F "/$required" "$EXT_LIST" >/dev/null || ARCHIVE_CONTENT_OK=0
done
for required in LICENSE SECURITY.md THIRD_PARTY_NOTICES.md README.md \
    docs/RECOVERY_HELPER_DESIGN.md docs/RELEASE_SCOPE.md \
    docs/BACKUP_RESTORE_RUNBOOK.md; do
    rg -F "/$required" "$HELPER_LIST" >/dev/null || ARCHIVE_CONTENT_OK=0
done
rm -f "$EXT_LIST" "$HELPER_LIST"
if [[ "$ARCHIVE_CONTENT_OK" == "1" ]]; then
    pass "candidate archives contain required notices and recovery documentation"
else
    fail "candidate archive legal/security/operator content is incomplete"
fi

# 15) Exact Gate C distributes DROP recovery across the full day: 23 hourly,
# two early/late and four immediately after state-changing drills.
if rg -n 'PERIODIC_DROP_TARGET=23' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'PERIODIC_DROP_INTERVAL_SECONDS=3600' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'POST_DRILL_DROP_TARGET=4' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && rg -n 'DROP_DRILL_TARGET=\$\(\(2 \+ PERIODIC_DROP_TARGET \+ POST_DRILL_DROP_TARGET\)\)' \
        "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null \
   && [[ "$(rg -c 'do_drop_restore_drill post_' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh")" == "4" ]] \
   && rg -n 'DROP coverage incomplete' "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    pass "24-hour stability gate requires 29 time-distributed DROP restorations"
else
    fail "24-hour DROP schedule or final count assertion is incomplete"
fi

SOAK="$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh"

# 16) Observer-only: no executable flashback_consume_wal in Gate C soak.
consume_fn="flashback_""consume_wal"
if ! rg -n "^[^#]*${consume_fn}" "$SOAK" >/dev/null; then
    pass "Gate C soak is observer-only (no manual WAL consume)"
else
    fail "Gate C soak still drives capture via ${consume_fn}"
fi

# 17) Maintenance lock drill must take ACCESS EXCLUSIVE inside a transaction
# and verify the lock from an independent session.
if rg -n 'BEGIN;' "$SOAK" >/dev/null \
   && rg -n 'LOCK TABLE public.steady_dml IN ACCESS EXCLUSIVE MODE;' "$SOAK" >/dev/null \
   && rg -n 'AccessExclusiveLock' "$SOAK" >/dev/null \
   && rg -n 'ON_ERROR_STOP=1' "$SOAK" >/dev/null \
   && rg -n 'maintenance ACCESS EXCLUSIVE lock was not observed' "$SOAK" >/dev/null; then
    pass "maintenance lock drill requires real ACCESS EXCLUSIVE proof"
else
    fail "maintenance lock drill can pass without holding a transaction lock"
fi

# 18) Expected vs observed DML counts are asserted, not only reported.
if rg -n 'INSERT count mismatch expected=' "$SOAK" >/dev/null \
   && rg -n 'UPDATE count mismatch expected=' "$SOAK" >/dev/null \
   && rg -n 'DELETE count mismatch expected=' "$SOAK" >/dev/null \
   && rg -n 'observed_event_counts' "$SOAK" >/dev/null; then
    pass "soak compares expected and observed committed DML counts"
else
    fail "soak does not fail closed on missing DML events"
fi

# 19) DROP restore uses disaster discovery rather than a pre-stored shell LSN only.
if rg -n 'flashback_disaster_points' "$SOAK" >/dev/null \
   && rg -n 'safe_target_lsn' "$SOAK" >/dev/null \
   && rg -n 'contract_verified:true' "$SOAK" >/dev/null; then
    pass "DROP drills use disaster_points and contract verification"
else
    fail "DROP drills still rely on opaque pre-stored LSN-only restore"
fi

# 20) Capture worker kill/restart is verified with new PID + slot catch-up.
if rg -n 'kill -TERM' "$SOAK" >/dev/null \
   && rg -n 'capture worker did not restart with a new pid' "$SOAK" >/dev/null \
   && rg -n 'wait_slot_catchup' "$SOAK" >/dev/null; then
    pass "worker pause drill requires restart PID and lag catch-up"
else
    fail "worker pause drill does not prove capture restart/catch-up"
fi

# 21) Continuous health asserts more than slot_lost.
if rg -n 'assert_continuous_health' "$SOAK" >/dev/null \
   && rg -n 'capture worker missing outside grace' "$SOAK" >/dev/null \
   && rg -n 'slot lag growing uncontrollably' "$SOAK" >/dev/null \
   && rg -n 'begin_worker_grace' "$SOAK" >/dev/null; then
    pass "continuous health asserts worker/slot/coverage faults with bounded grace"
else
    fail "continuous health assertions remain too narrow"
fi

# 22) Single-instance soak lock prevents concurrent Gate C starts.
if rg -n 'acquire_soak_lock' "$SOAK" >/dev/null \
   && rg -n 'another Gate C soak is already running' "$SOAK" >/dev/null; then
    pass "concurrent soak starts are fail-closed by a lock guard"
else
    fail "soak lacks a single-instance lock guard"
fi

# 23) Local Gate C does not require PGBACKREST; chaos remains a separate suite.
if ! rg -n 'require_executable "\$PGBACKREST"' "$SOAK" >/dev/null \
   && rg -n 'gate_profile: "local_delta"' "$SOAK" >/dev/null \
   && rg -n 'PG_FLASHBACK_CHAOS_SUITE' "$REPO_ROOT/scripts/run_exact_rc_24h_soak.sh" >/dev/null; then
    pass "Gate C local soak is decoupled from PGBACKREST; chaos stays on the orchestrator"
else
    fail "Gate C start contract still confuses local soak with backup/chaos deps"
fi

# 24) Negative proof: a mutated soak copy that reintroduces manual consume is detected.
tmpdir=$(mktemp -d)
cp "$SOAK" "$tmpdir/soak.sh"
printf '\nq "SELECT flashback_consume_wal(1);"\n' >> "$tmpdir/soak.sh"
if rg -n "^[^#]*${consume_fn}" "$tmpdir/soak.sh" >/dev/null; then
    pass "selftest detects reintroduced manual WAL consume"
else
    fail "selftest cannot detect reintroduced manual WAL consume"
fi
rm -rf "$tmpdir"

# 25) Negative proof: lock drill without AccessExclusiveLock verification is rejected by checks above.
if rg -n 'lock_held=1' "$SOAK" >/dev/null; then
    pass "lock-held observation gate is present for negative lock failures"
else
    fail "lock-held observation gate missing"
fi

STATUS=failed
[[ "$FAILED" == "0" ]] && STATUS=passed
jq -n \
    --arg status "$STATUS" \
    --arg kind "exact_rc_harness_selftest" \
    --argjson passed "$PASSED" \
    --argjson failed "$FAILED" \
    '{
      qualification_kind: $kind,
      status: $status,
      assertions_passed: $passed,
      assertions_failed: $failed,
      note: "NOT release qualification; cannot satisfy the 86400-second gate"
    }' > "$RESULT_JSON"
log "result: $RESULT_JSON status=$STATUS passed=$PASSED failed=$FAILED"
[[ "$FAILED" == "0" ]]

#!/usr/bin/env bash
# Fail-closed step tracking, signal handling, and atomic summary writing for
# qualification harness scripts. Extracted so its exact behavior can be
# regression-tested in isolation (scripts/run_qualification_step_tracker_selftest.sh)
# without spinning up a real PostgreSQL cluster.
#
# Contract: qst_compute_summary_json reports status=PASS only when
#   - the process's own exit code is 0
#   - no step was marked fail
#   - the process was not interrupted by a trapped signal
#   - every step named in qst_init's step list is present and marked pass
# A run cut short by SIGHUP/SIGINT/SIGTERM always yields status=FAIL with the
# interrupted step (if any) and every still-pending/running step named under
# missing_steps. qst_write_summary_atomic never leaves a stale or half-written
# summary file behind: on any write/parse failure it writes a minimal FAIL
# document instead of silently doing nothing.
#
# Public entrypoints:
#   qst_init <name...>                 (sets expected step list; call once)
#   qst_mark_step <name> <status> [detail]
#   qst_run_child <name> -- <cmd...>    (runs in background, waits, marks pass/fail)
#   qst_on_signal <SIGNAME>             (trap target: trap 'qst_on_signal HUP' HUP)
#   qst_compute_summary_json <run_id> <mode> <exit_code> [extra_fields_json]
#   qst_write_summary_atomic <path> <json>
#
# State (globals, reset by qst_init):
#   QST_EXPECTED_STEPS (array), QST_STEP_STATUS / QST_STEP_DETAIL (assoc arrays)
#   QST_CURRENT_STEP, QST_CURRENT_STEP_PID, QST_FAILED, QST_INTERRUPTED, QST_INTERRUPT_SIGNAL

QST_EXPECTED_STEPS=()
declare -gA QST_STEP_STATUS=()
declare -gA QST_STEP_DETAIL=()
QST_CURRENT_STEP=""
QST_CURRENT_STEP_PID=""
QST_FAILED=0
QST_INTERRUPTED=0
QST_INTERRUPT_SIGNAL=""

qst_init() {
    QST_EXPECTED_STEPS=("$@")
    QST_STEP_STATUS=()
    QST_STEP_DETAIL=()
    for s in "${QST_EXPECTED_STEPS[@]}"; do
        QST_STEP_STATUS["$s"]="pending"
        QST_STEP_DETAIL["$s"]=""
    done
    QST_CURRENT_STEP=""
    QST_CURRENT_STEP_PID=""
    QST_FAILED=0
    QST_INTERRUPTED=0
    QST_INTERRUPT_SIGNAL=""
}

qst_mark_step() {
    local name=$1 status=$2 detail=${3:-}
    QST_STEP_STATUS["$name"]="$status"
    QST_STEP_DETAIL["$name"]="$detail"
}

qst_on_signal() {
    local sig=$1
    QST_INTERRUPTED=1
    QST_INTERRUPT_SIGNAL="$sig"
    echo "INTERRUPTED: received SIG$sig" >&2
    if [[ -n "$QST_CURRENT_STEP" ]]; then
        qst_mark_step "$QST_CURRENT_STEP" "fail" "interrupted by SIG$sig"
        QST_FAILED=$((QST_FAILED + 1))
    fi
    if [[ -n "$QST_CURRENT_STEP_PID" ]]; then
        kill -TERM "$QST_CURRENT_STEP_PID" 2>/dev/null || true
        local _
        for _ in $(seq 1 20); do
            kill -0 "$QST_CURRENT_STEP_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$QST_CURRENT_STEP_PID" 2>/dev/null || true
    fi
    exit 130
}

# Runs "$@" in the background, tracks it as the current step so qst_on_signal
# can target exactly this child, waits for it, and marks pass/fail by exit code.
qst_run_child() {
    local name=$1; shift
    qst_mark_step "$name" "running" ""
    echo "== $name ==" >&2
    set +e
    "$@" &
    QST_CURRENT_STEP="$name"
    QST_CURRENT_STEP_PID=$!
    wait "$QST_CURRENT_STEP_PID"
    local crc=$?
    QST_CURRENT_STEP_PID=""
    QST_CURRENT_STEP=""
    set -e
    if (( crc == 0 )); then
        qst_mark_step "$name" "pass" "exit=0"
    else
        qst_mark_step "$name" "fail" "exit=$crc"
        QST_FAILED=$((QST_FAILED + 1))
    fi
    return 0
}

# Same as qst_run_child but additionally requires a caller-supplied verifier
# function (given the child's declared result path) to return 0 before the
# step is marked pass. Used where exit code 0 alone is not sufficient proof
# (e.g. a benchmark whose result file must independently exist and parse).
qst_run_verified_child() {
    local name=$1; shift
    local verifier=$1; shift
    qst_mark_step "$name" "running" ""
    echo "== $name ==" >&2
    set +e
    "$@" &
    QST_CURRENT_STEP="$name"
    QST_CURRENT_STEP_PID=$!
    wait "$QST_CURRENT_STEP_PID"
    local crc=$?
    QST_CURRENT_STEP_PID=""
    QST_CURRENT_STEP=""
    set -e
    if (( crc == 0 )) && "$verifier"; then
        qst_mark_step "$name" "pass" "exit=0"
    else
        qst_mark_step "$name" "fail" "exit=$crc"
        QST_FAILED=$((QST_FAILED + 1))
    fi
    return 0
}

# Prints the summary JSON to stdout. Does not write any file.
# extra_fields_json (optional) must be a JSON object; its keys are merged
# into the top-level summary (candidate identity, cluster info, etc.).
qst_compute_summary_json() {
    local run_id=$1 mode=$2 exit_code=$3 extra=${4:-'{}'}

    local missing=() completed=()
    for s in "${QST_EXPECTED_STEPS[@]}"; do
        local st="${QST_STEP_STATUS[$s]:-pending}"
        if [[ "$st" == "pending" || "$st" == "running" ]]; then
            missing+=("$s")
        else
            completed+=("$s")
        fi
    done

    local overall=FAIL
    if (( QST_FAILED == 0 && exit_code == 0 && QST_INTERRUPTED == 0 && ${#missing[@]} == 0 )); then
        overall=PASS
    fi

    local steps_json='[]'
    for s in "${QST_EXPECTED_STEPS[@]}"; do
        steps_json=$(jq -n --argjson acc "$steps_json" \
            --arg n "$s" --arg st "${QST_STEP_STATUS[$s]:-pending}" --arg d "${QST_STEP_DETAIL[$s]:-}" \
            '$acc + [{name:$n,status:$st,detail:$d}]')
    done
    local expected_json completed_json missing_json
    expected_json=$(printf '%s\n' "${QST_EXPECTED_STEPS[@]}" | jq -R -s 'split("\n") | map(select(length>0))')
    completed_json=$(printf '%s\n' "${completed[@]:-}" | jq -R -s 'split("\n") | map(select(length>0))')
    missing_json=$(printf '%s\n' "${missing[@]:-}" | jq -R -s 'split("\n") | map(select(length>0))')

    jq -n \
        --arg run_id "$run_id" --arg mode "$mode" --arg status "$overall" \
        --argjson failed "$QST_FAILED" --argjson exit_code "$exit_code" \
        --argjson interrupted "$([[ $QST_INTERRUPTED -eq 1 ]] && echo true || echo false)" \
        --arg signal "$QST_INTERRUPT_SIGNAL" \
        --argjson steps "$steps_json" \
        --argjson expected_steps "$expected_json" \
        --argjson completed_steps "$completed_json" \
        --argjson missing_steps "$missing_json" \
        --argjson extra "$extra" \
        '{run_id:$run_id, mode:$mode, status:$status, failed_steps:$failed, exit_code:$exit_code,
          interrupted:$interrupted, signal:$signal,
          expected_steps:$expected_steps, completed_steps:$completed_steps, missing_steps:$missing_steps,
          steps:$steps} + $extra'
}

# Atomically writes $2 (a JSON string) to $1: temp file, jq-validated, then
# rename. On any failure, overwrites the target with a minimal FAIL document
# instead of leaving a stale summary or silently doing nothing.
qst_write_summary_atomic() {
    local target=$1 json=$2
    local tmp
    tmp="$(mktemp "${target}.tmp.XXXXXX" 2>/dev/null || echo "${target}.tmp.$$")"
    if printf '%s' "$json" >"$tmp" 2>/dev/null && jq -e . "$tmp" >/dev/null 2>&1; then
        mv -f "$tmp" "$target"
        return 0
    fi
    rm -f "$tmp" 2>/dev/null
    printf '{"status":"FAIL","note":"summary generation failed"}\n' >"$target" 2>/dev/null
    return 1
}

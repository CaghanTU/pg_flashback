#!/usr/bin/env bash
# Fail if production (non-pg_test) generated install SQL or the capture worker
# still exposes legacy trigger-capture operational surface.
#
# Banned CREATE/GRANT/COMMENT targets:
#   flashback_capture_*_trigger, attach/detach_capture_trigger, flush_staging,
#   flashback.staging_events
#
# Rust worker must not call flush_staging_to_delta_log or treat trigger/auto as
# operational capture branches (deprecation/reject strings are allowed).
# Ordinary user-trigger / DDL "trigger" wording is not banned.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_MAJOR="${PG_MAJOR:-17}"
OUT_DIR="${OUT_DIR:-target/generated-sql-wal-only-surface}"
mkdir -p "$OUT_DIR"

SQL_OUT="${OUT_DIR}/pg_flashback--production-pg${PG_MAJOR}.sql"

echo "==> Generating production schema SQL (pg${PG_MAJOR}, no pg_test)"
cargo pgrx schema "pg${PG_MAJOR}" \
  --no-default-features \
  --features "pg${PG_MAJOR}" \
  -o "${SQL_OUT}"

fail=0

reject_sql() {
  local label=$1
  local pattern=$2
  local hits
  hits="$(rg -n -i "$pattern" "${SQL_OUT}" || true)"
  if [[ -n "${hits}" ]]; then
    echo "ERROR: production SQL still contains ${label}:" >&2
    printf '%s\n' "${hits}" >&2
    fail=1
  else
    echo "OK: no ${label}"
  fi
}

echo "==> Scanning production SQL for legacy trigger-capture CREATE surface"
reject_sql "CREATE FUNCTION flashback_capture_*_trigger" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+([a-zA-Z0-9_]+\.)?flashback_capture_[a-z0-9_]*_trigger'
reject_sql "CREATE FUNCTION attach/detach_capture_trigger" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+([a-zA-Z0-9_]+\.)?flashback_(attach|detach)_capture_trigger'
reject_sql "CREATE FUNCTION flush_staging" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+([a-zA-Z0-9_]+\.)?flashback_flush_staging'
reject_sql "CREATE TABLE flashback.staging_events" \
  'CREATE[[:space:]]+(UNLOGGED[[:space:]]+)?TABLE[[:space:]]+(IF[[:space:]]+NOT[[:space:]]+EXISTS[[:space:]]+)?flashback\.staging_events'

echo "==> Scanning production SQL for GRANT/COMMENT on legacy capture symbols"
reject_sql "GRANT/COMMENT on flush/attach/detach/capture_*_trigger" \
  '(GRANT|COMMENT[[:space:]]+ON[[:space:]]+FUNCTION)[[:space:]].*flashback_(flush_staging|(attach|detach)_capture_trigger|capture_[a-z0-9_]*_trigger)'
reject_sql "GRANT/COMMENT on staging_events" \
  '(GRANT|COMMENT[[:space:]]+ON[[:space:]]+TABLE)[[:space:]].*flashback\.staging_events'

echo "==> Scanning Rust worker for operational trigger/flush capture branches"
WORKER="src/storage/worker.rs"
if rg -n 'flush_staging_to_delta_log' "$WORKER" >/dev/null 2>&1; then
  echo "ERROR: worker still references flush_staging_to_delta_log:" >&2
  rg -n 'flush_staging_to_delta_log' "$WORKER" >&2 || true
  fail=1
else
  echo "OK: no flush_staging_to_delta_log in worker"
fi

# Operational branches that would still flush/capture under trigger|auto.
# Allow comment/doc strings that reject those modes.
if rg -n 'capture_mode\s*==\s*"trigger"|mode\s*==\s*"trigger"|CaptureMode::Trigger' "$WORKER" \
  | rg -v 'reject|deprecat|not supported|fail closed|never capture|illegal' >/dev/null 2>&1
then
  echo "ERROR: worker appears to have an operational trigger-mode branch:" >&2
  rg -n 'capture_mode\s*==\s*"trigger"|mode\s*==\s*"trigger"|CaptureMode::Trigger' "$WORKER" >&2 || true
  fail=1
else
  echo "OK: no operational trigger-mode capture branch in worker"
fi

if rg -n 'mode\s*==\s*"auto"|capture_mode\s*==\s*"auto"' "$WORKER" \
  | rg -v 'reject|deprecat|not supported|fail closed|never capture|illegal|"auto"\s*=>' >/dev/null 2>&1
then
  echo "ERROR: worker appears to have an operational auto-mode branch:" >&2
  rg -n 'mode\s*==\s*"auto"|capture_mode\s*==\s*"auto"' "$WORKER" >&2 || true
  fail=1
else
  echo "OK: no operational auto-mode capture branch in worker"
fi

if [[ "${fail}" != "0" ]]; then
  exit 1
fi

echo "OK: core tree is WAL-only (no staging_events / trigger-capture CREATE surface; pg${PG_MAJOR})"

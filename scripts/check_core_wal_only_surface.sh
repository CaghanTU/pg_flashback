#!/usr/bin/env bash
# Fail if production (non-pg_test) generated install SQL or the capture worker
# still exposes legacy trigger-capture operational surface, or if WAL-only
# migration cleanup uses unsafe ownership/wildcard/CASCADE patterns.
#
# Banned CREATE/GRANT/COMMENT targets:
#   flashback_capture_*_trigger, attach/detach_capture_trigger, flush_staging,
#   flashback.staging_events
#
# Migration safety (source + generated SQL):
#   no tgname LIKE 'flashback_capture_%' cleanup
#   no DROP FUNCTION ... CASCADE for legacy capture symbols
#   no DROP TABLE flashback.staging_events CASCADE
#   no forked finalize algorithm in test_wal_seam.sql
#   finalize routine must REVOKE from PUBLIC/admin/monitor
#
# Rust worker must not call flush_staging_to_delta_log or treat trigger/auto as
# operational capture branches (deprecation/reject strings are allowed).
# Ordinary user-trigger / DDL "trigger" wording is not banned.
# Migration-only DROP of legacy capture symbols is not treated as CREATE surface.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_MAJOR="${PG_MAJOR:-17}"
OUT_DIR="${OUT_DIR:-target/generated-sql-wal-only-surface}"
mkdir -p "$OUT_DIR"

SQL_OUT="${OUT_DIR}/pg_flashback--production-pg${PG_MAJOR}.sql"
MIG_SRC="sql/functions/wal_only_migration.sql"
BOOT_SRC="sql/functions/schema_bootstrap.sql"
SEAM_SRC="sql/functions/test_wal_seam.sql"

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

reject_file() {
  local label=$1
  local file=$2
  local pattern=$3
  local hits
  hits="$(rg -n -i "$pattern" "${file}" || true)"
  if [[ -n "${hits}" ]]; then
    echo "ERROR: ${file} still contains ${label}:" >&2
    printf '%s\n' "${hits}" >&2
    fail=1
  else
    echo "OK: ${file}: no ${label}"
  fi
}

require_file() {
  local label=$1
  local file=$2
  local pattern=$3
  if ! rg -n -i "$pattern" "${file}" >/dev/null 2>&1; then
    echo "ERROR: ${file} missing required ${label} (pattern: ${pattern})" >&2
    fail=1
  else
    echo "OK: ${file}: has ${label}"
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

echo "==> Scanning migration sources for unsafe ownership/wildcard/CASCADE cleanup"
reject_file "wildcard tgname LIKE flashback_capture_% cleanup" "${MIG_SRC}" \
  "tgname[[:space:]]+LIKE[[:space:]]+'flashback_capture_%'"
reject_file "wildcard tgname LIKE flashback_capture_% cleanup" "${BOOT_SRC}" \
  "tgname[[:space:]]+LIKE[[:space:]]+'flashback_capture_%'"
reject_file "DROP FUNCTION ... CASCADE (legacy cleanup)" "${MIG_SRC}" \
  'DROP[[:space:]]+FUNCTION[^;]*CASCADE'
reject_file "DROP TABLE staging_events CASCADE" "${MIG_SRC}" \
  'DROP[[:space:]]+TABLE[^;]*flashback\.staging_events[^;]*CASCADE'
reject_file "DROP TABLE staging_events CASCADE" "${BOOT_SRC}" \
  'DROP[[:space:]]+TABLE[^;]*flashback\.staging_events[^;]*CASCADE'
reject_file "wildcard LIKE trigger cleanup" "${MIG_SRC}" \
  "LIKE[[:space:]]+'flashback_capture_%'"

# Generated production SQL must not ship unsafe cleanup either.
reject_sql "generated wildcard tgname LIKE flashback_capture_%" \
  "tgname[[:space:]]+LIKE[[:space:]]+'flashback_capture_%'"
reject_sql "generated DROP FUNCTION ... CASCADE on legacy capture symbols" \
  'DROP[[:space:]]+FUNCTION[^;]*flashback_(flush_staging|(attach|detach)_capture_trigger|capture_[a-z0-9_]*_trigger)[^;]*CASCADE'
reject_sql "generated DROP TABLE staging_events CASCADE" \
  'DROP[[:space:]]+TABLE[^;]*flashback\.staging_events[^;]*CASCADE'

echo "==> Verifying centralized finalize routine + privilege lockdown"
require_file "finalize routine definition" "${MIG_SRC}" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\('
require_file "ownership helper" "${MIG_SRC}" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+flashback_internal_is_pg_flashback_member\('
require_file "REVOKE finalize FROM PUBLIC" "${MIG_SRC}" \
  'REVOKE[[:space:]]+ALL[[:space:]]+ON[[:space:]]+FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)[[:space:]]+FROM[[:space:]]+PUBLIC'
require_file "REVOKE finalize FROM flashback_admin" "${MIG_SRC}" \
  'REVOKE[[:space:]]+ALL[[:space:]]+ON[[:space:]]+FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)[[:space:]]+FROM[[:space:]]+flashback_admin'
require_file "REVOKE finalize FROM pg_monitor" "${MIG_SRC}" \
  'REVOKE[[:space:]]+ALL[[:space:]]+ON[[:space:]]+FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)[[:space:]]+FROM[[:space:]]+pg_monitor'
require_file "finalize invocation at load" "${MIG_SRC}" \
  'SELECT[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)'

require_file "generated finalize definition" "${SQL_OUT}" \
  'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\('
require_file "generated REVOKE finalize FROM PUBLIC" "${SQL_OUT}" \
  'REVOKE[[:space:]]+ALL[[:space:]]+ON[[:space:]]+FUNCTION[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)[[:space:]]+FROM[[:space:]]+PUBLIC'

# No GRANT EXECUTE on finalize / ownership helper to PUBLIC/admin/monitor.
if rg -n -i \
  'GRANT[[:space:]]+.*ON[[:space:]]+FUNCTION[[:space:]].*flashback_internal_(finalize_wal_only_upgrade|is_pg_flashback_member)' \
  "${SQL_OUT}" "${MIG_SRC}" | rg -iv 'REVOKE' >/dev/null 2>&1
then
  echo "ERROR: GRANT on internal finalize/ownership helper is forbidden:" >&2
  rg -n -i \
    'GRANT[[:space:]]+.*ON[[:space:]]+FUNCTION[[:space:]].*flashback_internal_(finalize_wal_only_upgrade|is_pg_flashback_member)' \
    "${SQL_OUT}" "${MIG_SRC}" >&2 || true
  fail=1
else
  echo "OK: no GRANT on internal finalize/ownership helper"
fi

echo "==> Verifying test seam does not fork the migration algorithm"
require_file "test wrapper calls production finalize" "${SEAM_SRC}" \
  'PERFORM[[:space:]]+flashback_internal_finalize_wal_only_upgrade\(\)'
reject_file "forked DROP TRIGGER flashback_capture_* in test seam" "${SEAM_SRC}" \
  'DROP[[:space:]]+TRIGGER[[:space:]]+.*flashback_capture_'
reject_file "forked DROP FUNCTION capture/flush/attach in test seam" "${SEAM_SRC}" \
  'DROP[[:space:]]+FUNCTION[[:space:]]+.*flashback_(flush_staging|(attach|detach)_capture_trigger|capture_[a-z0-9_]*_trigger)'
reject_file "forked DROP TABLE staging_events in test seam" "${SEAM_SRC}" \
  'DROP[[:space:]]+TABLE[[:space:]]+.*flashback\.staging_events'
reject_file "wildcard LIKE cleanup in test seam" "${SEAM_SRC}" \
  "tgname[[:space:]]+LIKE[[:space:]]+'flashback_capture_%'"
reject_file "forked finalize allowlist arrays in test seam" "${SEAM_SRC}" \
  "v_trigger_names|v_capture_fn_oids|v_fn_sigs|flashback_internal_is_pg_flashback_member"

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

echo "OK: core tree is WAL-only with ownership-safe migration (pg${PG_MAJOR})"

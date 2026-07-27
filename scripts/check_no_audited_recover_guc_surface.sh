#!/usr/bin/env bash
# Fail if the user-settable pg_flashback.audited_recover_operation_id GUC
# ever comes back. It was removed because a Userset GUC of that name let any
# role holding EXECUTE on flashback_recover_execute set it directly via
# SET/set_config() and dress up an arbitrary flashback_restore_lsn call as
# though it were the audited output of some other operation_id. The audited
# operation_id is now a backend-local Rust execution context
# (flashback_internal_set/get/clear_audited_recover_context in
# src/runtime_guard.rs), never a GUC.
#
# Scans: the generated production install SQL, src/*.rs, and scripts/*.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_MAJOR="${PG_MAJOR:-17}"
OUT_DIR="${OUT_DIR:-target/generated-sql-no-audited-recover-guc-surface}"
mkdir -p "$OUT_DIR"
SQL_OUT="${OUT_DIR}/pg_flashback--production-pg${PG_MAJOR}.sql"

echo "==> Generating core install SQL (pg${PG_MAJOR})"
cargo pgrx schema "pg${PG_MAJOR}" \
  --no-default-features \
  --features "pg${PG_MAJOR}" \
  -o "${SQL_OUT}"

marker='pg_flashback.audited_recover_operation_id'
fail=0
SELF="$(basename "$0")"

scan() { # label file-or-dir
  local label=$1 target=$2 hits
  if [[ -d "$target" ]]; then
    hits="$(grep -rniF --exclude="$SELF" -- "$marker" "$target" 2>/dev/null || true)"
  else
    hits="$(grep -niF -- "$marker" "$target" 2>/dev/null || true)"
  fi
  if [[ -n "$hits" ]]; then
    echo "FAIL: forbidden GUC marker '$marker' in ${label}:" >&2
    printf '%s\n' "$hits" | sed 's/^/  /' >&2
    fail=1
  fi
}

scan "generated core SQL" "$SQL_OUT"
scan "core Rust (src/)" "src"
scan "packaging scripts (scripts/)" "scripts"

if [[ "$fail" -ne 0 ]]; then
  echo "FAIL: the user-settable audited-recover GUC surface has come back." >&2
  exit 1
fi

echo "OK: no user-settable audited-recover GUC surface (pg${PG_MAJOR})."

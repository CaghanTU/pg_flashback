#!/usr/bin/env bash
# Fail if the supported core tree carries any pgBackRest-backed recovery
# surface. The experimental backup prototype is deferred, not shipped
# (docs/DEFERRED_BACKUP.md); this keeps it from silently creeping back into
# the generated install SQL, the core Rust, or the packaging scripts.
#
# Scans: the generated core install SQL, src/*.rs, and scripts/*.sh (the
# packaging surface). The single deferred-history note in docs/ is exempt by
# construction — docs/ is not scanned.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_MAJOR="${PG_MAJOR:-17}"
OUT_DIR="${OUT_DIR:-target/generated-sql-no-backup-surface}"
mkdir -p "$OUT_DIR"
SQL_OUT="${OUT_DIR}/pg_flashback--production-pg${PG_MAJOR}.sql"

echo "==> Generating core install SQL (pg${PG_MAJOR})"
cargo pgrx schema "pg${PG_MAJOR}" \
  --no-default-features \
  --features "pg${PG_MAJOR}" \
  -o "${SQL_OUT}"

# Fixed markers that must never appear in the supported core surface.
markers=(
  'pgbackrest'
  'flashback_track_backup'
  'flashback_capture_backup_ddl_marker'
  'backup_anchors'
  'verified_backup_proofs'
  'backup_expire_leases'
  'backup_restore_requests'
  "recovery_profile = 'backup'"
  'proof_hmac_key_file'
)

fail=0
SELF="$(basename "$0")"

scan() { # label file-or-dir
  local label=$1 target=$2 m hits
  for m in "${markers[@]}"; do
    if [[ -d "$target" ]]; then
      # Exclude this checker: its marker list is the definition, not a usage.
      hits="$(grep -rniF --exclude="$SELF" -- "$m" "$target" 2>/dev/null || true)"
    else
      hits="$(grep -niF -- "$m" "$target" 2>/dev/null || true)"
    fi
    if [[ -n "$hits" ]]; then
      echo "FAIL: forbidden backup marker '$m' in ${label}:" >&2
      printf '%s\n' "$hits" | sed 's/^/  /' >&2
      fail=1
    fi
  done
}

scan "generated core SQL" "$SQL_OUT"
scan "core Rust (src/)" "src"
scan "packaging scripts (scripts/)" "scripts"

if [[ "$fail" -ne 0 ]]; then
  echo "FAIL: supported core tree still carries backup-recovery surface." >&2
  exit 1
fi

echo "OK: core tree is free of the deferred backup-recovery surface (pg${PG_MAJOR})."

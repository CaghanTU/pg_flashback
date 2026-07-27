#!/usr/bin/env bash
# Retention eligibility is identity/need based.  Normal runtime must never
# revive the legacy trigger-age deletion path or scan generation payload
# contents merely to decide whether those contents may be removed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FILE="$ROOT/sql/functions/retention_runtime.sql"

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

if rg -n -U \
  'SELECT\s+(count\s*\(\s*\*\s*\)|count\s*\([^)]*\)|min\s*\(\s*commit_lsn\s*\)|max\s*\(\s*commit_lsn\s*\))[\s\S]{0,240}FROM\s+flashback\.(delta_log|schema_versions)' \
  "$FILE"; then
  fail "retention runtime performs a payload content scan"
fi

if rg -n 'flashback_internal_snapshot_retire_legacy|Legacy trigger lifecycles' "$FILE"; then
  fail "legacy trigger-lifecycle cleanup is reachable from normal retention runtime"
fi

echo "OK: retention runtime is generation-only and identity/need based"

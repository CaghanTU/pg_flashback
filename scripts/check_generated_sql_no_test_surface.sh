#!/usr/bin/env bash
# Fail if production (non-pg_test) generated install SQL contains test-only
# functions or test semantics (name-prefix bypasses, synthetic-stream branches).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_MAJOR="${PG_MAJOR:-17}"
OUT_DIR="${OUT_DIR:-target/generated-sql-no-test-surface}"
mkdir -p "$OUT_DIR"

SQL_OUT="${OUT_DIR}/pg_flashback--production-pg${PG_MAJOR}.sql"

echo "==> Generating production schema SQL (pg${PG_MAJOR}, no pg_test)"
cargo pgrx schema "pg${PG_MAJOR}" \
  --no-default-features \
  --features "pg${PG_MAJOR}" \
  -o "${SQL_OUT}"

fail=0

echo "==> Scanning for CREATE ... FUNCTION ... flashback_test_*"
hits="$(
  rg -n -i \
    'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?FUNCTION[[:space:]]+([a-zA-Z0-9_]+\.)?flashback_test_' \
    "${SQL_OUT}" || true
)"
count=0
if [[ -n "${hits}" ]]; then
  count="$(printf '%s\n' "${hits}" | wc -l | tr -d ' ')"
fi
echo "flashback_test_* CREATE FUNCTION count: ${count}"
if [[ "${count}" != "0" ]]; then
  echo "ERROR: production generated SQL must not define flashback_test_* functions:" >&2
  printf '%s\n' "${hits}" >&2
  fail=1
fi

echo "==> Scanning for test-semantics markers"
# Markers that indicate test-only bypass/branch semantics leaked into production.
MARKER_PATTERN='flashback_test_|pg_flashback_test_|active_test_synthetic_slot|test_synthetic'
marker_hits="$(rg -n -i "${MARKER_PATTERN}" "${SQL_OUT}" || true)"
marker_count=0
if [[ -n "${marker_hits}" ]]; then
  marker_count="$(printf '%s\n' "${marker_hits}" | wc -l | tr -d ' ')"
fi
echo "test-semantics marker count: ${marker_count}"
if [[ "${marker_count}" != "0" ]]; then
  echo "ERROR: production generated SQL must not contain test-semantics markers:" >&2
  printf '%s\n' "${marker_hits}" >&2
  fail=1
fi

if [[ "${fail}" != "0" ]]; then
  exit 1
fi

echo "OK: no flashback_test_* functions or test-semantics markers in production generated SQL (${SQL_OUT})"

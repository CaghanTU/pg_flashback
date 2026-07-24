#!/usr/bin/env bash
# Fail if production (non-pg_test) generated install SQL defines flashback_test_*.
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
  exit 1
fi

echo "OK: no flashback_test_* functions in production generated SQL (${SQL_OUT})"

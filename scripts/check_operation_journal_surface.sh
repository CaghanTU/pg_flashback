#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if rg -n --glob '*.sql' \
    'INSERT[[:space:]]+INTO[[:space:]]+(flashback\.)?restore_log' \
    sql/functions; then
    echo "ERROR: restore_log must remain a read-only projection of operation_events" >&2
    exit 1
fi

if rg -n --glob '*.sql' \
    'CREATE[[:space:]]+TABLE([[:space:]]+IF[[:space:]]+NOT[[:space:]]+EXISTS)?[[:space:]]+flashback\.restore_log' \
    sql/functions; then
    echo "ERROR: production SQL recreates the legacy writable restore_log table" >&2
    exit 1
fi

rg -q \
    'CREATE OR REPLACE VIEW flashback\.restore_log AS' \
    sql/functions/operation_journal.sql

echo "operation journal surface: OK"

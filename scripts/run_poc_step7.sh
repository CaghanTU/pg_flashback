#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

cargo pgrx install --no-default-features -F pg17
cargo pgrx start pg17

/usr/local/pgsql-17/bin/psql \
  -h /home/caghan.linux/.pgrx \
  -p 28817 \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET wal_level = 'logical';" \
  -c "ALTER SYSTEM SET max_replication_slots = 10;"

cargo pgrx stop pg17
cargo pgrx start pg17

/usr/local/pgsql-17/bin/psql \
  -h /home/caghan.linux/.pgrx \
  -p 28817 \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -c "DROP EXTENSION IF EXISTS pg_flashback CASCADE;" \
  -c "CREATE EXTENSION pg_flashback;"

/usr/local/pgsql-17/bin/psql \
  -h /home/caghan.linux/.pgrx \
  -p 28817 \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -f sql/poc_step7_ddl_restore.sql

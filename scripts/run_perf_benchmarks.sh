#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

DATA_DIR="/home/caghan.linux/.pgrx/data-17"
PG_BIN="/usr/local/pgsql-17/bin"
PG_OPTS="-i -p 28817 -c unix_socket_directories=/home/caghan.linux/.pgrx -c shared_preload_libraries=pg_flashback"

stop_pg() {
  "${PG_BIN}/pg_ctl" stop -D "${DATA_DIR}" -m fast >/dev/null 2>&1 || true
}

start_pg() {
  "${PG_BIN}/pg_ctl" start -D "${DATA_DIR}" -o "${PG_OPTS}" -l "/home/caghan.linux/.pgrx/17.log"
}

cargo pgrx install --no-default-features -F pg17
stop_pg

if [[ -f "${DATA_DIR}/postmaster.pid" ]]; then
  stale_pid="$(head -n 1 "${DATA_DIR}/postmaster.pid" || true)"
  if [[ -z "${stale_pid}" ]] || ! ps -p "${stale_pid}" >/dev/null 2>&1; then
    rm -f "${DATA_DIR}/postmaster.pid"
  fi
fi

start_pg

/usr/local/pgsql-17/bin/psql \
  -h /home/caghan.linux/.pgrx \
  -p 28817 \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET wal_level = 'logical';" \
  -c "ALTER SYSTEM SET max_replication_slots = 20;"

stop_pg
start_pg

/usr/local/pgsql-17/bin/psql \
  -h /home/caghan.linux/.pgrx \
  -p 28817 \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -f sql/poc_perf_benchmarks.sql

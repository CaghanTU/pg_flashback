#!/bin/bash
# pg_flashback — Write Overhead Benchmark
# Measures DML performance with and without flashback capture enabled.
# Usage: ./scripts/run_benchmark.sh [port] [socket_dir]

set -euo pipefail

PORT="${1:-28817}"
SOCKDIR="${2:-$HOME/.pgrx}"

# Detect psql/pgbench: prefer pgrx-installed PG17, fall back to PATH
_PGRX_BIN="$HOME/.pgrx/17.*/pgrx-install/bin"
_RESOLVED=$(echo $_PGRX_BIN 2>/dev/null | tr ' ' '\n' | head -1)
if [[ -d "$_RESOLVED" ]]; then
  PSQL_BIN="$_RESOLVED/psql"
  PGBENCH_BIN="$_RESOLVED/pgbench"
else
  PSQL_BIN="$(command -v psql)"
  PGBENCH_BIN="$(command -v pgbench || true)"
fi
PSQL="$PSQL_BIN -h $SOCKDIR -p $PORT -d postgres -v ON_ERROR_STOP=on"
PGBENCH="$PGBENCH_BIN -h $SOCKDIR -p $PORT -d postgres"

echo "═══════════════════════════════════════════════════"
echo "  pg_flashback — Write Overhead Benchmark"
echo "  Port: $PORT   Socket: $SOCKDIR"
echo "═══════════════════════════════════════════════════"
echo ""

# ── Scenario 1: Bulk INSERT ──────────────────────────────────────────

echo "─── Scenario 1: Bulk INSERT 100K rows ───"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bench_bulk CASCADE;
CREATE TABLE bench_bulk (id serial PRIMARY KEY, val integer, data text);
SQL

# Without flashback
echo -n "  WITHOUT flashback: "
$PSQL -q -c "TRUNCATE bench_bulk;"
$PSQL -c "\timing on" -c "INSERT INTO bench_bulk (val, data) SELECT g, repeat('x',50) FROM generate_series(1,100000) g;" 2>&1 | grep "Time:"

# With flashback
$PSQL -q -c "SELECT flashback_track('bench_bulk');" -c "SELECT pg_sleep(0.5);"
echo -n "  WITH    flashback: "
$PSQL -q -c "TRUNCATE bench_bulk;"
$PSQL -c "\timing on" -c "INSERT INTO bench_bulk (val, data) SELECT g, repeat('x',50) FROM generate_series(1,100000) g;" 2>&1 | grep "Time:"

$PSQL -q -c "SELECT flashback_untrack('bench_bulk');"
$PSQL -q -c "DROP TABLE bench_bulk CASCADE;"
echo ""

# ── Scenario 2: Single-row INSERT/UPDATE/DELETE throughput ───────────

echo "─── Scenario 2: Single-row DML (10K iterations) ───"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bench_single CASCADE;
CREATE TABLE bench_single (id serial PRIMARY KEY, val integer, data text);
INSERT INTO bench_single (val, data) SELECT g, 'initial' FROM generate_series(1,10000) g;
SQL

# Without flashback
echo -n "  WITHOUT flashback: "
$PSQL -c "\timing on" -c "
DO \$\$
BEGIN
    FOR i IN 1..10000 LOOP
        UPDATE bench_single SET val = val + 1, data = 'upd' WHERE id = i;
    END LOOP;
END;
\$\$;
" 2>&1 | grep "Time:"

# With flashback
$PSQL -q -c "SELECT flashback_track('bench_single');" -c "SELECT pg_sleep(0.5);"
$PSQL -q -c "UPDATE bench_single SET val = 0, data = 'reset';"
echo -n "  WITH    flashback: "
$PSQL -c "\timing on" -c "
DO \$\$
BEGIN
    FOR i IN 1..10000 LOOP
        UPDATE bench_single SET val = val + 1, data = 'upd' WHERE id = i;
    END LOOP;
END;
\$\$;
" 2>&1 | grep "Time:"

$PSQL -q -c "SELECT flashback_untrack('bench_single');"
$PSQL -q -c "DROP TABLE bench_single CASCADE;"
echo ""

# ── Scenario 3: Mixed DML workload ──────────────────────────────────

echo "─── Scenario 3: Mixed INSERT+UPDATE+DELETE (5K each) ───"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bench_mixed CASCADE;
CREATE TABLE bench_mixed (id serial PRIMARY KEY, val integer, data text);
SQL

# Without flashback
echo -n "  WITHOUT flashback: "
$PSQL -c "\timing on" -c "
DO \$\$
BEGIN
    FOR i IN 1..5000 LOOP
        INSERT INTO bench_mixed (val, data) VALUES (i, 'new');
    END LOOP;
    FOR i IN 1..5000 LOOP
        UPDATE bench_mixed SET data = 'updated' WHERE id = i;
    END LOOP;
    DELETE FROM bench_mixed WHERE id <= 2500;
END;
\$\$;
" 2>&1 | grep "Time:"

$PSQL -q -c "TRUNCATE bench_mixed;"

# With flashback
$PSQL -q -c "SELECT flashback_track('bench_mixed');" -c "SELECT pg_sleep(0.5);"
echo -n "  WITH    flashback: "
$PSQL -c "\timing on" -c "
DO \$\$
BEGIN
    FOR i IN 1..5000 LOOP
        INSERT INTO bench_mixed (val, data) VALUES (i, 'new');
    END LOOP;
    FOR i IN 1..5000 LOOP
        UPDATE bench_mixed SET data = 'updated' WHERE id = i;
    END LOOP;
    DELETE FROM bench_mixed WHERE id <= 2500;
END;
\$\$;
" 2>&1 | grep "Time:"

$PSQL -q -c "SELECT flashback_untrack('bench_mixed');"
$PSQL -q -c "DROP TABLE bench_mixed CASCADE;"
echo ""

# ── Scenario 4: Wide table (15 columns) ────────────────────────────

echo "─── Scenario 4: Wide table UPDATE (15 cols, 5K rows) ───"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bench_wide CASCADE;
CREATE TABLE bench_wide (
    id serial PRIMARY KEY,
    c1 text, c2 text, c3 text, c4 integer, c5 integer,
    c6 numeric(12,4), c7 boolean, c8 timestamptz DEFAULT now(),
    c9 jsonb, c10 text, c11 integer, c12 numeric,
    c13 boolean, c14 text, c15 text
);
INSERT INTO bench_wide (c1,c2,c3,c4,c5,c6,c7,c9,c10,c11,c12,c13,c14,c15)
SELECT 'a','b','c',g,g*2,g*1.5,true,'{"k":1}','d',g,g*3.0,false,'e','f'
FROM generate_series(1,5000) g;
SQL

# Without flashback
echo -n "  WITHOUT flashback: "
$PSQL -c "\timing on" -c "UPDATE bench_wide SET c1='x', c4=c4+1, c6=c6+0.1, c9='{\"updated\":true}' WHERE id <= 5000;" 2>&1 | grep "Time:"

$PSQL -q -c "UPDATE bench_wide SET c1='a', c4=id, c6=id*1.5, c9='{\"k\":1}';"

# With flashback
$PSQL -q -c "SELECT flashback_track('bench_wide');" -c "SELECT pg_sleep(0.5);"
echo -n "  WITH    flashback: "
$PSQL -c "\timing on" -c "UPDATE bench_wide SET c1='x', c4=c4+1, c6=c6+0.1, c9='{\"updated\":true}' WHERE id <= 5000;" 2>&1 | grep "Time:"

$PSQL -q -c "SELECT flashback_untrack('bench_wide');"
$PSQL -q -c "DROP TABLE bench_wide CASCADE;"
echo ""

# ── Summary ─────────────────────────────────────────────────────────

echo "═══════════════════════════════════════════════════"
echo "  Benchmark complete."
echo "  Compare WITH vs WITHOUT times above to determine overhead."
echo "═══════════════════════════════════════════════════"

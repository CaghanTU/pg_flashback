#!/bin/bash
# pg_flashback — Capture Mode Comparison Benchmark
# Compares: Baseline vs Trigger mode vs WAL mode
# Each scenario runs 3x to reduce variance, reports median.
#
# Usage: ./scripts/run_mode_comparison.sh [port] [socket_dir]

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

# Extract timing value from psql \timing output (ms float)
extract_ms() {
    grep "Time:" | tail -1 | awk '{print $2}' | tr -d ','
}

# Run a SQL statement 3 times, print median time
bench_3x() {
    local label="$1"
    local sql="$2"
    local t1 t2 t3
    t1=$($PSQL -c "\timing on" -c "$sql" 2>&1 | extract_ms)
    t2=$($PSQL -c "\timing on" -c "$sql" 2>&1 | extract_ms)
    t3=$($PSQL -c "\timing on" -c "$sql" 2>&1 | extract_ms)
    # Median of 3
    local median
    median=$(echo "$t1 $t2 $t3" | tr ' ' '\n' | sort -n | sed -n '2p')
    printf "  %-26s %8.1f ms  (%.1f / %.1f / %.1f)\n" "$label" "$median" "$t1" "$t2" "$t3"
}

force_mode() {
    $PSQL -q -c "ALTER SYSTEM SET pg_flashback.capture_mode = '$1'" -c "SELECT pg_reload_conf();" > /dev/null
}

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║   pg_flashback — Capture Mode Comparison (Trigger vs WAL)    ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo "  3 runs per measurement, showing median (run1/run2/run3)"
echo ""

# ──────────────────────────────────────────────────────────────────────
# SCENARIO 1: Bulk INSERT 100K rows
# ──────────────────────────────────────────────────────────────────────
echo "━━━ Scenario 1: Bulk INSERT 100K rows ━━━━━━━━━━━━━━━━━━━━━━━━━"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bm1 CASCADE;
CREATE TABLE bm1 (id serial PRIMARY KEY, val integer, data text);
SQL

BULK_SQL="INSERT INTO bm1 (val, data) SELECT g, repeat('x',50) FROM generate_series(1,100000) g"
BULK_RESET="TRUNCATE bm1"

bench_3x "baseline (no capture):" "$BULK_SQL" 
$PSQL -q -c "$BULK_RESET"

# Trigger mode
force_mode trigger
$PSQL -q -c "SELECT flashback_track('bm1');"
bench_3x "trigger mode:" "$BULK_SQL"
$PSQL -q -c "$BULK_RESET"
$PSQL -q -c "SELECT flashback_untrack('bm1');"

# WAL mode
force_mode wal
$PSQL -q -c "SELECT flashback_track('bm1');"
bench_3x "WAL mode:" "$BULK_SQL"
$PSQL -q -c "SELECT flashback_untrack('bm1');"

$PSQL -q -c "DROP TABLE bm1 CASCADE;"
echo ""

# ──────────────────────────────────────────────────────────────────────
# SCENARIO 2: High-frequency small UPDATE (10K single-row updates)
# ──────────────────────────────────────────────────────────────────────
echo "━━━ Scenario 2: 10K Single-row UPDATEs ━━━━━━━━━━━━━━━━━━━━━━━━"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bm2 CASCADE;
CREATE TABLE bm2 (id serial PRIMARY KEY, val integer, data text);
INSERT INTO bm2 (val, data) SELECT g, 'init' FROM generate_series(1,10000) g;
SQL

SINGLE_SQL="UPDATE bm2 SET val = val + 1, data = 'upd' WHERE id = (1 + (random()*9999)::int)"
SINGLE_BATCH="DO \$\$ BEGIN FOR i IN 1..10000 LOOP UPDATE bm2 SET val=val+1, data='upd' WHERE id=i; END LOOP; END; \$\$"
SINGLE_RESET="UPDATE bm2 SET val=0, data='reset'"

bench_3x "baseline (no capture):" "$SINGLE_BATCH"
$PSQL -q -c "$SINGLE_RESET"

force_mode trigger
$PSQL -q -c "SELECT flashback_track('bm2');"
bench_3x "trigger mode:" "$SINGLE_BATCH"
$PSQL -q -c "$SINGLE_RESET"
$PSQL -q -c "SELECT flashback_untrack('bm2');"

force_mode wal
$PSQL -q -c "SELECT flashback_track('bm2');"
bench_3x "WAL mode:" "$SINGLE_BATCH"
$PSQL -q -c "SELECT flashback_untrack('bm2');"

$PSQL -q -c "DROP TABLE bm2 CASCADE;"
echo ""

# ──────────────────────────────────────────────────────────────────────
# SCENARIO 3: Mixed INSERT+UPDATE+DELETE
# ──────────────────────────────────────────────────────────────────────
echo "━━━ Scenario 3: Mixed DML (5K INSERT + 5K UPDATE + 2.5K DELETE) ━━"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bm3 CASCADE;
CREATE TABLE bm3 (id serial PRIMARY KEY, val integer, data text);
SQL

MIXED_SQL="DO \$\$ BEGIN
  FOR i IN 1..5000 LOOP INSERT INTO bm3 (val, data) VALUES (i, 'new'); END LOOP;
  FOR i IN 1..5000 LOOP UPDATE bm3 SET data='upd' WHERE id=i; END LOOP;
  DELETE FROM bm3 WHERE id <= 2500;
END; \$\$"
MIXED_RESET="TRUNCATE bm3 RESTART IDENTITY"

bench_3x "baseline (no capture):" "$MIXED_SQL"
$PSQL -q -c "$MIXED_RESET"

force_mode trigger
$PSQL -q -c "SELECT flashback_track('bm3');"
bench_3x "trigger mode:" "$MIXED_SQL"
$PSQL -q -c "$MIXED_RESET"
$PSQL -q -c "SELECT flashback_untrack('bm3');"

force_mode wal
$PSQL -q -c "SELECT flashback_track('bm3');"
bench_3x "WAL mode:" "$MIXED_SQL"
$PSQL -q -c "SELECT flashback_untrack('bm3');"

$PSQL -q -c "DROP TABLE bm3 CASCADE;"
echo ""

# ──────────────────────────────────────────────────────────────────────
# SCENARIO 4: Wide table (15 col) bulk update
# ──────────────────────────────────────────────────────────────────────
echo "━━━ Scenario 4: Wide Table UPDATE (15 cols, 5K rows) ━━━━━━━━━━━"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bm4 CASCADE;
CREATE TABLE bm4 (
    id serial PRIMARY KEY,
    c1 text, c2 text, c3 text, c4 integer, c5 integer,
    c6 numeric(12,4), c7 boolean, c8 timestamptz DEFAULT now(),
    c9 jsonb, c10 text, c11 integer, c12 numeric(12,4),
    c13 boolean, c14 text, c15 text
);
INSERT INTO bm4 (c1,c2,c3,c4,c5,c6,c7,c9,c10,c11,c12,c13,c14,c15)
SELECT 'a','b','c',g,g*2,g*1.5,true,'{"k":1}','d',g,g*3.0,false,'e','f'
FROM generate_series(1,5000) g;
SQL

WIDE_SQL="UPDATE bm4 SET c1='x', c4=c4+1, c6=c6+0.1, c9='{\"updated\":true}' WHERE id <= 5000"
WIDE_RESET="UPDATE bm4 SET c1='a', c4=id, c6=id*1.5, c9='{\"k\":1}'"

bench_3x "baseline (no capture):" "$WIDE_SQL"
$PSQL -q -c "$WIDE_RESET"

force_mode trigger
$PSQL -q -c "SELECT flashback_track('bm4');"
bench_3x "trigger mode:" "$WIDE_SQL"
$PSQL -q -c "$WIDE_RESET"
$PSQL -q -c "SELECT flashback_untrack('bm4');"

force_mode wal
$PSQL -q -c "SELECT flashback_track('bm4');"
bench_3x "WAL mode:" "$WIDE_SQL"
$PSQL -q -c "SELECT flashback_untrack('bm4');"

$PSQL -q -c "DROP TABLE bm4 CASCADE;"
echo ""

# ──────────────────────────────────────────────────────────────────────
# SCENARIO 5: Concurrent simulation via pgbench
# ──────────────────────────────────────────────────────────────────────
PGBENCH="/usr/local/pgsql-17/bin/pgbench -h $SOCKDIR -p $PORT -d postgres"

echo "━━━ Scenario 5: pgbench TPS (8 clients, 30s) ━━━━━━━━━━━━━━━━━━"

$PSQL -q <<'SQL'
DROP TABLE IF EXISTS bm5 CASCADE;
CREATE TABLE bm5 (id serial PRIMARY KEY, val integer DEFAULT 0, data text DEFAULT '');
INSERT INTO bm5 (val, data) SELECT g, repeat('x',20) FROM generate_series(1,1000) g;
SQL

PGBENCH_SCRIPT=$(mktemp /tmp/fg_bench_XXXXXX.sql)
cat > "$PGBENCH_SCRIPT" <<'PGBENCH_SQL'
\set id random(1, 1000)
UPDATE bm5 SET val = val + 1, data = 'upd' WHERE id = :id;
PGBENCH_SQL

run_pgbench() {
    local label="$1"
    local tps
    tps=$($PGBENCH -n -c 8 -j 4 -T 20 -f "$PGBENCH_SCRIPT" 2>&1 | grep "^tps" | head -1 | awk '{print $3}' | tr -d ',')
    printf "  %-26s %8.0f TPS\n" "$label" "$tps"
}

run_pgbench "baseline (no capture):"

force_mode trigger
$PSQL -q -c "SELECT flashback_track('bm5');"
run_pgbench "trigger mode:"
$PSQL -q -c "SELECT flashback_untrack('bm5');"

force_mode wal
$PSQL -q -c "SELECT flashback_track('bm5');"
run_pgbench "WAL mode:"
$PSQL -q -c "SELECT flashback_untrack('bm5');"

rm -f "$PGBENCH_SCRIPT"
$PSQL -q -c "DROP TABLE bm5 CASCADE;"
echo ""

# ── Restore mode back to auto ─────────────────────────────────────────
force_mode auto

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    Benchmark Complete                        ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "  WAL mode overhead vs baseline = latency of worker consuming async"
echo "  Trigger mode overhead = synchronous row-level trigger cost per DML"
echo "  TPS delta shows real-world concurrent throughput impact."

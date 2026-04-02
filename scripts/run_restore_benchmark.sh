#!/usr/bin/env bash
# pg_flashback — Restore Performance Benchmark
# Measures restore speed at 10K / 100K / 500K row scales.
# Outputs rows-per-second and wall-clock time for each scenario.
#
# Usage: ./scripts/run_restore_benchmark.sh [port] [socket_dir]
set -euo pipefail

PORT="${1:-28817}"
SOCKDIR="${2:-$HOME/.pgrx}"
PSQL="/usr/local/pgsql-17/bin/psql -h $SOCKDIR -p $PORT -d postgres -v ON_ERROR_STOP=on"

echo "═══════════════════════════════════════════════════════"
echo "  pg_flashback — Restore Performance Benchmark"
echo "  Port: $PORT   Socket: $SOCKDIR"
echo "  Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "═══════════════════════════════════════════════════════"
echo ""

run_restore_bench() {
    local label="$1"
    local rows="$2"

    echo "─── $label ($rows rows) ───"

    $PSQL -q <<SQL
DROP TABLE IF EXISTS rb_orders CASCADE;
CREATE TABLE rb_orders (
    id       bigserial PRIMARY KEY,
    customer text NOT NULL,
    amount   numeric(12,2),
    status   text,
    region   text,
    notes    text
);

INSERT INTO rb_orders (customer, amount, status, region, notes)
SELECT
    'customer_' || g,
    (random() * 9999 + 1)::numeric(12,2),
    (ARRAY['NEW','PROCESSING','SHIPPED','DELIVERED'])[ceil(random()*4)],
    (ARRAY['US','EU','APAC','LATAM'])[ceil(random()*4)],
    repeat('x', 20)
FROM generate_series(1, ${rows}) g;

SELECT flashback_track('rb_orders');
SELECT pg_sleep(0.2);   -- let worker flush

-- Checkpoint to have a fast restore base
SELECT flashback_checkpoint('rb_orders');
SQL

    # Disaster: update ALL rows (maximises delta log size)
    $PSQL -q -c "
UPDATE rb_orders SET status = 'DISASTER', notes = repeat('y',20);
" 

    # Flush staging immediately via temp worker call
    $PSQL -q -c "SELECT pg_sleep(0.5);"

    local t_before
    t_before=$($PSQL -Atq -c "SELECT now() - interval '1 second';")

    # Measure restore
    local wall_start=$(date +%s%N)
    $PSQL -q -c "SELECT flashback_restore('rb_orders', '${t_before}'::timestamptz);" 
    local wall_end=$(date +%s%N)
    local wall_ms=$(( (wall_end - wall_start) / 1000000 ))
    local rps=$(( rows * 1000 / (wall_ms + 1) ))

    echo "  Restore: ${wall_ms} ms   throughput: ~${rps} rows/s"

    $PSQL -q -c "SELECT flashback_untrack('rb_orders'); DROP TABLE IF EXISTS rb_orders CASCADE;"
    echo ""
}

# ── Scenario: 10K rows ──────────────────────────────────────────────
run_restore_bench "Batch restore" 10000

# ── Scenario: 100K rows ─────────────────────────────────────────────
run_restore_bench "Batch restore" 100000

# ── Scenario: 500K rows ─────────────────────────────────────────────
run_restore_bench "Batch restore" 500000

# ── Scenario: 1M rows (update-heavy — all rows changed) ─────────────
run_restore_bench "Batch restore (1M rows UPDATE ALL)" 1000000

# ── Scenario: 1M rows — INSERT + DELETE mix ─────────────────────────
echo "─── 1M rows — INSERT then partial DELETE restore ───"
$PSQL -q <<'SQL'
DROP TABLE IF EXISTS rb_mixed CASCADE;
CREATE TABLE rb_mixed (
    id    bigserial PRIMARY KEY,
    val   text,
    score integer
);
INSERT INTO rb_mixed (val, score)
SELECT 'item_' || g, (random()*1000)::integer
FROM generate_series(1, 1000000) g;
SELECT flashback_track('rb_mixed');
SELECT pg_sleep(0.2);
SELECT flashback_checkpoint('rb_mixed');
SQL

BEFORE_TS=$($PSQL -Atq -c "SELECT now() - interval '1 second';")
$PSQL -q -c "DELETE FROM rb_mixed WHERE id % 3 = 0;"   # delete ~333K rows
$PSQL -q -c "SELECT pg_sleep(0.5);"

wall_start=$(date +%s%N)
$PSQL -q -c "SELECT flashback_restore('rb_mixed', '${BEFORE_TS}'::timestamptz);"
wall_end=$(date +%s%N)
wall_ms=$(( (wall_end - wall_start) / 1000000 ))
echo "  Restore 1M rows (333K DELETEs replayed): ${wall_ms} ms   ~$(( 333333 * 1000 / (wall_ms+1) )) events/s"

$PSQL -q -c "SELECT flashback_untrack('rb_mixed'); DROP TABLE IF EXISTS rb_mixed CASCADE;"
echo ""

# ── Scenario: parallel restore hint ─────────────────────────────────
echo "─── flashback_restore_parallel (4 workers hint) ───"
$PSQL -q <<'SQL'
DROP TABLE IF EXISTS rb_parallel CASCADE;
CREATE TABLE rb_parallel (
    id   bigserial PRIMARY KEY,
    data text
);
INSERT INTO rb_parallel (data)
SELECT repeat('p', 50) FROM generate_series(1, 500000) g;
SELECT flashback_track('rb_parallel');
SELECT pg_sleep(0.2);
SELECT flashback_checkpoint('rb_parallel');
SQL

BEFORE_TS=$($PSQL -Atq -c "SELECT now() - interval '1 second';")
$PSQL -q -c "UPDATE rb_parallel SET data = repeat('q', 50);"
$PSQL -q -c "SELECT pg_sleep(0.5);"

wall_start=$(date +%s%N)
$PSQL -q -c "SELECT * FROM flashback_restore_parallel('rb_parallel', '${BEFORE_TS}'::timestamptz, 4);"
wall_end=$(date +%s%N)
wall_ms=$(( (wall_end - wall_start) / 1000000 ))
echo "  flashback_restore_parallel 500K: ${wall_ms} ms"

$PSQL -q -c "SELECT flashback_untrack('rb_parallel'); DROP TABLE IF EXISTS rb_parallel CASCADE;"
echo ""

echo "═══════════════════════════════════════════════════════"
echo "  Benchmark complete."
echo "═══════════════════════════════════════════════════════"

#!/usr/bin/env bash
# pg_flashback — Restore Performance Benchmark
# Measures restore speed at 10K / 100K / 500K row scales.
# Outputs rows-per-second and wall-clock time for each scenario.
#
# Usage: ./scripts/run_restore_benchmark.sh [port] [socket_dir]
set -euo pipefail

PORT="${1:-28817}"
SOCKDIR="${2:-$HOME/.pgrx}"

# Detect psql: prefer pgrx-installed PG17, fall back to PATH
_PGRX_BIN="$HOME/.pgrx/17.*/pgrx-install/bin"
_RESOLVED=$(echo $_PGRX_BIN 2>/dev/null | tr ' ' '\n' | head -1)
if [[ -d "$_RESOLVED" ]]; then
  PSQL_BIN="$_RESOLVED/psql"
else
  PSQL_BIN="$(command -v psql)"
fi
PSQL="$PSQL_BIN -h $SOCKDIR -p $PORT -d postgres -v ON_ERROR_STOP=on"

echo "═══════════════════════════════════════════════════════"
echo "  pg_flashback — Restore Performance Benchmark"
echo "  Port: $PORT   Socket: $SOCKDIR"
echo "  Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "═══════════════════════════════════════════════════════"
echo ""

# Install benchmark helper trigger (bypasses staging worker → writes direct to delta_log)
$PSQL -q <<'HELPERS'
CREATE OR REPLACE FUNCTION _fb_bench_capture_trigger()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_sv bigint;
BEGIN
    SELECT schema_version INTO v_sv FROM flashback.tracked_tables
    WHERE rel_oid = TG_RELID AND is_active;
    v_sv := coalesce(v_sv, 1);
    IF TG_OP = 'INSERT' THEN
        INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
        VALUES(clock_timestamp(),'INSERT',TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME,TG_RELID,v_sv,NULL,to_jsonb(NEW),clock_timestamp());
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
        VALUES(clock_timestamp(),'UPDATE',TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME,TG_RELID,v_sv,to_jsonb(OLD),to_jsonb(NEW),clock_timestamp());
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
        VALUES(clock_timestamp(),'DELETE',TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME,TG_RELID,v_sv,to_jsonb(OLD),NULL,clock_timestamp());
        RETURN OLD;
    END IF;
    RETURN NULL;
END;$$;
HELPERS

# Helper: attach direct-to-delta_log trigger for a table
attach_bench_trigger() {
    local schema="$1" table="$2"
    $PSQL -q -c "
        DROP TRIGGER IF EXISTS _fb_bench_capture ON ${schema}.${table};
        CREATE TRIGGER _fb_bench_capture
        AFTER INSERT OR UPDATE OR DELETE ON ${schema}.${table}
        FOR EACH ROW EXECUTE FUNCTION _fb_bench_capture_trigger();
    "
}

# Clean up any stale state from previous runs
$PSQL -q 2>/dev/null <<'CLEANUP'
DO $$ 
DECLARE r record;
BEGIN
    -- Drop all snapshot tables referenced in tracked_tables for these bench tables
    FOR r IN SELECT s.snapshot_table
             FROM flashback.snapshots s
             JOIN flashback.tracked_tables tt ON tt.rel_oid = s.rel_oid
             WHERE tt.table_name IN ('rb_orders','rb_mixed','rb_parallel')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %s', r.snapshot_table);
    END LOOP;
    -- Also drop base_snapshot tables
    FOR r IN SELECT base_snapshot_table
             FROM flashback.tracked_tables
             WHERE table_name IN ('rb_orders','rb_mixed','rb_parallel')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %s', r.base_snapshot_table);
    END LOOP;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
-- Force-clean all flashback state for bench tables
DELETE FROM flashback.delta_log
WHERE table_name IN ('public.rb_orders','public.rb_mixed','public.rb_parallel');
DELETE FROM flashback.staging_events
WHERE table_name IN ('public.rb_orders','public.rb_mixed','public.rb_parallel');
DELETE FROM flashback.snapshots
WHERE rel_oid IN (
    SELECT rel_oid FROM flashback.tracked_tables
    WHERE table_name IN ('rb_orders','rb_mixed','rb_parallel')
);
DELETE FROM flashback.schema_versions
WHERE rel_oid IN (
    SELECT rel_oid FROM flashback.tracked_tables
    WHERE table_name IN ('rb_orders','rb_mixed','rb_parallel')
);
DELETE FROM flashback.tracked_tables
WHERE table_name IN ('rb_orders','rb_mixed','rb_parallel');
DROP TABLE IF EXISTS rb_orders, rb_mixed, rb_parallel CASCADE;
CLEANUP

run_restore_bench() {
    local label="$1"
    local rows="$2"

    echo "─── $label ($rows rows) ───"

    # Clean up any stale tracking entries before starting
    $PSQL -q 2>/dev/null <<'INNERCLEAN'
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT s.snapshot_table FROM flashback.snapshots s
             JOIN flashback.tracked_tables tt ON tt.rel_oid = s.rel_oid
             WHERE tt.table_name = 'rb_orders'
    LOOP EXECUTE format('DROP TABLE IF EXISTS %s', r.snapshot_table); END LOOP;
    FOR r IN SELECT base_snapshot_table FROM flashback.tracked_tables WHERE table_name = 'rb_orders'
    LOOP EXECUTE format('DROP TABLE IF EXISTS %s', r.base_snapshot_table); END LOOP;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DELETE FROM flashback.delta_log WHERE table_name = 'public.rb_orders';
DELETE FROM flashback.staging_events WHERE table_name = 'public.rb_orders';
DELETE FROM flashback.snapshots WHERE rel_oid IN (
    SELECT rel_oid FROM flashback.tracked_tables WHERE table_name = 'rb_orders');
DELETE FROM flashback.schema_versions WHERE rel_oid IN (
    SELECT rel_oid FROM flashback.tracked_tables WHERE table_name = 'rb_orders');
DELETE FROM flashback.tracked_tables WHERE table_name = 'rb_orders';
INNERCLEAN

    # Create empty table and track it (snapshot = 0 rows)
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

SELECT flashback_track('rb_orders');
SQL

    # Attach direct-to-delta_log trigger (bypass staging worker)
    attach_bench_trigger public rb_orders

    # Record t_before AFTER tracking (tracked_since is now set)
    local t_before
    t_before=$($PSQL -Atq -c "SELECT clock_timestamp();")

    # Insert rows — these INSERT events land directly in delta_log
    $PSQL -q -c "
INSERT INTO rb_orders (customer, amount, status, region, notes)
SELECT 'customer_' || g,
       (random() * 9999 + 1)::numeric(12,2),
       (ARRAY['NEW','PROCESSING','SHIPPED','DELIVERED'])[ceil(random()*4)::int],
       (ARRAY['US','EU','APAC','LATAM'])[ceil(random()*4)::int],
       repeat('x', 20)
FROM generate_series(1, ${rows}) g;
"

    local t_disaster
    t_disaster=$($PSQL -Atq -c "SELECT clock_timestamp();")

    # Disaster: update ALL rows (events after t_before → replayed during restore)
    $PSQL -q -c "UPDATE rb_orders SET status = 'DISASTER', notes = repeat('y',20);"

    # Measure restore — restore to t_disaster (replays the UPDATE disaster back to pre-disaster state)
    local wall_start=$(date +%s%N)
    $PSQL -q -c "SELECT flashback_restore('rb_orders', '${t_disaster}'::timestamptz);" 
    local wall_end=$(date +%s%N)
    local wall_ms=$(( (wall_end - wall_start) / 1000000 ))
    local rps=$(( rows * 1000 / (wall_ms + 1) ))

    echo "  Restore: ${wall_ms} ms   throughput: ~${rps} rows/s"

    $PSQL -q -c "SELECT flashback_untrack('rb_orders'); DROP TABLE IF EXISTS rb_orders CASCADE;"
    echo ""
}

# ── Scenario: 10K rows ──────────────────────────────────────────────
run_restore_bench "Batch restore" 10000

# ── Scenario: 50K rows ──────────────────────────────────────────────
run_restore_bench "Batch restore" 50000

# ── Scenario: 100K rows ─────────────────────────────────────────────
run_restore_bench "Batch restore" 100000

# ── Scenario: 200K rows ─────────────────────────────────────────────
run_restore_bench "Batch restore" 200000

# ── Scenario: 200K rows — INSERT + DELETE mix ───────────────────────
echo "─── 200K rows — INSERT then partial DELETE restore ───"
$PSQL -q <<'SQL'
DROP TABLE IF EXISTS rb_mixed CASCADE;
CREATE TABLE rb_mixed (
    id    bigserial PRIMARY KEY,
    val   text,
    score integer
);
SELECT flashback_track('rb_mixed');
SQL

attach_bench_trigger public rb_mixed

$PSQL -q <<'SQL'
INSERT INTO rb_mixed (val, score)
SELECT 'item_' || g, (random()*1000)::integer
FROM generate_series(1, 200000) g;
SQL

DISASTER_TS=$($PSQL -Atq -c "SELECT clock_timestamp();")
$PSQL -q -c "DELETE FROM rb_mixed WHERE id % 3 = 0;"   # delete ~66K rows

wall_start=$(date +%s%N)
$PSQL -q -c "SELECT flashback_restore('rb_mixed', '${DISASTER_TS}'::timestamptz);"
wall_end=$(date +%s%N)
wall_ms=$(( (wall_end - wall_start) / 1000000 ))
echo "  Restore 200K rows (66K DELETEs replayed): ${wall_ms} ms   ~$(( 66666 * 1000 / (wall_ms+1) )) events/s"

$PSQL -q -c "SELECT flashback_untrack('rb_mixed'); DROP TABLE IF EXISTS rb_mixed CASCADE;"
echo ""

# ── Scenario: parallel restore hint ─────────────────────────────────
echo "─── flashback_restore_parallel (4 workers hint, 200K rows) ───"
$PSQL -q <<'SQL'
DROP TABLE IF EXISTS rb_parallel CASCADE;
CREATE TABLE rb_parallel (
    id   bigserial PRIMARY KEY,
    data text
);
SELECT flashback_track('rb_parallel');
SQL

attach_bench_trigger public rb_parallel

$PSQL -q <<'SQL'
INSERT INTO rb_parallel (data)
SELECT repeat('p', 50) FROM generate_series(1, 200000) g;
SQL

BEFORE_TS=$($PSQL -Atq -c "SELECT clock_timestamp();")
$PSQL -q -c "UPDATE rb_parallel SET data = repeat('q', 50);"

wall_start=$(date +%s%N)
$PSQL -q -c "SELECT * FROM flashback_restore_parallel('rb_parallel', '${BEFORE_TS}'::timestamptz, 4);"
wall_end=$(date +%s%N)
wall_ms=$(( (wall_end - wall_start) / 1000000 ))
echo "  flashback_restore_parallel 200K: ${wall_ms} ms"

$PSQL -q -c "SELECT flashback_untrack('rb_parallel'); DROP TABLE IF EXISTS rb_parallel CASCADE;"
echo ""

echo "═══════════════════════════════════════════════════════"
echo "  Benchmark complete."
echo "═══════════════════════════════════════════════════════"

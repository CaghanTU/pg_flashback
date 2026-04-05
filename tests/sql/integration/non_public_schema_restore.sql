-- Test: Tables in a non-public schema are tracked and restored correctly.
-- Validates that schema qualification is handled throughout capture/restore.
DO $tv$
DECLARE
    t_before timestamptz;
    v_cnt    bigint;
    v_val    text;
BEGIN
    -- ── Create custom schema ──────────────────────────────────
    CREATE SCHEMA IF NOT EXISTS it_ns;

    DROP TABLE IF EXISTS it_ns.orders CASCADE;
    CREATE TABLE it_ns.orders (
        order_id   serial PRIMARY KEY,
        customer   text   NOT NULL,
        total      numeric(10,2)
    );

    PERFORM flashback_track('it_ns.orders');
    PERFORM flashback_test_attach_capture_trigger('it_ns.orders'::regclass);

    INSERT INTO it_ns.orders (customer, total) VALUES
        ('Alice', 99.99),
        ('Bob',   149.50),
        ('Carol', 9.99);

    t_before := clock_timestamp();

    -- Mass delete
    DELETE FROM it_ns.orders;

    -- Confirm empty
    IF (SELECT count(*) FROM it_ns.orders) <> 0 THEN
        RAISE EXCEPTION 'delete did not take effect';
    END IF;

    PERFORM flashback_restore('it_ns.orders', t_before);

    -- Verify all rows are back
    SELECT count(*) INTO v_cnt FROM it_ns.orders;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    -- Verify content is intact
    SELECT customer INTO v_val FROM it_ns.orders WHERE total = 99.99;
    IF v_val <> 'Alice' THEN
        RAISE EXCEPTION 'wrong customer data after restore: got ''%''', v_val;
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────
    DROP TABLE IF EXISTS it_ns.orders CASCADE;
    DROP SCHEMA IF EXISTS it_ns CASCADE;
END;
$tv$;

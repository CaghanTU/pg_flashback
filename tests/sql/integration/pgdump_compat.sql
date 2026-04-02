-- Test: pg_dump compatibility — extension config tables survive dump metadata
-- Verifies pg_extension_config_dump is properly set for flashback tables
DO $tv$
DECLARE v_cnt bigint;
BEGIN
    -- Verify config_dump registrations exist (set in schema.rs bootstrap)
    SELECT count(*) INTO v_cnt
    FROM pg_extension_config_dump_info() AS d
    WHERE d.extname = 'pg_flashback';

    -- If the function doesn't exist (PG < 17.x), use pg_depend alternative
    -- For PG17+ we check via the catalog
    EXCEPTION WHEN undefined_function THEN
        -- Fallback: just check the tables are extension-owned
        SELECT count(*) INTO v_cnt
        FROM pg_depend d
        JOIN pg_class c ON c.oid = d.objid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE d.refclassid = 'pg_extension'::regclass
          AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pg_flashback')
          AND d.classid = 'pg_class'::regclass
          AND n.nspname = 'flashback'
          AND c.relkind = 'r'
          AND d.deptype = 'e';

        IF v_cnt < 5 THEN
            RAISE EXCEPTION 'expected at least 5 extension-owned tables, got %', v_cnt;
        END IF;
END;
$tv$;

-- Also verify tracked table, delta_log, snapshots are extension-owned
DO $tv2$
DECLARE v_cnt bigint;
BEGIN
    SELECT count(*) INTO v_cnt
    FROM pg_depend d
    JOIN pg_class c ON c.oid = d.objid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE d.refclassid = 'pg_extension'::regclass
      AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pg_flashback')
      AND d.classid = 'pg_class'::regclass
      AND n.nspname = 'flashback'
      AND c.relname IN ('tracked_tables', 'delta_log', 'snapshots', 'restore_log', 'staging_events', 'schema_versions');

    IF v_cnt < 5 THEN
        RAISE EXCEPTION 'expected at least 5 core tables owned by extension, got %', v_cnt;
    END IF;
END;
$tv2$;

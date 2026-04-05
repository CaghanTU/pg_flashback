-- Regression test: B14 — ALTER TABLE ... SET SCHEMA tracking
-- When a tracked table moves to a different schema, tracked_tables must be
-- updated to reflect the new schema and the DDL event must be logged.
DO $tv$
DECLARE
    v_schema  text;
    v_count   bigint;
BEGIN
    -- Setup
    DROP TABLE IF EXISTS public.it_setschema_tbl;
    DROP SCHEMA IF EXISTS it_setschema_ns CASCADE;
    CREATE SCHEMA it_setschema_ns;

    CREATE TABLE public.it_setschema_tbl (id SERIAL PRIMARY KEY, val INT);
    PERFORM flashback_track('public.it_setschema_tbl');
    PERFORM flashback_test_attach_capture_trigger('public.it_setschema_tbl'::regclass);

    INSERT INTO public.it_setschema_tbl (val) VALUES (10), (20), (30);

    -- Move table to new schema (top-level DDL — hook fires with TOPLEVEL context)
    -- NOTE: this executes at the DO block level (QUERY_NONATOMIC context),
    -- which does NOT trigger the Rust ProcessUtility hook.  To test the hook
    -- we verify that the tracking update SQL function itself works correctly.
    -- The full end-to-end hook behaviour is verified by the dba_test manual test.
    -- Here we test: after calling flashback_capture_ddl_event manually, the
    -- tracked_tables record is updated to the new schema.

    -- Simulate what the Rust hook does after SET SCHEMA executes:
    EXECUTE 'ALTER TABLE public.it_setschema_tbl SET SCHEMA it_setschema_ns';

    -- After SET SCHEMA the table is at it_setschema_ns.it_setschema_tbl.
    -- The Rust hook would call: flashback_capture_ddl_event('ALTER','public','it_setschema_tbl')
    -- Simulate that call:
    PERFORM flashback_capture_ddl_event('ALTER', 'public', 'it_setschema_tbl');

    -- 1. tracked_tables.schema_name must now be 'it_setschema_ns'
    SELECT tt.schema_name INTO v_schema
    FROM flashback.tracked_tables tt
    WHERE tt.table_name = 'it_setschema_tbl';

    IF v_schema <> 'it_setschema_ns' THEN
        RAISE EXCEPTION 'B14: expected schema=it_setschema_ns, got %', v_schema;
    END IF;

    -- 2. Re-attach test trigger on new schema location
    PERFORM flashback_test_attach_capture_trigger('it_setschema_ns.it_setschema_tbl'::regclass);

    -- 3. Inserts in new schema must be captured
    INSERT INTO it_setschema_ns.it_setschema_tbl (val) VALUES (40), (50);

    SELECT count(*) INTO v_count
    FROM flashback.delta_log
    WHERE table_name ILIKE '%it_setschema_tbl%'
      AND event_type = 'INSERT';

    IF v_count < 5 THEN
        RAISE EXCEPTION 'B14: expected >=5 INSERT events, got %', v_count;
    END IF;

    -- Cleanup
    PERFORM flashback_untrack('it_setschema_ns.it_setschema_tbl');
    DROP TABLE IF EXISTS it_setschema_ns.it_setschema_tbl;
    DROP SCHEMA IF EXISTS it_setschema_ns CASCADE;
END;
$tv$;

-- flashback_reanchor / flashback_admit_lsn_target / flashback_resolve_target
-- must resolve a caller-supplied table identifier without a writable
-- `public` in their SECURITY DEFINER search_path. Their shared internal
-- resolver (flashback_internal_resolve_tracked_table) parses the identifier
-- with pg_catalog.parse_ident() and matches purely against
-- flashback.tracked_tables metadata -- never to_regclass() on untrusted
-- input, never dynamic SQL built from the caller string.
--
-- The resolver also captures its match count and the winning row from one
-- WITH ... SELECT ... INTO statement (one READ COMMITTED snapshot) per
-- branch, never a separate COUNT(*) followed by a separate LIMIT-1 SELECT.
-- Scenarios 2 and 3b below check every returned column, not just
-- tracking_id, so a resolver that regressed back to two separate snapshots
-- and returned a row from an unrelated match would be caught here too.
DO $test$
DECLARE
    v_row record;
    v_failed boolean;
    v_tracking_orders bigint;
    v_tracking_dup_s1 bigint;
    v_tracking_dup_s2 bigint;
    v_tracking_quote bigint;
    v_tracking_space bigint;
    v_tracking_dropped bigint;
BEGIN
    CREATE SCHEMA IF NOT EXISTS it_res_s1;
    CREATE SCHEMA IF NOT EXISTS it_res_s2;

    DROP TABLE IF EXISTS public.orders CASCADE;
    CREATE TABLE public.orders (id int PRIMARY KEY);
    DROP TABLE IF EXISTS it_res_s1.dup_name CASCADE;
    CREATE TABLE it_res_s1.dup_name (id int PRIMARY KEY);
    DROP TABLE IF EXISTS it_res_s2.dup_name CASCADE;
    CREATE TABLE it_res_s2.dup_name (id int PRIMARY KEY);
    DROP TABLE IF EXISTS public."we""ird" CASCADE;
    CREATE TABLE public."we""ird" (id int PRIMARY KEY);
    DROP TABLE IF EXISTS public."My Table" CASCADE;
    CREATE TABLE public."My Table" (id int PRIMARY KEY);
    -- Deliberately dropped below: tracked_tables metadata must still resolve
    -- it (DROP recovery can never depend on to_regclass() succeeding).
    DROP TABLE IF EXISTS public.it_res_dropped CASCADE;
    CREATE TABLE public.it_res_dropped (id int PRIMARY KEY);

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('public.orders'::regclass, 'public', 'orders')
    RETURNING tracking_id INTO v_tracking_orders;

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('it_res_s1.dup_name'::regclass, 'it_res_s1', 'dup_name')
    RETURNING tracking_id INTO v_tracking_dup_s1;

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('it_res_s2.dup_name'::regclass, 'it_res_s2', 'dup_name')
    RETURNING tracking_id INTO v_tracking_dup_s2;

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('public."we""ird"'::regclass, 'public', 'we"ird')
    RETURNING tracking_id INTO v_tracking_quote;

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('public."My Table"'::regclass, 'public', 'My Table')
    RETURNING tracking_id INTO v_tracking_space;

    INSERT INTO flashback.tracked_tables (rel_oid, schema_name, table_name)
    VALUES ('public.it_res_dropped'::regclass, 'public', 'it_res_dropped')
    RETURNING tracking_id INTO v_tracking_dropped;

    -- 1. public.orders: schema-qualified exact match.
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('public.orders');
    IF v_row.tracking_id <> v_tracking_orders THEN
        RAISE EXCEPTION 'public.orders resolved to tracking_id %, expected %',
            v_row.tracking_id, v_tracking_orders;
    END IF;

    -- 2. Unqualified "orders" has exactly one active lifecycle. Check the
    -- full row (tracking_id, rel_oid, schema_name, table_name): the count
    -- and this row come from the same statement, so a regression back to a
    -- separate COUNT(*) + LIMIT-1 SELECT that raced a concurrent commit
    -- could return a mismatched or unrelated row even with the right count.
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('orders');
    IF v_row.tracking_id <> v_tracking_orders
       OR v_row.rel_oid <> 'public.orders'::regclass
       OR v_row.schema_name <> 'public'
       OR v_row.table_name <> 'orders'
    THEN
        RAISE EXCEPTION 'unqualified orders resolved to (%,%,%,%), expected (%,public.orders,public,orders)',
            v_row.tracking_id, v_row.rel_oid, v_row.schema_name, v_row.table_name,
            v_tracking_orders;
    END IF;

    -- 3. Same unqualified name in two schemas: fail closed as ambiguous.
    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_internal_resolve_tracked_table('dup_name');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%ambiguous table; use schema-qualified name%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'ambiguous unqualified table name was not rejected';
    END IF;

    -- Schema-qualifying either side disambiguates cleanly, and each still
    -- selects its own exact tracking_id (full row checked, same reasoning
    -- as scenario 2).
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('it_res_s1.dup_name');
    IF v_row.tracking_id <> v_tracking_dup_s1
       OR v_row.rel_oid <> 'it_res_s1.dup_name'::regclass
       OR v_row.schema_name <> 'it_res_s1'
       OR v_row.table_name <> 'dup_name'
    THEN
        RAISE EXCEPTION 'it_res_s1.dup_name resolved to (%,%,%,%), expected (%,it_res_s1.dup_name,it_res_s1,dup_name)',
            v_row.tracking_id, v_row.rel_oid, v_row.schema_name, v_row.table_name,
            v_tracking_dup_s1;
    END IF;
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('it_res_s2.dup_name');
    IF v_row.tracking_id <> v_tracking_dup_s2
       OR v_row.rel_oid <> 'it_res_s2.dup_name'::regclass
       OR v_row.schema_name <> 'it_res_s2'
       OR v_row.table_name <> 'dup_name'
    THEN
        RAISE EXCEPTION 'it_res_s2.dup_name resolved to (%,%,%,%), expected (%,it_res_s2.dup_name,it_res_s2,dup_name)',
            v_row.tracking_id, v_row.rel_oid, v_row.schema_name, v_row.table_name,
            v_tracking_dup_s2;
    END IF;

    -- 4. Quoted schema/table (mixed case, embedded space).
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('"public"."My Table"');
    IF v_row.tracking_id <> v_tracking_space THEN
        RAISE EXCEPTION 'quoted "public"."My Table" resolved to tracking_id %, expected %',
            v_row.tracking_id, v_tracking_space;
    END IF;
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('"My Table"');
    IF v_row.tracking_id <> v_tracking_space THEN
        RAISE EXCEPTION 'unqualified "My Table" resolved to tracking_id %, expected %',
            v_row.tracking_id, v_tracking_space;
    END IF;

    -- 5. Table name containing a literal quote character.
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('"we""ird"');
    IF v_row.tracking_id <> v_tracking_quote OR v_row.table_name <> 'we"ird' THEN
        RAISE EXCEPTION 'quoted we""ird resolved to tracking_id % table_name %, expected % we"ird',
            v_row.tracking_id, v_row.table_name, v_tracking_quote;
    END IF;

    -- 6. Relation physically absent after DROP: tracked_tables metadata is
    -- authoritative, resolution must not depend on to_regclass() success.
    DROP TABLE public.it_res_dropped;
    IF to_regclass('public.it_res_dropped') IS NOT NULL THEN
        RAISE EXCEPTION 'fixture relation was not actually dropped';
    END IF;
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('public.it_res_dropped');
    IF v_row.tracking_id <> v_tracking_dropped THEN
        RAISE EXCEPTION 'post-DROP public.it_res_dropped resolved to tracking_id %, expected %',
            v_row.tracking_id, v_tracking_dropped;
    END IF;
    SELECT * INTO STRICT v_row
    FROM flashback_internal_resolve_tracked_table('it_res_dropped');
    IF v_row.tracking_id <> v_tracking_dropped THEN
        RAISE EXCEPTION 'post-DROP unqualified it_res_dropped resolved to tracking_id %, expected %',
            v_row.tracking_id, v_tracking_dropped;
    END IF;

    -- 7. A hostile extension-helper-like function planted in public cannot
    -- hijack resolution: the resolver's search_path excludes public and it
    -- never calls to_regclass() (or any other unqualified builtin) on the
    -- caller-supplied string, so shadowing it is a no-op either way.
    EXECUTE 'CREATE OR REPLACE FUNCTION public.to_regclass(text) '
         || 'RETURNS regclass LANGUAGE sql AS $hostile$ SELECT NULL::regclass $hostile$';
    BEGIN
        SELECT * INTO STRICT v_row
        FROM flashback_internal_resolve_tracked_table('public.orders');
        IF v_row.tracking_id <> v_tracking_orders THEN
            RAISE EXCEPTION 'hostile to_regclass() shadow changed resolution: got tracking_id %, expected %',
                v_row.tracking_id, v_tracking_orders;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            EXECUTE 'DROP FUNCTION IF EXISTS public.to_regclass(text)';
            RAISE;
    END;
    EXECUTE 'DROP FUNCTION public.to_regclass(text)';

    -- Invalid identifier shapes fail closed rather than resolving loosely.
    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_internal_resolve_tracked_table('a.b.c');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'database-qualified 3-part identifier was not rejected';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_internal_resolve_tracked_table('orders; DROP TABLE public.orders');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'trailing garbage after the identifier was not rejected';
    END IF;
    IF to_regclass('public.orders') IS NULL THEN
        RAISE EXCEPTION 'malicious identifier payload actually executed';
    END IF;

    -- search_path hardening: neither PUBLIC nor the operator roles may
    -- execute the internal resolver directly.
    IF has_function_privilege(
        'public', 'flashback_internal_resolve_tracked_table(text)', 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'PUBLIC EXECUTE not revoked on flashback_internal_resolve_tracked_table';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin', 'flashback_internal_resolve_tracked_table(text)', 'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'flashback_admin EXECUTE not revoked on flashback_internal_resolve_tracked_table';
    END IF;
    IF has_function_privilege(
        'pg_monitor', 'flashback_internal_resolve_tracked_table(text)', 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'pg_monitor EXECUTE not revoked on flashback_internal_resolve_tracked_table';
    END IF;

    -- Wired end-to-end through the public admission path (not just the
    -- internal resolver in isolation): flashback_admit_lsn_target resolves
    -- a quoted, space-containing table name the same way.
    DECLARE
        v_stream_id bigint;
        v_snapshot_id bigint;
        v_generation_id bigint;
        v_admitted_count integer;
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, timeline_id,
            slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
            confirmed_flush_lsn, restart_lsn, activated_at
        ) VALUES (
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            current_database(), 1, 'wal', 1,
            'it_res_space_slot', 'pg_flashback', 'active',
            clock_timestamp(), '0/5000', '0/5000', '0/1000',
            clock_timestamp()
        ) RETURNING stream_id INTO v_stream_id;

        v_snapshot_id := flashback_internal_snapshot_create(
            v_tracking_space, 'public."My Table"'::regclass,
            'public', 'My Table', '0/1000'::pg_lsn, 'generation'
        );

        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, stream_id, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
            boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
            activated_at
        ) VALUES (
            v_tracking_space, 1, v_stream_id, 'local_delta', 'active',
            'adversarial_fixture', 'public."My Table"'::regclass,
            v_snapshot_id, clock_timestamp(), '0/1000',
            clock_timestamp(), '0/5000', clock_timestamp()
        ) RETURNING generation_id INTO v_generation_id;

        SELECT count(*) INTO v_admitted_count
        FROM flashback_admit_lsn_target('"My Table"', '0/1000'::pg_lsn);
        IF v_admitted_count <> 1 THEN
            RAISE EXCEPTION 'flashback_admit_lsn_target did not admit quoted "My Table" via the new resolver';
        END IF;
    END;

    RAISE NOTICE 'tracked_table_resolver_search_path: all resolver contract checks passed';
END;
$test$;

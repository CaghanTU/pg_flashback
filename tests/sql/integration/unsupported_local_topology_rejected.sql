-- Fail-closed topology gates for the local DROP product profile.
DO $tv$
DECLARE
    ok boolean := false;
BEGIN
    -- Partitioned parent
    DROP TABLE IF EXISTS public.it_unsup_part CASCADE;
    CREATE TABLE public.it_unsup_part (id int, ts date NOT NULL) PARTITION BY RANGE (ts);
    CREATE TABLE public.it_unsup_part_2026 PARTITION OF public.it_unsup_part
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    BEGIN
        PERFORM flashback_track('public.it_unsup_part');
        RAISE EXCEPTION 'expected partitioned parent rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'partitioned tables are not supported' THEN
            RAISE;
        END IF;
    END;

    -- Leaf partition
    BEGIN
        PERFORM flashback_track('public.it_unsup_part_2026');
        RAISE EXCEPTION 'expected leaf partition rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'partitions are not supported' THEN
            RAISE;
        END IF;
    END;
    DROP TABLE public.it_unsup_part CASCADE;

    -- UNLOGGED
    DROP TABLE IF EXISTS public.it_unsup_unlogged;
    CREATE UNLOGGED TABLE public.it_unsup_unlogged (id int PRIMARY KEY);
    BEGIN
        PERFORM flashback_track('public.it_unsup_unlogged');
        RAISE EXCEPTION 'expected UNLOGGED rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'permanent LOGGED tables are supported' THEN
            RAISE;
        END IF;
    END;
    DROP TABLE public.it_unsup_unlogged;

    -- TEMP
    CREATE TEMP TABLE it_unsup_temp (id int PRIMARY KEY);
    BEGIN
        PERFORM flashback_track('it_unsup_temp');
        RAISE EXCEPTION 'expected TEMP rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'temporary tables are not supported' THEN
            RAISE;
        END IF;
    END;
    DROP TABLE it_unsup_temp;

    -- Materialized view
    DROP MATERIALIZED VIEW IF EXISTS public.it_unsup_mview;
    DROP TABLE IF EXISTS public.it_unsup_mview_base;
    CREATE TABLE public.it_unsup_mview_base (id int PRIMARY KEY, v text);
    INSERT INTO public.it_unsup_mview_base VALUES (1, 'a');
    CREATE MATERIALIZED VIEW public.it_unsup_mview AS SELECT * FROM public.it_unsup_mview_base;
    BEGIN
        PERFORM flashback_track('public.it_unsup_mview');
        RAISE EXCEPTION 'expected matview rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'materialized views are not supported' THEN
            RAISE;
        END IF;
    END;
    DROP MATERIALIZED VIEW public.it_unsup_mview;
    DROP TABLE public.it_unsup_mview_base;

    -- Foreign table (skip if postgres_fdw unavailable)
    BEGIN
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        DROP SERVER IF EXISTS it_unsup_fdw_server CASCADE;
        CREATE SERVER it_unsup_fdw_server FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '127.0.0.1', dbname 'postgres', port '1');
        CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER it_unsup_fdw_server;
        DROP FOREIGN TABLE IF EXISTS public.it_unsup_foreign;
        CREATE FOREIGN TABLE public.it_unsup_foreign (id int)
            SERVER it_unsup_fdw_server OPTIONS (table_name 'pg_class');
        BEGIN
            PERFORM flashback_track('public.it_unsup_foreign');
            RAISE EXCEPTION 'expected foreign table rejection';
        EXCEPTION WHEN others THEN
            IF SQLERRM !~ 'foreign tables are not supported' THEN
                RAISE;
            END IF;
        END;
        DROP FOREIGN TABLE public.it_unsup_foreign;
        DROP SERVER it_unsup_fdw_server CASCADE;
        ok := true;
    EXCEPTION WHEN undefined_file OR feature_not_supported OR undefined_object THEN
        -- postgres_fdw not available in this build; topology gate still covered above.
        ok := true;
    END;

    IF NOT ok THEN
        RAISE EXCEPTION 'foreign-table topology probe failed unexpectedly';
    END IF;
END;
$tv$;

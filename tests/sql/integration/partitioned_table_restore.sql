-- First-release contract: partitioned tables are rejected fail-closed.
DO $tv$
BEGIN
    DROP TABLE IF EXISTS public.it_partitioned CASCADE;

    CREATE TABLE public.it_partitioned (
        id    serial,
        val   text NOT NULL,
        ts    date NOT NULL,
        PRIMARY KEY (id, ts)
    ) PARTITION BY RANGE (ts);

    CREATE TABLE public.it_part_2025 PARTITION OF public.it_partitioned
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    CREATE TABLE public.it_part_2026 PARTITION OF public.it_partitioned
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

    BEGIN
        PERFORM flashback_track('public.it_partitioned');
        RAISE EXCEPTION 'expected partitioned parent rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'partitioned tables are not supported' THEN
            RAISE;
        END IF;
    END;

    DROP TABLE public.it_partitioned CASCADE;
END;
$tv$;

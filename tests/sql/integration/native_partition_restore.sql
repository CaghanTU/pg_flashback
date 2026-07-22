-- First-release contract: native partitioned tables are rejected fail-closed.
DO $tv$
BEGIN
    DROP TABLE IF EXISTS public.it_native_part CASCADE;

    CREATE TABLE public.it_native_part (
        id    serial,
        val   text NOT NULL,
        yr    int  NOT NULL,
        PRIMARY KEY (id, yr)
    ) PARTITION BY RANGE (yr);

    CREATE TABLE public.it_native_part_2025
        PARTITION OF public.it_native_part FOR VALUES FROM (2025) TO (2026);
    CREATE TABLE public.it_native_part_2026
        PARTITION OF public.it_native_part FOR VALUES FROM (2026) TO (2027);

    BEGIN
        PERFORM flashback_track('public.it_native_part');
        RAISE EXCEPTION 'expected partitioned parent rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'partitioned tables are not supported' THEN
            RAISE;
        END IF;
    END;

    DROP TABLE public.it_native_part CASCADE;
END;
$tv$;

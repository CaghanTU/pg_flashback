-- First-release contract: leaf-partition restore paths are rejected at track time.
DO $tv$
BEGIN
    DROP TABLE IF EXISTS public.it_leaf_restore CASCADE;

    CREATE TABLE public.it_leaf_restore (
        id   serial,
        yr   int  NOT NULL,
        val  text NOT NULL,
        PRIMARY KEY (id, yr)
    ) PARTITION BY RANGE (yr);

    CREATE TABLE public.it_leaf_restore_2025
        PARTITION OF public.it_leaf_restore
        FOR VALUES FROM (2025) TO (2026);

    CREATE TABLE public.it_leaf_restore_2026
        PARTITION OF public.it_leaf_restore
        FOR VALUES FROM (2026) TO (2027);

    BEGIN
        PERFORM flashback_track('public.it_leaf_restore_2025');
        RAISE EXCEPTION 'expected leaf partition rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'partitions are not supported' THEN
            RAISE;
        END IF;
    END;

    DROP TABLE public.it_leaf_restore CASCADE;
END;
$tv$;

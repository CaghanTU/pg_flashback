-- First-release contract: classical INHERITS parents are rejected fail-closed
-- because DROP ... CASCADE would destroy child tables outside the single-table
-- local DROP product promise.
DO $tv$
BEGIN
    DROP TABLE IF EXISTS public.it_inh_cars    CASCADE;
    DROP TABLE IF EXISTS public.it_inh_trucks  CASCADE;
    DROP TABLE IF EXISTS public.it_inh_fleet   CASCADE;

    CREATE TABLE public.it_inh_fleet (
        id    SERIAL PRIMARY KEY,
        make  TEXT NOT NULL,
        year  INT  NOT NULL
    );
    CREATE TABLE public.it_inh_cars (
        doors INT DEFAULT 4
    ) INHERITS (public.it_inh_fleet);
    CREATE TABLE public.it_inh_trucks (
        payload_t NUMERIC
    ) INHERITS (public.it_inh_fleet);

    BEGIN
        PERFORM flashback_track('public.it_inh_fleet');
        RAISE EXCEPTION 'expected inheritance-parent rejection';
    EXCEPTION WHEN others THEN
        IF SQLERRM !~ 'inheritance children' THEN
            RAISE;
        END IF;
    END;

    DROP TABLE IF EXISTS public.it_inh_cars    CASCADE;
    DROP TABLE IF EXISTS public.it_inh_trucks  CASCADE;
    DROP TABLE IF EXISTS public.it_inh_fleet   CASCADE;
END;
$tv$;

DO $tv$
DECLARE got_error boolean := false;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_notracked;
    CREATE TABLE public.it_edge_notracked (id int primary key, v text);
    BEGIN
        PERFORM flashback_restore('public.it_edge_notracked', clock_timestamp());
    EXCEPTION WHEN OTHERS THEN
        got_error := true;
    END;

    IF NOT got_error THEN
        RAISE EXCEPTION 'restore without tracking should fail';
    END IF;
END;
$tv$;

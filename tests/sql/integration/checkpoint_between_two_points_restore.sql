-- Checkpoint API is disabled for WAL generations; restore to an intermediate
-- seam COMMIT LSN instead of flashback_checkpoint().
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_mid_lsn pg_lsn := '0/2000'::pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_between;
    CREATE TABLE public.it_ckpt_between (id int primary key, v text);
    INSERT INTO public.it_ckpt_between VALUES (1, 'v0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_ckpt_between') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    UPDATE public.it_ckpt_between SET v = 'v1' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_mid_lsn,
        clock_timestamp(),
        961001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"v0"}'::jsonb,
                'new', '{"id":1,"v":"v1"}'::jsonb
            )
        )
    );

    UPDATE public.it_ckpt_between SET v = 'v2' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        961002,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"v1"}'::jsonb,
                'new', '{"id":1,"v":"v2"}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_ckpt_between', v_mid_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_between WHERE id = 1 AND v = 'v1') THEN
        RAISE EXCEPTION 'checkpoint between restore failed';
    END IF;
END;
$tv$;

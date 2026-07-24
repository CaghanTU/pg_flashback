-- Checkpoint API is disabled for WAL generations; exercise the same scenario
-- with seam LSN targets (base snapshot as the "checkpoint" point).
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_after;
    CREATE TABLE public.it_ckpt_after (id int primary key, v text);
    INSERT INTO public.it_ckpt_after VALUES (1, 'a');

    SELECT flashback_test_bootstrap_lifecycle('public.it_ckpt_after') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_ckpt_after SET v = 'b' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        960001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"a"}'::jsonb,
                'new', '{"id":1,"v":"b"}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_ckpt_after', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_after WHERE id = 1 AND v = 'a') THEN
        RAISE EXCEPTION 'checkpoint restore failed';
    END IF;
END;
$tv$;

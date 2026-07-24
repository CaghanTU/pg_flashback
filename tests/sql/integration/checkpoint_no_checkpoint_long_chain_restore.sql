DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    i int;
    v_events jsonb := '[]'::jsonb;
    v_old int := 0;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_long;
    CREATE TABLE public.it_ckpt_long (id int primary key, v int);
    INSERT INTO public.it_ckpt_long VALUES (1,0);

    SELECT flashback_test_bootstrap_lifecycle('public.it_ckpt_long') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    FOR i IN 1..60 LOOP
      EXECUTE format('UPDATE public.it_ckpt_long SET v=%s WHERE id=1', i);
      v_events := v_events || jsonb_build_array(
          jsonb_build_object(
              'op', 'UPDATE',
              'old', jsonb_build_object('id', 1, 'v', v_old),
              'new', jsonb_build_object('id', 1, 'v', i)
          )
      );
      v_old := i;
    END LOOP;

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        937001,
        v_events
    );

    PERFORM flashback_restore_lsn('public.it_ckpt_long', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_long WHERE id=1 AND v=0) THEN
      RAISE EXCEPTION 'no-checkpoint long-chain restore failed';
    END IF;
END;
$tv$;

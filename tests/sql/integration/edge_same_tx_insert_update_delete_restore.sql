DROP TABLE IF EXISTS public.it_edge_same_tx;
CREATE TABLE public.it_edge_same_tx (id int primary key, status text);

DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.it_edge_same_tx') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    -- Live table ends empty after I/U/D; inject the same ordered commit.
    INSERT INTO public.it_edge_same_tx VALUES (1, 'a');
    UPDATE public.it_edge_same_tx SET status = 'b' WHERE id = 1;
    DELETE FROM public.it_edge_same_tx WHERE id = 1;

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        940001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"status":"a"}'::jsonb),
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"status":"a"}'::jsonb,
                'new', '{"id":1,"status":"b"}'::jsonb
            ),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"status":"b"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_edge_same_tx', v_boundary_lsn);
    IF EXISTS (SELECT 1 FROM public.it_edge_same_tx) THEN
        RAISE EXCEPTION 'same tx I/U/D restore failed';
    END IF;
END;
$tv$;

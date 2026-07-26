-- Coverage gap fill for flashback_materialize_lsn's column-metadata cache.
-- No existing DML restore test exercises a primary-key-less table.
-- flashback_build_update_set's pk_cols is an empty array (from schema_def's
-- empty primary_key), so pk_predicate must come back NULL and the caller
-- must fall through to the delete-old-then-insert-new path using
-- flashback_build_predicate/flashback_build_insert_parts against the *old*
-- row image -- never attempt an UPDATE with no WHERE-clause identity.
DROP TABLE IF EXISTS public.it_meta_no_pk CASCADE;

CREATE TABLE public.it_meta_no_pk (
    tag   text NOT NULL,
    score int  NOT NULL
);

DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/3000'::pg_lsn;
    v_cnt bigint;
    v_score int;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.it_meta_no_pk') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_meta_no_pk VALUES ('alpha', 1), ('beta', 2);
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/2000'::pg_lsn, clock_timestamp(), 953001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"tag":"alpha","score":1}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"tag":"beta","score":2}'::jsonb)
        )
    );

    UPDATE public.it_meta_no_pk SET score = 100 WHERE tag = 'alpha';
    PERFORM flashback_test_inject_commit(
        v_tracking_id, v_point_lsn, clock_timestamp(), 953002,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE',
                'old', '{"tag":"alpha","score":1}'::jsonb,
                'new', '{"tag":"alpha","score":100}'::jsonb)
        )
    );

    DELETE FROM public.it_meta_no_pk WHERE tag = 'beta';
    INSERT INTO public.it_meta_no_pk VALUES ('gamma', 3);
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/4000'::pg_lsn, clock_timestamp(), 953003,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"tag":"beta","score":2}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"tag":"gamma","score":3}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_meta_no_pk', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_meta_no_pk;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'no-PK restore: expected 2 rows at boundary, got %', v_cnt;
    END IF;
    SELECT score INTO v_score FROM public.it_meta_no_pk WHERE tag = 'alpha';
    IF v_score <> 100 THEN
        RAISE EXCEPTION 'no-PK restore: tag=alpha expected score 100 (updated), got %', v_score;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_meta_no_pk WHERE tag = 'beta') THEN
        RAISE EXCEPTION 'no-PK restore: tag=beta (pre-delete) should still exist at boundary';
    END IF;
    IF EXISTS (SELECT 1 FROM public.it_meta_no_pk WHERE tag = 'gamma') THEN
        RAISE EXCEPTION 'no-PK restore: tag=gamma (post-boundary insert) should not exist';
    END IF;
END;
$tv$;

DROP TABLE IF EXISTS public.it_meta_no_pk CASCADE;

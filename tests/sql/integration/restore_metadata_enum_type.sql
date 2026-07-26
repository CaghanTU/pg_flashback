-- Coverage gap fill for flashback_materialize_lsn's column-metadata cache
-- (collected once per restore instead of re-probed from pg_attribute/pg_type
-- per delta_log event). No existing DML restore test exercises an enum
-- column; enums are accepted by flashback_local_compatibility (unlike
-- generated columns, which are rejected at track time) and go through the
-- same flashback_build_predicate/flashback_build_insert_parts/
-- flashback_build_update_set helpers driven by the cached col_meta, so this
-- needs its own direct proof.
DROP TABLE IF EXISTS public.it_meta_enum CASCADE;
DROP TYPE IF EXISTS public.it_meta_status CASCADE;

CREATE TYPE public.it_meta_status AS ENUM ('pending', 'active', 'closed');
CREATE TABLE public.it_meta_enum (
    id     int PRIMARY KEY,
    status public.it_meta_status NOT NULL
);

DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/3000'::pg_lsn;
    v_status public.it_meta_status;
    v_cnt bigint;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.it_meta_enum') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_meta_enum VALUES (1, 'pending'), (2, 'active');
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/2000'::pg_lsn, clock_timestamp(), 951001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"status":"pending"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"status":"active"}'::jsonb)
        )
    );

    UPDATE public.it_meta_enum SET status = 'active' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id, v_point_lsn, clock_timestamp(), 951002,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE',
                'old', '{"id":1,"status":"pending"}'::jsonb,
                'new', '{"id":1,"status":"active"}'::jsonb)
        )
    );

    UPDATE public.it_meta_enum SET status = 'closed' WHERE id = 2;
    DELETE FROM public.it_meta_enum WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/4000'::pg_lsn, clock_timestamp(), 951003,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE',
                'old', '{"id":2,"status":"active"}'::jsonb,
                'new', '{"id":2,"status":"closed"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"status":"active"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_meta_enum', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_meta_enum;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'enum restore: expected 2 rows at boundary, got %', v_cnt;
    END IF;
    SELECT status INTO v_status FROM public.it_meta_enum WHERE id = 1;
    IF v_status <> 'active' THEN
        RAISE EXCEPTION 'enum restore: id=1 expected status active, got %', v_status;
    END IF;
    SELECT status INTO v_status FROM public.it_meta_enum WHERE id = 2;
    IF v_status <> 'active' THEN
        RAISE EXCEPTION 'enum restore: id=2 expected status active (pre-close), got %', v_status;
    END IF;
END;
$tv$;

DROP TABLE IF EXISTS public.it_meta_enum CASCADE;
DROP TYPE IF EXISTS public.it_meta_status CASCADE;

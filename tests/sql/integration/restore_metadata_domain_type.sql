-- Coverage gap fill for flashback_materialize_lsn's column-metadata cache.
-- No existing DML restore test exercises a domain column. The cached
-- col_meta type string must be the domain name itself (format_type resolves
-- to the domain, not its base type), and a CHECK-constrained domain must
-- still cast/replay correctly through flashback_build_insert_parts/
-- flashback_build_update_set/flashback_build_predicate.
DROP TABLE IF EXISTS public.it_meta_domain CASCADE;
DROP DOMAIN IF EXISTS public.it_meta_positive_int CASCADE;

CREATE DOMAIN public.it_meta_positive_int AS integer CHECK (VALUE > 0);
CREATE TABLE public.it_meta_domain (
    id     int PRIMARY KEY,
    amount public.it_meta_positive_int NOT NULL
);

DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/3000'::pg_lsn;
    v_amount int;
    v_cnt bigint;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.it_meta_domain') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_meta_domain VALUES (1, 10), (2, 20);
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/2000'::pg_lsn, clock_timestamp(), 952001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"amount":10}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"amount":20}'::jsonb)
        )
    );

    UPDATE public.it_meta_domain SET amount = 99 WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id, v_point_lsn, clock_timestamp(), 952002,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE',
                'old', '{"id":1,"amount":10}'::jsonb,
                'new', '{"id":1,"amount":99}'::jsonb)
        )
    );

    DELETE FROM public.it_meta_domain WHERE id = 2;
    PERFORM flashback_test_inject_commit(
        v_tracking_id, '0/4000'::pg_lsn, clock_timestamp(), 952003,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"amount":20}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_meta_domain', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_meta_domain;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'domain restore: expected 2 rows at boundary, got %', v_cnt;
    END IF;
    SELECT amount INTO v_amount FROM public.it_meta_domain WHERE id = 1;
    IF v_amount <> 99 THEN
        RAISE EXCEPTION 'domain restore: id=1 expected amount 99, got %', v_amount;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_meta_domain WHERE id = 2) THEN
        RAISE EXCEPTION 'domain restore: id=2 (pre-delete) should still exist at boundary';
    END IF;
END;
$tv$;

-- No terminal DROP TABLE/DROP DOMAIN CASCADE: pg_test rolls back this whole
-- transaction, and the restore just performed leaves the successor
-- generation "building" (not yet active) until that rollback/commit is
-- observed, so a same-transaction DROP touching the tracked table here
-- (directly or via domain CASCADE) would trip the schema-contract guard.

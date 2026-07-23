-- Independent restore proof unit checks (no full protect/recover).
DO $$
DECLARE
    v_schema_def jsonb;
    v_order jsonb;
    v_fp1 text;
    v_fp2 text;
    v_expected jsonb;
    v_actual jsonb;
    v_cmp jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_restore_verify CASCADE;
    CREATE TABLE public.it_restore_verify (
        id int PRIMARY KEY,
        val text NOT NULL
    );
    INSERT INTO public.it_restore_verify VALUES (1, 'alpha'), (2, 'beta');

    v_schema_def := flashback_collect_schema_def('public.it_restore_verify'::regclass);
    v_order := flashback_fingerprint_order_spec(v_schema_def);
    IF v_order->>'mode' IS DISTINCT FROM 'pk' THEN
        RAISE EXCEPTION 'expected pk order_spec, got %', v_order;
    END IF;

    v_fp1 := flashback_relation_full_data_fingerprint(
        'public.it_restore_verify'::regclass, v_order
    );
    -- Same order_spec must be stable.
    v_fp2 := flashback_relation_full_data_fingerprint(
        'public.it_restore_verify'::regclass, v_order
    );
    IF v_fp1 IS DISTINCT FROM v_fp2 THEN
        RAISE EXCEPTION 'fingerprint not deterministic';
    END IF;

    -- Old tautological capture path must be removed.
    BEGIN
        PERFORM flashback_capture_restore_expected_proof('public.it_restore_verify'::regclass);
        RAISE EXCEPTION 'expected capture_restore_expected_proof to be removed';
    EXCEPTION WHEN feature_not_supported THEN
        NULL;
    END;

    -- Multiplicity: duplicate rows without PK use full_row mode.
    DROP TABLE IF EXISTS public.it_restore_dupes CASCADE;
    CREATE TABLE public.it_restore_dupes (val text);
    INSERT INTO public.it_restore_dupes VALUES ('x'), ('x'), ('y');
    v_schema_def := jsonb_build_object(
        'primary_key', '[]'::jsonb,
        'columns', jsonb_build_array(jsonb_build_object('name', 'val', 'type', 'text'))
    );
    v_order := flashback_fingerprint_order_spec(v_schema_def);
    IF v_order->>'mode' IS DISTINCT FROM 'full_row' THEN
        RAISE EXCEPTION 'expected full_row order_spec';
    END IF;
    v_fp1 := flashback_relation_full_data_fingerprint(
        'public.it_restore_dupes'::regclass, v_order
    );
    DELETE FROM public.it_restore_dupes WHERE ctid = (
        SELECT ctid FROM public.it_restore_dupes WHERE val = 'x' LIMIT 1
    );
    v_fp2 := flashback_relation_full_data_fingerprint(
        'public.it_restore_dupes'::regclass, v_order
    );
    IF v_fp1 IS NOT DISTINCT FROM v_fp2 THEN
        RAISE EXCEPTION 'duplicate multiplicity not reflected in fingerprint';
    END IF;
END;
$$;

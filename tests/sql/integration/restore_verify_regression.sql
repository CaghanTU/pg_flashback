-- Post-restore verification helpers: self-consistency and fail-closed mismatch.
DO $tv$
DECLARE
    v_proof jsonb;
    v_result jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_restore_verify CASCADE;
    CREATE TABLE public.it_restore_verify (
        id int PRIMARY KEY,
        val text NOT NULL
    );
    INSERT INTO public.it_restore_verify VALUES (1, 'alpha'), (2, 'beta');

    v_proof := flashback_capture_restore_expected_proof('public.it_restore_verify'::regclass);
    IF v_proof->'inventory'->>'row_count' IS DISTINCT FROM '2' THEN
        RAISE EXCEPTION 'inventory row_count expected 2, got %',
            v_proof->'inventory'->>'row_count';
    END IF;

    v_result := flashback_verify_restored_relation(
        'public.it_restore_verify'::regclass,
        v_proof,
        NULL
    );
    IF v_result->>'status' IS DISTINCT FROM 'passed' THEN
        RAISE EXCEPTION 'self-consistency verify expected passed, got %', v_result;
    END IF;

    UPDATE public.it_restore_verify SET val = 'mutated' WHERE id = 1;
    BEGIN
        PERFORM flashback_verify_restored_relation(
            'public.it_restore_verify'::regclass,
            v_proof,
            NULL
        );
        RAISE EXCEPTION 'verify should fail after data mutation';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLERRM NOT LIKE '%data fingerprint mismatch%' THEN
                RAISE;
            END IF;
    END;
END;
$tv$;

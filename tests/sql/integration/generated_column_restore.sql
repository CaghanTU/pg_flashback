-- Generated columns are rejected by flashback_local_compatibility (staging).
DO $tv$
DECLARE
    v_report jsonb;
    v_err text;
BEGIN
    DROP TABLE IF EXISTS public.it_gen_col CASCADE;
    CREATE TABLE public.it_gen_col (
        id        int PRIMARY KEY,
        first_nm  text NOT NULL,
        last_nm   text NOT NULL,
        full_nm   text GENERATED ALWAYS AS (first_nm || ' ' || last_nm) STORED
    );

    v_report := flashback_local_compatibility('public.it_gen_col'::regclass);
    IF COALESCE((v_report->>'supported')::boolean, true) THEN
        RAISE EXCEPTION 'expected generated columns to be unsupported, got %', v_report;
    END IF;
    IF NOT (v_report->'rejected_features' ? 'generated_columns') THEN
        RAISE EXCEPTION 'expected rejected_features to include generated_columns: %', v_report;
    END IF;

    BEGIN
        PERFORM flashback_track('public.it_gen_col');
        RAISE EXCEPTION 'flashback_track unexpectedly accepted generated columns';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
        IF v_err NOT ILIKE '%not compatible%generated_columns%'
           AND v_err NOT ILIKE '%generated%' THEN
            RAISE EXCEPTION 'unexpected track error: %', v_err;
        END IF;
    END;
END;
$tv$;

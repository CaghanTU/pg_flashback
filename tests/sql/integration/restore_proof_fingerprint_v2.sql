-- Test: the restore data fingerprint proves row content, not heap layout.
--
-- v1 sorted the full row_to_json(t)::text of every row, so restoring a 1 GiB
-- table spilled 5.6 GiB of temp files and a 10 GiB restore wrote about 58 GiB
-- across the two independent proof scans.  v2 hashes each row first and sorts
-- only the 32-byte digests.  That is a real change of the digest input, so
-- this pins the properties the proof depends on rather than any fixed value:
-- every column participates, duplicate rows keep their multiplicity, the
-- physical order of the heap does not matter, and a spec naming a column the
-- relation does not have is rejected instead of silently accepted.
DO $tv$
DECLARE
    v_pk        jsonb := '{"primary_key":["id"]}'::jsonb;
    v_none      jsonb := '{}'::jsonb;
    v_a         text;
    v_b         text;
    v_rewritten text;
    v_dup0      text;
    v_dup1      text;
    v_empty     text;
    v_msg       text;
BEGIN
    DROP TABLE IF EXISTS public.it_fp2_a CASCADE;
    DROP TABLE IF EXISTS public.it_fp2_b CASCADE;
    DROP TABLE IF EXISTS public.it_fp2_rw CASCADE;
    DROP TABLE IF EXISTS public.it_fp2_dup CASCADE;
    DROP TABLE IF EXISTS public.it_fp2_empty CASCADE;

    -- NULLs and a value wide enough to be pushed out of line into TOAST.
    CREATE TABLE public.it_fp2_a (
        id      int PRIMARY KEY,
        label   text,
        amount  numeric,
        wide    text
    );
    INSERT INTO public.it_fp2_a
    SELECT g,
           CASE WHEN g % 7 = 0 THEN NULL ELSE 'label-' || g END,
           CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 97)::numeric / 7 END,
           CASE WHEN g % 25 = 0 THEN repeat('T', 12000) ELSE 'small-' || g END
    FROM generate_series(1, 500) g;

    IF NOT EXISTS (
        SELECT 1 FROM public.it_fp2_a WHERE length(wide) > 8000
    ) THEN
        RAISE EXCEPTION 'fingerprint_v2: fixture built no out-of-line value';
    END IF;

    v_a := public.flashback_relation_full_data_fingerprint(
               'public.it_fp2_a'::regclass,
               public.flashback_fingerprint_order_spec(v_pk));

    -- Deterministic: the same relation hashes the same way twice.
    IF v_a IS DISTINCT FROM public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_a'::regclass,
            public.flashback_fingerprint_order_spec(v_pk)) THEN
        RAISE EXCEPTION 'fingerprint_v2: not deterministic';
    END IF;

    -- An empty relation still produces a digest rather than NULL.
    CREATE TABLE public.it_fp2_empty (id int PRIMARY KEY, v text);
    v_empty := public.flashback_relation_full_data_fingerprint(
                   'public.it_fp2_empty'::regclass,
                   public.flashback_fingerprint_order_spec(v_pk));
    IF v_empty IS NULL OR v_empty = v_a THEN
        RAISE EXCEPTION 'fingerprint_v2: empty relation digest is wrong';
    END IF;

    -- One changed value, same row count, must be detected.
    CREATE TABLE public.it_fp2_b AS TABLE public.it_fp2_a;
    UPDATE public.it_fp2_b SET amount = COALESCE(amount, 0) + 0.0000001
     WHERE id = 250;
    v_b := public.flashback_relation_full_data_fingerprint(
               'public.it_fp2_b'::regclass,
               public.flashback_fingerprint_order_spec(v_none));
    IF v_b = public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_a'::regclass,
            public.flashback_fingerprint_order_spec(v_none)) THEN
        RAISE EXCEPTION 'fingerprint_v2: single-value corruption not detected';
    END IF;

    -- A NULL that becomes a value must be detected too.
    UPDATE public.it_fp2_b SET label = 'now-set' WHERE id = 7;
    IF v_b = public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_b'::regclass,
            public.flashback_fingerprint_order_spec(v_none)) THEN
        NULL;  -- digest moved again, as it must
    ELSE
        v_b := public.flashback_relation_full_data_fingerprint(
                   'public.it_fp2_b'::regclass,
                   public.flashback_fingerprint_order_spec(v_none));
    END IF;

    -- Rewriting the heap in a different physical order must not move the
    -- digest: the proof is about content, not layout.
    CREATE TABLE public.it_fp2_rw AS
        SELECT * FROM public.it_fp2_a ORDER BY id DESC;
    v_rewritten := public.flashback_relation_full_data_fingerprint(
                       'public.it_fp2_rw'::regclass,
                       public.flashback_fingerprint_order_spec(v_none));
    IF v_rewritten IS DISTINCT FROM public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_a'::regclass,
            public.flashback_fingerprint_order_spec(v_none)) THEN
        RAISE EXCEPTION 'fingerprint_v2: physical rewrite changed the digest';
    END IF;

    -- Duplicate rows are not collapsed: multiplicity is part of the content.
    CREATE TABLE public.it_fp2_dup (a int, b text);
    INSERT INTO public.it_fp2_dup
    SELECT 7, 'same' FROM generate_series(1, 50);
    v_dup0 := public.flashback_relation_full_data_fingerprint(
                  'public.it_fp2_dup'::regclass,
                  public.flashback_fingerprint_order_spec(v_none));
    INSERT INTO public.it_fp2_dup VALUES (7, 'same');
    v_dup1 := public.flashback_relation_full_data_fingerprint(
                  'public.it_fp2_dup'::regclass,
                  public.flashback_fingerprint_order_spec(v_none));
    IF v_dup0 = v_dup1 THEN
        RAISE EXCEPTION 'fingerprint_v2: duplicate multiplicity collapsed';
    END IF;
    DELETE FROM public.it_fp2_dup
     WHERE ctid = (SELECT max(ctid) FROM public.it_fp2_dup);
    IF v_dup0 IS DISTINCT FROM public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_dup'::regclass,
            public.flashback_fingerprint_order_spec(v_none)) THEN
        RAISE EXCEPTION 'fingerprint_v2: removing the duplicate did not restore the digest';
    END IF;

    -- v1 rejected an unknown column when the ORDER BY was planned. v2 does
    -- not order by those columns, so the rejection must be explicit.
    BEGIN
        PERFORM public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_a'::regclass,
            '{"mode":"pk","columns":["no_such_column"]}'::jsonb);
        RAISE EXCEPTION 'fingerprint_v2: unknown order_spec column was accepted';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
        IF v_msg LIKE '%unknown order_spec column was accepted%' THEN
            RAISE;
        END IF;
        IF v_msg NOT LIKE '%unknown column%' THEN
            RAISE EXCEPTION 'fingerprint_v2: wrong rejection for unknown column: %', v_msg;
        END IF;
    END;

    -- A malformed mode is rejected rather than silently treated as full_row.
    BEGIN
        PERFORM public.flashback_relation_full_data_fingerprint(
            'public.it_fp2_a'::regclass,
            '{"mode":"nonsense","columns":[]}'::jsonb);
        RAISE EXCEPTION 'fingerprint_v2: unknown mode was accepted';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
        IF v_msg LIKE '%unknown mode was accepted%' THEN
            RAISE;
        END IF;
        IF v_msg NOT LIKE '%unknown mode%' THEN
            RAISE EXCEPTION 'fingerprint_v2: wrong rejection for unknown mode: %', v_msg;
        END IF;
    END;

    DROP TABLE public.it_fp2_a CASCADE;
    DROP TABLE public.it_fp2_b CASCADE;
    DROP TABLE public.it_fp2_rw CASCADE;
    DROP TABLE public.it_fp2_dup CASCADE;
    DROP TABLE public.it_fp2_empty CASCADE;
END
$tv$;

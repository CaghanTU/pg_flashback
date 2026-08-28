-- Test: constraint definitions compare stably across a PostgreSQL re-render.
--
-- pg_get_constraintdef() is not round-trip stable for an IN-list over a
-- varchar column: PostgreSQL renders the array cast at array level, and once
-- that text is re-parsed (exactly what restore does when it rebuilds a
-- relation from schema_def) it comes back distributed over the elements.
-- Comparing the raw strings made every table carrying CHECK (col IN (...))
-- on a varchar column fail restore verification with "inventory digest
-- mismatch" and therefore be unrecoverable, although its data verified clean.
--
-- This proves (a) the instability is real, (b) canonicalization collapses it,
-- and (c) canonicalization still distinguishes genuinely different
-- constraints, including a pure precedence change.
DO $tv$
DECLARE
    v_def_original text;
    v_def_reparsed text;
BEGIN
    DROP TABLE IF EXISTS public.it_cdef_a CASCADE;
    DROP TABLE IF EXISTS public.it_cdef_b CASCADE;

    CREATE TABLE public.it_cdef_a (
        kind varchar(16) NOT NULL,
        CONSTRAINT it_cdef_chk CHECK (kind IN ('mixed', 'dense'))
    );
    -- contype='c' is required, not incidental: PostgreSQL 18 records NOT NULL
    -- as a real named constraint in pg_constraint, so an unfiltered read here
    -- can return the NOT NULL row and then try to re-apply it as a CHECK.
    SELECT pg_get_constraintdef(oid) INTO v_def_original
    FROM pg_constraint
    WHERE conrelid = 'public.it_cdef_a'::regclass
      AND contype = 'c' AND conname = 'it_cdef_chk';

    -- Rebuild the same constraint from its own rendered text, the way a
    -- restore rebuilds it from schema_def.
    CREATE TABLE public.it_cdef_b (kind varchar(16) NOT NULL);
    EXECUTE format('ALTER TABLE public.it_cdef_b ADD CONSTRAINT it_cdef_chk %s',
                   v_def_original);
    SELECT pg_get_constraintdef(oid) INTO v_def_reparsed
    FROM pg_constraint
    WHERE conrelid = 'public.it_cdef_b'::regclass
      AND contype = 'c' AND conname = 'it_cdef_chk';

    -- (a) the raw strings really do differ -- if PostgreSQL ever becomes
    -- round-trip stable here this assertion is what tells us the guard is
    -- no longer load-bearing, rather than silently passing forever.
    IF v_def_original IS NOT DISTINCT FROM v_def_reparsed THEN
        RAISE EXCEPTION 'expected pg_get_constraintdef to differ after re-parse, got % twice',
            v_def_original;
    END IF;

    -- (b) canonicalization collapses the difference
    IF public.flashback_canonical_constraint_def(v_def_original)
       IS DISTINCT FROM public.flashback_canonical_constraint_def(v_def_reparsed) THEN
        RAISE EXCEPTION 'canonical constraint defs still differ: % vs %',
            public.flashback_canonical_constraint_def(v_def_original),
            public.flashback_canonical_constraint_def(v_def_reparsed);
    END IF;

    -- (c) real differences must survive canonicalization
    IF public.flashback_canonical_constraint_def('CHECK ((kind)::text = ''a''::text)')
       IS NOT DISTINCT FROM public.flashback_canonical_constraint_def('CHECK ((kind)::text = ''b''::text)') THEN
        RAISE EXCEPTION 'canonicalization erased a changed literal';
    END IF;
    IF public.flashback_canonical_constraint_def('CHECK ((kind)::text = ''a''::text)')
       IS NOT DISTINCT FROM public.flashback_canonical_constraint_def('CHECK ((other)::text = ''a''::text)') THEN
        RAISE EXCEPTION 'canonicalization erased a changed column';
    END IF;
    IF public.flashback_canonical_constraint_def('CHECK (amount >= 0)')
       IS NOT DISTINCT FROM public.flashback_canonical_constraint_def('CHECK (amount > 0)') THEN
        RAISE EXCEPTION 'canonicalization erased a changed operator';
    END IF;
    IF public.flashback_canonical_constraint_def('CHECK ((a OR b) AND c)')
       IS NOT DISTINCT FROM public.flashback_canonical_constraint_def('CHECK (a OR (b AND c))') THEN
        RAISE EXCEPTION 'canonicalization erased operator precedence grouping';
    END IF;

    -- The inventory the restore actually digests must agree for the same
    -- constraint on both sides of a re-render. Compare the named CHECK entry
    -- only: PostgreSQL 18 also carries NOT NULL in pg_constraint under an
    -- auto-generated, table-name-derived name, which necessarily differs
    -- between these two probe tables and is not what this test is about.
    IF (SELECT c FROM jsonb_array_elements(
            public.flashback_canonical_inventory_from_relation('public.it_cdef_a'::regclass)->'constraints') c
        WHERE c->>'name' = 'it_cdef_chk')
       IS DISTINCT FROM
       (SELECT c FROM jsonb_array_elements(
            public.flashback_canonical_inventory_from_relation('public.it_cdef_b'::regclass)->'constraints') c
        WHERE c->>'name' = 'it_cdef_chk') THEN
        RAISE EXCEPTION 'canonical inventory CHECK entry differs across a re-render: % vs %',
            (SELECT c FROM jsonb_array_elements(
                public.flashback_canonical_inventory_from_relation('public.it_cdef_a'::regclass)->'constraints') c
             WHERE c->>'name' = 'it_cdef_chk'),
            (SELECT c FROM jsonb_array_elements(
                public.flashback_canonical_inventory_from_relation('public.it_cdef_b'::regclass)->'constraints') c
             WHERE c->>'name' = 'it_cdef_chk');
    END IF;

    DROP TABLE public.it_cdef_a CASCADE;
    DROP TABLE public.it_cdef_b CASCADE;
END $tv$;

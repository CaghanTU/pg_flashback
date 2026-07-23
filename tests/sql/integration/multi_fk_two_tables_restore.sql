-- Incoming FKs (peers referencing the table) are staging-rejected.
-- Outgoing FK from child -> parent remains in the preserve set.
DO $tv$
DECLARE
    v_parent jsonb;
    v_child jsonb;
    v_err text;
BEGIN
    DROP TABLE IF EXISTS public.it_fk_child CASCADE;
    DROP TABLE IF EXISTS public.it_fk_parent CASCADE;
    CREATE TABLE public.it_fk_parent (id int PRIMARY KEY, status text);
    CREATE TABLE public.it_fk_child (
        id int PRIMARY KEY,
        parent_id int REFERENCES public.it_fk_parent(id),
        qty int
    );

    v_parent := flashback_local_compatibility('public.it_fk_parent'::regclass);
    IF COALESCE((v_parent->>'supported')::boolean, true) THEN
        RAISE EXCEPTION 'parent with incoming FK must be unsupported: %', v_parent;
    END IF;
    IF NOT (v_parent->'rejected_features' ? 'incoming_foreign_keys') THEN
        RAISE EXCEPTION 'expected incoming_foreign_keys reject: %', v_parent;
    END IF;

    v_child := flashback_local_compatibility('public.it_fk_child'::regclass);
    IF NOT COALESCE((v_child->>'supported')::boolean, false) THEN
        RAISE EXCEPTION 'child with outgoing FK should be supported: %', v_child;
    END IF;

    BEGIN
        PERFORM flashback_track('public.it_fk_parent');
        RAISE EXCEPTION 'track unexpectedly accepted parent with incoming FK';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
        IF v_err NOT ILIKE '%incoming%' AND v_err NOT ILIKE '%not compatible%' THEN
            RAISE EXCEPTION 'unexpected parent track error: %', v_err;
        END IF;
    END;

    PERFORM flashback_track('public.it_fk_child');
    PERFORM flashback_unprotect('public.it_fk_child');
END;
$tv$;

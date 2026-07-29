-- COPY BINARY does not carry column collation.  The schema contract must
-- capture, recreate and independently verify the catalog collation identity.
DO $$
DECLARE
    v_schema_def jsonb;
    v_source_coll oid;
    v_shadow_coll oid;
    v_captured text;
    v_expected_cols jsonb;
    v_actual_cols jsonb;
    v_pk_name text;
    v_pk_def text;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_collation CASCADE;
    CREATE TABLE public.it_schema_collation (
        id bigint,
        default_text text,
        c_text text COLLATE pg_catalog."C" NOT NULL,
        CONSTRAINT it_schema_collation_named_pk
            PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED
    );

    SELECT attcollation
      INTO v_source_coll
    FROM pg_attribute
    WHERE attrelid = 'public.it_schema_collation'::regclass
      AND attname = 'c_text';

    v_schema_def := flashback_collect_schema_def(
        'public.it_schema_collation'::regclass
    );
    SELECT col->>'collation'
      INTO v_captured
    FROM jsonb_array_elements(v_schema_def->'columns') AS col
    WHERE col->>'name' = 'c_text';

    IF v_captured IS DISTINCT FROM 'pg_catalog."C"' THEN
        RAISE EXCEPTION 'qualified non-default collation was not captured: %',
            v_captured;
    END IF;

    PERFORM flashback_recreate_table_from_ddl(
        v_schema_def, 'pg_temp', 'it_schema_collation_shadow'
    );
    PERFORM flashback_apply_deferred_pk(
        'pg_temp', 'it_schema_collation_shadow', v_schema_def
    );
    SELECT attcollation
      INTO v_shadow_coll
    FROM pg_attribute
    WHERE attrelid = 'pg_temp.it_schema_collation_shadow'::regclass
      AND attname = 'c_text';

    IF v_shadow_coll IS DISTINCT FROM v_source_coll THEN
        RAISE EXCEPTION
            'collation changed during schema roundtrip: source=% shadow=%',
            v_source_coll, v_shadow_coll;
    END IF;

    SELECT con.conname, pg_get_constraintdef(con.oid, true)
      INTO v_pk_name, v_pk_def
    FROM pg_constraint con
    WHERE con.conrelid = 'pg_temp.it_schema_collation_shadow'::regclass
      AND con.contype = 'p';
    IF v_pk_name IS DISTINCT FROM 'it_schema_collation_named_pk'
       OR v_pk_def IS DISTINCT FROM
          'PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED'
    THEN
        RAISE EXCEPTION
            'named/deferrable PK drifted during roundtrip: name=% def=%',
            v_pk_name, v_pk_def;
    END IF;

    SELECT flashback_canonical_inventory_from_schema_def(v_schema_def)->'columns'
      INTO v_expected_cols;
    SELECT flashback_canonical_inventory_from_relation(
        'pg_temp.it_schema_collation_shadow'::regclass
    )->'columns'
      INTO v_actual_cols;
    IF v_expected_cols IS DISTINCT FROM v_actual_cols THEN
        RAISE EXCEPTION
            'expected/live canonical column inventories disagree: expected=% actual=%',
            v_expected_cols, v_actual_cols;
    END IF;
END;
$$;

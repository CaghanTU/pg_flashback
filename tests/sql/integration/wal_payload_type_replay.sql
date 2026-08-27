-- The WAL decoder serializes json/jsonb and array Datums through PostgreSQL's
-- text output inside a JSON string. Trigger payloads carry nested JSON values.
-- Replay helpers must accept both representations without double encoding.
DO $tv$
DECLARE
    v_cols text;
    v_vals text;
    v_set text;
    v_pred text;
    v_col_meta jsonb;
    v_pk_cols text[] := ARRAY['id'];
BEGIN
    CREATE TEMP TABLE it_payload_types (
        id bigint PRIMARY KEY,
        meta jsonb NOT NULL,
        nums integer[] NOT NULL
    );

    -- Same collection query flashback_materialize_lsn uses to build col_meta
    -- once per restore instead of re-probing the catalog per delta_log event.
    SELECT jsonb_object_agg(
        a.attname,
        jsonb_build_object(
            'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
            'is_array', (a.attndims > 0 OR t.typlen = -1 AND t.typelem <> 0),
            'is_json', t.typname IN ('jsonb', 'json'),
            'attnum', a.attnum
        )
    ) INTO v_col_meta
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = 'pg_temp.it_payload_types'::regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = '';

    SELECT col_list, val_list INTO v_cols, v_vals
    FROM flashback_build_insert_parts(
        v_col_meta,
        jsonb_build_object(
            'id', '1',
            'meta', '{"kind":"wal","nested":{"ok":true}}'::text,
            'nums', '{1,2,3}'::text
        )
    );
    EXECUTE format('INSERT INTO pg_temp.it_payload_types (%s) VALUES (%s)', v_cols, v_vals);
    IF (SELECT jsonb_typeof(meta) FROM pg_temp.it_payload_types WHERE id=1) <> 'object'
       OR (SELECT meta->>'kind' FROM pg_temp.it_payload_types WHERE id=1) <> 'wal'
       OR (SELECT nums FROM pg_temp.it_payload_types WHERE id=1) <> ARRAY[1,2,3]
    THEN
        RAISE EXCEPTION 'string-encoded WAL payload was replayed incorrectly';
    END IF;

    SELECT set_clause, pk_predicate INTO v_set, v_pred
    FROM flashback_build_update_set(
        v_col_meta,
        jsonb_build_object(
            'id', 1,
            'meta', jsonb_build_object('kind','trigger','nested',jsonb_build_object('ok',true)),
            'nums', to_jsonb(ARRAY[4,5])
        ),
        v_pk_cols
    );
    EXECUTE format('UPDATE pg_temp.it_payload_types SET %s WHERE %s', v_set, v_pred);
    IF (SELECT jsonb_typeof(meta) FROM pg_temp.it_payload_types WHERE id=1) <> 'object'
       OR (SELECT meta->>'kind' FROM pg_temp.it_payload_types WHERE id=1) <> 'trigger'
       OR (SELECT nums FROM pg_temp.it_payload_types WHERE id=1) <> ARRAY[4,5]
    THEN
        RAISE EXCEPTION 'nested trigger payload was replayed incorrectly';
    END IF;

    v_pred := flashback_build_predicate(
        v_col_meta,
        jsonb_build_object(
            'id', '1',
            'meta', '{"kind":"trigger","nested":{"ok":true}}'::text,
            'nums', '{4,5}'::text
        )
    );
    EXECUTE format('SELECT 1 FROM pg_temp.it_payload_types WHERE %s', v_pred);

    -- Scale/correctness contract: PK-backed DELETE replay must emit only the
    -- immutable key and ordinary equality so PostgreSQL can use the already
    -- built primary-key index.  Non-key replica-identity columns must not leak
    -- into this predicate.
    v_pred := flashback_build_pk_predicate(
        v_col_meta,
        jsonb_build_object(
            'id', '1',
            'meta', '{"kind":"must-not-appear"}'::text,
            'nums', '{9,9}'::text
        ),
        v_pk_cols
    );
    IF v_pred IS NULL
       OR v_pred NOT LIKE 'id = %'
       OR v_pred LIKE '%meta%'
       OR v_pred LIKE '%nums%'
       OR v_pred LIKE '%IS NOT DISTINCT FROM%'
    THEN
        RAISE EXCEPTION 'PK predicate is not index-usable or contains non-key columns: %', v_pred;
    END IF;
    EXECUTE format('DELETE FROM pg_temp.it_payload_types WHERE %s', v_pred);
    IF EXISTS (SELECT 1 FROM pg_temp.it_payload_types WHERE id = 1) THEN
        RAISE EXCEPTION 'PK predicate did not identify the expected row';
    END IF;

    -- Incomplete composite-key evidence must never produce a partial DELETE
    -- predicate.  The caller will use the correctness-first full-row fallback.
    v_pred := flashback_build_pk_predicate(
        v_col_meta,
        jsonb_build_object('id', '2'),
        ARRAY['id', 'nums']
    );
    IF v_pred IS NOT NULL THEN
        RAISE EXCEPTION 'incomplete PK evidence produced a partial predicate: %', v_pred;
    END IF;
END;
$tv$;

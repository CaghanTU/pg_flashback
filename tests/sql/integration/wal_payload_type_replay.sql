-- The WAL decoder serializes json/jsonb and array Datums through PostgreSQL's
-- text output inside a JSON string. Trigger payloads carry nested JSON values.
-- Replay helpers must accept both representations without double encoding.
DO $tv$
DECLARE
    v_cols text;
    v_vals text;
    v_set text;
    v_pred text;
BEGIN
    CREATE TEMP TABLE it_payload_types (
        id bigint PRIMARY KEY,
        meta jsonb NOT NULL,
        nums integer[] NOT NULL
    );

    SELECT col_list, val_list INTO v_cols, v_vals
    FROM flashback_build_insert_parts(
        'pg_temp.it_payload_types'::regclass,
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
        'pg_temp.it_payload_types'::regclass,
        jsonb_build_object(
            'id', 1,
            'meta', jsonb_build_object('kind','trigger','nested',jsonb_build_object('ok',true)),
            'nums', to_jsonb(ARRAY[4,5])
        )
    );
    EXECUTE format('UPDATE pg_temp.it_payload_types SET %s WHERE %s', v_set, v_pred);
    IF (SELECT jsonb_typeof(meta) FROM pg_temp.it_payload_types WHERE id=1) <> 'object'
       OR (SELECT meta->>'kind' FROM pg_temp.it_payload_types WHERE id=1) <> 'trigger'
       OR (SELECT nums FROM pg_temp.it_payload_types WHERE id=1) <> ARRAY[4,5]
    THEN
        RAISE EXCEPTION 'nested trigger payload was replayed incorrectly';
    END IF;

    v_pred := flashback_build_predicate(
        'pg_temp.it_payload_types'::regclass,
        jsonb_build_object(
            'id', '1',
            'meta', '{"kind":"trigger","nested":{"ok":true}}'::text,
            'nums', '{4,5}'::text
        )
    );
    EXECUTE format('SELECT 1 FROM pg_temp.it_payload_types WHERE %s', v_pred);
END;
$tv$;

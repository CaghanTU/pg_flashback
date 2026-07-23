-- =================================================================
-- Post-restore relation inventory and fail-closed verification.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_relation_data_fingerprint(
    p_relation regclass,
    p_max_rows bigint DEFAULT 100000
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_oid oid := p_relation::oid;
    v_schema text;
    v_name text;
    v_row_count bigint;
    v_pk_cols text[];
    v_order_clause text;
    v_digest text;
    v_rows_hashed bigint;
BEGIN
    IF p_max_rows < 1 THEN
        RAISE EXCEPTION 'flashback_relation_data_fingerprint: max rows must be positive';
    END IF;

    SELECT n.nspname, c.relname
      INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_relation_data_fingerprint: relation % does not exist', p_relation;
    END IF;

    EXECUTE format('SELECT count(*)::bigint FROM %I.%I', v_schema, v_name)
       INTO v_row_count;

    SELECT array_agg(att.attname ORDER BY k.ord)
      INTO v_pk_cols
    FROM pg_index i
    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
    JOIN pg_attribute att
      ON att.attrelid = i.indrelid
     AND att.attnum = k.attnum
    WHERE i.indrelid = v_oid
      AND i.indisprimary;

    IF v_pk_cols IS NOT NULL AND array_length(v_pk_cols, 1) > 0 THEN
        v_order_clause := (
            SELECT string_agg(format('%I', col), ', ')
            FROM unnest(v_pk_cols) AS col
        );
    ELSE
        v_order_clause := 'ctid';
    END IF;

    EXECUTE format(
        $q$
        WITH bounded AS (
            SELECT row_to_json(t)::text AS row_text,
                   row_number() OVER () AS ord
            FROM (
                SELECT *
                FROM %I.%I
                ORDER BY %s
                LIMIT %s
            ) t
        )
        SELECT md5(format('%%s|%%s', %L::text, COALESCE(string_agg(row_text, E'\n' ORDER BY ord), ''))),
               count(*)::bigint
        FROM bounded
        $q$,
        v_schema, v_name, v_order_clause, p_max_rows, v_row_count
    ) INTO v_digest, v_rows_hashed;

    RETURN jsonb_build_object(
        'algorithm', 'md5_ordered_row_json',
        'row_count', v_row_count,
        'rows_hashed', v_rows_hashed,
        'max_rows', p_max_rows,
        'order_by', COALESCE(to_jsonb(v_pk_cols), to_jsonb('ctid'::text)),
        'digest', v_digest
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_relation_inventory(
    p_relation regclass
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_oid oid := p_relation::oid;
    v_schema text;
    v_name text;
    v_row_count bigint;
    v_inv jsonb;
BEGIN
    SELECT n.nspname, c.relname
      INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_relation_inventory: relation % does not exist', p_relation;
    END IF;

    EXECUTE format('SELECT count(*)::bigint FROM %I.%I', v_schema, v_name)
       INTO v_row_count;

    SELECT jsonb_build_object(
        'relation', format('%I.%I', v_schema, v_name),
        'schema_fingerprint', public.flashback_payload_schema_fingerprint(p_relation),
        'row_count', v_row_count,
        'data_fingerprint', public.flashback_relation_data_fingerprint(p_relation),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', con.conname,
                    'type', con.contype::text,
                    'def', pg_get_constraintdef(con.oid, true)
                )
                ORDER BY con.conname, con.contype::text
            )
            FROM pg_constraint con
            WHERE con.conrelid = v_oid
              AND con.contype IN ('p', 'u', 'c', 'f')
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', idx.relname,
                    'def', pg_get_indexdef(i.indexrelid)
                )
                ORDER BY idx.relname
            )
            FROM pg_index i
            JOIN pg_class idx ON idx.oid = i.indexrelid
            WHERE i.indrelid = v_oid
        ), '[]'::jsonb),
        'owner', pg_get_userbyid(c.relowner),
        'table_acl', COALESCE(c.relacl::text[], ARRAY[]::text[]),
        'column_acls', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'acl', COALESCE(a.attacl::text[], ARRAY[]::text[])
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            WHERE a.attrelid = v_oid
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attacl IS NOT NULL
        ), '[]'::jsonb),
        'rls_enabled', c.relrowsecurity,
        'force_rls', c.relforcerowsecurity,
        'rls_policies', COALESCE((
            SELECT jsonb_agg(pol.polname ORDER BY pol.polname)
            FROM pg_policy pol
            WHERE pol.polrelid = v_oid
        ), '[]'::jsonb),
        'identity_columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'identity', a.attidentity::text,
                    'sequence', pg_get_serial_sequence(
                        format('%I.%I', v_schema, v_name),
                        a.attname
                    )
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            WHERE a.attrelid = v_oid
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attidentity <> ''
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', t.tgname,
                    'enabled', t.tgenabled::text
                )
                ORDER BY t.tgname
            )
            FROM pg_trigger t
            WHERE t.tgrelid = v_oid
              AND NOT t.tgisinternal
        ), '[]'::jsonb),
        'replica_identity', c.relreplident::text,
        'tablespace', ts.spcname,
        'reloptions', COALESCE(c.reloptions, ARRAY[]::text[]),
        'has_comments', EXISTS (
            SELECT 1
            FROM pg_description d
            WHERE d.objoid = v_oid
              AND d.objsubid = 0
        )
    )
      INTO v_inv
    FROM pg_class c
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
    WHERE c.oid = v_oid;

    RETURN v_inv;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_schema_def_column_signature(
    p_schema_def jsonb
)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $$
    SELECT md5(COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'name', elem->>'name',
                'type', elem->>'type',
                'not_null', COALESCE((elem->>'not_null')::boolean, false),
                'identity', COALESCE(elem->>'identity', ''),
                'generated', COALESCE(elem->>'generated', '')
            )
            ORDER BY elem->>'name'
        )::text
        FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
    ), '[]'));
$$;

CREATE OR REPLACE FUNCTION flashback_capture_restore_expected_proof(
    p_rel regclass,
    p_extra jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_inventory jsonb;
BEGIN
    v_inventory := public.flashback_relation_inventory(p_rel);
    RETURN jsonb_build_object(
        'captured_at', clock_timestamp(),
        'relation', v_inventory->>'relation',
        'inventory', v_inventory,
        'extra', COALESCE(p_extra, '{}'::jsonb)
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_verify_restored_relation(
    p_rel regclass,
    p_expected jsonb,
    p_target_schema_def jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_live jsonb;
    v_exp_inv jsonb;
    v_live_fp text;
    v_exp_fp text;
    v_def_sig text;
    v_live_sig text;
BEGIN
    IF p_expected IS NULL OR p_expected = '{}'::jsonb THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: expected proof is empty';
    END IF;

    v_exp_inv := p_expected->'inventory';
    IF v_exp_inv IS NULL OR v_exp_inv = 'null'::jsonb THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: expected proof missing inventory';
    END IF;

    v_live := public.flashback_relation_inventory(p_rel);

    IF v_live->>'schema_fingerprint' IS DISTINCT FROM v_exp_inv->>'schema_fingerprint' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: schema fingerprint mismatch for %',
            p_rel;
    END IF;

    IF (v_live->>'row_count')::bigint IS DISTINCT FROM (v_exp_inv->>'row_count')::bigint THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: row_count mismatch for % (live % expected %)',
            p_rel, v_live->>'row_count', v_exp_inv->>'row_count';
    END IF;

    IF v_live->'data_fingerprint' IS DISTINCT FROM v_exp_inv->'data_fingerprint' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: data fingerprint mismatch for %',
            p_rel;
    END IF;

    IF v_live->'constraints' IS DISTINCT FROM v_exp_inv->'constraints' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: constraint inventory mismatch for %',
            p_rel;
    END IF;

    IF v_live->'indexes' IS DISTINCT FROM v_exp_inv->'indexes' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: index inventory mismatch for %',
            p_rel;
    END IF;

    IF v_live->>'owner' IS DISTINCT FROM v_exp_inv->>'owner' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: owner mismatch for %',
            p_rel;
    END IF;

    IF to_jsonb(v_live->'table_acl') IS DISTINCT FROM to_jsonb(v_exp_inv->'table_acl') THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: table ACL mismatch for %',
            p_rel;
    END IF;

    IF v_live->'column_acls' IS DISTINCT FROM v_exp_inv->'column_acls' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: column ACL mismatch for %',
            p_rel;
    END IF;

    IF (v_live->>'rls_enabled')::boolean IS DISTINCT FROM (v_exp_inv->>'rls_enabled')::boolean
       OR (v_live->>'force_rls')::boolean IS DISTINCT FROM (v_exp_inv->>'force_rls')::boolean THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: RLS flags mismatch for %',
            p_rel;
    END IF;

    IF v_live->'rls_policies' IS DISTINCT FROM v_exp_inv->'rls_policies' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: RLS policy name mismatch for %',
            p_rel;
    END IF;

    IF v_live->'identity_columns' IS DISTINCT FROM v_exp_inv->'identity_columns' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: identity column mismatch for %',
            p_rel;
    END IF;

    IF v_live->'triggers' IS DISTINCT FROM v_exp_inv->'triggers' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: trigger inventory mismatch for %',
            p_rel;
    END IF;

    IF v_live->>'replica_identity' IS DISTINCT FROM v_exp_inv->>'replica_identity' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: replica identity mismatch for %',
            p_rel;
    END IF;

    IF v_live->>'tablespace' IS DISTINCT FROM v_exp_inv->>'tablespace' THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: tablespace mismatch for %',
            p_rel;
    END IF;

    IF to_jsonb(v_live->'reloptions') IS DISTINCT FROM to_jsonb(v_exp_inv->'reloptions') THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: reloptions mismatch for %',
            p_rel;
    END IF;

    IF (v_live->>'has_comments')::boolean IS DISTINCT FROM (v_exp_inv->>'has_comments')::boolean THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: table comment presence mismatch for %',
            p_rel;
    END IF;

    v_live_fp := v_live->>'schema_fingerprint';
    v_exp_fp := public.flashback_payload_schema_fingerprint(p_rel);
    IF v_live_fp IS DISTINCT FROM v_exp_fp THEN
        RAISE EXCEPTION 'flashback_verify_restored_relation: live schema fingerprint drift for %',
            p_rel;
    END IF;

    IF p_target_schema_def IS NOT NULL
       AND jsonb_array_length(COALESCE(p_target_schema_def->'columns', '[]'::jsonb)) > 0 THEN
        v_def_sig := public.flashback_schema_def_column_signature(p_target_schema_def);
        v_live_sig := md5(COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'type', format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'identity', a.attidentity::text,
                    'generated', a.attgenerated::text
                )
                ORDER BY a.attname
            )::text
            FROM pg_attribute a
            WHERE a.attrelid = p_rel::oid
              AND a.attnum > 0
              AND NOT a.attisdropped
        ), '[]'));
        IF v_def_sig IS DISTINCT FROM v_live_sig THEN
            RAISE EXCEPTION 'flashback_verify_restored_relation: target_schema_def column signature mismatch for %',
                p_rel;
        END IF;

        v_exp_fp := public.flashback_payload_schema_fingerprint(p_rel);
        IF v_exp_fp IS NULL THEN
            RAISE EXCEPTION 'flashback_verify_restored_relation: cannot compute schema fingerprint for %',
                p_rel;
        END IF;
    END IF;

    RETURN jsonb_build_object(
        'status', 'passed',
        'verified_at', clock_timestamp(),
        'relation', v_live->>'relation',
        'schema_fingerprint', v_live_fp,
        'row_count', v_live->'row_count',
        'expected_proof', p_expected,
        'target_schema_def_checked', (p_target_schema_def IS NOT NULL)
    );
END;
$$;

COMMENT ON FUNCTION flashback_relation_inventory(regclass)
    IS 'Catalog+heap inventory for post-restore verification (internal).';
COMMENT ON FUNCTION flashback_capture_restore_expected_proof(regclass, jsonb)
    IS 'Capture expected restore proof from the live relation (internal).';
COMMENT ON FUNCTION flashback_verify_restored_relation(regclass, jsonb, jsonb)
    IS 'Fail-closed restore verification; raises on any inventory mismatch (internal).';

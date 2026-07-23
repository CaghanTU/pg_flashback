-- =================================================================
-- Independent restore proof: expected from shadow+schema_def,
-- actual from live, compared fail-closed (never tautological).
-- =================================================================

-- Immutable order_spec is provided by Rust:
--   flashback_fingerprint_order_spec(jsonb) -> jsonb
--   flashback_relation_full_data_fingerprint(regclass, jsonb) -> text

CREATE OR REPLACE FUNCTION flashback_canonical_inventory_from_schema_def(
    p_schema_def jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog
AS $$
DECLARE
    v_constraints jsonb := '[]'::jsonb;
    v_indexes jsonb := '[]'::jsonb;
    v_triggers jsonb := '[]'::jsonb;
    v_policies jsonb := '[]'::jsonb;
    v_acl jsonb := '[]'::jsonb;
    v_comments jsonb := '[]'::jsonb;
    v_sequences jsonb := '[]'::jsonb;
    v_elem jsonb;
BEGIN
    -- Constraints: full semantic definition from schema_def.
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', elem->>'name',
            'type', COALESCE(elem->>'type', elem->>'contype'),
            'def', COALESCE(elem->>'def', elem->>'definition', '')
        )
        ORDER BY COALESCE(elem->>'name', ''), COALESCE(elem->>'type', '')
    ), '[]'::jsonb)
      INTO v_constraints
    FROM jsonb_array_elements(COALESCE(p_schema_def->'constraints', '[]'::jsonb)) elem;

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', elem->>'name',
            'def', COALESCE(elem->>'def', elem->>'definition', '')
        )
        ORDER BY COALESCE(elem->>'name', '')
    ), '[]'::jsonb)
      INTO v_indexes
    FROM jsonb_array_elements(COALESCE(p_schema_def->'indexes', '[]'::jsonb)) elem;

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', elem->>'name',
            'def', COALESCE(elem->>'def', elem->>'definition', '')
        )
        ORDER BY COALESCE(elem->>'name', '')
    ), '[]'::jsonb)
      INTO v_triggers
    FROM jsonb_array_elements(COALESCE(p_schema_def->'triggers', '[]'::jsonb)) elem
    WHERE COALESCE(elem->>'name', '') NOT LIKE 'flashback_capture_%';

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', elem->>'name',
            'cmd', elem->>'cmd',
            'permissive', COALESCE((elem->>'permissive')::boolean, true),
            'roles', COALESCE(elem->'roles', '[]'::jsonb),
            'qual', elem->>'qual',
            'with_check', elem->>'with_check'
        )
        ORDER BY COALESCE(elem->>'name', '')
    ), '[]'::jsonb)
      INTO v_policies
    FROM jsonb_array_elements(COALESCE(p_schema_def->'rls_policies', '[]'::jsonb)) elem;

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'grantee', elem->>'grantee',
            'privilege', elem->>'privilege',
            'is_grantable', COALESCE((elem->>'is_grantable')::boolean, false)
        )
        ORDER BY COALESCE(elem->>'grantee', ''), COALESCE(elem->>'privilege', '')
    ), '[]'::jsonb)
      INTO v_acl
    FROM jsonb_array_elements(COALESCE(p_schema_def->'acl', '[]'::jsonb)) elem;

    -- Comments: optional array of {target, text}; empty if absent.
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'target', COALESCE(elem->>'target', 'table'),
            'column', elem->>'column',
            'text', elem->>'text'
        )
        ORDER BY COALESCE(elem->>'target', ''), COALESCE(elem->>'column', ''), COALESCE(elem->>'text', '')
    ), '[]'::jsonb)
      INTO v_comments
    FROM jsonb_array_elements(COALESCE(p_schema_def->'comments', '[]'::jsonb)) elem;

    -- Sequence owned-by + expected post-restore state contract (edge setval):
    -- empty table => setval(start, false); else setval(edge, true).
    -- State values themselves are filled by callers that know live/shadow data.
    FOR v_elem IN
        SELECT elem
        FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
        WHERE elem->>'identity' IN ('a', 'd')
           OR COALESCE(elem->>'default', '') ~* 'nextval'
    LOOP
        v_sequences := v_sequences || jsonb_build_array(jsonb_build_object(
            'column', v_elem->>'name',
            'identity', COALESCE(v_elem->>'identity', ''),
            'sequence_schema', v_elem->'identity_options'->>'sequence_schema',
            'sequence_name', v_elem->'identity_options'->>'sequence_name',
            'increment', COALESCE((v_elem->'identity_options'->>'increment')::bigint, 1),
            'start', COALESCE((v_elem->'identity_options'->>'start')::bigint, 1),
            'state_policy', 'edge_after_restore'
        ));
    END LOOP;

    RETURN jsonb_build_object(
        'primary_key', COALESCE(p_schema_def->'primary_key', '[]'::jsonb),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', elem->>'name',
                    'type', elem->>'type',
                    'not_null', COALESCE((elem->>'not_null')::boolean, false),
                    'identity', COALESCE(elem->>'identity', ''),
                    'generated', COALESCE(elem->>'generated', ''),
                    'default', COALESCE(elem->>'default', '')
                )
                ORDER BY COALESCE((elem->>'attnum')::int, 0), elem->>'name'
            )
            FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
        ), '[]'::jsonb),
        'constraints', v_constraints,
        'indexes', v_indexes,
        'triggers', v_triggers,
        'rls_enabled', COALESCE((p_schema_def->>'rls_enabled')::boolean, false),
        'force_rls', COALESCE((p_schema_def->>'force_rls')::boolean, false),
        'rls_policies', v_policies,
        'owner', p_schema_def->>'owner',
        'acl', v_acl,
        'comments', v_comments,
        'sequences', v_sequences,
        'replica_identity', COALESCE(p_schema_def->>'replica_identity', 'd'),
        'tablespace', p_schema_def->>'tablespace',
        'reloptions', COALESCE(p_schema_def->'reloptions', '[]'::jsonb)
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_canonical_inventory_from_relation(
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
    v_inv jsonb;
BEGIN
    SELECT n.nspname, c.relname
      INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_canonical_inventory_from_relation: relation % missing', p_relation;
    END IF;

    SELECT jsonb_build_object(
        'primary_key', COALESCE((
            SELECT jsonb_agg(att.attname ORDER BY k.ord)
            FROM pg_index i
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute att
              ON att.attrelid = i.indrelid AND att.attnum = k.attnum
            WHERE i.indrelid = v_oid AND i.indisprimary
        ), '[]'::jsonb),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'type', format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'identity', a.attidentity::text,
                    'generated', a.attgenerated::text,
                    'default', pg_get_expr(ad.adbin, ad.adrelid)
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            LEFT JOIN pg_attrdef ad
              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE a.attrelid = v_oid
              AND a.attnum > 0
              AND NOT a.attisdropped
        ), '[]'::jsonb),
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
              AND NOT i.indisprimary
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', t.tgname,
                    'def', pg_get_triggerdef(t.oid)
                )
                ORDER BY t.tgname
            )
            FROM pg_trigger t
            WHERE t.tgrelid = v_oid
              AND NOT t.tgisinternal
              AND t.tgname NOT LIKE 'flashback_capture_%'
        ), '[]'::jsonb),
        'rls_enabled', c.relrowsecurity,
        'force_rls', c.relforcerowsecurity,
        'rls_policies', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', pol.polname,
                    'cmd', CASE pol.polcmd
                        WHEN 'r' THEN 'SELECT'
                        WHEN 'a' THEN 'INSERT'
                        WHEN 'w' THEN 'UPDATE'
                        WHEN 'd' THEN 'DELETE'
                        ELSE 'ALL'
                    END,
                    'permissive', pol.polpermissive,
                    'roles', COALESCE((
                        SELECT jsonb_agg(r.rolname ORDER BY r.rolname)
                        FROM pg_roles r
                        WHERE r.oid = ANY (pol.polroles)
                    ), '[]'::jsonb),
                    'qual', pg_get_expr(pol.polqual, pol.polrelid),
                    'with_check', pg_get_expr(pol.polwithcheck, pol.polrelid)
                )
                ORDER BY pol.polname
            )
            FROM pg_policy pol
            WHERE pol.polrelid = v_oid
        ), '[]'::jsonb),
        'owner', pg_get_userbyid(c.relowner),
        'acl', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'grantee', CASE
                        WHEN ae.grantee = 0 THEN 'PUBLIC'
                        ELSE (SELECT rolname FROM pg_roles WHERE oid = ae.grantee)
                    END,
                    'privilege', ae.privilege_type,
                    'is_grantable', ae.is_grantable
                )
                ORDER BY ae.grantee, ae.privilege_type
            )
            FROM aclexplode(c.relacl) AS ae(grantor, grantee, privilege_type, is_grantable)
        ), '[]'::jsonb),
        'comments', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'target', CASE WHEN d.objsubid = 0 THEN 'table' ELSE 'column' END,
                    'column', CASE
                        WHEN d.objsubid = 0 THEN NULL
                        ELSE (SELECT a.attname FROM pg_attribute a
                              WHERE a.attrelid = v_oid AND a.attnum = d.objsubid)
                    END,
                    'text', d.description
                )
                ORDER BY d.objsubid, d.description
            )
            FROM pg_description d
            WHERE d.objoid = v_oid
        ), '[]'::jsonb),
        'sequences', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'column', a.attname,
                    'identity', a.attidentity::text,
                    'sequence', pg_get_serial_sequence(format('%I.%I', v_schema, v_name), a.attname),
                    'last_value', (SELECT last_value FROM pg_sequences
                                   WHERE schemaname || '.' || sequencename
                                         = replace(pg_get_serial_sequence(
                                               format('%I.%I', v_schema, v_name), a.attname
                                           ), '"', '')),
                    -- pg_sequences has no is_called column; last_value is
                    -- NULL exactly when the sequence has never been called
                    -- (see PostgreSQL docs for pg_sequences.last_value), so
                    -- that is an equivalent, catalog-only way to derive it.
                    'is_called', (SELECT last_value IS NOT NULL FROM pg_sequences
                                  WHERE schemaname || '.' || sequencename
                                        = replace(pg_get_serial_sequence(
                                              format('%I.%I', v_schema, v_name), a.attname
                                          ), '"', '')),
                    'state_policy', 'edge_after_restore'
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            WHERE a.attrelid = v_oid
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND (a.attidentity <> '' OR pg_get_serial_sequence(
                      format('%I.%I', v_schema, v_name), a.attname) IS NOT NULL)
        ), '[]'::jsonb),
        'replica_identity', c.relreplident::text,
        'tablespace', ts.spcname,
        'reloptions', to_jsonb(COALESCE(c.reloptions, ARRAY[]::text[]))
    )
      INTO v_inv
    FROM pg_class c
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
    WHERE c.oid = v_oid;

    RETURN v_inv;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_inventory_digest(p_inventory jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, public
AS $$
    SELECT public.flashback_sha256(COALESCE(p_inventory::text, '{}'));
$$;

CREATE OR REPLACE FUNCTION flashback_derive_expected_row_count(
    p_tracking_id bigint,
    p_generation_id bigint,
    p_stream_id bigint,
    p_boundary_snapshot_id bigint,
    p_boundary_lsn pg_lsn,
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_count bigint;
    v_prev_lsn pg_lsn;
    v_prev_eid bigint;
    rec record;
BEGIN
    SELECT snap.row_count INTO v_count
    FROM flashback.snapshots snap
    WHERE snap.snapshot_id = p_boundary_snapshot_id
      AND snap.tracking_id = p_tracking_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'flashback_derive_expected_row_count: boundary snapshot % missing for tracking %',
            p_boundary_snapshot_id, p_tracking_id;
    END IF;

    v_prev_lsn := NULL;
    v_prev_eid := NULL;

    FOR rec IN
        SELECT d.event_id, d.event_type, d.commit_lsn
        FROM flashback.delta_log d
        WHERE d.tracking_id = p_tracking_id
          AND d.generation_id = p_generation_id
          AND d.stream_id = p_stream_id
          AND d.commit_lsn > p_boundary_lsn
          AND d.commit_lsn <= p_target_lsn
        ORDER BY d.commit_lsn, d.event_id
    LOOP
        IF v_prev_lsn IS NOT NULL THEN
            IF rec.commit_lsn < v_prev_lsn
               OR (rec.commit_lsn = v_prev_lsn AND rec.event_id < v_prev_eid) THEN
                RAISE EXCEPTION
                    'flashback_derive_expected_row_count: ambiguous/out-of-order events at LSN % (event %)',
                    rec.commit_lsn, rec.event_id;
            END IF;
        END IF;
        v_prev_lsn := rec.commit_lsn;
        v_prev_eid := rec.event_id;

        IF rec.event_type IN ('TRUNCATE', 'DROP') THEN
            v_count := 0;
        ELSIF rec.event_type = 'INSERT' THEN
            v_count := v_count + 1;
        ELSIF rec.event_type = 'DELETE' THEN
            v_count := v_count - 1;
            IF v_count < 0 THEN
                RAISE EXCEPTION
                    'flashback_derive_expected_row_count: row_count went negative at event %',
                    rec.event_id;
            END IF;
        ELSIF rec.event_type IN ('UPDATE', 'ALTER') THEN
            NULL; -- no count change
        ELSE
            RAISE EXCEPTION
                'flashback_derive_expected_row_count: unsupported event_type % at event %',
                rec.event_type, rec.event_id;
        END IF;
    END LOOP;

    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_expected_sequence_states(
    p_relation regclass,
    p_schema_def jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema text;
    v_name text;
    v_out jsonb := '[]'::jsonb;
    v_elem jsonb;
    v_edge bigint;
    v_inc bigint;
    v_start bigint;
    v_col text;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_relation::oid;

    FOR v_elem IN
        SELECT elem
        FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
        WHERE elem->>'identity' IN ('a', 'd')
    LOOP
        v_col := v_elem->>'name';
        v_inc := COALESCE((v_elem->'identity_options'->>'increment')::bigint, 1);
        v_start := COALESCE((v_elem->'identity_options'->>'start')::bigint, 1);
        IF v_inc < 0 THEN
            EXECUTE format('SELECT min(%I) FROM %I.%I', v_col, v_schema, v_name) INTO v_edge;
        ELSE
            EXECUTE format('SELECT max(%I) FROM %I.%I', v_col, v_schema, v_name) INTO v_edge;
        END IF;
        IF v_edge IS NULL THEN
            v_out := v_out || jsonb_build_array(jsonb_build_object(
                'column', v_col,
                'last_value', v_start,
                'is_called', false,
                'state_policy', 'edge_after_restore'
            ));
        ELSE
            v_out := v_out || jsonb_build_array(jsonb_build_object(
                'column', v_col,
                'last_value', v_edge,
                'is_called', true,
                'state_policy', 'edge_after_restore'
            ));
        END IF;
    END LOOP;
    RETURN v_out;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_build_expected_restore_proof(
    p_shadow regclass,
    p_schema_def jsonb,
    p_manifest jsonb,
    p_binding jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema text;
    v_name text;
    v_order_spec jsonb;
    v_data_fp text;
    v_shadow_count bigint;
    v_derived_count bigint;
    v_inventory jsonb;
    v_seq_states jsonb;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_shadow::oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_build_expected_restore_proof: shadow % missing', p_shadow;
    END IF;

    v_order_spec := public.flashback_fingerprint_order_spec(p_schema_def);
    v_data_fp := public.flashback_relation_full_data_fingerprint(p_shadow, v_order_spec);

    EXECUTE format('SELECT count(*)::bigint FROM %I.%I', v_schema, v_name)
       INTO v_shadow_count;

    v_derived_count := public.flashback_derive_expected_row_count(
        (p_binding->>'tracking_id')::bigint,
        (p_binding->>'generation_id')::bigint,
        (p_binding->>'stream_id')::bigint,
        (p_binding->>'boundary_snapshot_id')::bigint,
        (p_binding->>'boundary_lsn')::pg_lsn,
        (p_binding->>'target_lsn')::pg_lsn
    );

    IF v_shadow_count IS DISTINCT FROM v_derived_count THEN
        RAISE EXCEPTION
            'flashback_build_expected_restore_proof: shadow row_count % != derived % (tracking %, gen %, stream %, target %)',
            v_shadow_count, v_derived_count,
            p_binding->>'tracking_id', p_binding->>'generation_id',
            p_binding->>'stream_id', p_binding->>'target_lsn';
    END IF;

    v_inventory := public.flashback_canonical_inventory_from_schema_def(p_schema_def);
    v_seq_states := public.flashback_expected_sequence_states(p_shadow, p_schema_def);
    v_inventory := v_inventory || jsonb_build_object('sequence_states', v_seq_states);

    RETURN jsonb_build_object(
        'proof_version', 2,
        'expected_source', 'shadow',
        'relation_name', format('%I.%I', v_schema, v_name),
        'relation_oid', p_shadow::oid,
        'order_spec', v_order_spec,
        'row_count', v_shadow_count,
        'data_fingerprint', v_data_fp,
        'inventory', v_inventory,
        'inventory_digest', public.flashback_inventory_digest(v_inventory),
        'manifest', COALESCE(p_manifest, '{}'::jsonb),
        'binding', p_binding,
        'captured_at', clock_timestamp()
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_build_actual_restore_proof(
    p_live regclass,
    p_binding jsonb,
    p_order_spec jsonb,
    p_schema_def jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema text;
    v_name text;
    v_row_count bigint;
    v_data_fp text;
    v_inventory jsonb;
    v_seq_states jsonb;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_live::oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_build_actual_restore_proof: live % missing', p_live;
    END IF;

    EXECUTE format('SELECT count(*)::bigint FROM %I.%I', v_schema, v_name)
       INTO v_row_count;

    v_data_fp := public.flashback_relation_full_data_fingerprint(p_live, p_order_spec);
    v_inventory := public.flashback_canonical_inventory_from_relation(p_live);
    v_seq_states := public.flashback_expected_sequence_states(p_live, p_schema_def);
    -- Actual sequence_states compared against the edge contract (not raw pg_sequences
    -- alone), so restore setval positioning is the verified semantic.
    v_inventory := v_inventory || jsonb_build_object('sequence_states', v_seq_states);

    RETURN jsonb_build_object(
        'proof_version', 2,
        'actual_source', 'live',
        'relation_name', format('%I.%I', v_schema, v_name),
        'relation_oid', p_live::oid,
        'order_spec', p_order_spec,
        'row_count', v_row_count,
        'data_fingerprint', v_data_fp,
        'inventory', v_inventory,
        'inventory_digest', public.flashback_inventory_digest(
            -- Compare semantic subset aligned with expected schema_def inventory.
            public.flashback_canonical_inventory_from_schema_def(p_schema_def)
            || jsonb_build_object('sequence_states', v_seq_states)
        ),
        'binding', p_binding,
        'captured_at', clock_timestamp()
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_compare_restore_proofs(
    p_expected jsonb,
    p_actual jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
BEGIN
    IF p_expected IS NULL OR p_actual IS NULL THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: expected/actual proof is null';
    END IF;

    IF p_expected->>'expected_source' IS DISTINCT FROM 'shadow' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: expected_source must be shadow';
    END IF;
    IF p_actual->>'actual_source' IS DISTINCT FROM 'live' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: actual_source must be live';
    END IF;

    -- Phase names must differ (shadow vs final); OID continuity is allowed.
    IF p_expected->>'relation_name' IS NOT DISTINCT FROM p_actual->>'relation_name' THEN
        RAISE EXCEPTION
            'flashback_compare_restore_proofs: expected and actual relation names must differ (got %)',
            p_expected->>'relation_name';
    END IF;

    IF p_expected->'order_spec' IS DISTINCT FROM p_actual->'order_spec' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: order_spec mismatch';
    END IF;

    IF (p_expected->>'row_count')::bigint IS DISTINCT FROM (p_actual->>'row_count')::bigint THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: row_count mismatch (expected % actual %)',
            p_expected->>'row_count', p_actual->>'row_count';
    END IF;

    IF p_expected->>'data_fingerprint' IS DISTINCT FROM p_actual->>'data_fingerprint' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: data fingerprint mismatch';
    END IF;

    IF p_expected->>'inventory_digest' IS DISTINCT FROM p_actual->>'inventory_digest' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: inventory digest mismatch';
    END IF;

    IF p_expected->'inventory'->'sequence_states'
       IS DISTINCT FROM p_actual->'inventory'->'sequence_states' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: sequence state mismatch';
    END IF;

    IF p_expected->'binding'->>'tracking_id'
       IS DISTINCT FROM p_actual->'binding'->>'tracking_id'
       OR p_expected->'binding'->>'generation_id'
          IS DISTINCT FROM p_actual->'binding'->>'generation_id'
       OR p_expected->'binding'->>'target_lsn'
          IS DISTINCT FROM p_actual->'binding'->>'target_lsn' THEN
        RAISE EXCEPTION 'flashback_compare_restore_proofs: binding mismatch';
    END IF;

    RETURN jsonb_build_object(
        'status', 'passed',
        'verified_at', clock_timestamp(),
        'expected_source', 'shadow',
        'actual_source', 'live',
        'expected_relname', p_expected->>'relation_name',
        'actual_relname', p_actual->>'relation_name',
        'expected_oid', p_expected->>'relation_oid',
        'actual_oid', p_actual->>'relation_oid',
        'oid_continuity', (p_expected->>'relation_oid' = p_actual->>'relation_oid'),
        'row_count', p_actual->'row_count',
        'data_fingerprint', p_actual->>'data_fingerprint',
        'inventory_digest', p_actual->>'inventory_digest',
        'expected_proof', p_expected,
        'actual_proof', p_actual
    );
END;
$$;

-- Compatibility wrappers for older call sites / unit tests.
CREATE OR REPLACE FUNCTION flashback_relation_inventory(p_relation regclass)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema text;
    v_name text;
    v_count bigint;
    v_inv jsonb;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_relation::oid;
    EXECUTE format('SELECT count(*)::bigint FROM %I.%I', v_schema, v_name) INTO v_count;
    v_inv := public.flashback_canonical_inventory_from_relation(p_relation);
    RETURN v_inv || jsonb_build_object(
        'relation', format('%I.%I', v_schema, v_name),
        'row_count', v_count
    );
END;
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
BEGIN
    RAISE EXCEPTION
        'flashback_capture_restore_expected_proof: removed; use flashback_build_expected_restore_proof on the pre-swap shadow'
        USING ERRCODE = 'feature_not_supported';
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
    v_order jsonb;
    v_actual jsonb;
    v_binding jsonb;
BEGIN
    IF p_expected IS NULL OR p_expected->>'expected_source' IS DISTINCT FROM 'shadow' THEN
        RAISE EXCEPTION
            'flashback_verify_restored_relation: expected proof must be shadow-sourced (got %)',
            COALESCE(p_expected->>'expected_source', 'null');
    END IF;
    v_order := p_expected->'order_spec';
    v_binding := COALESCE(p_expected->'binding', '{}'::jsonb);
    v_actual := public.flashback_build_actual_restore_proof(
        p_rel, v_binding, v_order, COALESCE(p_target_schema_def, '{}'::jsonb)
    );
    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'verification_raise' THEN
        RAISE EXCEPTION 'pg_flashback: test_restore_failpoint=verification_raise'
            USING ERRCODE = 'query_canceled';
    END IF;
    RETURN public.flashback_compare_restore_proofs(p_expected, v_actual);
END;
$$;

COMMENT ON FUNCTION flashback_build_expected_restore_proof(regclass, jsonb, jsonb, jsonb)
    IS 'Build immutable expected restore proof from pre-swap shadow + schema_def (internal).';
COMMENT ON FUNCTION flashback_build_actual_restore_proof(regclass, jsonb, jsonb, jsonb)
    IS 'Build actual restore proof from post-swap live relation (internal).';
COMMENT ON FUNCTION flashback_compare_restore_proofs(jsonb, jsonb)
    IS 'Fail-closed compare of independent expected/actual restore proofs (internal).';

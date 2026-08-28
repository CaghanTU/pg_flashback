-- =================================================================
-- Independent restore proof: expected from shadow+schema_def,
-- actual from live, compared fail-closed (never tautological).
-- =================================================================

-- Immutable order_spec is provided by Rust:
--   flashback_fingerprint_order_spec(jsonb) -> jsonb
--   flashback_relation_full_data_fingerprint(regclass, jsonb) -> text

-- Canonical, round-trip-stable rendering of a constraint definition.
--
-- pg_get_constraintdef() is NOT round-trip stable for a very common shape:
-- an IN-list over a varchar column. PostgreSQL renders the array cast at
-- array level, and once that text is re-parsed -- which is exactly what
-- restore does when it rebuilds the relation from schema_def -- it comes
-- back distributed over the elements:
--
--   captured : ... = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[])
--   rebuilt  : ... = ANY (ARRAY[('a'::character varying)::text, ('b'::character varying)::text])
--
-- Both are the same constraint. Comparing the raw strings made every table
-- carrying CHECK (col IN (...)) on a varchar column fail restore
-- verification with "inventory digest mismatch", and therefore made such a
-- table unrecoverable even though its data and every other attribute
-- verified clean.
--
-- This normalizes only what PostgreSQL itself varies between those two
-- renderings: cast decorations, and parentheses wrapping a single atom.
-- Identifiers, literals, operators and real grouping are preserved, so a
-- changed value/column/operator -- or changed precedence such as
-- (a OR b) AND c versus a OR (b AND c) -- still compares unequal. Column
-- type drift is caught independently by the inventory's own 'columns'
-- section, which is still compared verbatim.
CREATE OR REPLACE FUNCTION flashback_canonical_constraint_def(p_def text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog
AS $fn$
DECLARE
    s text := COALESCE(p_def, '');
    prev text;
BEGIN
    IF s = '' THEN
        RETURN '';
    END IF;
    s := regexp_replace(s, '::\s*"[^"]+"(\[\])?', '', 'g');
    s := regexp_replace(s, '::\s*[A-Za-z_][A-Za-z_0-9]*(\s+[A-Za-z_][A-Za-z_0-9]*)*(\[\])?', '', 'g');
    LOOP
        prev := s;
        s := regexp_replace(s,
             '\(\s*([A-Za-z_][A-Za-z_0-9$]*|''[^'']*''|[0-9]+(\.[0-9]+)?)\s*\)', '\1', 'g');
        s := regexp_replace(s, '\(\s*\(([^()]*)\)\s*\)', '(\1)', 'g');
        EXIT WHEN s = prev;
    END LOOP;
    RETURN btrim(regexp_replace(s, '\s+', ' ', 'g'));
END;
$fn$;

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
    -- Constraints: full semantic definition from schema_def, canonicalized
    -- so a PostgreSQL re-render of the same constraint is not read as drift.
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', elem->>'name',
            'type', COALESCE(elem->>'type', elem->>'contype'),
            'def', public.flashback_canonical_constraint_def(
                       COALESCE(elem->>'def', elem->>'definition', ''))
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
        ) || CASE
            WHEN elem ? 'enabled'
            THEN jsonb_build_object('enabled', elem->>'enabled')
            ELSE '{}'::jsonb
        END
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
           OR COALESCE(elem->>'default_expr', '') ~* '^nextval\('
    LOOP
        v_sequences := v_sequences || jsonb_build_array(jsonb_build_object(
            'column', v_elem->>'name',
            'identity', COALESCE(v_elem->>'identity', ''),
            'sequence_schema', v_elem->'identity_options'->>'sequence_schema',
            'sequence_name', v_elem->'identity_options'->>'sequence_name',
            'data_type', v_elem->'identity_options'->>'data_type',
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
                    'default', COALESCE(elem->>'default_expr', '')
                ) || CASE
                    -- Old retained schema_def payloads predate explicit
                    -- collation capture.  Absence means "not proven", not
                    -- "expect an empty/default collation".
                    WHEN elem ? 'collation'
                    THEN jsonb_build_object(
                        'collation', COALESCE(elem->>'collation', '')
                    )
                    ELSE '{}'::jsonb
                END
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
    ) || CASE
        WHEN p_schema_def ? 'primary_key_constraint'
        THEN jsonb_build_object(
            'primary_key_constraint',
            COALESCE(p_schema_def->'primary_key_constraint', 'null'::jsonb)
        )
        ELSE '{}'::jsonb
    END;
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
        'primary_key_constraint', COALESCE((
            SELECT jsonb_build_object(
                'name', con.conname,
                -- Deliberately NOT canonicalized: the schema_def side leaves
                -- this key raw, so canonicalizing only here would introduce
                -- the very asymmetry this fix exists to remove. A PRIMARY KEY
                -- definition has no cast decoration to begin with, so it is
                -- round-trip stable as emitted. The same constraint is also
                -- carried, canonicalized on both sides, in 'constraints'.
                'def', pg_get_constraintdef(con.oid, true)
            )
            FROM pg_constraint con
            WHERE con.conrelid = v_oid
              AND con.contype = 'p'
            LIMIT 1
        ), 'null'::jsonb),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'type', format_type(a.atttypid, a.atttypmod),
                    'collation', CASE
                        WHEN a.attcollation <> 0
                        THEN format('%I.%I', coll_n.nspname, coll.collname)
                        ELSE ''
                    END,
                    'not_null', a.attnotnull,
                    'identity', a.attidentity::text,
                    'generated', a.attgenerated::text,
                    'default', COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            LEFT JOIN pg_attrdef ad
              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            LEFT JOIN pg_collation coll
              ON coll.oid = a.attcollation
            LEFT JOIN pg_namespace coll_n
              ON coll_n.oid = coll.collnamespace
            WHERE a.attrelid = v_oid
              AND a.attnum > 0
              AND NOT a.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', con.conname,
                    'type', con.contype::text,
                    'def', public.flashback_canonical_constraint_def(pg_get_constraintdef(con.oid, true))
                )
                ORDER BY con.conname, con.contype::text
            )
            FROM pg_constraint con
            WHERE con.conrelid = v_oid
              AND con.contype IN ('u', 'c', 'f')
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
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint con
                  WHERE con.conindid = i.indexrelid
              )
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', t.tgname,
                    'def', pg_get_triggerdef(t.oid),
                    'enabled', t.tgenabled::text
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
                -- Role OIDs are cluster-local and do not define the logical
                -- ACL set.  Match schema_def's name-based canonical order so
                -- identical grants hash identically after restore.
                ORDER BY CASE
                    WHEN ae.grantee = 0 THEN 'PUBLIC'
                    ELSE (SELECT rolname FROM pg_roles WHERE oid = ae.grantee)
                END,
                ae.privilege_type
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
                -- Match flashback_canonical_inventory_from_schema_def's
                -- ordering exactly (target, column, text) so an identical
                -- comment set produces an identical digest regardless of
                -- which side computed it.
                ORDER BY CASE WHEN d.objsubid = 0 THEN 'table' ELSE 'column' END,
                         COALESCE((SELECT a.attname FROM pg_attribute a
                                   WHERE a.attrelid = v_oid AND a.attnum = d.objsubid), ''),
                         d.description
            )
            FROM pg_description d
            WHERE d.objoid = v_oid
              AND d.classoid = 'pg_class'::regclass
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
    v_state jsonb;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_relation::oid;

    FOR v_elem IN
        SELECT elem
        FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
        WHERE elem->>'identity' IN ('a', 'd')
           OR COALESCE(elem->>'default_expr', '') ~* '^nextval\('
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
            v_state := jsonb_build_object(
                'column', v_col,
                'last_value', v_start,
                'is_called', false,
                'state_policy', 'edge_after_restore'
            );
        ELSE
            v_state := jsonb_build_object(
                'column', v_col,
                'last_value', v_edge,
                'is_called', true,
                'state_policy', 'edge_after_restore'
            );
        END IF;
        IF v_elem->'identity_options' IS NOT NULL
           AND v_elem->'identity_options' <> 'null'::jsonb
        THEN
            v_state := v_state || jsonb_strip_nulls(jsonb_build_object(
                'sequence_schema', CASE WHEN v_elem->'identity_options' ? 'sequence_schema'
                                        THEN v_elem#>>'{identity_options,sequence_schema}' END,
                'sequence_name', CASE WHEN v_elem->'identity_options' ? 'sequence_name'
                                      THEN v_elem#>>'{identity_options,sequence_name}' END,
                'data_type', CASE WHEN v_elem->'identity_options' ? 'data_type'
                                  THEN v_elem#>>'{identity_options,data_type}' END,
                'start', CASE WHEN v_elem->'identity_options' ? 'start'
                              THEN (v_elem#>>'{identity_options,start}')::bigint END,
                'increment', CASE WHEN v_elem->'identity_options' ? 'increment'
                                  THEN (v_elem#>>'{identity_options,increment}')::bigint END,
                'min', CASE WHEN v_elem->'identity_options' ? 'min'
                            THEN (v_elem#>>'{identity_options,min}')::bigint END,
                'max', CASE WHEN v_elem->'identity_options' ? 'max'
                            THEN (v_elem#>>'{identity_options,max}')::bigint END,
                'cache', CASE WHEN v_elem->'identity_options' ? 'cache'
                              THEN (v_elem#>>'{identity_options,cache}')::bigint END,
                'cycle', CASE WHEN v_elem->'identity_options' ? 'cycle'
                              THEN (v_elem#>>'{identity_options,cycle}')::boolean END
            ));
        END IF;
        v_out := v_out || jsonb_build_array(v_state);
    END LOOP;
    RETURN v_out;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_actual_sequence_states(
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
    v_col text;
    v_seq_reg regclass;
    v_seq_schema text;
    v_seq_name text;
    v_last_value bigint;
    v_is_called boolean;
    v_state jsonb;
    v_seq_options record;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_relation::oid;

    FOR v_elem IN
        SELECT elem
        FROM jsonb_array_elements(COALESCE(p_schema_def->'columns', '[]'::jsonb)) elem
        WHERE elem->>'identity' IN ('a', 'd')
           OR COALESCE(elem->>'default_expr', '') ~* '^nextval\('
    LOOP
        v_col := v_elem->>'name';
        v_seq_reg := to_regclass(pg_get_serial_sequence(
            format('%I.%I', v_schema, v_name), v_col
        ));
        IF v_seq_reg IS NULL THEN
            RAISE EXCEPTION
                'flashback_actual_sequence_states: owned sequence missing for %.%.%',
                v_schema, v_name, v_col;
        END IF;
        SELECT n.nspname, c.relname
          INTO v_seq_schema, v_seq_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = v_seq_reg::oid
          AND c.relkind = 'S';
        IF NOT FOUND THEN
            RAISE EXCEPTION
                'flashback_actual_sequence_states: sequence identity invalid for %.%.%',
                v_schema, v_name, v_col;
        END IF;

        EXECUTE format(
            'SELECT last_value::bigint, is_called FROM %I.%I',
            v_seq_schema, v_seq_name
        ) INTO v_last_value, v_is_called;

        v_state := jsonb_build_object(
            'column', v_col,
            'last_value', v_last_value,
            'is_called', v_is_called,
            'state_policy', 'edge_after_restore'
        );
        IF v_elem->'identity_options' IS NOT NULL
           AND v_elem->'identity_options' <> 'null'::jsonb
        THEN
            SELECT s.seqstart, s.seqincrement, s.seqmin, s.seqmax,
                   s.seqcache, s.seqcycle, format_type(s.seqtypid, NULL) AS data_type
              INTO v_seq_options
            FROM pg_sequence s
            WHERE s.seqrelid = v_seq_reg::oid;
            v_state := v_state || jsonb_strip_nulls(jsonb_build_object(
                'sequence_schema', CASE WHEN v_elem->'identity_options' ? 'sequence_schema'
                                        THEN v_seq_schema END,
                'sequence_name', CASE WHEN v_elem->'identity_options' ? 'sequence_name'
                                      THEN v_seq_name END,
                'data_type', CASE WHEN v_elem->'identity_options' ? 'data_type'
                                  THEN v_seq_options.data_type END,
                'start', CASE WHEN v_elem->'identity_options' ? 'start'
                              THEN v_seq_options.seqstart END,
                'increment', CASE WHEN v_elem->'identity_options' ? 'increment'
                                  THEN v_seq_options.seqincrement END,
                'min', CASE WHEN v_elem->'identity_options' ? 'min'
                            THEN v_seq_options.seqmin END,
                'max', CASE WHEN v_elem->'identity_options' ? 'max'
                            THEN v_seq_options.seqmax END,
                'cache', CASE WHEN v_elem->'identity_options' ? 'cache'
                              THEN v_seq_options.seqcache END,
                'cycle', CASE WHEN v_elem->'identity_options' ? 'cycle'
                              THEN v_seq_options.seqcycle END
            ));
        END IF;
        v_out := v_out || jsonb_build_array(v_state);
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
    -- schema_def never records the pre-drop replica identity (nothing
    -- captures it), and it would be the wrong expectation even if it did:
    -- flashback_internal_restore_lsn_core unconditionally sets REPLICA
    -- IDENTITY FULL on every local_delta restore (needed for this
    -- lifecycle's own ongoing old-row-image WAL capture; the pre-drop
    -- original is remembered and restored only on flashback_untrack, a
    -- different operation). The expected proof must assert what restore
    -- actually and deliberately produces, not schema_def's unpopulated
    -- default.
    v_inventory := v_inventory || jsonb_build_object('replica_identity', 'f');
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
        -- 'sequences' is excluded from the digest: its shape here (static
        -- identity/increment/start from schema_def) deliberately differs
        -- from the live side's shape (last_value/is_called), and live
        -- sequence correctness is already independently compared via the
        -- 'sequence_states' key below (flashback_compare_restore_proofs
        -- checks it directly, not through this digest).
        'inventory_digest', public.flashback_inventory_digest(v_inventory - 'sequences'),
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
    -- Compare newly captured metadata exactly, while keeping old retained
    -- schema_def payloads restorable.  A historical payload cannot prove a
    -- field that did not exist in its contract, so remove only those new
    -- actual-side fields when the target schema_def lacks them.
    IF NOT p_schema_def ? 'primary_key_constraint' THEN
        v_inventory := v_inventory - 'primary_key_constraint';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
            COALESCE(p_schema_def->'columns', '[]'::jsonb)
        ) AS col
        WHERE col ? 'collation'
    ) THEN
        v_inventory := jsonb_set(
            v_inventory,
            '{columns}',
            COALESCE((
                SELECT jsonb_agg(col - 'collation')
                FROM jsonb_array_elements(
                    COALESCE(v_inventory->'columns', '[]'::jsonb)
                ) AS col
            ), '[]'::jsonb)
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
            COALESCE(p_schema_def->'triggers', '[]'::jsonb)
        ) AS trig
        WHERE trig ? 'enabled'
    ) THEN
        v_inventory := jsonb_set(
            v_inventory,
            '{triggers}',
            COALESCE((
                SELECT jsonb_agg(trig - 'enabled' ORDER BY trig->>'name')
                FROM jsonb_array_elements(
                    COALESCE(v_inventory->'triggers', '[]'::jsonb)
                ) AS trig
            ), '[]'::jsonb)
        );
    END IF;
    v_seq_states := public.flashback_actual_sequence_states(p_live, p_schema_def);
    -- This side reads each live sequence relation's last_value/is_called.
    -- The expected side derives the edge contract independently from table
    -- contents, so a wrong setval can no longer verify itself.
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
        -- A3: the digest actually compared by flashback_compare_restore_proofs
        -- must be computed from this function's OWN live-catalog inventory
        -- (v_inventory, from flashback_canonical_inventory_from_relation
        -- against the just-restored relation p_live), never recomputed from
        -- p_schema_def -- that would make the "actual" proof a pure function
        -- of the same stored metadata the expected side already derives from,
        -- so any drift between the live relation and schema_def (a dropped
        -- constraint/index/policy/trigger, a changed ACL/owner/comment/
        -- tablespace/reloptions/FORCE RLS) would never fail verification.
        -- 'sequences' is excluded for the same shape-parity reason as the
        -- expected side; 'sequence_states' (already merged into v_inventory
        -- above) is what flashback_compare_restore_proofs actually checks
        -- for sequence correctness.
        'inventory_digest', public.flashback_inventory_digest(v_inventory - 'sequences'),
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

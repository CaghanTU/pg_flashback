-- =================================================================
-- Machine-checked local_delta compatibility gate.
--
-- This is the single source of truth for "will pg_flashback correctly
-- protect/reconstruct this table" for the local DROP-recovery product.
-- docs/SUPPORT.md must describe exactly the preserve/reject lists below
-- and nothing more — it documents this gate, it does not replace it.
--
-- Preserve set (supported, reconstructed/verified by the restore engine):
--   ordinary columns; identity/serial columns; PRIMARY KEY/UNIQUE/CHECK
--   constraints; outgoing FOREIGN KEY constraints; plain btree indexes;
--   owner; table/column ACL; TOAST; replica identity; basic RLS policies;
--   ordinary (non-internal) triggers; comments; tablespace/reloptions;
--   owned sequences.
--
-- Reject set (fail closed; table cannot be tracked / epoch cannot recover):
--   partitioned tables and partitions; classical inheritance; foreign
--   tables; TEMP/UNLOGGED tables; materialized views; extension-owned
--   relations; exclusion constraints; rules; security labels;
--   publications; INCOMING foreign keys; non-btree, expression, or
--   partial indexes; generated columns (reconstruction is not proven —
--   rejected by default even for "simple" cases).
-- =================================================================

-- Live-relation classifier index. Uses the catalog directly, so it can see
-- everything (incoming FK, extension ownership, rules, security labels,
-- publications, exclusion constraints, exact index access method/partial/
-- expression shape) that the reduced schema_def JSONB cannot represent.
CREATE OR REPLACE FUNCTION flashback_local_compatibility(p_rel regclass)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_oid oid := p_rel;
    v_relkind "char";
    v_persistence "char";
    v_schema text;
    v_name text;
    v_owner text;
    v_parent_relkind "char";
    v_rejected text[] := ARRAY[]::text[];
    v_preserved text[] := ARRAY[]::text[];
    v_detected jsonb;
    v_generated_cols text[];
    v_identity_cols text[];
    v_pk_present boolean;
    v_unique_count integer;
    v_check_count integer;
    v_outgoing_fk_count integer;
    v_incoming_fk_count integer;
    v_exclusion_count integer;
    v_btree_idx_count integer;
    v_nonbtree_idx text[];
    v_partial_idx text[];
    v_expr_idx text[];
    v_is_partition_child boolean;
    v_has_inh_children boolean;
    v_has_inh_parent boolean;
    v_extension_owned boolean;
    v_rule_count integer;
    v_seclabel_count integer;
    v_publication_count integer;
    v_trigger_count integer;
    v_policy_count integer;
    v_rls_enabled boolean;
    v_comment_count integer;
    v_sequence_count integer;
    v_toast boolean;
    v_replident "char";
    v_tablespace text;
    v_reloptions text[];
BEGIN
    IF v_oid IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'relation', NULL,
            'supported', false,
            'detected_features', '{}'::jsonb,
            'preserved_features', '[]'::jsonb,
            'rejected_features', to_jsonb(ARRAY['relation_does_not_exist']),
            'reason', 'relation does not exist',
            'action', 'reject'
        );
    END IF;

    SELECT c.relkind, c.relpersistence, n.nspname, c.relname,
           r.rolname, (c.reltoastrelid <> 0), c.relreplident,
           NULLIF(ts.spcname, ''), c.reloptions
      INTO v_relkind, v_persistence, v_schema, v_name,
           v_owner, v_toast, v_replident,
           v_tablespace, v_reloptions
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_roles r ON r.oid = c.relowner
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
    WHERE c.oid = v_oid;

    -- Relation kind / persistence gate.
    IF v_schema = 'pg_temp' OR v_schema LIKE 'pg_temp_%' OR v_schema LIKE 'pg_toast_temp_%' THEN
        v_rejected := v_rejected || ARRAY['temp_table'];
    END IF;
    IF v_relkind = 'p' THEN
        v_rejected := v_rejected || ARRAY['partitioned_table'];
    ELSIF v_relkind = 'f' THEN
        v_rejected := v_rejected || ARRAY['foreign_table'];
    ELSIF v_relkind = 'm' THEN
        v_rejected := v_rejected || ARRAY['materialized_view'];
    ELSIF v_relkind <> 'r' THEN
        v_rejected := v_rejected || ARRAY[format('unsupported_relkind_%s', v_relkind)];
    END IF;
    IF v_persistence = 'u' THEN
        v_rejected := v_rejected || ARRAY['unlogged_table'];
    ELSIF v_persistence = 't' THEN
        v_rejected := v_rejected || ARRAY['temp_table'];
    END IF;

    SELECT p.relkind INTO v_parent_relkind
    FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
    WHERE i.inhrelid = v_oid
    LIMIT 1;
    v_is_partition_child := (v_parent_relkind = 'p');
    IF v_is_partition_child THEN
        v_rejected := v_rejected || ARRAY['partition_of_partitioned_table'];
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_inherits i WHERE i.inhparent = v_oid
    ) INTO v_has_inh_children;
    SELECT EXISTS (
        SELECT 1 FROM pg_inherits i
        WHERE i.inhrelid = v_oid AND NOT v_is_partition_child
    ) INTO v_has_inh_parent;
    IF v_has_inh_children THEN
        v_rejected := v_rejected || ARRAY['classical_inheritance_children'];
    END IF;
    IF v_has_inh_parent THEN
        v_rejected := v_rejected || ARRAY['classical_inheritance_parent'];
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = v_oid
          AND d.deptype = 'e'
    ) INTO v_extension_owned;
    IF v_extension_owned THEN
        v_rejected := v_rejected || ARRAY['extension_owned'];
    END IF;

    -- Columns: generated (rejected by default) and identity (preserved).
    SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
      INTO v_generated_cols
    FROM pg_attribute a
    WHERE a.attrelid = v_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attgenerated <> '';
    IF array_length(v_generated_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['generated_columns'];
    END IF;

    SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
      INTO v_identity_cols
    FROM pg_attribute a
    WHERE a.attrelid = v_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attidentity <> '';
    IF array_length(v_identity_cols, 1) > 0 THEN
        v_preserved := v_preserved || ARRAY['identity_or_serial_columns'];
    END IF;

    -- Constraints.
    SELECT EXISTS (
        SELECT 1 FROM pg_constraint con
        WHERE con.conrelid = v_oid AND con.contype = 'p'
    ) INTO v_pk_present;
    SELECT count(*) INTO v_unique_count
    FROM pg_constraint con WHERE con.conrelid = v_oid AND con.contype = 'u';
    SELECT count(*) INTO v_check_count
    FROM pg_constraint con WHERE con.conrelid = v_oid AND con.contype = 'c';
    SELECT count(*) INTO v_outgoing_fk_count
    FROM pg_constraint con WHERE con.conrelid = v_oid AND con.contype = 'f';
    SELECT count(*) INTO v_incoming_fk_count
    FROM pg_constraint con WHERE con.confrelid = v_oid AND con.contype = 'f';
    SELECT count(*) INTO v_exclusion_count
    FROM pg_constraint con WHERE con.conrelid = v_oid AND con.contype = 'x';

    IF v_pk_present THEN
        v_preserved := v_preserved || ARRAY['primary_key'];
    END IF;
    IF v_unique_count > 0 THEN
        v_preserved := v_preserved || ARRAY['unique_constraints'];
    END IF;
    IF v_check_count > 0 THEN
        v_preserved := v_preserved || ARRAY['check_constraints'];
    END IF;
    IF v_outgoing_fk_count > 0 THEN
        v_preserved := v_preserved || ARRAY['outgoing_foreign_keys'];
    END IF;
    IF v_incoming_fk_count > 0 THEN
        v_rejected := v_rejected || ARRAY['incoming_foreign_keys'];
    END IF;
    IF v_exclusion_count > 0 THEN
        v_rejected := v_rejected || ARRAY['exclusion_constraints'];
    END IF;

    -- Indexes: only plain btree, non-partial, non-expression indexes are
    -- reconstructed/verified. indkey containing a 0 entry means at least one
    -- expression key column.
    SELECT count(*) FILTER (
               WHERE am.amname = 'btree'
                 AND i.indpred IS NULL
                 AND NOT (0 = ANY(i.indkey))
           ),
           COALESCE(array_agg(DISTINCT ic.relname) FILTER (
               WHERE am.amname <> 'btree'
           ), ARRAY[]::text[]),
           COALESCE(array_agg(DISTINCT ic.relname) FILTER (
               WHERE i.indpred IS NOT NULL
           ), ARRAY[]::text[]),
           COALESCE(array_agg(DISTINCT ic.relname) FILTER (
               WHERE 0 = ANY(i.indkey)
           ), ARRAY[]::text[])
      INTO v_btree_idx_count, v_nonbtree_idx, v_partial_idx, v_expr_idx
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_am am ON am.oid = ic.relam
    WHERE i.indrelid = v_oid;

    IF v_btree_idx_count > 0 THEN
        v_preserved := v_preserved || ARRAY['btree_indexes'];
    END IF;
    IF array_length(v_nonbtree_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['non_btree_indexes'];
    END IF;
    IF array_length(v_partial_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['partial_indexes'];
    END IF;
    IF array_length(v_expr_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['expression_indexes'];
    END IF;

    SELECT count(*) INTO v_rule_count
    FROM pg_rewrite rw
    WHERE rw.ev_class = v_oid AND rw.rulename <> '_RETURN';
    IF v_rule_count > 0 THEN
        v_rejected := v_rejected || ARRAY['rules'];
    END IF;

    SELECT count(*) INTO v_seclabel_count
    FROM pg_seclabel sl
    WHERE sl.objoid = v_oid AND sl.classoid = 'pg_class'::regclass AND sl.objsubid = 0;
    IF v_seclabel_count > 0 THEN
        v_rejected := v_rejected || ARRAY['security_labels'];
    END IF;

    SELECT count(*) INTO v_publication_count
    FROM pg_publication_rel pr
    WHERE pr.prrelid = v_oid;
    IF v_publication_count > 0 THEN
        v_rejected := v_rejected || ARRAY['publications'];
    END IF;

    SELECT count(*) INTO v_trigger_count
    FROM pg_trigger tg
    WHERE tg.tgrelid = v_oid AND NOT tg.tgisinternal
      AND tg.tgname NOT LIKE 'flashback_capture_%';
    IF v_trigger_count > 0 THEN
        v_preserved := v_preserved || ARRAY['ordinary_triggers'];
    END IF;

    SELECT count(*) INTO v_policy_count
    FROM pg_policy pol WHERE pol.polrelid = v_oid;
    SELECT c.relrowsecurity INTO v_rls_enabled FROM pg_class c WHERE c.oid = v_oid;
    IF v_policy_count > 0 OR v_rls_enabled THEN
        v_preserved := v_preserved || ARRAY['basic_rls_policies'];
    END IF;

    SELECT count(*) INTO v_comment_count
    FROM pg_description d
    WHERE d.objoid = v_oid AND d.classoid = 'pg_class'::regclass;
    IF v_comment_count > 0 THEN
        v_preserved := v_preserved || ARRAY['comments'];
    END IF;

    SELECT count(*) INTO v_sequence_count
    FROM pg_depend d
    JOIN pg_class sc ON sc.oid = d.objid AND sc.relkind = 'S'
    WHERE d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype IN ('a', 'i');
    IF v_sequence_count > 0 THEN
        v_preserved := v_preserved || ARRAY['owned_sequences'];
    END IF;

    IF v_toast THEN
        v_preserved := v_preserved || ARRAY['toast'];
    END IF;
    v_preserved := v_preserved || ARRAY['replica_identity'];
    v_preserved := v_preserved || ARRAY['owner_and_acl'];
    v_preserved := v_preserved || ARRAY['tablespace_and_reloptions'];
    v_preserved := v_preserved || ARRAY['ordinary_columns'];

    v_detected := jsonb_build_object(
        'schema', v_schema,
        'table', v_name,
        'relkind', v_relkind,
        'persistence', v_persistence,
        'owner', v_owner,
        'generated_columns', to_jsonb(v_generated_cols),
        'identity_columns', to_jsonb(v_identity_cols),
        'primary_key', v_pk_present,
        'unique_constraints', v_unique_count,
        'check_constraints', v_check_count,
        'outgoing_fk', v_outgoing_fk_count,
        'incoming_fk', v_incoming_fk_count,
        'exclusion_constraints', v_exclusion_count,
        'btree_indexes', v_btree_idx_count,
        'non_btree_indexes', to_jsonb(v_nonbtree_idx),
        'partial_indexes', to_jsonb(v_partial_idx),
        'expression_indexes', to_jsonb(v_expr_idx),
        'inheritance_children', v_has_inh_children,
        'inheritance_parent', v_has_inh_parent,
        'partition_child', v_is_partition_child,
        'extension_owned', v_extension_owned,
        'rules', v_rule_count,
        'security_labels', v_seclabel_count,
        'publications', v_publication_count,
        'triggers', v_trigger_count,
        'rls_policies', v_policy_count,
        'rls_enabled', COALESCE(v_rls_enabled, false),
        'comments', v_comment_count,
        'owned_sequences', v_sequence_count,
        'toast', v_toast,
        'replica_identity', v_replident,
        'tablespace', v_tablespace,
        'reloptions', to_jsonb(v_reloptions)
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'relation', format('%I.%I', v_schema, v_name),
        'supported', (array_length(v_rejected, 1) IS NULL),
        'detected_features', v_detected,
        'preserved_features', to_jsonb(v_preserved),
        'rejected_features', to_jsonb(v_rejected),
        'reason', CASE
            WHEN array_length(v_rejected, 1) IS NULL THEN NULL
            ELSE array_to_string(v_rejected, ', ')
        END,
        'action', CASE
            WHEN array_length(v_rejected, 1) IS NULL THEN 'track'
            ELSE 'reject'
        END
    );
END;
$$;

-- Text-only classifier for one captured `indexes[]` entry ({name, def}) from
-- a stored schema_def epoch. pg_get_indexdef() always renders an explicit
-- "USING <method>" clause and a trailing "WHERE <predicate>" for partial
-- indexes, so both are detected by substring match. Expression indexes are
-- detected by the presence of a nested "(" inside the column-list
-- parenthesis (a plain column list is a simple comma-separated identifier/
-- collation/opclass/sort-order list with no nested parentheses).
CREATE OR REPLACE FUNCTION flashback_local_compatibility_classify_index_def(p_def text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog
AS $$
DECLARE
    v_def text := COALESCE(p_def, '');
    v_is_btree boolean;
    v_is_partial boolean;
    v_cols text;
    v_is_expression boolean;
BEGIN
    v_is_btree := (v_def ~* '\musing\s+btree\M');
    v_is_partial := (v_def ~* '\)\s*where\s');
    v_cols := substring(v_def from '\(((?:[^()]|\([^()]*\))*)\)');
    v_is_expression := (v_cols IS NOT NULL AND v_cols ~ '\(');
    RETURN jsonb_build_object(
        'is_btree', v_is_btree,
        'is_partial', v_is_partial,
        'is_expression', COALESCE(v_is_expression, false)
    );
END;
$$;

-- Epoch classifier for a stored schema_def JSONB (flashback_collect_schema_def
-- shape). Used to gate recover-plan target schema epochs where the live
-- catalog for the original relation may no longer exist (post-DROP).
-- schema_def does not capture incoming FK, exclusion constraints, rules,
-- security labels, publications, or extension ownership — those classes are
-- gated exclusively at protect-time by flashback_local_compatibility(regclass).
CREATE OR REPLACE FUNCTION flashback_local_compatibility_schema_def(p_schema_def jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_def jsonb := COALESCE(p_schema_def, '{}'::jsonb);
    v_rejected text[] := ARRAY[]::text[];
    v_preserved text[] := ARRAY[]::text[];
    v_generated_cols text[];
    v_identity_cols text[];
    v_pk_len integer;
    v_unique_count integer;
    v_check_count integer;
    v_fk_count integer;
    v_idx record;
    v_class jsonb;
    v_nonbtree_idx text[] := ARRAY[]::text[];
    v_partial_idx text[] := ARRAY[]::text[];
    v_expr_idx text[] := ARRAY[]::text[];
    v_btree_count integer := 0;
    v_trigger_count integer;
    v_policy_count integer;
BEGIN
    SELECT
        COALESCE(array_agg(col->>'name') FILTER (WHERE COALESCE(col->>'generated', '') <> ''), ARRAY[]::text[]),
        COALESCE(array_agg(col->>'name') FILTER (WHERE COALESCE(col->>'identity', '') <> ''), ARRAY[]::text[])
      INTO v_generated_cols, v_identity_cols
    FROM jsonb_array_elements(COALESCE(v_def->'columns', '[]'::jsonb)) col;

    IF array_length(v_generated_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['generated_columns'];
    END IF;
    IF array_length(v_identity_cols, 1) > 0 THEN
        v_preserved := v_preserved || ARRAY['identity_or_serial_columns'];
    END IF;

    -- jsonb_array_length() errors on a scalar. schema_def stores JSON null
    -- (not SQL NULL) for fields like 'partitions' when not applicable
    -- (api_track_capture.sql's `CASE ... ELSE NULL END`), so COALESCE alone
    -- does not protect these lookups; normalize any non-array value to '[]'.
    v_def := v_def
        || jsonb_build_object('primary_key', CASE WHEN jsonb_typeof(v_def->'primary_key') = 'array' THEN v_def->'primary_key' ELSE '[]'::jsonb END)
        || jsonb_build_object('constraints', CASE WHEN jsonb_typeof(v_def->'constraints') = 'array' THEN v_def->'constraints' ELSE '[]'::jsonb END)
        || jsonb_build_object('indexes', CASE WHEN jsonb_typeof(v_def->'indexes') = 'array' THEN v_def->'indexes' ELSE '[]'::jsonb END)
        || jsonb_build_object('partitions', CASE WHEN jsonb_typeof(v_def->'partitions') = 'array' THEN v_def->'partitions' ELSE '[]'::jsonb END)
        || jsonb_build_object('triggers', CASE WHEN jsonb_typeof(v_def->'triggers') = 'array' THEN v_def->'triggers' ELSE '[]'::jsonb END)
        || jsonb_build_object('rls_policies', CASE WHEN jsonb_typeof(v_def->'rls_policies') = 'array' THEN v_def->'rls_policies' ELSE '[]'::jsonb END)
        || jsonb_build_object('columns', CASE WHEN jsonb_typeof(v_def->'columns') = 'array' THEN v_def->'columns' ELSE '[]'::jsonb END);

    v_pk_len := jsonb_array_length(COALESCE(v_def->'primary_key', '[]'::jsonb));
    IF v_pk_len > 0 THEN
        v_preserved := v_preserved || ARRAY['primary_key'];
    END IF;

    SELECT
        count(*) FILTER (WHERE con->>'type' = 'u'),
        count(*) FILTER (WHERE con->>'type' = 'c'),
        count(*) FILTER (WHERE con->>'type' = 'f')
      INTO v_unique_count, v_check_count, v_fk_count
    FROM jsonb_array_elements(COALESCE(v_def->'constraints', '[]'::jsonb)) con;

    IF COALESCE(v_unique_count, 0) > 0 THEN
        v_preserved := v_preserved || ARRAY['unique_constraints'];
    END IF;
    IF COALESCE(v_check_count, 0) > 0 THEN
        v_preserved := v_preserved || ARRAY['check_constraints'];
    END IF;
    IF COALESCE(v_fk_count, 0) > 0 THEN
        v_preserved := v_preserved || ARRAY['outgoing_foreign_keys'];
    END IF;

    FOR v_idx IN
        SELECT idx->>'name' AS name, idx->>'def' AS def
        FROM jsonb_array_elements(COALESCE(v_def->'indexes', '[]'::jsonb)) idx
    LOOP
        v_class := flashback_local_compatibility_classify_index_def(v_idx.def);
        IF NOT COALESCE((v_class->>'is_btree')::boolean, false) THEN
            v_nonbtree_idx := v_nonbtree_idx || ARRAY[v_idx.name];
        ELSIF COALESCE((v_class->>'is_partial')::boolean, false) THEN
            v_partial_idx := v_partial_idx || ARRAY[v_idx.name];
        ELSIF COALESCE((v_class->>'is_expression')::boolean, false) THEN
            v_expr_idx := v_expr_idx || ARRAY[v_idx.name];
        ELSE
            v_btree_count := v_btree_count + 1;
        END IF;
    END LOOP;

    IF v_btree_count > 0 THEN
        v_preserved := v_preserved || ARRAY['btree_indexes'];
    END IF;
    IF array_length(v_nonbtree_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['non_btree_indexes'];
    END IF;
    IF array_length(v_partial_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['partial_indexes'];
    END IF;
    IF array_length(v_expr_idx, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['expression_indexes'];
    END IF;

    IF (v_def ? 'partition_by' AND v_def->>'partition_by' IS NOT NULL)
       OR jsonb_array_length(COALESCE(v_def->'partitions', '[]'::jsonb)) > 0
    THEN
        v_rejected := v_rejected || ARRAY['partitioned_table'];
    END IF;

    v_trigger_count := jsonb_array_length(COALESCE(v_def->'triggers', '[]'::jsonb));
    IF v_trigger_count > 0 THEN
        v_preserved := v_preserved || ARRAY['ordinary_triggers'];
    END IF;

    v_policy_count := jsonb_array_length(COALESCE(v_def->'rls_policies', '[]'::jsonb));
    IF v_policy_count > 0 OR COALESCE((v_def->>'rls_enabled')::boolean, false) THEN
        v_preserved := v_preserved || ARRAY['basic_rls_policies'];
    END IF;

    IF v_def ? 'owner' THEN
        v_preserved := v_preserved || ARRAY['owner_and_acl'];
    END IF;

    v_preserved := v_preserved || ARRAY['ordinary_columns'];

    RETURN jsonb_build_object(
        'schema_version', 1,
        'relation', format('%s.%s', v_def->>'schema', v_def->>'table'),
        'supported', (array_length(v_rejected, 1) IS NULL),
        'detected_features', jsonb_build_object(
            'generated_columns', to_jsonb(v_generated_cols),
            'identity_columns', to_jsonb(v_identity_cols),
            'primary_key_columns', v_pk_len,
            'unique_constraints', COALESCE(v_unique_count, 0),
            'check_constraints', COALESCE(v_check_count, 0),
            'outgoing_fk', COALESCE(v_fk_count, 0),
            'non_btree_indexes', to_jsonb(v_nonbtree_idx),
            'partial_indexes', to_jsonb(v_partial_idx),
            'expression_indexes', to_jsonb(v_expr_idx),
            'triggers', v_trigger_count,
            'rls_policies', v_policy_count
        ),
        'preserved_features', to_jsonb(v_preserved),
        'rejected_features', to_jsonb(v_rejected),
        'reason', CASE
            WHEN array_length(v_rejected, 1) IS NULL THEN NULL
            ELSE array_to_string(v_rejected, ', ')
        END,
        'action', CASE
            WHEN array_length(v_rejected, 1) IS NULL THEN 'restore'
            ELSE 'reject'
        END,
        'note', 'epoch check operates on the captured schema_def shape only; incoming FK, exclusion constraints, rules, security labels, publications and extension ownership are gated at protect-time by flashback_local_compatibility(regclass)'
    );
END;
$$;

-- Fail-closed gate: RAISE unless flashback_local_compatibility(p_rel) reports
-- supported=true. Intended to be called under SECURITY DEFINER from guarded
-- lifecycle entry points (flashback_track), matching the
-- flashback_require_supported_local_table style.
CREATE OR REPLACE FUNCTION flashback_require_local_compatibility(p_rel regclass)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_report jsonb;
BEGIN
    v_report := flashback_local_compatibility(p_rel);
    IF NOT COALESCE((v_report->>'supported')::boolean, false) THEN
        RAISE EXCEPTION
            'pg_flashback: % is not compatible with the local DROP recovery product: %',
            COALESCE(v_report->>'relation', p_rel::text),
            COALESCE(v_report->>'reason', 'unknown')
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'See docs/SUPPORT.md preserve/reject lists; remove or restructure the rejected features before protect.',
                  DETAIL = v_report::text;
    END IF;
END;
$$;

COMMENT ON FUNCTION flashback_local_compatibility(regclass) IS
    'Machine-checked local_delta preserve/reject compatibility report for a live relation. Source of truth for docs/SUPPORT.md.';
COMMENT ON FUNCTION flashback_local_compatibility_schema_def(jsonb) IS
    'Machine-checked compatibility report for a stored schema_def epoch (post-DROP / target generation). Narrower than the live-relation check.';
COMMENT ON FUNCTION flashback_require_local_compatibility(regclass) IS
    '[Internal] Fail-closed compatibility gate called from flashback_track(). Not granted directly to flashback_admin.';

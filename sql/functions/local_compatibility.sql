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
--   owner; table-level ACL; TOAST; replica identity; basic RLS policies;
--   ordinary (non-internal) triggers; comments; ordinary owned sequences
--   (identity/serial identity, options, ownership, and safe edge state).
--
-- Reject set (fail closed; table cannot be tracked / epoch cannot recover):
--   partitioned tables and partitions; classical inheritance; foreign
--   tables; TEMP/UNLOGGED tables; materialized views; extension-owned
--   relations; exclusion constraints; rules; security labels;
--   publications; INCOMING foreign keys; non-btree, expression, or
--   partial indexes; generated columns (reconstruction is not proven —
--   rejected by default even for "simple" cases); column-level ACL;
--   non-default tablespace/reloptions; per-column storage/compression
--   overrides; custom owner/ACL/comment metadata on owned sequences.
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
    v_external_sequence_cols text[];
    v_owned_sequence_without_default_cols text[];
    v_custom_storage_cols text[];
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
    v_custom_sequence_metadata_count integer;
    v_toast boolean;
    v_replident "char";
    v_tablespace text;
    v_reloptions text[];
    v_force_rls boolean;
    v_column_acl_count integer;
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
           NULLIF(ts.spcname, ''), c.reloptions, c.relforcerowsecurity
      INTO v_relkind, v_persistence, v_schema, v_name,
           v_owner, v_toast, v_replident,
           v_tablespace, v_reloptions, v_force_rls
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

    -- A nextval() default backed by a sequence that is not owned by this
    -- table is an external dependency, not part of this table's artifact
    -- set.  Do not promise a self-contained DROP recovery for it.
    SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
      INTO v_external_sequence_cols
    FROM pg_attribute a
    JOIN pg_attrdef ad
      ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
    WHERE a.attrelid = v_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND pg_get_expr(ad.adbin, ad.adrelid) ~* '^nextval\('
      AND NOT EXISTS (
          SELECT 1
          FROM pg_depend d
          JOIN pg_class seqc
            ON seqc.oid = d.objid AND seqc.relkind = 'S'
          WHERE d.classid = 'pg_class'::regclass
            AND d.refclassid = 'pg_class'::regclass
            AND d.refobjid = a.attrelid
            AND d.refobjsubid = a.attnum
            AND d.deptype IN ('a', 'i')
      );
    IF array_length(v_external_sequence_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['external_sequence_default'];
    END IF;

    SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
      INTO v_owned_sequence_without_default_cols
    FROM pg_depend d
    JOIN pg_class seqc
      ON seqc.oid = d.objid AND seqc.relkind = 'S'
    JOIN pg_attribute a
      ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
    LEFT JOIN pg_attrdef ad
      ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
    WHERE d.classid = 'pg_class'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype IN ('a', 'i')
      AND a.attidentity = ''
      AND COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '') !~* '^nextval\(';
    IF array_length(v_owned_sequence_without_default_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['owned_sequence_without_column_default'];
    END IF;

    SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
      INTO v_custom_storage_cols
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = v_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (
          a.attstorage IS DISTINCT FROM t.typstorage
          OR a.attcompression::text <> ''
      );
    IF array_length(v_custom_storage_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['column_storage_or_compression'];
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
    SELECT count(*) INTO v_custom_sequence_metadata_count
    FROM pg_depend d
    JOIN pg_class seqc ON seqc.oid = d.objid AND seqc.relkind = 'S'
    JOIN pg_class tbl ON tbl.oid = d.refobjid
    WHERE d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype IN ('a', 'i')
      AND (
          seqc.relowner IS DISTINCT FROM tbl.relowner
          OR seqc.relacl IS NOT NULL
          OR obj_description(seqc.oid, 'pg_class') IS NOT NULL
      );
    IF v_custom_sequence_metadata_count > 0 THEN
        v_rejected := v_rejected || ARRAY['custom_owned_sequence_metadata'];
    END IF;

    -- Column-level ACL is never captured or restored (only the table-level
    -- ACL is); a table with any column GRANT cannot honestly be tracked.
    SELECT count(*) INTO v_column_acl_count
    FROM pg_attribute a
    WHERE a.attrelid = v_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attacl IS NOT NULL;
    IF v_column_acl_count > 0 THEN
        v_rejected := v_rejected || ARRAY['column_acl'];
    END IF;

    -- A non-default tablespace or non-empty storage reloptions are never
    -- captured or the base table recreated with them (only dependent-view
    -- reloptions are handled) -- reject rather than silently drop them.
    IF v_tablespace IS NOT NULL THEN
        v_rejected := v_rejected || ARRAY['non_default_tablespace'];
    END IF;
    IF v_reloptions IS NOT NULL AND array_length(v_reloptions, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['storage_reloptions'];
    END IF;

    IF v_toast THEN
        v_preserved := v_preserved || ARRAY['toast'];
    END IF;
    v_preserved := v_preserved || ARRAY['replica_identity'];
    v_preserved := v_preserved || ARRAY['owner_and_acl'];
    v_preserved := v_preserved || ARRAY['ordinary_columns'];
    IF v_force_rls THEN
        v_preserved := v_preserved || ARRAY['force_row_level_security'];
    END IF;

    v_detected := jsonb_build_object(
        'schema', v_schema,
        'table', v_name,
        'relkind', v_relkind,
        'persistence', v_persistence,
        'owner', v_owner,
        'generated_columns', to_jsonb(v_generated_cols),
        'identity_columns', to_jsonb(v_identity_cols),
        'external_sequence_columns', to_jsonb(v_external_sequence_cols),
        'owned_sequence_without_default_columns',
            to_jsonb(v_owned_sequence_without_default_cols),
        'custom_storage_columns', to_jsonb(v_custom_storage_cols),
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
        'custom_owned_sequence_metadata', v_custom_sequence_metadata_count,
        'toast', v_toast,
        'replica_identity', v_replident,
        'tablespace', v_tablespace,
        'reloptions', to_jsonb(v_reloptions),
        'force_rls', COALESCE(v_force_rls, false),
        'column_acl_count', v_column_acl_count
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
    v_external_sequence_cols text[];
    v_owned_sequence_without_default_cols text[];
    v_custom_storage_cols text[];
    v_custom_sequence_cols text[];
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
        COALESCE(array_agg(col->>'name') FILTER (WHERE COALESCE(col->>'identity', '') <> ''), ARRAY[]::text[]),
        COALESCE(array_agg(col->>'name') FILTER (
            WHERE COALESCE((col->>'storage_custom')::boolean, false)
        ), ARRAY[]::text[]),
        COALESCE(array_agg(col->>'name') FILTER (
            WHERE COALESCE((col#>>'{identity_options,custom_metadata}')::boolean, false)
        ), ARRAY[]::text[]),
        COALESCE(array_agg(col->>'name') FILTER (
            WHERE COALESCE(col->>'default_expr', '') ~* '^nextval\('
              AND (
                  col->'identity_options' IS NULL
                  OR col->'identity_options' = 'null'::jsonb
              )
        ), ARRAY[]::text[]),
        COALESCE(array_agg(col->>'name') FILTER (
            WHERE col->'identity_options' IS NOT NULL
              AND col->'identity_options' <> 'null'::jsonb
              AND COALESCE(col->>'identity', '') = ''
              AND COALESCE(col->>'default_expr', '') !~* '^nextval\('
        ), ARRAY[]::text[])
      INTO v_generated_cols, v_identity_cols,
           v_custom_storage_cols, v_custom_sequence_cols,
           v_external_sequence_cols,
           v_owned_sequence_without_default_cols
    FROM jsonb_array_elements(COALESCE(v_def->'columns', '[]'::jsonb)) col;

    IF array_length(v_generated_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['generated_columns'];
    END IF;
    IF array_length(v_identity_cols, 1) > 0 THEN
        v_preserved := v_preserved || ARRAY['identity_or_serial_columns'];
    END IF;
    IF array_length(v_custom_storage_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['column_storage_or_compression'];
    END IF;
    IF array_length(v_custom_sequence_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['custom_owned_sequence_metadata'];
    END IF;
    IF array_length(v_external_sequence_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['external_sequence_default'];
    END IF;
    IF array_length(v_owned_sequence_without_default_cols, 1) > 0 THEN
        v_rejected := v_rejected || ARRAY['owned_sequence_without_column_default'];
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
            'external_sequence_columns', to_jsonb(v_external_sequence_cols),
            'owned_sequence_without_default_columns',
                to_jsonb(v_owned_sequence_without_default_cols),
            'custom_storage_columns', to_jsonb(v_custom_storage_cols),
            'custom_sequence_metadata_columns', to_jsonb(v_custom_sequence_cols),
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

-- One authority for the "live catalog still matches the latest captured
-- schema epoch" invariant. Related-object DDL is not always represented by
-- an ALTER TABLE ProcessUtility node, so both destructive DDL and a live-table
-- restore call this guard before they can consume a stale schema contract.
CREATE OR REPLACE FUNCTION flashback_require_current_schema_contract(
    p_tracking_id bigint,
    p_current_schema_def jsonb DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_version bigint;
    v_generation_id bigint;
    v_expected jsonb;
    v_current jsonb;
BEGIN
    SELECT tt.rel_oid, tt.schema_version, cg.generation_id
      INTO v_rel_oid, v_schema_version, v_generation_id
    FROM flashback.tracked_tables tt
    JOIN flashback.coverage_generations cg
      ON cg.tracking_id = tt.tracking_id
     AND cg.state = 'active'
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
    LIMIT 1;

    IF v_rel_oid IS NULL OR v_generation_id IS NULL THEN
        RAISE EXCEPTION
            'pg_flashback: current schema contract cannot be proven for inactive lifecycle %',
            p_tracking_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT sv.schema_def
      INTO v_expected
    FROM flashback.schema_versions sv
    WHERE sv.tracking_id = p_tracking_id
      AND sv.generation_id = v_generation_id
      AND sv.schema_version = COALESCE(v_schema_version, 1)
    ORDER BY sv.commit_lsn DESC NULLS LAST,
             sv.applied_lsn DESC,
             sv.applied_at DESC
    LIMIT 1;

    IF v_expected IS NULL THEN
        RAISE EXCEPTION
            'pg_flashback: current protected schema epoch has no complete schema contract'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run pg_flashback maintain/reanchor for this table before destructive DDL or recovery.';
    END IF;

    v_current := COALESCE(
        p_current_schema_def,
        public.flashback_collect_schema_def(v_rel_oid)
    );
    IF v_current IS NULL OR v_current IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'pg_flashback: live schema metadata changed outside the captured table-DDL epoch'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run pg_flashback maintain/reanchor after index, trigger, policy, ACL/comment, or owned-sequence DDL.';
    END IF;
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

-- =================================================================
-- Correctness-qualified COMMIT-LSN restore/query/recovery APIs.
-- Local capacity/write-stall admission lives in local_capacity.sql and is
-- invoked through flashback_local_restore_preflight() before ACCESS EXCLUSIVE.
-- =================================================================

-- Prove that no committed logical change for the relations being swapped is
-- still waiting in the slot. Slot advancement is transactional: repeatedly
-- calling get_changes() inside the restore transaction can return the same
-- prefix until that outer transaction commits. The restore path must therefore
-- never attempt to drain here. It takes ACCESS EXCLUSIVE first, fixes one WAL
-- barrier, and either proves the bounded prefix empty or aborts without
-- changing the relation so the normal worker can catch up in another
-- transaction and the caller can retry.
CREATE OR REPLACE FUNCTION flashback_assert_relation_wal_drained(
    p_rel_oids oid[],
    p_max_scan_bytes bigint DEFAULT 16777216
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_oid_list text;
    v_slot_name text;
    v_confirmed_flush_lsn pg_lsn;
    v_barrier_lsn pg_lsn;
    v_scan_bytes numeric;
    v_has_output boolean;
BEGIN
    IF p_rel_oids IS NULL OR array_length(p_rel_oids, 1) IS NULL THEN
        RAISE EXCEPTION 'flashback_assert_relation_wal_drained: relation OID list is empty';
    END IF;
    IF p_max_scan_bytes < 1 THEN
        RAISE EXCEPTION 'flashback_assert_relation_wal_drained: max scan bytes must be positive';
    END IF;

    SELECT string_agg(DISTINCT rel_oid::text, ',' ORDER BY rel_oid::text)
      INTO v_oid_list
    FROM unnest(p_rel_oids) AS ids(rel_oid)
    WHERE rel_oid IS NOT NULL;
    IF v_oid_list IS NULL THEN
        RAISE EXCEPTION 'flashback_assert_relation_wal_drained: relation OID list contains no usable OID';
    END IF;

    v_slot_name := flashback_effective_slot_name();
    SELECT confirmed_flush_lsn
      INTO v_confirmed_flush_lsn
    FROM pg_replication_slots
    WHERE slot_name = v_slot_name
      AND database = current_database();
    IF v_confirmed_flush_lsn IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: logical slot % is unavailable during restore', v_slot_name
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- ACCESS EXCLUSIVE was acquired by the caller before this point, so every
    -- writer that could have touched these OIDs has committed or aborted. This
    -- insert position is therefore a stable upper bound for relation changes.
    v_barrier_lsn := pg_current_wal_insert_lsn();
    v_scan_bytes := GREATEST(
        pg_wal_lsn_diff(v_barrier_lsn, v_confirmed_flush_lsn),
        0
    );
    IF v_scan_bytes > p_max_scan_bytes THEN
        RAISE EXCEPTION 'pg_flashback: logical slot is % bytes behind the locked restore barrier',
            v_scan_bytes
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Let the WAL worker catch up, then retry the restore. No table changes were made.';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM pg_logical_slot_peek_changes(
            v_slot_name, v_barrier_lsn, 1,
            'tracked_oids', v_oid_list
        ) AS ch(lsn, xid, data)
        WHERE ch.data LIKE '{%'
    ) INTO v_has_output;

    IF v_has_output THEN
        RAISE EXCEPTION 'pg_flashback: committed WAL for relation OIDs % is still pending at restore barrier %',
            v_oid_list, v_barrier_lsn
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Let the WAL worker consume the backlog, then retry the restore. No table changes were made.';
    END IF;

    RETURN v_barrier_lsn;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_materialize_lsn(
    p_target_table text,
    p_target_lsn pg_lsn,
    p_destination_schema text,
    p_destination_table text,
    p_build_secondary_indexes boolean DEFAULT false
)
RETURNS TABLE (
    tracking_id bigint,
    source_generation_id bigint,
    stream_id bigint,
    source_rel_oid oid,
    source_schema_name text,
    source_table_name text,
    target_schema_def jsonb,
    skipped_defaults jsonb,
    materialized_rel_oid oid,
    events_applied bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    admission record;
    rec record;
    v_schema_def jsonb;
    v_has_full_schema_def boolean := false;
    v_skipped_defaults jsonb;
    v_dest_oid oid;
    v_col_list text;
    v_pred text;
    v_cols text;
    v_vals text;
    v_set_clause text;
    v_pk_pred text;
    v_identity_override text := '';
    v_applied bigint := 0;
    v_col_meta jsonb;
    v_pk_cols text[];
BEGIN
    IF p_destination_schema NOT IN ('flashback', 'pg_temp') THEN
        RAISE EXCEPTION 'flashback_materialize_lsn: destination schema % is not allowed',
            p_destination_schema;
    END IF;
    IF p_destination_table IS NULL OR p_destination_table !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'flashback_materialize_lsn: unsafe destination table name';
    END IF;

    SELECT * INTO STRICT admission
    FROM flashback_admit_lsn_target(p_target_table, p_target_lsn);

    SELECT COALESCE(
               sv.schema_def,
               jsonb_build_object(
                   'schema', admission.schema_name,
                   'table', admission.table_name,
                   'columns', COALESCE(sv.columns, '[]'::jsonb),
                   'primary_key', COALESCE(sv.primary_key, '[]'::jsonb),
                   'constraints', COALESCE(sv.constraints -> 'check_unique_fk', '[]'::jsonb),
                   'indexes', COALESCE(sv.constraints -> 'indexes', '[]'::jsonb),
                   'partition_by', sv.constraints -> 'partition_by',
                   'partitions', sv.constraints -> 'partitions',
                   'triggers', COALESCE(sv.constraints -> 'triggers', '[]'::jsonb),
                   'rls_policies', COALESCE(sv.constraints -> 'rls_policies', '[]'::jsonb),
                   'rls_enabled', COALESCE((sv.constraints -> 'rls_enabled')::boolean, false),
                   'force_rls', COALESCE((sv.constraints -> 'force_rls')::boolean, false)
               )
           ),
           sv.schema_def IS NOT NULL
      INTO v_schema_def, v_has_full_schema_def
    FROM flashback.schema_versions sv
    WHERE sv.tracking_id = admission.tracking_id
      AND sv.generation_id = admission.generation_id
      AND sv.commit_lsn IS NOT NULL
      AND sv.commit_lsn <= p_target_lsn
    ORDER BY sv.commit_lsn DESC, sv.schema_version DESC
    LIMIT 1;

    IF v_schema_def IS NULL THEN
        SELECT snap.schema_def INTO v_schema_def
        FROM flashback.snapshots snap
        WHERE snap.snapshot_id = admission.boundary_snapshot_id
          AND snap.tracking_id = admission.tracking_id;
    ELSIF NOT v_has_full_schema_def THEN
        -- schema_versions stores structural pieces only; ownership/ACL/comment
        -- metadata for historical pre-schema_def rows lives on the boundary
        -- snapshot. New rows use the complete canonical schema_def above.
        SELECT v_schema_def || jsonb_strip_nulls(jsonb_build_object(
                   'owner', snap.schema_def->>'owner',
                   'acl', snap.schema_def->'acl',
                   'comments', snap.schema_def->'comments',
                   'force_rls', snap.schema_def->'force_rls'
               ))
          INTO v_schema_def
        FROM flashback.snapshots snap
        WHERE snap.snapshot_id = admission.boundary_snapshot_id
          AND snap.tracking_id = admission.tracking_id;
    END IF;
    IF v_schema_def IS NULL OR jsonb_array_length(COALESCE(v_schema_def->'columns', '[]'::jsonb)) = 0 THEN
        RAISE EXCEPTION 'pg_flashback: generation % has no usable boundary schema',
            admission.generation_id;
    END IF;

    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                   p_destination_schema, p_destination_table);
    v_skipped_defaults := flashback_recreate_table_from_ddl(
        v_schema_def, p_destination_schema, p_destination_table
    );
    v_dest_oid := to_regclass(format('%I.%I', p_destination_schema, p_destination_table));
    IF v_dest_oid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: failed to create materialization table %.%',
            p_destination_schema, p_destination_table;
    END IF;

    SELECT string_agg(format('%I', a.attname), ', ' ORDER BY a.attnum)
      INTO v_col_list
    FROM pg_attribute a
    WHERE a.attrelid = v_dest_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = '';

    IF v_col_list IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: materialized schema has no insertable columns';
    END IF;

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
    WHERE a.attrelid = v_dest_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = '';

    SELECT ARRAY(
        SELECT jsonb_array_elements_text(COALESCE(v_schema_def->'primary_key', '[]'::jsonb))
    ) INTO v_pk_cols;

    SELECT CASE WHEN EXISTS (
               SELECT 1 FROM pg_attribute
               WHERE attrelid = v_dest_oid
                 AND attnum > 0
                 AND NOT attisdropped
                 AND attidentity <> ''
           ) THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END
      INTO v_identity_override;

    PERFORM flashback_internal_snapshot_materialize(
        admission.boundary_snapshot_id, admission.tracking_id,
        p_destination_schema, p_destination_table,
        v_col_list, v_identity_override
    );

    PERFORM flashback_apply_deferred_pk(
        p_destination_schema, p_destination_table, v_schema_def
    );

    FOR rec IN
        SELECT d.event_id, d.event_type, d.old_data, d.new_data
        FROM flashback.delta_log d
        WHERE d.tracking_id = admission.tracking_id
          AND d.generation_id = admission.generation_id
          AND d.stream_id = admission.stream_id
          AND d.commit_lsn > admission.boundary_lsn
          AND d.commit_lsn <= p_target_lsn
        ORDER BY d.commit_lsn, d.event_id
    LOOP
        IF rec.event_type = 'ALTER' THEN
            -- The destination was created directly from the target schema.
            CONTINUE;
        ELSIF rec.event_type IN ('DROP', 'TRUNCATE') THEN
            EXECUTE format('TRUNCATE TABLE %I.%I', p_destination_schema, p_destination_table);
        ELSIF rec.event_type = 'INSERT' THEN
            SELECT col_list, val_list INTO v_cols, v_vals
            FROM flashback_build_insert_parts(v_col_meta, rec.new_data);
            IF v_cols IS NOT NULL AND v_cols <> '' THEN
                EXECUTE format('INSERT INTO %I.%I (%s)%s VALUES (%s)',
                               p_destination_schema, p_destination_table,
                               v_cols, v_identity_override, v_vals);
            END IF;
        ELSIF rec.event_type = 'DELETE' THEN
            -- The deferred PK is already present on the materialization table.
            -- Prefer its unique, index-usable identity; only genuinely PK-less
            -- tables use the full-row/ctid correctness fallback.
            v_pred := flashback_build_pk_predicate(
                v_col_meta, rec.old_data, v_pk_cols
            );
            IF v_pred IS NOT NULL AND v_pred <> '' THEN
                EXECUTE format(
                    'DELETE FROM %I.%I WHERE %s',
                    p_destination_schema, p_destination_table, v_pred
                );
            ELSE
                v_pred := flashback_build_predicate(v_col_meta, rec.old_data);
                IF v_pred IS NOT NULL AND v_pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN '
                        '(SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        p_destination_schema, p_destination_table,
                        p_destination_schema, p_destination_table, v_pred
                    );
                END IF;
            END IF;
        ELSIF rec.event_type = 'UPDATE' THEN
            SELECT us.set_clause, us.pk_predicate
              INTO v_set_clause, v_pk_pred
            FROM flashback_build_update_set(v_col_meta, rec.new_data, v_pk_cols) us;
            IF v_set_clause IS NOT NULL AND v_set_clause <> ''
               AND v_pk_pred IS NOT NULL AND v_pk_pred <> ''
            THEN
                EXECUTE format('UPDATE %I.%I SET %s WHERE %s',
                               p_destination_schema, p_destination_table,
                               v_set_clause, v_pk_pred);
            ELSE
                v_pred := flashback_build_predicate(v_col_meta, rec.old_data);
                IF v_pred IS NOT NULL AND v_pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN '
                        '(SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        p_destination_schema, p_destination_table,
                        p_destination_schema, p_destination_table, v_pred
                    );
                END IF;
                SELECT col_list, val_list INTO v_cols, v_vals
                FROM flashback_build_insert_parts(v_col_meta, rec.new_data);
                IF v_cols IS NOT NULL AND v_cols <> '' THEN
                    EXECUTE format('INSERT INTO %I.%I (%s)%s VALUES (%s)',
                                   p_destination_schema, p_destination_table,
                                   v_cols, v_identity_override, v_vals);
                END IF;
            END IF;
        END IF;
        v_applied := v_applied + 1;
    END LOOP;

    IF p_build_secondary_indexes THEN
        PERFORM flashback_apply_deferred_indexes(
            admission.schema_name, admission.table_name,
            p_destination_schema, p_destination_table,
            v_schema_def
        );
    END IF;

    tracking_id := admission.tracking_id;
    source_generation_id := admission.generation_id;
    stream_id := admission.stream_id;
    source_rel_oid := admission.rel_oid;
    source_schema_name := admission.schema_name;
    source_table_name := admission.table_name;
    target_schema_def := v_schema_def;
    skipped_defaults := v_skipped_defaults;
    materialized_rel_oid := to_regclass(format('%I.%I', p_destination_schema, p_destination_table));
    events_applied := v_applied;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_query_lsn(
    p_target_table text,
    p_target_lsn pg_lsn,
    p_filter_clause text DEFAULT NULL
)
RETURNS SETOF record
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tmp text := format('_fb_query_lsn_%s_%s', pg_backend_pid(),
                         floor(random() * 1000000)::integer);
    v_query text;
BEGIN
    -- A free-form predicate cannot be safely sanitized while this API runs as
    -- SECURITY DEFINER.  Keep the compatibility argument but fail closed;
    -- callers can apply WHERE to the returned recordset in their own query,
    -- where it executes with caller privileges.
    IF p_filter_clause IS NOT NULL THEN
        RAISE EXCEPTION 'flashback_query_lsn: filter_clause is not supported in the correctness-qualified API'
            USING HINT = 'Pass NULL, declare the returned record columns, and apply WHERE in the outer SELECT.';
    END IF;

    PERFORM 1 FROM flashback_materialize_lsn(
        p_target_table, p_target_lsn, 'pg_temp', v_tmp, false
    );

    v_query := format('SELECT * FROM pg_temp.%I', v_tmp);

    RETURN QUERY EXECUTE v_query;
    -- RETURN QUERY has already copied the rows into the function's tuplestore.
    -- Drop the materialization immediately so a second query in the same
    -- backend can preserve the original named PK/index identities without
    -- colliding with objects left behind by the first call.
    EXECUTE format('DROP TABLE pg_temp.%I', v_tmp);
END;
$$;

CREATE OR REPLACE FUNCTION flashback_recover_deleted_lsn(
    p_target_table text,
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    admission record;
    v_tmp text := format('_fb_recover_lsn_%s_%s', pg_backend_pid(),
                         floor(random() * 1000000)::integer);
    v_pk_condition text;
    v_col_list text;
    v_recovered bigint;
BEGIN
    SELECT * INTO STRICT admission
    FROM flashback_admit_lsn_target(p_target_table, p_target_lsn);

    PERFORM 1 FROM flashback_materialize_lsn(
        p_target_table, p_target_lsn, 'pg_temp', v_tmp, false
    );

    SELECT string_agg(format('live.%I = hist.%I', a.attname, a.attname),
                      ' AND ' ORDER BY k.ord)
      INTO v_pk_condition
    FROM pg_index i
    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
    WHERE i.indrelid = admission.rel_oid
      AND i.indisprimary;

    IF v_pk_condition IS NULL THEN
        RAISE EXCEPTION 'flashback_recover_deleted_lsn: table % has no primary key',
            p_target_table;
    END IF;

    SELECT string_agg(format('%I', live.attname), ', ' ORDER BY live.attnum)
      INTO v_col_list
    FROM pg_attribute live
    JOIN pg_attribute hist
      ON hist.attrelid = to_regclass(format('pg_temp.%I', v_tmp))
     AND hist.attname = live.attname
     AND hist.attnum > 0
     AND NOT hist.attisdropped
    WHERE live.attrelid = admission.rel_oid
      AND live.attnum > 0
      AND NOT live.attisdropped
      AND live.attgenerated = '';

    EXECUTE format(
        'INSERT INTO %I.%I (%s) '
        'SELECT %s FROM pg_temp.%I hist '
        'WHERE NOT EXISTS ('
        '  SELECT 1 FROM %I.%I live WHERE %s'
        ')',
        admission.schema_name, admission.table_name, v_col_list,
        v_col_list, v_tmp,
        admission.schema_name, admission.table_name, v_pk_condition
    );
    GET DIAGNOSTICS v_recovered = ROW_COUNT;
    EXECUTE format('DROP TABLE pg_temp.%I', v_tmp);
    RETURN v_recovered;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_restore_lsn_lock_phase(
    p_target_table text,
    p_target_lsn pg_lsn
)
RETURNS TABLE (
    out_tracking_id bigint,
    out_rel_oid oid,
    out_schema_name text,
    out_table_name text,
    out_disaster_event_id bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    admission record;
    v_boundary_snap record;
    materialized record;
    def_rec record;
    v_shadow_name text;
    v_new_rel_oid oid;
    v_old_rel_oid oid;
    v_live_oid oid;
    v_capacity_rel oid;
    v_snapshot_capacity_checked boolean := false;
    v_current_generation_id bigint;
    v_new_stream_id bigint;
    v_new_snapshot_id bigint;
    v_new_snapshot_table text;
    v_new_generation_id bigint;
    v_generation_no bigint;
    v_schema_version bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_row_count bigint;
    v_seq_name text;
    v_seq_schema text;
    v_seq_bare text;
    v_identity_edge bigint;
    v_identity_start bigint;
    v_identity_increment bigint;
    v_cur_schema text;
    v_cur_name text;
    v_want_schema text;
    v_want_name text;
    v_restored_rel regclass;
    v_expected_proof jsonb;
    v_restore_verification jsonb;
    v_disaster_event_id bigint := NULL;
    v_disaster_commit_lsn pg_lsn := NULL;
    v_source_xid bigint := NULL;
    v_disaster_event_type text := NULL;
    v_audited_op bigint;
    v_audited_command text;
    v_audited_tracking_id bigint;
    v_audited_target_lsn pg_lsn;
    v_audited_state text;
BEGIN
    SELECT * INTO STRICT admission
    FROM flashback_admit_lsn_target(p_target_table, p_target_lsn);
    PERFORM flashback_internal_lock_lifecycle(admission.tracking_id);
    SELECT * INTO STRICT admission
    FROM flashback_admit_lsn_target(p_target_table, p_target_lsn);

    v_live_oid := to_regclass(format('%I.%I', admission.schema_name, admission.table_name));
    IF v_live_oid IS NOT NULL AND v_live_oid IS DISTINCT FROM admission.rel_oid THEN
        RAISE EXCEPTION 'pg_flashback: refusing restore of %.% because a different relation already uses that name (live oid %, tracked oid %)',
            admission.schema_name, admission.table_name, v_live_oid, admission.rel_oid
            USING HINT = 'Rename or move the newer table before recover; pg_flashback will not silently overwrite an identity-mismatched relation.';
    END IF;
    -- Exact event-bound DROP dependency manifests are mandatory for DROP
    -- reconstruction (live relation gone) and for audited recover selections
    -- whose disaster is a DROP. Pure DML/schema point-in-time restore against
    -- a still-live relation must not invent or require a DROP identity.
    --
    -- The audited operation_id comes from a backend-local Rust execution
    -- context (flashback_internal_get_audited_recover_context), never from a
    -- user-settable GUC: only flashback_recover_execute's owner-run body may
    -- set it. Even so, an operation_id that does not actually describe THIS
    -- restore (wrong command/state/tracking_id/target_lsn) must never be
    -- trusted for disaster_event_id binding -- fail closed rather than
    -- silently falling back to "unaudited" or borrowing an unrelated
    -- operation's disaster identity.
    v_audited_op := flashback_internal_get_audited_recover_context();
    IF v_audited_op IS NOT NULL THEN
        SELECT o.disaster_event_id, o.command, o.tracking_id, o.target_lsn, s.state
          INTO v_disaster_event_id, v_audited_command, v_audited_tracking_id,
               v_audited_target_lsn, v_audited_state
        FROM flashback.operations o
        JOIN flashback.operation_current_state s ON s.operation_id = o.operation_id
        WHERE o.operation_id = v_audited_op;

        IF v_audited_command IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: audited recover context refers to unknown operation %', v_audited_op
                USING ERRCODE = 'serialization_failure';
        END IF;
        IF v_audited_command IS DISTINCT FROM 'recover'
           OR v_audited_state IS DISTINCT FROM 'started'
           OR v_audited_tracking_id IS DISTINCT FROM admission.tracking_id
           OR v_audited_target_lsn IS DISTINCT FROM p_target_lsn
        THEN
            RAISE EXCEPTION 'pg_flashback: audited recover context (operation %, command=%, state=%, tracking_id=%, target_lsn=%) does not match this restore (table %, tracking_id=%, target_lsn=%)',
                v_audited_op, v_audited_command, v_audited_state, v_audited_tracking_id, v_audited_target_lsn,
                p_target_table, admission.tracking_id, p_target_lsn
                USING ERRCODE = 'serialization_failure';
        END IF;
    END IF;

    IF v_live_oid IS NULL THEN
        IF v_disaster_event_id IS NULL THEN
            SELECT dl.event_id, dl.commit_lsn, dl.source_xid
              INTO v_disaster_event_id, v_disaster_commit_lsn, v_source_xid
            FROM flashback.delta_log dl
            WHERE dl.tracking_id = admission.tracking_id
              AND dl.event_type = 'DROP'
              AND dl.commit_lsn IS NOT NULL
              AND dl.commit_lsn > p_target_lsn
            ORDER BY dl.commit_lsn ASC, dl.event_id ASC
            LIMIT 1;
        ELSE
            -- Audited disaster_event_id: still an untrusted foreign key into
            -- delta_log until its own tracking_id and event_type are checked
            -- here -- an operation row can never borrow another lifecycle's
            -- DROP identity just because tracking_id/target_lsn happened to
            -- match above.
            SELECT dl.commit_lsn, dl.source_xid
              INTO v_disaster_commit_lsn, v_source_xid
            FROM flashback.delta_log dl
            WHERE dl.event_id = v_disaster_event_id
              AND dl.tracking_id = admission.tracking_id
              AND dl.event_type = 'DROP'
            LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'pg_flashback: audited disaster_event_id % is not a DROP event for tracking_id %',
                    v_disaster_event_id, admission.tracking_id
                    USING ERRCODE = 'serialization_failure';
            END IF;
        END IF;

        IF v_disaster_event_id IS NULL THEN
            RAISE EXCEPTION
                'pg_flashback: restore refused: cannot resolve exact DROP identity for dependency manifest (tracking_id %, target_lsn %)',
                admission.tracking_id, p_target_lsn
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = 'Use flashback_recover_begin/execute so the selected disaster_event_id is audited, or ensure the DROP is captured in delta_log.';
        END IF;

        PERFORM flashback_bind_drop_dependency_manifests();
        PERFORM flashback_require_supported_drop_manifest(
            admission.tracking_id,
            v_disaster_event_id
        );
    ELSIF v_disaster_event_id IS NOT NULL THEN
        -- Same untrusted-foreign-key concern as the DROP-reconstruct branch
        -- above: an audited disaster_event_id must belong to this admitted
        -- tracking_id, never a different lifecycle's event.
        SELECT dl.commit_lsn, dl.source_xid, dl.event_type
          INTO v_disaster_commit_lsn, v_source_xid, v_disaster_event_type
        FROM flashback.delta_log dl
        WHERE dl.event_id = v_disaster_event_id
          AND dl.tracking_id = admission.tracking_id
        LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pg_flashback: audited disaster_event_id % does not belong to tracking_id %',
                v_disaster_event_id, admission.tracking_id
                USING ERRCODE = 'serialization_failure';
        END IF;

        IF v_disaster_event_type = 'DROP' THEN
            PERFORM flashback_bind_drop_dependency_manifests();
            PERFORM flashback_require_supported_drop_manifest(
                admission.tracking_id,
                v_disaster_event_id
            );
        END IF;
    END IF;
    IF v_live_oid IS NOT NULL THEN
        v_capacity_rel := v_live_oid;
    ELSE
        -- DROP TABLE reconstruct: live heap is gone. Budget restore peak from
        -- the admitted boundary snapshot payload (heap+TOAST proxy).
        SELECT * INTO v_boundary_snap
        FROM flashback_internal_snapshot_resolve(admission.boundary_snapshot_id, admission.tracking_id);
        v_capacity_rel := v_boundary_snap.payload_relid;
        IF v_capacity_rel IS NULL
           AND v_boundary_snap.storage_backend = 'external_zstd'
        THEN
            PERFORM flashback_local_restore_preflight_snapshot(
                admission.boundary_snapshot_id, admission.tracking_id
            );
            v_snapshot_capacity_checked := true;
        ELSIF v_capacity_rel IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: cannot admit restore capacity for dropped table % without boundary snapshot %',
                p_target_table, admission.snapshot_table;
        END IF;
    END IF;

    -- Capacity preflight before the final-strength relation lock (or before
    -- reconstruct when the live relation is already absent).
    IF NOT v_snapshot_capacity_checked THEN
        PERFORM flashback_local_restore_preflight(v_capacity_rel);
    END IF;
    PERFORM flashback_apply_local_boundary_lock_timeout();

    IF v_live_oid IS NOT NULL THEN
        -- Freeze the old physical relation, then prove its bounded logical prefix
        -- empty without trying to advance the slot in this transaction.
        BEGIN
            EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
                           admission.schema_name, admission.table_name);
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'pg_flashback: local restore lock wait exceeded local_boundary_write_stall_ms'
                USING ERRCODE = 'lock_not_available',
                      HINT = 'Retry when the table is idle or raise the write-stall budget.';
        END;

        -- Re-resolve identity and compare the live metadata only after the
        -- final-strength relation lock is held. A concurrent rename/drop and
        -- replacement or related-object DDL must not fit between the proof
        -- and the eventual swap.
        v_live_oid := to_regclass(
            format('%I.%I', admission.schema_name, admission.table_name)
        );
        IF v_live_oid IS NULL OR v_live_oid IS DISTINCT FROM admission.rel_oid THEN
            RAISE EXCEPTION
                'pg_flashback: refusing restore of %.% because relation identity changed while acquiring the restore lock (live oid %, tracked oid %)',
                admission.schema_name, admission.table_name, v_live_oid, admission.rel_oid
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Retry after resolving the concurrent DDL; pg_flashback will not restore across an identity race.';
        END IF;
        -- A live-table PITR must not swap in a schema derived from a stale
        -- epoch after related-object DDL that the table-node hook could not
        -- map. DROP/TRUNCATE run the same authority while holding the same
        -- ACCESS EXCLUSIVE strength.
        PERFORM public.flashback_require_current_schema_contract(
            admission.tracking_id, NULL
        );
    END IF;

    out_tracking_id := admission.tracking_id;
    out_rel_oid := admission.rel_oid;
    out_schema_name := admission.schema_name;
    out_table_name := admission.table_name;
    out_disaster_event_id := v_disaster_event_id;
    RETURN NEXT;
    RETURN;
END;
$$;

REVOKE ALL ON FUNCTION flashback_restore_lsn_lock_phase(text, pg_lsn) FROM PUBLIC;

DROP FUNCTION IF EXISTS flashback_internal_restore_lsn_core(text, pg_lsn, bigint);

CREATE OR REPLACE FUNCTION flashback_internal_restore_lsn_core(
    p_target_table text,
    p_target_lsn pg_lsn,
    p_stream_id bigint,
    p_disaster_event_id bigint DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    admission record;
    v_boundary_snap record;
    materialized record;
    def_rec record;
    v_shadow_name text;
    v_new_rel_oid oid;
    v_old_rel_oid oid;
    v_live_oid oid;
    v_capacity_rel oid;
    v_snapshot_capacity_checked boolean := false;
    v_current_generation_id bigint;
    v_new_stream_id bigint;
    v_new_snapshot_id bigint;
    v_new_snapshot_table text;
    v_new_generation_id bigint;
    v_generation_no bigint;
    v_schema_version bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_seq_name text;
    v_seq_schema text;
    v_seq_bare text;
    v_identity_edge bigint;
    v_identity_start bigint;
    v_identity_increment bigint;
    v_cur_schema text;
    v_cur_name text;
    v_want_schema text;
    v_want_name text;
    v_table_owner text;
    v_restored_rel regclass;
    v_expected_proof jsonb;
    v_restore_verification jsonb;
    v_audited_op bigint;
    v_audited_command text;
    v_audited_tracking_id bigint;
    v_audited_target_lsn pg_lsn;
    v_audited_disaster_event_id bigint;
    v_audited_state text;
BEGIN
    PERFORM public.flashback_set_restore_in_progress(true);

    IF p_stream_id IS NULL THEN
        RAISE EXCEPTION 'flashback_internal_restore_lsn_core: stream_id is required';
    END IF;
    v_new_stream_id := p_stream_id;

    SELECT * INTO STRICT admission
    FROM flashback_admit_lsn_target(p_target_table, p_target_lsn);

    v_live_oid := to_regclass(format('%I.%I', admission.schema_name, admission.table_name));
    IF v_live_oid IS NOT NULL THEN
        v_capacity_rel := v_live_oid;
    ELSE
        SELECT * INTO v_boundary_snap
        FROM flashback_internal_snapshot_resolve(admission.boundary_snapshot_id, admission.tracking_id);
        v_capacity_rel := v_boundary_snap.payload_relid;
        IF v_capacity_rel IS NULL
           AND v_boundary_snap.storage_backend = 'external_zstd'
        THEN
            PERFORM flashback_local_restore_preflight_snapshot(
                admission.boundary_snapshot_id, admission.tracking_id
            );
            v_snapshot_capacity_checked := true;
        ELSIF v_capacity_rel IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: cannot revalidate restore capacity for dropped table % without boundary snapshot %',
                p_target_table, admission.snapshot_table;
        END IF;
    END IF;
    -- Revalidate capacity before materialization.
    IF NOT v_snapshot_capacity_checked THEN
        PERFORM flashback_local_restore_preflight(v_capacity_rel);
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = admission.tracking_id
          AND cg.state IN ('building', 'capturing')
    ) THEN
        RAISE EXCEPTION 'pg_flashback: tracking lifecycle % already has a pending generation',
            admission.tracking_id;
    END IF;

    SELECT generation_id INTO v_current_generation_id
    FROM flashback.coverage_generations
    WHERE tracking_id = admission.tracking_id
      AND state = 'active'
    FOR UPDATE;
    IF v_current_generation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no active current generation exists for restore finalization';
    END IF;

    v_old_rel_oid := admission.rel_oid;
    v_shadow_name := format('__fb_lsn_shadow_%s', admission.tracking_id);

    -- Deterministic test barrier (SUSET GUC; empty/off in release defaults).
    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'before_materialize' THEN
        RAISE EXCEPTION 'pg_flashback: test_restore_failpoint=before_materialize'
            USING ERRCODE = 'query_canceled';
    END IF;

    SELECT * INTO STRICT materialized
    FROM flashback_materialize_lsn(
        p_target_table, p_target_lsn, 'flashback', v_shadow_name, true
    );

    -- Independent expected proof from pre-swap shadow + target schema_def.
    -- Manifest selection uses the lock-phase disaster identity only — never
    -- "latest for tracking_id".
    v_expected_proof := flashback_build_expected_restore_proof(
        to_regclass(format('flashback.%I', v_shadow_name)),
        materialized.target_schema_def,
        COALESCE((
            SELECT to_jsonb(m) - 'raw_ddl'
            FROM flashback.drop_dependency_manifests m
            WHERE m.tracking_id = admission.tracking_id
              AND m.disaster_event_id IS NOT DISTINCT FROM p_disaster_event_id
            LIMIT 1
        ), '{}'::jsonb),
        jsonb_build_object(
            'tracking_id', admission.tracking_id,
            'generation_id', admission.generation_id,
            'stream_id', admission.stream_id,
            'boundary_snapshot_id', admission.boundary_snapshot_id,
            'boundary_lsn', admission.boundary_lsn,
            'target_lsn', p_target_lsn,
            'disaster_event_id', p_disaster_event_id
        )
    );

    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_expected_proof_before_swap' THEN
        RAISE EXCEPTION 'pg_flashback: test_restore_failpoint=after_expected_proof_before_swap'
            USING ERRCODE = 'query_canceled';
    END IF;

    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_materialize_before_swap' THEN
        RAISE EXCEPTION 'pg_flashback: test_restore_failpoint=after_materialize_before_swap'
            USING ERRCODE = 'query_canceled';
    END IF;

    v_new_rel_oid := flashback_finalize_shadow_swap(
        materialized.source_schema_name,
        materialized.source_table_name,
        'flashback', v_shadow_name,
        materialized.target_schema_def
    );
    IF v_new_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore_lsn: shadow swap failed for %', p_target_table;
    END IF;

    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_before_commit' THEN
        RAISE EXCEPTION 'pg_flashback: test_restore_failpoint=after_swap_before_commit'
            USING ERRCODE = 'query_canceled';
    END IF;

    IF v_new_rel_oid <> v_old_rel_oid THEN
        UPDATE flashback.tracked_tables
           SET rel_oid = v_new_rel_oid
         WHERE tracking_id = admission.tracking_id;
    END IF;

    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL',
                   materialized.source_schema_name, materialized.source_table_name);
    SELECT pg_get_userbyid(c.relowner)
      INTO STRICT v_table_owner
    FROM pg_class c
    WHERE c.oid = v_new_rel_oid;

    FOR def_rec IN
        SELECT elem->>'col' AS col_name,
               elem->>'default_expr' AS default_expr,
               elem->'sequence_options' AS sequence_options
        FROM jsonb_array_elements(COALESCE(materialized.skipped_defaults, '[]'::jsonb)) elem
    LOOP
        IF def_rec.sequence_options IS NOT NULL
           AND def_rec.sequence_options <> 'null'::jsonb
        THEN
            v_seq_schema := def_rec.sequence_options->>'sequence_schema';
            v_seq_bare := def_rec.sequence_options->>'sequence_name';
            IF v_seq_schema IS NULL OR v_seq_bare IS NULL THEN
                RAISE EXCEPTION 'pg_flashback: owned sequence identity missing for column %',
                    def_rec.col_name;
            END IF;
            IF to_regclass(format('%I.%I', v_seq_schema, v_seq_bare)) IS NOT NULL THEN
                RAISE EXCEPTION
                    'pg_flashback: cannot restore owned sequence %.%; name is occupied',
                    v_seq_schema, v_seq_bare
                    USING ERRCODE = 'duplicate_table';
            END IF;
            EXECUTE format(
                'CREATE SEQUENCE %I.%I AS %s START WITH %s INCREMENT BY %s '
                'MINVALUE %s MAXVALUE %s CACHE %s %s',
                v_seq_schema,
                v_seq_bare,
                COALESCE(def_rec.sequence_options->>'data_type', 'bigint'),
                def_rec.sequence_options->>'start',
                def_rec.sequence_options->>'increment',
                def_rec.sequence_options->>'min',
                def_rec.sequence_options->>'max',
                def_rec.sequence_options->>'cache',
                CASE
                    WHEN COALESCE((def_rec.sequence_options->>'cycle')::boolean, false)
                    THEN 'CYCLE'
                    ELSE 'NO CYCLE'
                END
            );
            v_identity_increment := COALESCE(
                (def_rec.sequence_options->>'increment')::bigint, 1
            );
            v_identity_start := COALESCE(
                (def_rec.sequence_options->>'start')::bigint, 1
            );
            IF v_identity_increment < 0 THEN
                EXECUTE format('SELECT min(%I) FROM %I.%I',
                               def_rec.col_name,
                               materialized.source_schema_name,
                               materialized.source_table_name)
                  INTO v_identity_edge;
            ELSE
                EXECUTE format('SELECT max(%I) FROM %I.%I',
                               def_rec.col_name,
                               materialized.source_schema_name,
                               materialized.source_table_name)
                  INTO v_identity_edge;
            END IF;
            IF v_identity_edge IS NULL THEN
                PERFORM setval(format('%I.%I', v_seq_schema, v_seq_bare)::regclass,
                               v_identity_start, false);
            ELSE
                PERFORM setval(format('%I.%I', v_seq_schema, v_seq_bare)::regclass,
                               v_identity_edge, true);
            END IF;
            -- OWNED BY requires the sequence and table to have the same
            -- owner. The sequence is created by this SECURITY DEFINER
            -- routine, while finalize has already restored the table's
            -- application owner.
            EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO %I',
                           v_seq_schema, v_seq_bare, v_table_owner);
            EXECUTE format('ALTER SEQUENCE %I.%I OWNED BY %I.%I.%I',
                           v_seq_schema, v_seq_bare,
                           materialized.source_schema_name,
                           materialized.source_table_name, def_rec.col_name);
        ELSE
            -- An unowned external sequence is not part of this table's
            -- artifact set. It must still exist; never fabricate it.
            v_seq_name := substring(def_rec.default_expr FROM $re$nextval\('([^']+)'$re$);
            v_seq_name := regexp_replace(v_seq_name, '::[a-zA-Z_ ]+$', '');
            IF v_seq_name IS NULL OR to_regclass(v_seq_name) IS NULL THEN
                RAISE EXCEPTION
                    'pg_flashback: external sequence default for column % cannot be resolved',
                    def_rec.col_name;
            END IF;
        END IF;
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
                       materialized.source_schema_name, materialized.source_table_name,
                       def_rec.col_name, def_rec.default_expr);
    END LOOP;

    -- Identity sequences are created with the shadow table and move with it,
    -- but explicit historical values do not advance their state. Position each
    -- sequence at the recovered edge so the first application INSERT cannot
    -- collide (or run backwards into an existing value for descending IDs).
    FOR def_rec IN
        SELECT elem->>'name' AS col_name,
               elem->'identity_options' AS identity_options
        FROM jsonb_array_elements(
            COALESCE(materialized.target_schema_def->'columns', '[]'::jsonb)
        ) elem
        WHERE elem->>'identity' IN ('a', 'd')
    LOOP
        v_seq_name := pg_get_serial_sequence(
            format('%I.%I', materialized.source_schema_name, materialized.source_table_name),
            def_rec.col_name
        );
        IF v_seq_name IS NULL OR to_regclass(v_seq_name) IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: restored identity sequence missing for %.%.%',
                materialized.source_schema_name,
                materialized.source_table_name,
                def_rec.col_name;
        END IF;
        v_identity_increment := COALESCE(
            (def_rec.identity_options->>'increment')::bigint, 1
        );
        v_identity_start := COALESCE(
            (def_rec.identity_options->>'start')::bigint, 1
        );
        IF v_identity_increment < 0 THEN
            EXECUTE format('SELECT min(%I) FROM %I.%I',
                           def_rec.col_name,
                           materialized.source_schema_name,
                           materialized.source_table_name)
              INTO v_identity_edge;
        ELSE
            EXECUTE format('SELECT max(%I) FROM %I.%I',
                           def_rec.col_name,
                           materialized.source_schema_name,
                           materialized.source_table_name)
              INTO v_identity_edge;
        END IF;
        IF v_identity_edge IS NULL THEN
            PERFORM setval(to_regclass(v_seq_name), v_identity_start, false);
        ELSE
            PERFORM setval(to_regclass(v_seq_name), v_identity_edge, true);
        END IF;

        -- Restore the original qualified identity sequence name when captured.
        v_want_schema := COALESCE(
            def_rec.identity_options->>'sequence_schema',
            materialized.source_schema_name
        );
        v_want_name := def_rec.identity_options->>'sequence_name';
        IF v_want_name IS NOT NULL AND v_want_name <> '' THEN
            IF to_regclass(format('%I.%I', v_want_schema, v_want_name)) IS NOT NULL
               AND to_regclass(format('%I.%I', v_want_schema, v_want_name))
                   IS DISTINCT FROM to_regclass(v_seq_name)
            THEN
                RAISE EXCEPTION
                    'pg_flashback: cannot restore identity sequence name %.%; name is occupied by an unrelated object',
                    v_want_schema, v_want_name
                    USING ERRCODE = 'duplicate_table';
            END IF;

            SELECT n.nspname, c.relname INTO v_cur_schema, v_cur_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = to_regclass(v_seq_name);

            IF v_cur_schema IS DISTINCT FROM v_want_schema THEN
                EXECUTE format(
                    'ALTER SEQUENCE %I.%I SET SCHEMA %I',
                    v_cur_schema, v_cur_name, v_want_schema
                );
                v_cur_schema := v_want_schema;
            END IF;
            IF v_cur_name IS DISTINCT FROM v_want_name THEN
                EXECUTE format(
                    'ALTER SEQUENCE %I.%I RENAME TO %I',
                    v_cur_schema, v_cur_name, v_want_name
                );
            END IF;
        END IF;
    END LOOP;

    v_restored_rel := to_regclass(format('%I.%I',
        materialized.source_schema_name, materialized.source_table_name));
    IF v_restored_rel IS NULL THEN
        RAISE EXCEPTION 'flashback_restore_lsn: restored relation missing after swap for %',
            p_target_table;
    END IF;

    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_mutate_row' THEN
        -- Deterministic post-swap corruption for adversarial verification tests.
        EXECUTE format(
            'DELETE FROM %I.%I WHERE ctid = (SELECT ctid FROM %I.%I LIMIT 1)',
            materialized.source_schema_name, materialized.source_table_name,
            materialized.source_schema_name, materialized.source_table_name
        );
    END IF;

    -- A3 adversarial coverage: corrupt catalog-level metadata (not row data)
    -- on the just-swapped-in relation, proving flashback_verify_restored_relation
    -- catches drift from the live catalog rather than tautologically
    -- recomputing the same schema_def-derived value on both sides.
    IF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_drop_index' THEN
        EXECUTE format(
            'DROP INDEX %I.%I',
            materialized.source_schema_name,
            (SELECT idx.relname
               FROM pg_index i
               JOIN pg_class idx ON idx.oid = i.indexrelid
              WHERE i.indrelid = v_restored_rel
                AND NOT i.indisprimary
                AND NOT EXISTS (
                    SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid
                )
              LIMIT 1)
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_revoke_acl' THEN
        EXECUTE format(
            'REVOKE ALL ON %I.%I FROM PUBLIC',
            materialized.source_schema_name, materialized.source_table_name
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_change_owner' THEN
        EXECUTE format(
            'ALTER TABLE %I.%I OWNER TO %I',
            materialized.source_schema_name, materialized.source_table_name,
            (SELECT r.rolname FROM pg_roles r
              WHERE r.rolname <> (SELECT pg_get_userbyid(c.relowner)
                                    FROM pg_class c WHERE c.oid = v_restored_rel)
              ORDER BY r.rolname LIMIT 1)
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_drop_pk' THEN
        -- Primary key is verified via the inventory's separate 'primary_key'
        -- column-name array, not the general 'constraints' array (which
        -- deliberately excludes contype='p' to match schema_def's own
        -- collection) -- a distinct code path from the unique/check case
        -- below, so it needs its own adversarial proof.
        EXECUTE format(
            'ALTER TABLE %I.%I DROP CONSTRAINT %I',
            materialized.source_schema_name, materialized.source_table_name,
            (SELECT con.conname FROM pg_constraint con
              WHERE con.conrelid = v_restored_rel AND con.contype = 'p'
              LIMIT 1)
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_drop_constraint' THEN
        EXECUTE format(
            'ALTER TABLE %I.%I DROP CONSTRAINT %I',
            materialized.source_schema_name, materialized.source_table_name,
            (SELECT con.conname FROM pg_constraint con
              WHERE con.conrelid = v_restored_rel AND con.contype IN ('u', 'c')
              LIMIT 1)
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_disable_rls' THEN
        EXECUTE format(
            'ALTER TABLE %I.%I DISABLE ROW LEVEL SECURITY',
            materialized.source_schema_name, materialized.source_table_name
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_disable_force_rls' THEN
        EXECUTE format(
            'ALTER TABLE %I.%I NO FORCE ROW LEVEL SECURITY',
            materialized.source_schema_name, materialized.source_table_name
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_drop_comment' THEN
        EXECUTE format(
            'COMMENT ON TABLE %I.%I IS NULL',
            materialized.source_schema_name, materialized.source_table_name
        );
    ELSIF NULLIF(current_setting('pg_flashback.test_restore_failpoint', true), '')
         = 'after_swap_change_replica_identity' THEN
        -- Restore always forces FULL; DEFAULT is a genuine, detectable drift
        -- from what flashback_build_expected_restore_proof hardcodes as the
        -- only correct post-restore value while a table is actively tracked.
        EXECUTE format(
            'ALTER TABLE %I.%I REPLICA IDENTITY DEFAULT',
            materialized.source_schema_name, materialized.source_table_name
        );
    END IF;

    v_restore_verification := flashback_verify_restored_relation(
        v_restored_rel,
        v_expected_proof,
        materialized.target_schema_def
    );

    v_boundary_xid := (txid_current() % 4294967296)::bigint;
    v_provisional_lsn := pg_current_wal_insert_lsn();

    v_new_snapshot_id := flashback_internal_snapshot_create(
        admission.tracking_id, v_new_rel_oid,
        materialized.source_schema_name, materialized.source_table_name,
        v_provisional_lsn, 'generation', materialized.target_schema_def
    );
    v_new_snapshot_table := format('snap_%s_%s',
                                   admission.tracking_id, v_new_snapshot_id);

    SELECT COALESCE(max(generation_no), 0) + 1 INTO v_generation_no
    FROM flashback.coverage_generations
    WHERE tracking_id = admission.tracking_id;

    v_new_generation_id := flashback_internal_create_coverage_generation(
        p_tracking_id => admission.tracking_id,
        p_generation_no => v_generation_no,
        p_stream_id => v_new_stream_id,
        p_boundary_kind => 'post_restore',
        p_rel_oid_at_boundary => v_new_rel_oid,
        p_boundary_snapshot_id => v_new_snapshot_id,
        p_boundary_lsn => NULL,
        p_boundary_time => NULL,
        p_boundary_xid => v_boundary_xid,
        p_boundary_marker => format('post-restore:%s:%s:%s', admission.tracking_id, v_boundary_xid, v_generation_no),
        p_parent_generation_id => v_current_generation_id,
        p_recovery_profile => 'local_delta',
        p_details => jsonb_build_object('source_generation_id', admission.generation_id, 'restored_target_lsn', p_target_lsn)
    );

    SELECT COALESCE(max(schema_version), 0) + 1 INTO v_schema_version
    FROM flashback.schema_versions
    WHERE tracking_id = admission.tracking_id;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, stream_id, source_xid,
        schema_version, applied_at, applied_lsn,
        columns, primary_key, constraints, schema_def, helper_schema_sha256
    ) VALUES (
        v_new_rel_oid, admission.tracking_id, v_new_generation_id,
        v_new_stream_id, v_boundary_xid,
        v_schema_version, clock_timestamp(), v_provisional_lsn,
        COALESCE(materialized.target_schema_def->'columns', '[]'::jsonb),
        COALESCE(materialized.target_schema_def->'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(materialized.target_schema_def->'constraints', '[]'::jsonb),
            'indexes', COALESCE(materialized.target_schema_def->'indexes', '[]'::jsonb),
            'partition_by', materialized.target_schema_def->'partition_by',
            'partitions', materialized.target_schema_def->'partitions',
            'triggers', COALESCE(materialized.target_schema_def->'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(materialized.target_schema_def->'rls_policies', '[]'::jsonb),
            'rls_enabled', COALESCE((materialized.target_schema_def->'rls_enabled')::boolean, false)
        ),
        materialized.target_schema_def,
        flashback_helper_schema_sha256(v_new_rel_oid)
    );

    UPDATE flashback.tracked_tables
       SET base_snapshot_table = format('flashback.%I', v_new_snapshot_table),
           schema_version = v_schema_version,
           is_active = true
     WHERE tracking_id = admission.tracking_id;

    -- The restore transaction swaps the user relation while capture is
    -- suppressed, so its post-restore generation also needs an explicit WAL
    -- marker.  The decoder uses this transaction's COMMIT record to seal the
    -- parent and activate the successor at one exact boundary.
    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'BOUNDARY',
            'kind', 'post_restore',
            'tracking_id', admission.tracking_id,
            'generation_id', v_new_generation_id
        )::text
    );

    RAISE NOTICE 'pg_flashback: restore applied at target LSN %; successor coverage is pending until this transaction COMMIT is consumed. Check flashback_health() before another historical operation.',
        p_target_lsn;

    -- Attach exact successor binding and proof to the durable recover
    -- operation. The audited operation_id comes from the backend-local Rust
    -- execution context, never a user-settable GUC. Re-verify the binding
    -- against the operation header one more time here (command/state/
    -- tracking_id/target_lsn/disaster_event_id) rather than trusting that the
    -- lock-phase check earlier in the same restore still holds -- fail closed
    -- rather than silently attaching this restore's proof to an operation
    -- header that does not actually describe it.
    v_audited_op := flashback_internal_get_audited_recover_context();
    IF v_audited_op IS NULL THEN
        -- The privileged raw restore escape hatch still gets an immutable
        -- journal record.  It skips pre-authorized plan-token admission, not
        -- post-restore coverage verification or audit.
        v_audited_op := public.flashback_operation_begin(
            p_command => 'restore_lsn',
            p_table => p_target_table,
            p_tracking_id => admission.tracking_id,
            p_disaster_event_id => p_disaster_event_id,
            p_generation_id => admission.generation_id,
            p_target_lsn => p_target_lsn,
            p_details => jsonb_build_object('raw_privileged_restore', true)
        );
    END IF;

    SELECT o.command, o.tracking_id, o.target_lsn, o.disaster_event_id, s.state
      INTO v_audited_command, v_audited_tracking_id, v_audited_target_lsn,
           v_audited_disaster_event_id, v_audited_state
    FROM flashback.operations o
    JOIN flashback.operation_current_state s ON s.operation_id = o.operation_id
    WHERE o.operation_id = v_audited_op;

    IF v_audited_command IS NULL
       OR v_audited_command NOT IN ('recover', 'restore_lsn')
       OR v_audited_state IS DISTINCT FROM 'started'
       OR v_audited_tracking_id IS DISTINCT FROM admission.tracking_id
       OR v_audited_target_lsn IS DISTINCT FROM p_target_lsn
       OR v_audited_disaster_event_id IS DISTINCT FROM p_disaster_event_id
    THEN
        RAISE EXCEPTION 'pg_flashback: restore journal context (operation %) no longer matches this restore at finalize (table %, tracking_id=%, target_lsn=%, disaster_event_id=%)',
            v_audited_op, p_target_table, admission.tracking_id, p_target_lsn, p_disaster_event_id
            USING ERRCODE = 'serialization_failure';
    END IF;

    PERFORM flashback_operation_append_event(
        v_audited_op,
        'applied_coverage_pending',
        NULL, NULL,
        'restore applied; waiting for exact successor coverage',
        jsonb_build_object(
            'rows_affected', materialized.events_applied,
            'expected_proof', v_expected_proof,
            'restore_verification', v_restore_verification,
            'successor', jsonb_build_object(
                'tracking_id', admission.tracking_id,
                'generation_id', v_new_generation_id,
                'stream_id', v_new_stream_id,
                'boundary_xid', v_boundary_xid,
                'boundary_marker', format(
                    'post-restore:%s:%s:%s',
                    admission.tracking_id, v_boundary_xid, v_generation_no
                ),
                'restored_target_lsn', p_target_lsn,
                'rel_oid_at_boundary', v_new_rel_oid
            )
        )
    );

    PERFORM public.flashback_set_restore_in_progress(false);
    RETURN materialized.events_applied;
EXCEPTION WHEN OTHERS THEN
    IF v_shadow_name IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS flashback.%I CASCADE', v_shadow_name);
    END IF;
    PERFORM public.flashback_set_restore_in_progress(false);
    RAISE;
END;
$$;

-- Role-scoped denies for flashback_admin / pg_monitor are applied by the
-- extension-wide deny-by-default pass in rbac_grants.sql (roles may not exist
-- yet when this file is loaded during CREATE EXTENSION).
REVOKE ALL ON FUNCTION flashback_internal_restore_lsn_core(text, pg_lsn, bigint, bigint) FROM PUBLIC;

CREATE OR REPLACE FUNCTION flashback_restore_lsn(
    p_target_table text,
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_stream_id bigint;
    v_locked record;
BEGIN
    PERFORM flashback_require_primary('flashback_restore_lsn');

    IF flashback_internal_get_audited_recover_context() IS NULL
       AND NOT COALESCE(
            NULLIF(current_setting('pg_flashback.allow_unaudited_restore', true), '')::boolean,
            false
       )
    THEN
        RAISE EXCEPTION
            'pg_flashback: flashback_restore_lsn requires audited recover context'
            USING ERRCODE = 'insufficient_privilege',
                  HINT = 'Use flashback_recover_begin + flashback_recover_execute, or set pg_flashback.allow_unaudited_restore=on for emergency/lab use only.';
    END IF;

    -- Stream serialization is the outermost lock in every WAL lifecycle
    -- operation.  Taking it before table/generation locks prevents a cycle in
    -- which the worker owns the stream lock while waiting on this restore's
    -- metadata transaction and the restore waits back on the worker.
    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: cannot establish WAL stream for post-restore boundary';
    END IF;

    SELECT * INTO STRICT v_locked
    FROM flashback_restore_lsn_lock_phase(p_target_table, p_target_lsn);

    PERFORM flashback_assert_relation_wal_drained(ARRAY[v_locked.out_rel_oid]);

    RETURN flashback_internal_restore_lsn_core(
        p_target_table,
        p_target_lsn,
        v_stream_id,
        v_locked.out_disaster_event_id
    );
EXCEPTION WHEN OTHERS THEN
    PERFORM public.flashback_set_restore_in_progress(false);
    RAISE;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_restore_lsn(
    p_tables text[],
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table text;
    v_ordered_tables text[];
    v_requested_count integer;
    v_tracking_count integer;
    v_has_cycle boolean;
    v_stream_id bigint;
    lock_rec record;
    v_total bigint := 0;
BEGIN
    IF p_tables IS NULL OR array_length(p_tables, 1) IS NULL THEN
        RAISE EXCEPTION 'flashback_restore_lsn: tables array is empty';
    END IF;

    -- Keep the database-wide stream lock outside every stable tracking lock,
    -- matching worker/single-restore ordering.
    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: cannot establish WAL stream for multi-table restore';
    END IF;

    v_requested_count := array_length(p_tables, 1);
    WITH requested AS (
        SELECT r.table_name, r.ord
        FROM unnest(p_tables) WITH ORDINALITY AS r(table_name, ord)
    ), admitted AS (
        SELECT r.table_name, r.ord, a.tracking_id, a.rel_oid
        FROM requested r
        CROSS JOIN LATERAL flashback_admit_lsn_target(
            r.table_name, p_target_lsn
        ) a
    )
    SELECT count(DISTINCT tracking_id) INTO v_tracking_count
    FROM admitted;
    IF v_tracking_count <> v_requested_count THEN
        RAISE EXCEPTION 'flashback_restore_lsn: duplicate table lifecycle in request';
    END IF;

    -- Acquire every lifecycle lock before mutating any table.  Ascending stable
    -- IDs make concurrent multi-table requests deterministic.
    FOR lock_rec IN
        WITH requested AS (
            SELECT r.table_name
            FROM unnest(p_tables) AS r(table_name)
        )
        SELECT DISTINCT a.tracking_id, a.schema_name, a.table_name
        FROM requested r
        CROSS JOIN LATERAL flashback_admit_lsn_target(
            r.table_name, p_target_lsn
        ) a
        ORDER BY a.tracking_id
    LOOP
        PERFORM flashback_internal_lock_lifecycle(lock_rec.tracking_id);
        EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
                       lock_rec.schema_name, lock_rec.table_name);
    END LOOP;

    -- Parents are restored before children.  A cycle is rejected rather than
    -- relying on an arbitrary input order under disabled constraint triggers.
    WITH RECURSIVE rels AS (
        SELECT r.table_name, r.ord, a.rel_oid
        FROM unnest(p_tables) WITH ORDINALITY AS r(table_name, ord)
        CROSS JOIN LATERAL flashback_admit_lsn_target(
            r.table_name, p_target_lsn
        ) a
    ), edges AS (
        SELECT c.conrelid AS child_relid, c.confrelid AS parent_relid
        FROM pg_constraint c
        JOIN rels child_r ON child_r.rel_oid = c.conrelid
        JOIN rels parent_r ON parent_r.rel_oid = c.confrelid
        WHERE c.contype = 'f'
    ), walk AS (
        SELECT r.rel_oid AS start_relid, r.rel_oid AS current_relid,
               ARRAY[r.rel_oid]::oid[] AS path, 0::integer AS depth,
               false AS cycle
        FROM rels r

        UNION ALL

        SELECT w.start_relid, e.parent_relid,
               w.path || e.parent_relid,
               w.depth + 1,
               e.parent_relid = ANY(w.path)
        FROM walk w
        JOIN edges e ON e.child_relid = w.current_relid
        WHERE NOT w.cycle
    ), max_depth AS (
        SELECT start_relid AS rel_oid, max(depth) AS depth
        FROM walk
        GROUP BY start_relid
    )
    SELECT
        array_agg(r.table_name ORDER BY COALESCE(md.depth, 0), r.ord),
        COALESCE((SELECT bool_or(cycle) FROM walk), false)
      INTO v_ordered_tables, v_has_cycle
    FROM rels r
    LEFT JOIN max_depth md USING (rel_oid);

    IF v_has_cycle THEN
        RAISE EXCEPTION 'flashback_restore_lsn: circular foreign keys exist inside the requested table set'
            USING HINT = 'Restore the cycle with a separately reviewed constraint-management plan.';
    END IF;

    PERFORM set_config('session_replication_role', 'replica', true);
    BEGIN
        FOREACH v_table IN ARRAY v_ordered_tables LOOP
            v_total := v_total + flashback_restore_lsn(v_table, p_target_lsn);
        END LOOP;
        PERFORM set_config('session_replication_role', 'origin', true);
        RETURN v_total;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('session_replication_role', 'origin', true);
        RAISE;
    END;
END;
$$;

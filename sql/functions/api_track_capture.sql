-- =================================================================
-- Public API: schema def collection, WAL track/untrack,
-- checkpoint, retention, history, DDL capture.
-- =================================================================

-- Returns 'wal' when capture_mode is unset/empty/wal.
-- Explicit trigger or auto (and any other value) fails closed — never silently
-- treat auto as wal.
CREATE OR REPLACE FUNCTION flashback_effective_capture_mode()
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_mode text;
BEGIN
    v_mode := NULLIF(btrim(COALESCE(current_setting('pg_flashback.capture_mode', true), '')), '');
    IF v_mode IS NULL OR v_mode = 'wal' THEN
        RETURN 'wal';
    END IF;
    RAISE EXCEPTION 'pg_flashback: capture_mode=% is not supported; only wal is operational',
        v_mode
        USING HINT = 'Set pg_flashback.capture_mode=wal and wal_level=logical. Values trigger and auto are rejected fail-closed.';
END;
$$;

-- Effective replication slot name for THIS database.
-- Logical replication slots are database-specific and slot names are
-- cluster-wide unique, so the default derives from the database name.
-- pg_flashback.slot_name overrides it (single-database installs only).
-- Slot names may contain only lower-case letters, digits and underscores.
CREATE OR REPLACE FUNCTION flashback_effective_slot_name()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        NULLIF(current_setting('pg_flashback.slot_name', true), ''),
        left('pg_flashback_' ||
             lower(regexp_replace(current_database(), '[^a-zA-Z0-9_]', '_', 'g')),
             63)
    );
$$;

CREATE OR REPLACE FUNCTION flashback_collect_schema_def(input_rel_oid oid)
RETURNS jsonb
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(
        'schema', n.nspname,
        'table', c.relname,
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'attnum', a.attnum,
                    'type_oid', a.atttypid,
                    'typmod', a.atttypmod,
                    'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'default_expr', pg_get_expr(d.adbin, d.adrelid),
                    'generated', a.attgenerated,
                    'identity', a.attidentity,
                    'identity_options', CASE
                        WHEN a.attidentity <> '' THEN (
                            SELECT jsonb_build_object(
                                'start', s.seqstart,
                                'increment', s.seqincrement,
                                'min', s.seqmin,
                                'max', s.seqmax,
                                'cache', s.seqcache,
                                'cycle', s.seqcycle,
                                'sequence_schema', nsp.nspname,
                                'sequence_name', seqc.relname,
                                'sequence_qualified', format('%I.%I', nsp.nspname, seqc.relname)
                            )
                            FROM pg_depend dep
                            JOIN pg_class seqc ON seqc.oid = dep.objid AND seqc.relkind = 'S'
                            JOIN pg_namespace nsp ON nsp.oid = seqc.relnamespace
                            JOIN pg_sequence s ON s.seqrelid = dep.objid
                            WHERE dep.classid = 'pg_class'::regclass
                              AND dep.refclassid = 'pg_class'::regclass
                              AND dep.refobjid = a.attrelid
                              AND dep.refobjsubid = a.attnum
                              AND dep.deptype = 'i'
                            LIMIT 1
                        )
                        ELSE NULL
                    END
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d
                ON d.adrelid = a.attrelid
               AND d.adnum = a.attnum
            WHERE a.attrelid = c.oid
              AND a.attnum > 0
              AND NOT a.attisdropped
        ), '[]'::jsonb),
        'primary_key', COALESCE((
            SELECT jsonb_agg(att.attname ORDER BY k.ord)
            FROM pg_index i
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = k.attnum
            WHERE i.indrelid = c.oid
              AND i.indisprimary
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', con.conname,
                    'type', con.contype,
                    'def', pg_get_constraintdef(con.oid, true)
                )
                ORDER BY con.conname
            )
            FROM pg_constraint con
            WHERE con.conrelid = c.oid
              AND con.contype IN ('c', 'u', 'f')
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', ic.relname,
                    'def', pg_get_indexdef(i.indexrelid)
                )
                ORDER BY ic.relname
            )
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = c.oid
              AND NOT i.indisprimary
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint con
                  WHERE con.conindid = i.indexrelid
              )
        ), '[]'::jsonb),
        'partition_by', CASE
            WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid)
            ELSE NULL
        END,
        'partitions', CASE
            WHEN c.relkind = 'p' THEN COALESCE((
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'name', child.relname,
                        'schema', cn.nspname,
                        'bound', pg_get_expr(child.relpartbound, child.oid)
                    )
                    ORDER BY child.relname
                )
                FROM pg_inherits inh
                JOIN pg_class child ON child.oid = inh.inhrelid
                JOIN pg_namespace cn ON cn.oid = child.relnamespace
                WHERE inh.inhparent = c.oid
            ), '[]'::jsonb)
            ELSE NULL
        END,
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', tg.tgname,
                    'def', pg_get_triggerdef(tg.oid)
                )
                ORDER BY tg.tgname
            )
            FROM pg_trigger tg
            WHERE tg.tgrelid = c.oid
              AND NOT tg.tgisinternal
              AND tg.tgname NOT LIKE 'flashback_capture_%'
        ), '[]'::jsonb),
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
                    'permissive', (pol.polpermissive),
                    'roles', COALESCE((
                        SELECT jsonb_agg(rolname)
                        FROM pg_roles r2
                        WHERE r2.oid = ANY(pol.polroles)
                    ), '[]'::jsonb),
                    'qual', pg_get_expr(pol.polqual, pol.polrelid),
                    'with_check', pg_get_expr(pol.polwithcheck, pol.polrelid)
                )
                ORDER BY pol.polname
            )
            FROM pg_policy pol
            WHERE pol.polrelid = c.oid
        ), '[]'::jsonb),
        'rls_enabled', c.relrowsecurity,
        'force_rls', c.relforcerowsecurity,
        -- Ownership metadata is required for flashback_restore_lsn after a
        -- real DROP TABLE, when the live relation is gone and cannot donate
        -- owner/ACL during finalize_shadow_swap.
        'owner', (SELECT rolname FROM pg_roles WHERE oid = c.relowner),
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
                              WHERE a.attrelid = c.oid AND a.attnum = d.objsubid)
                    END,
                    'text', d.description
                )
                ORDER BY d.objsubid, d.description
            )
            FROM pg_description d
            WHERE d.objoid = c.oid AND d.classoid = 'pg_class'::regclass
        ), '[]'::jsonb)
    )
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = input_rel_oid;
$$;

-- Fail-closed topology gate for the local DROP product profile.
-- Ordinary permanent LOGGED tables only; partitioned/foreign/matview/temp/unlogged rejected.
CREATE OR REPLACE FUNCTION flashback_require_supported_local_table(p_rel regclass)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_relkind "char";
    v_persistence "char";
    v_schema text;
    v_name text;
    v_parent_relkind "char";
BEGIN
    IF p_rel IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table does not exist';
    END IF;

    SELECT c.relkind, c.relpersistence, n.nspname, c.relname
      INTO v_relkind, v_persistence, v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_rel;

    IF v_relkind IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table does not exist';
    END IF;

    IF v_schema = 'pg_temp' OR v_schema LIKE 'pg_temp_%' OR v_schema LIKE 'pg_toast_temp_%' THEN
        RAISE EXCEPTION 'flashback_track: temporary tables are not supported by the local DROP recovery product'
            USING HINT = 'Protect a permanent LOGGED ordinary table instead.';
    END IF;

    IF v_relkind = 'p' THEN
        RAISE EXCEPTION 'flashback_track: partitioned tables are not supported by the local DROP recovery product (relkind=p)'
            USING HINT = 'Protect an ordinary non-partitioned LOGGED table.';
    ELSIF v_relkind = 'f' THEN
        RAISE EXCEPTION 'flashback_track: foreign tables are not supported by the local DROP recovery product'
            USING HINT = 'Protect an ordinary LOGGED table stored in PostgreSQL.';
    ELSIF v_relkind = 'm' THEN
        RAISE EXCEPTION 'flashback_track: materialized views are not supported by the local DROP recovery product'
            USING HINT = 'Protect an ordinary LOGGED table.';
    ELSIF v_relkind <> 'r' THEN
        RAISE EXCEPTION 'flashback_track: unsupported relation kind % for local DROP recovery', v_relkind
            USING HINT = 'First release supports ordinary LOGGED tables only.';
    END IF;

    IF v_persistence <> 'p' THEN
        RAISE EXCEPTION 'flashback_track: only permanent LOGGED tables are supported (persistence=%)',
            v_persistence
            USING HINT = 'UNLOGGED and temporary tables cannot provide durable DROP recovery.';
    END IF;

    -- Leaf partitions look like ordinary tables (relkind=r) but inherit from a
    -- partitioned parent; refuse them for the local product contract.
    SELECT p.relkind INTO v_parent_relkind
    FROM pg_inherits i
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE i.inhrelid = p_rel
    LIMIT 1;
    IF v_parent_relkind = 'p' THEN
        RAISE EXCEPTION 'flashback_track: table partitions are not supported by the local DROP recovery product'
            USING HINT = 'Protect an ordinary non-partitioned LOGGED table.';
    END IF;

    -- Classical inheritance children are destroyed by DROP ... CASCADE on the
    -- parent. The local product recovers one named table, not a CASCADE tree.
    IF EXISTS (
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = p_rel
          AND c.relkind = 'r'
    ) THEN
        RAISE EXCEPTION 'flashback_track: % has inheritance children; DROP ... CASCADE multi-object recovery is not supported',
            format('%I.%I', v_schema, v_name)
            USING HINT = 'pg_flashback recovers one ordinary table. Flatten inheritance or untrack children-first designs outside this release.';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_track(target_table text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_replica_identity_was "char" := 'd';
    v_replica_identity_index text := NULL;
    v_stream_id bigint;
    v_tracking_id bigint;
    v_snapshot_id bigint;
    v_generation_id bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
BEGIN
    PERFORM flashback_require_primary('flashback_track');
    -- Fail closed before any metadata: only wal is legal (raises for trigger/auto).
    PERFORM flashback_effective_capture_mode();

    SELECT c.oid, n.nspname, c.relname
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table % does not exist', target_table;
    END IF;

    PERFORM flashback_require_supported_local_table(v_rel_oid);
    -- Machine-checked preserve/reject gate (docs/SUPPORT.md is generated from
    -- this contract). Narrower topology checks above stay in place; this is
    -- the broader feature-by-feature compatibility surface.
    PERFORM flashback_require_local_compatibility(v_rel_oid);

    -- Clean-txn / isolation gates apply only after the table is known to be
    -- supportable. Callers probing unsupported topologies may already have
    -- created the relation in this transaction; they must still receive the
    -- topology/compatibility error rather than a dedicated-txn diagnostic.
    IF txid_current_if_assigned() IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_track() must run before any write in a dedicated transaction'
            USING HINT = 'COMMIT or ROLLBACK, then call flashback_track() as the first write in a new READ COMMITTED transaction.';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_track() requires READ COMMITTED isolation for a fresh post-lock snapshot';
    END IF;

    -- Fail closed when this database has no admitted, running capture worker.
    -- Membership in target_databases is not enough: max_workers truncation or a
    -- missing process would otherwise create a lifecycle that never consumes WAL.
    PERFORM flashback_require_admitted_capture_worker('flashback_track()');

    -- Ensure the replication slot exists. The slot is created HERE and only
    -- here — the background worker merely checks for it — so a creation
    -- failure must abort tracking (fail-closed): returning success without a
    -- slot would mean silently capturing nothing.
    -- pg_create_logical_replication_slot requires a transaction that has
    -- not performed writes yet.
    -- Lock order for every qualified lifecycle operation is database
    -- stream -> canonical pre-identity key -> stable tracking ID ->
    -- relation.  Take the database key before slot creation so two first
    -- trackers cannot race while creating the same per-database slot.
    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );

    -- Capture the current replica identity BEFORE we change it so that
    -- flashback_untrack() can restore the table to its original setting.
    SELECT c.relreplident INTO v_replica_identity_was
    FROM pg_class c WHERE c.oid = v_rel_oid;

    -- If the table uses REPLICA IDENTITY USING INDEX, remember which index
    -- so untrack can restore it exactly.
    IF v_replica_identity_was = 'i' THEN
        SELECT ic.relname INTO v_replica_identity_index
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE i.indrelid = v_rel_oid
          AND i.indisreplident;
    END IF;

    -- Logical slots are database-specific: a slot with our name that
    -- belongs to ANOTHER database cannot decode this database's changes,
    -- so the existence check must be scoped to current_database().
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots
        WHERE slot_name = flashback_effective_slot_name()
          AND database = current_database()
    ) THEN
        IF EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name = flashback_effective_slot_name()
        ) THEN
            RAISE EXCEPTION 'pg_flashback: replication slot % already exists but belongs to another database. WAL capture cannot work for %. Set pg_flashback.slot_name to a database-unique name.',
                flashback_effective_slot_name(), current_database();
        END IF;
        BEGIN
            PERFORM pg_create_logical_replication_slot(
                flashback_effective_slot_name(),
                'pg_flashback'
            );
            RAISE NOTICE 'pg_flashback: created logical replication slot %',
                flashback_effective_slot_name();
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'pg_flashback: could not create replication slot % (%). Without a slot, WAL capture would silently miss every change, so tracking is aborted.',
                flashback_effective_slot_name(), SQLERRM
                USING HINT = format(
                    'Run flashback_track in a fresh transaction with no prior writes, or create the slot manually first: SELECT pg_create_logical_replication_slot(%L, %L);',
                    flashback_effective_slot_name(), 'pg_flashback');
        END;
    END IF;

    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: WAL stream could not be activated for slot %',
            flashback_effective_slot_name();
    END IF;

    SELECT b.out_tracking_id, b.out_generation_id, b.out_boundary_xid,
           b.out_snapshot_id, b.out_provisional_lsn
      INTO v_tracking_id, v_generation_id, v_boundary_xid, v_snapshot_id, v_provisional_lsn
    FROM flashback_bootstrap_local_delta_lifecycle_core(
        v_rel_oid,
        v_stream_id,
        v_replica_identity_was,
        v_replica_identity_index
    ) AS b;

    -- Tracking itself only mutates flashback.* metadata, which the output
    -- plugin deliberately filters to avoid a worker feedback loop.  Emit one
    -- transactional marker so the decoder publishes this transaction's real
    -- COMMIT record and the building generation can acquire an exact
    -- COMMIT-LSN boundary.
    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'BOUNDARY',
            'kind', 'initial_track',
            'tracking_id', v_tracking_id,
            'generation_id', v_generation_id
        )::text
    );
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_checkpoint(target_table text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_checkpoint: legacy checkpoint API is disabled for correctness-qualified WAL generations'
        USING HINT = 'Use the generation-aware maintenance re-anchor operation when it is available; automatic full-table checkpoints are intentionally disabled.';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_take_due_checkpoints()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_take_due_checkpoints: periodic full-table checkpoint API is permanently disabled for correctness-qualified WAL coverage'
        USING HINT = 'Automatic full-table checkpoints are intentionally disabled in the WAL-only architecture.';
END;
$$;

-- Consume decoded changes from this database's logical replication slot
-- into delta_log. Normally invoked by the background worker every cycle;
-- callable manually for testing or after worker downtime.
-- Events are stamped with the transaction's REAL commit time (emitted by
-- the output plugin in its commit message) and the change LSN, so PITR
-- stays accurate even when consumption lags behind commits.
-- Returns the number of events inserted into delta_log.
CREATE OR REPLACE FUNCTION flashback_consume_wal(batch_size integer DEFAULT 4096)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_inserted integer := 0;
    v_stream_id bigint;
    v_frontier_lsn pg_lsn;
    v_frontier_time timestamptz;
    v_confirmed_flush_lsn pg_lsn;
    v_restart_lsn pg_lsn;
    v_missing_commits integer;
    v_has_output boolean;
    v_tracked_oids text;
    v_slot_name text;
    v_scan_start_lsn pg_lsn;
    v_upto_lsn pg_lsn;
    v_scan_window_bytes constant bigint := 16777216;
    v_empty_min_advance_bytes constant bigint := 65536;
    v_discarded integer;
    pending record;
    v_gap_inserted integer;
    lock_rec record;
    v_recheck_wal_status text;
BEGIN
    IF to_regclass('flashback.delta_log') IS NULL THEN
        RETURN 0;
    END IF;

    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RETURN 0;
    END IF;

    -- Freeze the upper bound before reading lifecycle metadata. A tracking or
    -- re-anchor transaction that commits after this point is beyond the fixed
    -- prefix even if a later READ COMMITTED statement can already see its
    -- catalog rows. Conversely, every commit included by this bound was
    -- visible before the tracked-OID snapshot below.
    v_slot_name := flashback_effective_slot_name();
    SELECT confirmed_flush_lsn
      INTO v_scan_start_lsn
    FROM pg_replication_slots
    WHERE slot_name = v_slot_name
      AND database = current_database();

    IF v_scan_start_lsn IS NULL THEN
        RETURN 0;
    END IF;

    v_upto_lsn := LEAST(
        pg_current_wal_insert_lsn(),
        v_scan_start_lsn + v_scan_window_bytes
    );
    IF v_upto_lsn <= v_scan_start_lsn THEN
        RETURN 0;
    END IF;

    -- Decode OIDs belonging to active local_delta lifecycles. Historical
    -- local_delta generation OIDs are retained because a restore swaps the
    -- physical relation while the slot may still contain pre-swap WAL.
    SELECT COALESCE(string_agg(rel_oid::text, ',' ORDER BY rel_oid), '')
      INTO v_tracked_oids
    FROM (
        SELECT tt.rel_oid
        FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND tt.recovery_profile = 'local_delta'
        UNION
        SELECT cg.rel_oid_at_boundary
        FROM flashback.coverage_generations cg
        JOIN flashback.tracked_tables tt USING (tracking_id)
        WHERE tt.is_active
          AND tt.recovery_profile = 'local_delta'
          AND cg.state IN ('building', 'active', 'sealed')
    ) recoverable_relations;

    -- Preflight the fixed prefix with tuple payload conversion disabled. A
    -- single transaction may decode to more than PostgreSQL's 256 MiB varlena
    -- limit, so never aggregate the peek into one JSONB value. This lightweight
    -- pass exists only to identify lifecycle locks before get_changes: logical
    -- slot advancement is not safely undone by a later PL/pgSQL exception.
    BEGIN
        IF NULLIF(current_setting('pg_flashback.test_consume_wal_failpoint', true), '')
             = 'fake_non_lost_55000'
        THEN
            -- TEST ONLY: proves the exception handler below distinguishes a
            -- genuine slot-loss SQLSTATE 55000 from an unrelated one instead
            -- of always treating this SQLSTATE as slot loss.
            RAISE EXCEPTION 'pg_flashback: test_consume_wal_failpoint=fake_non_lost_55000'
                USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
        SELECT EXISTS (
            SELECT 1
            FROM pg_logical_slot_peek_changes(
                     v_slot_name, v_upto_lsn, batch_size,
                     'tracked_oids', v_tracked_oids,
                     'metadata_only', 'true'
                 ) AS ch(lsn, xid, data)
            WHERE ch.data LIKE '{%'
        ) INTO v_has_output;
    EXCEPTION WHEN OBJECT_NOT_IN_PREREQUISITE_STATE THEN
        -- SQLSTATE 55000 is not unique to slot invalidation: e.g. pointing
        -- pg_logical_slot_peek_changes at a physical replication slot raises
        -- the identical SQLSTATE with an unrelated message ("cannot use
        -- physical replication slot for logical decoding"). Catching the
        -- SQLSTATE alone and unconditionally declaring the slot lost would
        -- misclassify any such unrelated failure as slot loss and silently
        -- swallow it (RETURN 0) instead of surfacing it. Re-verify against
        -- the authoritative wal_status column before treating this as the
        -- TOCTOU race against a concurrent checkpoint/WAL-removal cycle that
        -- flashback_ensure_active_wal_stream's own wal_status check can miss;
        -- anything else re-raises the original error unchanged.
        SELECT wal_status INTO v_recheck_wal_status
        FROM pg_replication_slots
        WHERE slot_name = v_slot_name;
        IF FOUND AND v_recheck_wal_status = 'lost' THEN
            -- Fail closed the same way the upstream check does rather than
            -- let the raw error propagate: an uncaught ERROR here kills the
            -- whole background worker process, and the postmaster's
            -- bgw_restart_time relaunch hits the identical error immediately,
            -- producing an unbounded once-a-second restart loop.
            PERFORM public.flashback_mark_capture_stream_broken(
                v_stream_id,
                'replication_slot_lost',
                jsonb_build_object('slot_name', v_slot_name, 'detected_in', 'flashback_consume_wal')
            );
            RETURN 0;
        END IF;
        RAISE;
    END;

    IF NOT v_has_output THEN
        -- Avoid a self-sustaining metadata-WAL loop for tiny internal tails.
        IF pg_wal_lsn_diff(v_upto_lsn, v_scan_start_lsn)
               < v_empty_min_advance_bytes
        THEN
            RETURN 0;
        END IF;

        PERFORM flashback_internal_advance_capture_stream_progress(
            v_stream_id,
            NULL,
            NULL,
            NULL,
            NULL,
            jsonb_build_object(
                'safe_slot_advance_start_lsn', v_scan_start_lsn,
                'safe_slot_advance_upto_lsn', v_upto_lsn,
                'safe_slot_advance_recorded_at', clock_timestamp()
            ),
            NULL
        );

        SELECT count(*)::integer
          INTO v_discarded
        FROM pg_logical_slot_get_changes(
            v_slot_name, v_upto_lsn, batch_size,
            'tracked_oids', v_tracked_oids,
            'metadata_only', 'true'
        );
        IF v_discarded <> 0 THEN
            -- A transaction can finish COMMIT between two READ COMMITTED
            -- decoding statements while its commit record is already below
            -- the fixed LSN bound. Logical-slot advancement performed by
            -- get_changes is not transactional, so this exception deliberately
            -- leaves the stream to fail closed on the next lifecycle audit. It
            -- must never be described as a safe retry: the decoded rows were
            -- not admitted to the trusted history.
            RAISE EXCEPTION 'pg_flashback: empty metadata peek/get race consumed % unexpected rows; capture coverage must be re-anchored',
                v_discarded
                USING ERRCODE = 'data_corrupted',
                      HINT = 'Inspect flashback_health(); re-anchor affected tables before accepting later restore targets.';
        END IF;

        -- PostgreSQL may confirm through the end of the containing WAL record,
        -- a few bytes beyond the requested upto_lsn. Record the catalog's
        -- actual post-consume position in the same successful call; otherwise
        -- the next lifecycle audit mistakes our own bounded empty-prefix
        -- advancement for an external slot move and creates a false gap.
        SELECT confirmed_flush_lsn, restart_lsn
          INTO v_confirmed_flush_lsn, v_restart_lsn
        FROM pg_replication_slots
        WHERE slot_name = v_slot_name
          AND database = current_database();

        PERFORM flashback_internal_advance_capture_stream_progress(
            v_stream_id,
            NULL,
            NULL,
            v_confirmed_flush_lsn,
            v_restart_lsn,
            jsonb_build_object(
                'safe_slot_advance_start_lsn', v_scan_start_lsn,
                'safe_slot_advance_upto_lsn', v_confirmed_flush_lsn,
                'safe_slot_advance_recorded_at', clock_timestamp()
            ),
            NULL
        );
        RETURN 0;
    END IF;

    DROP TABLE IF EXISTS pg_temp._fb_wal_peek;
    CREATE TEMP TABLE _fb_wal_peek (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_peek(change_lsn, source_xid, data, ord)
    SELECT ch.lsn, ch.xid::text::bigint, ch.data::jsonb, ch.ord
    FROM pg_logical_slot_peek_changes(
             v_slot_name, v_upto_lsn, batch_size,
             'tracked_oids', v_tracked_oids,
             'metadata_only', 'true'
         )
         WITH ORDINALITY AS ch(lsn, xid, data, ord)
    WHERE ch.data LIKE '{%'
    ORDER BY ch.ord;

    -- Pin only lifecycles touched by this peeked batch (plus building boundary
    -- resolutions and pending protected DDL for commits in the batch). Waiting
    -- on every active lifecycle made an unrelated restore/maintenance hold
    -- head-of-line block capture for other tables.
    --
    -- If any required lifecycle pin is busy, skip without get_changes so the
    -- slot does not advance past rows we are not allowed to promote yet.
    DROP TABLE IF EXISTS pg_temp._fb_wal_lock_ids;
    CREATE TEMP TABLE _fb_wal_lock_ids (
        tracking_id bigint PRIMARY KEY
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_lock_ids(tracking_id)
    SELECT DISTINCT needed.tracking_id
    FROM (
        SELECT tt.tracking_id
        FROM _fb_wal_peek p
        JOIN flashback.tracked_tables tt
          ON tt.is_active
         AND tt.recovery_profile = 'local_delta'
         AND tt.rel_oid = (p.data->>'oid')::oid
        WHERE (p.data->>'op') IN (
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'ALTER'
        )

        UNION

        SELECT cg.tracking_id
        FROM _fb_wal_peek p
        JOIN flashback.coverage_generations cg
          ON cg.rel_oid_at_boundary = (p.data->>'oid')::oid
         AND cg.state IN ('building', 'active', 'sealed')
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
         AND tt.is_active
         AND tt.recovery_profile = 'local_delta'
        WHERE (p.data->>'op') IN (
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'ALTER'
        )

        UNION

        SELECT cg.tracking_id
        FROM flashback.coverage_generations cg
        WHERE cg.state = 'building'
          AND cg.stream_id = v_stream_id
          AND EXISTS (
              SELECT 1
              FROM _fb_wal_peek p
              WHERE p.data ? 'commit'
                AND (p.data->>'commit')::bigint = cg.boundary_xid
          )

        UNION

        SELECT tt.tracking_id
        FROM flashback.pending_wal_events pend
        JOIN flashback.tracked_tables tt
          ON tt.rel_oid = pend.rel_oid
         AND tt.is_active
        WHERE pend.stream_id = v_stream_id
          AND EXISTS (
              SELECT 1
              FROM _fb_wal_peek p
              WHERE p.data ? 'commit'
                AND (p.data->>'commit')::bigint = pend.source_xid
          )
    ) needed
    WHERE needed.tracking_id IS NOT NULL;

    FOR lock_rec IN
        SELECT tracking_id FROM _fb_wal_lock_ids ORDER BY tracking_id
    LOOP
        IF NOT flashback_internal_try_lock_lifecycle(lock_rec.tracking_id) THEN
            RETURN 0;
        END IF;
    END LOOP;

    -- The required locks are now pinned. Record this consumer's exact safe
    -- advancement and fetch the full payload in the same transaction.
    PERFORM flashback_internal_advance_capture_stream_progress(
        v_stream_id,
        NULL,
        NULL,
        NULL,
        NULL,
        jsonb_build_object(
            'safe_slot_advance_start_lsn', v_scan_start_lsn,
            'safe_slot_advance_upto_lsn', v_upto_lsn,
            'safe_slot_advance_recorded_at', clock_timestamp()
        ),
        NULL
    );

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
    SELECT ch.lsn, ch.xid::text::bigint, ch.data::jsonb, ch.ord
    FROM pg_logical_slot_get_changes(
             v_slot_name, v_upto_lsn, batch_size,
             'tracked_oids', v_tracked_oids
         )
         WITH ORDINALITY AS ch(lsn, xid, data, ord)
    WHERE ch.data LIKE '{%'
    ORDER BY ch.ord;

    -- The full pass must describe the same ordered logical records as the
    -- lightweight preflight. Payload fields intentionally differ.
    IF EXISTS (
        (SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_peek
         EXCEPT ALL
         SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_batch)
        UNION ALL
        (SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_batch
         EXCEPT ALL
         SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_peek)
    ) THEN
        RAISE EXCEPTION 'pg_flashback: metadata/full prefix changed during preflight; retrying without slot advancement'
            USING ERRCODE = 'serialization_failure';
    END IF;

    -- Shared promote path (also used by the pg_test injection seam).
    SELECT confirmed_flush_lsn, restart_lsn
      INTO v_confirmed_flush_lsn, v_restart_lsn
    FROM pg_replication_slots
    WHERE slot_name = v_slot_name
      AND database = current_database();

    v_inserted := flashback_apply_decoded_wal_batch(
        v_stream_id,
        v_confirmed_flush_lsn,
        v_restart_lsn
    )::integer;

    RETURN v_inserted;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_retention_status()
RETURNS TABLE(
    table_name        text,
    retention_interval interval,
    oldest_delta      timestamptz,
    newest_delta      timestamptz,
    delta_count       bigint,
    restorable_window interval,
    retention_warning boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
BEGIN
    FOR rec IN
        SELECT tt.rel_oid, tt.tracking_id,
               format('%I.%I', tt.schema_name, tt.table_name) AS tbl,
               tt.retention_interval AS ri
        FROM flashback.tracked_tables tt WHERE tt.is_active
    LOOP
        RETURN QUERY
        SELECT rec.tbl, rec.ri,
               min(d.event_time), max(d.event_time),
               count(*)::bigint,
               (clock_timestamp() - COALESCE(min(d.event_time), clock_timestamp()))::interval,
               COALESCE((clock_timestamp() - min(d.event_time)) > (rec.ri * 0.9), false)
        FROM flashback.delta_log d
        WHERE d.tracking_id = rec.tracking_id
           OR (d.tracking_id IS NULL AND d.rel_oid = rec.rel_oid);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_history(target_table text, lookback interval)
RETURNS TABLE(
    event_time timestamptz,
    event_type text,
    row_identity jsonb,
    old_data jsonb,
    new_data jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_tracking_id bigint;
    v_pk_cols text[];
    rec record;
BEGIN
    -- Single canonical, ambiguity-safe resolver; an ambiguous unqualified
    -- name must fail closed rather than silently returning one schema's
    -- history and hiding the other's.
    SELECT r.rel_oid, r.tracking_id INTO v_rel_oid, v_tracking_id
    FROM public.flashback_internal_resolve_tracked_table(target_table) r;

    IF v_rel_oid IS NULL THEN
        SELECT d.rel_oid INTO v_rel_oid
        FROM flashback.delta_log d
        WHERE d.committed_at IS NOT NULL
          AND (d.table_name = target_table OR d.table_name = format('public.%s', target_table))
        ORDER BY d.event_id DESC LIMIT 1;
    END IF;

    IF v_rel_oid IS NULL THEN RETURN; END IF;

    SELECT ARRAY(
        SELECT jsonb_array_elements_text(
            COALESCE(flashback_collect_schema_def(v_rel_oid)->'primary_key', '[]'::jsonb)
        )
    ) INTO v_pk_cols;

    FOR rec IN
        SELECT d.event_time, d.event_type, d.old_data, d.new_data
        FROM flashback.delta_log d
        WHERE (
            (v_tracking_id IS NOT NULL AND d.tracking_id = v_tracking_id)
            OR (v_tracking_id IS NULL AND d.rel_oid = v_rel_oid)
        )
          AND d.committed_at IS NOT NULL
          AND d.event_time >= clock_timestamp() - lookback
        ORDER BY d.event_time DESC
    LOOP
        event_time := rec.event_time;
        event_type := rec.event_type;
        old_data := rec.old_data;
        new_data := rec.new_data;

        IF array_length(v_pk_cols, 1) IS NULL THEN
            row_identity := COALESCE(rec.new_data, rec.old_data);
        ELSE
            SELECT COALESCE(jsonb_object_agg(pk, COALESCE(rec.new_data -> pk, rec.old_data -> pk)), '{}'::jsonb)
              INTO row_identity
            FROM unnest(v_pk_cols) AS pk;
        END IF;

        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_untrack(target_table text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_tracking_id bigint;
    v_schema_name text;
    v_table_name text;
    v_base_snapshot text;
    v_recovery_profile text;
    v_has_generations boolean := false;
    snap_rec record;
BEGIN
    PERFORM flashback_require_primary('flashback_untrack');

    -- Single canonical, ambiguity-safe resolver; an ambiguous unqualified
    -- name must fail closed rather than silently untracking one of several
    -- same-named lifecycles across schemas.
    SELECT r.rel_oid, r.tracking_id, r.schema_name, r.table_name
      INTO v_rel_oid, v_tracking_id, v_schema_name, v_table_name
    FROM public.flashback_internal_resolve_tracked_table(target_table) r;

    IF v_rel_oid IS NULL THEN RETURN false; END IF;

    -- Not part of the name-resolution algorithm: a plain lookup by the
    -- already-resolved tracking_id, not a second name search.
    SELECT tt.base_snapshot_table, tt.recovery_profile
      INTO v_base_snapshot, v_recovery_profile
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id;

    SELECT EXISTS (
        SELECT 1 FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = v_tracking_id
    ) INTO v_has_generations;

    IF v_has_generations THEN
        -- Qualified WAL lifecycle operations use database-stream -> stable
        -- tracking order. Untrack consumes the slot before retiring the
        -- binding, so taking only the tracking key first would deadlock
        -- against the worker (which takes the database key first).
        PERFORM flashback_internal_lock_database_stream(
            (SELECT oid FROM pg_database WHERE datname = current_database())
        );
        PERFORM flashback_internal_lock_lifecycle(v_tracking_id);
        IF EXISTS (
            SELECT 1 FROM flashback.coverage_generations cg
            WHERE cg.tracking_id = v_tracking_id AND cg.state = 'building'
        ) THEN
            RAISE EXCEPTION 'flashback_untrack: lifecycle % has a pending generation', v_tracking_id
                USING HINT = 'Wait for its boundary COMMIT LSN to resolve before untracking.';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM flashback.generation_payload_retirements r
            WHERE r.tracking_id = v_tracking_id
              AND r.state = 'retiring'
        ) THEN
            RAISE EXCEPTION 'flashback_untrack: lifecycle % has an unfinished retention cleanup',
                v_tracking_id
                USING HINT = 'Resume the durable generation retirement, then retry untrack.';
        END IF;
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE',
                           v_schema_name, v_table_name);
        END IF;
        PERFORM flashback_consume_wal(50000);
    END IF;

    -- Restore the table's original REPLICA IDENTITY. flashback_track() forced
    -- it to FULL; leaving it there permanently causes write amplification and
    -- changes logical decoding behaviour for the application after untrack.
    IF v_recovery_profile = 'local_delta'
       AND to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL
    THEN
        DECLARE
            v_original_ri    "char";
            v_original_ri_idx text;
            v_ri_clause      text;
        BEGIN
            SELECT tt.replica_identity_was, tt.replica_identity_index
              INTO v_original_ri, v_original_ri_idx
            FROM flashback.tracked_tables tt WHERE tt.rel_oid = v_rel_oid;

            v_ri_clause := CASE COALESCE(v_original_ri, 'd')
                WHEN 'f' THEN 'FULL'
                WHEN 'n' THEN 'NOTHING'
                WHEN 'i' THEN
                    CASE WHEN v_original_ri_idx IS NOT NULL
                         THEN 'USING INDEX ' || quote_ident(v_original_ri_idx)
                         ELSE 'DEFAULT'   -- index name unknown; fall back
                    END
                ELSE 'DEFAULT'
            END;
            -- This ALTER runs while the row is still actively tracked (the
            -- DELETE below hasn't happened yet): without the guard, A2's
            -- corrected nested-DDL capture would treat pg_flashback's own
            -- untrack cleanup as user DDL and route it through the qualified
            -- capture pipeline, which can reject it if the generation isn't
            -- in the exact expected state. Bypass via the same explicit
            -- backend-local flag flashback_restore_lsn uses, not a context
            -- assumption.
            PERFORM flashback_set_restore_in_progress(true);
            BEGIN
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY %s',
                    v_schema_name, v_table_name, v_ri_clause);
            EXCEPTION WHEN OTHERS THEN
                PERFORM flashback_set_restore_in_progress(false);
                RAISE;
            END;
            PERFORM flashback_set_restore_in_progress(false);
        END;
    END IF;

    -- v_base_snapshot is tracked_tables.base_snapshot_table's compatibility
    -- text projection of the same row the loop below already reaches by
    -- snapshot_id/tracking_id (v_has_generations is always true for a
    -- WAL-only local_delta lifecycle), so a separate early drop here would
    -- only race SnapshotStore's own drop for the identical artifact.

    FOR snap_rec IN
        SELECT snapshot_id, tracking_id, payload_state
        FROM flashback.snapshots
        WHERE tracking_id = v_tracking_id
    LOOP
        IF snap_rec.payload_state = 'available' THEN
            PERFORM public.flashback_internal_snapshot_retire(
                snap_rec.snapshot_id, snap_rec.tracking_id, 'retired'
            );
        END IF;
    END LOOP;

    DELETE FROM flashback.delta_log
    WHERE (v_has_generations AND tracking_id = v_tracking_id)
       OR (NOT v_has_generations AND rel_oid = v_rel_oid);
    DELETE FROM flashback.schema_versions
    WHERE (v_has_generations AND tracking_id = v_tracking_id)
       OR (NOT v_has_generations AND rel_oid = v_rel_oid);

    IF v_has_generations THEN
        FOR snap_rec IN
            SELECT generation_id, tracking_id, valid_through_lsn
            FROM flashback.coverage_generations
            WHERE tracking_id = v_tracking_id
              AND state = 'active'
            ORDER BY generation_id
        LOOP
            PERFORM flashback_internal_transition_coverage_generation(
                snap_rec.generation_id,
                snap_rec.tracking_id,
                'active',
                'sealed',
                'untracked',
                NULL,
                NULL,
                NULL,
                NULL,
                snap_rec.valid_through_lsn + 1,
                clock_timestamp(),
                '{}'::jsonb
            );
        END LOOP;
        FOR snap_rec IN
            SELECT generation_id, tracking_id
            FROM flashback.coverage_generations
            WHERE tracking_id = v_tracking_id
              AND state = 'sealed'
            ORDER BY generation_id
        LOOP
            PERFORM flashback_internal_transition_coverage_generation(
                snap_rec.generation_id,
                snap_rec.tracking_id,
                'sealed',
                'retired',
                'untracked',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                '{}'::jsonb
            );
        END LOOP;
    END IF;

    DELETE FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    RETURN true;
END;
$$;

-- Destructive DDL removes heap/TOAST files that logical decoding can still
-- need for an older UPDATE whose unchanged varlena values remain represented
-- by on-disk TOAST pointers. Lock the relation first, then let the independent
-- capture worker drain a fixed committed prefix while those files still exist.
--
-- Do not call flashback_consume_wal() here: slot advancement would belong to
-- the caller's open DDL transaction. The background worker owns the separate
-- transaction required to advance durably while this transaction waits.
CREATE OR REPLACE FUNCTION flashback_internal_prepare_destructive_ddl(
    input_schema text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_tracking_id bigint;
    v_schema_name text;
    v_table_name text;
    v_slot_name text;
    v_barrier_lsn pg_lsn;
    v_has_pending boolean;
    v_deadline timestamptz;
    v_timeout_ms integer;
    v_database_oid oid;
    v_drain_lock_acquired boolean;
    v_synthetic_stream boolean;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN
        RETURN;
    END IF;

    IF input_schema IS NULL OR input_schema = '' THEN
        v_rel_oid := to_regclass(quote_ident(input_table));
    ELSE
        v_rel_oid := to_regclass(format('%I.%I', input_schema, input_table));
    END IF;
    IF v_rel_oid IS NULL THEN
        RETURN;
    END IF;

    SELECT tt.tracking_id, n.nspname, c.relname
      INTO v_tracking_id, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    JOIN pg_class c ON c.oid = tt.rel_oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND tt.rel_oid = v_rel_oid;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- A stream established through the normal admission path is always
    -- backed by a genuine physical replication slot; a stream explicitly
    -- marked as having no physical slot backing it is not, and there is no
    -- real WAL to drain in the first place for it -- the pre-drain wait
    -- below exists to protect a real decoder against a real unlinked TOAST
    -- pointer, which cannot happen without real WAL. Skip straight to the
    -- destructive DDL rather than peeking a slot that was never created.
    --
    -- No matching active generation at all (NOT FOUND) is treated the same
    -- way: there is no real backlog to protect either, and this is not a
    -- silent gap -- flashback_capture_ddl_event's own
    -- flashback_capture_configuration_guard check, immediately after this
    -- function returns, independently re-verifies an active generation
    -- exists and fails closed if a qualified lifecycle genuinely lacks one.
    SELECT COALESCE((cs.details->>'no_physical_slot')::boolean, false)
      INTO v_synthetic_stream
    FROM flashback.coverage_generations cg
    JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
    WHERE cg.tracking_id = v_tracking_id
      AND cg.state = 'active';
    IF NOT FOUND OR COALESCE(v_synthetic_stream, false) THEN
        RETURN;
    END IF;

    -- SHARE blocks INSERT/UPDATE/DELETE and competing destructive DDL, while
    -- still allowing logical decoding to open the relation under ACCESS SHARE.
    -- Once granted, the fixed barrier is stable because no new writer can
    -- commit against this relation. The real DROP/TRUNCATE upgrades this to
    -- ACCESS EXCLUSIVE only after the decoder has drained.
    v_timeout_ms := public.flashback_apply_local_boundary_lock_timeout();
    EXECUTE format(
        'LOCK TABLE %I.%I IN SHARE MODE',
        v_schema_name,
        v_table_name
    );

    v_slot_name := public.flashback_effective_slot_name();
    SELECT oid INTO v_database_oid
    FROM pg_database
    WHERE datname = current_database();
    v_barrier_lsn := pg_current_wal_insert_lsn();
    v_timeout_ms := GREATEST(v_timeout_ms, 1);
    v_deadline := clock_timestamp() + make_interval(secs => v_timeout_ms / 1000.0);

    LOOP
        -- Coordinate with the worker's session-scoped stream lock. Hold it
        -- only for the metadata peek; release it whenever work remains so the
        -- independent worker can consume and commit that prefix.
        v_drain_lock_acquired := pg_try_advisory_lock(
            public.flashback_internal_lock_ns_stream(),
            v_database_oid::integer
        );
        IF v_drain_lock_acquired THEN
            BEGIN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_logical_slot_peek_changes(
                        v_slot_name,
                        v_barrier_lsn,
                        1,
                        'tracked_oids',
                        v_rel_oid::text,
                        'metadata_only',
                        'true'
                    ) AS ch(lsn, xid, data)
                    WHERE ch.data LIKE '{%'
                )
                  INTO v_has_pending;
                PERFORM pg_advisory_unlock(
                    public.flashback_internal_lock_ns_stream(),
                    v_database_oid::integer
                );
                v_drain_lock_acquired := false;
            EXCEPTION
                WHEN OTHERS THEN
                    PERFORM pg_advisory_unlock(
                        public.flashback_internal_lock_ns_stream(),
                        v_database_oid::integer
                    );
                    v_drain_lock_acquired := false;
                    RAISE;
            END;
        ELSE
            v_has_pending := true;
        END IF;

        EXIT WHEN NOT v_has_pending;

        IF clock_timestamp() >= v_deadline THEN
            RAISE EXCEPTION
                'pg_flashback: destructive DDL refused because committed WAL for % did not drain before the % ms write-stall limit',
                format('%I.%I', v_schema_name, v_table_name), v_timeout_ms
                USING ERRCODE = 'lock_not_available',
                      HINT = 'Let the capture worker catch up, inspect flashback_health(), then retry. The table was not changed.';
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION flashback_internal_prepare_destructive_ddl(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION flashback_capture_ddl_event(
    event_type text,
    input_schema text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    tracked record;
    ddl_info jsonb;
    row_snapshot jsonb;
    new_version bigint;
    ddl_event_time timestamptz;
    ddl_event_lsn pg_lsn;
    v_actual_schema text;
    v_actual_table  text;
    v_generation_id bigint;
    v_stream_id bigint;
    v_stream_state text;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN RETURN; END IF;

    -- After RENAME TABLE the hook fires with the NEW name.
    -- tracked_tables still has the OLD name but same OID.
    -- Try new name first; fall back to OID-based lookup.
    IF input_schema IS NULL OR input_schema = '' THEN
        SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = input_table
          AND tt.is_active
        ORDER BY tt.tracked_since DESC LIMIT 1;
    ELSE
        SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.schema_name = input_schema AND tt.table_name = input_table
          AND tt.is_active
        ORDER BY tt.tracked_since DESC
        LIMIT 1;
    END IF;

    -- If not found by name, try by OID (handles RENAME TABLE: new name passed,
    -- tracked_tables still has old name, but OID is stable).
    IF tracked.rel_oid IS NULL THEN
        DECLARE v_oid oid;
        BEGIN
            IF input_schema IS NOT NULL AND input_schema <> '' THEN
                v_oid := to_regclass(format('%I.%I', input_schema, input_table));
            ELSE
                v_oid := to_regclass(input_table);
            END IF;
            IF v_oid IS NOT NULL THEN
                SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
                  INTO tracked
                FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = v_oid
                  AND tt.is_active
                ORDER BY tt.tracked_since DESC LIMIT 1;
            END IF;
        END;
    END IF;

    IF tracked.rel_oid IS NULL THEN RETURN; END IF;

    IF tracked.recovery_profile = 'local_delta' THEN
        -- DDL has no row trigger to mediate a session-local SUSET override.
        -- Reconcile it synchronously before routing the event; a refused guard
        -- fails the hook closed so the DDL cannot commit against an unrecorded
        -- qualified lifecycle.
        IF NOT flashback_capture_configuration_guard(tracked.rel_oid) THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because capture configuration is disabled or no active WAL epoch exists'
                USING HINT = 'Restore pg_flashback.enabled/capture_mode, then establish a new exact boundary with flashback_reanchor().';
        END IF;

        -- Serialize DDL routing with stream breaks and generation retirement.
        -- The configuration reconciler holds the database-stream key first
        -- and then this key; this path never takes the outer database key.
        PERFORM flashback_internal_lock_lifecycle(tracked.tracking_id);

        -- An existing qualified lifecycle is routed by its durable generation
        -- binding, never by a caller's session-local capture_mode GUC. This
        -- prevents `SET capture_mode=trigger` from silently sending DDL around
        -- the protected WAL path.
        SELECT cg.generation_id, cg.stream_id, cs.state
          INTO v_generation_id, v_stream_id, v_stream_state
        FROM flashback.coverage_generations cg
        JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
        WHERE cg.tracking_id = tracked.tracking_id
          AND cg.state = 'active'
        LIMIT 1;

        IF v_generation_id IS NULL AND (
            EXISTS (
                SELECT 1 FROM flashback.coverage_generations cg
                WHERE cg.tracking_id = tracked.tracking_id
            )
            OR COALESCE(
                   NULLIF(btrim(COALESCE(current_setting('pg_flashback.capture_mode', true), '')), ''),
                   'wal'
               ) = 'wal'
        ) THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because tracking lifecycle % has no active WAL generation',
                tracked.tracking_id;
        END IF;
        IF v_generation_id IS NOT NULL AND v_stream_state <> 'active' THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because WAL stream % is %',
                v_stream_id, v_stream_state
                USING HINT = 'Restore pg_flashback.enabled/capture_mode, then establish a new exact boundary with flashback_reanchor().';
        END IF;
    END IF;

    -- Qualified local_delta DDL uses the shared staging core so production
    -- hooks and the pg_test seam cannot diverge on metadata writes.
    IF tracked.recovery_profile = 'local_delta' AND v_generation_id IS NOT NULL THEN
        PERFORM flashback_stage_local_delta_ddl_event(
            tracked.tracking_id,
            event_type,
            (txid_current() % 4294967296)::bigint,
            NULL,   -- event_lsn: core uses pg_current_wal_insert_lsn()
            NULL,   -- ddl_info: core collects from live relation
            true,   -- collect_row_snapshot
            true    -- emit logical commit marker
        );
        RETURN;
    END IF;

    -- Resolve current (post-DDL) actual name from catalog
    SELECT n.nspname, c.relname
      INTO v_actual_schema, v_actual_table
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = tracked.rel_oid;

    -- RENAME TABLE / SET SCHEMA: update tracked_tables with the new name
    IF v_actual_schema IS NOT NULL AND v_actual_table IS NOT NULL
       AND (v_actual_schema <> tracked.schema_name OR v_actual_table <> tracked.table_name)
    THEN
        UPDATE flashback.tracked_tables
           SET schema_name = v_actual_schema,
               table_name  = v_actual_table
         WHERE rel_oid = tracked.rel_oid;

        RAISE NOTICE 'pg_flashback: table renamed/moved from %.% to %.% — tracking updated',
            tracked.schema_name, tracked.table_name, v_actual_schema, v_actual_table;

        tracked.schema_name := v_actual_schema;
        tracked.table_name  := v_actual_table;
    END IF;

    ddl_event_time := clock_timestamp();
    ddl_event_lsn := pg_current_wal_insert_lsn();

    IF upper(event_type) = 'ALTER' THEN
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1) + 1;

        UPDATE flashback.tracked_tables
        SET schema_version = new_version
        WHERE rel_oid = tracked.rel_oid;

        INSERT INTO flashback.schema_versions (
            rel_oid, tracking_id, generation_id, stream_id, source_xid,
            schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
            columns, primary_key, constraints, helper_schema_sha256
        )
        SELECT
            tracked.rel_oid,
            NULL,
            NULL, NULL,
            NULL,
            new_version, ddl_event_time, ddl_event_lsn,
            clock_timestamp(),
            NULL,
            COALESCE(ddl_info -> 'columns', '[]'::jsonb),
            COALESCE(ddl_info -> 'primary_key', '[]'::jsonb),
            jsonb_build_object(
                'check_unique_fk', COALESCE(ddl_info -> 'constraints', '[]'::jsonb),
                'indexes', COALESCE(ddl_info -> 'indexes', '[]'::jsonb),
                'partition_by', ddl_info -> 'partition_by',
                'partitions', ddl_info -> 'partitions',
                'triggers', COALESCE(ddl_info -> 'triggers', '[]'::jsonb),
                'rls_policies', COALESCE(ddl_info -> 'rls_policies', '[]'::jsonb),
                'rls_enabled', COALESCE((ddl_info -> 'rls_enabled')::boolean, false)
            ),
            flashback_helper_schema_sha256(tracked.rel_oid);
    ELSE
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1);
    END IF;

    DECLARE
        v_row_count bigint;
    BEGIN
        EXECUTE format(
            'SELECT count(*) FROM (SELECT 1 FROM %I.%I LIMIT 100001) q',
            tracked.schema_name, tracked.table_name
        ) INTO v_row_count;
        IF v_row_count > 100000 THEN
            RAISE WARNING 'pg_flashback: table %.% has % rows — skipping inline DDL snapshot (checkpoint data preserved)',
                tracked.schema_name, tracked.table_name, v_row_count;
            row_snapshot := NULL;
        ELSE
            EXECUTE format(
                'SELECT COALESCE(jsonb_agg(to_jsonb(t)), ''[]''::jsonb) FROM %I.%I t',
                tracked.schema_name, tracked.table_name
            ) INTO row_snapshot;
        END IF;
    END;

    INSERT INTO flashback.delta_log (
        event_time, event_type, table_name, rel_oid, source_xid,
        committed_at, lsn, schema_version, old_data, new_data, ddl_info
    )
    VALUES (
        ddl_event_time, upper(event_type),
        format('%I.%I', tracked.schema_name, tracked.table_name),
        tracked.rel_oid, (txid_current() % 4294967296)::bigint,
        clock_timestamp(), ddl_event_lsn,
        new_version, row_snapshot, NULL, ddl_info
    );
END;
$$;

-- ----------------------------------------------------------------
-- flashback_ensure_delta_partition
-- ----------------------------------------------------------------
-- Called by the background worker each cycle (via run_ensure_partitions).
-- Creates monthly range partitions for delta_log if it is a partitioned table.
-- Idempotent — safe to call repeatedly.
-- Creates the current month's partition and next month's partition
-- (pre-created 7 days before month-end to prevent data loss at rollover).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_ensure_delta_partition(for_date date DEFAULT CURRENT_DATE)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_is_partitioned boolean;
    v_month_start    timestamptz;
    v_month_end      timestamptz;
    v_next_start     timestamptz;
    v_next_end       timestamptz;
    v_part_name      text;
    v_next_part_name text;
BEGIN
    -- Only act if delta_log is a partitioned table
    SELECT c.relkind = 'p'
      INTO v_is_partitioned
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flashback' AND c.relname = 'delta_log';

    IF NOT FOUND OR NOT v_is_partitioned THEN
        RETURN;
    END IF;

    -- Current month boundaries
    v_month_start := date_trunc('month', for_date::timestamptz);
    v_month_end   := date_trunc('month', for_date::timestamptz) + interval '1 month';
    v_part_name   := 'delta_log_' || to_char(for_date, 'YYYY_MM');

    PERFORM flashback__create_range_partition(v_part_name, v_month_start, v_month_end);

    -- Pre-create next month's partition when within the last 7 days of the month
    IF for_date >= (v_month_end::date - 7) THEN
        v_next_start     := v_month_end;
        v_next_end       := v_month_end + interval '1 month';
        v_next_part_name := 'delta_log_' || to_char(v_next_start, 'YYYY_MM');
        PERFORM flashback__create_range_partition(v_next_part_name, v_next_start, v_next_end);
    END IF;
END;
$$;

-- ----------------------------------------------------------------
-- flashback__create_range_partition (internal helper)
-- ----------------------------------------------------------------
-- Creates a monthly delta_log partition.
-- If the default partition has rows in this range, migrates them first.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback__create_range_partition(
    p_part_name  text,
    p_range_from timestamptz,
    p_range_to   timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_default_oid oid;
    v_part_oid    oid;
    v_is_owned    boolean;
    v_tmp_table   text;
BEGIN
    -- Existing partitions from older releases may not yet be extension
    -- members.  The dependency check is the hot-path: this function runs on
    -- every worker cycle, so an already-owned partition must not take an
    -- ACCESS EXCLUSIVE relation lock every 75 ms.
    v_part_oid := to_regclass(format('flashback.%I', p_part_name));
    IF v_part_oid IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_depend d
            JOIN pg_extension e ON e.oid = d.refobjid
            WHERE d.classid = 'pg_class'::regclass
              AND d.objid = v_part_oid
              AND d.objsubid = 0
              AND d.refclassid = 'pg_extension'::regclass
              AND d.deptype = 'e'
              AND e.extname = 'pg_flashback'
        ) INTO v_is_owned;
        IF NOT v_is_owned THEN
            PERFORM flashback_own_payload_table(v_part_oid::regclass);
        END IF;
        RETURN;
    END IF;

    -- Check if default partition has rows in this range (would block CREATE PARTITION)
    v_default_oid := to_regclass('flashback.delta_log_default');
    IF v_default_oid IS NOT NULL THEN
        v_tmp_table := '_fb_part_mig_' || p_part_name;

        -- Move rows out of default partition into a temp table
        EXECUTE format(
            'CREATE TEMP TABLE %I ON COMMIT PRESERVE ROWS AS
             WITH migrated AS (
                 DELETE FROM flashback.delta_log_default
                 WHERE committed_at >= %L AND committed_at < %L
                 RETURNING *
             )
             SELECT * FROM migrated',
            v_tmp_table, p_range_from, p_range_to
        );
    END IF;

    -- Now create the named partition (default partition is clear)
    EXECUTE format(
        'CREATE TABLE flashback.%I PARTITION OF flashback.delta_log
         FOR VALUES FROM (%L) TO (%L)',
        p_part_name, p_range_from, p_range_to
    );
    PERFORM flashback_own_payload_table(
        to_regclass(format('flashback.%I', p_part_name))
    );

    -- Re-insert migrated rows into the new named partition
    IF v_default_oid IS NOT NULL THEN
        EXECUTE format(
            'INSERT INTO flashback.%I SELECT * FROM %I',
            p_part_name, v_tmp_table
        );
        EXECUTE format('DROP TABLE IF EXISTS %I', v_tmp_table);
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Clean up temp table if it was created
    IF v_tmp_table IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', v_tmp_table);
    END IF;
    RAISE WARNING 'flashback: could not create or adopt partition %: %', p_part_name, SQLERRM;
    RAISE;
END;
$$;

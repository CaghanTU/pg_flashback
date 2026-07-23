-- RBAC contract: extension routines are deny-by-default, and only the explicit
-- API allowlist is granted to delegated roles.
DO $tv$
DECLARE
    v_public_routines text;
    v_allowlist_diff text;
    v_api record;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        RAISE EXCEPTION 'flashback_admin role does not exist';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_recovery_agent') THEN
        RAISE EXCEPTION 'flashback_recovery_agent role does not exist';
    END IF;

    -- Default PostgreSQL routine ACLs include PUBLIC EXECUTE. The extension's
    -- finalize SQL must remove it from every production routine, including new
    -- SECURITY DEFINER helpers that are not in a hand-maintained name list.
    SELECT string_agg(
               format('%I.%I(%s)',
                   n.nspname,
                   p.proname,
                   pg_get_function_identity_arguments(p.oid)),
               ', ' ORDER BY p.oid::regprocedure::text
           )
      INTO v_public_routines
    FROM pg_depend d
    JOIN pg_extension e
      ON e.oid = d.refobjid
     AND d.refclassid = 'pg_extension'::regclass
    JOIN pg_proc p
      ON d.classid = 'pg_proc'::regclass
     AND p.oid = d.objid
    JOIN pg_namespace n ON n.oid = p.pronamespace
    CROSS JOIN LATERAL aclexplode(
        COALESCE(p.proacl, acldefault('f', p.proowner))
    ) AS routine_acl
    WHERE e.extname = 'pg_flashback'
      AND d.deptype = 'e'
      -- pgrx test wrappers are extension members in the `tests` schema, but
      -- are not shipped production routines. The control file pins production
      -- routines to `public`.
      AND n.nspname = 'public'
      AND routine_acl.grantee = 0
      AND routine_acl.privilege_type = 'EXECUTE';

    IF v_public_routines IS NOT NULL THEN
        RAISE EXCEPTION 'PUBLIC can execute pg_flashback routines: %',
            v_public_routines;
    END IF;

    IF NOT has_schema_privilege('flashback_admin', 'flashback', 'USAGE') THEN
        RAISE EXCEPTION 'flashback_admin lacks USAGE on flashback schema';
    END IF;
    IF has_schema_privilege('flashback_admin', 'flashback', 'CREATE') THEN
        RAISE EXCEPTION
            'flashback_admin has CREATE on flashback schema; SECURITY DEFINER helper shadowing is possible';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flashback'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND (
              has_table_privilege('flashback_admin', c.oid, 'INSERT')
              OR has_table_privilege('flashback_admin', c.oid, 'UPDATE')
              OR has_table_privilege('flashback_admin', c.oid, 'DELETE')
              OR has_table_privilege('flashback_admin', c.oid, 'TRUNCATE')
              OR has_table_privilege('flashback_admin', c.oid, 'REFERENCES')
              OR has_table_privilege('flashback_admin', c.oid, 'TRIGGER')
          )
    ) THEN
        RAISE EXCEPTION
            'flashback_admin can mutate internal tables; delegated administration must be API-only';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_default_acl da
        CROSS JOIN LATERAL aclexplode(da.defaclacl) default_acl
        WHERE da.defaclnamespace = 'flashback'::regnamespace
          AND default_acl.grantee = 'flashback_admin'::regrole
          AND default_acl.privilege_type IN (
              'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'
          )
    ) THEN
        RAISE EXCEPTION
            'future flashback payload tables inherit mutating flashback_admin privileges';
    END IF;

    IF has_table_privilege('public', 'flashback.pending_wal_events', 'INSERT')
       OR has_table_privilege('public', 'flashback.pending_wal_events', 'UPDATE')
       OR has_table_privilege('public', 'flashback.pending_wal_events', 'DELETE')
    THEN
        RAISE EXCEPTION 'PUBLIC can forge protected pending WAL events';
    END IF;

    -- Runtime payload is created after CREATE EXTENSION, so verify the
    -- adoption helper transfers ownership and strips delegated-role ACLs
    -- rather than relying only on install-time grants.
    EXECUTE 'CREATE TABLE flashback.snap_987654321_987654321 (id integer)';
    PERFORM flashback_own_payload_table(
        'flashback.snap_987654321_987654321'::regclass
    );
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_extension e ON e.extname = 'pg_flashback'
        WHERE c.oid = 'flashback.snap_987654321_987654321'::regclass
          AND c.relowner = e.extowner
    ) THEN
        RAISE EXCEPTION 'runtime payload owner is not the extension owner';
    END IF;
    IF has_table_privilege(
        'flashback_admin',
        'flashback.snap_987654321_987654321',
        'INSERT,UPDATE,DELETE,TRUNCATE'
    ) OR has_table_privilege(
        'flashback_recovery_agent',
        'flashback.snap_987654321_987654321',
        'INSERT,UPDATE,DELETE,TRUNCATE'
    ) THEN
        RAISE EXCEPTION 'runtime payload remains writable by a delegated role';
    END IF;
    PERFORM flashback_drop_payload_table(
        'flashback.snap_987654321_987654321'::regclass
    );

    -- Compare direct delegated-role ACLs with the complete allowlist. This
    -- catches both an accidentally exposed new helper and a stale grant that
    -- survives an extension upgrade after an API is removed.
    WITH allowed(grantee, signature) AS (
        VALUES
            ('flashback_admin', 'public.flashback_track(text)'),
            ('flashback_admin', 'public.flashback_untrack(text)'),
            ('flashback_admin', 'public.flashback_restore(text,timestamp with time zone)'),
            ('flashback_admin', 'public.flashback_restore(text[],timestamp with time zone)'),
            ('flashback_admin', 'public.flashback_restore_lsn(text,pg_lsn)'),
            ('flashback_admin', 'public.flashback_restore_lsn(text[],pg_lsn)'),
            ('flashback_admin', 'public.flashback_restore_parallel(text,timestamp with time zone,integer)'),
            ('flashback_admin', 'public.flashback_recover_deleted(text,timestamp with time zone)'),
            ('flashback_admin', 'public.flashback_recover_deleted_lsn(text,pg_lsn)'),
            ('flashback_admin', 'public.flashback_checkpoint(text)'),
            ('flashback_admin', 'public.flashback_reanchor(text)'),
            ('flashback_admin', 'public.flashback_flush_staging(integer)'),
            ('flashback_admin', 'public.flashback_consume_wal(integer)'),
            ('flashback_admin', 'public.flashback_apply_retention()'),
            ('flashback_admin', 'public.flashback_query(text,timestamp with time zone,text)'),
            ('flashback_admin', 'public.flashback_query_lsn(text,pg_lsn,text)'),
            ('flashback_admin', 'public.flashback_resolve_target(text,timestamp with time zone)'),
            ('flashback_admin', 'public.flashback_health()'),
            ('flashback_admin', 'public.flashback_doctor()'),
            ('flashback_admin', 'public.flashback_disaster_points(text,interval)'),
            ('flashback_admin', 'public.flashback_recover_plan(text,interval,bigint,timestamp with time zone,pg_lsn)'),
            ('flashback_admin', 'public.flashback_recover_begin(text,text,interval,bigint,timestamp with time zone,pg_lsn)'),
            ('flashback_admin', 'public.flashback_recover_execute(text,text,interval,bigint,timestamp with time zone,pg_lsn,bigint)'),
            ('flashback_admin', 'public.flashback_recover_mark_failed(bigint,text,text,text,jsonb)'),
            ('flashback_admin', 'public.flashback_reconcile_recover_operations(interval)'),
            ('flashback_admin', 'public.flashback_operation_history(text,interval)'),
            ('flashback_admin', 'public.flashback_finalize_recover_operations()'),
            ('flashback_admin', 'public.flashback_unprotect(text)'),
            ('flashback_admin', 'public.flashback_finalize_unprotect_operations()'),
            ('flashback_admin', 'public.flashback_cleanup(bigint,boolean)'),
            ('flashback_admin', 'public.flashback_refresh_storage_summary_cache()'),
            ('flashback_admin', 'public.flashback_disk_retention_status()'),
            ('flashback_admin', 'public.flashback_slot_status_snapshot()'),
            ('flashback_admin', 'public.flashback_worker_readiness()'),
            ('flashback_admin', 'public.flashback_canonical_target_databases()'),
            ('flashback_admin', 'public.flashback_admitted_target_databases()'),
            ('flashback_admin', 'public.flashback_max_worker_pairs()'),
            ('flashback_admin', 'public.flashback_capture_worker_pid()'),
            ('flashback_admin', 'public.flashback_maintenance_worker_pid()'),
            ('flashback_admin', 'public.flashback_history(text,interval)'),
            ('flashback_admin', 'public.flashback_retention_status()'),
            ('flashback_admin', 'public.flashback_is_restore_in_progress(oid)'),
            ('flashback_admin', 'public.flashback_local_restore_preflight(regclass)'),
            ('flashback_admin', 'public.flashback_estimate_local_restore_peak_bytes(regclass)'),
            ('flashback_admin', 'public.flashback_measure_local_capacity(regclass)'),
            ('flashback_admin', 'public.flashback_admit_local_capacity(regclass,text)'),
            ('flashback_admin', 'public.flashback_advise(regclass)'),
            ('flashback_admin', 'public.flashback_config_recommend(regclass, text)'),
            ('flashback_admin', 'public.flashback_list_lifecycles()'),
            ('flashback_admin', 'public.flashback_status_snapshot(text)'),
            ('flashback_admin', 'public.flashback_relation_filesystem_available_bytes(regclass)'),
            ('flashback_admin', 'public.flashback_tablespace_filesystem_available_bytes(oid)'),
            ('flashback_admin', 'public.flashback_track_backup(text,text)'),
            ('flashback_admin', 'public.flashback_set_backup_coverage(text,pg_lsn,pg_lsn)'),
            ('flashback_admin', 'public.flashback_activate_backup_anchor(text,text,text,text,numeric,bigint,text,text,pg_lsn,pg_lsn,timestamp with time zone)'),
            ('flashback_admin', 'public.flashback_advance_backup_frontier(text,pg_lsn,bigint)'),
            ('flashback_admin', 'public.flashback_consume_verified_backup_proof(bigint)'),
            ('flashback_admin', 'public.flashback_consume_verified_wal_frontier_proof(bigint)'),
            ('flashback_admin', 'public.flashback_backup_disaster_points(text,interval)'),
            ('flashback_admin', 'public.flashback_prepare_backup_restore(text,pg_lsn)'),
            ('flashback_admin', 'public.flashback_finalize_backup_restore(text)'),
            ('flashback_admin', 'public.flashback_fail_backup_restore(text,text,boolean)'),
            ('flashback_admin', 'public.flashback_adopt_existing_payload_tables()'),
            ('flashback_recovery_agent', 'public.flashback_claim_backup_restore(text)'),
            ('flashback_recovery_agent', 'public.flashback_accept_backup_restore(text,jsonb)'),
            ('flashback_recovery_agent', 'public.flashback_fail_backup_restore(text,text,boolean)'),
            ('flashback_recovery_agent', 'public.flashback_backup_anchor_verification_context(bigint)'),
            ('flashback_recovery_agent', 'public.flashback_backup_frontier_verification_context(bigint)'),
            ('flashback_recovery_agent', 'public.flashback_active_backup_labels()'),
            ('flashback_recovery_agent', 'public.flashback_begin_backup_expire(text,text,text)'),
            ('flashback_recovery_agent', 'public.flashback_complete_backup_expire(bigint,text,text,text)'),
            ('flashback_recovery_agent', 'public.flashback_begin_backup_anchor_advancement(bigint)'),
            ('flashback_recovery_agent', 'public.flashback_retire_sealed_backup_generation(bigint)'),
            ('flashback_recovery_agent', 'public.flashback_active_backup_anchor_contexts()'),
            ('flashback_recovery_agent', 'public.flashback_freeze_missing_backup_anchor(bigint,bigint,jsonb)'),
            ('flashback_recovery_agent', 'public.flashback_freeze_backup_generation(bigint,text,jsonb)'),
            ('flashback_recovery_agent', 'public.flashback_backup_proof_result(text,bigint)'),
            ('flashback_recovery_agent', 'public.flashback_frontier_proof_result(text,bigint)'),
            ('flashback_recovery_agent', 'public.flashback_install_verified_backup_proof(text,bigint,text,text,text,text,numeric,bigint,text,text,pg_lsn,pg_lsn,timestamp with time zone,jsonb,text)'),
            ('flashback_recovery_agent', 'public.flashback_install_verified_wal_frontier_proof(text,bigint,bigint,text,text,text,bigint,pg_lsn,text,timestamp with time zone,jsonb,text)'),
            ('flashback_recovery_agent', 'public.flashback_backup_proof_attestation_payload(text,bigint,text,text,text,text,numeric,bigint,text,text,pg_lsn,pg_lsn,text,pg_lsn)'),
            ('flashback_recovery_agent', 'public.flashback_wal_frontier_attestation_payload(text,bigint,bigint,text,text,text,bigint,pg_lsn,text)'),
            ('flashback_recovery_agent', 'public.flashback_consume_verified_backup_proof(bigint)'),
            ('flashback_recovery_agent', 'public.flashback_consume_verified_wal_frontier_proof(bigint)'),
            ('pg_monitor', 'public.flashback_retention_status()'),
            ('pg_monitor', 'public.flashback_is_restore_in_progress(oid)'),
            ('pg_monitor', 'public.flashback_health()'),
            ('pg_monitor', 'public.flashback_doctor()'),
            ('pg_monitor', 'public.flashback_disaster_points(text,interval)'),
            ('pg_monitor', 'public.flashback_disk_retention_status()'),
            ('pg_monitor', 'public.flashback_advise(regclass)'),
            ('pg_monitor', 'public.flashback_slot_status_snapshot()'),
            ('pg_monitor', 'public.flashback_worker_readiness()'),
            ('pg_monitor', 'public.flashback_canonical_target_databases()'),
            ('pg_monitor', 'public.flashback_admitted_target_databases()'),
            ('pg_monitor', 'public.flashback_max_worker_pairs()'),
            ('pg_monitor', 'public.flashback_capture_worker_pid()'),
            ('pg_monitor', 'public.flashback_maintenance_worker_pid()')
    ), actual AS (
        SELECT pg_get_userbyid(routine_acl.grantee) AS grantee,
               format('%I.%I(%s)',
                   n.nspname,
                   p.proname,
                   replace(oidvectortypes(p.proargtypes), ', ', ',')) AS signature
        FROM pg_depend d
        JOIN pg_extension e
          ON e.oid = d.refobjid
         AND d.refclassid = 'pg_extension'::regclass
        JOIN pg_proc p
          ON d.classid = 'pg_proc'::regclass
         AND p.oid = d.objid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL aclexplode(p.proacl) AS routine_acl
        WHERE e.extname = 'pg_flashback'
          AND d.deptype = 'e'
          AND routine_acl.privilege_type = 'EXECUTE'
          AND routine_acl.grantee IN (
              'flashback_admin'::regrole,
              'flashback_recovery_agent'::regrole,
              'pg_monitor'::regrole
          )
    ), acl_diff AS (
        SELECT 'unexpected'::text AS issue, grantee, signature
        FROM (SELECT * FROM actual EXCEPT SELECT * FROM allowed) unexpected
        UNION ALL
        SELECT 'missing'::text AS issue, grantee, signature
        FROM (SELECT * FROM allowed EXCEPT SELECT * FROM actual) missing
    )
    SELECT string_agg(
               format('%s %s: %s', issue, grantee, signature),
               '; ' ORDER BY issue, grantee, signature
           )
      INTO v_allowlist_diff
    FROM acl_diff;

    IF v_allowlist_diff IS NOT NULL THEN
        RAISE EXCEPTION 'delegated routine ACL differs from allowlist: %',
            v_allowlist_diff;
    END IF;

    -- Positive API allowlist for the delegated administrator. Keep destructive
    -- and data-revealing entry points here so an accidental missing grant fails
    -- closed during packaging.
    FOR v_api IN
        SELECT *
        FROM (VALUES
            ('public.flashback_track(text)'),
            ('public.flashback_untrack(text)'),
            ('public.flashback_restore(text,timestamp with time zone)'),
            ('public.flashback_restore(text[],timestamp with time zone)'),
            ('public.flashback_restore_lsn(text,pg_lsn)'),
            ('public.flashback_restore_lsn(text[],pg_lsn)'),
            ('public.flashback_restore_parallel(text,timestamp with time zone,integer)'),
            ('public.flashback_recover_deleted(text,timestamp with time zone)'),
            ('public.flashback_recover_deleted_lsn(text,pg_lsn)'),
            ('public.flashback_checkpoint(text)'),
            ('public.flashback_reanchor(text)'),
            ('public.flashback_query(text,timestamp with time zone,text)'),
            ('public.flashback_query_lsn(text,pg_lsn,text)'),
            ('public.flashback_resolve_target(text,timestamp with time zone)'),
            ('public.flashback_health()'),
            ('public.flashback_advise(regclass)'),
            ('public.flashback_history(text,interval)'),
            ('public.flashback_track_backup(text,text)'),
            ('public.flashback_activate_backup_anchor(text,text,text,text,numeric,bigint,text,text,pg_lsn,pg_lsn,timestamp with time zone)'),
            ('public.flashback_prepare_backup_restore(text,pg_lsn)'),
            ('public.flashback_finalize_backup_restore(text)'),
            ('public.flashback_adopt_existing_payload_tables()')
        ) AS expected(signature)
    LOOP
        IF NOT has_function_privilege(
            'flashback_admin', v_api.signature, 'EXECUTE'
        ) THEN
            RAISE EXCEPTION 'flashback_admin lacks EXECUTE on %', v_api.signature;
        END IF;
    END LOOP;

    IF NOT has_function_privilege(
        'flashback_recovery_agent',
        'public.flashback_claim_backup_restore(text)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'flashback_recovery_agent',
        'public.flashback_accept_backup_restore(text,jsonb)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'flashback_recovery_agent lacks its helper API allowlist';
    END IF;

    IF has_function_privilege(
        'flashback_admin',
        'public.flashback_set_restore_in_progress(boolean)',
        'EXECUTE'
    ) OR has_function_privilege(
        'flashback_admin',
        'public.flashback_attach_capture_trigger(text,text)',
        'EXECUTE'
    ) OR has_function_privilege(
        'flashback_admin',
        'public.flashback_detach_capture_trigger(text,text)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION
            'flashback_admin can execute capture-bypass internals';
    END IF;

    IF has_function_privilege(
        'flashback_recovery_agent',
        'public.flashback_restore(text,timestamp with time zone)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'flashback_recovery_agent can execute production restore';
    END IF;

    -- pg_monitor must NOT execute payload-bearing flashback_history().
    IF has_function_privilege(
        'pg_monitor', 'public.flashback_history(text,interval)', 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'pg_monitor must not execute flashback_history (row payloads)';
    END IF;
    IF has_function_privilege(
        'pg_monitor',
        'public.flashback_restore(text,timestamp with time zone)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'pg_monitor routine allowlist is incorrect';
    END IF;

    -- Mutating public APIs execute as the extension owner; the ACL checks above
    -- are meaningful only if these core entry points retain SECURITY DEFINER.
    IF EXISTS (
        SELECT expected.proname
        FROM (VALUES
            ('flashback_track'),
            ('flashback_checkpoint'),
            ('flashback_reanchor'),
            ('flashback_restore'),
            ('flashback_restore_lsn'),
            ('flashback_restore_parallel'),
            ('flashback_recover_deleted'),
            ('flashback_recover_deleted_lsn'),
            ('flashback_recover_execute'),
            ('flashback_operation_begin'),
            ('flashback_finalize_recover_operations'),
            ('flashback_unprotect'),
            ('flashback_cleanup')
        ) AS expected(proname)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
              AND p.proname = expected.proname
              AND p.prosecdef
        )
    ) THEN
        RAISE EXCEPTION 'a mutating flashback API lost SECURITY DEFINER';
    END IF;
END;
$tv$;

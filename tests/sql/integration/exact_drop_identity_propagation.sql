-- Exact DROP identity must propagate lock_phase -> restore core into
-- expected_proof.binding / manifest selection (never "latest for tracking").
--
-- pg_test runs the whole script in one transaction, so distinct synthetic
-- source_xid values are required to bind two DROP manifests unambiguously.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tid bigint;
    v_boundary pg_lsn;
    v_xid bigint;
    v_drop1_id bigint;
    v_drop2_id bigint;
    v_audit_op bigint;
    v_locked RECORD;
    v_proof jsonb;
    v_binding jsonb;
    v_manifest jsonb;
    v_notes text[];
    v_dml_op bigint;
    v_dml_proof jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_exact_drop_id CASCADE;
    DROP FUNCTION IF EXISTS public.it_exact_drop_id_trg_fn() CASCADE;

    CREATE FUNCTION public.it_exact_drop_id_trg_fn()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;

    CREATE TABLE public.it_exact_drop_id(
        id int PRIMARY KEY,
        note text NOT NULL
    );
    INSERT INTO public.it_exact_drop_id VALUES (1, 'drop1-marker');

    v_boot := public.flashback_test_bootstrap_lifecycle('public.it_exact_drop_id');
    v_tid := (v_boot->>'tracking_id')::bigint;
    v_boundary := (v_boot->>'boundary_lsn')::pg_lsn;

    -- DROP #1: distinctive supported trigger in dependency manifest.
    CREATE TRIGGER it_exact_drop_id_drop1_trg
        BEFORE INSERT ON public.it_exact_drop_id
        FOR EACH ROW EXECUTE FUNCTION public.it_exact_drop_id_trg_fn();

    PERFORM public.flashback_capture_drop_dependency_manifest(
        'public', 'it_exact_drop_id', false
    );
    v_xid := 91001;
    UPDATE flashback.drop_dependency_manifests
       SET source_xid = v_xid
     WHERE tracking_id = v_tid
       AND disaster_event_id IS NULL;
    DROP TABLE public.it_exact_drop_id CASCADE;
    PERFORM public.flashback_test_inject_ddl_commit(
        v_tid,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM public.flashback_bind_drop_dependency_manifests();

    SELECT disaster_event_id INTO v_drop1_id
    FROM flashback.drop_dependency_manifests
    WHERE tracking_id = v_tid
      AND disaster_event_id IS NOT NULL
    ORDER BY disaster_event_id ASC
    LIMIT 1;
    IF v_drop1_id IS NULL THEN
        RAISE EXCEPTION 'exact_drop_identity: DROP#1 manifest not bound';
    END IF;

    -- Restore + resolve so a second DROP can land on the same tracking_id.
    PERFORM public.flashback_test_restore_lsn('public.it_exact_drop_id', v_boundary);
    PERFORM public.flashback_test_resolve_post_restore_boundary(v_tid, '0/2500'::pg_lsn);

    TRUNCATE public.it_exact_drop_id;
    INSERT INTO public.it_exact_drop_id VALUES (1, 'drop2-marker');
    PERFORM public.flashback_test_inject_commit(
        v_tid,
        '0/2800'::pg_lsn,
        clock_timestamp(),
        91002,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"note":"drop2-marker"}'::jsonb)
        )
    );

    CREATE TRIGGER it_exact_drop_id_drop2_trg
        BEFORE INSERT ON public.it_exact_drop_id
        FOR EACH ROW EXECUTE FUNCTION public.it_exact_drop_id_trg_fn();

    PERFORM public.flashback_capture_drop_dependency_manifest(
        'public', 'it_exact_drop_id', false
    );
    v_xid := 91003;
    UPDATE flashback.drop_dependency_manifests
       SET source_xid = v_xid
     WHERE tracking_id = v_tid
       AND disaster_event_id IS NULL;
    DROP TABLE public.it_exact_drop_id CASCADE;
    PERFORM public.flashback_test_inject_ddl_commit(
        v_tid,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM public.flashback_bind_drop_dependency_manifests();

    SELECT disaster_event_id INTO v_drop2_id
    FROM flashback.drop_dependency_manifests
    WHERE tracking_id = v_tid
      AND disaster_event_id IS NOT NULL
      AND disaster_event_id <> v_drop1_id
    ORDER BY disaster_event_id DESC
    LIMIT 1;

    IF v_drop2_id IS NULL OR v_drop2_id <= v_drop1_id THEN
        RAISE EXCEPTION
            'exact_drop_identity: expected distinct later DROP#2 id (got %, earlier %)',
            v_drop2_id, v_drop1_id;
    END IF;
    IF (SELECT count(*) FROM flashback.drop_dependency_manifests
        WHERE tracking_id = v_tid AND disaster_event_id IS NOT NULL) < 2
    THEN
        RAISE EXCEPTION 'exact_drop_identity: expected two bound DROP manifests';
    END IF;

    -- Audited recover selection of DROP#1 while DROP#2 is the latest identity.
    v_audit_op := public.flashback_operation_begin(
        'recover',
        'public.it_exact_drop_id',
        v_tid,
        1,
        'exact-drop-id-token',
        v_drop1_id,
        NULL,
        v_boundary,
        jsonb_build_object('test', 'exact_drop_identity_propagation')
    );
    PERFORM public.flashback_internal_set_audited_recover_context(v_audit_op);

    SELECT * INTO v_locked
    FROM public.flashback_restore_lsn_lock_phase('public.it_exact_drop_id', v_boundary);

    IF v_locked.out_disaster_event_id IS DISTINCT FROM v_drop1_id THEN
        RAISE EXCEPTION
            'exact_drop_identity: lock_phase returned % expected audited DROP#1 % (not latest DROP#2 %)',
            v_locked.out_disaster_event_id, v_drop1_id, v_drop2_id;
    END IF;

    PERFORM public.flashback_test_restore_lsn('public.it_exact_drop_id', v_boundary);

    SELECT payload->'expected_proof' INTO v_proof
    FROM flashback.operation_events
    WHERE operation_id = v_audit_op
      AND event_type = 'applied_coverage_pending'
    ORDER BY event_id DESC
    LIMIT 1;

    IF v_proof IS NULL OR v_proof = 'null'::jsonb THEN
        RAISE EXCEPTION 'exact_drop_identity: expected_proof missing on audited recover op %',
            v_audit_op;
    END IF;

    v_binding := v_proof->'binding';
    IF COALESCE(v_binding->>'disaster_event_id', '') <> v_drop1_id::text THEN
        RAISE EXCEPTION
            'exact_drop_identity: expected_proof.binding.disaster_event_id=% want DROP#1 % (must not use DROP#2 %)',
            v_binding->>'disaster_event_id', v_drop1_id, v_drop2_id;
    END IF;

    v_manifest := v_proof->'manifest';
    IF COALESCE(v_manifest->>'disaster_event_id', '') <> v_drop1_id::text THEN
        RAISE EXCEPTION
            'exact_drop_identity: proof.manifest.disaster_event_id=% want DROP#1 % (DROP#2 manifest must not be selected)',
            v_manifest->>'disaster_event_id', v_drop1_id;
    END IF;
    IF COALESCE(v_manifest->'manifest'->'triggers', '[]'::jsonb)
       @> '[{"name":"it_exact_drop_id_drop2_trg"}]'::jsonb
    THEN
        RAISE EXCEPTION 'exact_drop_identity: DROP#2 trigger present in selected manifest';
    END IF;
    IF NOT (COALESCE(v_manifest->'manifest'->'triggers', '[]'::jsonb)
            @> '[{"name":"it_exact_drop_id_drop1_trg"}]'::jsonb)
    THEN
        RAISE EXCEPTION 'exact_drop_identity: DROP#1 trigger missing from selected manifest: %',
            v_manifest->'manifest'->'triggers';
    END IF;

    IF to_regclass('public.it_exact_drop_id') IS NULL THEN
        RAISE EXCEPTION 'exact_drop_identity: restored relation missing';
    END IF;

    SELECT array_agg(note ORDER BY id) INTO v_notes
    FROM public.it_exact_drop_id;
    IF v_notes IS DISTINCT FROM ARRAY['drop1-marker'] THEN
        RAISE EXCEPTION 'exact_drop_identity: audited DROP#1 restore data mismatch: %', v_notes;
    END IF;

    -- Live DML restore path: disaster_event_id must stay NULL in proof binding.
    PERFORM public.flashback_test_resolve_post_restore_boundary(v_tid, '0/4000'::pg_lsn);
    INSERT INTO public.it_exact_drop_id VALUES (2, 'live-dml');
    PERFORM public.flashback_test_inject_commit(
        v_tid,
        '0/5000'::pg_lsn,
        clock_timestamp(),
        91004,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"note":"live-dml"}'::jsonb)
        )
    );

    v_dml_op := public.flashback_operation_begin(
        'recover',
        'public.it_exact_drop_id',
        v_tid,
        1,
        'exact-drop-id-dml-token',
        NULL,  -- live DML: no disaster identity
        NULL,
        '0/5000'::pg_lsn,
        jsonb_build_object('test', 'exact_drop_identity_dml')
    );
    PERFORM public.flashback_internal_set_audited_recover_context(v_dml_op);
    PERFORM public.flashback_test_restore_lsn('public.it_exact_drop_id', '0/5000'::pg_lsn);

    SELECT payload->'expected_proof' INTO v_dml_proof
    FROM flashback.operation_events
    WHERE operation_id = v_dml_op
      AND event_type = 'applied_coverage_pending'
    ORDER BY event_id DESC
    LIMIT 1;

    IF v_dml_proof IS NULL OR v_dml_proof = 'null'::jsonb THEN
        RAISE EXCEPTION 'exact_drop_identity: DML expected_proof missing';
    END IF;
    IF v_dml_proof->'binding' ? 'disaster_event_id'
       AND jsonb_typeof(v_dml_proof->'binding'->'disaster_event_id') <> 'null'
       AND NULLIF(v_dml_proof->'binding'->>'disaster_event_id', '') IS NOT NULL
    THEN
        RAISE EXCEPTION
            'exact_drop_identity: live DML restore must keep disaster_event_id NULL, got %',
            v_dml_proof->'binding'->'disaster_event_id';
    END IF;

    SELECT array_agg(note ORDER BY id) INTO v_notes
    FROM public.it_exact_drop_id;
    IF NOT ('live-dml' = ANY (v_notes)) OR ('drop2-marker' = ANY (v_notes)) THEN
        RAISE EXCEPTION 'exact_drop_identity: post-DML restore data mismatch: %', v_notes;
    END IF;

    -- RBAC: new 4-arg core signature must not be EXECUTE-able by PUBLIC/admin/monitor.
    IF has_function_privilege(
        'public',
        'public.flashback_internal_restore_lsn_core(text,pg_lsn,bigint,bigint)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'exact_drop_identity: PUBLIC EXECUTE not revoked on new core signature';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin',
            'public.flashback_internal_restore_lsn_core(text,pg_lsn,bigint,bigint)',
            'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'exact_drop_identity: flashback_admin EXECUTE not revoked on new core signature';
    END IF;
    IF has_function_privilege(
        'pg_monitor',
        'public.flashback_internal_restore_lsn_core(text,pg_lsn,bigint,bigint)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'exact_drop_identity: pg_monitor EXECUTE not revoked on new core signature';
    END IF;

    DROP TABLE IF EXISTS public.it_exact_drop_id CASCADE;
    DROP FUNCTION IF EXISTS public.it_exact_drop_id_trg_fn() CASCADE;
END;
$tv$;

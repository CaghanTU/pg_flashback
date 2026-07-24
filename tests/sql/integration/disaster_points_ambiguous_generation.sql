-- Forced ambiguous/missing coverage generation for a disaster event must be
-- non_restorable (never LIMIT 1 hope on generation_no).
-- adversarial-fixture: two overlapping eligible generations plus a DROP row
-- whose generation_id does not uniquely resolve (binding shape forbids
-- tracking_id with NULL generation_id).

DROP TABLE IF EXISTS public.amb_drop CASCADE;
CREATE TABLE public.amb_drop(id int PRIMARY KEY, v text);

DO $$
DECLARE
    v_boot jsonb;
    v_tid bigint;
    v_gid bigint;
    v_stream bigint;
    v_boundary pg_lsn;
    v_oid oid;
    v_lsn pg_lsn := '0/5000'::pg_lsn;
    v_snapshot_id bigint;
    r record;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.amb_drop') INTO v_boot;
    v_tid := (v_boot->>'tracking_id')::bigint;
    v_gid := (v_boot->>'generation_id')::bigint;
    v_stream := (v_boot->>'stream_id')::bigint;
    v_boundary := (v_boot->>'boundary_lsn')::pg_lsn;
    v_oid := (v_boot->>'rel_oid')::oid;
    v_snapshot_id := (v_boot->>'snapshot_id')::bigint;

    IF v_gid IS NULL THEN
        RAISE EXCEPTION 'test setup: no active generation';
    END IF;

    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           sealed_at = clock_timestamp(),
           superseded_before_lsn = '0/6000'::pg_lsn,
           superseded_before_time = clock_timestamp()
     WHERE generation_id = v_gid;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_oid, v_tid, format('flashback.base_snapshot_t%s_adv', v_tid::text),
        v_boundary, '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, stream_id, generation_no, state, recovery_profile,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_lsn, boundary_time,
        valid_through_lsn, valid_through_time, activated_at, details
    ) VALUES (
        v_tid,
        v_stream,
        2,
        'active',
        'local_delta',
        'adversarial_fixture',
        v_oid,
        v_snapshot_id,
        v_boundary,
        clock_timestamp(),
        v_boundary,
        clock_timestamp(),
        clock_timestamp(),
        jsonb_build_object('adversarial_fixture', true)
    );

    INSERT INTO public.amb_drop VALUES (1, 'a');

    -- adversarial-fixture: DROP bound to a non-existent generation_id so
    -- disaster_points cannot uniquely resolve among overlapping gens.
    INSERT INTO flashback.delta_log (
        tracking_id, rel_oid, table_name, event_type, event_time,
        commit_lsn, committed_at, generation_id, stream_id, schema_version
    ) VALUES (
        v_tid, v_oid, 'public.amb_drop', 'DROP', clock_timestamp(),
        v_lsn, clock_timestamp(), 9223372036854775807, v_stream, 1
    );

    SELECT * INTO r
    FROM flashback_disaster_points('public.amb_drop', interval '1 day')
    WHERE event_type = 'DROP'
    ORDER BY disaster_commit_lsn DESC
    LIMIT 1;

    IF r.status = 'restorable' THEN
        RAISE EXCEPTION 'ambiguous generation was restorable';
    END IF;
    IF r.reason IS NULL OR r.reason NOT ILIKE '%ambiguous%' THEN
        RAISE EXCEPTION 'expected ambiguous reason, got %', r.reason;
    END IF;
END $$;

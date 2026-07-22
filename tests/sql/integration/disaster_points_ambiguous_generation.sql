-- Forced ambiguous coverage generation for a disaster event must be
-- non_restorable (never LIMIT 1 hope on generation_no).
SELECT flashback_test_attach_capture_trigger('public', 'amb_drop');

CREATE TABLE public.amb_drop(id int PRIMARY KEY, v text);
SELECT flashback_track('public.amb_drop');

-- Seed a synthetic second eligible sealed generation overlapping the first
-- so disaster_points cannot uniquely resolve when generation_id is null.
DO $$
DECLARE
    v_tid bigint;
    v_gid bigint;
    v_stream bigint;
    v_boundary pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables
     WHERE table_name = 'amb_drop' ORDER BY tracked_since DESC LIMIT 1;
    SELECT generation_id, stream_id, boundary_lsn
      INTO v_gid, v_stream, v_boundary
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tid AND state = 'active'
    LIMIT 1;

    IF v_gid IS NULL THEN
        RAISE EXCEPTION 'test setup: no active generation';
    END IF;

    INSERT INTO flashback.coverage_generations (
        tracking_id, stream_id, generation_no, state, recovery_profile,
        boundary_lsn, boundary_time, schema_version
    )
    SELECT
        v_tid,
        v_stream,
        COALESCE(MAX(generation_no), 0) + 1,
        'sealed',
        'local_delta',
        v_boundary,
        clock_timestamp(),
        1
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tid;
END $$;

INSERT INTO public.amb_drop VALUES (1, 'a');
-- Insert a DROP-like delta_log row with NULL generation_id so the ambiguous
-- eligibility OR-branch is exercised (synthetic; exact-WAL suite covers real DROP).
DO $$
DECLARE
    v_tid bigint;
    v_oid oid;
    v_lsn pg_lsn := pg_current_wal_lsn();
BEGIN
    SELECT tracking_id, rel_oid INTO v_tid, v_oid
    FROM flashback.tracked_tables WHERE table_name = 'amb_drop' LIMIT 1;
    INSERT INTO flashback.delta_log (
        tracking_id, rel_oid, table_name, event_type, event_time,
        commit_lsn, committed_at, generation_id
    ) VALUES (
        v_tid, v_oid, 'public.amb_drop', 'DROP', clock_timestamp(),
        v_lsn, clock_timestamp(), NULL
    );
END $$;

DO $$
DECLARE
    r record;
BEGIN
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

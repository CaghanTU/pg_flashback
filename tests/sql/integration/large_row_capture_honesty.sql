-- A5: pg_flashback.max_row_size is documented as "rows larger than this (in
-- bytes) are skipped during capture to prevent OOM." Silently skipping a
-- row's payload while still applying its DML would leave a delta_log/
-- pending_wal_events row with no old/new data -- indistinguishable from a
-- genuine event once inserted, and unrecoverable without ever raising.
-- fb_decode_change (src/capture/wal_decoder.rs) cannot itself be exercised
-- under pg_test (it requires a real physical replication slot, impossible
-- inside pg_test's single-transaction-per-test model -- see other tests'
-- comments for the same limitation). This test instead proves the SQL-side
-- half of the contract directly: flashback_apply_decoded_wal_batch must
-- detect the "oversized" marker the decoder emits in place of a real
-- payload and freeze the stream/open a durable gap, exactly like it already
-- does for a missing COMMIT record, rather than silently promoting a
-- data-less delta. The decoder side (oversized detection + marker shape) is
-- validated live against the persistent dev instance.
DO $setup$
DECLARE
    v_boot jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_a5_oversized CASCADE;
    CREATE TABLE public.it_a5_oversized (id int PRIMARY KEY, v text);
    INSERT INTO public.it_a5_oversized VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_a5_oversized');
END;
$setup$;

DO $oversized_scenario$
DECLARE
    v_tid bigint;
    v_gen record;
    v_xid bigint;
    v_commit_lsn CONSTANT pg_lsn := '0/9800'::pg_lsn;
    v_inserted bigint;
BEGIN
    SELECT tracking_id INTO v_tid
    FROM flashback.tracked_tables WHERE table_name = 'it_a5_oversized';

    SELECT generation_id, stream_id, rel_oid_at_boundary INTO v_gen
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tid AND state = 'active';

    v_xid := (txid_current() % 4294967296)::bigint;

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    -- Mirrors exactly what fb_decode_change emits for a row whose captured
    -- old+new JSON exceeds pg_flashback.max_row_size: 'op'/'oid'/'xid' are
    -- present (so the metadata-only preflight/full-pass consistency check
    -- in flashback_consume_wal still agrees on shape) but 'old'/'new' are
    -- replaced by the 'oversized' marker.
    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord) VALUES
    (v_commit_lsn, v_xid, jsonb_build_object(
        'op', 'UPDATE', 'schema', 'public', 'table', 'it_a5_oversized',
        'oid', v_gen.rel_oid_at_boundary, 'xid', v_xid,
        'oversized', true, 'captured_len', 999999
    ), 1),
    (v_commit_lsn, v_xid, jsonb_build_object(
        'commit', v_xid, 'lsn', v_commit_lsn::text,
        'commit_time', (
            EXTRACT(EPOCH FROM (clock_timestamp() - TIMESTAMPTZ '2000-01-01 00:00:00+00'))
            * 1000000
        )::bigint
    ), 2);

    PERFORM flashback_internal_lock_lifecycle(v_tid);
    v_inserted := flashback_apply_decoded_wal_batch(v_gen.stream_id, NULL, NULL);

    IF v_inserted <> 0 THEN
        RAISE EXCEPTION 'large_row_capture_honesty: oversized-row batch was applied instead of frozen (rows_inserted=%)',
            v_inserted;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.capture_streams
        WHERE stream_id = v_gen.stream_id
          AND state = 'broken'
          AND invalidation_reason = 'row_exceeds_max_row_size'
    ) THEN
        RAISE EXCEPTION 'large_row_capture_honesty: oversized row did not freeze the capture stream';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.delta_log
        WHERE tracking_id = v_tid
          AND event_type = 'UPDATE'
          AND commit_lsn = v_commit_lsn
    ) THEN
        RAISE EXCEPTION 'large_row_capture_honesty: oversized row was silently promoted into delta_log with no row data';
    END IF;
END;
$oversized_scenario$;

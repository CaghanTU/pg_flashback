-- flashback_cleanup's payload-retirement loop was migrated to SnapshotStore
-- (flashback_internal_snapshot_retire) in the same change that introduced
-- storage_backend/locator. Before that change it issued one blanket
-- `UPDATE flashback.snapshots SET payload_state = 'retired' WHERE
-- tracking_id = ...` across every row regardless of current state; the new
-- terminal-state-immutability guard trigger would reject that blanket
-- UPDATE the instant a tracking_id has any already-retired snapshot. This
-- proves cleanup only ever retires the rows that are actually `available`
-- and leaves an already-terminal row untouched rather than raising.
DROP TABLE IF EXISTS public.it_cleanup_snapshot_store CASCADE;

CREATE TABLE public.it_cleanup_snapshot_store (id integer PRIMARY KEY, note text);
INSERT INTO public.it_cleanup_snapshot_store VALUES (1, 'v0');

DO $test$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_rel_oid oid;
    v_boundary_snapshot_id bigint;
    v_extra_snapshot_id bigint;
    v_extra_relid regclass;
    v_dry jsonb;
    v_result jsonb;
    v_remaining_available integer;
    v_terminal_state text;
BEGIN
    v_rel_oid := 'public.it_cleanup_snapshot_store'::regclass;

    SELECT flashback_test_bootstrap_lifecycle('public.it_cleanup_snapshot_store') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    SELECT boundary_snapshot_id INTO v_boundary_snapshot_id
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id AND state = 'active';
    IF v_boundary_snapshot_id IS NULL THEN
        RAISE EXCEPTION 'test setup invariant broken: no active generation boundary snapshot';
    END IF;

    -- Manufacture a second available artifact under the same tracking_id
    -- (what a reanchor or restore successor would leave behind), then
    -- retire it up front so cleanup must skip an already-terminal row.
    v_extra_snapshot_id := flashback_internal_snapshot_create(
        v_tracking_id, v_rel_oid, 'public', 'it_cleanup_snapshot_store',
        pg_current_wal_insert_lsn(), 'generation'
    );
    PERFORM flashback_internal_snapshot_retire(v_extra_snapshot_id, v_tracking_id, 'retired');

    SELECT payload_state INTO v_terminal_state
    FROM flashback.snapshots WHERE snapshot_id = v_extra_snapshot_id;
    IF v_terminal_state <> 'retired' THEN
        RAISE EXCEPTION 'test setup invariant broken: extra snapshot not retired';
    END IF;

    -- Seal the lifecycle unprotected directly (bypassing the async stop
    -- marker/decoder mechanics, which are outside this test's scope) so
    -- flashback_cleanup's precondition is satisfied.
    UPDATE flashback.tracked_tables
       SET is_active = false, protection_state = 'unprotected'
     WHERE tracking_id = v_tracking_id;

    v_dry := flashback_cleanup(v_tracking_id, true);
    IF v_dry->>'status' <> 'dry_run' THEN
        RAISE EXCEPTION 'dry run did not report dry_run status: %', v_dry;
    END IF;
    IF (SELECT payload_state FROM flashback.snapshots WHERE snapshot_id = v_boundary_snapshot_id)
        <> 'available'
    THEN
        RAISE EXCEPTION 'dry run mutated the boundary snapshot';
    END IF;

    v_result := flashback_cleanup(v_tracking_id, false);
    IF v_result->>'status' <> 'cleaned' THEN
        RAISE EXCEPTION 'cleanup did not report cleaned status: %', v_result;
    END IF;

    IF (SELECT payload_state FROM flashback.snapshots WHERE snapshot_id = v_boundary_snapshot_id)
        <> 'retired'
    THEN
        RAISE EXCEPTION 'boundary snapshot was not retired by cleanup';
    END IF;
    IF (SELECT payload_state FROM flashback.snapshots WHERE snapshot_id = v_extra_snapshot_id)
        <> 'retired'
    THEN
        RAISE EXCEPTION 'pre-retired snapshot was mutated away from retired by cleanup';
    END IF;

    SELECT count(*) INTO v_remaining_available
    FROM flashback.snapshots
    WHERE tracking_id = v_tracking_id AND payload_state = 'available';
    IF v_remaining_available <> 0 THEN
        RAISE EXCEPTION 'cleanup left % available snapshot(s) behind', v_remaining_available;
    END IF;

    IF EXISTS (SELECT 1 FROM flashback.delta_log WHERE tracking_id = v_tracking_id) THEN
        RAISE EXCEPTION 'cleanup left delta_log rows behind';
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.schema_versions WHERE tracking_id = v_tracking_id) THEN
        RAISE EXCEPTION 'cleanup left schema_versions rows behind';
    END IF;

    -- Retire is a physical DROP: the payload relation for the
    -- freshly-retired boundary snapshot must actually be gone.
    SELECT payload_relid INTO v_extra_relid
    FROM flashback_internal_snapshot_resolve(v_boundary_snapshot_id, v_tracking_id);
    IF v_extra_relid IS NOT NULL THEN
        RAISE EXCEPTION 'cleanup retired the boundary snapshot state but left its payload relation behind';
    END IF;
END;
$test$;

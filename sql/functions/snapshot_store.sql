-- =================================================================
-- SnapshotStore: internal, unstable boundary around snapshot artifacts.
--
-- Not a public extension API and not a stable provider ABI. Every
-- production call site that needs to create, resolve, materialize,
-- measure, or retire a `local_delta` snapshot payload must go through
-- exactly one of the primitives below instead of touching
-- flashback.snapshots.snapshot_table / to_regclass() / CTAS / DROP
-- directly. This exists so that a future non-heap backend can be added
-- without re-splitting restore/retention/health code a second time; no
-- such backend is written in this step. The only backend implemented
-- here is `heap_v1`, whose physical behavior is byte-for-byte the same
-- CTAS-based snapshot this codebase already used before this file
-- existed (see git history of lifecycle_bootstrap_core.sql,
-- coverage_runtime.sql and restore_lsn.sql for the code this replaces).
--
-- All functions here are SECURITY DEFINER with
-- SET search_path = pg_catalog, flashback, pg_temp, and are REVOKEd from
-- PUBLIC, flashback_admin and pg_monitor at the bottom of this file.
-- None of them accept or execute caller-supplied free SQL; every
-- identifier is either resolved from flashback.tracked_tables /
-- flashback.snapshots catalog rows or built with %I.
-- =================================================================

-- ------------------------------------------------------------------
-- Single mutation authority for flashback.snapshots.payload_state.
-- Mirrors state_authority.sql's CAS shape: FOR UPDATE, expected-state
-- check, WHERE-clause CAS, ROW_COUNT verification. The guard trigger in
-- schema_bootstrap.sql (flashback_guard_snapshot_artifact) is the last
-- line of defense; this function is the only intended caller of the
-- state-mutating UPDATE.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_transition(
    p_snapshot_id bigint,
    p_tracking_id bigint,
    p_expected_states text[],
    p_new_state text,
    p_storage_backend text DEFAULT NULL,
    p_locator jsonb DEFAULT NULL,
    p_snapshot_table text DEFAULT NULL,
    p_row_count bigint DEFAULT NULL,
    p_schema_def jsonb DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.snapshots%ROWTYPE;
    v_n integer;
BEGIN
    IF p_snapshot_id IS NULL OR p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: snapshot transition requires snapshot_id and tracking_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_new_state IS NULL OR p_expected_states IS NULL OR cardinality(p_expected_states) = 0 THEN
        RAISE EXCEPTION 'pg_flashback: snapshot transition requires new state and expected state(s)'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown snapshot artifact % (tracking %)',
            p_snapshot_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.payload_state = p_new_state THEN
        RETURN false;
    END IF;

    IF NOT (v_row.payload_state = ANY (p_expected_states)) THEN
        RAISE EXCEPTION
            'pg_flashback: snapshot artifact % CAS failed: have state %, expected one of %, wanted %',
            p_snapshot_id, v_row.payload_state, p_expected_states, p_new_state
            USING ERRCODE = 'serialization_failure';
    END IF;

    UPDATE flashback.snapshots
       SET payload_state = p_new_state,
           storage_backend = CASE WHEN v_row.payload_state = 'creating'
                                   THEN COALESCE(p_storage_backend, storage_backend)
                                   ELSE storage_backend END,
           locator = CASE WHEN v_row.payload_state = 'creating'
                          THEN COALESCE(p_locator, locator)
                          ELSE locator END,
           snapshot_table = CASE WHEN v_row.payload_state = 'creating'
                                  THEN COALESCE(p_snapshot_table, snapshot_table)
                                  ELSE snapshot_table END,
           row_count = CASE WHEN v_row.payload_state = 'creating'
                             THEN COALESCE(p_row_count, row_count)
                             ELSE row_count END,
           schema_def = CASE WHEN v_row.payload_state = 'creating'
                              THEN COALESCE(p_schema_def, schema_def)
                              ELSE schema_def END,
           available_at = CASE WHEN p_new_state = 'available'
                                THEN COALESCE(available_at, clock_timestamp())
                                ELSE available_at END,
           retired_at = CASE WHEN p_new_state IN ('retired', 'missing', 'aborted')
                             THEN COALESCE(retired_at, clock_timestamp())
                             ELSE retired_at END
     WHERE snapshot_id = p_snapshot_id
       AND tracking_id IS NOT DISTINCT FROM p_tracking_id
       AND payload_state = v_row.payload_state;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % transition raced (expected exactly 1 row)',
            p_snapshot_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- create: catalog reservation -> heap CTAS -> extension ownership ->
-- row count/schema metadata -> available finalize, all inside the
-- caller's transaction (CTAS stays atomic; no cross-transaction backend
-- is written in this step). Preserves each existing call site's exact
-- physical naming convention via p_naming_style:
--   'initial_track' -> base_snapshot_t<tracking_id>   (first track only)
--   'generation'     -> snap_<tracking_id>_<snapshot_id> (reanchor and
--                        post-restore successor)
-- The caller must already hold the relevant lifecycle lock and must
-- have already verified p_source_schema.p_source_table is still
-- p_rel_oid under that lock; this function does not re-take locks so
-- it does not change existing lock ordering/behavior.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_create(
    p_tracking_id bigint,
    p_rel_oid oid,
    p_source_schema text,
    p_source_table text,
    p_snapshot_lsn pg_lsn,
    p_naming_style text DEFAULT 'generation',
    p_schema_def_override jsonb DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_snapshot_id bigint;
    v_relation_name text;
    v_schema_def jsonb;
    v_row_count bigint;
BEGIN
    IF p_tracking_id IS NULL OR p_rel_oid IS NULL
       OR p_source_schema IS NULL OR p_source_table IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: snapshot create requires tracking_id, rel_oid, source schema/table'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_naming_style NOT IN ('initial_track', 'generation') THEN
        RAISE EXCEPTION 'pg_flashback: unknown snapshot naming style %', p_naming_style
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- The post-restore successor path (restore_lsn.sql) passes an override:
    -- the schema_def it already resolved for the *historical target LSN*
    -- (merged with the boundary snapshot's owner/ACL), which is not always
    -- identical to a fresh catalog introspection of the just-swapped-in
    -- relation. Every other caller leaves this NULL and gets the same
    -- self-computed value this function always used.
    v_schema_def := COALESCE(
        p_schema_def_override,
        public.flashback_collect_schema_def(p_rel_oid),
        '{}'::jsonb
    );

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        p_rel_oid, p_tracking_id, '', p_snapshot_lsn,
        v_schema_def, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snapshot_id;

    v_relation_name := CASE p_naming_style
        WHEN 'initial_track' THEN format('base_snapshot_t%s', p_tracking_id)
        ELSE format('snap_%s_%s', p_tracking_id, v_snapshot_id)
    END;

    -- Defensive: a stale relation at this exact reserved name (upgrade/
    -- retry leftover) must never be silently reused as if it were this
    -- artifact's payload.
    PERFORM public.flashback_drop_payload_table(
        to_regclass(format('flashback.%I', v_relation_name))
    );
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I',
                   v_relation_name, p_source_schema, p_source_table);
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_relation_name))
    );
    EXECUTE format('SELECT count(*) FROM flashback.%I', v_relation_name)
      INTO v_row_count;

    PERFORM public.flashback_internal_snapshot_transition(
        v_snapshot_id,
        p_tracking_id,
        ARRAY['creating'],
        'available',
        'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', v_relation_name),
        format('flashback.%I', v_relation_name),
        v_row_count,
        NULL
    );

    RETURN v_snapshot_id;
END;
$$;

-- Defensive/forward-compatible: mark a `creating` artifact as permanently
-- failed instead of leaving it stuck. No current call site needs this
-- (CTAS is atomic within one transaction, so a rollback already discards
-- the whole row) but the state machine supports it for a future
-- non-transactional backend; covered by a direct regression test.
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_abort(
    p_snapshot_id bigint,
    p_tracking_id bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
    SELECT public.flashback_internal_snapshot_transition(
        p_snapshot_id, p_tracking_id, ARRAY['creating'], 'aborted'
    );
$$;

-- ------------------------------------------------------------------
-- resolve: exact snapshot_id + tracking_id lookup. Never raises for a
-- missing/non-available row; returns zero rows instead so callers can
-- decide whether that is expected (e.g. probing before require_available).
-- payload_relid is computed from the structured locator, never by
-- parsing snapshot_table.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_resolve(
    p_snapshot_id bigint,
    p_tracking_id bigint
)
RETURNS TABLE (
    snapshot_id bigint,
    tracking_id bigint,
    rel_oid oid,
    payload_state text,
    storage_backend text,
    locator jsonb,
    payload_relid regclass,
    row_count bigint,
    schema_def jsonb,
    snapshot_lsn pg_lsn,
    captured_at timestamptz,
    available_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
BEGIN
    IF p_snapshot_id IS NULL OR p_tracking_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        s.snapshot_id, s.tracking_id, s.rel_oid, s.payload_state,
        s.storage_backend, s.locator,
        CASE
            WHEN s.storage_backend = 'heap_v1'
                 AND s.locator ? 'schema' AND s.locator ? 'relation'
            THEN to_regclass(format('%I.%I',
                     s.locator->>'schema', s.locator->>'relation'))
            ELSE NULL
        END,
        s.row_count, s.schema_def, s.snapshot_lsn, s.captured_at, s.available_at
    FROM flashback.snapshots s
    WHERE s.snapshot_id = p_snapshot_id
      AND s.tracking_id = p_tracking_id;
END;
$$;

-- require_available: resolve + fail-closed validation. This is the only
-- sanctioned replacement for the
--   payload_state = 'available' AND to_regclass(snapshot_table) IS NOT NULL
--   AND flashback_payload_is_owned(to_regclass(snapshot_table))
-- pattern that used to be duplicated at every admission/retention/health
-- call site.
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_require_available(
    p_snapshot_id bigint,
    p_tracking_id bigint
)
RETURNS TABLE (
    snapshot_id bigint,
    tracking_id bigint,
    rel_oid oid,
    payload_relid regclass,
    row_count bigint,
    schema_def jsonb,
    snapshot_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row record;
    v_kind text;
BEGIN
    SELECT * INTO v_row
    FROM public.flashback_internal_snapshot_resolve(p_snapshot_id, p_tracking_id);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown snapshot artifact % (tracking %)',
            p_snapshot_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_row.payload_state <> 'available' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % is % (not available)',
            p_snapshot_id, v_row.payload_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_row.storage_backend <> 'heap_v1' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % has unsupported backend %',
            p_snapshot_id, v_row.storage_backend
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF v_row.payload_relid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % payload relation is missing',
            p_snapshot_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    v_kind := public.flashback_payload_kind(v_row.payload_relid);
    IF v_kind NOT IN ('base_snapshot', 'checkpoint_snapshot') THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % relation % is not a recognized payload',
            p_snapshot_id, v_row.payload_relid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF NOT public.flashback_payload_is_owned(v_row.payload_relid) THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % relation % is not pg_flashback-owned',
            p_snapshot_id, v_row.payload_relid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    RETURN QUERY
    SELECT v_row.snapshot_id, v_row.tracking_id, v_row.rel_oid,
           v_row.payload_relid, v_row.row_count, v_row.schema_def, v_row.snapshot_lsn;
END;
$$;

-- ------------------------------------------------------------------
-- materialize: copy a snapshot artifact's rows into an already-created
-- destination relation. Restore call sites pass an already %I-quoted,
-- already-validated column list (built from the destination relation's
-- own pg_attribute, same as before this module existed); this function
-- is what turns that into `INSERT ... SELECT ... FROM <resolved source>`
-- so no caller ever embeds a free-text snapshot_table in a FROM clause
-- again.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_materialize(
    p_snapshot_id bigint,
    p_tracking_id bigint,
    p_dest_schema text,
    p_dest_table text,
    p_column_list text,
    p_identity_override text DEFAULT ''
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_source record;
BEGIN
    IF p_dest_schema IS NULL OR p_dest_table IS NULL OR p_column_list IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: snapshot materialize requires destination schema/table/columns'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_source
    FROM public.flashback_internal_snapshot_require_available(p_snapshot_id, p_tracking_id);

    EXECUTE format(
        'INSERT INTO %I.%I (%s)%s SELECT %s FROM %s',
        p_dest_schema, p_dest_table,
        p_column_list, COALESCE(p_identity_override, ''), p_column_list,
        v_source.payload_relid::text
    );
END;
$$;

-- ------------------------------------------------------------------
-- measure/describe: byte size per matching artifact, for capacity,
-- monitoring and health. Replaces direct
-- pg_total_relation_size(to_regclass(snapshot_table)) call sites. A row
-- whose payload cannot be resolved (already dropped, wrong backend,
-- etc.) reports 0 bytes rather than raising, matching the existing
-- to_regclass(...) IS NOT NULL filters this replaces.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_sizes(
    p_tracking_id bigint DEFAULT NULL,
    p_payload_states text[] DEFAULT ARRAY['available']
)
RETURNS TABLE (
    snapshot_id bigint,
    tracking_id bigint,
    size_bytes bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
    SELECT r.snapshot_id, r.tracking_id,
           COALESCE(pg_total_relation_size(r.payload_relid), 0)
    FROM flashback.snapshots s
    CROSS JOIN LATERAL public.flashback_internal_snapshot_resolve(s.snapshot_id, s.tracking_id) r
    WHERE (p_tracking_id IS NULL OR s.tracking_id = p_tracking_id)
      AND s.payload_state = ANY (p_payload_states);
$$;

-- ------------------------------------------------------------------
-- retire: drop the exact artifact's physical payload and transition it
-- to a terminal state (retired for planned retention/cleanup, missing
-- for an incidental loss such as a broken capture stream discarding a
-- building generation's snapshot). Idempotent when the artifact is
-- already at the requested terminal state; fail-closed when it is
-- already terminal at a *different* state, or when it is not yet
-- available (creating/retiring is a caller ordering bug, not something
-- to paper over).
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_retire(
    p_snapshot_id bigint,
    p_tracking_id bigint,
    p_target_state text DEFAULT 'retired'
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row record;
BEGIN
    IF p_target_state NOT IN ('retired', 'missing') THEN
        RAISE EXCEPTION 'pg_flashback: snapshot retire target state must be retired or missing, got %',
            p_target_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM public.flashback_internal_snapshot_resolve(p_snapshot_id, p_tracking_id);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown snapshot artifact % (tracking %)',
            p_snapshot_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.payload_state = p_target_state THEN
        RETURN false;
    END IF;
    IF v_row.payload_state IN ('retired', 'missing', 'aborted') THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % is already terminal at % (wanted %)',
            p_snapshot_id, v_row.payload_state, p_target_state
            USING ERRCODE = 'serialization_failure';
    END IF;
    IF v_row.payload_state <> 'available' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % is % (not available); refuse to retire',
            p_snapshot_id, v_row.payload_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM public.flashback_internal_snapshot_transition(
        p_snapshot_id, p_tracking_id, ARRAY['available'], 'retiring'
    );

    IF v_row.storage_backend = 'heap_v1' AND v_row.payload_relid IS NOT NULL THEN
        PERFORM public.flashback_drop_payload_table(v_row.payload_relid);
    END IF;

    PERFORM public.flashback_internal_snapshot_transition(
        p_snapshot_id, p_tracking_id, ARRAY['retiring'], p_target_state
    );
    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- refine_boundary: single authority for refining snapshot_lsn and
-- captured_at when resolving a building coverage generation's exact
-- boundary transaction COMMIT LSN/time.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_refine_boundary(
    p_snapshot_id bigint,
    p_tracking_id bigint,
    p_generation_id bigint,
    p_stream_id bigint,
    p_commit_lsn pg_lsn,
    p_committed_at timestamptz
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_snap flashback.snapshots%ROWTYPE;
    v_gen flashback.coverage_generations%ROWTYPE;
    v_commit_xid bigint;
BEGIN
    IF p_snapshot_id IS NULL OR p_tracking_id IS NULL OR p_generation_id IS NULL
       OR p_stream_id IS NULL OR p_commit_lsn IS NULL OR p_committed_at IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: snapshot boundary refinement requires snapshot_id, tracking_id, generation_id, stream_id, commit_lsn, committed_at'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_snap
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown snapshot artifact % (tracking %)',
            p_snapshot_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_snap.payload_state <> 'available' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % state is % (must be available for boundary refinement)',
            p_snapshot_id, v_snap.payload_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT * INTO v_gen
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown coverage generation % (tracking %)',
            p_generation_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_gen.state NOT IN ('building', 'active') THEN
        RAISE EXCEPTION 'pg_flashback: coverage generation % state is % (must be building or active for boundary refinement)',
            p_generation_id, v_gen.state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF v_gen.boundary_snapshot_id IS DISTINCT FROM p_snapshot_id THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % is not boundary_snapshot_id (%) for generation %',
            p_snapshot_id, v_gen.boundary_snapshot_id, p_generation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_gen.stream_id IS DISTINCT FROM p_stream_id THEN
        RAISE EXCEPTION 'pg_flashback: stream_id mismatch for generation %: expected %, got %',
            p_generation_id, v_gen.stream_id, p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Verify commit coordinate in capture_commits matches boundary_xid
    SELECT source_xid INTO v_commit_xid
    FROM flashback.capture_commits
    WHERE stream_id = p_stream_id
      AND commit_lsn = p_commit_lsn
      AND committed_at = p_committed_at;
    IF NOT FOUND OR v_commit_xid IS NULL OR v_commit_xid <> v_gen.boundary_xid THEN
        RAISE EXCEPTION 'pg_flashback: commit coordinate LSN % time % on stream % does not match capture_commits xid % for generation %',
            p_commit_lsn, p_committed_at, p_stream_id, v_gen.boundary_xid, p_generation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Idempotent check: if snapshot coordinate ALREADY matches target refinement coordinate
    IF v_snap.snapshot_lsn = p_commit_lsn AND v_snap.captured_at = p_committed_at THEN
        IF v_gen.state = 'active' AND (v_gen.boundary_lsn IS DISTINCT FROM p_commit_lsn OR v_gen.boundary_time IS DISTINCT FROM p_committed_at) THEN
            RAISE EXCEPTION 'pg_flashback: snapshot % matches coordinate LSN % time % but generation % is active with mismatching boundary LSN % time %',
                p_snapshot_id, p_commit_lsn, p_committed_at, p_generation_id, v_gen.boundary_lsn, v_gen.boundary_time
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        RETURN false; -- Idempotent retry
    END IF;

    -- Fail-closed checks when snapshot or generation boundary was already refined to a different LSN
    IF v_gen.boundary_lsn IS NOT NULL THEN
        IF p_commit_lsn < v_gen.boundary_lsn THEN
            RAISE EXCEPTION 'pg_flashback: refined snapshot LSN % cannot regress from existing refined LSN %',
                p_commit_lsn, v_gen.boundary_lsn
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        RAISE EXCEPTION 'pg_flashback: generation % boundary already refined to LSN % (time %), cannot refine to different coordinate LSN % (time %)',
            p_generation_id, v_gen.boundary_lsn, v_gen.boundary_time, p_commit_lsn, p_committed_at
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE flashback.snapshots
       SET snapshot_lsn = p_commit_lsn,
           captured_at = p_committed_at
     WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id;

    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- retire_legacy: migration-only primitive to safely retire legacy
-- snapshot artifacts (tracking_id IS NULL). Physical payload is dropped
-- if pg_flashback-owned and unreferenced by any active lifecycle; the
-- catalog row is retained in a terminal state (retired or missing).
-- Never DELETEs snapshot history rows. Fail-closed for user-owned relations.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_retire_legacy(
    p_snapshot_id bigint,
    p_target_state text DEFAULT 'retired'
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_snap flashback.snapshots%ROWTYPE;
    v_parts text[];
    v_schema text;
    v_rel text;
    v_oid oid;
    v_kind text;
    v_is_owned boolean;
BEGIN
    IF p_snapshot_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: snapshot_id is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF p_target_state NOT IN ('retired', 'missing') THEN
        RAISE EXCEPTION 'pg_flashback: invalid target state % for legacy snapshot retirement (must be retired or missing)',
            p_target_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_snap
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id IS NULL
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown legacy snapshot artifact %', p_snapshot_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Idempotent terminal state retry
    IF v_snap.payload_state = p_target_state THEN
        RETURN false;
    END IF;

    -- Different terminal state target fails closed
    IF v_snap.payload_state IN ('retired', 'missing', 'aborted') THEN
        RAISE EXCEPTION 'pg_flashback: legacy snapshot % is already in terminal state % (cannot transition to %)',
            p_snapshot_id, v_snap.payload_state, p_target_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF v_snap.payload_state NOT IN ('available', 'retiring') THEN
        RAISE EXCEPTION 'pg_flashback: legacy snapshot % state is % (must be available or retiring)',
            p_snapshot_id, v_snap.payload_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Active reference check 1: coverage_generations boundary_snapshot_id
    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE boundary_snapshot_id = p_snapshot_id
    ) THEN
        RAISE EXCEPTION 'pg_flashback: legacy snapshot % is referenced as boundary_snapshot_id in coverage_generations',
            p_snapshot_id
            USING ERRCODE = 'dependent_objects_still_exist';
    END IF;

    -- Active reference check 2: tracked_tables base_snapshot_table
    IF v_snap.snapshot_table IS NOT NULL AND EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE base_snapshot_table = v_snap.snapshot_table
    ) THEN
        RAISE EXCEPTION 'pg_flashback: legacy snapshot % (table %) is referenced as base_snapshot_table in tracked_tables',
            p_snapshot_id, v_snap.snapshot_table
            USING ERRCODE = 'dependent_objects_still_exist';
    END IF;

    -- Parse payload relation identity
    v_parts := NULL;
    IF v_snap.storage_backend = 'heap_v1' AND v_snap.locator IS NOT NULL
       AND v_snap.locator ? 'schema' AND v_snap.locator ? 'relation'
    THEN
        v_schema := v_snap.locator->>'schema';
        v_rel := v_snap.locator->>'relation';
        v_oid := to_regclass(format('%I.%I', v_schema, v_rel));
    ELSIF v_snap.snapshot_table IS NOT NULL AND btrim(v_snap.snapshot_table) <> '' THEN
        BEGIN
            v_parts := pg_catalog.parse_ident(v_snap.snapshot_table, true);
            IF cardinality(v_parts) = 2 THEN
                v_schema := v_parts[1];
                v_rel := v_parts[2];
                v_oid := to_regclass(format('%I.%I', v_schema, v_rel));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_oid := NULL;
        END;
    END IF;

    -- State graph: available -> retiring transition first (if dropping payload)
    IF v_snap.payload_state = 'available' THEN
        UPDATE flashback.snapshots
           SET payload_state = 'retiring'
         WHERE snapshot_id = p_snapshot_id AND tracking_id IS NULL;
    END IF;

    -- Payload inspection and drop
    IF v_oid IS NOT NULL THEN
        -- Active reference check 3: open operations / recovery active on relation
        IF EXISTS (
            SELECT 1 FROM flashback.operations o
            JOIN flashback.operation_current_state s ON s.operation_id = o.operation_id
            WHERE (o.details->>'snapshot_table' = v_snap.snapshot_table
               OR o.details->>'relation' = format('%I.%I', v_schema, v_rel))
              AND s.state NOT IN ('verified', 'failed', 'abandoned', 'unprotected', 'cleaned', 'sealed')
        ) THEN
            RAISE EXCEPTION 'pg_flashback: legacy snapshot % payload is in use by active operation',
                p_snapshot_id
                USING ERRCODE = 'dependent_objects_still_exist';
        END IF;

        -- BOTH payload kind AND extension ownership MUST be verified
        v_kind := public.flashback_payload_kind(v_oid);
        v_is_owned := public.flashback_payload_is_owned(v_oid);

        IF v_kind NOT IN ('base_snapshot', 'checkpoint_snapshot') OR NOT v_is_owned THEN
            -- Reject without setting metadata to retired/missing!
            RAISE EXCEPTION 'pg_flashback: legacy snapshot % payload % is not an owned flashback payload (kind=%, owned=%)',
                p_snapshot_id, v_snap.snapshot_table, COALESCE(v_kind, 'none'), COALESCE(v_is_owned, false)
                USING ERRCODE = 'invalid_parameter_value';
        END IF;

        -- Drop payload relation safely
        PERFORM public.flashback_drop_payload_table(v_oid);
    END IF;

    -- Final transition to target state (retired or missing)
    UPDATE flashback.snapshots
       SET payload_state = CASE WHEN v_oid IS NULL OR v_kind IS NULL THEN 'missing' ELSE p_target_state END,
           retired_at = COALESCE(retired_at, clock_timestamp())
     WHERE snapshot_id = p_snapshot_id AND tracking_id IS NULL;

    RETURN true;
END;
$$;

COMMENT ON FUNCTION flashback_internal_snapshot_transition(bigint, bigint, text[], text, text, jsonb, text, bigint, jsonb)
    IS '[Internal] SnapshotStore: sole mutation authority for flashback.snapshots.payload_state (CAS).';
COMMENT ON FUNCTION flashback_internal_snapshot_create(bigint, oid, text, text, pg_lsn, text, jsonb)
    IS '[Internal] SnapshotStore: create a heap_v1 snapshot artifact (reserve, CTAS, own, count, finalize).';
COMMENT ON FUNCTION flashback_internal_snapshot_abort(bigint, bigint)
    IS '[Internal] SnapshotStore: mark a creating artifact permanently failed.';
COMMENT ON FUNCTION flashback_internal_snapshot_resolve(bigint, bigint)
    IS '[Internal] SnapshotStore: exact snapshot_id+tracking_id lookup; never raises.';
COMMENT ON FUNCTION flashback_internal_snapshot_require_available(bigint, bigint)
    IS '[Internal] SnapshotStore: resolve + fail-closed availability/ownership validation.';
COMMENT ON FUNCTION flashback_internal_snapshot_materialize(bigint, bigint, text, text, text, text)
    IS '[Internal] SnapshotStore: copy an available artifact''s rows into an already-created destination relation.';
COMMENT ON FUNCTION flashback_internal_snapshot_sizes(bigint, text[])
    IS '[Internal] SnapshotStore: per-artifact byte size for capacity/monitoring/health.';
COMMENT ON FUNCTION flashback_internal_snapshot_retire(bigint, bigint, text)
    IS '[Internal] SnapshotStore: drop the exact artifact''s payload and transition to retired or missing.';
COMMENT ON FUNCTION flashback_internal_snapshot_refine_boundary(bigint, bigint, bigint, bigint, pg_lsn, timestamptz)
    IS '[Internal] SnapshotStore: refine snapshot_lsn and captured_at when resolving a building generation boundary.';
COMMENT ON FUNCTION flashback_internal_snapshot_retire_legacy(bigint, text)
    IS '[Internal] SnapshotStore: safely retire legacy (tracking_id IS NULL) snapshot artifact payload.';

REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_transition(bigint, bigint, text[], text, text, jsonb, text, bigint, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_create(bigint, oid, text, text, pg_lsn, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_abort(bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_resolve(bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_require_available(bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_materialize(bigint, bigint, text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_sizes(bigint, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire(bigint, bigint, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_refine_boundary(bigint, bigint, bigint, bigint, pg_lsn, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire_legacy(bigint, text) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_transition(bigint, bigint, text[], text, text, jsonb, text, bigint, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_create(bigint, oid, text, text, pg_lsn, text, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_abort(bigint, bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_resolve(bigint, bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_require_available(bigint, bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_materialize(bigint, bigint, text, text, text, text) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_sizes(bigint, text[]) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire(bigint, bigint, text) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_refine_boundary(bigint, bigint, bigint, bigint, pg_lsn, timestamptz) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire_legacy(bigint, text) FROM flashback_admin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_transition(bigint, bigint, text[], text, text, jsonb, text, bigint, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_create(bigint, oid, text, text, pg_lsn, text, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_abort(bigint, bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_resolve(bigint, bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_require_available(bigint, bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_materialize(bigint, bigint, text, text, text, text) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_sizes(bigint, text[]) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire(bigint, bigint, text) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_refine_boundary(bigint, bigint, bigint, bigint, pg_lsn, timestamptz) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_retire_legacy(bigint, text) FROM pg_monitor';
    END IF;
END
$$;

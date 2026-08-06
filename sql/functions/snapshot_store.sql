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
    FROM public.flashback_internal_snapshot_resolve(p_snapshot_id, p_tracking_id);
    IF NOT FOUND OR v_source.payload_state IS DISTINCT FROM 'available' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % is not available', p_snapshot_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF v_source.storage_backend = 'external_zstd' THEN
        PERFORM public.flashback_internal_materialize_external_snapshot(
            p_snapshot_id, p_tracking_id, p_dest_schema, p_dest_table
        );
        RETURN;
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
           CASE WHEN s.storage_backend = 'external_zstd'
                THEN COALESCE(s.external_compressed_bytes, 0)
                ELSE COALESCE(pg_total_relation_size(r.payload_relid), 0)
           END
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

    -- heap_v1 is materialized before marker resolution and therefore must
    -- already be available. external_zstd deliberately resolves the marker
    -- while its independently copied artifact is still `creating`; this
    -- writes only the exact coordinate and does not make the artifact or
    -- generation eligible for recovery.
    IF v_snap.payload_state <> 'available'
       AND NOT (
           v_snap.payload_state = 'creating'
           AND v_snap.storage_backend = 'external_zstd'
           AND v_gen.storage_backend = 'external_zstd'
           AND v_gen.state = 'building'
       )
    THEN
        RAISE EXCEPTION 'pg_flashback: snapshot artifact % state/backend %/% is not eligible for boundary refinement',
            p_snapshot_id, v_snap.payload_state, v_snap.storage_backend
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

-- ------------------------------------------------------------------
-- Step 9 / Stage 5: reserve a snapshot artifact row without copying any
-- data now. This is the INSERT-only half of flashback_internal_snapshot_
-- create -- no CTAS, no relation, no row count -- for backends whose
-- artifact creation cannot happen atomically inside the reservation's own
-- transaction (external_zstd's copy is a separate, later transaction; see
-- flashback_internal_reserve_online_generation below). snapshot_lsn is
-- left NULL (legal only for a still-creating external_zstd row, per
-- snapshots_lsn_shape_check) and schema_def is left as an honest empty
-- placeholder -- both are filled in later, while still 'creating', by
-- flashback_internal_bind_online_boundary (Stage 6), under the table
-- lock, once the real boundary is known. heap_v1 never calls this
-- function; flashback_internal_snapshot_create is unaffected and remains
-- its only construction path.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_snapshot_reserve(
    p_tracking_id bigint,
    p_rel_oid oid,
    p_storage_backend text
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_snapshot_id bigint;
BEGIN
    IF p_tracking_id IS NULL OR p_rel_oid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: snapshot reserve requires tracking_id and rel_oid'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- snapshots_lsn_shape_check only permits a NULL snapshot_lsn on a
    -- still-creating external_zstd row; any other backend inserted here
    -- with no LSN would simply fail that constraint, but rejecting it
    -- explicitly gives a clear, named error instead of an opaque
    -- constraint violation.
    IF p_storage_backend IS DISTINCT FROM 'external_zstd' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot reserve (no-copy reservation) is only defined for external_zstd, got %',
            p_storage_backend
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at, storage_backend
    ) VALUES (
        p_rel_oid, p_tracking_id, '', NULL,
        '{}'::jsonb, 0, clock_timestamp(), p_storage_backend
    ) RETURNING snapshot_id INTO v_snapshot_id;

    RETURN v_snapshot_id;
END;
$$;

-- ------------------------------------------------------------------
-- Step 9 / Stage 5: centralized online-reservation authority (plan §1a).
-- Atomically binds tracking_id, rel_oid, stream_id, the newly-reserved
-- snapshot_id, generation_id, generation_no, parent_generation_id,
-- operation_nonce and storage_backend in one durably-committed
-- transaction -- durably visible via ordinary MVCC to the later marker,
-- copy, finalizer and reconciler transactions of the online-snapshot
-- protocol, none of which exist as a single transaction with this one.
--
-- Reuses, verbatim in effect, the same stream/lifecycle/parent-generation
-- validation flashback_internal_create_coverage_generation already
-- performs (that function itself is NOT modified: every existing caller
-- keeps requiring an already-available boundary snapshot, unchanged), and
-- the same durable 'building'-row admission check flashback_reanchor
-- already relies on today -- this durable row, not the session-level
-- lock taken by the caller before this call, is the exclusivity invariant
-- for the entire (much longer, external-copy-spanning) online-create
-- window; see plan §1i.
--
-- storage_backend is currently restricted to external_zstd: heap_v1's
-- boundary snapshot is always already available by construction (CTAS is
-- atomic), so it has no use for a no-copy reservation and continues to
-- use flashback_internal_create_coverage_generation directly, completely
-- unaffected by this function's existence.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_reserve_online_generation(
    p_tracking_id bigint,
    p_rel_oid oid,
    p_stream_id bigint,
    p_generation_no bigint,
    p_parent_generation_id bigint,
    p_storage_backend text,
    p_operation_nonce bigint,
    p_recovery_profile text DEFAULT 'local_delta',
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(generation_id bigint, snapshot_id bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_stream flashback.capture_streams%ROWTYPE;
    v_lifecycle_retired_at timestamptz;
    v_parent_tracking_id bigint;
    v_snapshot_id bigint;
    v_gen_id bigint;
    v_marker text;
BEGIN
    IF p_tracking_id IS NULL OR p_rel_oid IS NULL OR p_stream_id IS NULL
       OR p_generation_no IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: online generation reservation requires tracking_id, rel_oid, stream_id, generation_no'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_operation_nonce IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: online generation reservation requires operation_nonce'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_storage_backend IS DISTINCT FROM 'external_zstd' THEN
        RAISE EXCEPTION 'pg_flashback: online generation reservation is only defined for external_zstd, got %',
            p_storage_backend
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Same stream/lifecycle validation flashback_internal_create_coverage_
    -- generation already performs (reused logic, not a weaker copy).
    SELECT * INTO v_stream
    FROM flashback.capture_streams
    WHERE stream_id = p_stream_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: stream % does not exist', p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_stream.database_oid IS DISTINCT FROM (
        SELECT oid FROM pg_database WHERE datname = current_database()
    ) OR v_stream.database_name IS DISTINCT FROM current_database()::name THEN
        RAISE EXCEPTION 'pg_flashback: stream % belongs to database % (oid %), not current database %',
            p_stream_id, v_stream.database_name, v_stream.database_oid, current_database()
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_stream.state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'pg_flashback: cannot reserve online generation on stream % in state %',
            p_stream_id, v_stream.state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM public.flashback_internal_lock_database_stream(v_stream.database_oid);
    PERFORM public.flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT retired_at INTO v_lifecycle_retired_at
    FROM flashback.tracking_lifecycles
    WHERE tracking_id = p_tracking_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: tracking lifecycle % does not exist', p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_lifecycle_retired_at IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: tracking lifecycle % is retired', p_tracking_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF p_parent_generation_id IS NOT NULL THEN
        -- This function RETURNS TABLE(generation_id bigint, snapshot_id
        -- bigint), which makes generation_id/snapshot_id plpgsql OUT-
        -- parameter variable names for the rest of this function body --
        -- an unqualified `generation_id` column reference here would
        -- collide with that OUT parameter (plpgsql.variable_conflict
        -- defaults to 'error': ambiguous column reference at runtime), so
        -- the column is qualified explicitly, unlike the otherwise-
        -- identical check in flashback_internal_create_coverage_generation
        -- (which has no such OUT parameter and does not need this).
        SELECT cg.tracking_id INTO v_parent_tracking_id
        FROM flashback.coverage_generations cg
        WHERE cg.generation_id = p_parent_generation_id;
        IF NOT FOUND OR v_parent_tracking_id IS DISTINCT FROM p_tracking_id THEN
            RAISE EXCEPTION 'pg_flashback: parent generation % does not belong to tracking %',
                p_parent_generation_id, p_tracking_id
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
    END IF;

    -- Durable admission check flashback_reanchor already relies on today:
    -- under the same lifecycle lock just taken above, a second concurrent
    -- reservation attempt for this tracking_id must fail here. This
    -- durable 'building' row -- not the caller's session-level lock, which
    -- only serializes the brief reservation call itself -- is what
    -- protects the entire, much longer, online-create window that
    -- follows (plan §1i).
    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE tracking_id = p_tracking_id AND state = 'building'
    ) THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle % already has a pending generation',
            p_tracking_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    v_snapshot_id := public.flashback_internal_snapshot_reserve(
        p_tracking_id, p_rel_oid, p_storage_backend
    );

    -- Non-null placeholder boundary_marker: satisfies coverage_generations_
    -- state_shape_check's 'building' requirement honestly -- a real,
    -- recognizable "reservation pending" value, not a fake coordinate.
    -- Replaced with the real online_external:... marker by
    -- flashback_internal_bind_online_boundary (Stage 6) once the marker
    -- transaction resolves a real boundary_xid under the table lock.
    -- boundary_xid and boundary_lsn stay NULL until then (the FK to
    -- flashback.snapshots(snapshot_id, tracking_id, snapshot_lsn) is
    -- MATCH SIMPLE, so a NULL boundary_lsn paired with this reservation's
    -- NULL snapshot_lsn is not enforced -- not a dangling reference).
    v_marker := 'online_pending:' || p_operation_nonce::text;

    -- Generation-row construction stays in state_authority.sql.  The
    -- operation_nonce column's UNIQUE constraint remains the database-level
    -- aliasing guard; this SnapshotStore routine only coordinates the atomic
    -- snapshot reservation + centralized generation construction.
    v_gen_id := public.flashback_internal_create_online_generation_reservation(
        p_tracking_id,
        p_generation_no,
        p_stream_id,
        p_rel_oid,
        v_snapshot_id,
        v_marker,
        p_parent_generation_id,
        COALESCE(p_recovery_profile, 'local_delta'),
        p_storage_backend,
        p_operation_nonce,
        COALESCE(p_details, '{}'::jsonb)
            || jsonb_build_object('operation_nonce', p_operation_nonce)
    );

    RETURN QUERY SELECT v_gen_id, v_snapshot_id;
END;
$$;

-- ------------------------------------------------------------------
-- Step 9 / Stage 6: the materializable-column contract for the external_
-- zstd binary artifact format (plan §5). Narrower than flashback_collect_
-- schema_def (api_track_capture.sql), which captures the full DDL-fidelity
-- shape (constraints, indexes, generated columns, sequences) for restore.
-- This returns exactly the fields src/storage/external_zstd_format.rs's
-- ColumnDescriptor needs to encode/decode one row's binary representation,
-- for exactly the columns that format can carry: attnum > 0, not dropped,
-- not generated (generated column values are recomputed on restore, never
-- carried in the artifact -- matching flashback_collect_schema_def's own
-- documented rule for attgenerated elsewhere in this codebase).
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_materializable_columns(p_rel_oid oid)
RETURNS jsonb
LANGUAGE sql
STABLE
SET search_path = pg_catalog, flashback, pg_temp
AS $$
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'attnum', a.attnum,
            'atttypid', a.atttypid,
            'atttypmod', a.atttypmod,
            'attcollation', a.attcollation,
            'attnotnull', a.attnotnull,
            'attidentity', a.attidentity::text,
            'name', a.attname
        )
        ORDER BY a.attnum
    ), '[]'::jsonb)
    FROM pg_attribute a
    WHERE a.attrelid = p_rel_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = ''
$$;

-- ------------------------------------------------------------------
-- Step 9 / Stage 6: flashback_internal_bind_online_boundary (plan §1d,
-- marker-transaction step M4). Captures the schema_def/column_contract
-- for an online external_zstd reservation *under the caller's already-held
-- table lock* -- reservation time (Stage 5) is too early, since nothing
-- has locked the target table yet at that point, so any DDL committed
-- between reservation and the marker transaction's lock acquisition would
-- otherwise go unnoticed. The caller is responsible for having already
-- locked the target relation (SHARE ROW EXCLUSIVE, plan §1e M2) and
-- revalidated its identity under that lock (the existing pattern
-- flashback_reanchor already performs after acquiring its own lock) --
-- this function cannot itself verify a lock is held (no portable SQL-level
-- introspection for "do I hold this lock"), so that precondition is
-- enforced by caller discipline, the same way every other authority
-- function in this file documents its own "caller must already hold X"
-- preconditions.
--
-- CAS-shaped like every other SnapshotStore/state-authority mutation in
-- this codebase: FOR UPDATE, expected-state check (state='building' AND
-- boundary_xid IS NULL on the generation row; payload_state='creating' on
-- the snapshot row), WHERE-clause CAS on the UPDATEs, ROW_COUNT
-- verification. A second call against an already-bound generation (or any
-- other state-shape violation) fails closed with object_not_in_
-- prerequisite_state rather than silently re-stamping a new boundary --
-- this IS the "no raw UPDATE bypass" guarantee: the only way to move
-- boundary_xid/boundary_marker/schema_def/external_column_contract out of
-- their Stage-5 placeholder values is this one authority call, exactly
-- once, and flashback.coverage_generations/flashback.snapshots grant no
-- direct table-level INSERT/UPDATE/DELETE to any delegated role (verified
-- generically, for every table in the flashback schema, by
-- rbac_enforcement.sql).
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_bind_online_boundary(
    p_generation_id bigint,
    p_tracking_id bigint,
    p_snapshot_id bigint,
    p_rel_oid oid
)
RETURNS TABLE(boundary_xid bigint, boundary_marker text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_gen flashback.coverage_generations%ROWTYPE;
    v_snap flashback.snapshots%ROWTYPE;
    v_xid bigint;
    v_marker text;
    v_schema_def jsonb;
    v_column_contract jsonb;
    v_n integer;
BEGIN
    IF p_generation_id IS NULL OR p_tracking_id IS NULL
       OR p_snapshot_id IS NULL OR p_rel_oid IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: bind online boundary requires generation_id, tracking_id, snapshot_id, rel_oid'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_gen
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: online generation % (tracking %) does not exist',
            p_generation_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_gen.storage_backend IS DISTINCT FROM 'external_zstd' THEN
        RAISE EXCEPTION 'pg_flashback: bind_online_boundary is only defined for external_zstd generations'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_gen.state IS DISTINCT FROM 'building' THEN
        RAISE EXCEPTION 'pg_flashback: online generation % is not building (state=%)',
            p_generation_id, v_gen.state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_gen.boundary_xid IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: online generation % boundary is already bound (xid=%)',
            p_generation_id, v_gen.boundary_xid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_gen.rel_oid_at_boundary IS DISTINCT FROM p_rel_oid THEN
        RAISE EXCEPTION 'pg_flashback: online generation % rel_oid mismatch (expected %, got %)',
            p_generation_id, v_gen.rel_oid_at_boundary, p_rel_oid
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_gen.boundary_snapshot_id IS DISTINCT FROM p_snapshot_id THEN
        RAISE EXCEPTION 'pg_flashback: online generation % does not reference snapshot %',
            p_generation_id, p_snapshot_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_snap
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % (tracking %) does not exist',
            p_snapshot_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_snap.payload_state IS DISTINCT FROM 'creating' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % is not creating (state=%)',
            p_snapshot_id, v_snap.payload_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_snap.storage_backend IS DISTINCT FROM 'external_zstd' THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % is not external_zstd', p_snapshot_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_xid := txid_current();
    v_marker := format('online_external:%s:%s:%s', p_tracking_id, v_gen.generation_no, v_xid);
    v_schema_def := public.flashback_collect_schema_def(p_rel_oid);
    v_column_contract := public.flashback_internal_materializable_columns(p_rel_oid);

    IF v_column_contract = '[]'::jsonb THEN
        RAISE EXCEPTION 'pg_flashback: relation % has no materializable columns', p_rel_oid
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- This function's own RETURNS TABLE(boundary_xid, boundary_marker)
    -- makes those names plpgsql OUT-parameter variables for the rest of
    -- this function body -- the WHERE clause below qualifies the column
    -- explicitly (coverage_generations.boundary_xid) to avoid the exact
    -- ambiguous-column-reference bug fixed in flashback_internal_reserve_
    -- online_generation (Stage 5).
    UPDATE flashback.coverage_generations
       SET boundary_xid = v_xid,
           boundary_marker = v_marker
     WHERE generation_id = p_generation_id
       AND tracking_id = p_tracking_id
       AND state = 'building'
       AND coverage_generations.boundary_xid IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: online generation % boundary bind raced (expected exactly 1 row)',
            p_generation_id
            USING ERRCODE = 'serialization_failure';
    END IF;

    UPDATE flashback.snapshots
       SET schema_def = v_schema_def,
           external_column_contract = v_column_contract
     WHERE snapshot_id = p_snapshot_id
       AND tracking_id = p_tracking_id
       AND payload_state = 'creating';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % boundary bind raced (expected exactly 1 row)',
            p_snapshot_id
            USING ERRCODE = 'serialization_failure';
    END IF;

    RETURN QUERY SELECT v_xid, v_marker;
END;
$$;

-- Publish one fully staged external_zstd artifact. This is the only SQL
-- authority allowed to fill external artifact evidence and perform the
-- creating -> available transition. The filesystem finalizer must already
-- have fsynced and atomically published the directory; a transaction abort
-- after that point leaves a resumable published orphan, never an available
-- row pointing at partial bytes.
CREATE OR REPLACE FUNCTION flashback_internal_publish_external_snapshot(
    p_snapshot_id bigint,
    p_tracking_id bigint,
    p_generation_id bigint,
    p_operation_nonce bigint,
    p_locator jsonb,
    p_row_count bigint,
    p_codec text,
    p_format_version integer,
    p_uncompressed_bytes bigint,
    p_compressed_bytes bigint,
    p_checksum_sha256 text,
    p_column_contract jsonb,
    p_schema_def_sha256 text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_generation flashback.coverage_generations%ROWTYPE;
    v_snapshot flashback.snapshots%ROWTYPE;
    v_stream_state text;
    v_n integer;
BEGIN
    IF p_snapshot_id IS NULL OR p_tracking_id IS NULL OR p_generation_id IS NULL
       OR p_operation_nonce IS NULL OR p_operation_nonce <= 0
       OR p_locator IS NULL OR p_row_count IS NULL OR p_row_count < 0
       OR p_codec IS DISTINCT FROM 'zstd' OR p_format_version IS DISTINCT FROM 1
       OR p_uncompressed_bytes IS NULL OR p_uncompressed_bytes < 0
       OR p_compressed_bytes IS NULL OR p_compressed_bytes <= 0
       OR p_checksum_sha256 IS NULL
       OR p_checksum_sha256 !~ '^[0-9a-f]{64}$'
       OR p_column_contract IS NULL
       OR p_schema_def_sha256 IS NULL
       OR p_schema_def_sha256 !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'pg_flashback: incomplete or invalid external snapshot publication evidence'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_locator->>'system_identifier' IS DISTINCT FROM (pg_control_system()).system_identifier::text
       OR p_locator->>'database_oid' IS DISTINCT FROM (
           SELECT oid::text FROM pg_database WHERE datname = current_database()
       )
       OR p_locator->>'tracking_id' IS DISTINCT FROM p_tracking_id::text
       OR p_locator->>'snapshot_id' IS DISTINCT FROM p_snapshot_id::text
       OR p_locator->>'nonce' IS DISTINCT FROM p_operation_nonce::text
    THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot locator does not match immutable identity'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT * INTO v_generation
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id
      AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_generation.state NOT IN ('building', 'active')
       OR v_generation.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_generation.operation_nonce IS DISTINCT FROM p_operation_nonce
       OR v_generation.boundary_snapshot_id IS DISTINCT FROM p_snapshot_id
       OR v_generation.boundary_xid IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: external generation % is not ready for artifact publication',
            p_generation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT state INTO v_stream_state
    FROM flashback.capture_streams
    WHERE stream_id = v_generation.stream_id
    FOR SHARE;
    IF NOT FOUND OR v_stream_state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'pg_flashback: external generation % capture stream is not healthy',
            p_generation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT * INTO v_snapshot
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id
      AND tracking_id = p_tracking_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot % does not exist', p_snapshot_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_snapshot.payload_state = 'available' THEN
        IF v_snapshot.storage_backend IS DISTINCT FROM 'external_zstd'
           OR v_snapshot.locator IS DISTINCT FROM p_locator
           OR v_snapshot.row_count IS DISTINCT FROM p_row_count
           OR v_snapshot.external_codec IS DISTINCT FROM p_codec
           OR v_snapshot.external_format_version IS DISTINCT FROM p_format_version
           OR v_snapshot.external_uncompressed_bytes IS DISTINCT FROM p_uncompressed_bytes
           OR v_snapshot.external_compressed_bytes IS DISTINCT FROM p_compressed_bytes
           OR v_snapshot.external_checksum_sha256 IS DISTINCT FROM p_checksum_sha256
           OR v_snapshot.external_column_contract IS DISTINCT FROM p_column_contract
           OR v_snapshot.schema_def_sha256 IS DISTINCT FROM p_schema_def_sha256
        THEN
            RAISE EXCEPTION 'pg_flashback: conflicting idempotent external snapshot publication for %',
                p_snapshot_id
                USING ERRCODE = 'serialization_failure';
        END IF;
        RETURN false;
    END IF;

    IF v_snapshot.payload_state IS DISTINCT FROM 'creating'
       OR v_snapshot.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_snapshot.snapshot_lsn IS NULL
       OR v_snapshot.external_column_contract IS DISTINCT FROM p_column_contract
       OR public.flashback_sha256(v_snapshot.schema_def::text) IS DISTINCT FROM p_schema_def_sha256
    THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot % database evidence does not match staged artifact',
            p_snapshot_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    UPDATE flashback.snapshots
       SET payload_state = 'available',
           locator = p_locator,
           snapshot_table = '',
           row_count = p_row_count,
           available_at = COALESCE(available_at, clock_timestamp()),
           external_codec = p_codec,
           external_format_version = p_format_version,
           external_uncompressed_bytes = p_uncompressed_bytes,
           external_compressed_bytes = p_compressed_bytes,
           external_checksum_sha256 = p_checksum_sha256,
           external_column_contract = p_column_contract,
           schema_def_sha256 = p_schema_def_sha256
     WHERE snapshot_id = p_snapshot_id
       AND tracking_id = p_tracking_id
       AND payload_state = 'creating';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot % publication raced', p_snapshot_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- Activate an external generation only after its exact marker boundary and
-- immutable artifact are both proven.  This is deliberately separate from
-- WAL consumption: observing a COMMIT can refine coordinates, but can never
-- make partial or missing filesystem bytes recoverable.
CREATE OR REPLACE FUNCTION flashback_internal_activate_external_generation(
    p_generation_id bigint,
    p_tracking_id bigint,
    p_snapshot_id bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_generation flashback.coverage_generations%ROWTYPE;
    v_parent flashback.coverage_generations%ROWTYPE;
    v_snapshot flashback.snapshots%ROWTYPE;
BEGIN
    IF p_generation_id IS NULL OR p_tracking_id IS NULL OR p_snapshot_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: external activation requires exact generation, tracking, and snapshot identities'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT * INTO v_generation
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id
      AND tracking_id = p_tracking_id
      AND boundary_snapshot_id = p_snapshot_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_generation.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_generation.state NOT IN ('building', 'active')
    THEN
        RAISE EXCEPTION 'pg_flashback: external generation % is not eligible for activation',
            p_generation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT * INTO v_snapshot
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id
      AND tracking_id = p_tracking_id
    FOR SHARE;
    IF NOT FOUND
       OR v_snapshot.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_snapshot.payload_state IS DISTINCT FROM 'available'
       OR v_snapshot.snapshot_lsn IS NULL
       OR v_snapshot.captured_at IS NULL
       OR v_snapshot.locator IS NULL
       OR v_snapshot.external_checksum_sha256 IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot % is not verified and available',
            p_snapshot_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF v_generation.state = 'active' THEN
        RETURN false;
    END IF;

    IF v_generation.parent_generation_id IS NOT NULL THEN
        SELECT * INTO v_parent
        FROM flashback.coverage_generations
        WHERE generation_id = v_generation.parent_generation_id
          AND tracking_id = p_tracking_id
        FOR UPDATE;

        IF FOUND AND v_parent.state = 'active' THEN
            PERFORM public.flashback_internal_transition_coverage_generation(
                v_parent.generation_id,
                p_tracking_id,
                'active',
                'sealed',
                'external_successor_artifact_available',
                NULL,
                NULL,
                CASE WHEN v_parent.stream_id = v_generation.stream_id
                     THEN v_snapshot.snapshot_lsn ELSE NULL END,
                CASE WHEN v_parent.stream_id = v_generation.stream_id
                     THEN v_snapshot.captured_at ELSE NULL END,
                v_snapshot.snapshot_lsn,
                v_snapshot.captured_at,
                '{}'::jsonb
            );
        END IF;
    END IF;

    PERFORM public.flashback_internal_transition_coverage_generation(
        p_generation_id,
        p_tracking_id,
        'building',
        'active',
        'external_artifact_available',
        v_snapshot.snapshot_lsn,
        v_snapshot.captured_at,
        v_snapshot.snapshot_lsn,
        v_snapshot.captured_at,
        NULL,
        NULL,
        jsonb_build_object('snapshot_id', p_snapshot_id)
    );

    UPDATE flashback.coverage_gaps
       SET gap_end_lsn = v_snapshot.snapshot_lsn,
           gap_end_time = v_snapshot.captured_at,
           reanchored_by_generation_id = p_generation_id,
           reanchored_at = clock_timestamp()
     WHERE tracking_id = p_tracking_id
       AND source_generation_id = v_generation.parent_generation_id
       AND reanchored_by_generation_id IS NULL
       AND gap_start_lsn < v_snapshot.snapshot_lsn;

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
COMMENT ON FUNCTION flashback_internal_snapshot_reserve(bigint, oid, text)
    IS '[Internal] SnapshotStore: reserve a creating snapshot artifact row without copying data (external_zstd only).';
COMMENT ON FUNCTION flashback_internal_reserve_online_generation(bigint, oid, bigint, bigint, bigint, text, bigint, text, jsonb)
    IS '[Internal] SnapshotStore: atomically reserve a building generation + creating snapshot for an online (non-blocking) external_zstd create.';
COMMENT ON FUNCTION flashback_internal_publish_external_snapshot(bigint, bigint, bigint, bigint, jsonb, bigint, text, integer, bigint, bigint, text, jsonb, text)
    IS '[Internal] SnapshotStore: publish a fully fsynced external_zstd artifact and atomically bind its immutable evidence.';
COMMENT ON FUNCTION flashback_internal_activate_external_generation(bigint, bigint, bigint)
    IS '[Internal] SnapshotStore: activate external coverage only after exact boundary and immutable artifact availability are proven.';

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
REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_reserve(bigint, oid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_reserve_online_generation(bigint, oid, bigint, bigint, bigint, text, bigint, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_publish_external_snapshot(bigint, bigint, bigint, bigint, jsonb, bigint, text, integer, bigint, bigint, text, jsonb, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_activate_external_generation(bigint, bigint, bigint) FROM PUBLIC;

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
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_reserve(bigint, oid, text) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_reserve_online_generation(bigint, oid, bigint, bigint, bigint, text, bigint, text, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_publish_external_snapshot(bigint, bigint, bigint, bigint, jsonb, bigint, text, integer, bigint, bigint, text, jsonb, text) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_activate_external_generation(bigint, bigint, bigint) FROM flashback_admin';
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
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_reserve(bigint, oid, text) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_reserve_online_generation(bigint, oid, bigint, bigint, bigint, text, bigint, text, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_publish_external_snapshot(bigint, bigint, bigint, bigint, jsonb, bigint, text, integer, bigint, bigint, text, jsonb, text) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_activate_external_generation(bigint, bigint, bigint) FROM pg_monitor';
    END IF;
END
$$;

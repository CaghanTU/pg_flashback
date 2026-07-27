-- A2: DROP/TRUNCATE/ALTER issued from inside a DO block, a PL/pgSQL function
-- body, or dynamic EXECUTE reach ProcessUtility with context QUERY (or
-- QUERY_NONATOMIC for a self-transacting procedure), not TOPLEVEL. The DDL
-- hook previously gated all capture logic on context == TOPLEVEL, so none of
-- these ever ran pre-drain, wrote a pre-DROP dependency manifest, or staged a
-- DDL event -- the exact same DROP issued directly at the top level was
-- captured, but wrapped one level of PL/pgSQL it silently was not.
--
-- flashback.drop_dependency_manifests is written synchronously by the hook
-- itself (capture_drop_dependency_manifests in src/capture/ddl_hook.rs),
-- before standard_ProcessUtility runs, with no dependency on the background
-- WAL worker -- it is the most direct observable proof that the pre-DROP
-- capture path actually executed for a given DROP TABLE statement. This is
-- what the DROP scenarios below exercise; nested TRUNCATE/ALTER need a real
-- physical replication slot (see the note further down) and are validated
-- live instead.
DO $setup$
DECLARE
    v_boot jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_nested_do CASCADE;
    CREATE TABLE public.it_nested_do (id int PRIMARY KEY);
    v_boot := flashback_test_bootstrap_lifecycle('public.it_nested_do');

    DROP TABLE IF EXISTS public.it_nested_fn CASCADE;
    CREATE TABLE public.it_nested_fn (id int PRIMARY KEY);
    v_boot := flashback_test_bootstrap_lifecycle('public.it_nested_fn');

    -- No incoming-FK child here: the local DROP recovery product refuses to
    -- capture a manifest for a table with an incoming FK dependency
    -- (unsupported topology, unrelated to A2). CASCADE with nothing that
    -- actually needs cascading is enough to prove the cascade_requested flag
    -- is captured correctly for a nested/nonatomic DROP.
    DROP TABLE IF EXISTS public.it_nested_cascade_parent CASCADE;
    CREATE TABLE public.it_nested_cascade_parent (id int PRIMARY KEY);
    v_boot := flashback_test_bootstrap_lifecycle('public.it_nested_cascade_parent');

    DROP TABLE IF EXISTS public.it_nested_rollback CASCADE;
    CREATE TABLE public.it_nested_rollback (id int PRIMARY KEY);
    v_boot := flashback_test_bootstrap_lifecycle('public.it_nested_rollback');
END;
$setup$;

-- Scenario: DROP issued via dynamic EXECUTE inside a DO block.
DO $do_drop$
BEGIN
    EXECUTE 'DROP TABLE public.it_nested_do';
END;
$do_drop$;

DO $do_drop_check$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM flashback.drop_dependency_manifests m
        JOIN flashback.tracked_tables tt ON tt.tracking_id = m.tracking_id
        WHERE tt.table_name = 'it_nested_do'
    ) THEN
        RAISE EXCEPTION 'nested_ddl_capture: DO-block DROP was not captured (no dependency manifest)';
    END IF;
END;
$do_drop_check$;

-- Scenario: DROP issued from inside a PL/pgSQL function body (not dynamic
-- EXECUTE -- a literal DROP TABLE statement compiled into the function).
DO $mk_fn$
BEGIN
    DROP FUNCTION IF EXISTS public.it_nested_fn_dropper();
    CREATE FUNCTION public.it_nested_fn_dropper() RETURNS void
    LANGUAGE plpgsql AS $body$
    BEGIN
        DROP TABLE public.it_nested_fn;
    END;
    $body$;
END;
$mk_fn$;

SELECT public.it_nested_fn_dropper();

DO $fn_drop_check$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM flashback.drop_dependency_manifests m
        JOIN flashback.tracked_tables tt ON tt.tracking_id = m.tracking_id
        WHERE tt.table_name = 'it_nested_fn'
    ) THEN
        RAISE EXCEPTION 'nested_ddl_capture: function-body DROP was not captured (no dependency manifest)';
    END IF;
END;
$fn_drop_check$;

-- Nested/dynamic TRUNCATE and ALTER are deliberately not exercised here:
-- both route through the qualified local_delta staging core
-- (flashback_stage_local_delta_ddl_event / prepare_destructive_ddl's
-- pre-drain), which needs the real physical replication slot's progress --
-- pg_test's synthetic bootstrap stream does not provide one (the same
-- live-WAL-only limitation noted in audited_recover_context_failclosed.sql).
-- The DROP scenarios above already prove the context gate itself is fixed
-- (capture_drop_dependency_manifests is independent of the WAL worker/slot);
-- TRUNCATE/ALTER are validated against a live instance with a real logical
-- slot instead, where a DO-block TRUNCATE/ALTER now drains and captures
-- exactly like a top-level one.

-- Scenario: DROP ... CASCADE issued via dynamic EXECUTE inside a DO block
-- must still produce exactly one manifest for the DROPped table, with the
-- cascade_requested flag correctly threaded through the nested DDL hook
-- (drop_stmt_requests_cascade reads the DropStmt's own behavior field, not
-- something context-dependent, but the manifest capture call that carries it
-- only happens at all once the context gate is fixed).
DO $do_drop_cascade$
BEGIN
    EXECUTE 'DROP TABLE public.it_nested_cascade_parent CASCADE';
END;
$do_drop_cascade$;

DO $cascade_check$
DECLARE
    v_count bigint;
    v_cascade boolean;
BEGIN
    SELECT count(*), bool_and(m.cascade_requested) INTO v_count, v_cascade
    FROM flashback.drop_dependency_manifests m
    JOIN flashback.tracked_tables tt ON tt.tracking_id = m.tracking_id
    WHERE tt.table_name = 'it_nested_cascade_parent';
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'nested_ddl_capture: expected exactly one manifest for nested DROP CASCADE, got %', v_count;
    END IF;
    IF NOT v_cascade THEN
        RAISE EXCEPTION 'nested_ddl_capture: cascade_requested was not captured for nested DROP CASCADE';
    END IF;
END;
$cascade_check$;

-- Scenario: a nested DROP inside a subtransaction that then rolls back must
-- leave no manifest behind -- capture is part of the same transaction as the
-- DDL it describes, so an aborted subtransaction must undo both together.
DO $do_drop_rollback$
BEGIN
    BEGIN
        EXECUTE 'DROP TABLE public.it_nested_rollback';
        RAISE EXCEPTION 'synthetic rollback trigger';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END;
$do_drop_rollback$;

DO $rollback_check$
BEGIN
    IF to_regclass('public.it_nested_rollback') IS NULL THEN
        RAISE EXCEPTION 'nested_ddl_capture: test setup: rolled-back DROP should not have taken effect';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.drop_dependency_manifests m
        JOIN flashback.tracked_tables tt ON tt.tracking_id = m.tracking_id
        WHERE tt.table_name = 'it_nested_rollback'
    ) THEN
        RAISE EXCEPTION 'nested_ddl_capture: rolled-back nested DROP left a dependency manifest behind';
    END IF;
END;
$rollback_check$;

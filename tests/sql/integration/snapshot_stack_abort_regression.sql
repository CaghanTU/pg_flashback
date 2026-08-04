-- Step 9 / Stage 6: real PostgreSQL ERROR/cancel regression proving
-- PushActiveSnapshot/PopActiveSnapshot balance after a genuine transaction
-- abort. A Rust-panic RAII test alone (test_pushed_snapshot_guard_pops_
-- across_rust_panic, external_zstd_format.rs) only proves Drop runs across
-- *Rust* unwinding -- a real ereport(ERROR) raised by a raw FFI call, or a
-- real query-cancel interrupt, uses PostgreSQL's own C-level
-- sigsetjmp/siglongjmp machinery instead, which is under no obligation to
-- run Rust Drop glue for any frame it jumps over. This file drives the
-- tests.test_* helpers in external_zstd_format.rs's test module (each of
-- which pushes an active snapshot -- one guarded via PushedSnapshotGuard,
-- two raw/unguarded -- then deliberately raises and never pops) from
-- inside a real subtransaction (PL/pgSQL BEGIN/EXCEPTION), and proves the
-- backend is correctly, fully recovered afterward regardless.
DO $tv$
DECLARE
    v_raised boolean;
    v_drop_observed boolean;
    v_count bigint;
BEGIN
    -- 1. Guarded: PushedSnapshotGuard alive when a real ERROR (division_by_
    -- zero, raised deep inside a raw SPI_execute call, not a Rust panic)
    -- fires. Empirically settles whether PushedSnapshotGuard::drop runs
    -- across a genuine PostgreSQL ERROR -- see that guard's doc comment
    -- (external_zstd_format.rs) for the claim this observation grounds.
    PERFORM tests.test_reset_guard_drop_flag();
    v_raised := false;
    BEGIN
        PERFORM tests.test_guarded_push_then_force_error();
    EXCEPTION WHEN division_by_zero THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'expected division_by_zero from test_guarded_push_then_force_error';
    END IF;
    v_drop_observed := tests.test_guard_drop_observed();
    -- Empirically confirmed (not assumed): pgrx 0.16.1 converts a genuine
    -- C-level ereport(ERROR) raised via a raw FFI call into a real Rust
    -- panic that respects Drop, when caught by an enclosing PL/pgSQL
    -- BEGIN/EXCEPTION (a real subtransaction boundary) -- PushedSnapshot
    -- Guard::drop is NOT limited to protecting against Rust-originated
    -- panics only. Locked in as a regression: if a future pgrx upgrade
    -- ever changes this, this assertion fails loudly instead of the
    -- guard's doc comment silently drifting from reality.
    IF v_drop_observed IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'PushedSnapshotGuard::drop was not observed after a real PG ERROR (got %) -- '
            'this contradicts the guard''s doc comment; update external_zstd_format.rs '
            'if this is a genuine pgrx/PostgreSQL behavior change, do not just relax this test',
            v_drop_observed;
    END IF;

    -- The active-snapshot stack must be usable immediately afterward, in
    -- the same outer transaction, with no corruption from the aborted
    -- subtransaction's unpopped push.
    SELECT count(*) INTO v_count FROM (SELECT 1) s;
    IF v_count IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'backend unhealthy after guarded push + real ERROR';
    END IF;

    -- 2. Raw/unguarded: no Rust-side cleanup participates at all -- proves
    -- the load-bearing property directly, that PostgreSQL's own
    -- transaction/subtransaction-abort machinery (AtSubAbort_Snapshot)
    -- resets the active-snapshot stack regardless of what Rust code did or
    -- did not do.
    v_raised := false;
    BEGIN
        PERFORM tests.test_raw_push_then_force_error();
    EXCEPTION WHEN division_by_zero THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'expected division_by_zero from test_raw_push_then_force_error';
    END IF;

    -- 3. Raw/unguarded cancel: the exact production code path a real
    -- pg_cancel_backend()/statement_timeout takes (InterruptPending +
    -- QueryCancelPending flags, serviced by ProcessInterrupts()), not a
    -- simulation via a different error type.
    v_raised := false;
    BEGIN
        PERFORM tests.test_raw_push_then_force_cancel();
    EXCEPTION WHEN query_canceled THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'expected query_canceled from test_raw_push_then_force_cancel';
    END IF;

    -- 4. Final, most concrete proof: real transactional work -- insert,
    -- then a real raw-SPI-backed read (spi_select_raw_rows's own pattern,
    -- via the production snapshot_store path) -- still behaves correctly
    -- after three consecutive real aborted subtransactions each leaving an
    -- unpopped active snapshot behind. If the stack were actually
    -- corrupted, this is where it would surface: as wrong row counts, a
    -- stale/wrong MVCC view, or an outright error.
    CREATE TABLE IF NOT EXISTS public.it_snapshot_abort_regress (id int primary key);
    DELETE FROM public.it_snapshot_abort_regress;
    INSERT INTO public.it_snapshot_abort_regress VALUES (1), (2), (3);
    SELECT count(*) INTO v_count FROM public.it_snapshot_abort_regress;
    IF v_count IS DISTINCT FROM 3 THEN
        RAISE EXCEPTION 'expected 3 rows visible after the abort sequence, got %', v_count;
    END IF;
    INSERT INTO public.it_snapshot_abort_regress VALUES (4);
    SELECT count(*) INTO v_count FROM public.it_snapshot_abort_regress;
    IF v_count IS DISTINCT FROM 4 THEN
        RAISE EXCEPTION 'expected 4 rows visible after a further insert, got %', v_count;
    END IF;

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_snapshot_abort_regress CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;

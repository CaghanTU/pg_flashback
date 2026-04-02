-- Test: WAL capture API functions exist and are callable
DO $tv$
DECLARE
    v_exists boolean;
    v_slot_exists boolean;
BEGIN
    -- Verify all WAL functions exist
    IF to_regprocedure('flashback_create_wal_slot(text)') IS NULL THEN
        RAISE EXCEPTION 'flashback_create_wal_slot(text) not found';
    END IF;
    IF to_regprocedure('flashback_drop_wal_slot(text)') IS NULL THEN
        RAISE EXCEPTION 'flashback_drop_wal_slot(text) not found';
    END IF;
    IF to_regprocedure('flashback_wal_consume(integer, text)') IS NULL THEN
        RAISE EXCEPTION 'flashback_wal_consume(int, text) not found';
    END IF;
    IF to_regprocedure('flashback_wal_status(text)') IS NULL THEN
        RAISE EXCEPTION 'flashback_wal_status(text) not found';
    END IF;

    -- flashback_wal_status should return slot_exists=false for non-existent slot
    SELECT ws.slot_exists INTO v_slot_exists
    FROM flashback_wal_status('nonexistent_slot') ws;
    IF v_slot_exists THEN
        RAISE EXCEPTION 'Expected slot_exists=false for nonexistent slot';
    END IF;

    -- flashback_wal_consume should return 0 for non-existent slot
    IF flashback_wal_consume(100, 'nonexistent_slot') <> 0 THEN
        RAISE EXCEPTION 'Expected 0 from wal_consume with no slot';
    END IF;

    -- Verify SECURITY DEFINER on WAL functions
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'flashback_create_wal_slot'
          AND p.prosecdef = true
    ) INTO v_exists;
    IF NOT v_exists THEN
        RAISE EXCEPTION 'flashback_create_wal_slot should be SECURITY DEFINER';
    END IF;

    -- Verify grants to flashback_admin
    SELECT EXISTS (
        SELECT 1 FROM information_schema.role_routine_grants
        WHERE routine_name = 'flashback_wal_consume'
          AND grantee = 'flashback_admin'
    ) INTO v_exists;
    IF NOT v_exists THEN
        RAISE EXCEPTION 'flashback_wal_consume should be granted to flashback_admin';
    END IF;

    RAISE NOTICE 'WAL capture API tests passed (wal_level=%)', current_setting('wal_level');
END;
$tv$;

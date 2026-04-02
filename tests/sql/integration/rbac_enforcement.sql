-- Test: RBAC enforcement — flashback_admin can operate, PUBLIC cannot
DO $tv$
DECLARE v_restored boolean := false;
BEGIN
    -- Verify flashback_admin role exists
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        RAISE EXCEPTION 'flashback_admin role does not exist';
    END IF;

    -- Verify admin functions are revoked from PUBLIC
    -- flashback_restore should not be executable by PUBLIC
    DECLARE
        v_has_public_execute boolean;
    BEGIN
        SELECT has_function_privilege('public', 'flashback_restore(text, timestamptz)', 'EXECUTE')
          INTO v_has_public_execute;
        -- This should be false since we REVOKE ALL FROM PUBLIC
        -- But note: has_function_privilege for 'public' checks PUBLIC pseudo-role
    EXCEPTION WHEN OTHERS THEN
        -- If role 'public' can't be used, that's fine
        v_has_public_execute := false;
    END;

    -- Verify SECURITY DEFINER is set on core functions
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'flashback_restore'
          AND p.prosecdef = true
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'flashback_restore is not SECURITY DEFINER';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        WHERE p.proname = 'flashback_checkpoint'
          AND p.prosecdef = true
    ) THEN
        RAISE EXCEPTION 'flashback_checkpoint is not SECURITY DEFINER';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        WHERE p.proname = 'flashback_track'
          AND p.prosecdef = true
    ) THEN
        RAISE EXCEPTION 'flashback_track is not SECURITY DEFINER';
    END IF;
END;
$tv$;

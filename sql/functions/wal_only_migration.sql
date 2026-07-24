-- =================================================================
-- WAL-only capture finalize: drop legacy trigger DML capture objects
-- that may still exist from a prior install. Fresh installs never
-- recreate them; schema_bootstrap already refuses non-empty
-- staging_events and drops the empty table.
-- =================================================================

DROP FUNCTION IF EXISTS flashback_flush_staging(integer) CASCADE;
DROP FUNCTION IF EXISTS flashback_attach_capture_trigger(text, text) CASCADE;
DROP FUNCTION IF EXISTS flashback_detach_capture_trigger(text, text) CASCADE;
DROP FUNCTION IF EXISTS flashback_capture_insert_trigger() CASCADE;
DROP FUNCTION IF EXISTS flashback_capture_insert_row_trigger() CASCADE;
DROP FUNCTION IF EXISTS flashback_capture_delete_row_trigger() CASCADE;
DROP FUNCTION IF EXISTS flashback_capture_update_trigger() CASCADE;
DROP FUNCTION IF EXISTS flashback_capture_delete_trigger() CASCADE;

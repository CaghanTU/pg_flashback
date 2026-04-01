TRUNCATE flashback.delta_log;
TRUNCATE flashback.snapshots RESTART IDENTITY;
TRUNCATE flashback.tracked_tables;
TRUNCATE flashback.schema_versions RESTART IDENTITY;

CREATE OR REPLACE FUNCTION flashback_test_capture_dml_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_version bigint;
BEGIN
    SELECT schema_version
      INTO v_schema_version
    FROM flashback.tracked_tables
    WHERE rel_oid = TG_RELID
      AND is_active;

    IF v_schema_version IS NULL THEN
        v_schema_version := 1;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid, schema_version, old_data, new_data, committed_at)
        VALUES (clock_timestamp(), 'INSERT', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_RELID, v_schema_version, NULL, to_jsonb(NEW), clock_timestamp());
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid, schema_version, old_data, new_data, committed_at)
        VALUES (clock_timestamp(), 'UPDATE', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_RELID, v_schema_version, to_jsonb(OLD), to_jsonb(NEW), clock_timestamp());
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid, schema_version, old_data, new_data, committed_at)
        VALUES (clock_timestamp(), 'DELETE', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_RELID, v_schema_version, to_jsonb(OLD), NULL, clock_timestamp());
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_test_attach_capture_trigger(target_table regclass)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema text;
    v_table text;
BEGIN
    SELECT n.nspname, c.relname
      INTO v_schema, v_table
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = target_table;

        EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_row ON %I.%I', v_schema, v_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_test_capture ON %I.%I', v_schema, v_table);
    EXECUTE format(
        'CREATE TRIGGER flashback_test_capture AFTER INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION flashback_test_capture_dml_trigger()',
        v_schema,
        v_table
    );
END;
$$;

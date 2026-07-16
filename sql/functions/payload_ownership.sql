-- =================================================================
-- Runtime payload ownership
-- =================================================================
-- Snapshot tables and monthly delta partitions are created after
-- CREATE EXTENSION has finished.  PostgreSQL does not automatically make
-- those relations extension members, so an ordinary pg_dump would otherwise
-- export them as application tables while deliberately omitting the tracking
-- metadata that gives them meaning.  These helpers make membership explicit.
--
-- Imported recovery artifacts are also temporary extension payload while a
-- request is artifact_ready.  They are adopted on acceptance and released in
-- the same transaction immediately before the validated shadow swap.

CREATE OR REPLACE FUNCTION flashback_payload_kind(p_relation regclass)
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_schema  text;
    v_name    text;
    v_relkind "char";
BEGIN
    IF p_relation IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT n.nspname, c.relname, c.relkind
      INTO v_schema, v_name, v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_relation::oid;

    IF NOT FOUND OR v_relkind <> 'r' THEN
        RETURN NULL;
    END IF;

    IF v_schema = 'flashback' AND v_name ~ '^base_snapshot_[0-9]+$' THEN
        RETURN 'base_snapshot';
    END IF;
    IF v_schema = 'flashback' AND v_name ~ '^snap_[0-9]+_[0-9]+$' THEN
        RETURN 'checkpoint_snapshot';
    END IF;
    -- Every non-default direct child of delta_log is runtime recovery
    -- payload, regardless of the partition's operator-chosen name.  The
    -- built-in default partition is created during CREATE EXTENSION and is
    -- already a core extension member.
    IF v_schema = 'flashback'
       AND v_name <> 'delta_log_default'
       AND EXISTS (
           SELECT 1
           FROM pg_inherits i
           WHERE i.inhrelid = p_relation::oid
             AND i.inhparent = to_regclass('flashback.delta_log')
       )
    THEN
        RETURN 'delta_partition';
    END IF;
    IF v_schema = 'flashback_import' AND v_name ~ '^r_[0-9a-f]{16}$' THEN
        RETURN 'restore_artifact';
    END IF;

    RETURN NULL;
END;
$$;

-- O(1) with respect to payload data size: prove that a runtime relation is a
-- recognized object owned by this extension, without scanning its heap.
CREATE OR REPLACE FUNCTION flashback_payload_is_owned(p_relation regclass)
RETURNS boolean
LANGUAGE sql
STABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT public.flashback_payload_kind(p_relation) IS NOT NULL
       AND EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_extension e
              ON e.extname = 'pg_flashback'
             AND c.relowner = e.extowner
            JOIN pg_depend d
              ON d.classid = 'pg_class'::regclass
             AND d.objid = c.oid
             AND d.objsubid = 0
             AND d.refclassid = 'pg_extension'::regclass
             AND d.refobjid = e.oid
             AND d.deptype = 'e'
            WHERE c.oid = p_relation::oid
       )
$$;

-- Catalog-only structural fingerprint for policy-B retirement evidence. This
-- deliberately hashes tuple layout, not heap contents, so its cost is bounded
-- by the number of columns rather than snapshot size.
CREATE OR REPLACE FUNCTION flashback_payload_schema_fingerprint(
    p_relation regclass
)
RETURNS text
LANGUAGE sql
STABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT md5(jsonb_build_object(
        'relkind', c.relkind::text,
        'persistence', c.relpersistence::text,
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'attnum', a.attnum,
                    'name', a.attname,
                    'type_oid', a.atttypid,
                    'typmod', a.atttypmod,
                    'collation_oid', a.attcollation,
                    'not_null', a.attnotnull,
                    'dropped', a.attisdropped,
                    'identity', a.attidentity::text,
                    'generated', a.attgenerated::text,
                    'storage', a.attstorage::text,
                    'compression', a.attcompression::text
                ) ORDER BY a.attnum
            )
            FROM pg_attribute a
            WHERE a.attrelid = c.oid
              AND a.attnum > 0
        ), '[]'::jsonb)
    )::text)
    FROM pg_class c
    WHERE c.oid = p_relation::oid
$$;

CREATE OR REPLACE FUNCTION flashback_own_payload_table(p_relation regclass)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_oid       oid;
    v_schema    text;
    v_name      text;
    v_kind      text;
    v_extension text;
    v_extension_owner name;
BEGIN
    IF p_relation IS NULL THEN
        RAISE EXCEPTION 'flashback_own_payload_table: relation does not exist';
    END IF;
    v_oid := p_relation::oid;

    SELECT n.nspname, c.relname
      INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_own_payload_table: relation % disappeared', v_oid;
    END IF;

    EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', v_schema, v_name);
    IF to_regclass(format('%I.%I', v_schema, v_name))::oid IS DISTINCT FROM v_oid THEN
        RAISE EXCEPTION 'flashback_own_payload_table: relation identity changed while locking';
    END IF;

    v_kind := public.flashback_payload_kind(v_oid::regclass);
    IF v_kind IS NULL THEN
        RAISE EXCEPTION
            'flashback_own_payload_table: %.% is not a recognized pg_flashback payload table',
            v_schema, v_name;
    END IF;

    SELECT pg_get_userbyid(e.extowner)
      INTO STRICT v_extension_owner
    FROM pg_extension e
    WHERE e.extname = 'pg_flashback';

    -- Runtime payload must not remain owned or writable by the delegated role
    -- that created/imported it. Extension membership controls dump/drop
    -- behavior, but does not by itself change the relation owner or ACL.
    EXECUTE format(
        'ALTER TABLE %I.%I OWNER TO %I',
        v_schema, v_name, v_extension_owner
    );
    EXECUTE format(
        'REVOKE ALL PRIVILEGES ON TABLE %I.%I FROM PUBLIC, flashback_admin, flashback_recovery_agent, pg_monitor',
        v_schema, v_name
    );

    SELECT e.extname INTO v_extension
    FROM pg_depend d
    JOIN pg_extension e ON e.oid = d.refobjid
    WHERE d.classid = 'pg_class'::regclass
      AND d.objid = v_oid
      AND d.objsubid = 0
      AND d.refclassid = 'pg_extension'::regclass
      AND d.deptype = 'e';

    IF v_extension = 'pg_flashback' THEN
        RETURN false;
    ELSIF v_extension IS NOT NULL THEN
        RAISE EXCEPTION
            'flashback_own_payload_table: %.% belongs to extension %, not pg_flashback',
            v_schema, v_name, v_extension;
    END IF;

    EXECUTE format(
        'ALTER EXTENSION pg_flashback ADD TABLE %I.%I',
        v_schema, v_name
    );

    IF NOT EXISTS (
        SELECT 1
        FROM pg_depend d
        JOIN pg_extension e ON e.oid = d.refobjid
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = v_oid
          AND d.objsubid = 0
          AND d.refclassid = 'pg_extension'::regclass
          AND d.deptype = 'e'
          AND e.extname = 'pg_flashback'
    ) THEN
        RAISE EXCEPTION
            'flashback_own_payload_table: failed to adopt %.% as extension payload',
            v_schema, v_name;
    END IF;

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_release_payload_table(p_relation regclass)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_oid       oid;
    v_schema    text;
    v_name      text;
    v_kind      text;
    v_extension text;
    v_relation_owner oid;
    v_extension_owner oid;
BEGIN
    IF p_relation IS NULL THEN
        RETURN false;
    END IF;
    v_oid := p_relation::oid;

    SELECT n.nspname, c.relname, c.relowner
      INTO v_schema, v_name, v_relation_owner
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', v_schema, v_name);
    IF to_regclass(format('%I.%I', v_schema, v_name))::oid IS DISTINCT FROM v_oid THEN
        RAISE EXCEPTION 'flashback_release_payload_table: relation identity changed while locking';
    END IF;

    v_kind := public.flashback_payload_kind(v_oid::regclass);
    IF v_kind IS NULL THEN
        RAISE EXCEPTION
            'flashback_release_payload_table: %.% is not a recognized pg_flashback payload table',
            v_schema, v_name;
    END IF;

    SELECT e.extowner
      INTO STRICT v_extension_owner
    FROM pg_extension e
    WHERE e.extname = 'pg_flashback';
    IF v_relation_owner IS DISTINCT FROM v_extension_owner THEN
        RAISE EXCEPTION
            'flashback_release_payload_table: %.% owner changed from extension owner',
            v_schema, v_name;
    END IF;

    SELECT e.extname INTO v_extension
    FROM pg_depend d
    JOIN pg_extension e ON e.oid = d.refobjid
    WHERE d.classid = 'pg_class'::regclass
      AND d.objid = v_oid
      AND d.objsubid = 0
      AND d.refclassid = 'pg_extension'::regclass
      AND d.deptype = 'e';

    IF v_extension IS NULL THEN
        RETURN false;
    ELSIF v_extension <> 'pg_flashback' THEN
        RAISE EXCEPTION
            'flashback_release_payload_table: %.% belongs to extension %, not pg_flashback',
            v_schema, v_name, v_extension;
    END IF;

    EXECUTE format(
        'ALTER EXTENSION pg_flashback DROP TABLE %I.%I',
        v_schema, v_name
    );
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_drop_payload_table(p_relation regclass)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_oid    oid;
    v_schema text;
    v_name   text;
BEGIN
    IF p_relation IS NULL THEN
        RETURN false;
    END IF;
    v_oid := p_relation::oid;

    SELECT n.nspname, c.relname
      INTO v_schema, v_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_oid;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- This also validates the reserved schema/name/kind before any DROP.
    PERFORM public.flashback_release_payload_table(v_oid::regclass);
    EXECUTE format('DROP TABLE %I.%I', v_schema, v_name);
    RETURN true;
END;
$$;

-- Upgrade helper for installations that created runtime payload before those
-- relations were made extension members.  The operation is idempotent and
-- verifies the complete reserved-name set before returning.
CREATE OR REPLACE FUNCTION flashback_adopt_existing_payload_tables()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_relation record;
    v_adopted  integer := 0;
BEGIN
    FOR v_relation IN
        SELECT c.oid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND (
              (n.nspname = 'flashback' AND (
                   c.relname ~ '^base_snapshot_[0-9]+$'
                OR c.relname ~ '^snap_[0-9]+_[0-9]+$'
                OR (
                    c.relname <> 'delta_log_default'
                    AND (
                        c.relname ~ '^delta_log_'
                        OR EXISTS (
                            SELECT 1
                            FROM pg_inherits i
                            WHERE i.inhrelid = c.oid
                              AND i.inhparent = to_regclass('flashback.delta_log')
                        )
                    )
                )
              ))
              OR
              (n.nspname = 'flashback_import'
               AND c.relname ~ '^r_[0-9a-f]{16}$')
          )
        ORDER BY c.oid
    LOOP
        -- A matching delta name that is not actually a delta_log partition is
        -- an invalid reserved object, not something the migration may bless.
        IF public.flashback_payload_kind(v_relation.oid::regclass) IS NULL THEN
            RAISE EXCEPTION
                'flashback_adopt_existing_payload_tables: reserved relation % is not a valid payload',
                v_relation.oid::regclass;
        END IF;
        IF public.flashback_own_payload_table(v_relation.oid::regclass) THEN
            v_adopted := v_adopted + 1;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE public.flashback_payload_kind(c.oid::regclass) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend d
              JOIN pg_extension e ON e.oid = d.refobjid
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid
                AND d.objsubid = 0
                AND d.refclassid = 'pg_extension'::regclass
                AND d.deptype = 'e'
                AND e.extname = 'pg_flashback'
          )
    ) THEN
        RAISE EXCEPTION
            'flashback_adopt_existing_payload_tables: one or more payload tables remain orphaned';
    END IF;

    RETURN v_adopted;
END;
$$;

COMMENT ON FUNCTION flashback_adopt_existing_payload_tables()
    IS 'Adopt legacy runtime snapshot, delta-partition, and restore-artifact tables as pg_flashback extension members; idempotent and fail-closed.';
COMMENT ON FUNCTION flashback_payload_kind(regclass)
    IS '[Internal] Classify a reserved pg_flashback runtime payload relation.';
COMMENT ON FUNCTION flashback_payload_is_owned(regclass)
    IS '[Internal] Verify payload kind, extension membership, and extension ownership without scanning heap data.';
COMMENT ON FUNCTION flashback_payload_schema_fingerprint(regclass)
    IS '[Internal] Return a catalog-only physical tuple-layout fingerprint for retention identity evidence.';
COMMENT ON FUNCTION flashback_own_payload_table(regclass)
    IS '[Internal] Add a validated runtime payload table to pg_flashback extension membership.';
COMMENT ON FUNCTION flashback_release_payload_table(regclass)
    IS '[Internal] Remove a validated runtime payload table from pg_flashback extension membership.';
COMMENT ON FUNCTION flashback_drop_payload_table(regclass)
    IS '[Internal] Safely detach and drop a validated runtime payload table.';

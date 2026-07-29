-- Unqualified table DDL must be bound under the caller's search_path before
-- the ProcessUtility hook switches to the extension owner. Same-named active
-- lifecycles in two schemas must never be selected by recency or by the
-- SECURITY DEFINER search_path.
CREATE SCHEMA IF NOT EXISTS it_oid_s1;
CREATE SCHEMA IF NOT EXISTS it_oid_s2;

CREATE TABLE it_oid_s1.route_truncate(id integer PRIMARY KEY, payload text);
CREATE TABLE it_oid_s2.route_truncate(id integer PRIMARY KEY, payload text);
CREATE TABLE it_oid_s1.route_alter(id integer PRIMARY KEY, payload text);
CREATE TABLE it_oid_s2.route_alter(id integer PRIMARY KEY, payload text);
CREATE TABLE it_oid_s1.route_drop(id integer PRIMARY KEY, payload text);
CREATE TABLE it_oid_s2.route_drop(id integer PRIMARY KEY, payload text);

SELECT flashback_test_bootstrap_lifecycle('it_oid_s1.route_truncate');
SELECT flashback_test_bootstrap_lifecycle('it_oid_s2.route_truncate');
SELECT flashback_test_bootstrap_lifecycle('it_oid_s1.route_alter');
SELECT flashback_test_bootstrap_lifecycle('it_oid_s2.route_alter');
SELECT flashback_test_bootstrap_lifecycle('it_oid_s1.route_drop');
SELECT flashback_test_bootstrap_lifecycle('it_oid_s2.route_drop');

SET LOCAL search_path = it_oid_s2, it_oid_s1, public, pg_catalog;

TRUNCATE TABLE route_truncate;
ALTER TABLE route_alter ADD COLUMN exact_oid_marker integer;
DROP TABLE route_drop;

SET LOCAL search_path = pg_catalog, public, flashback;

DO $check$
DECLARE
    v_s1_truncate bigint;
    v_s2_truncate bigint;
    v_s1_alter bigint;
    v_s2_alter bigint;
    v_s1_drop bigint;
    v_s2_drop bigint;
BEGIN
    SELECT tracking_id INTO STRICT v_s1_truncate
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s1' AND table_name = 'route_truncate' AND is_active;
    SELECT tracking_id INTO STRICT v_s2_truncate
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s2' AND table_name = 'route_truncate' AND is_active;
    SELECT tracking_id INTO STRICT v_s1_alter
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s1' AND table_name = 'route_alter' AND is_active;
    SELECT tracking_id INTO STRICT v_s2_alter
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s2' AND table_name = 'route_alter' AND is_active;
    SELECT tracking_id INTO STRICT v_s1_drop
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s1' AND table_name = 'route_drop' AND is_active;
    SELECT tracking_id INTO STRICT v_s2_drop
    FROM flashback.tracked_tables
    WHERE schema_name = 'it_oid_s2' AND table_name = 'route_drop' AND is_active;

    IF (SELECT count(*) FROM flashback.pending_wal_events
        WHERE tracking_id = v_s2_truncate AND event_type = 'TRUNCATE') <> 1
       OR EXISTS (
           SELECT 1 FROM flashback.pending_wal_events
           WHERE tracking_id = v_s1_truncate AND event_type = 'TRUNCATE'
       )
    THEN
        RAISE EXCEPTION 'unqualified TRUNCATE was not routed exclusively to caller-search_path OID';
    END IF;

    IF (SELECT count(*) FROM flashback.pending_wal_events
        WHERE tracking_id = v_s2_alter AND event_type = 'ALTER') <> 1
       OR EXISTS (
           SELECT 1 FROM flashback.pending_wal_events
           WHERE tracking_id = v_s1_alter AND event_type = 'ALTER'
       )
       OR NOT EXISTS (
           SELECT 1 FROM pg_attribute
           WHERE attrelid = 'it_oid_s2.route_alter'::regclass
             AND attname = 'exact_oid_marker'
             AND NOT attisdropped
       )
       OR EXISTS (
           SELECT 1 FROM pg_attribute
           WHERE attrelid = 'it_oid_s1.route_alter'::regclass
             AND attname = 'exact_oid_marker'
             AND NOT attisdropped
       )
    THEN
        RAISE EXCEPTION 'unqualified ALTER was not routed exclusively to caller-search_path OID';
    END IF;

    IF to_regclass('it_oid_s2.route_drop') IS NOT NULL
       OR to_regclass('it_oid_s1.route_drop') IS NULL
       OR (SELECT count(*) FROM flashback.pending_wal_events
           WHERE tracking_id = v_s2_drop AND event_type = 'DROP') <> 1
       OR EXISTS (
           SELECT 1 FROM flashback.pending_wal_events
           WHERE tracking_id = v_s1_drop AND event_type = 'DROP'
       )
       OR (SELECT count(*) FROM flashback.drop_dependency_manifests
           WHERE tracking_id = v_s2_drop) <> 1
       OR EXISTS (
           SELECT 1 FROM flashback.drop_dependency_manifests
           WHERE tracking_id = v_s1_drop
       )
    THEN
        RAISE EXCEPTION 'unqualified DROP was not routed exclusively to caller-search_path OID';
    END IF;
END;
$check$;

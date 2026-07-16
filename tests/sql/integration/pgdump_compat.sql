-- Test: pg_dump compatibility — no tracking/recovery state is exported.
DROP TABLE IF EXISTS public.it_pgdump_tracking_seq CASCADE;
CREATE TABLE public.it_pgdump_tracking_seq (id integer PRIMARY KEY);

DO $tv$
DECLARE
    v_cnt bigint;
    v_tracking_id bigint;
    v_sequence_last bigint;
BEGIN
    SELECT count(*) INTO v_cnt
    FROM pg_extension e
    CROSS JOIN LATERAL unnest(e.extconfig) AS config_oid
    WHERE e.extname = 'pg_flashback';

    IF v_cnt <> 0 THEN
        RAISE EXCEPTION
            'tracking/recovery state must not be extension config dump data; got % tables',
            v_cnt;
    END IF;

    IF to_regclass(pg_get_serial_sequence('flashback.tracking_lifecycles', 'tracking_id'))
       IS DISTINCT FROM 'flashback.tracking_id_seq'::regclass
    THEN
        RAISE EXCEPTION 'tracking_id sequence is not owned by tracking_lifecycles.tracking_id';
    END IF;

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table
    ) VALUES (
        'public.it_pgdump_tracking_seq'::regclass,
        'public', 'it_pgdump_tracking_seq', NULL
    ) RETURNING tracking_id INTO v_tracking_id;

    SELECT last_value INTO v_sequence_last FROM flashback.tracking_id_seq;
    IF v_sequence_last < v_tracking_id THEN
        RAISE EXCEPTION 'tracking_id sequence is behind allocated identity % (last=%)',
            v_tracking_id, v_sequence_last;
    END IF;
END;
$tv$;

-- Also verify core and coverage metadata tables are extension-owned.  Being
-- extension-owned makes pg_dump recreate their schema via CREATE EXTENSION; it
-- does not opt their data into a logical dump.
DO $tv2$
DECLARE v_cnt bigint;
BEGIN
    SELECT count(*) INTO v_cnt
    FROM pg_depend d
    JOIN pg_class c ON c.oid = d.objid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE d.refclassid = 'pg_extension'::regclass
      AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pg_flashback')
      AND d.classid = 'pg_class'::regclass
      AND n.nspname = 'flashback'
      AND c.relname IN (
          'tracking_lifecycles', 'tracked_tables', 'delta_log', 'snapshots', 'restore_log',
          'staging_events', 'schema_versions', 'capture_streams', 'capture_commits', 'backup_anchors',
          'coverage_generations', 'coverage_gaps', 'pending_wal_events',
          'backup_restore_requests'
      );

    IF v_cnt <> 14 THEN
        RAISE EXCEPTION 'expected 14 core/coverage tables owned by extension, got %', v_cnt;
    END IF;
END;
$tv2$;

-- Runtime payload created after CREATE EXTENSION must also become extension
-- members.  Exercise both automatic adoption and the idempotent upgrade path.
CREATE TABLE flashback.base_snapshot_990001 AS TABLE public.it_pgdump_tracking_seq;
CREATE TABLE flashback.snap_990001_990001 AS TABLE public.it_pgdump_tracking_seq;
CREATE TABLE flashback_import.r_0123456789abcdef AS TABLE public.it_pgdump_tracking_seq;
SELECT flashback__create_range_partition(
    'delta_log_2099_01',
    '2099-01-01 00:00:00+00'::timestamptz,
    '2099-02-01 00:00:00+00'::timestamptz
);

DO $payload$
DECLARE
    v_adopted integer;
    v_orphans text;
BEGIN
    SELECT flashback_adopt_existing_payload_tables() INTO v_adopted;
    IF v_adopted <> 3 THEN
        RAISE EXCEPTION 'expected migration to adopt 3 legacy payloads, got %',
            v_adopted;
    END IF;
    IF flashback_adopt_existing_payload_tables() <> 0 THEN
        RAISE EXCEPTION 'payload migration helper is not idempotent';
    END IF;

    SELECT string_agg(c.oid::regclass::text, ', ' ORDER BY c.oid)
      INTO v_orphans
    FROM pg_class c
    WHERE flashback_payload_kind(c.oid::regclass) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM pg_depend d
          JOIN pg_extension e ON e.oid = d.refobjid
          WHERE d.classid = 'pg_class'::regclass
            AND d.objid = c.oid
            AND d.refclassid = 'pg_extension'::regclass
            AND d.deptype = 'e'
            AND e.extname = 'pg_flashback'
      );
    IF v_orphans IS NOT NULL THEN
        RAISE EXCEPTION 'runtime payload tables remain outside extension ownership: %',
            v_orphans;
    END IF;

    PERFORM flashback_drop_payload_table('flashback.base_snapshot_990001'::regclass);
    PERFORM flashback_drop_payload_table('flashback.snap_990001_990001'::regclass);
    PERFORM flashback_drop_payload_table('flashback.delta_log_2099_01'::regclass);
    PERFORM flashback_drop_payload_table('flashback_import.r_0123456789abcdef'::regclass);
END;
$payload$;

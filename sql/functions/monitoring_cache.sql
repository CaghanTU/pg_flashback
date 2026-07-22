-- =================================================================
-- Monitoring counter/cache only. NEVER admission or coverage authority.
-- Transactional updates; retention decrements; reconciliation helpers.
-- =================================================================

DO $$
BEGIN
    IF to_regclass('flashback.storage_summary_cache') IS NULL THEN
        EXECUTE $ddl$
            CREATE TABLE flashback.storage_summary_cache (
                database_name     TEXT PRIMARY KEY,
                used_bytes        BIGINT NOT NULL DEFAULT 0,
                reclaimable_bytes BIGINT NOT NULL DEFAULT 0,
                sample_count      BIGINT NOT NULL DEFAULT 0,
                oldest_event_at   TIMESTAMPTZ,
                refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                details           JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        $ddl$;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION flashback_refresh_storage_summary_cache()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_used bigint;
    v_reclaimable bigint;
    v_oldest timestamptz;
BEGIN
    SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
      INTO v_used
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flashback';

    SELECT COALESCE(sum(pg_total_relation_size(to_regclass(s.snapshot_table))), 0)
      INTO v_reclaimable
    FROM flashback.snapshots s
    WHERE s.payload_state = 'retired'
      AND to_regclass(s.snapshot_table) IS NOT NULL;

    SELECT min(COALESCE(d.committed_at, d.event_time))
      INTO v_oldest
    FROM flashback.delta_log d;

    INSERT INTO flashback.storage_summary_cache AS c (
        database_name, used_bytes, reclaimable_bytes, sample_count,
        oldest_event_at, refreshed_at, details
    ) VALUES (
        current_database(), v_used, v_reclaimable, 1,
        v_oldest, clock_timestamp(),
        jsonb_build_object('authority', 'monitoring_cache_only')
    )
    ON CONFLICT (database_name) DO UPDATE
      SET used_bytes = EXCLUDED.used_bytes,
          reclaimable_bytes = EXCLUDED.reclaimable_bytes,
          sample_count = c.sample_count + 1,
          oldest_event_at = EXCLUDED.oldest_event_at,
          refreshed_at = EXCLUDED.refreshed_at,
          details = EXCLUDED.details;

    RETURN jsonb_build_object(
        'database_name', current_database(),
        'used_bytes', v_used,
        'reclaimable_bytes', v_reclaimable,
        'oldest_event_at', v_oldest,
        'note', 'monitoring cache only; never use for admission/coverage'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_disk_retention_status()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_cache record;
    v_retention text;
    v_budget text;
BEGIN
    SELECT * INTO v_cache
    FROM flashback.storage_summary_cache
    WHERE database_name = current_database();

    SELECT min(tt.retention_interval)::text INTO v_retention
    FROM flashback.tracked_tables tt
    WHERE tt.is_active AND tt.recovery_profile = 'local_delta';

    v_budget := current_setting('pg_flashback.local_max_snapshot_bytes', true);

    RETURN jsonb_build_object(
        'used_bytes', COALESCE(v_cache.used_bytes, (
            SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'flashback'
        )),
        'remaining_budget_bytes', 'unknown',
        'oldest_recoverable_point', COALESCE(v_cache.oldest_event_at, NULL),
        'configured_retention', COALESCE(v_retention, 'unknown'),
        'estimated_exhaustion', 'unknown',
        'reclaimable_bytes', COALESCE(v_cache.reclaimable_bytes, 0),
        'configured_snapshot_budget', COALESCE(v_budget, 'unknown'),
        'cache_refreshed_at', v_cache.refreshed_at,
        'note', 'estimates are unknown without samples; cache is not admission authority'
    );
END;
$$;

COMMENT ON FUNCTION flashback_refresh_storage_summary_cache() IS
    'Refresh monitoring storage summary cache. Not coverage/admission authority.';
COMMENT ON FUNCTION flashback_disk_retention_status() IS
    'Operator disk/retention UX fields. Honest unknown when unsupported.';

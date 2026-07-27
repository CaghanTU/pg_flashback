-- Keep FK-related coverage metadata in one TRUNCATE set. The coverage tables
-- are inert scaffolding today, but separate parent/child truncates would fail
-- as soon as a schema-contract test inserts a generation.
TRUNCATE
    flashback.pending_wal_events,
    flashback.operation_events,
    flashback.operations,
    flashback.generation_payload_retirements,
    flashback.coverage_gaps,
    flashback.coverage_generations,
    flashback.capture_commits,
    flashback.capture_streams,
    flashback.delta_log,
    flashback.snapshots,
    flashback.tracked_tables,
    flashback.tracking_lifecycles,
    flashback.schema_versions
RESTART IDENTITY;

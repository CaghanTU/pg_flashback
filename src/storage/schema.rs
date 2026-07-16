use pgrx::prelude::*;

extension_sql_file!(
    "../../sql/functions/schema_bootstrap.sql",
    name = "flashback_storage_schema_bootstrap",
    bootstrap,
);

extension_sql_file!(
    "../../sql/functions/payload_ownership.sql",
    name = "flashback_payload_ownership_helpers",
    requires = ["flashback_storage_schema_bootstrap"],
);

extension_sql_file!(
    "../../sql/functions/rbac_grants.sql",
    name = "flashback_rbac_grants",
    requires = [
        "flashback_api_track_capture",
        "flashback_coverage_runtime",
        "flashback_retention_runtime",
        "flashback_backup_restore_api",
        "flashback_restore_planner_api",
        "flashback_restore_lsn_api",
        "flashback_restore_replay_helpers",
        "flashback_payload_ownership_helpers",
        flashback_set_restore_in_progress,
        flashback_is_restore_in_progress,
    ],
    finalize,
);

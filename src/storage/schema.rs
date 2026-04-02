use pgrx::prelude::*;

extension_sql_file!(
    "../../sql/functions/schema_bootstrap.sql",
    name = "flashback_storage_schema_bootstrap",
    bootstrap,
);

extension_sql_file!(
    "../../sql/functions/rbac_grants.sql",
    name = "flashback_rbac_grants",
    requires = [
        "flashback_api_track_capture",
        "flashback_restore_planner_api",
        "flashback_restore_replay_helpers",
        flashback_set_restore_in_progress,
        flashback_is_restore_in_progress,
    ],
    finalize,
);

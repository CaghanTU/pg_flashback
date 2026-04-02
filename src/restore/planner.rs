use pgrx::prelude::*;

extension_sql_file!(
    "../../sql/functions/restore_planner.sql",
    name = "flashback_restore_planner_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_restore_replay_helpers"
    ],
);

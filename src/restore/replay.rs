use pgrx::prelude::*;

extension_sql_file!(
    "../../sql/functions/restore_helpers.sql",
    name = "flashback_restore_replay_helpers",
    requires = ["flashback_storage_schema_bootstrap"],
);

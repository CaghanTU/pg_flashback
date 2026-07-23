use pgrx::prelude::*;

extension_sql_file!(
    "../../sql/functions/restore_planner.sql",
    name = "flashback_restore_planner_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_restore_replay_helpers",
        "flashback_coverage_runtime"
    ],
);

extension_sql_file!(
    "../../sql/functions/restore_verify.sql",
    name = "flashback_restore_verify_helpers",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
    ],
);

extension_sql_file!(
    "../../sql/functions/restore_lsn.sql",
    name = "flashback_restore_lsn_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_restore_replay_helpers",
        "flashback_coverage_runtime",
        "flashback_restore_planner_api",
        "flashback_local_capacity",
        "flashback_worker_admission",
        "flashback_drop_dependency_manifest",
        "flashback_restore_verify_helpers",
    ],
);

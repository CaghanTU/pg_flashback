use pgrx::prelude::*;
use sha2::{Digest, Sha256};

#[pg_extern(immutable, strict)]
fn flashback_sha256(input: &str) -> String {
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

extension_sql_file!(
    "../sql/functions/local_capacity.sql",
    name = "flashback_local_capacity",
    requires = ["flashback_storage_schema_bootstrap"],
);

extension_sql_file!(
    "../sql/functions/worker_admission.sql",
    name = "flashback_worker_admission",
    requires = [
        "flashback_storage_schema_bootstrap",
        flashback_canonical_target_databases,
        flashback_admitted_target_databases,
        flashback_max_worker_pairs,
    ],
);

extension_sql_file!(
    "../sql/functions/api_track_capture.sql",
    name = "flashback_api_track_capture",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_local_capacity",
        "flashback_worker_admission"
    ],
);

extension_sql_file!(
    "../sql/functions/coverage_runtime.sql",
    name = "flashback_coverage_runtime",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_api_track_capture",
        "flashback_local_capacity"
    ],
);

extension_sql_file!(
    "../sql/functions/health_runtime.sql",
    name = "flashback_health_runtime",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_api_track_capture",
        "flashback_coverage_runtime",
        "flashback_local_capacity",
        "flashback_worker_admission"
    ],
);


extension_sql_file!(
    "../sql/functions/retention_runtime.sql",
    name = "flashback_retention_runtime",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_api_track_capture",
        "flashback_coverage_runtime"
    ],
);

extension_sql_file!(
    "../sql/functions/backup_restore_api.sql",
    name = "flashback_backup_restore_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_restore_replay_helpers",
        "flashback_worker_admission",
        flashback_sha256
    ],
);

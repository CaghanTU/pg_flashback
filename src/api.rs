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
    "../sql/functions/drop_dependency_manifest.sql",
    name = "flashback_drop_dependency_manifest",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_worker_admission"
    ],
);

extension_sql_file!(
    "../sql/functions/local_compatibility.sql",
    name = "flashback_local_compatibility",
    requires = ["flashback_storage_schema_bootstrap"],
);

extension_sql_file!(
    "../sql/functions/api_track_capture.sql",
    name = "flashback_api_track_capture",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_local_capacity",
        "flashback_worker_admission",
        "flashback_drop_dependency_manifest",
        "flashback_local_compatibility"
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
    "../sql/functions/operator_diagnosis.sql",
    name = "flashback_operator_diagnosis",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_api_track_capture",
        "flashback_coverage_runtime",
        "flashback_health_runtime",
        "flashback_local_capacity",
        "flashback_worker_admission"
    ],
);

extension_sql_file!(
    "../sql/functions/monitoring_cache.sql",
    name = "flashback_monitoring_cache",
    requires = ["flashback_storage_schema_bootstrap"],
);

extension_sql_file!(
    "../sql/functions/operator_projections.sql",
    name = "flashback_operator_projections",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_operator_diagnosis",
        "flashback_health_runtime",
        "flashback_worker_admission",
        "flashback_monitoring_cache"
    ],
);

extension_sql_file!(
    "../sql/functions/operation_journal.sql",
    name = "flashback_operation_journal",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_health_runtime",
        "flashback_worker_admission"
    ],
);

extension_sql_file!(
    "../sql/functions/maintain_uninstall.sql",
    name = "flashback_maintain_uninstall",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_operator_projections",
        "flashback_coverage_runtime",
        "flashback_operation_journal",
        "flashback_local_capacity",
        "flashback_worker_admission"
    ],
);

extension_sql_file!(
    "../sql/functions/recover_plan.sql",
    name = "flashback_recover_plan_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_operator_diagnosis",
        "flashback_operation_journal",
        "flashback_restore_lsn_api",
        "flashback_restore_verify_helpers",
        "flashback_drop_dependency_manifest",
        "flashback_local_compatibility",
        "flashback_worker_admission",
        flashback_sha256
    ],
);

extension_sql_file!(
    "../sql/functions/unprotect_cleanup.sql",
    name = "flashback_unprotect_cleanup_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_api_track_capture",
        "flashback_operation_journal",
        "flashback_worker_admission",
        "flashback_payload_ownership_helpers"
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

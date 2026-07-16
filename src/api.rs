use pgrx::prelude::*;
use sha2::{Digest, Sha256};

#[pg_extern(immutable, strict)]
fn flashback_sha256(input: &str) -> String {
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

extension_sql_file!(
    "../sql/functions/api_track_capture.sql",
    name = "flashback_api_track_capture",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers"
    ],
);

extension_sql_file!(
    "../sql/functions/coverage_runtime.sql",
    name = "flashback_coverage_runtime",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_api_track_capture"
    ],
);

extension_sql_file!(
    "../sql/functions/backup_restore_api.sql",
    name = "flashback_backup_restore_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_payload_ownership_helpers",
        "flashback_restore_replay_helpers",
        flashback_sha256
    ],
);

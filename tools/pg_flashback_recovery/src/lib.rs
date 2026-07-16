pub mod error;
pub mod model;
mod pgbackrest;
mod probe;

use std::fs;
use std::path::Path;

use serde::de::DeserializeOwned;

use crate::error::RecoveryError;
use crate::model::{
    RecoveryConfig, RecoveryEngine, RestorePlan, RestoreRequest, SnapshotProvider, TargetKind,
};
use crate::pgbackrest::{direct_backup_tree, parse_lsn, read_backup_catalog, select_backup};

pub use crate::probe::run_probe;

const MAX_CONTRACT_BYTES: u64 = 1024 * 1024;

/// Load and deserialize a JSON contract file.
///
/// # Errors
///
/// Returns [`RecoveryError::ReadJson`] when the file cannot be read or does not
/// match the requested contract type.
pub fn load_json<T: DeserializeOwned>(path: &Path) -> Result<T, RecoveryError> {
    let size = fs::metadata(path)
        .map_err(|error| RecoveryError::ReadJson {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?
        .len();
    if size > MAX_CONTRACT_BYTES {
        return Err(RecoveryError::ReadJson {
            path: path.to_path_buf(),
            message: format!("contract exceeds {MAX_CONTRACT_BYTES} bytes"),
        });
    }
    let bytes = fs::read(path).map_err(|error| RecoveryError::ReadJson {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    serde_json::from_slice(&bytes).map_err(|error| RecoveryError::ReadJson {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

/// Validate a request, choose an eligible backup and emit an execution plan.
///
/// # Errors
///
/// Returns a structured [`RecoveryError`] when the request/configuration is
/// invalid, the stanza is busy or unhealthy, no backup covers the target, or
/// the configured work quota is insufficient.
pub fn build_plan(
    config: &RecoveryConfig,
    request: &RestoreRequest,
) -> Result<RestorePlan, RecoveryError> {
    validate_request(request)?;
    if request.target.kind != TargetKind::Lsn {
        return Err(RecoveryError::UnsupportedTarget(format!(
            "{:?}",
            request.target.kind
        )));
    }
    parse_lsn(&request.target.value).map_err(RecoveryError::InvalidRequest)?;

    let probe = run_probe(config)?;
    if !probe.pgbackrest.ok {
        return Err(RecoveryError::CommandFailed {
            program: config.pgbackrest_bin.clone(),
            message: probe.pgbackrest.detail,
        });
    }
    if !probe.repository.ok {
        return Err(RecoveryError::InvalidConfig(probe.repository.detail));
    }

    let backups = read_backup_catalog(config)?;
    let selected = select_backup(&backups, &request.target.value)?;
    if let Some(size) = selected.size_bytes {
        if size > config.max_work_bytes {
            return Err(RecoveryError::WorkQuotaExceeded {
                required: size,
                limit: config.max_work_bytes,
            });
        }
    }

    let direct_tree = direct_backup_tree(&config.repository_path, &config.stanza, &selected.label);
    let tree_is_startable = direct_tree.join("PG_VERSION").is_file();
    let snapshot_eligible = config.snapshot_provider == SnapshotProvider::XfsReflink
        && probe.snapshot_direct_eligible
        && tree_is_startable;
    let (engine, fallback_reason, phases) = if snapshot_eligible {
        (
            RecoveryEngine::SnapshotDirect,
            None,
            vec![
                "acquire_shared_repository_lock",
                "revalidate_backup_label",
                "reflink_clone_backup",
                "native_postgresql_recovery",
                "validate_table",
                "extract_custom_dump",
                "emit_result_manifest",
                "cleanup",
            ],
        )
    } else {
        (
            RecoveryEngine::ClassicRestore,
            Some(if tree_is_startable {
                "snapshot capability or expire coordination is unavailable".to_owned()
            } else {
                "selected backup is not a directly startable plain pg_data tree".to_owned()
            }),
            vec![
                "revalidate_backup_label",
                "pgbackrest_classic_restore",
                "native_postgresql_recovery",
                "validate_table",
                "extract_custom_dump",
                "emit_result_manifest",
                "cleanup",
            ],
        )
    };

    let work_dir = config.work_root.join(&request.request_id);
    let artifact_path = work_dir.join("target-table.dump");
    Ok(RestorePlan {
        status: "planned",
        request_id: request.request_id.clone(),
        profile: config.profile.clone(),
        engine,
        fallback_reason,
        stanza: config.stanza.clone(),
        repository_key: config.repository_key,
        backup_label: selected.label,
        backup_type: selected.backup_type,
        backup_stop_lsn: selected.stop_lsn,
        target: request.target.clone(),
        database: request.database.clone(),
        table: request.table.clone(),
        expected_schema_version: request.expected_schema_version,
        expected_fingerprint: request.expected_fingerprint.clone(),
        work_dir,
        artifact_path,
        phases,
    })
}

/// Execute a previously validated table recovery request.
///
/// # Errors
///
/// The phase-1 skeleton always returns [`RecoveryError::ExecutionDisabled`]
/// after validating that a plan can be built. Execution remains fail-closed
/// until lifecycle, pinning, quota and cancellation gates have E2E coverage.
pub fn restore_table(
    config: &RecoveryConfig,
    request: &RestoreRequest,
) -> Result<RestorePlan, RecoveryError> {
    let _ = build_plan(config, request)?;
    Err(RecoveryError::ExecutionDisabled)
}

fn validate_request(request: &RestoreRequest) -> Result<(), RecoveryError> {
    if !safe_request_id(&request.request_id) {
        return Err(RecoveryError::InvalidRequest(
            "request_id may contain only ASCII letters, digits, underscore and hyphen".to_owned(),
        ));
    }
    for (name, value) in [
        ("database", request.database.as_str()),
        ("table.schema", request.table.schema.as_str()),
        ("table.name", request.table.name.as_str()),
    ] {
        if value.is_empty() || value.contains('\0') {
            return Err(RecoveryError::InvalidRequest(format!(
                "{name} must be non-empty and contain no NUL"
            )));
        }
    }
    if request.table.rel_oid == 0 {
        return Err(RecoveryError::InvalidRequest(
            "table.rel_oid must be greater than zero".to_owned(),
        ));
    }
    if request.target.value.is_empty() {
        return Err(RecoveryError::InvalidRequest(
            "target.value must not be empty".to_owned(),
        ));
    }
    if request.target.observed_at_unix_seconds <= 0 {
        return Err(RecoveryError::InvalidRequest(
            "target.observed_at_unix_seconds must be positive".to_owned(),
        ));
    }
    if request.expected_schema_version == 0 {
        return Err(RecoveryError::InvalidRequest(
            "expected_schema_version must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn safe_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[cfg(test)]
mod tests {
    use crate::model::{RecoveryTarget, RestoreRequest, TableRef, TargetKind};

    use super::validate_request;

    fn request(id: &str) -> RestoreRequest {
        RestoreRequest {
            request_id: id.to_owned(),
            database: "appdb".to_owned(),
            table: TableRef {
                schema: "public".to_owned(),
                name: "orders".to_owned(),
                rel_oid: 16_384,
            },
            target: RecoveryTarget {
                kind: TargetKind::Lsn,
                value: "0/100".to_owned(),
                observed_at_unix_seconds: 1,
                inclusive: true,
            },
            expected_schema_version: 1,
            expected_fingerprint: None,
        }
    }

    #[test]
    fn request_id_cannot_escape_work_root() {
        assert!(validate_request(&request("valid-id_1")).is_ok());
        assert!(validate_request(&request("../escape")).is_err());
        assert!(validate_request(&request("has/slash")).is_err());
    }

    #[test]
    fn request_requires_positive_target_time_and_schema_version() {
        let mut invalid_time = request("invalid-time");
        invalid_time.target.observed_at_unix_seconds = 0;
        assert!(validate_request(&invalid_time).is_err());

        let mut invalid_schema = request("invalid-schema");
        invalid_schema.expected_schema_version = 0;
        assert!(validate_request(&invalid_schema).is_err());
    }
}

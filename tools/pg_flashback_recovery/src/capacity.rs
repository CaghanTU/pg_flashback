use std::fs;
use std::path::Path;

use crate::error::RecoveryError;
use crate::model::RecoveryConfig;

/// Default free-space reserve (64 MiB) used when configs omit `min_free_bytes`.
#[allow(dead_code)] // referenced by unit tests and documents the historical floor
pub(crate) const DEFAULT_MIN_FREE_BYTES: u64 = 64 * 1024 * 1024;

/// Apparent byte size of a file or directory tree, following the same walk as
/// the historical per-request quota check.
pub(crate) fn tree_apparent_bytes(path: &Path) -> Result<u64, RecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "reading work quota metadata",
        message: format!("{}: {error}", path.display()),
    })?;
    if !metadata.is_dir() {
        return Ok(metadata.len());
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| RecoveryError::Io {
        operation: "walking work quota directory",
        message: format!("{}: {error}", path.display()),
    })? {
        let entry = entry.map_err(|error| RecoveryError::Io {
            operation: "reading work quota entry",
            message: error.to_string(),
        })?;
        total = total.saturating_add(tree_apparent_bytes(&entry.path())?);
    }
    Ok(total)
}

/// True for helper-managed request directories under `work_root`.
pub(crate) fn is_request_directory_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Sum apparent bytes for every request directory under `work_root`.
pub(crate) fn work_root_apparent_bytes(config: &RecoveryConfig) -> Result<u64, RecoveryError> {
    if !config.work_root.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(&config.work_root).map_err(|error| RecoveryError::Io {
        operation: "scanning work_root for aggregate quota",
        message: error.to_string(),
    })? {
        let entry = entry.map_err(|error| RecoveryError::Io {
            operation: "reading work_root aggregate entry",
            message: error.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_request_directory_name(&name) {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| RecoveryError::Io {
            operation: "inspecting work_root aggregate entry",
            message: error.to_string(),
        })?;
        if !file_type.is_dir() {
            continue;
        }
        total = total.saturating_add(tree_apparent_bytes(&entry.path())?);
    }
    Ok(total)
}

pub(crate) fn available_work_space(config: &RecoveryConfig) -> Result<u64, RecoveryError> {
    fs2::available_space(&config.work_root).map_err(|error| RecoveryError::Io {
        operation: "checking work filesystem free space",
        message: error.to_string(),
    })
}

pub(crate) fn check_free_space(config: &RecoveryConfig) -> Result<(), RecoveryError> {
    let available = available_work_space(config)?;
    if available < config.min_free_bytes {
        return Err(RecoveryError::FreeSpaceExhausted {
            available,
            required: config.min_free_bytes,
        });
    }
    Ok(())
}

pub(crate) fn check_request_quota(
    config: &RecoveryConfig,
    work_dir: &Path,
) -> Result<(), RecoveryError> {
    let work_bytes = tree_apparent_bytes(work_dir)?;
    if work_bytes > config.max_work_bytes {
        return Err(RecoveryError::WorkQuotaExceeded {
            required: work_bytes,
            limit: config.max_work_bytes,
        });
    }
    Ok(())
}

pub(crate) fn check_work_root_quota(config: &RecoveryConfig) -> Result<(), RecoveryError> {
    let used = work_root_apparent_bytes(config)?;
    if used > config.max_work_root_bytes {
        return Err(RecoveryError::WorkRootQuotaExceeded {
            used,
            limit: config.max_work_root_bytes,
        });
    }
    Ok(())
}

/// Enforce per-request quota, aggregate `work_root` quota and free-space reserve.
pub(crate) fn check_capacity(
    config: &RecoveryConfig,
    work_dir: &Path,
) -> Result<(), RecoveryError> {
    check_request_quota(config, work_dir)?;
    check_work_root_quota(config)?;
    check_free_space(config)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{
        check_free_space, check_request_quota, check_work_root_quota, is_request_directory_name,
        tree_apparent_bytes, work_root_apparent_bytes, DEFAULT_MIN_FREE_BYTES,
    };
    use crate::error::RecoveryError;
    use crate::model::{RecoveryConfig, SnapshotProvider};

    fn test_config(work_root: PathBuf) -> RecoveryConfig {
        RecoveryConfig {
            profile: "test".to_owned(),
            pgbackrest_bin: PathBuf::from("/bin/true"),
            pgbackrest_config: PathBuf::from("/tmp/pgbackrest.conf"),
            pg_bin_dir: PathBuf::from("/usr/bin"),
            cp_bin: PathBuf::from("/bin/cp"),
            repository_path: PathBuf::from("/tmp/repo"),
            repository_key: 1,
            stanza: "test".to_owned(),
            work_root,
            socket_root: PathBuf::from("/tmp/sockets"),
            recovery_port: 1,
            recovery_user: "postgres".to_owned(),
            snapshot_provider: SnapshotProvider::Disabled,
            expire_lock_path: PathBuf::from("/tmp/expire.lock"),
            max_work_bytes: 1024,
            max_work_root_bytes: 2048,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
            artifact_ttl_seconds: 3600,
            max_retained_artifacts: 10,
            max_retained_artifact_bytes: 4096,
            command_timeout_seconds: 1,
            recovery_timeout_seconds: 1,
        }
    }

    #[test]
    fn request_directory_names_reject_hidden_and_path_components() {
        assert!(is_request_directory_name("req-1"));
        assert!(!is_request_directory_name(".locks"));
        assert!(!is_request_directory_name("../x"));
        assert!(!is_request_directory_name("a/b"));
    }

    #[test]
    fn tree_and_aggregate_quota_count_request_directories_only() {
        let root = std::env::temp_dir().join(format!(
            "pgfb-capacity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join(".locks")).unwrap();
        fs::create_dir_all(root.join("req-a")).unwrap();
        fs::write(root.join("req-a").join("blob"), vec![0_u8; 100]).unwrap();
        fs::write(root.join(".locks").join("ignored"), vec![0_u8; 500]).unwrap();

        assert_eq!(tree_apparent_bytes(&root.join("req-a")).unwrap(), 100);
        let config = test_config(root.clone());
        assert_eq!(work_root_apparent_bytes(&config).unwrap(), 100);
        assert!(check_request_quota(&config, &root.join("req-a")).is_ok());

        fs::write(root.join("req-a").join("blob2"), vec![0_u8; 1000]).unwrap();
        assert!(matches!(
            check_request_quota(&config, &root.join("req-a")),
            Err(RecoveryError::WorkQuotaExceeded { .. })
        ));

        fs::create_dir_all(root.join("req-b")).unwrap();
        fs::write(root.join("req-b").join("blob"), vec![0_u8; 1200]).unwrap();
        assert!(matches!(
            check_work_root_quota(&config),
            Err(RecoveryError::WorkRootQuotaExceeded { .. })
        ));

        let mut tight_free = test_config(root.clone());
        tight_free.min_free_bytes = u64::MAX;
        assert!(matches!(
            check_free_space(&tight_free),
            Err(RecoveryError::FreeSpaceExhausted { .. })
        ));

        fs::remove_dir_all(&root).unwrap();
    }
}

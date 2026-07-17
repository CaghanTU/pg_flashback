use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::Component;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::RecoveryError;
use crate::model::{CheckResult, ProbeReport, RecoveryConfig, SnapshotProvider};
use nix::sys::statfs::{statfs, XFS_SUPER_MAGIC};

struct ProbeDirectory(PathBuf);

impl Drop for ProbeDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Probe the configured binaries, storage paths, coordination path and `CoW` support.
///
/// # Errors
///
/// Returns [`RecoveryError::InvalidConfig`] when the operator-owned
/// configuration violates a fail-closed path or naming requirement.
pub fn run_probe(config: &RecoveryConfig) -> Result<ProbeReport, RecoveryError> {
    validate_config(config)?;

    let pgbackrest = version_check(&config.pgbackrest_bin, &["version"]);
    let pgbackrest_config = readable_file(&config.pgbackrest_config, "pgBackRest config");
    let postgres = version_check(&config.pg_bin_dir.join("postgres"), &["--version"]);
    let pg_ctl = version_check(&config.pg_bin_dir.join("pg_ctl"), &["--version"]);
    let psql = version_check(&config.pg_bin_dir.join("psql"), &["--version"]);
    let pg_dump = version_check(&config.pg_bin_dir.join("pg_dump"), &["--version"]);
    let repository = readable_directory(&config.repository_path, "repository");
    let work_root = writable_work_root(&config.work_root);
    let socket_root = writable_work_root(&config.socket_root);
    let expire_coordination = coordination_check(&config.expire_lock_path);
    let copy_on_write = if config.snapshot_provider == SnapshotProvider::XfsReflink && work_root.ok
    {
        reflink_check(&config.cp_bin, &config.work_root)
    } else {
        CheckResult {
            ok: false,
            detail: "snapshot provider disabled or work root unavailable".to_owned(),
        }
    };

    let snapshot_direct_eligible = pgbackrest.ok
        && pgbackrest_config.ok
        && postgres.ok
        && pg_ctl.ok
        && psql.ok
        && pg_dump.ok
        && repository.ok
        && work_root.ok
        && socket_root.ok
        && expire_coordination.ok
        && copy_on_write.ok;

    Ok(ProbeReport {
        status: "ok",
        profile: config.profile.clone(),
        pgbackrest,
        pgbackrest_config,
        postgres,
        pg_ctl,
        psql,
        pg_dump,
        repository,
        work_root,
        socket_root,
        expire_coordination,
        copy_on_write,
        snapshot_direct_eligible,
    })
}

#[allow(clippy::too_many_lines)]
pub fn validate_config(config: &RecoveryConfig) -> Result<(), RecoveryError> {
    for (name, path) in [
        ("pgbackrest_bin", config.pgbackrest_bin.as_path()),
        ("pgbackrest_config", config.pgbackrest_config.as_path()),
        ("pg_bin_dir", config.pg_bin_dir.as_path()),
        ("cp_bin", config.cp_bin.as_path()),
        ("repository_path", config.repository_path.as_path()),
        ("work_root", config.work_root.as_path()),
        ("socket_root", config.socket_root.as_path()),
        ("expire_lock_path", config.expire_lock_path.as_path()),
    ] {
        if !path.is_absolute() {
            return Err(RecoveryError::InvalidConfig(format!(
                "{name} must be an absolute path"
            )));
        }
        if path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(RecoveryError::InvalidConfig(format!(
                "{name} may not contain . or .. path components"
            )));
        }
    }
    if !safe_token(&config.profile) {
        return Err(RecoveryError::InvalidConfig(
            "profile may contain only ASCII letters, digits, underscore and hyphen".to_owned(),
        ));
    }
    if !safe_token(&config.stanza) {
        return Err(RecoveryError::InvalidConfig(
            "stanza may contain only ASCII letters, digits, underscore and hyphen".to_owned(),
        ));
    }
    if config.repository_key == 0 {
        return Err(RecoveryError::InvalidConfig(
            "repository_key must be at least 1".to_owned(),
        ));
    }
    if config.max_work_bytes == 0 {
        return Err(RecoveryError::InvalidConfig(
            "max_work_bytes must be greater than zero".to_owned(),
        ));
    }
    if config.max_work_root_bytes == 0 {
        return Err(RecoveryError::InvalidConfig(
            "max_work_root_bytes must be greater than zero".to_owned(),
        ));
    }
    if config.max_work_root_bytes < config.max_work_bytes {
        return Err(RecoveryError::InvalidConfig(
            "max_work_root_bytes must be at least max_work_bytes".to_owned(),
        ));
    }
    if config.min_free_bytes == 0 {
        return Err(RecoveryError::InvalidConfig(
            "min_free_bytes must be greater than zero".to_owned(),
        ));
    }
    if config.recovery_port == 0 {
        return Err(RecoveryError::InvalidConfig(
            "recovery_port must be greater than zero".to_owned(),
        ));
    }
    if config.recovery_user.is_empty() || config.recovery_user.contains('\0') {
        return Err(RecoveryError::InvalidConfig(
            "recovery_user must be non-empty and contain no NUL".to_owned(),
        ));
    }
    if config.command_timeout_seconds == 0 || config.recovery_timeout_seconds == 0 {
        return Err(RecoveryError::InvalidConfig(
            "command and recovery timeouts must be greater than zero".to_owned(),
        ));
    }
    if config.work_root == config.socket_root {
        return Err(RecoveryError::InvalidConfig(
            "work_root and socket_root must be different directories".to_owned(),
        ));
    }
    for (name, path) in [
        ("repository_path", config.repository_path.as_path()),
        ("work_root", config.work_root.as_path()),
        ("socket_root", config.socket_root.as_path()),
    ] {
        if path == Path::new("/") {
            return Err(RecoveryError::InvalidConfig(format!(
                "{name} may not be the filesystem root"
            )));
        }
    }
    if paths_overlap(&config.work_root, &config.repository_path)
        || paths_overlap(&config.socket_root, &config.repository_path)
        || paths_overlap(&config.socket_root, &config.work_root)
    {
        return Err(RecoveryError::InvalidConfig(
            "repository_path, work_root and socket_root must not overlap".to_owned(),
        ));
    }
    for (name, path) in [
        ("repository_path", config.repository_path.as_path()),
        ("work_root", config.work_root.as_path()),
        ("socket_root", config.socket_root.as_path()),
        ("expire_lock_path", config.expire_lock_path.as_path()),
    ] {
        reject_symlink_components(name, path)?;
    }
    Ok(())
}

fn reject_symlink_components(name: &str, path: &Path) -> Result<(), RecoveryError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(RecoveryError::InvalidConfig(format!(
                    "{name} may not traverse a symlink: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(RecoveryError::InvalidConfig(format!(
                    "cannot inspect {name} component {}: {error}",
                    current.display()
                )));
            }
        }
    }
    Ok(())
}

fn paths_overlap(first: &Path, second: &Path) -> bool {
    first.starts_with(second) || second.starts_with(first)
}

fn version_check(program: &Path, args: &[&str]) -> CheckResult {
    match Command::new(program).args(args).output() {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            CheckResult {
                ok: true,
                detail: if stdout.is_empty() { stderr } else { stdout },
            }
        }
        Ok(output) => CheckResult {
            ok: false,
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        },
        Err(error) => CheckResult {
            ok: false,
            detail: error.to_string(),
        },
    }
}

fn readable_directory(path: &Path, label: &str) -> CheckResult {
    match fs::read_dir(path) {
        Ok(_) => CheckResult {
            ok: true,
            detail: format!("{label} is readable: {}", path.display()),
        },
        Err(error) => CheckResult {
            ok: false,
            detail: format!("{}: {error}", path.display()),
        },
    }
}

fn readable_file(path: &Path, label: &str) -> CheckResult {
    match File::open(path) {
        Ok(_) => CheckResult {
            ok: true,
            detail: format!("{label} is readable: {}", path.display()),
        },
        Err(error) => CheckResult {
            ok: false,
            detail: format!("{}: {error}", path.display()),
        },
    }
}

fn writable_work_root(path: &Path) -> CheckResult {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    match builder.create(path).and_then(|()| {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(std::io::Error::other("path is not a real directory"));
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
    }) {
        Ok(()) => CheckResult {
            ok: true,
            detail: format!("secure writable root is available: {}", path.display()),
        },
        Err(error) => CheckResult {
            ok: false,
            detail: format!("{}: {error}", path.display()),
        },
    }
}

fn coordination_check(lock_path: &Path) -> CheckResult {
    let Some(parent) = lock_path.parent() else {
        return CheckResult {
            ok: false,
            detail: "expire lock has no parent directory".to_owned(),
        };
    };
    if !parent.is_dir() {
        return CheckResult {
            ok: false,
            detail: format!("expire lock parent does not exist: {}", parent.display()),
        };
    }
    if lock_path.is_symlink() {
        return CheckResult {
            ok: false,
            detail: format!("expire lock must not be a symlink: {}", lock_path.display()),
        };
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(lock_path);
    let lock = match lock {
        Ok(lock) => lock,
        Err(error) => {
            return CheckResult {
                ok: false,
                detail: format!("cannot open expire coordination lock: {error}"),
            }
        }
    };
    if let Err(error) = lock.set_permissions(fs::Permissions::from_mode(0o600)) {
        return CheckResult {
            ok: false,
            detail: format!("cannot secure expire coordination lock: {error}"),
        };
    }
    CheckResult {
        ok: true,
        detail: format!(
            "external backup/expire wrapper must take an exclusive lock on {}",
            lock_path.display()
        ),
    }
}

fn reflink_check(cp_bin: &Path, work_root: &Path) -> CheckResult {
    match statfs(work_root) {
        Ok(stats) if stats.filesystem_type() == XFS_SUPER_MAGIC => {}
        Ok(stats) => {
            return CheckResult {
                ok: false,
                detail: format!(
                    "snapshot provider xfs_reflink requires XFS; filesystem type is {:?}",
                    stats.filesystem_type()
                ),
            };
        }
        Err(error) => {
            return CheckResult {
                ok: false,
                detail: format!("cannot identify work-root filesystem: {error}"),
            };
        }
    }
    let probe_path = work_root.join(format!(".reflink-probe-{}", std::process::id()));
    if let Err(error) = fs::create_dir(&probe_path) {
        return CheckResult {
            ok: false,
            detail: format!("cannot create probe directory: {error}"),
        };
    }
    let _guard = ProbeDirectory(probe_path.clone());
    let source = probe_path.join("source");
    let clone = probe_path.join("clone");
    let payload = vec![0xA5; 256 * 1024];

    let write_result = File::create(&source).and_then(|mut file| {
        file.write_all(&payload)?;
        file.sync_all()
    });
    if let Err(error) = write_result {
        return CheckResult {
            ok: false,
            detail: format!("cannot write probe file: {error}"),
        };
    }

    let output = Command::new(cp_bin)
        .arg("--reflink=always")
        .arg("--")
        .arg(&source)
        .arg(&clone)
        .output();
    match output {
        Ok(output) if output.status.success() => match fs::read(&clone) {
            Ok(content) if content == payload => CheckResult {
                ok: true,
                detail: "cp --reflink=always succeeded and content matched".to_owned(),
            },
            Ok(_) => CheckResult {
                ok: false,
                detail: "reflink clone content mismatch".to_owned(),
            },
            Err(error) => CheckResult {
                ok: false,
                detail: format!("cannot read reflink clone: {error}"),
            },
        },
        Ok(output) => CheckResult {
            ok: false,
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        },
        Err(error) => CheckResult {
            ok: false,
            detail: error.to_string(),
        },
    }
}

fn safe_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

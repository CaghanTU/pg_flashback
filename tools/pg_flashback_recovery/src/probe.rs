use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::RecoveryError;
use crate::model::{CheckResult, ProbeReport, RecoveryConfig, SnapshotProvider};

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
    let pg_dump = version_check(&config.pg_bin_dir.join("pg_dump"), &["--version"]);
    let repository = readable_directory(&config.repository_path, "repository");
    let work_root = writable_work_root(&config.work_root);
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
        && pg_dump.ok
        && repository.ok
        && work_root.ok
        && expire_coordination.ok
        && copy_on_write.ok;

    Ok(ProbeReport {
        status: "ok",
        profile: config.profile.clone(),
        pgbackrest,
        pgbackrest_config,
        postgres,
        pg_dump,
        repository,
        work_root,
        expire_coordination,
        copy_on_write,
        snapshot_direct_eligible,
    })
}

pub fn validate_config(config: &RecoveryConfig) -> Result<(), RecoveryError> {
    for (name, path) in [
        ("pgbackrest_bin", config.pgbackrest_bin.as_path()),
        ("pgbackrest_config", config.pgbackrest_config.as_path()),
        ("pg_bin_dir", config.pg_bin_dir.as_path()),
        ("cp_bin", config.cp_bin.as_path()),
        ("repository_path", config.repository_path.as_path()),
        ("work_root", config.work_root.as_path()),
        ("expire_lock_path", config.expire_lock_path.as_path()),
    ] {
        if !path.is_absolute() {
            return Err(RecoveryError::InvalidConfig(format!(
                "{name} must be an absolute path"
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
    Ok(())
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
    match fs::create_dir_all(path) {
        Ok(()) => CheckResult {
            ok: true,
            detail: format!("work root is available: {}", path.display()),
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
    if let Err(error) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
    {
        return CheckResult {
            ok: false,
            detail: format!("cannot open expire coordination lock: {error}"),
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

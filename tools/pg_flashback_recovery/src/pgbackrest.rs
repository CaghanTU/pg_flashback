use std::path::Path;
use std::process::Command;

use serde::Deserialize;

use crate::error::RecoveryError;
use crate::model::RecoveryConfig;

#[derive(Debug, Deserialize)]
struct StanzaInfo {
    name: String,
    #[serde(default)]
    status: StanzaStatus,
    #[serde(default)]
    backup: Vec<BackupInfo>,
}

#[derive(Debug, Default, Deserialize)]
struct StanzaStatus {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    lock: LockStatus,
}

#[derive(Debug, Default, Deserialize)]
struct LockStatus {
    #[serde(default)]
    backup: HeldLock,
}

#[derive(Debug, Default, Deserialize)]
struct HeldLock {
    #[serde(default)]
    held: bool,
}

#[derive(Debug, Deserialize)]
struct BackupInfo {
    label: String,
    #[serde(rename = "type")]
    backup_type: String,
    #[serde(default)]
    error: bool,
    lsn: Option<BackupLsn>,
    info: Option<BackupSizeInfo>,
}

#[derive(Debug, Deserialize)]
struct BackupLsn {
    stop: String,
}

#[derive(Debug, Deserialize)]
struct BackupSizeInfo {
    size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedBackup {
    pub label: String,
    pub backup_type: String,
    pub stop_lsn: String,
    pub size_bytes: Option<u64>,
}

pub fn read_backup_catalog(config: &RecoveryConfig) -> Result<Vec<SelectedBackup>, RecoveryError> {
    let repo_arg = format!("--repo={}", config.repository_key);
    let config_arg = format!("--config={}", config.pgbackrest_config.display());
    let stanza_arg = format!("--stanza={}", config.stanza);
    let output = Command::new(&config.pgbackrest_bin)
        .args([
            config_arg.as_str(),
            stanza_arg.as_str(),
            repo_arg.as_str(),
            "--output=json",
            "info",
        ])
        .output()
        .map_err(|error| RecoveryError::CommandFailed {
            program: config.pgbackrest_bin.clone(),
            message: error.to_string(),
        })?;

    if !output.status.success() {
        return Err(RecoveryError::CommandFailed {
            program: config.pgbackrest_bin.clone(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    let stanzas: Vec<StanzaInfo> = serde_json::from_slice(&output.stdout)
        .map_err(|error| RecoveryError::InvalidPgBackRestJson(error.to_string()))?;
    let stanza = stanzas
        .into_iter()
        .find(|item| item.name == config.stanza)
        .ok_or_else(|| RecoveryError::NoEligibleBackup(config.stanza.clone()))?;

    if stanza.status.lock.backup.held {
        return Err(RecoveryError::RepositoryBusy);
    }
    if stanza.status.code != 0 {
        return Err(RecoveryError::InvalidPgBackRestJson(format!(
            "stanza {} is unhealthy (status code {})",
            config.stanza, stanza.status.code
        )));
    }

    Ok(stanza
        .backup
        .into_iter()
        .filter(|backup| {
            !backup.error && backup.backup_type == "full" && safe_backup_label(&backup.label)
        })
        .filter_map(|backup| {
            let lsn = backup.lsn?;
            Some(SelectedBackup {
                label: backup.label,
                backup_type: backup.backup_type,
                stop_lsn: lsn.stop,
                size_bytes: backup.info.and_then(|info| info.size),
            })
        })
        .collect())
}

fn safe_backup_label(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub fn select_backup(
    backups: &[SelectedBackup],
    target_lsn: &str,
) -> Result<SelectedBackup, RecoveryError> {
    let target = parse_lsn(target_lsn).map_err(RecoveryError::InvalidRequest)?;
    let mut parsed = backups
        .iter()
        .filter_map(|backup| parse_lsn(&backup.stop_lsn).ok().map(|lsn| (lsn, backup)))
        .collect::<Vec<_>>();

    if parsed.is_empty() {
        return Err(RecoveryError::NoEligibleBackup(
            "configured stanza".to_owned(),
        ));
    }
    parsed.sort_unstable_by_key(|(lsn, _)| *lsn);
    parsed
        .into_iter()
        .rev()
        .find(|(lsn, _)| *lsn <= target)
        .map(|(_, backup)| backup.clone())
        .ok_or_else(|| RecoveryError::TargetBeforeOldestBackup {
            target: target_lsn.to_owned(),
        })
}

pub fn direct_backup_tree(repository: &Path, stanza: &str, label: &str) -> std::path::PathBuf {
    repository
        .join("backup")
        .join(stanza)
        .join(label)
        .join("pg_data")
}

pub fn parse_lsn(value: &str) -> Result<u64, String> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| format!("invalid PostgreSQL LSN: {value}"))?;
    if high.is_empty() || low.is_empty() {
        return Err(format!("invalid PostgreSQL LSN: {value}"));
    }
    let high =
        u64::from_str_radix(high, 16).map_err(|_| format!("invalid PostgreSQL LSN: {value}"))?;
    let low =
        u64::from_str_radix(low, 16).map_err(|_| format!("invalid PostgreSQL LSN: {value}"))?;
    if low > u64::from(u32::MAX) || high > u64::from(u32::MAX) {
        return Err(format!("invalid PostgreSQL LSN: {value}"));
    }
    Ok((high << 32) | low)
}

#[cfg(test)]
mod tests {
    use super::{parse_lsn, safe_backup_label, select_backup, SelectedBackup};
    use crate::error::RecoveryError;

    fn backup(label: &str, stop_lsn: &str) -> SelectedBackup {
        SelectedBackup {
            label: label.to_owned(),
            backup_type: "full".to_owned(),
            stop_lsn: stop_lsn.to_owned(),
            size_bytes: Some(100),
        }
    }

    #[test]
    fn parses_postgresql_lsn() {
        assert_eq!(parse_lsn("0/16B6C50").unwrap(), 0x016B_6C50);
        assert_eq!(parse_lsn("A/00000001").unwrap(), (10_u64 << 32) | 1);
        assert!(parse_lsn("bad").is_err());
        assert!(parse_lsn("1/100000000").is_err());
    }

    #[test]
    fn selects_latest_full_backup_not_after_target() {
        let backups = vec![
            backup("first", "0/100"),
            backup("third", "0/300"),
            backup("second", "0/200"),
        ];
        let selected = select_backup(&backups, "0/250").unwrap();
        assert_eq!(selected.label, "second");
    }

    #[test]
    fn target_before_oldest_backup_fails_closed() {
        let error = select_backup(&[backup("first", "0/100")], "0/FF").unwrap_err();
        assert!(matches!(
            error,
            RecoveryError::TargetBeforeOldestBackup { .. }
        ));
    }

    #[test]
    fn backup_label_cannot_escape_repository_root() {
        assert!(safe_backup_label("20260716-091134F_20260716-101010I"));
        assert!(!safe_backup_label("../backup"));
        assert!(!safe_backup_label("label/child"));
    }
}

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("cannot read JSON file {path}: {message}")]
    ReadJson { path: PathBuf, message: String },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid restore request: {0}")]
    InvalidRequest(String),

    #[error("command {program} failed: {message}")]
    CommandFailed { program: PathBuf, message: String },

    #[error("pgBackRest returned invalid JSON: {0}")]
    InvalidPgBackRestJson(String),

    #[error("pgBackRest stanza is busy with backup or expire")]
    RepositoryBusy,

    #[error("target LSN {target} is older than every eligible full backup")]
    TargetBeforeOldestBackup { target: String },

    #[error("no completed full backup with an LSN is available for stanza {0}")]
    NoEligibleBackup(String),

    #[error("target kind {0} is in the contract but is not implemented by the phase-1 planner")]
    UnsupportedTarget(String),

    #[error("selected backup requires {required} bytes, above max_work_bytes={limit}")]
    WorkQuotaExceeded { required: u64, limit: u64 },

    #[error("restore execution is deliberately disabled in the phase-1 skeleton; run plan and complete the execution safety gates first")]
    ExecutionDisabled,

    #[error("I/O error while {operation}: {message}")]
    Io {
        operation: &'static str,
        message: String,
    },
}

impl RecoveryError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ReadJson { .. } => "read_json_failed",
            Self::InvalidConfig(_) => "invalid_config",
            Self::InvalidRequest(_) => "invalid_request",
            Self::CommandFailed { .. } => "command_failed",
            Self::InvalidPgBackRestJson(_) => "invalid_pgbackrest_json",
            Self::RepositoryBusy => "repository_busy",
            Self::TargetBeforeOldestBackup { .. } => "target_before_oldest_backup",
            Self::NoEligibleBackup(_) => "no_eligible_backup",
            Self::UnsupportedTarget(_) => "unsupported_target",
            Self::WorkQuotaExceeded { .. } => "work_quota_exceeded",
            Self::ExecutionDisabled => "execution_disabled",
            Self::Io { .. } => "io_error",
        }
    }
}

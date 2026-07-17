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

    #[error("selected backup changed while acquiring the repository lock: planned={planned}, actual={actual}")]
    SelectedBackupChanged { planned: String, actual: String },

    #[error("target LSN {target} is older than every eligible full backup")]
    TargetBeforeOldestBackup { target: String },

    #[error("no completed full backup with an LSN is available for stanza {0}")]
    NoEligibleBackup(String),

    #[error(
        "target kind {0} is in the contract but is not supported by the first-release executor"
    )]
    UnsupportedTarget(String),

    #[error("selected backup requires {required} bytes, above max_work_bytes={limit}")]
    WorkQuotaExceeded { required: u64, limit: u64 },

    #[error("work_root aggregate usage {used} bytes exceeds max_work_root_bytes={limit}")]
    WorkRootQuotaExceeded { used: u64, limit: u64 },

    #[error("filesystem free space {available} bytes is below min_free_bytes={required}")]
    FreeSpaceExhausted { available: u64, required: u64 },

    #[error("request {0} is already running")]
    RequestAlreadyRunning(String),

    #[error("recovery profile {0} already has an active restore")]
    RecoveryBusy(String),

    #[error("request ID {0} already belongs to a different restore contract")]
    RequestConflict(String),

    #[error("restore was cancelled")]
    Cancelled,

    #[error("{operation} exceeded its {timeout_seconds}s timeout")]
    CommandTimeout {
        operation: &'static str,
        timeout_seconds: u64,
    },

    #[error("unsupported release topology: {0}")]
    UnsupportedTopology(String),

    #[error("recovered table {0} does not exist at the requested target")]
    TableNotFound(String),

    #[error("recovered table OID mismatch: expected={expected}, actual={actual}")]
    TableIdentityMismatch { expected: u32, actual: u32 },

    #[error("recovered fingerprint mismatch: expected={expected}, actual={actual}")]
    FingerprintMismatch { expected: String, actual: String },

    #[error("recovered schema fingerprint mismatch: expected={expected}, actual={actual}")]
    SchemaFingerprintMismatch { expected: String, actual: String },

    #[error("recovery target {target} is unreachable with the available archived WAL")]
    RecoveryTargetUnreachable { target: String },

    #[error("cleanup is incomplete: {0}")]
    CleanupFailed(String),

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
            Self::SelectedBackupChanged { .. } => "selected_backup_changed",
            Self::TargetBeforeOldestBackup { .. } => "target_before_oldest_backup",
            Self::NoEligibleBackup(_) => "no_eligible_backup",
            Self::UnsupportedTarget(_) => "unsupported_target",
            Self::WorkQuotaExceeded { .. } => "work_quota_exceeded",
            Self::WorkRootQuotaExceeded { .. } => "work_root_quota_exceeded",
            Self::FreeSpaceExhausted { .. } => "free_space_exhausted",
            Self::RequestAlreadyRunning(_) => "request_already_running",
            Self::RecoveryBusy(_) => "recovery_busy",
            Self::RequestConflict(_) => "request_conflict",
            Self::Cancelled => "cancelled",
            Self::CommandTimeout { .. } => "command_timeout",
            Self::UnsupportedTopology(_) => "unsupported_topology",
            Self::TableNotFound(_) => "table_not_found",
            Self::TableIdentityMismatch { .. } => "table_identity_mismatch",
            Self::FingerprintMismatch { .. } => "fingerprint_mismatch",
            Self::SchemaFingerprintMismatch { .. } => "schema_fingerprint_mismatch",
            Self::RecoveryTargetUnreachable { .. } => "recovery_target_unreachable",
            Self::CleanupFailed(_) => "cleanup_failed",
            Self::Io { .. } => "io_error",
        }
    }
}

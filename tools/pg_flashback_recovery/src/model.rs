use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryConfig {
    pub profile: String,
    pub pgbackrest_bin: PathBuf,
    pub pgbackrest_config: PathBuf,
    pub pg_bin_dir: PathBuf,
    pub cp_bin: PathBuf,
    pub repository_path: PathBuf,
    pub repository_key: u32,
    pub stanza: String,
    pub work_root: PathBuf,
    pub socket_root: PathBuf,
    pub recovery_port: u16,
    pub recovery_user: String,
    pub snapshot_provider: SnapshotProvider,
    pub expire_lock_path: PathBuf,
    pub max_work_bytes: u64,
    /// Aggregate apparent-byte ceiling across all request directories under
    /// `work_root` (excluding helper metadata directories such as `.locks`).
    #[serde(default = "default_max_work_root_bytes")]
    pub max_work_root_bytes: u64,
    /// Minimum free filesystem bytes that must remain throughout recovery.
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    /// Successful artifacts older than this many seconds become GC-eligible.
    /// Zero disables age-based expiry; count/byte caps still apply.
    #[serde(default = "default_artifact_ttl_seconds")]
    pub artifact_ttl_seconds: u64,
    /// Maximum number of completed artifacts retained under `work_root`.
    /// Zero means unlimited count.
    #[serde(default)]
    pub max_retained_artifacts: u64,
    /// Maximum total completed-artifact dump bytes retained under `work_root`.
    /// Zero means unlimited bytes.
    #[serde(default)]
    pub max_retained_artifact_bytes: u64,
    pub command_timeout_seconds: u64,
    pub recovery_timeout_seconds: u64,
    /// Least-privilege connection used only by proof verification and
    /// coordinated expiration commands. Passwords stay outside this JSON
    /// (for example in an operator-owned PGPASSFILE).
    #[serde(default)]
    pub controller: Option<ControllerConnection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerConnection {
    pub host: PathBuf,
    pub port: u16,
    pub database: String,
    pub user: String,
}

fn default_max_work_root_bytes() -> u64 {
    u64::MAX
}

fn default_min_free_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_artifact_ttl_seconds() -> u64 {
    86_400
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotProvider {
    XfsReflink,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreRequest {
    pub request_id: String,
    pub database: String,
    pub table: TableRef,
    pub target: RecoveryTarget,
    pub expected_schema_version: u64,
    #[serde(default)]
    pub expected_schema_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_fingerprint: Option<String>,
    /// Extension coverage pins echoed verbatim for accept round-trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracking_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_anchor_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
    pub rel_oid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryTarget {
    pub kind: TargetKind,
    pub value: String,
    pub observed_at_unix_seconds: i64,
    pub inclusive: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Lsn,
    Time,
    Xid,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeReport {
    pub status: &'static str,
    pub profile: String,
    pub pgbackrest: CheckResult,
    pub pgbackrest_config: CheckResult,
    pub postgres: CheckResult,
    pub pg_ctl: CheckResult,
    pub psql: CheckResult,
    pub pg_dump: CheckResult,
    pub repository: CheckResult,
    pub work_root: CheckResult,
    pub socket_root: CheckResult,
    pub expire_coordination: CheckResult,
    pub copy_on_write: CheckResult,
    pub snapshot_direct_eligible: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryEngine {
    SnapshotDirect,
    ClassicRestore,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestorePlan {
    pub status: String,
    pub request_id: String,
    pub profile: String,
    pub engine: RecoveryEngine,
    pub fallback_reason: Option<String>,
    pub stanza: String,
    pub repository_key: u32,
    pub backup_label: String,
    pub backup_type: String,
    pub backup_stop_lsn: String,
    pub estimated_backup_bytes: Option<u64>,
    pub target: RecoveryTarget,
    pub database: String,
    pub table: TableRef,
    pub expected_schema_version: u64,
    pub expected_schema_sha256: Option<String>,
    pub expected_fingerprint: Option<String>,
    pub work_dir: PathBuf,
    pub artifact_path: PathBuf,
    pub phases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionDurations {
    pub materialize_ms: u64,
    pub recovery_ms: u64,
    pub validate_ms: u64,
    pub extract_ms: u64,
    pub total_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreResult {
    #[serde(default)]
    pub result_format_version: u32,
    #[serde(default)]
    pub helper_version: String,
    pub status: String,
    pub request: RestoreRequest,
    pub profile: String,
    pub engine: RecoveryEngine,
    pub stanza: String,
    pub repository_key: u32,
    pub backup_label: String,
    pub backup_stop_lsn: String,
    pub postgres_version: String,
    pub pgbackrest_version: String,
    pub recovered_row_count: u64,
    pub recovered_owner: String,
    pub recovered_acl: Vec<RecoveredAclEntry>,
    #[serde(default)]
    pub recovered_schema_sha256: String,
    #[serde(default)]
    pub artifact_schema: String,
    #[serde(default)]
    pub artifact_table: String,
    #[serde(default)]
    pub artifact_schema_sha256: String,
    pub recovered_fingerprint: String,
    pub artifact_path: PathBuf,
    pub artifact_bytes: u64,
    pub artifact_sha256: String,
    pub durations: ExecutionDurations,
    pub cleanup_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveredAclEntry {
    pub grantee: String,
    pub privilege: String,
    pub is_grantable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionState {
    pub request_id: String,
    pub phase: String,
    pub updated_at_unix_seconds: u64,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactPin {
    pub request_id: String,
    pub pinned_at_unix_seconds: u64,
    pub reason: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupVerificationRequest {
    pub request_id: String,
    pub tracking_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupVerificationResult {
    pub result_format_version: u32,
    pub status: String,
    pub verification_kind: String,
    pub request_id: String,
    pub tracking_id: i64,
    pub generation_id: i64,
    pub profile: String,
    pub repository_key: u32,
    pub stanza: String,
    pub backup_label: String,
    pub timeline_id: u32,
    pub manifest_reference: String,
    pub manifest_sha256: String,
    pub verified_lsn: String,
    pub proof_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpireResult {
    pub status: String,
    pub profile: String,
    pub stanza: String,
    pub protected_backup_labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcAction {
    Keep,
    Remove,
    Reconcile,
    SkipBusy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcDecision {
    pub request_id: String,
    pub action: GcAction,
    pub reason: String,
    pub bytes: u64,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GcReport {
    pub status: &'static str,
    pub dry_run: bool,
    pub decisions: Vec<GcDecision>,
    pub removed_requests: u64,
    pub freed_bytes: u64,
    pub retained_artifacts: u64,
    pub retained_artifact_bytes: u64,
    pub audit_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse<'a> {
    pub status: &'static str,
    pub code: &'a str,
    pub message: String,
}

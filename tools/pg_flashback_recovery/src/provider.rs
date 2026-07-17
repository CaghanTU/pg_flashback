//! Internal, unstable physical recovery provider seam.
//!
//! This module deliberately does **not** publish a stable ABI/API. pgBackRest is
//! the only implemented and production-qualified provider in v0.1.0. Dependency
//! identity is generic enough for one FULL backup today and future
//! FULL+differential/incremental or snapshot-hold sets, but those providers are
//! not implemented here.

use serde::{Deserialize, Serialize};

use crate::error::RecoveryError;
use crate::model::{
    AnchorAuditReport, BackupVerificationRequest, BackupVerificationResult, ExpireResult,
    ProbeReport, RecoveryConfig, RestorePlan, RestoreRequest, RestoreResult,
};
use crate::pgbackrest::{self, SelectedBackup};
use crate::{
    audit_anchors, build_plan, expire_backups, restore_table, run_probe, verify_anchor,
    verify_frontier,
};

/// Kind of physical recovery dependency that can be pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    /// Completed FULL backup (the only production-qualified anchor in v0.1.0).
    FullBackup,
    /// Reserved for a future differential member of a backup chain.
    DifferentialBackup,
    /// Reserved for a future incremental member of a backup chain.
    IncrementalBackup,
    /// Contiguous archived WAL range required for recovery.
    ArchivedWalRange,
    /// Reserved for a future filesystem/snapshot hold.
    SnapshotHold,
}

/// Immutable identity of one recovery dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyIdentity {
    pub kind: DependencyKind,
    pub repository_key: u32,
    pub stanza: String,
    pub label: Option<String>,
    pub system_identifier: Option<u64>,
    pub timeline_id: Option<u32>,
    pub start_lsn: Option<String>,
    pub stop_lsn: Option<String>,
    pub manifest_reference: Option<String>,
    pub manifest_sha256: Option<String>,
}

/// One discovered physical recovery point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalRecoveryPoint {
    pub anchor: DependencyIdentity,
    pub required_wal: DependencyIdentity,
    pub eligible: bool,
    pub rejection_reason: Option<String>,
}

/// Set of dependencies that must remain pinned for an admitted restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencySet {
    pub members: Vec<DependencyIdentity>,
}

/// Opaque pin handle for an active or sealed generation dependency set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyPin {
    pub pin_id: String,
    pub dependencies: DependencySet,
}

/// Contiguous verified WAL frontier evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFrontierEvidence {
    pub through_lsn: String,
    pub status: String,
}

/// Internal provider capability surface. Unstable; not a public SDK.
#[allow(dead_code)]
pub(crate) trait PhysicalRecoveryProvider {
    fn name(&self) -> &'static str;

    fn probe(&self, config: &RecoveryConfig) -> Result<ProbeReport, RecoveryError>;

    /// Discover candidate recovery points. Reserved for future multi-candidate
    /// selection; v0.1.0 planning still goes through [`Self::plan`].
    fn discover_recovery_points(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<Vec<PhysicalRecoveryPoint>, RecoveryError>;

    /// Verify one discovered recovery point before materialization.
    fn verify_recovery_point(
        &self,
        config: &RecoveryConfig,
        point: &PhysicalRecoveryPoint,
    ) -> Result<(), RecoveryError>;

    fn plan(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<RestorePlan, RecoveryError>;

    fn materialize_cluster_at_target(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<RestoreResult, RecoveryError>;

    /// Enumerate pinned/required dependencies for an admitted restore.
    fn enumerate_dependencies(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<DependencySet, RecoveryError>;

    fn pin_dependencies(
        &self,
        config: &RecoveryConfig,
        dependencies: &DependencySet,
    ) -> Result<DependencyPin, RecoveryError>;

    /// Release a previously acquired dependency pin.
    fn release_dependencies(
        &self,
        config: &RecoveryConfig,
        pin: &DependencyPin,
    ) -> Result<(), RecoveryError>;

    fn verify_wal_frontier(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<WalFrontierEvidence, RecoveryError>;

    fn activate_anchor(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<BackupVerificationResult, RecoveryError>;

    fn health_audit(&self, config: &RecoveryConfig) -> Result<AnchorAuditReport, RecoveryError>;

    fn expire_unpinned(&self, config: &RecoveryConfig) -> Result<ExpireResult, RecoveryError>;
}

/// Sole v0.1.0 provider: local POSIX pgBackRest repository + archived WAL.
pub(crate) struct PgBackRestProvider;

impl PhysicalRecoveryProvider for PgBackRestProvider {
    fn name(&self) -> &'static str {
        "pgbackrest"
    }

    fn probe(&self, config: &RecoveryConfig) -> Result<ProbeReport, RecoveryError> {
        run_probe(config)
    }

    fn discover_recovery_points(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<Vec<PhysicalRecoveryPoint>, RecoveryError> {
        let catalog = pgbackrest::read_backup_catalog(config)?;
        let selected = match pgbackrest::select_backup(&catalog, &request.target.value) {
            Ok(backup) => backup,
            Err(error) => {
                return Ok(vec![PhysicalRecoveryPoint {
                    anchor: DependencyIdentity {
                        kind: DependencyKind::FullBackup,
                        repository_key: config.repository_key,
                        stanza: config.stanza.clone(),
                        label: None,
                        system_identifier: None,
                        timeline_id: None,
                        start_lsn: None,
                        stop_lsn: None,
                        manifest_reference: None,
                        manifest_sha256: None,
                    },
                    required_wal: wal_dependency(config, None, None),
                    eligible: false,
                    rejection_reason: Some(error.to_string()),
                }]);
            }
        };
        Ok(vec![point_from_selected(config, &selected, true, None)])
    }

    fn verify_recovery_point(
        &self,
        config: &RecoveryConfig,
        point: &PhysicalRecoveryPoint,
    ) -> Result<(), RecoveryError> {
        if point.anchor.kind != DependencyKind::FullBackup {
            return Err(RecoveryError::UnsupportedTopology(
                "v0.1.0 qualifies only FULL backup anchors".to_owned(),
            ));
        }
        if !point.eligible {
            return Err(RecoveryError::UnsupportedTopology(
                point
                    .rejection_reason
                    .clone()
                    .unwrap_or_else(|| "recovery point is not eligible".to_owned()),
            ));
        }
        let label = point.anchor.label.as_deref().ok_or_else(|| {
            RecoveryError::UnsupportedTopology("FULL backup label is required".to_owned())
        })?;
        let catalog = pgbackrest::read_backup_catalog(config)?;
        if !catalog.iter().any(|backup| backup.label == label) {
            return Err(RecoveryError::UnsupportedTopology(format!(
                "FULL backup label {label} is not present in the repository catalog"
            )));
        }
        Ok(())
    }

    fn plan(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<RestorePlan, RecoveryError> {
        build_plan(config, request)
    }

    fn materialize_cluster_at_target(
        &self,
        config: &RecoveryConfig,
        request: &RestoreRequest,
    ) -> Result<RestoreResult, RecoveryError> {
        restore_table(config, request)
    }

    fn enumerate_dependencies(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<DependencySet, RecoveryError> {
        Ok(DependencySet {
            members: vec![
                DependencyIdentity {
                    kind: DependencyKind::FullBackup,
                    repository_key: config.repository_key,
                    stanza: config.stanza.clone(),
                    label: None,
                    system_identifier: None,
                    timeline_id: None,
                    start_lsn: None,
                    stop_lsn: None,
                    manifest_reference: None,
                    manifest_sha256: None,
                },
                wal_dependency(config, None, None),
                DependencyIdentity {
                    kind: DependencyKind::ArchivedWalRange,
                    repository_key: config.repository_key,
                    stanza: format!("{}#{}", config.stanza, request.tracking_id),
                    label: Some(request.request_id.clone()),
                    system_identifier: None,
                    timeline_id: None,
                    start_lsn: None,
                    stop_lsn: None,
                    manifest_reference: None,
                    manifest_sha256: None,
                },
            ],
        })
    }

    fn pin_dependencies(
        &self,
        _config: &RecoveryConfig,
        dependencies: &DependencySet,
    ) -> Result<DependencyPin, RecoveryError> {
        // Durable pin state lives in extension generation metadata; the helper
        // returns a stable identity for audit/GC coordination.
        let pin_id = dependencies
            .members
            .iter()
            .filter_map(|member| member.label.clone())
            .collect::<Vec<_>>()
            .join("+");
        Ok(DependencyPin {
            pin_id: if pin_id.is_empty() {
                "wal-only".to_owned()
            } else {
                pin_id
            },
            dependencies: dependencies.clone(),
        })
    }

    fn release_dependencies(
        &self,
        _config: &RecoveryConfig,
        _pin: &DependencyPin,
    ) -> Result<(), RecoveryError> {
        Ok(())
    }

    fn verify_wal_frontier(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<WalFrontierEvidence, RecoveryError> {
        let result = verify_frontier(config, request)?;
        Ok(WalFrontierEvidence {
            through_lsn: result.verified_lsn,
            status: result.status,
        })
    }

    fn activate_anchor(
        &self,
        config: &RecoveryConfig,
        request: &BackupVerificationRequest,
    ) -> Result<BackupVerificationResult, RecoveryError> {
        verify_anchor(config, request)
    }

    fn health_audit(&self, config: &RecoveryConfig) -> Result<AnchorAuditReport, RecoveryError> {
        audit_anchors(config)
    }

    fn expire_unpinned(&self, config: &RecoveryConfig) -> Result<ExpireResult, RecoveryError> {
        expire_backups(config)
    }
}

#[allow(dead_code)]
fn wal_dependency(
    config: &RecoveryConfig,
    start_lsn: Option<String>,
    stop_lsn: Option<String>,
) -> DependencyIdentity {
    DependencyIdentity {
        kind: DependencyKind::ArchivedWalRange,
        repository_key: config.repository_key,
        stanza: config.stanza.clone(),
        label: None,
        system_identifier: None,
        timeline_id: None,
        start_lsn,
        stop_lsn,
        manifest_reference: None,
        manifest_sha256: None,
    }
}

#[allow(dead_code)]
fn point_from_selected(
    config: &RecoveryConfig,
    selected: &SelectedBackup,
    eligible: bool,
    rejection_reason: Option<String>,
) -> PhysicalRecoveryPoint {
    PhysicalRecoveryPoint {
        anchor: DependencyIdentity {
            kind: DependencyKind::FullBackup,
            repository_key: config.repository_key,
            stanza: config.stanza.clone(),
            label: Some(selected.label.clone()),
            system_identifier: Some(selected.database_system_id),
            timeline_id: None,
            start_lsn: Some(selected.start_lsn.clone()),
            stop_lsn: Some(selected.stop_lsn.clone()),
            manifest_reference: None,
            manifest_sha256: None,
        },
        required_wal: wal_dependency(config, Some(selected.stop_lsn.clone()), None),
        eligible,
        rejection_reason,
    }
}

/// Return the only production-qualified provider for v0.1.0.
pub(crate) fn qualified_provider() -> PgBackRestProvider {
    provider_by_name("pgbackrest").expect("pgbackrest is the only qualified provider")
}

/// Resolve a provider by unstable internal name. Unknown names fail closed.
pub(crate) fn provider_by_name(name: &str) -> Result<PgBackRestProvider, RecoveryError> {
    match name {
        "pgbackrest" | "default" | "" => Ok(PgBackRestProvider),
        other => Err(RecoveryError::UnsupportedTopology(format!(
            "physical recovery provider '{other}' is not implemented; v0.1.0 qualifies only pgbackrest"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SnapshotProvider;
    use std::path::PathBuf;

    fn sample_config() -> RecoveryConfig {
        RecoveryConfig {
            profile: "backup".to_owned(),
            pgbackrest_bin: PathBuf::from("/bin/true"),
            pgbackrest_config: PathBuf::from("/tmp/pgbackrest.conf"),
            pg_bin_dir: PathBuf::from("/usr/bin"),
            cp_bin: PathBuf::from("/bin/cp"),
            repository_path: PathBuf::from("/tmp/repo"),
            repository_key: 1,
            stanza: "main".to_owned(),
            work_root: PathBuf::from("/tmp/work"),
            socket_root: PathBuf::from("/tmp/sock"),
            recovery_port: 1,
            recovery_user: "postgres".to_owned(),
            snapshot_provider: SnapshotProvider::Disabled,
            expire_lock_path: PathBuf::from("/tmp/expire.lock"),
            max_work_bytes: 1024,
            max_work_root_bytes: 4096,
            min_free_bytes: 1024,
            artifact_ttl_seconds: 60,
            max_retained_artifacts: 1,
            max_retained_artifact_bytes: 1024,
            command_timeout_seconds: 1,
            recovery_timeout_seconds: 1,
            controller: None,
            proof_hmac_key_file: None,
        }
    }

    #[test]
    fn only_pgbackrest_is_qualified() {
        assert_eq!(qualified_provider().name(), "pgbackrest");
        assert!(provider_by_name("pgbackrest").is_ok());
        assert!(provider_by_name("barman").is_err());
        assert!(provider_by_name("wal-g").is_err());
    }

    #[test]
    fn release_dependencies_is_a_no_op_today() {
        let config = sample_config();
        let provider = qualified_provider();
        let pin = provider
            .pin_dependencies(
                &config,
                &DependencySet {
                    members: vec![DependencyIdentity {
                        kind: DependencyKind::FullBackup,
                        repository_key: 1,
                        stanza: "main".to_owned(),
                        label: Some("label".to_owned()),
                        system_identifier: None,
                        timeline_id: None,
                        start_lsn: None,
                        stop_lsn: None,
                        manifest_reference: None,
                        manifest_sha256: None,
                    }],
                },
            )
            .expect("pin");
        provider
            .release_dependencies(&config, &pin)
            .expect("release");
        let deps = provider
            .enumerate_dependencies(
                &config,
                &crate::model::BackupVerificationRequest {
                    request_id: "req".to_owned(),
                    tracking_id: 1,
                },
            )
            .expect("enumerate");
        assert!(!deps.members.is_empty());
    }

    #[test]
    fn dependency_kinds_are_extensible_without_selecting_them() {
        let config = sample_config();
        let pin = qualified_provider()
            .pin_dependencies(
                &config,
                &DependencySet {
                    members: vec![DependencyIdentity {
                        kind: DependencyKind::FullBackup,
                        repository_key: 1,
                        stanza: "main".to_owned(),
                        label: Some("20260101-000000F".to_owned()),
                        system_identifier: None,
                        timeline_id: None,
                        start_lsn: None,
                        stop_lsn: None,
                        manifest_reference: None,
                        manifest_sha256: None,
                    }],
                },
            )
            .expect("pin");
        assert_eq!(pin.pin_id, "20260101-000000F");
        assert!(matches!(
            pin.dependencies.members[0].kind,
            DependencyKind::FullBackup
        ));
    }
}

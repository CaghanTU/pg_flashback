use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;

use crate::capacity::{is_request_directory_name, tree_apparent_bytes};
use crate::error::RecoveryError;
use crate::executor::{
    acquire_profile_lock, acquire_request_lock_for_id, prepare_secure_directory,
    reconcile_abandoned_requests, remove_dir_if_exists, remove_file_if_exists, write_json_atomic,
};
use crate::load_json;
use crate::model::{
    ArtifactPin, ExecutionState, GcAction, GcDecision, GcReport, RecoveryConfig, RestoreResult,
};

const PIN_FILE_NAME: &str = "artifact.pin";
const RESULT_FILE_NAME: &str = "result.json";
const ARTIFACT_FILE_NAME: &str = "target-table.dump";
const STATE_FILE_NAME: &str = "state.json";

#[derive(Debug)]
struct ArtifactCandidate {
    request_id: String,
    work_dir: PathBuf,
    completed_at: u64,
    artifact_bytes: u64,
    tree_bytes: u64,
    pinned: bool,
    pin_age_seconds: u64,
}

/// Run helper artifact GC, optionally as a dry-run that only records decisions.
///
/// # Errors
///
/// Returns a structured [`RecoveryError`] when locks, filesystem walks or audit
/// writes fail. Failures never release pins optimistically.
#[allow(clippy::too_many_lines)]
pub fn run_gc(config: &RecoveryConfig, dry_run: bool) -> Result<GcReport, RecoveryError> {
    prepare_secure_directory(&config.work_root)?;
    let audit_dir = config.work_root.join(".gc");
    prepare_secure_directory(&audit_dir)?;
    let audit_path = audit_dir.join("audit.jsonl");
    let _profile_lock = acquire_profile_lock(config)?;

    // Crash leftovers are cleaned first so GC inspects durable result state.
    if !dry_run {
        reconcile_abandoned_requests(config)?;
    }

    let mut decisions = Vec::new();
    let mut candidates = Vec::new();
    let now = unix_seconds()?;

    if config.work_root.exists() {
        for entry in fs::read_dir(&config.work_root).map_err(|error| RecoveryError::Io {
            operation: "scanning work_root for GC",
            message: error.to_string(),
        })? {
            let entry = entry.map_err(|error| RecoveryError::Io {
                operation: "reading GC candidate entry",
                message: error.to_string(),
            })?;
            let Some(request_id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !is_request_directory_name(&request_id) {
                continue;
            }
            let file_type = entry.file_type().map_err(|error| RecoveryError::Io {
                operation: "inspecting GC candidate entry",
                message: error.to_string(),
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let work_dir = entry.path();
            let request_lock = match try_acquire_request_lock(config, &request_id) {
                Ok(lock) => lock,
                Err(RecoveryError::RequestAlreadyRunning(_)) => {
                    let decision = GcDecision {
                        request_id: request_id.clone(),
                        action: GcAction::SkipBusy,
                        reason: "request lock is held by an active restore".to_owned(),
                        bytes: tree_apparent_bytes(&work_dir).unwrap_or(0),
                        dry_run,
                    };
                    append_audit(&audit_path, &decision)?;
                    decisions.push(decision);
                    continue;
                }
                Err(error) => return Err(error),
            };

            // Revalidate durable pin/result state only after the per-request lock.
            let inspection = inspect_request(config, &request_id, &work_dir, now)?;
            drop(request_lock);

            match inspection {
                RequestInspection::Keep { reason, bytes } => {
                    let decision = GcDecision {
                        request_id,
                        action: GcAction::Keep,
                        reason,
                        bytes,
                        dry_run,
                    };
                    append_audit(&audit_path, &decision)?;
                    decisions.push(decision);
                }
                RequestInspection::RemoveNow { reason, bytes } => {
                    let decision = GcDecision {
                        request_id: request_id.clone(),
                        action: GcAction::Remove,
                        reason: reason.clone(),
                        bytes,
                        dry_run,
                    };
                    append_audit(&audit_path, &decision)?;
                    if !dry_run {
                        remove_request_directory(config, &request_id, &work_dir)?;
                    }
                    decisions.push(decision);
                }
                RequestInspection::Completed(candidate) => candidates.push(candidate),
            }
        }
    }

    candidates.sort_by(|left, right| {
        left.completed_at
            .cmp(&right.completed_at)
            .then_with(|| left.request_id.cmp(&right.request_id))
    });

    let mut retained = Vec::new();
    for candidate in candidates {
        let ttl_expired = config.artifact_ttl_seconds > 0
            && now.saturating_sub(candidate.completed_at) >= config.artifact_ttl_seconds;
        let pin_expired = candidate.pinned
            && config.artifact_ttl_seconds > 0
            && candidate.pin_age_seconds >= config.artifact_ttl_seconds;

        if ttl_expired || pin_expired {
            let reason = if pin_expired {
                format!(
                    "artifact pin exceeded TTL of {} seconds",
                    config.artifact_ttl_seconds
                )
            } else {
                format!(
                    "completed artifact exceeded TTL of {} seconds",
                    config.artifact_ttl_seconds
                )
            };
            let decision = GcDecision {
                request_id: candidate.request_id.clone(),
                action: GcAction::Remove,
                reason,
                bytes: candidate.tree_bytes,
                dry_run,
            };
            append_audit(&audit_path, &decision)?;
            if !dry_run {
                let _lock = acquire_request_lock_for_id(config, &candidate.request_id)?;
                // Revalidate pin after lock acquisition immediately before delete.
                if still_protected_completed(config, &candidate.work_dir, now)? {
                    let keep = GcDecision {
                        request_id: candidate.request_id,
                        action: GcAction::Keep,
                        reason: "revalidated as protected after lock acquisition".to_owned(),
                        bytes: candidate.tree_bytes,
                        dry_run,
                    };
                    append_audit(&audit_path, &keep)?;
                    decisions.push(keep);
                    continue;
                }
                remove_request_directory(config, &candidate.request_id, &candidate.work_dir)?;
            }
            decisions.push(decision);
            continue;
        }

        if candidate.pinned {
            let decision = GcDecision {
                request_id: candidate.request_id.clone(),
                action: GcAction::Keep,
                reason: "artifact is pinned awaiting import".to_owned(),
                bytes: candidate.tree_bytes,
                dry_run,
            };
            append_audit(&audit_path, &decision)?;
            decisions.push(decision);
            retained.push(candidate);
            continue;
        }

        retained.push(candidate);
    }

    enforce_retention_caps(config, dry_run, &audit_path, &mut retained, &mut decisions)?;

    let removed_count = decisions
        .iter()
        .filter(|decision| matches!(decision.action, GcAction::Remove | GcAction::Reconcile))
        .count() as u64;
    let freed_bytes = decisions
        .iter()
        .filter(|decision| matches!(decision.action, GcAction::Remove | GcAction::Reconcile))
        .map(|decision| decision.bytes)
        .fold(0_u64, u64::saturating_add);
    let retained_artifacts = retained.len() as u64;
    let retained_artifact_bytes = retained
        .iter()
        .map(|candidate| candidate.artifact_bytes)
        .fold(0_u64, u64::saturating_add);

    Ok(GcReport {
        status: "ok",
        dry_run,
        decisions,
        removed_requests: removed_count,
        freed_bytes,
        retained_artifacts,
        retained_artifact_bytes,
        audit_path,
    })
}

/// Release an awaiting-import pin after the controller has imported the dump.
///
/// # Errors
///
/// Returns [`RecoveryError`] when the request lock cannot be taken or the pin
/// file cannot be removed.
pub fn unpin_artifact(config: &RecoveryConfig, request_id: &str) -> Result<(), RecoveryError> {
    if !is_request_directory_name(request_id) {
        return Err(RecoveryError::InvalidRequest(
            "request_id may contain only ASCII letters, digits, underscore and hyphen".to_owned(),
        ));
    }
    let _profile_lock = acquire_profile_lock(config)?;
    let _request_lock = acquire_request_lock_for_id(config, request_id)?;
    let pin_path = config.work_root.join(request_id).join(PIN_FILE_NAME);
    if pin_path.is_file() {
        remove_file_if_exists(&pin_path)?;
    }
    Ok(())
}

/// Write a durable awaiting-import pin for a completed artifact.
pub(crate) fn write_artifact_pin(
    work_dir: &Path,
    request_id: &str,
    artifact_sha256: &str,
) -> Result<(), RecoveryError> {
    let pin = ArtifactPin {
        request_id: request_id.to_owned(),
        pinned_at_unix_seconds: unix_seconds()?,
        reason: "awaiting_import".to_owned(),
        artifact_sha256: artifact_sha256.to_owned(),
    };
    write_json_atomic(&work_dir.join(PIN_FILE_NAME), &pin)
}

enum RequestInspection {
    Keep { reason: String, bytes: u64 },
    RemoveNow { reason: String, bytes: u64 },
    Completed(ArtifactCandidate),
}

fn inspect_request(
    config: &RecoveryConfig,
    request_id: &str,
    work_dir: &Path,
    now: u64,
) -> Result<RequestInspection, RecoveryError> {
    let tree_bytes = tree_apparent_bytes(work_dir)?;
    let state_path = work_dir.join(STATE_FILE_NAME);
    let result_path = work_dir.join(RESULT_FILE_NAME);
    let artifact_path = work_dir.join(ARTIFACT_FILE_NAME);
    let active_process = work_dir.join("active-process.json");
    let pgdata = work_dir.join("pgdata");

    if active_process.is_file() || pgdata.exists() {
        return Ok(RequestInspection::RemoveNow {
            reason: "abandoned runtime material remains after reconciliation window".to_owned(),
            bytes: tree_bytes,
        });
    }

    if result_path.is_file() {
        let result: RestoreResult = load_json(&result_path)?;
        if result.status == "completed"
            && result.cleanup_complete
            && artifact_path.is_file()
            && result.artifact_path == artifact_path
        {
            let pin_path = work_dir.join(PIN_FILE_NAME);
            let (pinned, pin_age) = if pin_path.is_file() {
                let pin: ArtifactPin = load_json(&pin_path)?;
                if pin.request_id != request_id {
                    return Ok(RequestInspection::RemoveNow {
                        reason: "artifact pin request_id does not match directory".to_owned(),
                        bytes: tree_bytes,
                    });
                }
                (
                    true,
                    now.saturating_sub(pin.pinned_at_unix_seconds),
                )
            } else {
                (false, 0)
            };
            let completed_at = state_completed_at(&state_path)?.unwrap_or(now);
            return Ok(RequestInspection::Completed(ArtifactCandidate {
                request_id: request_id.to_owned(),
                work_dir: work_dir.to_path_buf(),
                completed_at,
                artifact_bytes: result.artifact_bytes,
                tree_bytes,
                pinned,
                pin_age_seconds: pin_age,
            }));
        }
        return Ok(RequestInspection::RemoveNow {
            reason: "result.json is present but is not a reusable completed artifact".to_owned(),
            bytes: tree_bytes,
        });
    }

    if state_path.is_file() {
        let state: ExecutionState = load_json(&state_path)?;
        if matches!(
            state.phase.as_str(),
            "failed" | "cleanup_failed" | "reconciled_after_crash"
        ) {
            return Ok(RequestInspection::RemoveNow {
                reason: format!("request phase {} leaves no reusable artifact", state.phase),
                bytes: tree_bytes,
            });
        }
        return Ok(RequestInspection::Keep {
            reason: format!("request phase {} has no completed artifact yet", state.phase),
            bytes: tree_bytes,
        });
    }

    // Empty or metadata-only leftovers (contracts may remain elsewhere).
    if tree_bytes == 0
        || fs::read_dir(work_dir)
            .map_err(|error| RecoveryError::Io {
                operation: "listing empty request directory",
                message: error.to_string(),
            })?
            .next()
            .is_none()
    {
        return Ok(RequestInspection::RemoveNow {
            reason: "empty request directory".to_owned(),
            bytes: tree_bytes,
        });
    }

    let _ = config;
    Ok(RequestInspection::RemoveNow {
        reason: "orphan request directory without completed result".to_owned(),
        bytes: tree_bytes,
    })
}

fn still_protected_completed(
    config: &RecoveryConfig,
    work_dir: &Path,
    now: u64,
) -> Result<bool, RecoveryError> {
    match inspect_request(
        config,
        work_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(""),
        work_dir,
        now,
    )? {
        RequestInspection::Completed(candidate) => {
            let ttl_expired = config.artifact_ttl_seconds > 0
                && now.saturating_sub(candidate.completed_at) >= config.artifact_ttl_seconds;
            let pin_expired = candidate.pinned
                && config.artifact_ttl_seconds > 0
                && candidate.pin_age_seconds >= config.artifact_ttl_seconds;
            Ok(!(ttl_expired || pin_expired) && candidate.pinned)
        }
        RequestInspection::Keep { .. } => Ok(true),
        RequestInspection::RemoveNow { .. } => Ok(false),
    }
}

fn enforce_retention_caps(
    config: &RecoveryConfig,
    dry_run: bool,
    audit_path: &Path,
    retained: &mut Vec<ArtifactCandidate>,
    decisions: &mut Vec<GcDecision>,
) -> Result<(), RecoveryError> {
    // Evict oldest unpinned completed artifacts first until caps are satisfied.
    retained.sort_by(|left, right| {
        left.completed_at
            .cmp(&right.completed_at)
            .then_with(|| left.request_id.cmp(&right.request_id))
    });

    loop {
        let count = retained.len() as u64;
        let bytes = retained
            .iter()
            .map(|candidate| candidate.artifact_bytes)
            .fold(0_u64, u64::saturating_add);
        let over_count =
            config.max_retained_artifacts > 0 && count > config.max_retained_artifacts;
        let over_bytes = config.max_retained_artifact_bytes > 0
            && bytes > config.max_retained_artifact_bytes;
        if !over_count && !over_bytes {
            break;
        }

        let Some(index) = retained.iter().position(|candidate| !candidate.pinned) else {
            // Fail closed: pinned artifacts are never evicted for capacity caps.
            break;
        };
        let candidate = retained.remove(index);
        let reason = if over_count {
            format!(
                "exceeds max_retained_artifacts={}",
                config.max_retained_artifacts
            )
        } else {
            format!(
                "exceeds max_retained_artifact_bytes={}",
                config.max_retained_artifact_bytes
            )
        };
        let decision = GcDecision {
            request_id: candidate.request_id.clone(),
            action: GcAction::Remove,
            reason,
            bytes: candidate.tree_bytes,
            dry_run,
        };
        append_audit(audit_path, &decision)?;
        if !dry_run {
            let _lock = acquire_request_lock_for_id(config, &candidate.request_id)?;
            let pin_path = candidate.work_dir.join(PIN_FILE_NAME);
            if pin_path.is_file() {
                let keep = GcDecision {
                    request_id: candidate.request_id,
                    action: GcAction::Keep,
                    reason: "pin appeared after selection; refusing deletion".to_owned(),
                    bytes: candidate.tree_bytes,
                    dry_run,
                };
                append_audit(audit_path, &keep)?;
                decisions.push(keep);
                continue;
            }
            remove_request_directory(config, &candidate.request_id, &candidate.work_dir)?;
        }
        decisions.push(decision);
    }

    for candidate in retained.iter() {
        if decisions
            .iter()
            .any(|decision| decision.request_id == candidate.request_id)
        {
            continue;
        }
        let decision = GcDecision {
            request_id: candidate.request_id.clone(),
            action: GcAction::Keep,
            reason: "within retention caps".to_owned(),
            bytes: candidate.tree_bytes,
            dry_run,
        };
        append_audit(audit_path, &decision)?;
        decisions.push(decision);
    }
    Ok(())
}

fn remove_request_directory(
    config: &RecoveryConfig,
    request_id: &str,
    work_dir: &Path,
) -> Result<(), RecoveryError> {
    remove_dir_if_exists(work_dir).map_err(|error| {
        RecoveryError::CleanupFailed(format!(
            "cannot remove GC target {}: {error}",
            work_dir.display()
        ))
    })?;
    let contract = config
        .work_root
        .join(".contracts")
        .join(format!("{request_id}.json"));
    remove_file_if_exists(&contract)?;
    let lock = config
        .work_root
        .join(".locks")
        .join(format!("{request_id}.lock"));
    remove_file_if_exists(&lock)?;
    Ok(())
}

fn state_completed_at(path: &Path) -> Result<Option<u64>, RecoveryError> {
    if !path.is_file() {
        return Ok(None);
    }
    let state: ExecutionState = load_json(path)?;
    if state.phase == "completed" {
        Ok(Some(state.updated_at_unix_seconds))
    } else {
        Ok(None)
    }
}

fn try_acquire_request_lock(
    config: &RecoveryConfig,
    request_id: &str,
) -> Result<File, RecoveryError> {
    let lock_dir = config.work_root.join(".locks");
    prepare_secure_directory(&lock_dir)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .truncate(false)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(lock_dir.join(format!("{request_id}.lock")))
        .map_err(|error| RecoveryError::Io {
            operation: "opening request lock for GC",
            message: error.to_string(),
        })?;
    FileExt::try_lock_exclusive(&lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            RecoveryError::RequestAlreadyRunning(request_id.to_owned())
        } else {
            RecoveryError::Io {
                operation: "locking request for GC",
                message: error.to_string(),
            }
        }
    })?;
    Ok(lock)
}

fn append_audit(path: &Path, decision: &GcDecision) -> Result<(), RecoveryError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening GC audit log",
            message: error.to_string(),
        })?;
    let mut line = serde_json::to_vec(decision).map_err(|error| RecoveryError::Io {
        operation: "serializing GC audit record",
        message: error.to_string(),
    })?;
    line.push(b'\n');
    file.write_all(&line).map_err(|error| RecoveryError::Io {
        operation: "appending GC audit record",
        message: error.to_string(),
    })?;
    file.sync_all().map_err(|error| RecoveryError::Io {
        operation: "fsyncing GC audit log",
        message: error.to_string(),
    })?;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    Ok(())
}

fn unix_seconds() -> Result<u64, RecoveryError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| RecoveryError::Io {
            operation: "reading unix time for GC",
            message: error.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{run_gc, write_artifact_pin, PIN_FILE_NAME};
    use crate::executor::{prepare_secure_directory, write_json_atomic};
    use crate::model::{
        ArtifactPin, ExecutionDurations, ExecutionState, GcAction, RecoveryConfig,
        RecoveryEngine, RestoreRequest, RestoreResult, SnapshotProvider, TableRef, TargetKind,
        RecoveryTarget,
    };

    fn test_config(work_root: PathBuf) -> RecoveryConfig {
        RecoveryConfig {
            profile: "test".to_owned(),
            pgbackrest_bin: PathBuf::from("/bin/true"),
            pgbackrest_config: PathBuf::from("/tmp/pgbackrest.conf"),
            pg_bin_dir: PathBuf::from("/usr/bin"),
            cp_bin: PathBuf::from("/bin/cp"),
            repository_path: PathBuf::from("/tmp/repo-gc"),
            repository_key: 1,
            stanza: "test".to_owned(),
            work_root,
            socket_root: PathBuf::from("/tmp/sockets-gc"),
            recovery_port: 1,
            recovery_user: "postgres".to_owned(),
            snapshot_provider: SnapshotProvider::Disabled,
            expire_lock_path: PathBuf::from("/tmp/expire-gc.lock"),
            max_work_bytes: 1_000_000,
            max_work_root_bytes: 1_000_000,
            min_free_bytes: 1,
            artifact_ttl_seconds: 60,
            max_retained_artifacts: 1,
            max_retained_artifact_bytes: 0,
            command_timeout_seconds: 1,
            recovery_timeout_seconds: 1,
        }
    }

    fn completed_result(request_id: &str, work_dir: &std::path::Path, bytes: u64) -> RestoreResult {
        RestoreResult {
            result_format_version: 3,
            helper_version: env!("CARGO_PKG_VERSION").to_owned(),
            status: "completed".to_owned(),
            request: RestoreRequest {
                request_id: request_id.to_owned(),
                database: "db".to_owned(),
                table: TableRef {
                    schema: "public".to_owned(),
                    name: "t".to_owned(),
                    rel_oid: 1,
                },
                target: RecoveryTarget {
                    kind: TargetKind::Lsn,
                    value: "0/1".to_owned(),
                    observed_at_unix_seconds: 1,
                    inclusive: true,
                },
                expected_schema_version: 1,
                expected_schema_sha256: None,
                expected_fingerprint: None,
                tracking_id: None,
                generation_id: None,
                backup_anchor_id: None,
            },
            profile: "test".to_owned(),
            engine: RecoveryEngine::ClassicRestore,
            stanza: "test".to_owned(),
            repository_key: 1,
            backup_label: "label".to_owned(),
            backup_stop_lsn: "0/1".to_owned(),
            postgres_version: "17".to_owned(),
            pgbackrest_version: "2".to_owned(),
            recovered_row_count: 1,
            recovered_owner: "postgres".to_owned(),
            recovered_acl: vec![],
            recovered_schema_sha256: "a".repeat(64),
            artifact_schema: "flashback_import".to_owned(),
            artifact_table: "r_deadbeefdeadbeef".to_owned(),
            artifact_schema_sha256: "b".repeat(64),
            recovered_fingerprint: "1|2".to_owned(),
            artifact_path: work_dir.join("target-table.dump"),
            artifact_bytes: bytes,
            artifact_sha256: "c".repeat(64),
            durations: ExecutionDurations {
                materialize_ms: 1,
                recovery_ms: 1,
                validate_ms: 1,
                extract_ms: 1,
                total_ms: 4,
            },
            cleanup_complete: true,
        }
    }

    #[test]
    fn gc_removes_expired_unpinned_and_keeps_fresh_pins() {
        let root = std::env::temp_dir().join(format!(
            "pgfb-gc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&root);
        prepare_secure_directory(&root).unwrap();
        let config = test_config(root.clone());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let old_id = "old-artifact";
        let old_dir = root.join(old_id);
        prepare_secure_directory(&old_dir).unwrap();
        fs::write(old_dir.join("target-table.dump"), vec![0_u8; 32]).unwrap();
        write_json_atomic(
            &old_dir.join("result.json"),
            &completed_result(old_id, &old_dir, 32),
        )
        .unwrap();
        write_json_atomic(
            &old_dir.join("state.json"),
            &ExecutionState {
                request_id: old_id.to_owned(),
                phase: "completed".to_owned(),
                updated_at_unix_seconds: 1,
                detail: None,
            },
        )
        .unwrap();

        let pinned_id = "pinned-artifact";
        let pinned_dir = root.join(pinned_id);
        prepare_secure_directory(&pinned_dir).unwrap();
        fs::write(pinned_dir.join("target-table.dump"), vec![1_u8; 32]).unwrap();
        write_json_atomic(
            &pinned_dir.join("result.json"),
            &completed_result(pinned_id, &pinned_dir, 32),
        )
        .unwrap();
        write_json_atomic(
            &pinned_dir.join("state.json"),
            &ExecutionState {
                request_id: pinned_id.to_owned(),
                phase: "completed".to_owned(),
                updated_at_unix_seconds: now,
                detail: None,
            },
        )
        .unwrap();
        write_artifact_pin(&pinned_dir, pinned_id, &"c".repeat(64)).unwrap();

        let dry = run_gc(&config, true).unwrap();
        assert!(dry.decisions.iter().any(|decision| {
            decision.request_id == old_id && decision.action == GcAction::Remove
        }));
        assert!(dry.decisions.iter().any(|decision| {
            decision.request_id == pinned_id && decision.action == GcAction::Keep
        }));
        assert!(old_dir.exists());

        let live = run_gc(&config, false).unwrap();
        assert!(!old_dir.exists());
        assert!(pinned_dir.join(PIN_FILE_NAME).is_file());
        let pin: ArtifactPin = crate::load_json(&pinned_dir.join(PIN_FILE_NAME)).unwrap();
        assert_eq!(pin.reason, "awaiting_import");
        assert!(live.audit_path.is_file());

        fs::remove_dir_all(&root).unwrap();
    }
}

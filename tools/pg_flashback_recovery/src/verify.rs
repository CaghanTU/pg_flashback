use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::RecoveryError;
use crate::executor::{
    acquire_profile_lock, acquire_repository_lock, acquire_request_lock_for_id,
    prepare_secure_directory, write_json_atomic,
};
use crate::model::{
    BackupVerificationRequest, BackupVerificationResult, ExpireResult, RecoveryConfig,
};
use crate::pgbackrest::{parse_lsn, read_repository_metadata, SelectedBackup};
use crate::{load_json, MAX_CONTRACT_BYTES};

const RESULT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
struct AnchorContext {
    tracking_id: i64,
    helper_profile: String,
    generation_id: i64,
    marker_commit_lsn: String,
    database_system_identifier: String,
    timeline_id: u32,
}

#[derive(Debug, Deserialize)]
struct FrontierContext {
    tracking_id: i64,
    helper_profile: String,
    generation_id: i64,
    repository_key: String,
    stanza: String,
    backup_label: String,
    backup_stop_lsn: String,
    anchor_timeline_id: u32,
    database_system_identifier: String,
    wal_segment_size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct InstalledAnchor {
    proof_id: i64,
    generation_id: i64,
}

#[derive(Debug, Deserialize)]
struct InstalledFrontier {
    proof_id: i64,
    status: String,
    generation_id: i64,
    valid_through_lsn: String,
}

/// Verify and consume one repository-derived FULL backup anchor.
///
/// # Errors
///
/// Returns a structured error for configuration, database identity,
/// repository, manifest, lock, or proof-consumption failures.
#[allow(clippy::too_many_lines)]
pub fn verify_anchor(
    config: &RecoveryConfig,
    request: &BackupVerificationRequest,
) -> Result<BackupVerificationResult, RecoveryError> {
    validate_request(request)?;
    prepare_secure_directory(&config.work_root)?;
    let _profile_lock = acquire_profile_lock(config)?;
    let _request_lock = acquire_request_lock_for_id(config, &request.request_id)?;
    if let Some(existing) = existing_result(config, request, "anchor")? {
        return Ok(existing);
    }
    let _repository_lock = acquire_repository_lock(config, false)?;

    let context: AnchorContext = query_json(
        config,
        &format!(
            "SELECT flashback_backup_anchor_verification_context({})::text",
            request.tracking_id
        ),
    )?;
    validate_context_identity(
        config,
        request,
        context.tracking_id,
        &context.helper_profile,
    )?;
    let marker =
        parse_lsn(&context.marker_commit_lsn).map_err(RecoveryError::VerificationFailed)?;
    let system_id = context
        .database_system_identifier
        .parse::<u64>()
        .map_err(|_| {
            RecoveryError::VerificationFailed("invalid live system identifier".to_owned())
        })?;

    let metadata = read_repository_metadata(config)?;
    let mut eligible = metadata
        .backups
        .iter()
        .filter(|backup| backup.backup_type == "full")
        .filter_map(|backup| {
            let start = parse_lsn(&backup.start_lsn).ok()?;
            let stop = parse_lsn(&backup.stop_lsn).ok()?;
            let timeline = backup.archive_start.as_deref().and_then(segment_timeline)?;
            (start > marker
                && stop >= start
                && backup.database_system_id == system_id
                && timeline == context.timeline_id)
                .then_some((start, backup))
        })
        .collect::<Vec<_>>();
    eligible.sort_unstable_by_key(|(start, _)| *start);
    let backup = eligible.first().map(|(_, backup)| *backup).ok_or_else(|| {
        RecoveryError::VerificationFailed(
            "no repository-derived FULL backup starts after the resolved marker".to_owned(),
        )
    })?;

    let manifest = config
        .repository_path
        .join("backup")
        .join(&config.stanza)
        .join(&backup.label)
        .join("backup.manifest");
    let manifest_sha256 = verify_manifest(&manifest, backup, system_id)?;
    let manifest_reference = manifest
        .strip_prefix(&config.repository_path)
        .map_err(|_| {
            RecoveryError::VerificationFailed(
                "manifest escaped the configured repository".to_owned(),
            )
        })?
        .to_string_lossy()
        .into_owned();

    let sql = format!(
        "WITH installed AS (
           SELECT flashback_install_verified_backup_proof(
             {request_id}, {tracking_id}, {profile}, {repository_key}, {stanza},
             {label}, {system_id}, {timeline}, {manifest_reference}, {manifest_sha},
             {start_lsn}::pg_lsn, {stop_lsn}::pg_lsn, clock_timestamp(),
             jsonb_build_object('verification_source','recovery_helper',
                                'repository_lock','shared',
                                'manifest_verified',true)
           ) AS proof_id
         )
         SELECT jsonb_build_object(
           'proof_id', proof_id,
           'generation_id', flashback_consume_verified_backup_proof(proof_id)
         )::text FROM installed",
        request_id = sql_literal(&request.request_id),
        tracking_id = request.tracking_id,
        profile = sql_literal(&config.profile),
        repository_key = sql_literal(&config.repository_key.to_string()),
        stanza = sql_literal(&config.stanza),
        label = sql_literal(&backup.label),
        system_id = system_id,
        timeline = context.timeline_id,
        manifest_reference = sql_literal(&manifest_reference),
        manifest_sha = sql_literal(&manifest_sha256),
        start_lsn = sql_literal(&backup.start_lsn),
        stop_lsn = sql_literal(&backup.stop_lsn),
    );
    let installed: InstalledAnchor = query_json(config, &sql)?;
    if installed.generation_id != context.generation_id {
        return Err(RecoveryError::VerificationFailed(
            "consumed anchor generation differs from verification context".to_owned(),
        ));
    }
    let result = BackupVerificationResult {
        result_format_version: RESULT_FORMAT_VERSION,
        status: "verified".to_owned(),
        verification_kind: "anchor".to_owned(),
        request_id: request.request_id.clone(),
        tracking_id: request.tracking_id,
        generation_id: installed.generation_id,
        profile: config.profile.clone(),
        repository_key: config.repository_key,
        stanza: config.stanza.clone(),
        backup_label: backup.label.clone(),
        timeline_id: context.timeline_id,
        manifest_reference,
        manifest_sha256,
        verified_lsn: backup.stop_lsn.clone(),
        proof_id: installed.proof_id,
    };
    persist_result(config, &result)?;
    Ok(result)
}

/// Verify and consume the contiguous archived-WAL frontier.
///
/// # Errors
///
/// Returns a structured error for configuration, database identity,
/// repository, archive continuity, lock, or proof-consumption failures.
#[allow(clippy::too_many_lines)]
pub fn verify_frontier(
    config: &RecoveryConfig,
    request: &BackupVerificationRequest,
) -> Result<BackupVerificationResult, RecoveryError> {
    validate_request(request)?;
    prepare_secure_directory(&config.work_root)?;
    let _profile_lock = acquire_profile_lock(config)?;
    let _request_lock = acquire_request_lock_for_id(config, &request.request_id)?;
    if let Some(existing) = existing_result(config, request, "frontier")? {
        return Ok(existing);
    }
    let _repository_lock = acquire_repository_lock(config, false)?;

    let context: FrontierContext = query_json(
        config,
        &format!(
            "SELECT flashback_backup_frontier_verification_context({})::text",
            request.tracking_id
        ),
    )?;
    validate_context_identity(
        config,
        request,
        context.tracking_id,
        &context.helper_profile,
    )?;
    if context.repository_key != config.repository_key.to_string()
        || context.stanza != config.stanza
    {
        return Err(RecoveryError::VerificationFailed(
            "active anchor repository identity differs from helper configuration".to_owned(),
        ));
    }

    let metadata = match read_repository_metadata(config) {
        Ok(metadata) => metadata,
        Err(error) => {
            freeze_repository_failure(
                config,
                request.tracking_id,
                "repository metadata unavailable",
            );
            return Err(error);
        }
    };
    let system_id = context
        .database_system_identifier
        .parse::<u64>()
        .map_err(|_| {
            RecoveryError::VerificationFailed("invalid live system identifier".to_owned())
        })?;
    if metadata.archive.database_system_id != system_id {
        return Err(RecoveryError::VerificationFailed(
            "archive system identifier differs from the active PostgreSQL cluster".to_owned(),
        ));
    }
    let (frontier_lsn, timeline, archive_sha) = match contiguous_archive_frontier(
        config,
        &metadata.archive.archive_id,
        &context.backup_stop_lsn,
        context.wal_segment_size_bytes,
    ) {
        Ok(frontier) => frontier,
        Err(error) => {
            freeze_repository_failure(
                config,
                request.tracking_id,
                "archived WAL proof unavailable",
            );
            return Err(error);
        }
    };

    let sql = format!(
        "WITH installed AS (
           SELECT flashback_install_verified_wal_frontier_proof(
             {request_id}, {tracking_id}, {generation_id}, {profile},
             {repository_key}, {stanza}, {timeline}, {frontier}::pg_lsn,
             {archive_sha}, clock_timestamp(),
             jsonb_build_object('verification_source','recovery_helper',
                                'repository_lock','shared',
                                'archive_contiguous',true)
           ) AS proof_id
         ), consumed AS (
           SELECT proof_id,
                  flashback_consume_verified_wal_frontier_proof(proof_id) AS result
           FROM installed
         )
         SELECT jsonb_build_object(
           'proof_id', proof_id,
           'status', result->>'status',
           'generation_id', (result->>'generation_id')::bigint,
           'valid_through_lsn', result->>'valid_through_lsn'
         )::text FROM consumed",
        request_id = sql_literal(&request.request_id),
        tracking_id = request.tracking_id,
        generation_id = context.generation_id,
        profile = sql_literal(&config.profile),
        repository_key = sql_literal(&config.repository_key.to_string()),
        stanza = sql_literal(&config.stanza),
        timeline = timeline,
        frontier = sql_literal(&frontier_lsn),
        archive_sha = sql_literal(&archive_sha),
    );
    let installed: InstalledFrontier = query_json(config, &sql)?;
    let result = BackupVerificationResult {
        result_format_version: RESULT_FORMAT_VERSION,
        status: installed.status.clone(),
        verification_kind: "frontier".to_owned(),
        request_id: request.request_id.clone(),
        tracking_id: request.tracking_id,
        generation_id: installed.generation_id,
        profile: config.profile.clone(),
        repository_key: config.repository_key,
        stanza: config.stanza.clone(),
        backup_label: context.backup_label,
        timeline_id: timeline,
        manifest_reference: String::new(),
        manifest_sha256: archive_sha,
        verified_lsn: installed.valid_through_lsn,
        proof_id: installed.proof_id,
    };
    persist_result(config, &result)?;
    if installed.status == "timeline_mismatch" || timeline != context.anchor_timeline_id {
        return Err(RecoveryError::VerificationFailed(
            "repository timeline mismatches the active anchor; coverage was frozen".to_owned(),
        ));
    }
    Ok(result)
}

/// Run coordinated expiration only when no active generation pins a backup.
///
/// # Errors
///
/// Returns a structured error when locks, controller inspection, active pins,
/// or the pgBackRest expiration command prevent completion.
pub fn expire_backups(config: &RecoveryConfig) -> Result<ExpireResult, RecoveryError> {
    prepare_secure_directory(&config.work_root)?;
    let _profile_lock = acquire_profile_lock(config)?;
    let _repository_lock = acquire_repository_lock(config, true)?;
    let labels: Vec<String> = query_json(
        config,
        "SELECT to_json(flashback_active_backup_labels())::text",
    )?;
    if !labels.is_empty() {
        return Err(RecoveryError::ProtectedBackups(labels.join(",")));
    }
    let output = Command::new(&config.pgbackrest_bin)
        .arg(format!("--config={}", config.pgbackrest_config.display()))
        .arg(format!("--stanza={}", config.stanza))
        .arg(format!("--repo={}", config.repository_key))
        .arg("expire")
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
    Ok(ExpireResult {
        status: "expired".to_owned(),
        profile: config.profile.clone(),
        stanza: config.stanza.clone(),
        protected_backup_labels: Vec::new(),
    })
}

fn validate_request(request: &BackupVerificationRequest) -> Result<(), RecoveryError> {
    if request.tracking_id <= 0
        || request.request_id.is_empty()
        || request.request_id.len() > 128
        || !request
            .request_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(RecoveryError::InvalidRequest(
            "verification request requires a positive tracking_id and safe request_id".to_owned(),
        ));
    }
    Ok(())
}

fn validate_context_identity(
    config: &RecoveryConfig,
    request: &BackupVerificationRequest,
    tracking_id: i64,
    helper_profile: &str,
) -> Result<(), RecoveryError> {
    if tracking_id != request.tracking_id || helper_profile != config.profile {
        return Err(RecoveryError::VerificationFailed(
            "tracking lifecycle or helper profile changed during verification".to_owned(),
        ));
    }
    Ok(())
}

fn freeze_repository_failure(config: &RecoveryConfig, tracking_id: i64, detail: &str) {
    let sql = format!(
        "SELECT flashback_freeze_backup_generation(
           {tracking_id}, 'repository_verification_failed',
           jsonb_build_object('helper_detail', {detail})
         )::text",
        detail = sql_literal(detail),
    );
    let _: Result<serde_json::Value, RecoveryError> = query_json(config, &sql);
}

fn query_json<T: for<'de> Deserialize<'de>>(
    config: &RecoveryConfig,
    sql: &str,
) -> Result<T, RecoveryError> {
    let controller = config.controller.as_ref().ok_or_else(|| {
        RecoveryError::InvalidConfig(
            "controller connection is required for proof verification".to_owned(),
        )
    })?;
    let psql = config.pg_bin_dir.join("psql");
    let mut child = Command::new(&psql)
        .arg("-X")
        .arg("-qAt")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-h")
        .arg(&controller.host)
        .arg("-p")
        .arg(controller.port.to_string())
        .arg("-U")
        .arg(&controller.user)
        .arg("-d")
        .arg(&controller.database)
        .env("PGAPPNAME", "pg_flashback_recovery_verifier")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| RecoveryError::CommandFailed {
            program: psql.clone(),
            message: error.to_string(),
        })?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| RecoveryError::Io {
            operation: "opening verifier SQL stdin",
            message: "stdin unavailable".to_owned(),
        })?
        .write_all(format!("{sql};\n").as_bytes())
        .map_err(|error| RecoveryError::Io {
            operation: "writing verifier SQL",
            message: error.to_string(),
        })?;
    let output = child
        .wait_with_output()
        .map_err(|error| RecoveryError::CommandFailed {
            program: psql.clone(),
            message: error.to_string(),
        })?;
    if !output.status.success() {
        return Err(RecoveryError::CommandFailed {
            program: psql,
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    serde_json::from_slice(output.stdout.trim_ascii()).map_err(|error| {
        RecoveryError::VerificationFailed(format!("controller returned invalid JSON: {error}"))
    })
}

fn verify_manifest(
    path: &Path,
    backup: &SelectedBackup,
    system_id: u64,
) -> Result<String, RecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "reading backup manifest metadata",
        message: error.to_string(),
    })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_CONTRACT_BYTES
    {
        return Err(RecoveryError::VerificationFailed(
            "manifest must be a bounded regular non-symlink file".to_owned(),
        ));
    }
    let bytes = fs::read(path).map_err(|error| RecoveryError::Io {
        operation: "reading backup manifest",
        message: error.to_string(),
    })?;
    let text = String::from_utf8_lossy(&bytes);
    for expected in [
        format!("backup-label=\"{}\"", backup.label),
        "backup-type=\"full\"".to_owned(),
        format!("backup-lsn-start=\"{}\"", backup.start_lsn),
        format!("backup-lsn-stop=\"{}\"", backup.stop_lsn),
        format!("db-system-id={system_id}"),
    ] {
        if !text.contains(&expected) {
            return Err(RecoveryError::VerificationFailed(format!(
                "manifest does not bind {expected}"
            )));
        }
    }
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn contiguous_archive_frontier(
    config: &RecoveryConfig,
    archive_id: &str,
    start_lsn: &str,
    segment_size: u64,
) -> Result<(String, u32, String), RecoveryError> {
    if segment_size == 0 || (u64::from(u32::MAX) + 1) % segment_size != 0 {
        return Err(RecoveryError::VerificationFailed(
            "invalid WAL segment size".to_owned(),
        ));
    }
    let root = config
        .repository_path
        .join("archive")
        .join(&config.stanza)
        .join(archive_id);
    let mut segments = BTreeSet::new();
    collect_archive_segments(&root, &root, 0, &mut segments)?;
    let start = parse_lsn(start_lsn).map_err(RecoveryError::VerificationFailed)?;
    let mut segment = segment_name(1, start - (start % segment_size), segment_size);
    let timeline = segments
        .iter()
        .find(|name| name[8..] == segment[8..])
        .and_then(|name| segment_timeline(name))
        .ok_or_else(|| {
            RecoveryError::VerificationFailed(
                "archive does not contain the backup stop WAL segment".to_owned(),
            )
        })?;
    segment.replace_range(..8, &format!("{timeline:08X}"));
    let mut proof = Sha256::new();
    let mut frontier = start;
    for _ in 0..1_000_000 {
        if !segments.contains(&segment) {
            break;
        }
        proof.update(segment.as_bytes());
        let segment_start = segment_start_lsn(&segment, segment_size)?;
        frontier = segment_start + segment_size;
        segment = segment_name(timeline, frontier, segment_size);
    }
    if frontier <= start {
        return Err(RecoveryError::VerificationFailed(
            "no contiguous archived WAL frontier was proven".to_owned(),
        ));
    }
    Ok((
        format_lsn(frontier),
        timeline,
        format!("{:x}", proof.finalize()),
    ))
}

fn collect_archive_segments(
    root: &Path,
    path: &Path,
    depth: usize,
    segments: &mut BTreeSet<String>,
) -> Result<(), RecoveryError> {
    if depth > 4 {
        return Err(RecoveryError::VerificationFailed(
            "archive directory nesting exceeds the supported plain-repository layout".to_owned(),
        ));
    }
    for entry in fs::read_dir(path).map_err(|error| RecoveryError::Io {
        operation: "scanning archived WAL",
        message: error.to_string(),
    })? {
        let entry = entry.map_err(|error| RecoveryError::Io {
            operation: "reading archived WAL entry",
            message: error.to_string(),
        })?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| RecoveryError::Io {
            operation: "reading archived WAL metadata",
            message: error.to_string(),
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RecoveryError::VerificationFailed(
                "archive path contains a symlink".to_owned(),
            ));
        }
        if metadata.is_dir() {
            collect_archive_segments(root, &entry.path(), depth + 1, segments)?;
        } else if metadata.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() >= 24
                && name[..24].bytes().all(|byte| byte.is_ascii_hexdigit())
                && entry.path().starts_with(root)
            {
                segments.insert(name[..24].to_ascii_uppercase());
            }
        }
    }
    Ok(())
}

fn segment_timeline(segment: &str) -> Option<u32> {
    (segment.len() >= 24).then(|| u32::from_str_radix(&segment[..8], 16).ok())?
}

fn segment_start_lsn(segment: &str, segment_size: u64) -> Result<u64, RecoveryError> {
    if segment.len() != 24 {
        return Err(RecoveryError::VerificationFailed(
            "invalid WAL segment name".to_owned(),
        ));
    }
    let log = u64::from_str_radix(&segment[8..16], 16)
        .map_err(|_| RecoveryError::VerificationFailed("invalid WAL log id".to_owned()))?;
    let seg = u64::from_str_radix(&segment[16..24], 16)
        .map_err(|_| RecoveryError::VerificationFailed("invalid WAL segment id".to_owned()))?;
    Ok((log << 32) + seg * segment_size)
}

fn segment_name(timeline: u32, lsn: u64, segment_size: u64) -> String {
    let log = lsn >> 32;
    let seg = (lsn & u64::from(u32::MAX)) / segment_size;
    format!("{timeline:08X}{log:08X}{seg:08X}")
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn & u64::from(u32::MAX))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn result_path(config: &RecoveryConfig, request_id: &str) -> PathBuf {
    config
        .work_root
        .join(".proof-results")
        .join(format!("{request_id}.json"))
}

fn existing_result(
    config: &RecoveryConfig,
    request: &BackupVerificationRequest,
    kind: &str,
) -> Result<Option<BackupVerificationResult>, RecoveryError> {
    let path = result_path(config, &request.request_id);
    if !path.exists() {
        return Ok(None);
    }
    let result: BackupVerificationResult = load_json(&path)?;
    if result.tracking_id != request.tracking_id || result.verification_kind != kind {
        return Err(RecoveryError::RequestConflict(request.request_id.clone()));
    }
    Ok(Some(result))
}

fn persist_result(
    config: &RecoveryConfig,
    result: &BackupVerificationResult,
) -> Result<(), RecoveryError> {
    let directory = config.work_root.join(".proof-results");
    prepare_secure_directory(&directory)?;
    write_json_atomic(&result_path(config, &result.request_id), result)
}

#[cfg(test)]
mod tests {
    use crate::model::BackupVerificationRequest;

    use super::{format_lsn, segment_name, segment_start_lsn, validate_request};

    #[test]
    fn verification_request_binds_safe_id_and_positive_tracking_id() {
        assert!(validate_request(&BackupVerificationRequest {
            request_id: "verify-42".to_owned(),
            tracking_id: 42,
        })
        .is_ok());
        assert!(validate_request(&BackupVerificationRequest {
            request_id: "../reuse".to_owned(),
            tracking_id: 42,
        })
        .is_err());
        assert!(validate_request(&BackupVerificationRequest {
            request_id: "verify-42".to_owned(),
            tracking_id: 0,
        })
        .is_err());
    }

    #[test]
    fn wal_segment_and_lsn_conversion_round_trip() {
        let segment_size = 16 * 1024 * 1024;
        let lsn = 0x0000_000A_1200_0000;
        let segment = segment_name(7, lsn, segment_size);
        assert_eq!(segment, "000000070000000A00000012");
        assert_eq!(segment_start_lsn(&segment, segment_size).unwrap(), lsn);
        assert_eq!(format_lsn(lsn + segment_size), "A/13000000");
    }
}

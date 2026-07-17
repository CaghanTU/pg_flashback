use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::error::RecoveryError;
use crate::executor::{
    acquire_profile_lock, acquire_repository_lock, acquire_request_lock_for_id,
    prepare_secure_directory, write_json_atomic,
};
use crate::model::{
    AnchorAuditFinding, AnchorAuditReport, BackupVerificationRequest, BackupVerificationResult,
    ExpireResult, ReconcileAnchorAction, ReconcileAnchorsReport, RecoveryConfig,
};
use crate::pgbackrest::{parse_lsn, read_repository_metadata, SelectedBackup};
use crate::{load_json, MAX_CONTRACT_BYTES};

const RESULT_FORMAT_VERSION: u32 = 1;
type HmacSha256 = Hmac<Sha256>;

fn proof_hmac(config: &RecoveryConfig, payload: &str) -> Result<String, RecoveryError> {
    let path = config.proof_hmac_key_file.as_ref().ok_or_else(|| {
        RecoveryError::InvalidConfig(
            "proof_hmac_key_file is required for repository proof verification".to_owned(),
        )
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "inspecting proof HMAC key",
        message: error.to_string(),
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RecoveryError::InvalidConfig(
            "proof_hmac_key_file must be a regular non-symlink file".to_owned(),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(RecoveryError::InvalidConfig(
            "proof_hmac_key_file must not grant group or other permissions".to_owned(),
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening proof HMAC key",
            message: error.to_string(),
        })?;
    let mut encoded = String::new();
    file.take(4097)
        .read_to_string(&mut encoded)
        .map_err(|error| RecoveryError::Io {
            operation: "reading proof HMAC key",
            message: error.to_string(),
        })?;
    let encoded = encoded.trim();
    if encoded.len() != 64 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RecoveryError::InvalidConfig(
            "proof_hmac_key_file must contain exactly 64 hexadecimal characters".to_owned(),
        ));
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).map_err(|_| {
            RecoveryError::InvalidConfig("proof_hmac_key_file contains invalid hex".to_owned())
        })?;
    }
    let mut mac = HmacSha256::new_from_slice(&key).map_err(|_| {
        RecoveryError::InvalidConfig("proof HMAC key has invalid length".to_owned())
    })?;
    mac.update(payload.as_bytes());
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        }))
}

#[derive(Debug, Deserialize)]
struct AnchorContext {
    tracking_id: i64,
    helper_profile: String,
    generation_id: i64,
    #[serde(default)]
    boundary_kind: Option<String>,
    marker_commit_lsn: String,
    #[serde(default)]
    #[allow(dead_code)]
    predecessor_generation_id: Option<i64>,
    #[serde(default)]
    predecessor_backup_label: Option<String>,
    #[serde(default)]
    predecessor_backup_stop_lsn: Option<String>,
    #[serde(default)]
    predecessor_valid_through_lsn: Option<String>,
    database_system_identifier: String,
    timeline_id: u32,
    wal_segment_size_bytes: u64,
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
    live_timeline_id: u32,
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

#[derive(Debug, Deserialize)]
struct ExpireLease {
    status: String,
    lease_id: Option<i64>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AnchorAuditContext {
    tracking_id: i64,
    generation_id: i64,
    helper_profile: String,
    repository_key: String,
    stanza: String,
    backup_label: String,
    database_system_identifier: String,
    manifest_reference: String,
    manifest_sha256: String,
}

#[derive(Debug, Deserialize)]
struct DurableProof {
    proof_id: i64,
    tracking_id: i64,
    generation_id: Option<i64>,
    helper_profile: String,
    repository_key: String,
    stanza: String,
    backup_label: String,
    timeline_id: u32,
    manifest_reference: Option<String>,
    manifest_sha256: Option<String>,
    archive_proof_sha256: Option<String>,
    verified_lsn: String,
    status: Option<String>,
    consumed: bool,
}

#[derive(Debug)]
struct ArchiveSegment {
    path: PathBuf,
    stored_name: String,
    checksum_sha1: String,
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
    if let Some(existing) = durable_proof_result(config, request, "anchor")? {
        persist_result(config, &existing)?;
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
    if metadata.archive.database_system_id != system_id {
        return Err(RecoveryError::VerificationFailed(
            "archive system identifier differs from the active PostgreSQL cluster".to_owned(),
        ));
    }

    let boundary_kind = context.boundary_kind.as_deref().unwrap_or("initial_track");
    let forbid_retained = matches!(boundary_kind, "post_restore" | "full_reanchor");
    let pred_stop = context
        .predecessor_backup_stop_lsn
        .as_deref()
        .map(parse_lsn)
        .transpose()
        .map_err(RecoveryError::VerificationFailed)?;
    let pred_valid_through = context
        .predecessor_valid_through_lsn
        .as_deref()
        .map(parse_lsn)
        .transpose()
        .map_err(RecoveryError::VerificationFailed)?;
    let pred_label = context.predecessor_backup_label.as_deref();

    // Prefer a fresh FULL after the marker (shortest replay). Otherwise, for
    // initial_track only, select the newest retained FULL with stop <= marker
    // and continuous WAL through the marker. Never auto-start a FULL backup.
    let mut fresh = metadata
        .backups
        .iter()
        .filter(|backup| backup.backup_type == "full")
        .filter(|backup| pred_label.is_none_or(|label| backup.label != label))
        .filter_map(|backup| {
            let start = parse_lsn(&backup.start_lsn).ok()?;
            let stop = parse_lsn(&backup.stop_lsn).ok()?;
            let timeline = backup.archive_start.as_deref().and_then(segment_timeline)?;
            let mut ok = start > marker
                && stop >= start
                && backup.database_system_id == system_id
                && timeline == context.timeline_id;
            if boundary_kind == "full_reanchor" {
                let pred_stop = pred_stop?;
                let pred_valid_through = pred_valid_through?;
                ok = ok && stop > pred_stop && stop <= pred_valid_through;
            }
            ok.then_some((stop, backup))
        })
        .collect::<Vec<_>>();
    // Prefer the newest eligible fresh FULL (largest stop) for advancement;
    // for initial activation prefer earliest start after marker via stop order
    // among post-marker FULLs (usually one).
    fresh.sort_unstable_by_key(|(stop, _)| std::cmp::Reverse(*stop));

    let mut selected_mode = "fresh_full_after_marker";
    let mut wal_through: Option<String> = None;
    let backup = if let Some((_, backup)) = fresh.first() {
        *backup
    } else if forbid_retained {
        return Err(RecoveryError::VerificationFailed(format!(
            "boundary_kind {boundary_kind} requires a fresh FULL after the marker; retained pre-marker FULLs are ineligible"
        )));
    } else {
        let mut retained = metadata
            .backups
            .iter()
            .filter(|backup| backup.backup_type == "full")
            .filter_map(|backup| {
                let start = parse_lsn(&backup.start_lsn).ok()?;
                let stop = parse_lsn(&backup.stop_lsn).ok()?;
                let timeline = backup.archive_start.as_deref().and_then(segment_timeline)?;
                // Reject overlapping backups (start <= marker < stop).
                (stop <= marker
                    && stop >= start
                    && backup.database_system_id == system_id
                    && timeline == context.timeline_id)
                    .then_some((stop, backup))
            })
            .collect::<Vec<_>>();
        retained.sort_unstable_by_key(|(stop, _)| std::cmp::Reverse(*stop));

        let mut chosen = None;
        for (_, candidate) in retained {
            match contiguous_archive_frontier(
                config,
                &metadata.archive.archive_id,
                &candidate.stop_lsn,
                context.wal_segment_size_bytes,
                context.timeline_id,
            ) {
                Ok((frontier, _)) => {
                    let frontier_lsn =
                        parse_lsn(&frontier).map_err(RecoveryError::VerificationFailed)?;
                    if frontier_lsn >= marker {
                        selected_mode = "retained_full_plus_wal";
                        wal_through = Some(frontier);
                        chosen = Some(candidate);
                        break;
                    }
                }
                Err(RecoveryError::TimelineMismatch { .. }) => {
                    return Err(RecoveryError::VerificationFailed(
                        "retained FULL archive timeline/history mismatches the live cluster"
                            .to_owned(),
                    ));
                }
                Err(_) => {}
            }
        }
        chosen.ok_or_else(|| {
            RecoveryError::VerificationFailed(
                "no eligible retained FULL with continuous WAL through the marker, and no fresh FULL after the marker; take an explicit FULL backup then retry verify-anchor".to_owned(),
            )
        })?
    };

    let manifest = config
        .repository_path
        .join("backup")
        .join(&config.stanza)
        .join(&backup.label)
        .join("backup.manifest");
    let manifest_sha256 = verify_manifest(&manifest, &config.repository_path, backup, system_id)?;
    let manifest_reference = manifest
        .strip_prefix(&config.repository_path)
        .map_err(|_| {
            RecoveryError::VerificationFailed(
                "manifest escaped the configured repository".to_owned(),
            )
        })?
        .to_string_lossy()
        .into_owned();

    // For fresh activation, still prove contiguous archive from backup stop so
    // activation does not advertise recoverability without WAL evidence.
    if selected_mode == "fresh_full_after_marker" {
        let (frontier, _) = contiguous_archive_frontier(
            config,
            &metadata.archive.archive_id,
            &backup.stop_lsn,
            context.wal_segment_size_bytes,
            context.timeline_id,
        )?;
        let frontier_lsn = parse_lsn(&frontier).map_err(RecoveryError::VerificationFailed)?;
        let stop_lsn = parse_lsn(&backup.stop_lsn).map_err(RecoveryError::VerificationFailed)?;
        if frontier_lsn < stop_lsn {
            return Err(RecoveryError::VerificationFailed(
                "archived WAL is not continuous through the selected FULL stop LSN".to_owned(),
            ));
        }
    }

    let wal_through_sql = match &wal_through {
        Some(lsn) => format!("{}::pg_lsn", sql_literal(lsn)),
        None => "NULL::pg_lsn".to_owned(),
    };
    let pin_id = format!(
        "anchor-{}-{}-{}",
        request.tracking_id, backup.label, selected_mode
    );
    let required_dependencies = serde_json::json!([
        {
            "kind": "full_backup",
            "label": backup.label,
            "start_lsn": backup.start_lsn,
            "stop_lsn": backup.stop_lsn,
            "manifest_sha256": manifest_sha256,
        },
        {
            "kind": "archived_wal_range",
            "start_lsn": backup.stop_lsn,
            "stop_lsn": wal_through.as_deref().unwrap_or(backup.stop_lsn.as_str()),
            "timeline_id": context.timeline_id,
        }
    ]);
    write_dependency_pin(config, &pin_id, &required_dependencies)?;

    let payload: String = query_json(
        config,
        &format!(
            "SELECT to_json(flashback_backup_proof_attestation_payload(
              {request_id}, {tracking_id}, {profile}, {repository_key}, {stanza},
              {label}, {system_id}, {timeline}, {manifest_reference}, {manifest_sha},
              {start_lsn}::pg_lsn, {stop_lsn}::pg_lsn,
              {activation_mode}, {wal_through}))::text",
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
            activation_mode = sql_literal(selected_mode),
            wal_through = wal_through_sql,
        ),
    )?;
    let attestation = proof_hmac(config, &payload)?;
    let details = format!(
        "jsonb_build_object(
            'verification_source','recovery_helper',
            'repository_lock','shared',
            'manifest_verified',true,
            'activation_mode',{activation_mode},
            'wal_verified_through_lsn',{wal_through},
            'dependency_pin_id',{pin_id},
            'required_dependencies',{deps}::jsonb
         )",
        activation_mode = sql_literal(selected_mode),
        wal_through = wal_through_sql,
        pin_id = sql_literal(&pin_id),
        deps = sql_literal(&required_dependencies.to_string()),
    );
    let sql = format!(
        "WITH installed AS (
           SELECT flashback_install_verified_backup_proof(
             {request_id}, {tracking_id}, {profile}, {repository_key}, {stanza},
             {label}, {system_id}, {timeline}, {manifest_reference}, {manifest_sha},
             {start_lsn}::pg_lsn, {stop_lsn}::pg_lsn, clock_timestamp(),
             {details},
             {attestation}
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
        details = details,
        attestation = sql_literal(&attestation),
    );
    let installed: InstalledAnchor = query_json(config, &sql)?;
    if installed.generation_id != context.generation_id {
        return Err(RecoveryError::VerificationFailed(
            "consumed anchor generation differs from verification context".to_owned(),
        ));
    }
    let verified_lsn = wal_through
        .clone()
        .unwrap_or_else(|| backup.stop_lsn.clone());
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
        verified_lsn,
        proof_id: installed.proof_id,
    };
    persist_result(config, &result)?;
    Ok(result)
}

fn write_dependency_pin(
    config: &RecoveryConfig,
    pin_id: &str,
    dependencies: &serde_json::Value,
) -> Result<(), RecoveryError> {
    let pin_dir = config.work_root.join("dependency-pins");
    prepare_secure_directory(&pin_dir)?;
    let path = pin_dir.join(format!("{pin_id}.json"));
    let body = serde_json::json!({
        "pin_id": pin_id,
        "pinned_at_unix_seconds": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        "dependencies": dependencies,
    });
    write_json_atomic(&path, &body)
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
    if let Some(existing) = durable_proof_result(config, request, "frontier")? {
        persist_result(config, &existing)?;
        if existing.status == "timeline_mismatch" {
            return Err(RecoveryError::VerificationFailed(
                "repository timeline mismatches the active anchor; coverage was frozen".to_owned(),
            ));
        }
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
    if context.live_timeline_id != context.anchor_timeline_id {
        freeze_timeline_mismatch(
            config,
            request.tracking_id,
            context.anchor_timeline_id,
            context.live_timeline_id,
            "live PostgreSQL timeline differs from the retained backup anchor",
        );
        return Err(RecoveryError::TimelineMismatch {
            expected: context.anchor_timeline_id,
            observed: context.live_timeline_id,
        });
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
    let (frontier_lsn, archive_sha) = match contiguous_archive_frontier(
        config,
        &metadata.archive.archive_id,
        &context.backup_stop_lsn,
        context.wal_segment_size_bytes,
        context.anchor_timeline_id,
    ) {
        Ok(frontier) => frontier,
        Err(error @ RecoveryError::TimelineMismatch { expected, observed }) => {
            freeze_timeline_mismatch(
                config,
                request.tracking_id,
                expected,
                observed,
                "archived WAL contains a newer PostgreSQL timeline",
            );
            return Err(error);
        }
        Err(error) => {
            freeze_repository_failure(
                config,
                request.tracking_id,
                "archived WAL proof unavailable",
            );
            return Err(error);
        }
    };

    let payload: String = query_json(
        config,
        &format!(
            "SELECT to_json(flashback_wal_frontier_attestation_payload(
              {request_id}, {tracking_id}, {generation_id}, {profile},
              {repository_key}, {stanza}, {timeline}, {frontier}::pg_lsn,
              {archive_sha}))::text",
            request_id = sql_literal(&request.request_id),
            tracking_id = request.tracking_id,
            generation_id = context.generation_id,
            profile = sql_literal(&config.profile),
            repository_key = sql_literal(&config.repository_key.to_string()),
            stanza = sql_literal(&config.stanza),
            timeline = context.anchor_timeline_id,
            frontier = sql_literal(&frontier_lsn),
            archive_sha = sql_literal(&archive_sha),
        ),
    )?;
    let attestation = proof_hmac(config, &payload)?;
    let sql = format!(
        "WITH installed AS (
           SELECT flashback_install_verified_wal_frontier_proof(
             {request_id}, {tracking_id}, {generation_id}, {profile},
             {repository_key}, {stanza}, {timeline}, {frontier}::pg_lsn,
             {archive_sha}, clock_timestamp(),
             jsonb_build_object('verification_source','recovery_helper',
                                'repository_lock','shared',
                                'archive_contiguous',true),
             {attestation}
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
        timeline = context.anchor_timeline_id,
        frontier = sql_literal(&frontier_lsn),
        archive_sha = sql_literal(&archive_sha),
        attestation = sql_literal(&attestation),
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
        timeline_id: context.anchor_timeline_id,
        manifest_reference: String::new(),
        manifest_sha256: archive_sha,
        verified_lsn: installed.valid_through_lsn,
        proof_id: installed.proof_id,
    };
    persist_result(config, &result)?;
    if installed.status == "timeline_mismatch" {
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
    let lease: ExpireLease = query_json(
        config,
        &format!(
            "SELECT flashback_begin_backup_expire({}, {}, {})::text",
            sql_literal(&config.profile),
            sql_literal(&config.repository_key.to_string()),
            sql_literal(&config.stanza)
        ),
    )?;
    if lease.status == "protected" {
        return Err(RecoveryError::ProtectedBackups(lease.labels.join(",")));
    }
    if lease.status == "busy" {
        return Err(RecoveryError::RepositoryBusy);
    }
    if lease.status != "started" && lease.status != "resumed" {
        return Err(RecoveryError::VerificationFailed(format!(
            "controller returned unexpected expiration lease status {}",
            lease.status
        )));
    }
    let lease_id = lease.lease_id.ok_or_else(|| {
        RecoveryError::VerificationFailed("controller expiration lease has no lease_id".to_owned())
    })?;
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
        // Deliberately retain the durable lease. A corrected retry resumes it
        // and reruns pgBackRest expire idempotently; allowing generation
        // activation after an uncertain partial expiration would be unsafe.
        return Err(RecoveryError::CommandFailed {
            program: config.pgbackrest_bin.clone(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let completion: ExpireLease = query_json(
        config,
        &format!(
            "SELECT flashback_complete_backup_expire({}, {}, {}, {})::text",
            lease_id,
            sql_literal(&config.profile),
            sql_literal(&config.repository_key.to_string()),
            sql_literal(&config.stanza)
        ),
    )?;
    if completion.status != "completed" || completion.lease_id != Some(lease_id) {
        return Err(RecoveryError::VerificationFailed(
            "controller did not durably complete the expiration lease".to_owned(),
        ));
    }
    Ok(ExpireResult {
        status: "expired".to_owned(),
        profile: config.profile.clone(),
        stanza: config.stanza.clone(),
        protected_backup_labels: Vec::new(),
    })
}

/// Discover newer FULL backups and advance/retire backup anchors without
/// creating backups. Intended for systemd timer / cron, not the PG backend.
///
/// # Errors
///
/// Returns configuration, lock, repository, or controller failures. Per-tracking
/// advancement failures are recorded as blocked actions when fail-closed.
#[allow(clippy::too_many_lines)]
pub fn reconcile_anchors(
    config: &RecoveryConfig,
    dry_run: bool,
) -> Result<ReconcileAnchorsReport, RecoveryError> {
    prepare_secure_directory(&config.work_root)?;
    // Do not hold helper flock locks across verify_anchor: that command acquires
    // the same profile/repository locks and would self-deadlock. PostgreSQL
    // advisory locks inside begin/retire/consume serialize coverage mutations.

    let tracking_ids: Vec<i64> = query_json(
        config,
        &format!(
            "SELECT COALESCE(jsonb_agg(tt.tracking_id ORDER BY tt.tracking_id), '[]'::jsonb)::text
             FROM flashback.tracked_tables tt
             WHERE tt.is_active
               AND tt.recovery_profile = 'backup'
               AND tt.helper_profile = {}",
            sql_literal(&config.profile)
        ),
    )?;
    let metadata = read_repository_metadata(config)?;
    let mut actions = Vec::new();

    for tracking_id in tracking_ids {
        let active: Option<ActiveAnchorRow> = query_json(
            config,
            &format!(
                "SELECT COALESCE((
                   SELECT jsonb_build_object(
                     'generation_id', cg.generation_id,
                     'backup_label', ba.backup_label,
                     'backup_stop_lsn', ba.backup_stop_lsn::text,
                     'valid_through_lsn', cg.valid_through_lsn::text,
                     'marker_lsn', COALESCE(cg.details->>'tracking_marker_lsn', cg.boundary_lsn::text)
                   )
                   FROM flashback.coverage_generations cg
                   JOIN flashback.backup_anchors ba
                     ON ba.backup_anchor_id = cg.backup_anchor_id
                    AND ba.tracking_id = cg.tracking_id
                   WHERE cg.tracking_id = {tracking_id}
                     AND cg.recovery_profile = 'backup'
                     AND cg.state = 'active'
                   LIMIT 1
                 ), 'null'::jsonb)::text"
            ),
        )?;
        let building: Option<BuildingRow> = query_json(
            config,
            &format!(
                "SELECT COALESCE((
                   SELECT jsonb_build_object(
                     'generation_id', cg.generation_id,
                     'boundary_kind', cg.boundary_kind
                   )
                   FROM flashback.coverage_generations cg
                   WHERE cg.tracking_id = {tracking_id}
                     AND cg.recovery_profile = 'backup'
                     AND cg.state = 'building'
                   ORDER BY cg.generation_no DESC
                   LIMIT 1
                 ), 'null'::jsonb)::text"
            ),
        )?;

        if let Some(building) = building {
            if building.boundary_kind != "full_reanchor" {
                actions.push(ReconcileAnchorAction {
                    tracking_id,
                    action: "blocked".to_owned(),
                    detail: format!(
                        "building generation {} ({}) blocks advancement",
                        building.generation_id, building.boundary_kind
                    ),
                    generation_id: Some(building.generation_id),
                    backup_label: None,
                });
                continue;
            }
            if dry_run {
                actions.push(ReconcileAnchorAction {
                    tracking_id,
                    action: "would_activate_successor".to_owned(),
                    detail: "resuming full_reanchor building generation".to_owned(),
                    generation_id: Some(building.generation_id),
                    backup_label: None,
                });
                continue;
            }
            let request_id = format!(
                "reconcile-activate-{tracking_id}-{}",
                building.generation_id
            );
            match verify_anchor(
                config,
                &BackupVerificationRequest {
                    request_id,
                    tracking_id,
                },
            ) {
                Ok(result) => actions.push(ReconcileAnchorAction {
                    tracking_id,
                    action: "activated".to_owned(),
                    detail: "activated building full_reanchor successor".to_owned(),
                    generation_id: Some(result.generation_id),
                    backup_label: Some(result.backup_label),
                }),
                Err(error) => actions.push(ReconcileAnchorAction {
                    tracking_id,
                    action: "blocked".to_owned(),
                    detail: error.to_string(),
                    generation_id: Some(building.generation_id),
                    backup_label: None,
                }),
            }
            continue;
        }

        let Some(active) = active else {
            actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "noop".to_owned(),
                detail: "no active verified backup generation".to_owned(),
                generation_id: None,
                backup_label: None,
            });
            continue;
        };
        let pred_stop =
            parse_lsn(&active.backup_stop_lsn).map_err(RecoveryError::VerificationFailed)?;
        let pred_through =
            parse_lsn(&active.valid_through_lsn).map_err(RecoveryError::VerificationFailed)?;
        let marker = parse_lsn(&active.marker_lsn).map_err(RecoveryError::VerificationFailed)?;

        let mut candidates = metadata
            .backups
            .iter()
            .filter(|backup| backup.backup_type == "full")
            .filter(|backup| backup.label != active.backup_label)
            .filter_map(|backup| {
                let start = parse_lsn(&backup.start_lsn).ok()?;
                let stop = parse_lsn(&backup.stop_lsn).ok()?;
                (start > marker && stop > pred_stop && stop <= pred_through)
                    .then_some((stop, backup))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(stop, _)| std::cmp::Reverse(*stop));
        let Some((_, candidate)) = candidates.first() else {
            actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "noop".to_owned(),
                detail: "no newer eligible FULL within the proven WAL frontier".to_owned(),
                generation_id: Some(active.generation_id),
                backup_label: Some(active.backup_label.clone()),
            });
            continue;
        };

        if dry_run {
            actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "would_advance".to_owned(),
                detail: format!(
                    "eligible successor FULL {} stop={}",
                    candidate.label, candidate.stop_lsn
                ),
                generation_id: Some(active.generation_id),
                backup_label: Some(candidate.label.clone()),
            });
            continue;
        }

        let began: AdvancementBegin = query_json(
            config,
            &format!("SELECT flashback_begin_backup_anchor_advancement({tracking_id})::text"),
        )?;
        if began.status != "started" && began.status != "resumed" {
            actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "blocked".to_owned(),
                detail: began
                    .reason
                    .unwrap_or_else(|| format!("advancement status {}", began.status)),
                generation_id: began.generation_id,
                backup_label: None,
            });
            continue;
        }
        let request_id = format!(
            "reconcile-advance-{tracking_id}-{}",
            began.generation_id.unwrap_or_default()
        );
        match verify_anchor(
            config,
            &BackupVerificationRequest {
                request_id,
                tracking_id,
            },
        ) {
            Ok(result) => actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "advanced".to_owned(),
                detail: format!("activated successor FULL {}", result.backup_label),
                generation_id: Some(result.generation_id),
                backup_label: Some(result.backup_label),
            }),
            Err(error) => actions.push(ReconcileAnchorAction {
                tracking_id,
                action: "blocked".to_owned(),
                detail: error.to_string(),
                generation_id: began.generation_id,
                backup_label: Some(candidate.label.clone()),
            }),
        }
    }

    // Retire sealed predecessors whose exclusive ranges are outside retention.
    let sealed_ids: Vec<i64> = query_json(
        config,
        &format!(
            "SELECT COALESCE(jsonb_agg(cg.generation_id ORDER BY cg.generation_id), '[]'::jsonb)::text
             FROM flashback.coverage_generations cg
             JOIN flashback.tracked_tables tt ON tt.tracking_id = cg.tracking_id
             WHERE tt.is_active
               AND tt.recovery_profile = 'backup'
               AND tt.helper_profile = {}
               AND cg.recovery_profile = 'backup'
               AND cg.state = 'sealed'
               AND cg.superseded_before_lsn IS NOT NULL",
            sql_literal(&config.profile)
        ),
    )?;
    for generation_id in sealed_ids {
        if dry_run {
            let preview: RetirementResult = query_json(
                config,
                &format!(
                    "SELECT jsonb_build_object(
                       'status', CASE
                         WHEN cg.sealed_at <= clock_timestamp() - COALESCE(tt.retention_interval, interval '7 days')
                         THEN 'would_retire' ELSE 'retention_holds' END,
                       'generation_id', cg.generation_id,
                       'tracking_id', cg.tracking_id
                     )::text
                     FROM flashback.coverage_generations cg
                     JOIN flashback.tracked_tables tt ON tt.tracking_id = cg.tracking_id
                     WHERE cg.generation_id = {generation_id}"
                ),
            )?;
            actions.push(ReconcileAnchorAction {
                tracking_id: preview.tracking_id.unwrap_or(0),
                action: preview.status,
                detail: "sealed predecessor retention evaluation".to_owned(),
                generation_id: Some(generation_id),
                backup_label: None,
            });
            continue;
        }
        let retired: RetirementResult = query_json(
            config,
            &format!("SELECT flashback_retire_sealed_backup_generation({generation_id})::text"),
        )?;
        let tracking_id = retired.tracking_id.unwrap_or(0);
        if retired.status == "retired" {
            if let Ok(Some(pin_id)) = query_json::<Option<String>>(
                config,
                &format!(
                    "SELECT COALESCE(
                       to_jsonb(NULLIF(cg.details->>'dependency_pin_id', '')),
                       'null'::jsonb
                     )::text
                     FROM flashback.coverage_generations cg
                     WHERE cg.generation_id = {generation_id}"
                ),
            ) {
                let pin_path = config
                    .work_root
                    .join("dependency-pins")
                    .join(format!("{pin_id}.json"));
                let _ = std::fs::remove_file(pin_path);
            }
        }
        actions.push(ReconcileAnchorAction {
            tracking_id,
            action: retired.status,
            detail: retired
                .reason
                .unwrap_or_else(|| "sealed predecessor retirement".to_owned()),
            generation_id: Some(generation_id),
            backup_label: None,
        });
    }

    Ok(ReconcileAnchorsReport {
        status: "ok".to_owned(),
        profile: config.profile.clone(),
        stanza: config.stanza.clone(),
        dry_run,
        created_backup: false,
        actions,
    })
}

#[derive(Debug, Deserialize)]
struct ActiveAnchorRow {
    generation_id: i64,
    backup_label: String,
    backup_stop_lsn: String,
    valid_through_lsn: String,
    marker_lsn: String,
}

#[derive(Debug, Deserialize)]
struct BuildingRow {
    generation_id: i64,
    boundary_kind: String,
}

#[derive(Debug, Deserialize)]
struct AdvancementBegin {
    status: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    generation_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RetirementResult {
    status: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    tracking_id: Option<i64>,
}

/// Audit every retained backup anchor and durably freeze missing/corrupt ones.
///
/// # Errors
///
/// Returns an error when the controller, repository lock, or repository
/// catalog cannot be inspected. Definite per-anchor failures are returned as
/// degraded findings after their generations are frozen in the database.
pub fn audit_anchors(config: &RecoveryConfig) -> Result<AnchorAuditReport, RecoveryError> {
    prepare_secure_directory(&config.work_root)?;
    let _profile_lock = acquire_profile_lock(config)?;
    let _repository_lock = acquire_repository_lock(config, false)?;
    let contexts: Vec<AnchorAuditContext> = query_json(
        config,
        "SELECT flashback_active_backup_anchor_contexts()::text",
    )?;
    let metadata = read_repository_metadata(config)?;
    let mut findings = Vec::with_capacity(contexts.len());
    for context in &contexts {
        if context.helper_profile != config.profile
            || context.repository_key != config.repository_key.to_string()
            || context.stanza != config.stanza
        {
            return Err(RecoveryError::VerificationFailed(format!(
                "anchor {} belongs to a different helper/repository profile",
                context.generation_id
            )));
        }
        let verification = metadata
            .backups
            .iter()
            .find(|backup| backup.label == context.backup_label)
            .ok_or_else(|| "backup label is absent from the repository catalog".to_owned())
            .and_then(|backup| {
                if backup.backup_type != "full" {
                    return Err("anchor label no longer identifies a FULL backup".to_owned());
                }
                let expected_system_id = context
                    .database_system_identifier
                    .parse::<u64>()
                    .map_err(|_| "anchor system identifier is invalid".to_owned())?;
                let manifest = config.repository_path.join(&context.manifest_reference);
                let actual = verify_manifest(
                    &manifest,
                    &config.repository_path,
                    backup,
                    expected_system_id,
                )
                .map_err(|error| error.to_string())?;
                if actual != context.manifest_sha256 {
                    return Err("manifest digest differs from the installed anchor".to_owned());
                }
                Ok(())
            });
        match verification {
            Ok(()) => findings.push(AnchorAuditFinding {
                tracking_id: context.tracking_id,
                generation_id: context.generation_id,
                backup_label: context.backup_label.clone(),
                status: "ok".to_owned(),
                detail: "repository anchor and manifest verified".to_owned(),
            }),
            Err(detail) => {
                freeze_missing_anchor(config, context, &detail)?;
                findings.push(AnchorAuditFinding {
                    tracking_id: context.tracking_id,
                    generation_id: context.generation_id,
                    backup_label: context.backup_label.clone(),
                    status: "frozen".to_owned(),
                    detail,
                });
            }
        }
    }
    let degraded = findings.iter().any(|finding| finding.status == "frozen");
    Ok(AnchorAuditReport {
        status: if degraded { "degraded" } else { "ok" }.to_owned(),
        profile: config.profile.clone(),
        stanza: config.stanza.clone(),
        checked: contexts.len() as u64,
        findings,
    })
}

fn freeze_missing_anchor(
    config: &RecoveryConfig,
    context: &AnchorAuditContext,
    detail: &str,
) -> Result<(), RecoveryError> {
    let sql = format!(
        "SELECT flashback_freeze_missing_backup_anchor(
           {tracking_id}, {generation_id},
           jsonb_build_object('helper_detail', {detail},
                              'backup_label', {backup_label})
         )::text",
        tracking_id = context.tracking_id,
        generation_id = context.generation_id,
        detail = sql_literal(detail),
        backup_label = sql_literal(&context.backup_label),
    );
    let _: serde_json::Value = query_json(config, &sql)?;
    Ok(())
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

fn durable_proof_result(
    config: &RecoveryConfig,
    request: &BackupVerificationRequest,
    kind: &str,
) -> Result<Option<BackupVerificationResult>, RecoveryError> {
    let function = if kind == "anchor" {
        "flashback_backup_proof_result"
    } else {
        "flashback_frontier_proof_result"
    };
    let sql = format!(
        "SELECT COALESCE(
           {function}({request_id}, {tracking_id}),
           'null'::jsonb
         )::text",
        request_id = sql_literal(&request.request_id),
        tracking_id = request.tracking_id,
    );
    let Some(proof): Option<DurableProof> = query_json(config, &sql)? else {
        return Ok(None);
    };
    if !proof.consumed
        || proof.tracking_id != request.tracking_id
        || proof.helper_profile != config.profile
        || proof.repository_key != config.repository_key.to_string()
        || proof.stanza != config.stanza
    {
        return Err(RecoveryError::RequestConflict(request.request_id.clone()));
    }
    let generation_id = proof.generation_id.ok_or_else(|| {
        RecoveryError::VerificationFailed(
            "durable proof is consumed without a generation identity".to_owned(),
        )
    })?;
    let digest = if kind == "anchor" {
        proof.manifest_sha256.ok_or_else(|| {
            RecoveryError::VerificationFailed(
                "durable anchor proof has no manifest digest".to_owned(),
            )
        })?
    } else {
        proof.archive_proof_sha256.ok_or_else(|| {
            RecoveryError::VerificationFailed(
                "durable frontier proof has no archive digest".to_owned(),
            )
        })?
    };
    Ok(Some(BackupVerificationResult {
        result_format_version: RESULT_FORMAT_VERSION,
        status: proof.status.unwrap_or_else(|| "verified".to_owned()),
        verification_kind: kind.to_owned(),
        request_id: request.request_id.clone(),
        tracking_id: request.tracking_id,
        generation_id,
        profile: config.profile.clone(),
        repository_key: config.repository_key,
        stanza: config.stanza.clone(),
        backup_label: proof.backup_label,
        timeline_id: proof.timeline_id,
        manifest_reference: proof.manifest_reference.unwrap_or_default(),
        manifest_sha256: digest,
        verified_lsn: proof.verified_lsn,
        proof_id: proof.proof_id,
    }))
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

fn freeze_timeline_mismatch(
    config: &RecoveryConfig,
    tracking_id: i64,
    expected: u32,
    observed: u32,
    detail: &str,
) {
    let sql = format!(
        "SELECT flashback_freeze_backup_generation(
           {tracking_id}, 'timeline_mismatch',
           jsonb_build_object(
             'expected_timeline', {expected},
             'observed_timeline', {observed},
             'helper_detail', {detail}
           )
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
    repository_root: &Path,
    backup: &SelectedBackup,
    system_id: u64,
) -> Result<String, RecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "reading backup manifest metadata",
        message: error.to_string(),
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(RecoveryError::VerificationFailed(
            "manifest must be a regular non-symlink file".to_owned(),
        ));
    }
    let canonical_repository =
        fs::canonicalize(repository_root).map_err(|error| RecoveryError::Io {
            operation: "canonicalizing repository root",
            message: error.to_string(),
        })?;
    let canonical_manifest = fs::canonicalize(path).map_err(|error| RecoveryError::Io {
        operation: "canonicalizing backup manifest",
        message: error.to_string(),
    })?;
    if !canonical_manifest.starts_with(&canonical_repository) {
        return Err(RecoveryError::VerificationFailed(
            "manifest escaped the configured repository".to_owned(),
        ));
    }

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening backup manifest",
            message: error.to_string(),
        })?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut header = Vec::new();
    let mut line = Vec::new();
    let mut in_header = true;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| RecoveryError::Io {
                operation: "streaming backup manifest",
                message: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        if line.len() as u64 > MAX_CONTRACT_BYTES {
            return Err(RecoveryError::VerificationFailed(
                "manifest contains an unbounded line".to_owned(),
            ));
        }
        digest.update(&line);
        if in_header {
            if line.starts_with(b"[target:file]") {
                in_header = false;
            } else {
                if header.len().saturating_add(line.len()) as u64 > MAX_CONTRACT_BYTES {
                    return Err(RecoveryError::VerificationFailed(
                        "manifest header exceeds the verification limit".to_owned(),
                    ));
                }
                header.extend_from_slice(&line);
            }
        }
    }
    let text = String::from_utf8_lossy(&header);
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
    Ok(format!("{:x}", digest.finalize()))
}

fn contiguous_archive_frontier(
    config: &RecoveryConfig,
    archive_id: &str,
    start_lsn: &str,
    segment_size: u64,
    expected_timeline: u32,
) -> Result<(String, String), RecoveryError> {
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
    contiguous_archive_frontier_at(&root, start_lsn, segment_size, expected_timeline)
}

fn contiguous_archive_frontier_at(
    root: &Path,
    start_lsn: &str,
    segment_size: u64,
    expected_timeline: u32,
) -> Result<(String, String), RecoveryError> {
    let mut segments = BTreeMap::new();
    collect_archive_segments(root, root, 0, &mut segments)?;
    let start = parse_lsn(start_lsn).map_err(RecoveryError::VerificationFailed)?;
    let start_segment = start - (start % segment_size);
    if let Some(observed) = segments.keys().find_map(|name| {
        let timeline = segment_timeline(name)?;
        let segment_start = segment_start_lsn(name, segment_size).ok()?;
        (timeline > expected_timeline && segment_start + segment_size > start_segment)
            .then_some(timeline)
    }) {
        return Err(RecoveryError::TimelineMismatch {
            expected: expected_timeline,
            observed,
        });
    }
    let mut segment = segment_name(expected_timeline, start_segment, segment_size);
    let mut proof = Sha256::new();
    let mut frontier = start;
    for _ in 0..1_000_000 {
        let Some(archived) = segments.get(&segment) else {
            break;
        };
        verify_archive_segment(archived, segment_size)?;
        proof.update(archived.stored_name.as_bytes());
        proof.update([0]);
        let segment_start = segment_start_lsn(&segment, segment_size)?;
        frontier = segment_start + segment_size;
        segment = segment_name(expected_timeline, frontier, segment_size);
    }
    if frontier <= start {
        return Err(RecoveryError::VerificationFailed(
            "no contiguous archived WAL frontier was proven".to_owned(),
        ));
    }
    Ok((format_lsn(frontier), format!("{:x}", proof.finalize())))
}

fn collect_archive_segments(
    root: &Path,
    path: &Path,
    depth: usize,
    segments: &mut BTreeMap<String, ArchiveSegment>,
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
            if let Some((segment, checksum_sha1)) = parse_archive_segment_name(&name) {
                if !entry.path().starts_with(root) {
                    return Err(RecoveryError::VerificationFailed(
                        "archive segment escaped the configured archive root".to_owned(),
                    ));
                }
                let archived = ArchiveSegment {
                    path: entry.path(),
                    stored_name: name,
                    checksum_sha1,
                };
                if segments.insert(segment.clone(), archived).is_some() {
                    return Err(RecoveryError::VerificationFailed(format!(
                        "archive contains duplicate WAL segment {segment}"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn parse_archive_segment_name(name: &str) -> Option<(String, String)> {
    if name.len() != 65 || name.as_bytes().get(24) != Some(&b'-') {
        return None;
    }
    let segment = &name[..24];
    let checksum = &name[25..];
    if !segment.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some((segment.to_ascii_uppercase(), checksum.to_ascii_lowercase()))
}

fn verify_archive_segment(
    archived: &ArchiveSegment,
    segment_size: u64,
) -> Result<(), RecoveryError> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&archived.path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening archived WAL segment",
            message: error.to_string(),
        })?;
    let metadata = file.metadata().map_err(|error| RecoveryError::Io {
        operation: "reading archived WAL metadata",
        message: error.to_string(),
    })?;
    if !metadata.is_file() || metadata.len() != segment_size {
        return Err(RecoveryError::VerificationFailed(format!(
            "archived WAL segment {} has size {}, expected {segment_size}",
            archived.stored_name,
            metadata.len()
        )));
    }
    let mut digest = Sha1::new();
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|error| RecoveryError::Io {
            operation: "hashing archived WAL segment",
            message: error.to_string(),
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let actual = format!("{:x}", digest.finalize());
    if actual != archived.checksum_sha1 {
        return Err(RecoveryError::VerificationFailed(format!(
            "archived WAL segment {} failed checksum verification",
            archived.stored_name
        )));
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
    use std::fmt::Write as FmtWrite;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sha1::{Digest, Sha1};

    use crate::error::RecoveryError;
    use crate::model::BackupVerificationRequest;
    use crate::pgbackrest::SelectedBackup;

    use super::{
        contiguous_archive_frontier_at, format_lsn, parse_archive_segment_name, segment_name,
        segment_start_lsn, validate_request, verify_archive_segment, verify_manifest,
        ArchiveSegment,
    };

    fn temp_test_dir(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pg-flashback-verifier-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

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

    #[test]
    fn archive_parser_rejects_history_files_and_verifies_wal_checksum() {
        let root = temp_test_dir("archive");
        let segment_size = 1024_usize * 1024;
        let segment = "000000020000000A00000012";
        assert!(parse_archive_segment_name(&format!("{segment}.00000028.backup")).is_none());

        let bytes = vec![0x5a_u8; segment_size];
        let checksum = format!("{:x}", Sha1::digest(&bytes));
        let stored_name = format!("{segment}-{checksum}");
        let path = root.join(&stored_name);
        fs::write(&path, &bytes).unwrap();
        let archived = ArchiveSegment {
            path: path.clone(),
            stored_name,
            checksum_sha1: checksum,
        };
        verify_archive_segment(&archived, u64::try_from(segment_size).unwrap()).unwrap();

        fs::write(&path, vec![0_u8; segment_size]).unwrap();
        assert!(verify_archive_segment(&archived, u64::try_from(segment_size).unwrap()).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_frontier_requires_the_anchor_timeline() {
        let root = temp_test_dir("archive-timeline");
        let segment_size = 1024_u64 * 1024;
        let start = 0x0000_000A_1200_0000_u64;
        let timeline_one = segment_name(1, start, segment_size);
        let bytes = vec![0x42_u8; usize::try_from(segment_size).unwrap()];
        let checksum = format!("{:x}", Sha1::digest(&bytes));
        fs::write(root.join(format!("{timeline_one}-{checksum}")), bytes).unwrap();

        assert!(
            contiguous_archive_frontier_at(&root, &format_lsn(start), segment_size, 2).is_err()
        );
        let (frontier, proof) =
            contiguous_archive_frontier_at(&root, &format_lsn(start), segment_size, 1).unwrap();
        assert_eq!(frontier, format_lsn(start + segment_size));
        assert_eq!(proof.len(), 64);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_frontier_rejects_a_newer_timeline() {
        let root = temp_test_dir("archive-newer-timeline");
        let segment_size = 1024_u64 * 1024;
        let start = 0x0000_000A_1200_0000_u64;
        let bytes = vec![0x24_u8; usize::try_from(segment_size).unwrap()];
        let checksum = format!("{:x}", Sha1::digest(&bytes));
        for timeline in [1, 2] {
            let segment = segment_name(timeline, start, segment_size);
            fs::write(root.join(format!("{segment}-{checksum}")), &bytes).unwrap();
        }

        assert!(matches!(
            contiguous_archive_frontier_at(&root, &format_lsn(start), segment_size, 1),
            Err(RecoveryError::TimelineMismatch {
                expected: 1,
                observed: 2
            })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn large_manifest_is_streamed_instead_of_rejected_by_contract_limit() {
        let root = temp_test_dir("manifest");
        let label = "20260717-120000F";
        let backup_dir = root.join("backup").join("stanza").join(label);
        fs::create_dir_all(&backup_dir).unwrap();
        let path = backup_dir.join("backup.manifest");
        let mut manifest = format!(
            "[backup]\nbackup-label=\"{label}\"\nbackup-lsn-start=\"0/100\"\nbackup-lsn-stop=\"0/200\"\nbackup-type=\"full\"\n\n[backup:db]\ndb-system-id=42\n\n[target:file]\n"
        );
        for index in 0..40_000 {
            writeln!(manifest, "pg_data/base/1/{index}={{\"size\":8192}}").unwrap();
        }
        assert!(manifest.len() > 1024 * 1024);
        fs::write(&path, manifest).unwrap();
        let backup = SelectedBackup {
            label: label.to_owned(),
            backup_type: "full".to_owned(),
            start_lsn: "0/100".to_owned(),
            stop_lsn: "0/200".to_owned(),
            archive_start: Some("000000010000000000000001".to_owned()),
            archive_stop: Some("000000010000000000000002".to_owned()),
            database_id: 1,
            database_system_id: 42,
            size_bytes: Some(1),
        };
        let digest = verify_manifest(&path, &root, &backup, 42).unwrap();
        assert_eq!(digest.len(), 64);
        fs::remove_dir_all(root).unwrap();
    }
}

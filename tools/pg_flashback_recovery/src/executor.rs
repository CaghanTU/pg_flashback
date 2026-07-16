use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use signal_hook::consts::{SIGINT, SIGTERM};

use crate::error::RecoveryError;
use crate::load_json;
use crate::model::{
    ExecutionDurations, ExecutionState, RecoveredAclEntry, RecoveryConfig, RecoveryEngine,
    RestorePlan, RestoreRequest, RestoreResult,
};
use crate::pgbackrest::{direct_backup_tree, read_backup_catalog, select_backup};

const COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(10);
const PROCESS_STOP_TIMEOUT: u64 = 60;
const LOG_TAIL_BYTES: u64 = 16 * 1024;
const RESULT_FORMAT_VERSION: u32 = 3;
const MIN_FREE_BYTES: u64 = 64 * 1024 * 1024;
const ARTIFACT_SCHEMA: &str = "flashback_import";
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
struct ActiveProcessState {
    process_group: i32,
    start_ticks: u64,
    operation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RequestContract {
    contract_format_version: u32,
    profile: String,
    request: RestoreRequest,
}

#[derive(Debug, Deserialize)]
struct RecoveredSecurityMetadata {
    owner: String,
    acl: Vec<RecoveredAclEntry>,
}

struct ActiveProcessGuard {
    path: PathBuf,
}

impl Drop for ActiveProcessGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct Runtime<'a> {
    config: &'a RecoveryConfig,
    request: &'a RestoreRequest,
    plan: &'a RestorePlan,
    work_dir: PathBuf,
    pgdata: PathBuf,
    socket_dir: PathBuf,
    log_dir: PathBuf,
    state_path: PathBuf,
    result_path: PathBuf,
    artifact_path: PathBuf,
    cancellation: Arc<AtomicBool>,
    postgres_started: bool,
}

impl Runtime<'_> {
    fn write_state(&self, phase: &str, detail: Option<String>) -> Result<(), RecoveryError> {
        let state = ExecutionState {
            request_id: self.request.request_id.clone(),
            phase: phase.to_owned(),
            updated_at_unix_seconds: unix_seconds()?,
            detail,
        };
        write_json_atomic(&self.state_path, &state)
    }

    fn check_cancelled(&self) -> Result<(), RecoveryError> {
        if self.cancellation.load(Ordering::SeqCst) {
            Err(RecoveryError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn active_process_path(&self) -> PathBuf {
        self.work_dir.join("active-process.json")
    }

    fn check_work_quota(&self) -> Result<(), RecoveryError> {
        let work_bytes = tree_apparent_bytes(&self.work_dir)?;
        if work_bytes > self.config.max_work_bytes {
            return Err(RecoveryError::WorkQuotaExceeded {
                required: work_bytes,
                limit: self.config.max_work_bytes,
            });
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<(), RecoveryError> {
        let mut errors = Vec::new();
        if self.postgres_started || self.pgdata.join("postmaster.pid").exists() {
            if let Err(error) = stop_postgres(self.config, &self.pgdata) {
                errors.push(error.to_string());
            }
        }
        self.postgres_started = false;

        if self.pgdata.join("postmaster.pid").exists() {
            errors
                .push("postmaster.pid still exists; materialized pgdata was preserved".to_owned());
        } else if let Err(error) = remove_dir_if_exists(&self.pgdata) {
            errors.push(format!("removing pgdata: {error}"));
        }
        if let Err(error) = remove_dir_if_exists(&self.socket_dir) {
            errors.push(format!("removing socket directory: {error}"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(RecoveryError::CleanupFailed(errors.join("; ")))
        }
    }
}

impl Drop for Runtime<'_> {
    fn drop(&mut self) {
        if self.postgres_started || self.pgdata.exists() || self.socket_dir.exists() {
            let _ = self.cleanup();
        }
    }
}

/// Execute a planned recovery and produce a validated custom-format table dump.
///
/// # Errors
///
/// Returns a structured [`RecoveryError`] for validation, locking,
/// materialization, recovery, extraction or cleanup failures. Temporary
/// `PostgreSQL` is stopped and materialized data is removed on every handled
/// error path.
pub fn execute_restore(
    config: &RecoveryConfig,
    request: &RestoreRequest,
    plan: &RestorePlan,
) -> Result<RestoreResult, RecoveryError> {
    prepare_secure_directory(&config.work_root)?;
    prepare_secure_directory(&config.socket_root)?;
    let _request_lock = acquire_request_lock(config, request)?;
    preserve_request_contract(config, request)?;

    let work_dir = config.work_root.join(&request.request_id);
    reject_symlink_path(&work_dir, "request work directory")?;
    let result_path = work_dir.join("result.json");
    if let Some(existing) = reusable_result(config, request, &result_path)? {
        return Ok(existing);
    }
    let _profile_lock = acquire_profile_lock(config)?;
    reconcile_abandoned_requests(config)?;
    let mut runtime = prepare_runtime(config, request, plan, work_dir, result_path)?;
    runtime.write_state("accepted", None)?;

    let execution = execute_inner(&mut runtime);
    match execution {
        Ok(mut result) => {
            runtime.write_state("cleaning", None)?;
            runtime.cleanup()?;
            result.cleanup_complete = true;
            write_json_atomic(&runtime.result_path, &result)?;
            runtime.write_state("completed", None)?;
            Ok(result)
        }
        Err(error) => {
            let _ = fs::remove_file(&runtime.artifact_path);
            let _ = runtime.write_state("failed", Some(error.to_string()));
            if let Err(cleanup_error) = runtime.cleanup() {
                let _ = runtime.write_state(
                    "cleanup_failed",
                    Some(format!(
                        "original error: {error}; cleanup error: {cleanup_error}"
                    )),
                );
                return Err(cleanup_error);
            }
            Err(error)
        }
    }
}

fn acquire_request_lock(
    config: &RecoveryConfig,
    request: &RestoreRequest,
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
        .open(lock_dir.join(format!("{}.lock", request.request_id)))
        .map_err(|error| RecoveryError::Io {
            operation: "opening request lock",
            message: error.to_string(),
        })?;
    FileExt::try_lock_exclusive(&lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            RecoveryError::RequestAlreadyRunning(request.request_id.clone())
        } else {
            RecoveryError::Io {
                operation: "locking request",
                message: error.to_string(),
            }
        }
    })?;
    Ok(lock)
}

fn acquire_profile_lock(config: &RecoveryConfig) -> Result<File, RecoveryError> {
    let lock_path = config.work_root.join(".locks").join("execution.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .truncate(false)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(lock_path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening profile lock",
            message: error.to_string(),
        })?;
    FileExt::try_lock_exclusive(&lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            RecoveryError::RecoveryBusy(config.profile.clone())
        } else {
            RecoveryError::Io {
                operation: "locking recovery profile",
                message: error.to_string(),
            }
        }
    })?;
    Ok(lock)
}

fn preserve_request_contract(
    config: &RecoveryConfig,
    request: &RestoreRequest,
) -> Result<(), RecoveryError> {
    let contract_dir = config.work_root.join(".contracts");
    prepare_secure_directory(&contract_dir)?;
    let contract_path = contract_dir.join(format!("{}.json", request.request_id));
    reject_symlink_path(&contract_path, "request contract")?;
    let contract = RequestContract {
        contract_format_version: 1,
        profile: config.profile.clone(),
        request: request.clone(),
    };
    if contract_path.is_file() {
        let existing: RequestContract = load_json(&contract_path)?;
        if existing != contract {
            return Err(RecoveryError::RequestConflict(request.request_id.clone()));
        }
        return Ok(());
    }
    write_json_atomic(&contract_path, &contract)
}

fn reusable_result(
    config: &RecoveryConfig,
    request: &RestoreRequest,
    result_path: &Path,
) -> Result<Option<RestoreResult>, RecoveryError> {
    if !result_path.is_file() {
        return Ok(None);
    }
    reject_symlink_path(result_path, "cached result")?;
    let existing: RestoreResult = load_json(result_path)?;
    if existing.request != *request {
        return Err(RecoveryError::RequestConflict(request.request_id.clone()));
    }
    let expected_artifact_path = result_path
        .parent()
        .expect("result path always has a request directory")
        .join("target-table.dump");
    if existing.result_format_version == RESULT_FORMAT_VERSION
        && existing.helper_version == env!("CARGO_PKG_VERSION")
        && existing.status == "completed"
        && existing.profile == config.profile
        && existing.cleanup_complete
        && !existing.recovered_owner.is_empty()
        && !existing.recovered_schema_sha256.is_empty()
        && existing.artifact_schema == ARTIFACT_SCHEMA
        && existing.artifact_table == artifact_table_name(&request.request_id)
        && !existing.artifact_schema_sha256.is_empty()
        && !existing.recovered_fingerprint.is_empty()
        && existing.artifact_path == expected_artifact_path
        && existing.artifact_path.is_file()
        && !fs::symlink_metadata(&existing.artifact_path)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        && sha256_file(&existing.artifact_path)? == existing.artifact_sha256
    {
        return Ok(Some(existing));
    }
    Ok(None)
}

fn prepare_runtime<'a>(
    config: &'a RecoveryConfig,
    request: &'a RestoreRequest,
    plan: &'a RestorePlan,
    work_dir: PathBuf,
    result_path: PathBuf,
) -> Result<Runtime<'a>, RecoveryError> {
    let socket_dir = socket_directory(config, &request.request_id)?;
    reconcile_stale_request(config, &work_dir, &socket_dir)?;
    prepare_secure_directory(&work_dir)?;
    let log_dir = work_dir.join("logs");
    prepare_secure_directory(&log_dir)?;

    let available = fs2::available_space(&config.work_root).map_err(|error| RecoveryError::Io {
        operation: "checking initial work filesystem capacity",
        message: error.to_string(),
    })?;
    let minimum_free = if plan.engine == RecoveryEngine::ClassicRestore {
        plan.estimated_backup_bytes
            .unwrap_or(0)
            .saturating_add(MIN_FREE_BYTES)
    } else {
        MIN_FREE_BYTES.min(config.max_work_bytes)
    };
    if available < minimum_free {
        return Err(RecoveryError::WorkQuotaExceeded {
            required: minimum_free,
            limit: available,
        });
    }
    let cancellation = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGINT, Arc::clone(&cancellation)).map_err(|error| {
        RecoveryError::Io {
            operation: "registering SIGINT handler",
            message: error.to_string(),
        }
    })?;
    signal_hook::flag::register(SIGTERM, Arc::clone(&cancellation)).map_err(|error| {
        RecoveryError::Io {
            operation: "registering SIGTERM handler",
            message: error.to_string(),
        }
    })?;

    Ok(Runtime {
        config,
        request,
        plan,
        pgdata: work_dir.join("pgdata"),
        state_path: work_dir.join("state.json"),
        result_path,
        artifact_path: work_dir.join("target-table.dump"),
        work_dir,
        socket_dir,
        log_dir,
        cancellation,
        postgres_started: false,
    })
}

fn tree_apparent_bytes(path: &Path) -> Result<u64, RecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "reading work quota metadata",
        message: format!("{}: {error}", path.display()),
    })?;
    if !metadata.is_dir() {
        return Ok(metadata.len());
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| RecoveryError::Io {
        operation: "walking work quota directory",
        message: format!("{}: {error}", path.display()),
    })? {
        let entry = entry.map_err(|error| RecoveryError::Io {
            operation: "reading work quota entry",
            message: error.to_string(),
        })?;
        total = total.saturating_add(tree_apparent_bytes(&entry.path())?);
    }
    Ok(total)
}

fn execute_inner(runtime: &mut Runtime<'_>) -> Result<RestoreResult, RecoveryError> {
    let total_started = Instant::now();
    runtime.check_cancelled()?;

    let materialize_started = Instant::now();
    runtime.write_state(
        "materializing",
        Some(format!("engine={:?}", runtime.plan.engine)),
    )?;
    let repository_lock = materialize(runtime)?;
    let materialize_ms = elapsed_ms(materialize_started);
    runtime.check_work_quota()?;

    validate_materialized_cluster(runtime)?;
    if runtime.plan.engine == RecoveryEngine::SnapshotDirect {
        configure_snapshot_recovery(runtime)?;
    }

    let recovery_started = Instant::now();
    runtime.write_state("recovering", None)?;
    start_postgres(runtime)?;
    wait_for_promotion(runtime)?;
    FileExt::unlock(&repository_lock).map_err(|error| RecoveryError::Io {
        operation: "unlocking repository after recovery",
        message: error.to_string(),
    })?;
    let recovery_ms = elapsed_ms(recovery_started);
    runtime.check_work_quota()?;

    let validate_started = Instant::now();
    runtime.write_state("validating", None)?;
    let (row_count, schema_sha256, fingerprint, recovered_security) =
        validate_recovered_table(runtime)?;
    validate_expected_recovery(runtime, &schema_sha256, &fingerprint)?;
    let validate_ms = elapsed_ms(validate_started);

    let extract_started = Instant::now();
    runtime.write_state("extracting", None)?;
    let artifact_table = artifact_table_name(&runtime.request.request_id);
    let artifact_schema_sha256 = prepare_export_table(runtime, &artifact_table)?;
    extract_table(runtime)?;
    runtime.check_work_quota()?;
    let extract_ms = elapsed_ms(extract_started);
    let artifact_bytes = fs::metadata(&runtime.artifact_path)
        .map_err(|error| RecoveryError::Io {
            operation: "reading artifact metadata",
            message: error.to_string(),
        })?
        .len();
    if artifact_bytes == 0 {
        return Err(RecoveryError::Io {
            operation: "validating artifact",
            message: "pg_dump produced an empty artifact".to_owned(),
        });
    }
    fs::set_permissions(&runtime.artifact_path, fs::Permissions::from_mode(0o600)).map_err(
        |error| RecoveryError::Io {
            operation: "securing artifact permissions",
            message: error.to_string(),
        },
    )?;
    let artifact_sha256 = sha256_file(&runtime.artifact_path)?;

    let probe = crate::run_probe(runtime.config)?;
    Ok(RestoreResult {
        result_format_version: RESULT_FORMAT_VERSION,
        helper_version: env!("CARGO_PKG_VERSION").to_owned(),
        status: "completed".to_owned(),
        request: runtime.request.clone(),
        profile: runtime.config.profile.clone(),
        engine: runtime.plan.engine,
        stanza: runtime.config.stanza.clone(),
        repository_key: runtime.config.repository_key,
        backup_label: runtime.plan.backup_label.clone(),
        backup_stop_lsn: runtime.plan.backup_stop_lsn.clone(),
        postgres_version: probe.postgres.detail,
        pgbackrest_version: probe.pgbackrest.detail,
        recovered_row_count: row_count,
        recovered_owner: recovered_security.owner,
        recovered_acl: recovered_security.acl,
        recovered_schema_sha256: schema_sha256,
        artifact_schema: ARTIFACT_SCHEMA.to_owned(),
        artifact_table,
        artifact_schema_sha256,
        recovered_fingerprint: fingerprint,
        artifact_path: runtime.artifact_path.clone(),
        artifact_bytes,
        artifact_sha256,
        durations: ExecutionDurations {
            materialize_ms,
            recovery_ms,
            validate_ms,
            extract_ms,
            total_ms: elapsed_ms(total_started),
        },
        cleanup_complete: false,
    })
}

fn validate_expected_recovery(
    runtime: &Runtime<'_>,
    schema_sha256: &str,
    fingerprint: &str,
) -> Result<(), RecoveryError> {
    if let Some(expected) = &runtime.request.expected_schema_sha256 {
        if expected != schema_sha256 {
            return Err(RecoveryError::SchemaFingerprintMismatch {
                expected: expected.clone(),
                actual: schema_sha256.to_owned(),
            });
        }
    }
    if let Some(expected) = &runtime.request.expected_fingerprint {
        if expected != fingerprint {
            return Err(RecoveryError::FingerprintMismatch {
                expected: expected.clone(),
                actual: fingerprint.to_owned(),
            });
        }
    }
    Ok(())
}

fn materialize(runtime: &mut Runtime<'_>) -> Result<File, RecoveryError> {
    let repository_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .truncate(false)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&runtime.config.expire_lock_path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening repository coordination lock",
            message: error.to_string(),
        })?;
    FileExt::try_lock_shared(&repository_lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            RecoveryError::RepositoryBusy
        } else {
            RecoveryError::Io {
                operation: "locking repository for restore",
                message: error.to_string(),
            }
        }
    })?;

    let current = select_backup(
        &read_backup_catalog(runtime.config)?,
        &runtime.request.target.value,
    )?;
    if current.label != runtime.plan.backup_label
        || current.stop_lsn != runtime.plan.backup_stop_lsn
    {
        return Err(RecoveryError::SelectedBackupChanged {
            planned: format!(
                "{}@{}",
                runtime.plan.backup_label, runtime.plan.backup_stop_lsn
            ),
            actual: format!("{}@{}", current.label, current.stop_lsn),
        });
    }

    match runtime.plan.engine {
        RecoveryEngine::SnapshotDirect => materialize_snapshot(runtime)?,
        RecoveryEngine::ClassicRestore => materialize_classic(runtime)?,
    }
    Ok(repository_lock)
}

fn materialize_snapshot(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    let source = direct_backup_tree(
        &runtime.config.repository_path,
        &runtime.config.stanza,
        &runtime.plan.backup_label,
    );
    reject_repository_symlink_components(&runtime.config.repository_path, &source)?;
    validate_no_tablespaces(&source)?;
    fs::create_dir(&runtime.pgdata).map_err(|error| RecoveryError::Io {
        operation: "creating snapshot pgdata directory",
        message: error.to_string(),
    })?;
    fs::set_permissions(&runtime.pgdata, fs::Permissions::from_mode(0o700)).map_err(|error| {
        RecoveryError::Io {
            operation: "securing snapshot pgdata directory",
            message: error.to_string(),
        }
    })?;

    let mut command = Command::new(&runtime.config.cp_bin);
    command
        .arg("-a")
        .arg("--reflink=always")
        .arg("--")
        .arg(source.join("."))
        .arg(&runtime.pgdata);
    run_logged_command(
        &mut command,
        &runtime.log_dir.join("materialize-snapshot.log"),
        &runtime.active_process_path(),
        "snapshot materialization",
        runtime.config.command_timeout_seconds,
        &runtime.cancellation,
    )
}

fn materialize_classic(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    let config_arg = format!("--config={}", runtime.config.pgbackrest_config.display());
    let stanza_arg = format!("--stanza={}", runtime.config.stanza);
    let repo_arg = format!("--repo={}", runtime.config.repository_key);
    let pg_path_arg = format!("--pg1-path={}", runtime.pgdata.display());
    let set_arg = format!("--set={}", runtime.plan.backup_label);
    let target_arg = format!("--target={}", runtime.request.target.value);
    let mut command = Command::new(&runtime.config.pgbackrest_bin);
    command.args([
        config_arg.as_str(),
        stanza_arg.as_str(),
        repo_arg.as_str(),
        pg_path_arg.as_str(),
        set_arg.as_str(),
        "--type=lsn",
        target_arg.as_str(),
        "--target-action=promote",
        "--archive-mode=off",
    ]);
    if !runtime.request.target.inclusive {
        command.arg("--target-exclusive");
    }
    command.arg("restore");
    run_logged_command(
        &mut command,
        &runtime.log_dir.join("materialize-classic.log"),
        &runtime.active_process_path(),
        "classic pgBackRest restore",
        runtime.config.command_timeout_seconds,
        &runtime.cancellation,
    )
}

fn validate_materialized_cluster(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    validate_no_tablespaces(&runtime.pgdata)?;
    if runtime.pgdata.join("pg_wal").is_symlink() {
        return Err(RecoveryError::UnsupportedTopology(
            "symlinked pg_wal is not supported in the first release".to_owned(),
        ));
    }
    for name in [
        "postgresql.conf",
        "postgresql.auto.conf",
        "pg_hba.conf",
        "pg_ident.conf",
    ] {
        let path = runtime.pgdata.join(name);
        if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(RecoveryError::UnsupportedTopology(format!(
                "symlinked PostgreSQL configuration is not supported: {name}"
            )));
        }
    }
    let backup_major = fs::read_to_string(runtime.pgdata.join("PG_VERSION"))
        .map_err(|error| RecoveryError::Io {
            operation: "reading recovered PG_VERSION",
            message: error.to_string(),
        })?
        .trim()
        .to_owned();
    if !["15", "16", "17", "18"].contains(&backup_major.as_str()) {
        return Err(RecoveryError::UnsupportedTopology(format!(
            "PostgreSQL major {backup_major} is outside the supported 15-18 range"
        )));
    }
    let postgres = command_output(&runtime.config.pg_bin_dir.join("postgres"), &["--version"])?;
    if !postgres.contains(&format!(" {backup_major}.")) {
        return Err(RecoveryError::UnsupportedTopology(format!(
            "configured postgres binary does not match backup major {backup_major}: {postgres}"
        )));
    }
    Ok(())
}

fn validate_no_tablespaces(pgdata: &Path) -> Result<(), RecoveryError> {
    let tablespace_dir = pgdata.join("pg_tblspc");
    if tablespace_dir.is_dir() {
        let mut entries = fs::read_dir(&tablespace_dir).map_err(|error| RecoveryError::Io {
            operation: "inspecting tablespaces",
            message: error.to_string(),
        })?;
        if entries
            .next()
            .transpose()
            .map_err(|error| RecoveryError::Io {
                operation: "reading tablespace entry",
                message: error.to_string(),
            })?
            .is_some()
        {
            return Err(RecoveryError::UnsupportedTopology(
                "tablespaces are not supported in the first release".to_owned(),
            ));
        }
    }
    Ok(())
}

fn configure_snapshot_recovery(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    for file in [
        "postmaster.pid",
        "postmaster.opts",
        "standby.signal",
        "recovery.signal",
    ] {
        remove_file_if_exists(&runtime.pgdata.join(file))?;
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(runtime.pgdata.join("recovery.signal"))
        .map_err(|error| RecoveryError::Io {
            operation: "creating recovery.signal",
            message: error.to_string(),
        })?;

    let restore_command = [
        shell_quote(&runtime.config.pgbackrest_bin.display().to_string()),
        shell_quote(&format!(
            "--config={}",
            runtime.config.pgbackrest_config.display()
        )),
        shell_quote(&format!("--pg1-path={}", runtime.pgdata.display())),
        shell_quote(&format!("--stanza={}", runtime.config.stanza)),
        shell_quote(&format!("--repo={}", runtime.config.repository_key)),
        "archive-get".to_owned(),
        shell_quote("%f"),
        shell_quote("%p"),
    ]
    .join(" ");

    let mut auto_conf = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(runtime.pgdata.join("postgresql.auto.conf"))
        .map_err(|error| RecoveryError::Io {
            operation: "opening postgresql.auto.conf",
            message: error.to_string(),
        })?;
    writeln!(
        auto_conf,
        "\nrestore_command = {}\nrecovery_target_lsn = {}\nrecovery_target_inclusive = {}\nrecovery_target_action = 'promote'",
        guc_quote(&restore_command),
        guc_quote(&runtime.request.target.value),
        if runtime.request.target.inclusive {
            "on"
        } else {
            "off"
        }
    )
    .map_err(|error| RecoveryError::Io {
        operation: "writing recovery configuration",
        message: error.to_string(),
    })?;
    auto_conf.sync_all().map_err(|error| RecoveryError::Io {
        operation: "syncing recovery configuration",
        message: error.to_string(),
    })?;
    Ok(())
}

fn start_postgres(runtime: &mut Runtime<'_>) -> Result<(), RecoveryError> {
    prepare_secure_directory(&runtime.socket_dir)?;
    let hba_path = runtime.work_dir.join("recovery-pg_hba.conf");
    let mut hba = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&hba_path)
        .map_err(|error| RecoveryError::Io {
            operation: "creating recovery HBA",
            message: error.to_string(),
        })?;
    hba.write_all(b"local all all trust\n")
        .map_err(|error| RecoveryError::Io {
            operation: "writing recovery HBA",
            message: error.to_string(),
        })?;
    hba.sync_all().map_err(|error| RecoveryError::Io {
        operation: "syncing recovery HBA",
        message: error.to_string(),
    })?;

    let options = [
        format!(
            "-c {}",
            shell_quote(&format!("data_directory={}", runtime.pgdata.display()))
        ),
        format!("-p {}", runtime.config.recovery_port),
        format!(
            "-k {}",
            shell_quote(&runtime.socket_dir.display().to_string())
        ),
        format!("-c {}", shell_quote("listen_addresses=")),
        format!("-c {}", shell_quote("archive_mode=off")),
        format!(
            "-c {}",
            shell_quote(&format!("hba_file={}", hba_path.display()))
        ),
        format!("-c {}", shell_quote("shared_preload_libraries=")),
        format!("-c {}", shell_quote("local_preload_libraries=")),
        format!("-c {}", shell_quote("session_preload_libraries=")),
        format!("-c {}", shell_quote("archive_cleanup_command=")),
        format!("-c {}", shell_quote("recovery_end_command=")),
        format!("-c {}", shell_quote("logging_collector=off")),
        format!("-c {}", shell_quote("external_pid_file=")),
        format!("-c {}", shell_quote("unix_socket_permissions=0700")),
    ]
    .join(" ");

    let mut command = Command::new(runtime.config.pg_bin_dir.join("pg_ctl"));
    command
        .arg("-D")
        .arg(&runtime.pgdata)
        .arg("-l")
        .arg(runtime.log_dir.join("postgres.log"))
        .arg("-o")
        .arg(options)
        .arg("start")
        .arg("-w")
        .arg("-t")
        .arg(runtime.config.command_timeout_seconds.to_string());
    let result = run_logged_command(
        &mut command,
        &runtime.log_dir.join("pg-ctl-start.log"),
        &runtime.active_process_path(),
        "starting temporary PostgreSQL",
        runtime.config.command_timeout_seconds,
        &runtime.cancellation,
    );
    runtime.postgres_started = runtime.pgdata.join("postmaster.pid").exists();
    result?;
    runtime.postgres_started = true;
    Ok(())
}

fn wait_for_promotion(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    let started = Instant::now();
    let mut last_quota_check = Instant::now();
    let timeout = Duration::from_secs(runtime.config.recovery_timeout_seconds);
    loop {
        runtime.check_cancelled()?;
        if last_quota_check.elapsed() >= Duration::from_secs(1) {
            runtime.check_work_quota()?;
            last_quota_check = Instant::now();
        }
        if started.elapsed() > timeout {
            return Err(RecoveryError::CommandTimeout {
                operation: "native PostgreSQL recovery",
                timeout_seconds: runtime.config.recovery_timeout_seconds,
            });
        }
        if !runtime.pgdata.join("postmaster.pid").exists() {
            let detail = read_log_tail(&runtime.log_dir.join("postgres.log"));
            if detail.contains("recovery ended before configured recovery target was reached") {
                return Err(RecoveryError::RecoveryTargetUnreachable {
                    target: runtime.request.target.value.clone(),
                });
            }
            return Err(RecoveryError::CommandFailed {
                program: runtime.config.pg_bin_dir.join("postgres"),
                message: detail,
            });
        }
        if let Ok(value) = psql_output(runtime, "SELECT pg_is_in_recovery();") {
            if value == "f" {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn validate_recovered_table(
    runtime: &Runtime<'_>,
) -> Result<(u64, String, String, RecoveredSecurityMetadata), RecoveryError> {
    let qualified =
        qualified_identifier(&runtime.request.table.schema, &runtime.request.table.name);
    let oid = psql_output(
        runtime,
        &format!(
            "SELECT COALESCE(to_regclass({})::oid::text, '');",
            sql_literal(&qualified)
        ),
    )?;
    if oid.is_empty() {
        return Err(RecoveryError::TableNotFound(format!(
            "{}.{}",
            runtime.request.table.schema, runtime.request.table.name
        )));
    }
    let actual_oid = oid.parse::<u32>().map_err(|error| RecoveryError::Io {
        operation: "parsing recovered table OID",
        message: error.to_string(),
    })?;
    if actual_oid != runtime.request.table.rel_oid {
        return Err(RecoveryError::TableIdentityMismatch {
            expected: runtime.request.table.rel_oid,
            actual: actual_oid,
        });
    }

    let relkind = psql_output(
        runtime,
        &format!("SELECT relkind FROM pg_class WHERE oid = {actual_oid};"),
    )?;
    if relkind != "r" {
        return Err(RecoveryError::UnsupportedTopology(format!(
            "relation kind {relkind} is not an ordinary table"
        )));
    }

    let schema_contract = psql_output(runtime, &schema_contract_sql(actual_oid))?;
    let schema_sha256 = sha256_bytes(schema_contract.as_bytes());

    let fingerprint = psql_output(
        runtime,
        &format!(
            "SELECT count(*)::text || '|' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM {qualified} AS t;"
        ),
    )?;
    let (count, _) = fingerprint
        .split_once('|')
        .ok_or_else(|| RecoveryError::Io {
            operation: "parsing recovered fingerprint",
            message: format!("unexpected fingerprint format: {fingerprint}"),
        })?;
    let row_count = count.parse::<u64>().map_err(|error| RecoveryError::Io {
        operation: "parsing recovered row count",
        message: error.to_string(),
    })?;
    let security_json = psql_output(
        runtime,
        &format!(
            "SELECT jsonb_build_object(\
                'owner', pg_get_userbyid(c.relowner), \
                'acl', COALESCE((\
                    SELECT jsonb_agg(jsonb_build_object(\
                        'grantee', CASE WHEN ae.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(ae.grantee) END, \
                        'privilege', ae.privilege_type, \
                        'is_grantable', ae.is_grantable\
                    ) ORDER BY ae.grantee, ae.privilege_type, ae.is_grantable) \
                    FROM aclexplode(c.relacl) AS ae\
                ), '[]'::jsonb)\
            )::text FROM pg_class AS c WHERE c.oid = {actual_oid};"
        ),
    )?;
    let recovered_security: RecoveredSecurityMetadata = serde_json::from_str(&security_json)
        .map_err(|error| RecoveryError::Io {
            operation: "parsing recovered owner and ACL metadata",
            message: error.to_string(),
        })?;
    if recovered_security.owner.is_empty()
        || recovered_security.acl.iter().any(|entry| {
            entry.grantee.is_empty()
                || entry.privilege.is_empty()
                || entry.grantee.contains('\0')
                || entry.privilege.contains('\0')
        })
    {
        return Err(RecoveryError::Io {
            operation: "validating recovered owner and ACL metadata",
            message: "empty or invalid owner/ACL field".to_owned(),
        });
    }
    Ok((row_count, schema_sha256, fingerprint, recovered_security))
}

fn prepare_export_table(
    runtime: &Runtime<'_>,
    artifact_table: &str,
) -> Result<String, RecoveryError> {
    let original = qualified_identifier(&runtime.request.table.schema, &runtime.request.table.name);
    psql_output(
        runtime,
        &format!(
            "CREATE SCHEMA IF NOT EXISTS {artifact_schema}; ALTER TABLE {original} SET SCHEMA {artifact_schema}; ALTER TABLE {artifact_schema}.{original_table} RENAME TO {artifact_table};",
            artifact_schema = quote_identifier(ARTIFACT_SCHEMA),
            original_table = quote_identifier(&runtime.request.table.name),
            artifact_table = quote_identifier(artifact_table),
        ),
    )?;
    let schema_contract =
        psql_output(runtime, &schema_contract_sql(runtime.request.table.rel_oid))?;
    Ok(sha256_bytes(schema_contract.as_bytes()))
}

fn schema_contract_sql(table_oid: u32) -> String {
    format!(
        "SET search_path = pg_catalog; WITH columns AS (\
           SELECT a.attnum, a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, \
                  a.attnotnull, a.attidentity, a.attgenerated, \
                  CASE WHEN a.attcollation = 0 THEN NULL ELSE a.attcollation::regcollation::text END AS collation, \
                  pg_get_expr(d.adbin, d.adrelid) AS default_expression \
             FROM pg_attribute AS a \
             LEFT JOIN pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
            WHERE a.attrelid = {table_oid} AND a.attnum > 0 AND NOT a.attisdropped\
        ), constraints AS (\
           SELECT conname, contype, condeferrable, condeferred, convalidated, \
                  pg_get_constraintdef(oid, true) AS definition \
             FROM pg_constraint WHERE conrelid = {table_oid}\
        ), indexes AS (\
           SELECT c.relname, pg_get_indexdef(i.indexrelid) AS definition \
             FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indexrelid \
            WHERE i.indrelid = {table_oid}\
        ), triggers AS (\
           SELECT tgname, pg_get_triggerdef(oid, true) AS definition \
             FROM pg_trigger WHERE tgrelid = {table_oid} AND NOT tgisinternal\
        ), policies AS (\
           SELECT polname, polcmd, polpermissive, polroles, \
                  pg_get_expr(polqual, polrelid) AS using_expression, \
                  pg_get_expr(polwithcheck, polrelid) AS check_expression \
             FROM pg_policy WHERE polrelid = {table_oid}\
        ) \
        SELECT jsonb_build_object(\
          'table', (SELECT jsonb_build_object(\
              'schema', n.nspname, 'name', c.relname, 'kind', c.relkind, \
              'persistence', c.relpersistence, 'replica_identity', c.relreplident, \
              'row_security', c.relrowsecurity, 'force_row_security', c.relforcerowsecurity, \
              'options', c.reloptions, 'partition_bound', pg_get_expr(c.relpartbound, c.oid)) \
            FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace \
           WHERE c.oid = {table_oid}), \
          'columns', (SELECT COALESCE(jsonb_agg(to_jsonb(columns) ORDER BY attnum), '[]'::jsonb) FROM columns), \
          'constraints', (SELECT COALESCE(jsonb_agg(to_jsonb(constraints) ORDER BY conname), '[]'::jsonb) FROM constraints), \
          'indexes', (SELECT COALESCE(jsonb_agg(to_jsonb(indexes) ORDER BY relname), '[]'::jsonb) FROM indexes), \
          'triggers', (SELECT COALESCE(jsonb_agg(to_jsonb(triggers) ORDER BY tgname), '[]'::jsonb) FROM triggers), \
          'policies', (SELECT COALESCE(jsonb_agg(to_jsonb(policies) ORDER BY polname), '[]'::jsonb) FROM policies)\
        )::text;"
    )
}

fn extract_table(runtime: &Runtime<'_>) -> Result<(), RecoveryError> {
    let pattern = pg_dump_table_pattern(
        ARTIFACT_SCHEMA,
        &artifact_table_name(&runtime.request.request_id),
    );
    let mut command = Command::new(runtime.config.pg_bin_dir.join("pg_dump"));
    command
        .arg("--format=custom")
        .arg("--no-owner")
        .arg("--no-acl")
        .arg("--strict-names")
        .arg(format!("--table={pattern}"))
        .arg("--host")
        .arg(&runtime.socket_dir)
        .arg("--port")
        .arg(runtime.config.recovery_port.to_string())
        .arg("--username")
        .arg(&runtime.config.recovery_user)
        .arg("--dbname")
        .arg(&runtime.request.database)
        .arg("--file")
        .arg(&runtime.artifact_path);
    run_logged_command(
        &mut command,
        &runtime.log_dir.join("pg-dump.log"),
        &runtime.active_process_path(),
        "extracting target table",
        runtime.config.command_timeout_seconds,
        &runtime.cancellation,
    )
}

fn psql_output(runtime: &Runtime<'_>, sql: &str) -> Result<String, RecoveryError> {
    runtime.check_cancelled()?;
    let program = runtime.config.pg_bin_dir.join("psql");
    let mut command = Command::new(&program);
    command
        .arg("-X")
        .arg("-qAt")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-h")
        .arg(&runtime.socket_dir)
        .arg("-p")
        .arg(runtime.config.recovery_port.to_string())
        .arg("-U")
        .arg(&runtime.config.recovery_user)
        .arg("-d")
        .arg(&runtime.request.database);
    command.arg("-c").arg(sql);
    let output = run_captured_command(
        &mut command,
        &runtime.log_dir.join("psql-stdout.log"),
        &runtime.log_dir.join("psql-stderr.log"),
        &runtime.active_process_path(),
        "querying temporary PostgreSQL",
        runtime.config.command_timeout_seconds,
        &runtime.cancellation,
    )?;
    if !output.status.success() {
        return Err(RecoveryError::CommandFailed {
            program,
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn run_captured_command(
    command: &mut Command,
    stdout_path: &Path,
    stderr_path: &Path,
    active_process_path: &Path,
    operation: &'static str,
    timeout_seconds: u64,
    cancellation: &Arc<AtomicBool>,
) -> Result<std::process::Output, RecoveryError> {
    let program = PathBuf::from(command.get_program());
    let stdout_file = secure_output_file(stdout_path)?;
    let stderr_file = secure_output_file(stderr_path)?;
    command
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| RecoveryError::CommandFailed {
            program,
            message: error.to_string(),
        })?;
    let (process_group, _active_process) =
        register_active_process(&mut child, active_process_path, operation)?;
    let started = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);

    loop {
        if let Some(status) = child.try_wait().map_err(|error| RecoveryError::Io {
            operation: "waiting for captured command",
            message: error.to_string(),
        })? {
            return Ok(std::process::Output {
                status,
                stdout: fs::read(stdout_path).map_err(|error| RecoveryError::Io {
                    operation: "reading captured stdout",
                    message: error.to_string(),
                })?,
                stderr: fs::read(stderr_path).map_err(|error| RecoveryError::Io {
                    operation: "reading captured stderr",
                    message: error.to_string(),
                })?,
            });
        }
        if cancellation.load(Ordering::SeqCst) {
            terminate_process_group(&mut child, process_group);
            return Err(RecoveryError::Cancelled);
        }
        if started.elapsed() > timeout {
            terminate_process_group(&mut child, process_group);
            return Err(RecoveryError::CommandTimeout {
                operation,
                timeout_seconds,
            });
        }
        thread::sleep(COMMAND_POLL_INTERVAL);
    }
}

fn secure_output_file(path: &Path) -> Result<File, RecoveryError> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening captured command output",
            message: error.to_string(),
        })
}

fn run_logged_command(
    command: &mut Command,
    log_path: &Path,
    active_process_path: &Path,
    operation: &'static str,
    timeout_seconds: u64,
    cancellation: &Arc<AtomicBool>,
) -> Result<(), RecoveryError> {
    let program = PathBuf::from(command.get_program());
    let log = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(log_path)
        .map_err(|error| RecoveryError::Io {
            operation: "opening command log",
            message: error.to_string(),
        })?;
    let stderr = log.try_clone().map_err(|error| RecoveryError::Io {
        operation: "cloning command log handle",
        message: error.to_string(),
    })?;
    command
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| RecoveryError::CommandFailed {
            program: program.clone(),
            message: error.to_string(),
        })?;
    let (process_group, _active_process) =
        register_active_process(&mut child, active_process_path, operation)?;
    let started = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);

    loop {
        if let Some(status) = child.try_wait().map_err(|error| RecoveryError::Io {
            operation: "waiting for child command",
            message: error.to_string(),
        })? {
            return if status.success() {
                Ok(())
            } else {
                Err(RecoveryError::CommandFailed {
                    program,
                    message: read_log_tail(log_path),
                })
            };
        }
        if cancellation.load(Ordering::SeqCst) {
            terminate_process_group(&mut child, process_group);
            return Err(RecoveryError::Cancelled);
        }
        if started.elapsed() > timeout {
            terminate_process_group(&mut child, process_group);
            return Err(RecoveryError::CommandTimeout {
                operation,
                timeout_seconds,
            });
        }
        thread::sleep(COMMAND_POLL_INTERVAL);
    }
}

fn child_process_group(child: &std::process::Child) -> Result<Pid, RecoveryError> {
    Ok(Pid::from_raw(i32::try_from(child.id()).map_err(
        |error| RecoveryError::Io {
            operation: "converting child process ID",
            message: error.to_string(),
        },
    )?))
}

fn register_active_process(
    child: &mut std::process::Child,
    path: &Path,
    operation: &'static str,
) -> Result<(Pid, ActiveProcessGuard), RecoveryError> {
    let registration = (|| {
        let process_group = child_process_group(child)?;
        let start_ticks = process_start_ticks(child.id())?;
        write_json_atomic(
            path,
            &ActiveProcessState {
                process_group: process_group.as_raw(),
                start_ticks,
                operation: operation.to_owned(),
            },
        )?;
        Ok((
            process_group,
            ActiveProcessGuard {
                path: path.to_path_buf(),
            },
        ))
    })();
    if registration.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    registration
}

fn process_start_ticks(process_id: u32) -> Result<u64, RecoveryError> {
    let stat_path = PathBuf::from(format!("/proc/{process_id}/stat"));
    let stat = fs::read_to_string(&stat_path).map_err(|error| RecoveryError::Io {
        operation: "reading child process identity",
        message: format!("{}: {error}", stat_path.display()),
    })?;
    let after_name = stat
        .rsplit_once(')')
        .map_or(stat.as_str(), |(_, tail)| tail);
    after_name
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| RecoveryError::Io {
            operation: "parsing child process identity",
            message: stat_path.display().to_string(),
        })?
        .parse::<u64>()
        .map_err(|error| RecoveryError::Io {
            operation: "parsing child process start time",
            message: error.to_string(),
        })
}

fn terminate_process_group(child: &mut std::process::Child, process_group: Pid) {
    let _ = killpg(process_group, Signal::SIGTERM);
    for _ in 0..20 {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = killpg(process_group, Signal::SIGKILL);
    let _ = child.wait();
}

fn stop_postgres(config: &RecoveryConfig, pgdata: &Path) -> Result<(), RecoveryError> {
    if !pgdata.join("postmaster.pid").exists() {
        return Ok(());
    }
    let pg_ctl = config.pg_bin_dir.join("pg_ctl");
    let stop = Command::new(&pg_ctl)
        .arg("-D")
        .arg(pgdata)
        .args(["stop", "-m", "fast", "-w", "-t"])
        .arg(PROCESS_STOP_TIMEOUT.to_string())
        .output();
    if matches!(&stop, Ok(output) if output.status.success()) {
        return Ok(());
    }
    let immediate = Command::new(&pg_ctl)
        .arg("-D")
        .arg(pgdata)
        .args(["stop", "-m", "immediate", "-w", "-t"])
        .arg(PROCESS_STOP_TIMEOUT.to_string())
        .output()
        .map_err(|error| RecoveryError::CleanupFailed(error.to_string()))?;
    if immediate.status.success() {
        Ok(())
    } else {
        Err(RecoveryError::CleanupFailed(
            String::from_utf8_lossy(&immediate.stderr).trim().to_owned(),
        ))
    }
}

fn reconcile_abandoned_requests(config: &RecoveryConfig) -> Result<(), RecoveryError> {
    for entry in fs::read_dir(&config.work_root).map_err(|error| RecoveryError::Io {
        operation: "scanning abandoned recovery requests",
        message: error.to_string(),
    })? {
        let entry = entry.map_err(|error| RecoveryError::Io {
            operation: "reading abandoned recovery entry",
            message: error.to_string(),
        })?;
        let file_type = entry.file_type().map_err(|error| RecoveryError::Io {
            operation: "inspecting abandoned recovery entry",
            message: error.to_string(),
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(request_id) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if request_id.starts_with('.') || !safe_request_directory_name(&request_id) {
            continue;
        }

        let work_dir = entry.path();
        let active_process_path = work_dir.join("active-process.json");
        let pgdata = work_dir.join("pgdata");
        let socket_dir = socket_directory(config, &request_id)?;
        let abandoned = active_process_path.is_file() || pgdata.exists() || socket_dir.exists();
        if !abandoned {
            continue;
        }

        terminate_recorded_process(&active_process_path)?;
        stop_postgres(config, &pgdata)?;
        remove_dir_if_exists(&pgdata).map_err(|error| {
            RecoveryError::CleanupFailed(format!(
                "cannot remove abandoned pgdata for {request_id}: {error}"
            ))
        })?;
        remove_dir_if_exists(&socket_dir).map_err(|error| {
            RecoveryError::CleanupFailed(format!(
                "cannot remove abandoned socket directory for {request_id}: {error}"
            ))
        })?;
        remove_file_if_exists(&work_dir.join("target-table.dump"))?;
        write_json_atomic(
            &work_dir.join("state.json"),
            &ExecutionState {
                request_id,
                phase: "reconciled_after_crash".to_owned(),
                updated_at_unix_seconds: unix_seconds()?,
                detail: None,
            },
        )?;
    }
    Ok(())
}

fn terminate_recorded_process(path: &Path) -> Result<(), RecoveryError> {
    if !path.is_file() {
        return Ok(());
    }
    let state: ActiveProcessState = load_json(path)?;
    if state.process_group <= 1 {
        return Err(RecoveryError::CleanupFailed(format!(
            "invalid recorded process group {}",
            state.process_group
        )));
    }
    let process_id = u32::try_from(state.process_group).map_err(|error| {
        RecoveryError::CleanupFailed(format!("invalid recorded process group: {error}"))
    })?;
    if process_matches(process_id, state.start_ticks) {
        let process_group = Pid::from_raw(state.process_group);
        let _ = killpg(process_group, Signal::SIGTERM);
        for _ in 0..20 {
            if !process_matches(process_id, state.start_ticks) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        if process_matches(process_id, state.start_ticks) {
            let _ = killpg(process_group, Signal::SIGKILL);
        }
        for _ in 0..20 {
            if !process_matches(process_id, state.start_ticks) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        if process_matches(process_id, state.start_ticks) {
            return Err(RecoveryError::CleanupFailed(format!(
                "recorded process group {} did not exit",
                state.process_group
            )));
        }
    }
    remove_file_if_exists(path)
}

fn process_matches(process_id: u32, start_ticks: u64) -> bool {
    process_start_ticks(process_id).is_ok_and(|actual| actual == start_ticks)
}

fn safe_request_directory_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn reconcile_stale_request(
    config: &RecoveryConfig,
    work_dir: &Path,
    socket_dir: &Path,
) -> Result<(), RecoveryError> {
    for path in [work_dir, socket_dir] {
        if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(RecoveryError::InvalidConfig(format!(
                "recovery runtime path may not be a symlink: {}",
                path.display()
            )));
        }
    }
    let pgdata = work_dir.join("pgdata");
    if pgdata.join("postmaster.pid").exists() {
        stop_postgres(config, &pgdata)?;
    }
    remove_dir_if_exists(work_dir).map_err(|error| RecoveryError::Io {
        operation: "removing stale request directory",
        message: error.to_string(),
    })?;
    remove_dir_if_exists(socket_dir).map_err(|error| RecoveryError::Io {
        operation: "removing stale socket directory",
        message: error.to_string(),
    })?;
    Ok(())
}

fn reject_symlink_path(path: &Path, label: &str) -> Result<(), RecoveryError> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(RecoveryError::InvalidConfig(format!(
            "{label} may not be a symlink: {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_repository_symlink_components(root: &Path, target: &Path) -> Result<(), RecoveryError> {
    let relative = target.strip_prefix(root).map_err(|_| {
        RecoveryError::InvalidConfig(format!(
            "snapshot source escaped repository root: {}",
            target.display()
        ))
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| RecoveryError::Io {
            operation: "inspecting snapshot source",
            message: format!("{}: {error}", current.display()),
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RecoveryError::UnsupportedTopology(format!(
                "snapshot source may not traverse a symlink: {}",
                current.display()
            )));
        }
    }
    Ok(())
}

fn prepare_secure_directory(path: &Path) -> Result<(), RecoveryError> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(RecoveryError::InvalidConfig(format!(
            "secure directory may not be a symlink: {}",
            path.display()
        )));
    }
    fs::create_dir_all(path).map_err(|error| RecoveryError::Io {
        operation: "creating secure directory",
        message: format!("{}: {error}", path.display()),
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|error| RecoveryError::Io {
        operation: "inspecting secure directory",
        message: format!("{}: {error}", path.display()),
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RecoveryError::InvalidConfig(format!(
            "secure path must be a real directory: {}",
            path.display()
        )));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        RecoveryError::Io {
            operation: "setting secure directory permissions",
            message: format!("{}: {error}", path.display()),
        }
    })
}

fn socket_directory(config: &RecoveryConfig, request_id: &str) -> Result<PathBuf, RecoveryError> {
    let digest = Sha256::digest(request_id.as_bytes());
    let suffix = digest[..8].iter().fold(String::new(), |mut text, byte| {
        write!(&mut text, "{byte:02x}").expect("writing to a String cannot fail");
        text
    });
    let path = config.socket_root.join(format!("pgfb-{suffix}"));
    let socket_file_len =
        path.as_os_str().as_bytes().len() + format!("/.s.PGSQL.{}", config.recovery_port).len();
    if socket_file_len >= 100 {
        return Err(RecoveryError::InvalidConfig(format!(
            "socket path is too long for PostgreSQL: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn pg_dump_table_pattern(schema: &str, table: &str) -> String {
    qualified_identifier(schema, table)
}

fn qualified_identifier(schema: &str, table: &str) -> String {
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn artifact_table_name(request_id: &str) -> String {
    let digest = Sha256::digest(request_id.as_bytes());
    let suffix = digest[..8].iter().fold(String::new(), |mut text, byte| {
        write!(&mut text, "{byte:02x}").expect("writing to a String cannot fail");
        text
    });
    format!("r_{suffix}")
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn guc_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn command_output(program: &Path, args: &[&str]) -> Result<String, RecoveryError> {
    let output = Command::new(program).args(args).output().map_err(|error| {
        RecoveryError::CommandFailed {
            program: program.to_path_buf(),
            message: error.to_string(),
        }
    })?;
    if !output.status.success() {
        return Err(RecoveryError::CommandFailed {
            program: program.to_path_buf(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), RecoveryError> {
    let parent = path.parent().ok_or_else(|| RecoveryError::Io {
        operation: "resolving JSON parent directory",
        message: path.display().to_string(),
    })?;
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".{}.{}-{counter}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state"),
        std::process::id()
    ));
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| RecoveryError::Io {
        operation: "serializing JSON state",
        message: error.to_string(),
    })?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)
        .map_err(|error| RecoveryError::Io {
            operation: "creating temporary JSON state",
            message: error.to_string(),
        })?;
    file.write_all(&bytes).map_err(|error| RecoveryError::Io {
        operation: "writing JSON state",
        message: error.to_string(),
    })?;
    file.write_all(b"\n").map_err(|error| RecoveryError::Io {
        operation: "terminating JSON state",
        message: error.to_string(),
    })?;
    file.sync_all().map_err(|error| RecoveryError::Io {
        operation: "syncing JSON state",
        message: error.to_string(),
    })?;
    fs::rename(&temp, path).map_err(|error| RecoveryError::Io {
        operation: "installing JSON state",
        message: error.to_string(),
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| RecoveryError::Io {
            operation: "syncing JSON parent directory",
            message: error.to_string(),
        })
}

fn sha256_file(path: &Path) -> Result<String, RecoveryError> {
    let mut file = File::open(path).map_err(|error| RecoveryError::Io {
        operation: "opening artifact for checksum",
        message: error.to_string(),
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| RecoveryError::Io {
            operation: "reading artifact for checksum",
            message: error.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_log_tail(path: &Path) -> String {
    let Ok(mut file) = File::open(path) else {
        return "command failed without a readable log".to_owned();
    };
    let length = file.metadata().map_or(0, |metadata| metadata.len());
    let offset = length.saturating_sub(LOG_TAIL_BYTES);
    let _ = file.seek(SeekFrom::Start(offset));
    let mut bytes = Vec::new();
    let _ = file.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).trim().to_owned()
}

fn remove_file_if_exists(path: &Path) -> Result<(), RecoveryError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RecoveryError::Io {
            operation: "removing stale PostgreSQL file",
            message: format!("{}: {error}", path.display()),
        }),
    }
}

fn remove_dir_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn unix_seconds() -> Result<u64, RecoveryError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| RecoveryError::Io {
            operation: "reading system time",
            message: error.to_string(),
        })
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{guc_quote, pg_dump_table_pattern, shell_quote, sql_literal};

    #[test]
    fn quotes_shell_and_postgresql_config_values() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
        assert_eq!(guc_quote("a'b"), "'a''b'");
    }

    #[test]
    fn quotes_pg_dump_identifier_pattern() {
        assert_eq!(
            pg_dump_table_pattern("odd schema", "we\"ird"),
            "\"odd schema\".\"we\"\"ird\""
        );
        assert_eq!(sql_literal("a'b"), "'a''b'");
    }
}

//! Durable staging writer for the external_zstd SnapshotStore backend.
//!
//! The copy transaction writes only below `.staging/<operation_nonce>`.
//! A staged artifact is not discoverable as a restorable payload until a
//! later finalizer publishes the whole directory with one atomic rename and
//! commits the SnapshotStore `creating -> available` transition.

use crate::storage::external_zstd::{
    fsync_fd, mkdir_beneath, open_beneath, open_dir_beneath, open_root_dir, rename_beneath,
    unlink_beneath, validate_component, DIR_MODE, FILE_MODE,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

pub const ARTIFACT_TMP: &str = "artifact.zst.tmp";
// Shared by finalization, restore, health, and retirement paths so the
// immutable on-disk contract cannot drift between lifecycle phases.
pub const ARTIFACT_FILE: &str = "artifact.zst";
pub const PROVISIONAL_PENDING: &str = "provisional.pending";
pub const PROVISIONAL_FILE: &str = "provisional.json";
pub const LEASE_FILE: &str = "lease";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalArtifactState {
    Absent,
    StagingActive,
    StagingIncomplete,
    StagingCommitted,
    Published,
}

impl ExternalArtifactState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::StagingActive => "staging_active",
            Self::StagingIncomplete => "staging_incomplete",
            Self::StagingCommitted => "staging_committed",
            Self::Published => "published",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProvisionalManifest {
    pub operation_nonce: u64,
    pub database_oid: u32,
    pub tracking_id: i64,
    pub generation_id: i64,
    pub snapshot_id: i64,
    pub format_version: u32,
    pub codec: String,
    pub row_count: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalManifest {
    pub format_version: u32,
    pub codec: String,
    pub system_identifier: String,
    pub database_oid: u32,
    pub tracking_id: i64,
    pub snapshot_id: i64,
    pub boundary_lsn: String,
    pub schema_def_sha256: String,
    pub column_contract: serde_json::Value,
    pub row_count: u64,
    pub external_uncompressed_bytes: u64,
    pub external_compressed_bytes: u64,
    pub external_checksum_sha256: String,
    pub pg_major: u32,
}

pub struct FinalizationInput {
    pub system_identifier: u64,
    pub database_oid: u32,
    pub tracking_id: i64,
    pub generation_id: i64,
    pub snapshot_id: i64,
    pub operation_nonce: u64,
    pub boundary_lsn: String,
    pub schema_def_sha256: String,
    pub column_contract: serde_json::Value,
    pub pg_major: u32,
}

pub struct CountingHashWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W> CountingHashWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (W, u64, String) {
        (
            self.inner,
            self.bytes,
            format!("{:x}", self.hasher.finalize()),
        )
    }
}

impl<W: Write> Write for CountingHashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes = self.bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn ensure_directory(parent: &OwnedFd, name: &str) -> io::Result<OwnedFd> {
    validate_component(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    match mkdir_beneath(parent.as_raw_fd(), name, DIR_MODE) {
        Ok(()) => fsync_fd(parent.as_raw_fd())?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    open_dir_beneath(parent.as_raw_fd(), name)
}

fn create_file(parent: &OwnedFd, name: &str) -> io::Result<File> {
    open_beneath(
        parent.as_raw_fd(),
        name,
        libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC,
        FILE_MODE,
    )
}

fn open_readonly(parent: &OwnedFd, name: &str) -> io::Result<File> {
    open_beneath(
        parent.as_raw_fd(),
        name,
        libc::O_RDONLY | libc::O_CLOEXEC,
        0,
    )
}

fn open_optional_dir(parent: &OwnedFd, name: &str) -> Result<Option<OwnedFd>, String> {
    match open_dir_beneath(parent.as_raw_fd(), name) {
        Ok(fd) => Ok(Some(fd)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("open directory {name}: {error}")),
    }
}

/// Inspect one exact DB-reserved artifact identity without following links or
/// trusting directory mtimes. The staging lease distinguishes a live copier
/// from a crashed/incomplete writer; the immutable final directory wins over
/// any stale staging observation because finalization publishes by rename.
pub fn probe_external_artifact_state(
    root: &Path,
    system_identifier: u64,
    database_oid: u32,
    tracking_id: i64,
    snapshot_id: i64,
    operation_nonce: u64,
) -> Result<ExternalArtifactState, String> {
    let components = [
        system_identifier.to_string(),
        database_oid.to_string(),
        tracking_id.to_string(),
        format!("{snapshot_id}-{operation_nonce}"),
        operation_nonce.to_string(),
    ];
    for component in &components {
        validate_component(component)?;
    }

    let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
    let Some(system_fd) = open_optional_dir(&root_fd, &components[0])? else {
        return Ok(ExternalArtifactState::Absent);
    };
    let Some(database_fd) = open_optional_dir(&system_fd, &components[1])? else {
        return Ok(ExternalArtifactState::Absent);
    };

    if let Some(tracking_fd) = open_optional_dir(&database_fd, &components[2])? {
        if open_optional_dir(&tracking_fd, &components[3])?.is_some() {
            return Ok(ExternalArtifactState::Published);
        }
    }

    let Some(staging_parent_fd) = open_optional_dir(&database_fd, ".staging")? else {
        return Ok(ExternalArtifactState::Absent);
    };
    let Some(staging_fd) = open_optional_dir(&staging_parent_fd, &components[4])? else {
        return Ok(ExternalArtifactState::Absent);
    };

    if let Ok(lease) = open_beneath(
        staging_fd.as_raw_fd(),
        LEASE_FILE,
        libc::O_RDWR | libc::O_CLOEXEC,
        0,
    ) {
        let rc = unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(ExternalArtifactState::StagingActive);
            }
            return Err(format!("lock staging lease for probe: {error}"));
        }
        let _ = unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_UN) };
    }

    if open_readonly(&staging_fd, PROVISIONAL_FILE).is_ok() {
        return Ok(ExternalArtifactState::StagingCommitted);
    }
    Ok(ExternalArtifactState::StagingIncomplete)
}

/// Remove only the exact staging directory associated with a durably-aborted
/// DB reservation. A live copier's lease makes this fail closed. Unexpected
/// files make the final rmdir fail instead of broadening the deletion set.
pub fn purge_staged_artifact(
    root: &Path,
    system_identifier: u64,
    database_oid: u32,
    operation_nonce: u64,
) -> Result<bool, String> {
    let components = [
        system_identifier.to_string(),
        database_oid.to_string(),
        operation_nonce.to_string(),
    ];
    for component in &components {
        validate_component(component)?;
    }
    let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
    let Some(system_fd) = open_optional_dir(&root_fd, &components[0])? else {
        return Ok(false);
    };
    let Some(database_fd) = open_optional_dir(&system_fd, &components[1])? else {
        return Ok(false);
    };
    let Some(staging_parent_fd) = open_optional_dir(&database_fd, ".staging")? else {
        return Ok(false);
    };
    let Some(staging_fd) = open_optional_dir(&staging_parent_fd, &components[2])? else {
        return Ok(false);
    };

    let lease = match open_beneath(
        staging_fd.as_raw_fd(),
        LEASE_FILE,
        libc::O_RDWR | libc::O_CLOEXEC,
        0,
    ) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("open staging lease for purge: {error}")),
    };
    if let Some(lease) = lease.as_ref() {
        let rc = unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(format!(
                "staging artifact is still owned by a copier: {}",
                io::Error::last_os_error()
            ));
        }
    }
    for name in [
        ARTIFACT_TMP,
        ARTIFACT_FILE,
        "manifest.json",
        PROVISIONAL_PENDING,
        PROVISIONAL_FILE,
        LEASE_FILE,
    ] {
        match unlink_beneath(staging_fd.as_raw_fd(), name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("remove staged {name}: {error}")),
        }
    }
    fsync_fd(staging_fd.as_raw_fd()).map_err(|e| format!("fsync purged staging directory: {e}"))?;
    drop(lease);
    drop(staging_fd);
    crate::storage::external_zstd::rmdir_beneath(staging_parent_fd.as_raw_fd(), &components[2])
        .map_err(|e| format!("remove staging artifact directory: {e}"))?;
    fsync_fd(staging_parent_fd.as_raw_fd())
        .map_err(|e| format!("fsync staging parent after purge: {e}"))?;
    Ok(true)
}

fn read_json<T: for<'de> Deserialize<'de>>(parent: &OwnedFd, name: &str) -> Result<T, String> {
    let file = open_readonly(parent, name).map_err(|e| format!("open {name}: {e}"))?;
    serde_json::from_reader(file).map_err(|e| format!("parse {name}: {e}"))
}

fn read_json_optional<T: for<'de> Deserialize<'de>>(
    parent: &OwnedFd,
    name: &str,
) -> Result<Option<T>, String> {
    let file = match open_readonly(parent, name) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("open {name}: {error}")),
    };
    serde_json::from_reader(file)
        .map(Some)
        .map_err(|e| format!("parse {name}: {e}"))
}

#[derive(Debug)]
enum ExclusiveJsonWriteError {
    AlreadyExists,
    Failed(String),
}

fn write_json_exclusive<T: Serialize>(
    parent: &OwnedFd,
    name: &str,
    value: &T,
) -> Result<(), ExclusiveJsonWriteError> {
    let mut file = match create_file(parent, name) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(ExclusiveJsonWriteError::AlreadyExists)
        }
        Err(error) => {
            return Err(ExclusiveJsonWriteError::Failed(format!(
                "create {name}: {error}"
            )))
        }
    };
    serde_json::to_writer(&mut file, value)
        .map_err(|e| ExclusiveJsonWriteError::Failed(format!("serialize {name}: {e}")))?;
    file.write_all(b"\n")
        .map_err(|e| ExclusiveJsonWriteError::Failed(format!("terminate {name}: {e}")))?;
    file.sync_all()
        .map_err(|e| ExclusiveJsonWriteError::Failed(format!("fsync {name}: {e}")))
}

pub struct ArtifactStream {
    staging_parent_fd: Option<OwnedFd>,
    staging_fd: Option<OwnedFd>,
    staging_name: String,
    lease: Option<File>,
    encoder: Option<CountingHashWriter<zstd::stream::write::Encoder<'static, File>>>,
    armed: bool,
}

impl ArtifactStream {
    pub fn create(
        root: &Path,
        system_identifier: u64,
        database_oid: u32,
        operation_nonce: u64,
        zstd_level: i32,
    ) -> Result<Self, String> {
        let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
        let system_fd = ensure_directory(&root_fd, &system_identifier.to_string())
            .map_err(|e| format!("create/open system artifact directory: {e}"))?;
        let database_fd = ensure_directory(&system_fd, &database_oid.to_string())
            .map_err(|e| format!("create/open database artifact directory: {e}"))?;
        let staging_parent_fd = ensure_directory(&database_fd, ".staging")
            .map_err(|e| format!("create/open staging parent: {e}"))?;
        let staging_name = operation_nonce.to_string();
        validate_component(&staging_name)?;
        mkdir_beneath(staging_parent_fd.as_raw_fd(), &staging_name, DIR_MODE)
            .map_err(|e| format!("create exclusive staging directory: {e}"))?;
        fsync_fd(staging_parent_fd.as_raw_fd())
            .map_err(|e| format!("fsync staging parent after create: {e}"))?;
        let staging_fd = open_dir_beneath(staging_parent_fd.as_raw_fd(), &staging_name)
            .map_err(|e| format!("open staging directory: {e}"))?;

        let lease = create_file(&staging_fd, LEASE_FILE)
            .map_err(|e| format!("create staging lease: {e}"))?;
        let flock_rc = unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if flock_rc != 0 {
            return Err(format!(
                "lock staging lease: {}",
                io::Error::last_os_error()
            ));
        }
        let artifact = create_file(&staging_fd, ARTIFACT_TMP)
            .map_err(|e| format!("create staged artifact: {e}"))?;
        let encoder = zstd::stream::write::Encoder::new(artifact, zstd_level)
            .map_err(|e| format!("create zstd encoder: {e}"))?;

        Ok(Self {
            staging_parent_fd: Some(staging_parent_fd),
            staging_fd: Some(staging_fd),
            staging_name,
            lease: Some(lease),
            encoder: Some(CountingHashWriter::new(encoder)),
            armed: true,
        })
    }

    pub fn writer(&mut self) -> Result<&mut impl Write, String> {
        self.encoder
            .as_mut()
            .ok_or_else(|| "artifact encoder is already finished".to_string())
    }

    pub fn finish_copy(
        mut self,
        mut provisional: ProvisionalManifest,
    ) -> Result<PendingArtifact, String> {
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| "artifact encoder is already finished".to_string())?;
        let (encoder, uncompressed_bytes, checksum_sha256) = encoder.finish();
        let artifact = encoder
            .finish()
            .map_err(|e| format!("finish zstd stream: {e}"))?;
        artifact
            .sync_all()
            .map_err(|e| format!("fsync staged artifact: {e}"))?;
        provisional.uncompressed_bytes = uncompressed_bytes;
        provisional.compressed_bytes = artifact
            .metadata()
            .map_err(|e| format!("stat staged artifact: {e}"))?
            .len();
        provisional.checksum_sha256 = checksum_sha256;

        let staging_fd = self
            .staging_fd
            .as_ref()
            .ok_or_else(|| "staging directory is no longer available".to_string())?;
        let mut provisional_file = create_file(staging_fd, PROVISIONAL_PENDING)
            .map_err(|e| format!("create provisional metadata: {e}"))?;
        serde_json::to_writer(&mut provisional_file, &provisional)
            .map_err(|e| format!("serialize provisional metadata: {e}"))?;
        provisional_file
            .write_all(b"\n")
            .map_err(|e| format!("terminate provisional metadata: {e}"))?;
        provisional_file
            .sync_all()
            .map_err(|e| format!("fsync provisional metadata: {e}"))?;
        fsync_fd(staging_fd.as_raw_fd()).map_err(|e| format!("fsync staging directory: {e}"))?;

        self.armed = false;
        Ok(PendingArtifact {
            staging_parent_fd: self.staging_parent_fd.take(),
            staging_fd: self.staging_fd.take(),
            staging_name: self.staging_name.clone(),
            lease: self.lease.take(),
            provisional,
            armed: true,
        })
    }
}

impl Drop for ArtifactStream {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(staging_fd) = self.staging_fd.as_ref() {
            let _ = unlink_beneath(staging_fd.as_raw_fd(), ARTIFACT_TMP);
            let _ = unlink_beneath(staging_fd.as_raw_fd(), PROVISIONAL_PENDING);
            let _ = unlink_beneath(staging_fd.as_raw_fd(), LEASE_FILE);
        }
        if let Some(parent) = self.staging_parent_fd.as_ref() {
            let _ = crate::storage::external_zstd::rmdir_beneath(
                parent.as_raw_fd(),
                &self.staging_name,
            );
            let _ = fsync_fd(parent.as_raw_fd());
        }
    }
}

pub struct PendingArtifact {
    staging_parent_fd: Option<OwnedFd>,
    staging_fd: Option<OwnedFd>,
    staging_name: String,
    lease: Option<File>,
    pub provisional: ProvisionalManifest,
    armed: bool,
}

impl PendingArtifact {
    /// Publish the copy-transaction commit receipt. This runs only after
    /// PostgreSQL's `CommitTransactionCommand()` has returned successfully;
    /// a crash before then leaves only `provisional.pending`, never a false
    /// durable claim that C5 committed.
    pub fn mark_copy_committed(mut self) -> Result<ProvisionalManifest, String> {
        let staging_fd = self
            .staging_fd
            .as_ref()
            .ok_or_else(|| "staging directory is no longer available".to_string())?;
        let staging_parent_fd = self
            .staging_parent_fd
            .as_ref()
            .ok_or_else(|| "staging parent is no longer available".to_string())?;
        rename_beneath(
            staging_fd.as_raw_fd(),
            PROVISIONAL_PENDING,
            staging_fd.as_raw_fd(),
            PROVISIONAL_FILE,
        )
        .map_err(|e| format!("publish provisional commit receipt: {e}"))?;
        fsync_fd(staging_fd.as_raw_fd())
            .map_err(|e| format!("fsync committed provisional metadata: {e}"))?;
        fsync_fd(staging_parent_fd.as_raw_fd())
            .map_err(|e| format!("fsync staging parent after provisional publish: {e}"))?;
        if let Some(lease) = self.lease.as_ref() {
            let _ = unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_UN) };
        }
        self.armed = false;
        Ok(self.provisional.clone())
    }
}

impl Drop for PendingArtifact {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(staging_fd) = self.staging_fd.as_ref() {
            let _ = unlink_beneath(staging_fd.as_raw_fd(), ARTIFACT_TMP);
            let _ = unlink_beneath(staging_fd.as_raw_fd(), PROVISIONAL_PENDING);
            let _ = unlink_beneath(staging_fd.as_raw_fd(), PROVISIONAL_FILE);
            let _ = unlink_beneath(staging_fd.as_raw_fd(), LEASE_FILE);
        }
        if let Some(parent) = self.staging_parent_fd.as_ref() {
            let _ = crate::storage::external_zstd::rmdir_beneath(
                parent.as_raw_fd(),
                &self.staging_name,
            );
            let _ = fsync_fd(parent.as_raw_fd());
        }
    }
}

pub struct FinalizedArtifact {
    final_dir_fd: OwnedFd,
    pub locator: serde_json::Value,
    pub provisional: ProvisionalManifest,
    pub manifest: FinalManifest,
}

/// Open an immutable published payload entirely through fd-relative,
/// no-symlink traversal and return its manifest plus compressed stream.
pub fn open_published_artifact(
    root: &Path,
    system_identifier: u64,
    database_oid: u32,
    tracking_id: i64,
    snapshot_id: i64,
    operation_nonce: u64,
) -> Result<(File, FinalManifest), String> {
    let components = [
        system_identifier.to_string(),
        database_oid.to_string(),
        tracking_id.to_string(),
        format!("{snapshot_id}-{operation_nonce}"),
    ];
    for component in &components {
        validate_component(component)?;
    }
    let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
    let system_fd = open_dir_beneath(root_fd.as_raw_fd(), &components[0])
        .map_err(|e| format!("open system artifact directory: {e}"))?;
    let database_fd = open_dir_beneath(system_fd.as_raw_fd(), &components[1])
        .map_err(|e| format!("open database artifact directory: {e}"))?;
    let tracking_fd = open_dir_beneath(database_fd.as_raw_fd(), &components[2])
        .map_err(|e| format!("open tracking artifact directory: {e}"))?;
    let final_fd = open_dir_beneath(tracking_fd.as_raw_fd(), &components[3])
        .map_err(|e| format!("open published artifact directory: {e}"))?;
    let manifest = read_json(&final_fd, "manifest.json")?;
    let artifact = open_readonly(&final_fd, ARTIFACT_FILE)
        .map_err(|e| format!("open published artifact: {e}"))?;
    let lock_rc = unsafe { libc::flock(artifact.as_raw_fd(), libc::LOCK_SH) };
    if lock_rc != 0 {
        return Err(format!(
            "lock published artifact for reading: {}",
            io::Error::last_os_error()
        ));
    }
    Ok((artifact, manifest))
}

/// Delete one exact published payload after acquiring an exclusive advisory
/// lock on `artifact.zst`. A concurrent restore holds a shared lock on the
/// same inode, so retirement fails closed and is retried later instead of
/// unlinking a stream that is currently being read.
pub fn purge_published_artifact(
    root: &Path,
    system_identifier: u64,
    database_oid: u32,
    tracking_id: i64,
    snapshot_id: i64,
    operation_nonce: u64,
) -> Result<bool, String> {
    let components = [
        system_identifier.to_string(),
        database_oid.to_string(),
        tracking_id.to_string(),
        format!("{snapshot_id}-{operation_nonce}"),
    ];
    for component in &components {
        validate_component(component)?;
    }
    let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
    let system_fd = match open_dir_beneath(root_fd.as_raw_fd(), &components[0]) {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("open system artifact directory: {error}")),
    };
    let database_fd = match open_dir_beneath(system_fd.as_raw_fd(), &components[1]) {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("open database artifact directory: {error}")),
    };
    let tracking_fd = match open_dir_beneath(database_fd.as_raw_fd(), &components[2]) {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("open tracking artifact directory: {error}")),
    };
    let final_fd = match open_dir_beneath(tracking_fd.as_raw_fd(), &components[3]) {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("open published artifact directory: {error}")),
    };
    let artifact = match open_readonly(&final_fd, ARTIFACT_FILE) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("open published artifact for purge: {error}")),
    };
    if let Some(artifact) = artifact.as_ref() {
        let lock_rc = unsafe { libc::flock(artifact.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if lock_rc != 0 {
            return Err(format!(
                "published artifact is in use by a restore: {}",
                io::Error::last_os_error()
            ));
        }
    }
    for name in [
        ARTIFACT_FILE,
        "manifest.json",
        PROVISIONAL_FILE,
        PROVISIONAL_PENDING,
        LEASE_FILE,
    ] {
        match unlink_beneath(final_fd.as_raw_fd(), name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("remove published {name}: {error}")),
        }
    }
    fsync_fd(final_fd.as_raw_fd()).map_err(|e| format!("fsync purged artifact directory: {e}"))?;
    drop(artifact);
    drop(final_fd);
    crate::storage::external_zstd::rmdir_beneath(tracking_fd.as_raw_fd(), &components[3])
        .map_err(|e| format!("remove published artifact directory: {e}"))?;
    fsync_fd(tracking_fd.as_raw_fd())
        .map_err(|e| format!("fsync tracking directory after purge: {e}"))?;
    Ok(true)
}

impl FinalizedArtifact {
    /// Remove copy-only coordination files after the database publication
    /// authority accepted the manifest. `artifact.zst` and `manifest.json`
    /// remain the complete immutable payload.
    pub fn cleanup_coordination_files(&self) -> Result<(), String> {
        for name in [PROVISIONAL_FILE, LEASE_FILE] {
            match unlink_beneath(self.final_dir_fd.as_raw_fd(), name) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("remove finalized {name}: {error}")),
            }
        }
        fsync_fd(self.final_dir_fd.as_raw_fd())
            .map_err(|e| format!("fsync finalized artifact directory: {e}"))
    }
}

pub fn finalize_staged_artifact(
    root: &Path,
    input: FinalizationInput,
) -> Result<FinalizedArtifact, String> {
    let root_fd = open_root_dir(root).map_err(|e| e.to_string())?;
    let system_name = input.system_identifier.to_string();
    let database_name = input.database_oid.to_string();
    let staging_name = input.operation_nonce.to_string();
    let tracking_name = input.tracking_id.to_string();
    let final_name = format!("{}-{}", input.snapshot_id, input.operation_nonce);
    for component in [
        system_name.as_str(),
        database_name.as_str(),
        staging_name.as_str(),
        tracking_name.as_str(),
        final_name.as_str(),
    ] {
        validate_component(component)?;
    }

    let system_fd = open_dir_beneath(root_fd.as_raw_fd(), &system_name)
        .map_err(|e| format!("open system artifact directory: {e}"))?;
    let database_fd = open_dir_beneath(system_fd.as_raw_fd(), &database_name)
        .map_err(|e| format!("open database artifact directory: {e}"))?;
    let staging_parent_fd = open_dir_beneath(database_fd.as_raw_fd(), ".staging")
        .map_err(|e| format!("open staging parent: {e}"))?;
    let tracking_fd = ensure_directory(&database_fd, &tracking_name)
        .map_err(|e| format!("create/open tracking artifact directory: {e}"))?;

    let (final_dir_fd, provisional, manifest) =
        match open_dir_beneath(staging_parent_fd.as_raw_fd(), &staging_name) {
            Ok(staging_fd) => {
                let lease = open_beneath(
                    staging_fd.as_raw_fd(),
                    LEASE_FILE,
                    libc::O_RDWR | libc::O_CLOEXEC,
                    0,
                )
                .map_err(|e| format!("open staging lease: {e}"))?;
                let lock_rc =
                    unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if lock_rc != 0 {
                    return Err(format!(
                        "external snapshot copy is still active: {}",
                        io::Error::last_os_error()
                    ));
                }
                let provisional: ProvisionalManifest = read_json(&staging_fd, PROVISIONAL_FILE)?;
                validate_provisional(&provisional, &input)?;

                match rename_beneath(
                    staging_fd.as_raw_fd(),
                    ARTIFACT_TMP,
                    staging_fd.as_raw_fd(),
                    ARTIFACT_FILE,
                ) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        open_readonly(&staging_fd, ARTIFACT_FILE)
                            .map_err(|_| format!("staged artifact is missing: {error}"))?;
                    }
                    Err(error) => return Err(format!("seal staged artifact name: {error}")),
                }
                crate::storage::worker::trigger_external_snapshot_failpoint(
                    "finalizer_after_artifact_seal",
                );

                let manifest = build_final_manifest(&provisional, &input);
                match write_json_exclusive(&staging_fd, "manifest.json", &manifest) {
                    Ok(()) => {}
                    Err(ExclusiveJsonWriteError::AlreadyExists) => {
                        let existing: FinalManifest = read_json(&staging_fd, "manifest.json")?;
                        if existing != manifest {
                            return Err(
                                "existing staged manifest conflicts with finalization input"
                                    .to_string(),
                            );
                        }
                    }
                    Err(ExclusiveJsonWriteError::Failed(error)) => return Err(error),
                }
                fsync_fd(staging_fd.as_raw_fd())
                    .map_err(|e| format!("fsync staging before publish: {e}"))?;
                crate::storage::worker::trigger_external_snapshot_failpoint(
                    "finalizer_after_manifest_fsync",
                );
                rename_beneath(
                    staging_parent_fd.as_raw_fd(),
                    &staging_name,
                    tracking_fd.as_raw_fd(),
                    &final_name,
                )
                .map_err(|e| format!("publish staged artifact directory: {e}"))?;
                fsync_fd(staging_parent_fd.as_raw_fd())
                    .map_err(|e| format!("fsync staging parent after publish: {e}"))?;
                fsync_fd(tracking_fd.as_raw_fd())
                    .map_err(|e| format!("fsync tracking parent after publish: {e}"))?;
                crate::storage::worker::trigger_external_snapshot_failpoint(
                    "finalizer_after_publish_rename",
                );
                let final_dir_fd = open_dir_beneath(tracking_fd.as_raw_fd(), &final_name)
                    .map_err(|e| format!("open published artifact directory: {e}"))?;
                (final_dir_fd, provisional, manifest)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let final_dir_fd = open_dir_beneath(tracking_fd.as_raw_fd(), &final_name)
                    .map_err(|e| format!("neither staged nor published artifact exists: {e}"))?;
                let manifest: FinalManifest = read_json(&final_dir_fd, "manifest.json")?;
                let provisional: ProvisionalManifest =
                    match read_json_optional(&final_dir_fd, PROVISIONAL_FILE)? {
                        Some(value) => value,
                        None => ProvisionalManifest {
                            operation_nonce: input.operation_nonce,
                            database_oid: input.database_oid,
                            tracking_id: input.tracking_id,
                            generation_id: input.generation_id,
                            snapshot_id: input.snapshot_id,
                            format_version: manifest.format_version,
                            codec: manifest.codec.clone(),
                            row_count: manifest.row_count,
                            uncompressed_bytes: manifest.external_uncompressed_bytes,
                            compressed_bytes: manifest.external_compressed_bytes,
                            checksum_sha256: manifest.external_checksum_sha256.clone(),
                        },
                    };
                validate_provisional(&provisional, &input)?;
                let expected = build_final_manifest(&provisional, &input);
                if manifest != expected {
                    return Err("published manifest conflicts with database evidence".to_string());
                }
                (final_dir_fd, provisional, manifest)
            }
            Err(error) => return Err(format!("open staged artifact directory: {error}")),
        };

    let artifact = open_readonly(&final_dir_fd, ARTIFACT_FILE)
        .map_err(|e| format!("open published artifact: {e}"))?;
    let compressed_len = artifact
        .metadata()
        .map_err(|e| format!("stat published artifact: {e}"))?
        .len();
    if compressed_len != provisional.compressed_bytes {
        return Err(format!(
            "published artifact length {compressed_len} != provisional {}",
            provisional.compressed_bytes
        ));
    }

    let locator = serde_json::json!({
        "system_identifier": system_name,
        "database_oid": database_name,
        "tracking_id": tracking_name,
        "snapshot_id": input.snapshot_id.to_string(),
        "nonce": staging_name,
    });
    Ok(FinalizedArtifact {
        final_dir_fd,
        locator,
        provisional,
        manifest,
    })
}

fn validate_provisional(
    provisional: &ProvisionalManifest,
    input: &FinalizationInput,
) -> Result<(), String> {
    if provisional.operation_nonce != input.operation_nonce
        || provisional.database_oid != input.database_oid
        || provisional.tracking_id != input.tracking_id
        || provisional.generation_id != input.generation_id
        || provisional.snapshot_id != input.snapshot_id
        || provisional.format_version != crate::storage::external_zstd_format::FORMAT_VERSION
        || provisional.codec != "zstd"
        || provisional.checksum_sha256.len() != 64
    {
        return Err(
            "provisional metadata does not match immutable reservation identity".to_string(),
        );
    }
    Ok(())
}

fn build_final_manifest(
    provisional: &ProvisionalManifest,
    input: &FinalizationInput,
) -> FinalManifest {
    FinalManifest {
        format_version: provisional.format_version,
        codec: provisional.codec.clone(),
        system_identifier: input.system_identifier.to_string(),
        database_oid: input.database_oid,
        tracking_id: input.tracking_id,
        snapshot_id: input.snapshot_id,
        boundary_lsn: input.boundary_lsn.clone(),
        schema_def_sha256: input.schema_def_sha256.clone(),
        column_contract: input.column_contract.clone(),
        row_count: provisional.row_count,
        external_uncompressed_bytes: provisional.uncompressed_bytes,
        external_compressed_bytes: provisional.compressed_bytes,
        external_checksum_sha256: provisional.checksum_sha256.clone(),
        pg_major: input.pg_major,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn staged_copy_is_durable_only_after_commit_receipt() {
        let root = std::env::temp_dir().join(format!(
            "pgfb-artifact-{}-{}",
            std::process::id(),
            991_001_u64
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(DIR_MODE)).unwrap();

        let mut stream = ArtifactStream::create(&root, 10, 20, 30, 1).unwrap();
        stream.writer().unwrap().write_all(b"payload").unwrap();
        let pending = stream
            .finish_copy(ProvisionalManifest {
                operation_nonce: 30,
                database_oid: 20,
                tracking_id: 40,
                generation_id: 50,
                snapshot_id: 60,
                format_version: 1,
                codec: "zstd".to_string(),
                row_count: 1,
                uncompressed_bytes: 0,
                compressed_bytes: 0,
                checksum_sha256: String::new(),
            })
            .unwrap();
        let stage = root.join("10/20/.staging/30");
        assert!(stage.join(PROVISIONAL_PENDING).exists());
        assert!(!stage.join(PROVISIONAL_FILE).exists());
        let manifest = pending.mark_copy_committed().unwrap();
        assert_eq!(manifest.uncompressed_bytes, 7);
        assert!(manifest.compressed_bytes > 0);
        assert!(stage.join(PROVISIONAL_FILE).exists());

        let mut decoded = Vec::new();
        zstd::stream::read::Decoder::new(File::open(stage.join(ARTIFACT_TMP)).unwrap())
            .unwrap()
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, b"payload");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn artifact_probe_and_exact_staging_purge_are_fail_closed() {
        let root = std::env::temp_dir().join(format!(
            "pgfb-artifact-state-{}-{}",
            std::process::id(),
            991_002_u64
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(DIR_MODE)).unwrap();

        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::Absent
        );
        let mut stream = ArtifactStream::create(&root, 10, 20, 30, 1).unwrap();
        stream.writer().unwrap().write_all(b"payload").unwrap();
        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::StagingActive
        );
        assert!(purge_staged_artifact(&root, 10, 20, 30).is_err());

        let pending = stream
            .finish_copy(ProvisionalManifest {
                operation_nonce: 30,
                database_oid: 20,
                tracking_id: 40,
                generation_id: 50,
                snapshot_id: 60,
                format_version: 1,
                codec: "zstd".to_string(),
                row_count: 1,
                uncompressed_bytes: 0,
                compressed_bytes: 0,
                checksum_sha256: String::new(),
            })
            .unwrap();
        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::StagingActive
        );
        pending.mark_copy_committed().unwrap();
        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::StagingCommitted
        );

        std::fs::remove_file(root.join("10/20/.staging/30/provisional.json")).unwrap();
        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::StagingIncomplete
        );
        assert!(purge_staged_artifact(&root, 10, 20, 30).unwrap());
        assert_eq!(
            probe_external_artifact_state(&root, 10, 20, 40, 60, 30).unwrap(),
            ExternalArtifactState::Absent
        );
        assert!(!purge_staged_artifact(&root, 10, 20, 30).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }
}

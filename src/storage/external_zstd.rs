//! external_zstd SnapshotStore backend: security-boundary primitives
//! (Step 9, Stage 2 of the implementation plan).
//!
//! Every filesystem operation this backend performs on an artifact is
//! fd-relative from a directory descriptor held on the validated artifact
//! root -- never by reconstructing and opening an absolute path after a
//! separate check (that pattern is exactly the TOCTOU window this module
//! exists to close).
//!
//! Primary path-safety mechanism: `openat2(2)` with
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` (Linux 5.6+), which resolves and
//! opens a path in one atomic kernel operation -- there is no window between
//! "checked" and "used". Fallback (older kernel / `openat2` genuinely
//! unavailable, `ENOSYS`): a component-by-component `openat`+`O_NOFOLLOW`
//! walk from a held directory fd, never a re-opened absolute path after a
//! separate check. This narrows, but does not eliminate, every TOCTOU
//! window -- stated once here, plainly, and not claimed otherwise anywhere
//! else in this module or its callers.
//!
//! `external_snapshot_root` validation is split in two, because `_PG_init`
//! runs before any database connection exists and cannot use SPI:
//!   - [`validate_root_os_level`]: OS-only checks (absolute path, exists,
//!     real directory, not a symlink, mode 0700, owned by the running OS
//!     user). Safe to call from `_PG_init`.
//!   - [`validate_root_spi_level`]: requires SPI (outside `data_directory`,
//!     outside every current tablespace location). Called after a database
//!     worker connects, and again before every persist/restore/cleanup/
//!     finalize operation, since tablespaces can be added later.
//!
//! The extension never creates `external_snapshot_root` itself -- it must
//! already exist, provisioned deliberately by the DBA.
//!
//! Stage 2 of a staged implementation (see the Step 9 plan, §15): these
//! primitives are exercised directly by the `#[pg_test]`s below, but their
//! real production callers (the DSM/copier handoff, the persist/finalize
//! transactions) land in later stages. `#![allow(dead_code)]` is temporary
//! scaffolding for that gap, removed once Stage 4+ wires real callers in --
//! not a permanent suppression.
#![allow(dead_code)]

use pgrx::prelude::*;
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

/// Mode bits for the artifact root and every directory created under it.
pub const DIR_MODE: u32 = 0o700;
/// Mode bits for artifact/manifest/lease files.
pub const FILE_MODE: u32 = 0o600;

// ── Root validation ────────────────────────────────────────────────────

/// OS-level-only validation of `external_snapshot_root`, safe to call from
/// `_PG_init` (no database connection, no SPI). Does not check
/// data_directory/tablespace overlap -- see [`validate_root_spi_level`].
pub fn validate_root_os_level(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("pg_flashback.external_snapshot_root is not set".to_string());
    }
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} must be an absolute path"
        ));
    }
    // symlink_metadata, not metadata: the root itself must not be a symlink,
    // and a symlinked root must be rejected before anything follows it.
    let meta = std::fs::symlink_metadata(p)
        .map_err(|e| format!("pg_flashback.external_snapshot_root {path:?}: {e}"))?;
    if meta.file_type().is_symlink() {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} must not be a symlink"
        ));
    }
    if !meta.is_dir() {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} must be a directory"
        ));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != DIR_MODE {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} must be mode {DIR_MODE:04o}, found {mode:04o}"
        ));
    }
    let running_uid = unsafe { libc::getuid() };
    if meta.uid() != running_uid {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} must be owned by the running PostgreSQL OS user (uid {running_uid}), found uid {}",
            meta.uid()
        ));
    }
    Ok(())
}

/// SPI-dependent validation of `external_snapshot_root`: must resolve
/// outside `data_directory` and outside every current tablespace location.
/// Must be called from inside an already-connected backend (after
/// `BackgroundWorker::connect_worker_to_spi` or an ordinary SQL call), and
/// again immediately before every persist/restore/cleanup/finalize
/// operation -- not cached, since a tablespace can be added later.
pub fn validate_root_spi_level(path: &str) -> Result<(), String> {
    let root_canon = std::fs::canonicalize(path)
        .map_err(|e| format!("pg_flashback.external_snapshot_root {path:?}: {e}"))?;

    let data_directory: Option<String> = Spi::get_one("SHOW data_directory")
        .map_err(|e| format!("cannot read data_directory: {e}"))?;
    let data_directory = data_directory.ok_or_else(|| "data_directory is unset".to_string())?;
    let data_directory_canon = std::fs::canonicalize(&data_directory)
        .map_err(|e| format!("cannot canonicalize data_directory {data_directory:?}: {e}"))?;
    if root_canon == data_directory_canon || root_canon.starts_with(&data_directory_canon) {
        return Err(format!(
            "pg_flashback.external_snapshot_root {path:?} resolves inside data_directory {data_directory:?} -- must be outside PGDATA"
        ));
    }

    let tablespace_paths: Result<Vec<String>, String> = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT pg_catalog.pg_tablespace_location(oid) AS loc \
                 FROM pg_catalog.pg_tablespace \
                 WHERE pg_catalog.pg_tablespace_location(oid) <> ''",
                None,
                &[],
            )
            .map_err(|e| format!("cannot list tablespace locations: {e}"))?;
        let mut paths = Vec::new();
        for row in table {
            if let Ok(Some(loc)) = row.get::<String>(1) {
                paths.push(loc);
            }
        }
        Ok(paths)
    });
    for ts_path in tablespace_paths? {
        let ts_canon = match std::fs::canonicalize(&ts_path) {
            Ok(c) => c,
            Err(_) => continue, // a tablespace whose directory can't be resolved can't overlap
        };
        if root_canon == ts_canon || root_canon.starts_with(&ts_canon) {
            return Err(format!(
                "pg_flashback.external_snapshot_root {path:?} resolves inside tablespace location {ts_path:?} -- must be outside every tablespace"
            ));
        }
    }
    Ok(())
}

// ── Path-component safety ──────────────────────────────────────────────

/// A single path component (directory or file name) valid for use under the
/// external artifact root: operation nonces, `tracking_id`/`snapshot_id`
/// text, and fixed filenames (`artifact.zst`, `manifest.json`, ...) all pass
/// this. Every value checked here is server-generated, never caller-
/// supplied SQL text -- this check runs anyway as defense in depth against
/// path traversal (`..`), absolute-path injection (embedded `/`), and
/// anything else that isn't a plain identifier.
pub fn validate_component(component: &str) -> Result<(), String> {
    if component.is_empty() {
        return Err("path component must not be empty".to_string());
    }
    if component == "." || component == ".." {
        return Err(format!("path component {component:?} is not allowed"));
    }
    if !component
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
    {
        return Err(format!(
            "path component {component:?} contains a character outside [A-Za-z0-9_.-]"
        ));
    }
    Ok(())
}

fn component_to_cstring(name: &str) -> io::Result<CString> {
    validate_component(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))
}

// ── fd-relative primitives ─────────────────────────────────────────────

/// Open a directory fd for the (OS-level-validated) artifact root itself.
/// `O_NOFOLLOW` here rejects a root that is secretly a symlink, mirroring
/// [`validate_root_os_level`]'s own check but enforced again at the actual
/// point of use.
pub fn open_root_dir(root: &Path) -> io::Result<OwnedFd> {
    let c_path = CString::new(root.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "root path contains NUL"))?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn openat2_beneath(dir_fd: RawFd, name: &CString, oflags: i32, mode: u32) -> io::Result<RawFd> {
    // libc::open_how is #[non_exhaustive] with no Default impl, so it cannot
    // be built with struct-literal syntax from outside the crate. It is a
    // plain repr(C) POD (three u64 fields), so zero-then-assign is the
    // standard, safe-in-effect construction for this kind of kernel ABI
    // struct.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = oflags as u64;
    how.mode = mode as u64;
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS;
    let ret = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            dir_fd,
            name.as_ptr(),
            &how as *const libc::open_how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ret as RawFd)
}

fn openat_nofollow_fallback(
    dir_fd: RawFd,
    name: &CString,
    oflags: i32,
    mode: u32,
) -> io::Result<RawFd> {
    // Fallback path: a single openat() call with O_NOFOLLOW from an
    // already-held directory fd. This still refuses to follow a symlink at
    // `name` itself, and never re-derives an absolute path from a separate
    // check -- but unlike openat2(RESOLVE_BENEATH), it does not resolve
    // and open atomically, so it narrows rather than eliminates every
    // TOCTOU window. See the module-level doc comment.
    let ret = unsafe { libc::openat(dir_fd, name.as_ptr(), oflags | libc::O_NOFOLLOW, mode) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ret)
}

/// Open (or create) `name` directly beneath the directory `dir_fd` refers
/// to. `name` must be a single validated path component (no `/`), never a
/// multi-segment path -- every intermediate directory in an artifact's full
/// path is opened as its own separate fd-relative step by the caller, so
/// no single call here ever resolves more than one path segment.
pub fn open_beneath(dir_fd: RawFd, name: &str, oflags: i32, mode: u32) -> io::Result<File> {
    let c_name = component_to_cstring(name)?;
    #[cfg(target_os = "linux")]
    {
        match openat2_beneath(dir_fd, &c_name, oflags, mode) {
            Ok(fd) => return Ok(unsafe { File::from_raw_fd(fd) }),
            Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => {
                // openat2 unavailable on this kernel; fall through.
            }
            Err(e) => return Err(e),
        }
    }
    let fd = openat_nofollow_fallback(dir_fd, &c_name, oflags, mode)?;
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Create a directory named `name` directly beneath `dir_fd`, mode
/// [`DIR_MODE`] unless overridden.
pub fn mkdir_beneath(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<()> {
    let c_name = component_to_cstring(name)?;
    let ret = unsafe { libc::mkdirat(dir_fd, c_name.as_ptr(), mode) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Open an existing directory named `name` directly beneath `dir_fd` as a
/// new directory fd, for further fd-relative operations inside it.
pub fn open_dir_beneath(dir_fd: RawFd, name: &str) -> io::Result<OwnedFd> {
    let f = open_beneath(dir_fd, name, libc::O_DIRECTORY | libc::O_CLOEXEC, 0)?;
    Ok(OwnedFd::from(f))
}

/// Atomically rename `old_name` (directly beneath `old_dir_fd`) to
/// `new_name` (directly beneath `new_dir_fd`) -- used for the single
/// atomic directory-level publish (staging -> final location). Both names
/// are validated single path components.
pub fn rename_beneath(
    old_dir_fd: RawFd,
    old_name: &str,
    new_dir_fd: RawFd,
    new_name: &str,
) -> io::Result<()> {
    let c_old = component_to_cstring(old_name)?;
    let c_new = component_to_cstring(new_name)?;
    let ret = unsafe { libc::renameat(old_dir_fd, c_old.as_ptr(), new_dir_fd, c_new.as_ptr()) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Remove a file named `name` directly beneath `dir_fd`.
pub fn unlink_beneath(dir_fd: RawFd, name: &str) -> io::Result<()> {
    let c_name = component_to_cstring(name)?;
    let ret = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Remove an (empty) directory named `name` directly beneath `dir_fd`.
pub fn rmdir_beneath(dir_fd: RawFd, name: &str) -> io::Result<()> {
    let c_name = component_to_cstring(name)?;
    let ret = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), libc::AT_REMOVEDIR) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// fsync a directory fd (fsync-ing the directory *entry*, e.g. after a
/// create/rename/unlink beneath it -- fsync on the `File`/`OwnedFd` handle
/// itself is what this calls under the hood, which is exactly what's needed
/// for a directory's own durability, distinct from fsync-ing a regular
/// file's *contents*).
pub fn fsync_fd(fd: RawFd) -> io::Result<()> {
    let ret = unsafe { libc::fsync(fd) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    #[pg_test]
    fn test_validate_component_rejects_traversal() {
        assert!(validate_component("..").is_err());
        assert!(validate_component(".").is_err());
        assert!(validate_component("").is_err());
        assert!(validate_component("a/b").is_err());
        assert!(validate_component("../etc/passwd").is_err());
        assert!(validate_component("a b").is_err());
        assert!(validate_component("valid_name-123.zst").is_ok());
        assert!(validate_component("manifest.json").is_ok());
    }

    #[pg_test]
    fn test_open_beneath_rejects_symlink_target() {
        let dir = std::env::temp_dir().join(format!("fb-ext-zstd-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let real_target = dir.join("real_target");
        std::fs::write(&real_target, b"secret").unwrap();
        let link_path = dir.join("link_name");
        let _ = std::fs::remove_file(&link_path);
        std::os::unix::fs::symlink(&real_target, &link_path).unwrap();

        let dir_fd = open_root_dir(&dir).unwrap();
        let result = open_beneath(dir_fd.as_raw_fd(), "link_name", libc::O_RDONLY, 0);
        assert!(
            result.is_err(),
            "opening a symlink beneath the directory must be refused"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[pg_test]
    fn test_mkdir_open_rename_rmdir_roundtrip() {
        let dir = std::env::temp_dir().join(format!("fb-ext-zstd-test2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_fd = open_root_dir(&dir).unwrap();

        mkdir_beneath(dir_fd.as_raw_fd(), "staging_x", DIR_MODE).unwrap();
        let staging_fd = open_dir_beneath(dir_fd.as_raw_fd(), "staging_x").unwrap();

        let mut f = open_beneath(
            staging_fd.as_raw_fd(),
            "artifact.zst",
            libc::O_CREAT | libc::O_WRONLY | libc::O_EXCL,
            FILE_MODE,
        )
        .unwrap();
        f.write_all(b"hello").unwrap();
        drop(f);

        let meta = std::fs::metadata(dir.join("staging_x/artifact.zst")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, FILE_MODE);

        // Atomic directory-level rename onto a not-yet-existing target name
        // (the F7 publish step's exact shape: one directory rename, not two
        // independent file renames).
        rename_beneath(
            dir_fd.as_raw_fd(),
            "staging_x",
            dir_fd.as_raw_fd(),
            "final_x",
        )
        .unwrap();
        assert!(dir.join("final_x/artifact.zst").exists());
        assert!(!dir.join("staging_x").exists());

        // A second rename attempt onto an already-*non-empty* existing
        // directory must fail closed (renameat onto a non-empty directory
        // is refused by the kernel), not silently merge/overwrite.
        mkdir_beneath(dir_fd.as_raw_fd(), "staging_y", DIR_MODE).unwrap();
        let staging_y_fd = open_dir_beneath(dir_fd.as_raw_fd(), "staging_y").unwrap();
        open_beneath(
            staging_y_fd.as_raw_fd(),
            "artifact.zst",
            libc::O_CREAT | libc::O_WRONLY | libc::O_EXCL,
            FILE_MODE,
        )
        .unwrap();
        let err = rename_beneath(
            dir_fd.as_raw_fd(),
            "staging_y",
            dir_fd.as_raw_fd(),
            "final_x",
        )
        .unwrap_err();
        // Linux's rename(2) documents both ENOTEMPTY and EEXIST as valid
        // errnos for "destination is a non-empty directory" -- which one is
        // returned is filesystem/kernel-version dependent, so both are
        // accepted rather than asserting one specific value.
        let errno = err.raw_os_error();
        assert!(
            errno == Some(libc::ENOTEMPTY) || errno == Some(libc::EEXIST),
            "expected ENOTEMPTY or EEXIST for rename onto a non-empty directory, got {errno:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[pg_test]
    fn test_validate_root_os_level_rejects_bad_mode() {
        let dir = std::env::temp_dir().join(format!("fb-ext-zstd-test3-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = validate_root_os_level(dir.to_str().unwrap()).unwrap_err();
        assert!(
            err.contains("0700"),
            "error should name the required mode: {err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[pg_test]
    fn test_validate_root_os_level_accepts_good_root() {
        let dir = std::env::temp_dir().join(format!("fb-ext-zstd-test4-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(DIR_MODE as u32)).unwrap();
        assert!(validate_root_os_level(dir.to_str().unwrap()).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[pg_test]
    fn test_validate_root_os_level_rejects_symlink() {
        let dir = std::env::temp_dir().join(format!("fb-ext-zstd-test5-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(DIR_MODE as u32)).unwrap();
        let link =
            std::env::temp_dir().join(format!("fb-ext-zstd-test5-link-{}", std::process::id()));
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        let err = validate_root_os_level(link.to_str().unwrap()).unwrap_err();
        assert!(
            err.contains("symlink"),
            "error should mention symlink: {err}"
        );
        std::fs::remove_file(&link).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}

//! Local-profile filesystem capacity probes.
//!
//! Free-space inspection cannot be done portably from SQL alone. These helpers
//! resolve the on-disk destination for a relation or tablespace and report
//! `statvfs(2)` available bytes. Size/free-space checks are estimates and race
//! with concurrent writers; admission still fails closed when the estimate
//! cannot prove the configured budgets.

use pgrx::prelude::*;
use pgrx::PgRelation;
use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

/// Return available filesystem bytes for the directory that holds `rel`.
///
/// For the default tablespace this is under `data_directory`. For a non-default
/// tablespace it follows `pg_relation_filepath` into `pg_tblspc/...`.
#[pg_extern(stable, strict, name = "flashback_relation_filesystem_available_bytes")]
fn flashback_relation_filesystem_available_bytes(rel: PgRelation) -> i64 {
    let absolute = relation_absolute_path(&rel)
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"));
    available_bytes_for_path(&absolute)
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"))
}

/// Return available filesystem bytes for a tablespace OID.
///
/// OID 0 selects the database default tablespace via `data_directory`.
#[pg_extern(
    stable,
    strict,
    name = "flashback_tablespace_filesystem_available_bytes"
)]
fn flashback_tablespace_filesystem_available_bytes(tablespace_oid: pgrx::pg_sys::Oid) -> i64 {
    let absolute = tablespace_absolute_path(tablespace_oid)
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"));
    available_bytes_for_path(&absolute)
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"))
}

/// Return available bytes on the configured external SnapshotStore root.
/// Root ownership/mode/symlink validation is repeated here so capacity advice
/// can never make an unsafe path look admissible.
#[pg_extern(stable, name = "flashback_external_filesystem_available_bytes")]
fn flashback_external_filesystem_available_bytes() -> i64 {
    let root = crate::storage::worker::external_snapshot_root()
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"));
    crate::storage::external_zstd::validate_root_os_level(&root)
        .and_then(|_| crate::storage::external_zstd::validate_root_spi_level(&root))
        .unwrap_or_else(|message| {
            pgrx::error!("pg_flashback: unsafe external snapshot root: {message}")
        });
    available_bytes_for_path(Path::new(&root))
        .unwrap_or_else(|message| pgrx::error!("pg_flashback: {message}"))
}

fn available_bytes_for_path(path: &Path) -> Result<i64, String> {
    let probe = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| path.to_path_buf())
    };
    let available = fs2::available_space(&probe).map_err(|error| {
        format!(
            "cannot inspect filesystem free space for {}: {error}",
            probe.display()
        )
    })?;
    i64::try_from(available).map_err(|_| {
        format!(
            "filesystem free space for {} exceeds signed bigint range",
            probe.display()
        )
    })
}

fn data_directory() -> Result<PathBuf, String> {
    // SAFETY: PostgreSQL keeps DataDir as a NUL-terminated C string for the
    // lifetime of the backend after InitPostgres.
    let raw = unsafe { pg_sys::DataDir };
    if raw.is_null() {
        return Err("PostgreSQL DataDir is unavailable".to_owned());
    }
    let cstr = unsafe { CStr::from_ptr(raw) };
    let bytes = cstr.to_bytes();
    if bytes.is_empty() {
        return Err("PostgreSQL DataDir is empty".to_owned());
    }
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
}

fn relation_absolute_path(rel: &PgRelation) -> Result<PathBuf, String> {
    let oid = rel.oid().to_u32();
    let relative = Spi::get_one::<String>(&format!(
        "SELECT pg_catalog.pg_relation_filepath('{oid}'::pg_catalog.oid)"
    ))
    .map_err(|error| format!("cannot resolve relation filepath: {error}"))?
    .ok_or_else(|| "pg_relation_filepath returned NULL".to_owned())?;
    if relative.is_empty() {
        return Err("pg_relation_filepath returned an empty path".to_owned());
    }
    let relative_path = PathBuf::from(relative);
    if relative_path.is_absolute() {
        Ok(relative_path)
    } else {
        Ok(data_directory()?.join(relative_path))
    }
}

fn tablespace_absolute_path(tablespace_oid: pg_sys::Oid) -> Result<PathBuf, String> {
    // reltablespace 0 means "use the database default tablespace".
    if tablespace_oid == pg_sys::InvalidOid || tablespace_oid == pg_sys::DEFAULTTABLESPACE_OID {
        return data_directory();
    }
    let oid_u32 = tablespace_oid.to_u32();
    let location = Spi::get_one::<String>(&format!(
        "SELECT pg_catalog.pg_tablespace_location('{oid_u32}'::pg_catalog.oid)"
    ))
    .map_err(|error| format!("cannot resolve tablespace location: {error}"))?
    .unwrap_or_default();
    if location.is_empty() {
        // pg_global / unresolved location: fall back to the data directory.
        return data_directory();
    }
    let path = PathBuf::from(location);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(data_directory()?.join(path))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn filesystem_probe_reports_positive_for_pg_class() {
        let available = Spi::get_one::<i64>(
            "SELECT flashback_relation_filesystem_available_bytes('pg_catalog.pg_class'::regclass)",
        )
        .expect("spi")
        .expect("value");
        assert!(
            available > 0,
            "expected positive free space, got {available}"
        );
    }
}

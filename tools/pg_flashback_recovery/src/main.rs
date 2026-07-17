use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use pg_flashback_recovery::error::RecoveryError;
use pg_flashback_recovery::model::{
    BackupVerificationRequest, ErrorResponse, RecoveryConfig, RestoreRequest,
};
use pg_flashback_recovery::{
    load_json, load_recovery_config, provider_audit_anchors, provider_expire_backups,
    provider_plan, provider_probe, provider_reconcile_anchors, provider_restore_table,
    provider_verify_anchor, provider_verify_frontier, run_gc, unpin_artifact,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(name = "pg-flashback-recovery")]
#[command(version)]
#[command(about = "Backup-backed pg_flashback table recovery helper")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Probe binaries, repository access, `CoW` support and coordination.
    Probe {
        #[arg(long)]
        config: PathBuf,
    },
    /// Select a backup and emit a non-mutating recovery plan.
    Plan {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
    /// Recover, validate and extract one table into a custom-format dump.
    RestoreTable {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
    /// Garbage-collect abandoned work and expired or excess artifacts.
    Gc {
        #[arg(long)]
        config: PathBuf,
        /// Report decisions without deleting anything.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Release an awaiting-import artifact pin after successful import.
    Unpin {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request_id: String,
    },
    /// Verify a repository-derived FULL backup and activate its one-time anchor.
    VerifyAnchor {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
    /// Verify contiguous archived WAL and advance the active backup frontier.
    VerifyFrontier {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
    /// Run coordinated pgBackRest expiration after rejecting active backup pins.
    Expire {
        #[arg(long)]
        config: PathBuf,
    },
    /// Audit retained repository anchors and freeze missing/corrupt coverage.
    AuditAnchors {
        #[arg(long)]
        config: PathBuf,
    },
    /// Discover newer FULL backups and advance/retire anchors (never creates backups).
    ReconcileAnchors {
        #[arg(long)]
        config: PathBuf,
        /// Report proposed actions without mutating coverage or the repository.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(value) => match serde_json::to_string_pretty(&value) {
            Ok(json) => {
                println!("{json}");
                ExitCode::SUCCESS
            }
            Err(error) => emit_error(&RecoveryError::Io {
                operation: "serializing command output",
                message: error.to_string(),
            }),
        },
        Err(error) => emit_error(&error),
    }
}

fn run(cli: Cli) -> Result<serde_json::Value, RecoveryError> {
    match cli.command {
        Commands::Probe { config } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            to_value(provider_probe(&config)?)
        }
        Commands::Plan { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: RestoreRequest = load_json(&request)?;
            to_value(provider_plan(&config, &request)?)
        }
        Commands::RestoreTable { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: RestoreRequest = load_json(&request)?;
            to_value(provider_restore_table(&config, &request)?)
        }
        Commands::Gc { config, dry_run } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            to_value(run_gc(&config, dry_run)?)
        }
        Commands::Unpin { config, request_id } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            unpin_artifact(&config, &request_id)?;
            to_value(serde_json::json!({
                "status": "ok",
                "request_id": request_id,
                "pin_released": true,
            }))
        }
        Commands::VerifyAnchor { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: BackupVerificationRequest = load_json(&request)?;
            to_value(provider_verify_anchor(&config, &request)?)
        }
        Commands::VerifyFrontier { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: BackupVerificationRequest = load_json(&request)?;
            to_value(provider_verify_frontier(&config, &request)?)
        }
        Commands::Expire { config } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            to_value(provider_expire_backups(&config)?)
        }
        Commands::AuditAnchors { config } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            to_value(provider_audit_anchors(&config)?)
        }
        Commands::ReconcileAnchors { config, dry_run } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            to_value(provider_reconcile_anchors(&config, dry_run)?)
        }
    }
}

fn to_value<T: Serialize>(value: T) -> Result<serde_json::Value, RecoveryError> {
    serde_json::to_value(value).map_err(|error| RecoveryError::Io {
        operation: "serializing command output",
        message: error.to_string(),
    })
}

fn emit_error(error: &RecoveryError) -> ExitCode {
    let response = ErrorResponse {
        status: "error",
        code: error.code(),
        message: error.to_string(),
    };
    let json = serde_json::to_string(&response)
        .unwrap_or_else(|_| "{\"status\":\"error\",\"code\":\"serialization_failed\"}".to_owned());
    eprintln!("{json}");
    ExitCode::from(2)
}

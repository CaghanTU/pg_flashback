use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use pg_flashback_recovery::error::RecoveryError;
use pg_flashback_recovery::model::{ErrorResponse, RecoveryConfig, RestoreRequest};
use pg_flashback_recovery::{
    build_plan, load_json, load_recovery_config, restore_table, run_probe,
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
            to_value(run_probe(&config)?)
        }
        Commands::Plan { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: RestoreRequest = load_json(&request)?;
            to_value(build_plan(&config, &request)?)
        }
        Commands::RestoreTable { config, request } => {
            let config: RecoveryConfig = load_recovery_config(&config)?;
            let request: RestoreRequest = load_json(&request)?;
            to_value(restore_table(&config, &request)?)
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

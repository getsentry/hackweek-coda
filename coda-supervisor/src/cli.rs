use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::Error;
use clap::{Parser, Subcommand};
use tracing::metadata::LevelFilter;

use crate::config::Config;
use crate::controller::Controller;

#[derive(Parser, Debug)]
pub struct Cli {
    /// The command to run.
    #[command(subcommand)]
    command: Commands,
    /// Enable the tracing console
    #[arg(long, global = true)]
    tokio_console: bool,
    /// Sets the log level
    #[arg(long, global = true, default_value = "warn")]
    log_level: LevelFilter,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Runs a worker.
    Run(RunCommand),
}

#[derive(Parser, Debug)]
pub struct RunCommand {
    /// The number of workers to spawn.
    #[arg(short = 'n', long = "worker-count")]
    worker_count: Option<usize>,
    /// Path to the config file.
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,
    /// The command and it's arguments to execute as worker.
    #[arg(last = true)]
    args: Vec<OsString>,
}

async fn run(cmd: RunCommand) -> Result<(), Error> {
    let mut config = match cmd.config {
        Some(ref filename) => Config::from_path(filename)?,
        None => Config::default(),
    };
    if let Some(n) = cmd.worker_count {
        config.set_worker_count(n);
    }
    let mut controller = Controller::new(&cmd.args, config)?;
    controller.run().await?;
    Ok(())
}

pub async fn execute() -> Result<(), Error> {
    use tracing_subscriber::prelude::*;

    let cli = Cli::parse();

    tracing_subscriber::registry()
        .with(cli.tokio_console.then(console_subscriber::spawn))
        .with(tracing_subscriber::fmt::layer().with_filter(cli.log_level))
        .init();

    match cli.command {
        Commands::Run(cmd) => run(cmd).await,
    }
}

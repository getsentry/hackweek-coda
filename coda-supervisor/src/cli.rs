use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::Error;
use clap::{Parser, Subcommand};
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::config::Config;
use crate::controller::Controller;

#[derive(Parser, Debug)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Runs a worker.
    Run(RunCommand),
}

#[derive(Parser, Debug)]
pub struct RunCommand {
    /// The command and it's arguments to execute as worker.
    args: Vec<OsString>,
    /// The number of workers to spawn.
    #[arg(short = 'n', long = "worker-count")]
    worker_count: Option<usize>,
    /// Path to the config file.
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,
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
    tracing_subscriber::registry()
        .with(console_subscriber::spawn())
        .with(tracing_subscriber::fmt::layer().with_filter(LevelFilter::DEBUG))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Run(cmd) => run(cmd).await,
    }
}

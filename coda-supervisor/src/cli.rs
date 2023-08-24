use std::ffi::OsString;
use std::net::SocketAddr;
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
    /// Enables pretty logging
    #[arg(long, global = true)]
    pretty_log: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Runs a worker.
    Run(RunCommand),
}

#[derive(Parser, Debug)]
pub struct RunCommand {
    /// Overrides the listen address.
    #[arg(short = 'l', long)]
    listen_addr: Option<SocketAddr>,
    /// The number of workers to spawn.
    #[arg(short = 'n', long)]
    worker_count: Option<usize>,
    /// Path to the config file.
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,
    /// The command and it's arguments to execute as worker.
    #[arg(last = true, required = true)]
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
    if let Some(addr) = cmd.listen_addr {
        config.set_listen_addr(addr);
    }
    let mut controller = Controller::new(&cmd.args, config).await?;
    controller.run().await?;
    Ok(())
}

pub async fn execute() -> Result<(), Error> {
    use tracing_subscriber::prelude::*;

    let cli = Cli::parse();

    tracing_subscriber::registry()
        .with(cli.tokio_console.then(console_subscriber::spawn))
        .with(cli.pretty_log.then(|| {
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_filter(cli.log_level)
        }))
        .with(
            (!cli.pretty_log).then(|| tracing_subscriber::fmt::layer().with_filter(cli.log_level)),
        )
        .init();

    match cli.command {
        Commands::Run(cmd) => run(cmd).await,
    }
}

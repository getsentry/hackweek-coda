use std::ffi::OsString;

use anyhow::Error;
use clap::{Parser, Subcommand};
use tracing::metadata::LevelFilter;

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
    #[arg(short = 'n', long = "worker-count", default_value = "4")]
    worker_count: usize,
}

async fn run(cmd: RunCommand) -> Result<(), Error> {
    let mut controller = Controller::new(&cmd.args)?;
    controller.spawn_workers(cmd.worker_count).await?;

    controller.run_loop().await?;

    Ok(())
}

pub async fn execute() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(cmd) => run(cmd).await,
    }
}

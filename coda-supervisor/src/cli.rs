use std::path::PathBuf;
use std::process::Command;

use anyhow::Error;
use clap::{Parser, Subcommand};

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
    args: Vec<PathBuf>,
    /// The number of workers to spawn.
    #[arg(short = 'n', long = "worker-count", default_value = "4")]
    worker_count: usize,
}

async fn run(cmd: RunCommand) -> Result<(), Error> {
    let mut controller = Controller::new()?;
    let mut c = Command::new(&cmd.args[0]);
    c.args(&cmd.args[1..]);
    for _ in 0..cmd.worker_count {
        controller.spawn_worker(&mut c).await?;
    }
    Ok(())
}

pub async fn execute() -> Result<(), Error> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(cmd) => run(cmd).await,
    }
}

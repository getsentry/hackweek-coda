use std::collections::BTreeMap;
use std::net::SocketAddr;

use anyhow::{Context, Error};
use clap::{Parser, Subcommand};
use serde_json::Value;

#[derive(Parser, Debug)]
pub struct Cli {
    /// The command to run.
    #[command(subcommand)]
    command: Commands,
    /// Overrides the listen address.
    #[arg(short = 'i', long, global = true)]
    addr: Option<SocketAddr>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Spawn a workflow.
    SpawnWorkflow(SpawnWorkflowCommand),
}

#[derive(Parser, Debug)]
pub struct SpawnWorkflowCommand {
    /// The name of the task to run.
    #[arg(short, long)]
    name: String,
    /// The arguments to the workflow.
    #[arg(short, long = "arg")]
    args: Vec<String>,
}

pub async fn execute() -> Result<(), Error> {
    let cli = Cli::parse();
    match cli.command {
        Commands::SpawnWorkflow(cmd) => spawn_workflow(cmd).await,
    }
}

async fn spawn_workflow(cmd: SpawnWorkflowCommand) -> Result<(), Error> {
    let mut defines = BTreeMap::new();
    for item in &cmd.args {
        if let Some((key, raw_value)) = item.split_once(":=") {
            defines.insert(key, interpret_raw_value(raw_value)?);
        } else if let Some((key, string_value)) = item.split_once('=') {
            defines.insert(key, Value::from(string_value));
        } else {
            defines.insert(item, Value::from(true));
        }
    }

    dbg!(defines);

    Ok(())
}

fn interpret_raw_value(s: &str) -> Result<Value, Error> {
    serde_json::from_str::<Value>(s)
        .with_context(|| format!("invalid raw value '{}' (not valid JSON)", s))
}

#[tokio::main]
pub async fn main() {
    if let Err(err) = execute().await {
        eprintln!("error: {}", err);
        let mut source_opt = err.source();
        while let Some(source) = source_opt {
            eprintln!();
            eprintln!("caused by: {source}");
            source_opt = source.source();
        }
        std::process::exit(1);
    }
}

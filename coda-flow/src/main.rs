use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Error;
use tracing::metadata::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::concurrency::FlowMainLoop;
use crate::persistence::{Database, History};

mod concurrency;
mod core;
mod persistence;
mod transport;

// Base things to implement:
// * Main loop that receives incoming events from TCP (for now only TCP)
// * Internal in-memory representation of a supervisor, together with all the tasks/workflows supported
// * Basic representation of queues in the database
// * Basic representation of workflow/task state in the database

pub async fn execute() -> Result<(), Error> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 56019);
    let db = Arc::new(Database::connect("localhost", "5455", "coda", "coda").await?);
    let history = History::init(db.clone()).await;
    history.prepare_tables().await;
    let mut main_loop = FlowMainLoop::new(Some(addr)).await?;
    main_loop.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(console_subscriber::spawn())
        .with(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_filter(LevelFilter::DEBUG),
        )
        .init();

    match execute().await {
        Ok(_) => {}
        Err(error) => {
            println!("An error occurred {}", error)
        }
    }
}

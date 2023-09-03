use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Error;

use crate::concurrency::FlowMainLoop;

mod persistence;
mod transport;
mod core;
mod concurrency;

// Base things to implement:
// * Main loop that receives incoming events from TCP (for now only TCP)
// * Internal in-memory representation of a supervisor, together with all the tasks/workflows supported
// * Basic representation of queues in the database
// * Basic representation of workflow/task state in the database

pub async fn execute() -> Result<(), Error> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5890);
    let mut main_loop = FlowMainLoop::new(Some(addr)).await?;
    main_loop.run().await?;
    Ok(())
}


#[tokio::main]
async fn main() {
    match execute().await {
        Ok(_) => {}
        Err(error) => {
            println!("An error occurred {}", error)
        }
    }
}

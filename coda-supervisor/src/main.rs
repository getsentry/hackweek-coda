mod cli;
mod controller;

#[tokio::main]
async fn main() {
    // coda-supervisor run --workers 4 python foo.py
    // 1. start a socket for request/response IPC communication
    // 2. collect a list of all tasks and workflows per worker (also including the queues for the tasks/workflows)
    //    1. submits list of interests to the Flow Service
    //    2. Flow Service creates a queue specifically for the supervisor
    //    3. The queue is filled asynchronously by the Flow Service which is able to
    // 3.

    if let Err(err) = cli::execute().await {
        eprintln!("error: {}", err);
        std::process::exit(1);
    }
}

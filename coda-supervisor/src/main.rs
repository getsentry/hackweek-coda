mod cli;
mod config;
mod controller;
mod storage;

#[tokio::main]
async fn main() {
    if let Err(err) = cli::execute().await {
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

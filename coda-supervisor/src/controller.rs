use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::pipe;
use tokio::time;

use anyhow::Error;

pub struct Controller {
    workers: Vec<Worker>,
    home: TempDir,
}

struct Worker {
    child: Child,
    rx: pipe::Receiver,
    tx: pipe::Sender,
}

impl Controller {
    pub fn new() -> Result<Controller, Error> {
        Ok(Controller {
            workers: Vec::new(),
            home: tempfile::tempdir()?,
        })
    }

    pub async fn spawn_worker(&mut self, cmd: &mut Command) -> Result<(), Error> {
        let idx = self.workers.len();
        let rx_path = self.home.path().join(format!("rx-{}.pipe", idx));
        let tx_path = self.home.path().join(format!("tx-{}.pipe", idx));

        mkfifo(&rx_path, stat::Mode::S_IRWXU)?;
        mkfifo(&tx_path, stat::Mode::S_IRWXU)?;

        let rx = pipe::OpenOptions::new().open_receiver(&rx_path)?;

        cmd.env("CODA_WORKER_WRITE_PATH", &rx_path);
        cmd.env("CODA_WORKER_READ_PATH", &tx_path);
        let child = cmd.spawn()?;

        let tx = loop {
            match pipe::OpenOptions::new().open_sender(&tx_path) {
                Ok(tx) => break tx,
                Err(e) if e.raw_os_error() == Some(ENXIO) => {}
                Err(e) => return Err(e.into()),
            }
            time::sleep(Duration::from_millis(50)).await;
        };

        self.workers.push(Worker { child, rx, tx });
        Ok(())
    }
}

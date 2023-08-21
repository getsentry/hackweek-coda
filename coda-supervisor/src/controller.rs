use bytes::{BufMut, BytesMut};
use coda_ipc::{HelloWorker, Message, RequestWorkerShutdown};
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use serde::Serialize;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::pipe;
use tokio::sync::Mutex;
use tokio::time;
use uuid::Uuid;

use anyhow::Error;

pub struct Controller {
    workers: Vec<Worker>,
    home: TempDir,
}

struct WorkerComms {
    rx: pipe::Receiver,
    tx: pipe::Sender,
}

/// Represents a worker process.
pub struct Worker {
    worker_id: Uuid,
    child: Child,
    comms: Mutex<WorkerComms>,
}

impl Controller {
    /// Creates a fresh controller.
    pub fn new() -> Result<Controller, Error> {
        Ok(Controller {
            workers: Vec::new(),
            home: tempfile::tempdir()?,
        })
    }

    /// Spawns a single worker and returns the ID.
    pub async fn spawn_worker(&mut self, cmd: &mut Command) -> Result<Uuid, Error> {
        let worker_id = Uuid::new_v4();
        let rx_path = self.home.path().join(format!("rx-{}.pipe", worker_id));
        let tx_path = self.home.path().join(format!("tx-{}.pipe", worker_id));

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

        let worker = Worker {
            worker_id,
            child,
            comms: Mutex::new(WorkerComms { rx, tx }),
        };
        worker
            .send_msg(Message::HelloWorker(HelloWorker { worker_id }))
            .await?;
        self.workers.push(worker);
        Ok(worker_id)
    }

    /// Returns a worker with a specific ID.
    pub fn get_worker(&self, worker_id: Uuid) -> Option<&Worker> {
        self.workers
            .iter()
            .filter(|x| x.worker_id == worker_id)
            .next()
    }

    /// Iterates over all workers.
    pub fn iter_workers(&self) -> impl Iterator<Item = &Worker> {
        self.workers.iter()
    }
}

impl Worker {
    /// Sends a message into the worker.
    pub async fn send_msg<S: Serialize>(&self, msg: S) -> Result<(), Error> {
        let mut buf = Vec::<u8>::new();
        ciborium::into_writer(&msg, &mut buf).unwrap();
        let mut prefix = BytesMut::with_capacity(4);
        prefix.put_u32(buf.len() as u32);
        let mut comms = self.comms.lock().await;
        comms.tx.write_all(&prefix[..]).await?;
        comms.tx.write_all(&buf[..]).await?;
        Ok(())
    }

    /// Requests the worker to shut down.
    pub async fn request_shutdown(&self) -> Result<(), Error> {
        self.send_msg(Message::RequestWorkerShutdown(RequestWorkerShutdown {}))
            .await?;
        Ok(())
    }
}

use bytes::{Buf, BufMut, BytesMut};
use coda_ipc::{HelloWorker, Message, RequestWorkerShutdown};
use futures::future::{select_all, FutureExt};
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use std::collections::HashSet;
use std::mem;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::pipe;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{event, Level};
use uuid::Uuid;

use anyhow::Error;

pub struct Controller {
    workers: Vec<Worker>,
    home: TempDir,
}

#[derive(Default, Debug)]
struct WorkerState {
    tasks: HashSet<String>,
    workflows: HashSet<String>,
}

/// Represents a worker process.
pub struct Worker {
    worker_id: Uuid,
    child: Child,
    rx: Mutex<pipe::Receiver>,
    tx: Mutex<pipe::Sender>,
    state: Mutex<WorkerState>,
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

        event!(Level::INFO, "Worker {} spawning", worker_id);

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
            rx: Mutex::new(rx),
            tx: Mutex::new(tx),
            state: Mutex::new(WorkerState::default()),
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

    /// Runs the main communication loop.
    pub async fn run_loop(&mut self) -> Result<(), Error> {
        let mut worker_futures = self
            .workers
            .iter()
            .map(|x| x.read_msg().boxed())
            .collect::<Vec<_>>();
        loop {
            let (msg, ready_idx, mut remaining_worker_futures) =
                select_all(mem::take(&mut worker_futures)).await;
            self.handle_message(&self.workers[ready_idx], msg?).await?;
            remaining_worker_futures.push(self.workers[ready_idx].read_msg().boxed());
            worker_futures = remaining_worker_futures;
        }
        Ok(())
    }

    async fn handle_message(&self, worker: &Worker, msg: Message) -> Result<(), Error> {
        event!(Level::DEBUG, "worker message {:?}", msg);
        match msg {
            Message::Ping(_) => {
                println!("client sent a ping");
            }
            Message::WorkerStart(cmd) => {
                let mut state = worker.state.lock().await;
                state.tasks = cmd.tasks;
                state.workflows = cmd.workflows;
                event!(Level::DEBUG, "worker registered {:?}", state);
            }
            other => {
                event!(Level::WARN, "unhandled message {:?}", other);
            }
        }
        Ok(())
    }
}

impl Worker {
    /// Sends a message into the worker.
    pub async fn send_msg(&self, msg: Message) -> Result<(), Error> {
        let mut buf = Vec::<u8>::new();
        ciborium::into_writer(&msg, &mut buf).unwrap();
        let mut prefix = BytesMut::with_capacity(4);
        prefix.put_u32(buf.len() as u32);
        let mut tx = self.tx.lock().await;
        tx.write_all(&prefix[..]).await?;
        tx.write_all(&buf[..]).await?;
        Ok(())
    }

    /// Receives a single message.
    async fn read_msg(&self) -> Result<Message, Error> {
        let mut bytes = [0u8; 4];
        let mut rx = self.rx.lock().await;
        rx.read_exact(&mut bytes).await?;
        let len = (&bytes[..]).get_u32();
        let mut buf = vec![0; len as usize];
        rx.read_exact(&mut buf).await?;
        Ok(ciborium::from_reader(&buf[..])?)
    }

    /// Requests the worker to shut down.
    pub async fn request_shutdown(&self) -> Result<(), Error> {
        event!(Level::INFO, "Worker {} requesting shutdown", self.worker_id);
        self.send_msg(Message::RequestWorkerShutdown(RequestWorkerShutdown {}))
            .await?;
        Ok(())
    }
}

use bytes::{Buf, BufMut, BytesMut};
use coda_ipc::{HelloWorker, Message};
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::pipe;
use tokio::sync::mpsc::{self, unbounded_channel};
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{event, Level};
use uuid::Uuid;

use anyhow::{anyhow, Error};

pub struct Controller {
    command: Vec<OsString>,
    target_worker_count: usize,
    workers: Vec<Worker>,
    worker_tx: mpsc::UnboundedSender<(Uuid, Result<Message, Error>)>,
    worker_rx: mpsc::UnboundedReceiver<(Uuid, Result<Message, Error>)>,
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
    tx: Mutex<pipe::Sender>,
    child: Child,
    state: Mutex<WorkerState>,
}

impl Controller {
    /// Creates a fresh controller.
    pub fn new(cmd: &[OsString], target_worker_count: usize) -> Result<Controller, Error> {
        let (worker_tx, worker_rx) = unbounded_channel();
        Ok(Controller {
            command: cmd.iter().cloned().collect(),
            target_worker_count,
            workers: Vec::new(),
            worker_tx,
            worker_rx,
            home: tempfile::tempdir()?,
        })
    }

    /// Runs the controller.
    pub async fn run(&mut self) -> Result<(), Error> {
        self.spawn_workers().await?;
        self.event_loop().await?;
        Ok(())
    }

    /// Spawns a given number of workers.
    async fn spawn_workers(&mut self) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for _ in 0..self.target_worker_count {
            let mut cmd = Command::new(&self.command[0]);
            cmd.args(&self.command[1..]);
            let worker_tx = self.worker_tx.clone();
            let dir = self.home.path().to_path_buf();
            set.spawn(async move { spawn_worker(dir, cmd, worker_tx).await });
        }

        while let Some(worker) = set.join_next().await {
            self.workers.push(worker??);
        }

        for worker in &self.workers {
            self.send_msg(
                worker.worker_id,
                Message::HelloWorker(HelloWorker {
                    worker_id: worker.worker_id,
                }),
            )
            .await?;
        }

        Ok(())
    }

    /// Send a message to a worker.
    async fn send_msg(&self, worker_id: Uuid, msg: Message) -> Result<(), Error> {
        let worker = self
            .workers
            .iter()
            .find(|x| x.worker_id == worker_id)
            .ok_or_else(|| anyhow!("cannot find worker"))?;
        let mut tx = worker.tx.lock().await;
        send_msg(&mut tx, msg).await
    }

    /// Finds a worker by worker ID.
    fn get_worker(&self, worker_id: Uuid) -> Option<&Worker> {
        self.workers.iter().find(|x| x.worker_id == worker_id)
    }

    /// Removes a worker
    fn remove_worker(&mut self, worker_id: Uuid) -> Option<Worker> {
        let idx = self.workers.iter().position(|x| x.worker_id == worker_id)?;
        let mut worker = self.workers.remove(idx);
        worker.child.kill().ok();
        Some(worker)
    }

    /// Runs the main communication loop.
    async fn event_loop(&mut self) -> Result<(), Error> {
        while let Some((worker_id, rv)) = self.worker_rx.recv().await {
            match rv {
                Ok(msg) => {
                    if let Some(worker) = self.get_worker(worker_id) {
                        self.handle_message(worker, msg).await?;
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "worker errored: {}", err);
                    self.remove_worker(worker_id);
                }
            }
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

/// Spawns a worker that sends its messages into `worker_tx`.
async fn spawn_worker(
    dir: PathBuf,
    mut cmd: Command,
    worker_tx: mpsc::UnboundedSender<(Uuid, Result<Message, Error>)>,
) -> Result<Worker, Error> {
    let worker_id = Uuid::new_v4();
    let rx_path = dir.join(format!("rx-{}.pipe", worker_id));
    let tx_path = dir.join(format!("tx-{}.pipe", worker_id));

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
        tx: Mutex::new(tx),
        child,
        state: Mutex::new(WorkerState::default()),
    };
    let rx = Mutex::new(rx);

    let worker_tx = worker_tx.clone();
    tokio::spawn(async move {
        loop {
            let msg = read_msg(&mut rx.lock().await).await;
            let failed = msg.is_err();
            if worker_tx.send((worker_id, msg)).is_err() || failed {
                break;
            }
        }
    });

    Ok(worker)
}

async fn read_msg(rx: &mut MutexGuard<'_, pipe::Receiver>) -> Result<Message, Error> {
    let mut bytes = [0u8; 4];
    rx.read_exact(&mut bytes).await?;
    let len = (&bytes[..]).get_u32();
    let mut buf = vec![0; len as usize];
    rx.read_exact(&mut buf).await?;
    Ok(ciborium::from_reader(&buf[..])?)
}

async fn send_msg(tx: &mut MutexGuard<'_, pipe::Sender>, msg: Message) -> Result<(), Error> {
    let mut buf = Vec::<u8>::new();
    ciborium::into_writer(&msg, &mut buf).unwrap();
    let mut prefix = BytesMut::with_capacity(4);
    prefix.put_u32(buf.len() as u32);
    tx.write_all(&prefix[..]).await?;
    tx.write_all(&buf[..]).await?;
    Ok(())
}

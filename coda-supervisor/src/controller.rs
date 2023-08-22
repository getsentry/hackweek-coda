use std::collections::HashSet;
use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Error};
use bytes::{Buf, BufMut, BytesMut};
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::pipe;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinSet;
use tokio::{signal, time};
use tracing::{event, Level};
use uuid::Uuid;

use coda_ipc::{Fail, Message, Ping, WorkerDied};

pub struct Controller {
    command: Vec<OsString>,
    target_worker_count: usize,
    workers: Vec<Worker>,
    worker_tx: mpsc::Sender<(Uuid, Result<Message, Error>)>,
    worker_rx: mpsc::Receiver<(Uuid, Result<Message, Error>)>,
    home: TempDir,
    shutting_down: bool,
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
    state: Mutex<WorkerState>,
}

impl Controller {
    /// Creates a fresh controller.
    pub fn new(cmd: &[OsString], target_worker_count: usize) -> Result<Controller, Error> {
        let (worker_tx, worker_rx) = mpsc::channel(20 * target_worker_count);
        Ok(Controller {
            command: cmd.iter().cloned().collect(),
            target_worker_count,
            workers: Vec::new(),
            worker_tx,
            worker_rx,
            home: tempfile::tempdir()?,
            shutting_down: false,
        })
    }

    /// Runs the controller.
    pub async fn run(&mut self) -> Result<(), Error> {
        self.spawn_workers().await?;
        self.event_loop().await?;
        Ok(())
    }

    /// Spawns an extra worker.
    fn spawn_worker(&self) -> impl Future<Output = Result<Worker, Error>> {
        let mut cmd = Command::new(&self.command[0]);
        cmd.args(&self.command[1..]);
        let worker_tx = self.worker_tx.clone();
        let dir = self.home.path().to_path_buf();
        async move { spawn_worker(dir, cmd, worker_tx).await }
    }

    /// Spawns a given number of workers.
    async fn spawn_workers(&mut self) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for _ in 0..self.target_worker_count {
            set.spawn(self.spawn_worker());
        }

        while let Some(worker) = set.join_next().await {
            self.workers.push(worker??);
        }

        // make one worker fail
        self.send_msg(self.workers[0].worker_id, Message::Fail(Fail {}))
            .await?;
        self.send_msg(self.workers[0].worker_id, Message::Ping(Ping {}))
            .await?;

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
    fn get_worker(&self, worker_id: Uuid) -> Result<&Worker, Error> {
        self.workers
            .iter()
            .find(|x| x.worker_id == worker_id)
            .ok_or_else(|| anyhow!("worker '{}' is gone", worker_id))
    }

    /// Runs the main communication loop
    async fn event_loop(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                Some((worker_id, rv)) = self.worker_rx.recv() => {
                    match rv {
                        Ok(msg) => {
                            self.handle_message(worker_id, msg).await?;
                        }
                        Err(err) => {
                            event!(Level::ERROR, "worker errored: {}", err);
                        }
                    }
                }
                _ = signal::ctrl_c() => {
                    self.shutting_down = true;
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&mut self, worker_id: Uuid, msg: Message) -> Result<(), Error> {
        event!(Level::DEBUG, "worker message {:?}", msg);
        match msg {
            Message::Ping(_) => {
                println!("client sent a ping");
            }
            Message::WorkerDied(cmd) => {
                println!("worker {} died (status = {:?})", cmd.worker_id, cmd.status);
                self.workers.retain(|x| x.worker_id != worker_id);
                if !self.shutting_down {
                    self.workers.push(self.spawn_worker().await?);
                }
            }
            Message::WorkerStart(cmd) => {
                let worker = self.get_worker(worker_id)?;
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
    mainloop_tx: mpsc::Sender<(Uuid, Result<Message, Error>)>,
) -> Result<Worker, Error> {
    let worker_id = Uuid::new_v4();
    let rx_path = dir.join(format!("rx-{}.pipe", worker_id));
    let tx_path = dir.join(format!("tx-{}.pipe", worker_id));

    event!(Level::INFO, "Worker {} spawning", worker_id);

    mkfifo(&rx_path, stat::Mode::S_IRWXU)?;
    mkfifo(&tx_path, stat::Mode::S_IRWXU)?;

    let mut rx = pipe::OpenOptions::new().open_receiver(&rx_path)?;

    cmd.env("CODA_WORKER_WRITE_PATH", &rx_path);
    cmd.env("CODA_WORKER_READ_PATH", &tx_path);
    cmd.env("CODA_WORKER_ID", worker_id.to_string());
    let mut child = cmd.spawn()?;

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
        state: Mutex::new(WorkerState::default()),
    };

    {
        let mainloop_tx = mainloop_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = read_msg(&mut rx).await;
                let failed = msg.is_err();
                if mainloop_tx.send((worker_id, msg)).await.is_err() || failed {
                    break;
                }
            }
        });
    }

    {
        let mainloop_tx = mainloop_tx.clone();
        tokio::spawn(async move {
            let status = match child.wait().await {
                Ok(status) => status.code(),
                Err(_) => None,
            };
            mainloop_tx
                .send((
                    worker_id,
                    Ok(Message::WorkerDied(WorkerDied { worker_id, status })),
                ))
                .await
                .ok();
        });
    }

    Ok(worker)
}

async fn read_msg(rx: &mut pipe::Receiver) -> Result<Message, Error> {
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

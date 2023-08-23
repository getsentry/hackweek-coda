use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use ciborium::Value;
use futures::future::Either;
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::unix::pipe;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::{signal, time};
use tracing::{event, Level};
use uuid::Uuid;

use coda_ipc::{Cmd, Event, Message, Req, Resp, Task, WorkerDied, Workflow};

use crate::config::Config;
use crate::storage::{DequeuedItem, Storage};

pub struct Controller {
    command: Vec<OsString>,
    workers: Vec<Worker>,
    mainloop_tx: mpsc::Sender<(Recipient, Result<Message, Error>)>,
    mainloop_rx: mpsc::Receiver<(Recipient, Result<Message, Error>)>,
    listener: Option<Listener>,
    home: TempDir,
    shutting_down: bool,
    storage: Storage,
    config: Config,
}

struct Listener {
    listener: TcpListener,
    clients: HashMap<SocketAddr, OwnedWriteHalf>,
    mainloop_tx: mpsc::Sender<(Recipient, Result<Message, Error>)>,
}

#[derive(Debug, Copy, Clone, Hash, Ord, Eq, PartialEq, PartialOrd)]
pub enum Recipient {
    Worker(Uuid),
    Client(SocketAddr),
}

#[derive(Default, Debug)]
struct WorkerState {
    tasks: HashSet<String>,
    workflows: HashSet<String>,
}

/// Represents a worker process.
pub struct Worker {
    worker_id: Uuid,
    tx: pipe::Sender,
    state: WorkerState,
}

impl Controller {
    /// Creates a fresh controller.
    pub async fn new(cmd: &[OsString], config: Config) -> Result<Controller, Error> {
        let (mainloop_tx, mainloop_rx) = mpsc::channel(20 * config.worker_count());
        Ok(Controller {
            command: cmd.iter().cloned().collect(),
            workers: Vec::new(),
            mainloop_tx: mainloop_tx.clone(),
            mainloop_rx,
            home: tempfile::tempdir()?,
            shutting_down: false,
            storage: Storage::from_config(&config),
            listener: if let Some(addr) = config.listen_addr() {
                Some(Listener::connect(addr, mainloop_tx).await?)
            } else {
                None
            },
            config,
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
        let worker_tx = self.mainloop_tx.clone();
        let dir = self.home.path().to_path_buf();
        async move { spawn_worker(dir, cmd, worker_tx).await }
    }

    /// Spawns a given number of workers.
    async fn spawn_workers(&mut self) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for _ in 0..self.config.worker_count() {
            set.spawn(self.spawn_worker());
        }

        while let Some(worker) = set.join_next().await {
            self.workers.push(worker??);
        }

        Ok(())
    }

    /// Send a message to a recipient.
    async fn send_msg(&mut self, recipient: Recipient, msg: Message) -> Result<(), Error> {
        // TODO: buffer up messages and send them from a separate
        // spawn / in the event loop rather than awaiting here.
        let mut bytes = pack_msg(msg)?;
        match recipient {
            Recipient::Worker(worker_id) => {
                let worker = self
                    .workers
                    .iter_mut()
                    .find(|x| x.worker_id == worker_id)
                    .ok_or_else(|| anyhow!("cannot find worker"))?;
                worker.tx.write_all_buf(&mut bytes).await?
            }
            Recipient::Client(addr) => {
                if let Some(ref mut listener) = self.listener {
                    let sock = listener
                        .clients
                        .get_mut(&addr)
                        .ok_or_else(|| anyhow!("cannot find client"))?;
                    sock.write_all_buf(&mut bytes).await?
                } else {
                    bail!("cannot send to socket, no listener");
                }
            }
        }
        Ok(())
    }

    /// Finds a worker by worker ID.
    fn get_worker_mut(&mut self, worker_id: Uuid) -> Result<&mut Worker, Error> {
        self.workers
            .iter_mut()
            .find(|x| x.worker_id == worker_id)
            .ok_or_else(|| anyhow!("worker '{}' is gone", worker_id))
    }

    /// Runs the main communication loop
    async fn event_loop(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                _ = signal::ctrl_c() => {
                    self.shutting_down = true;
                    break;
                }
                _ = self.event_loop_iterate() => {}
            }
        }

        Ok(())
    }

    async fn event_loop_iterate(&mut self) -> Result<(), Error> {
        tokio::select! {
            Some((recipient, rv)) = self.mainloop_rx.recv() => {
                match rv {
                    Ok(msg) => {
                        match self.handle_message(recipient, msg).await {
                            Ok(()) => {}
                            Err(err) => {
                                event!(Level::ERROR, "message handler errored: {}", err);
                            }
                        }
                    }
                    Err(err) => {
                        event!(Level::ERROR, "worker errored: {}", err);
                    }
                }
            }
            _ = self.listener
                .as_mut()
                .map(|x| Either::Left(x.accept()))
                .unwrap_or(Either::Right(futures::future::pending())) => {}
            item = self.storage.dequeue() => {
                // XXX: this basically always gives it to the same worker
                // and even when the worker is busy.
                match item {
                    DequeuedItem::Task(task) => {
                        for worker in self.workers.iter() {
                            if worker.state.tasks.contains(&task.task_name) {
                                self.send_msg(Recipient::Worker(worker.worker_id), Message::Req(Req {
                                    request_id: None,
                                    cmd: Cmd::ExecuteTask(task),
                                })).await?;
                                break;
                            }
                        }
                    }
                    DequeuedItem::Workflow(workflow) => {
                        for worker in self.workers.iter() {
                            if worker.state.workflows.contains(&workflow.workflow_name) {
                                self.send_msg(Recipient::Worker(worker.worker_id), Message::Req(Req {
                                    request_id: None,
                                    cmd: Cmd::ExecuteWorkflow(workflow),
                                })).await?;
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, recipient: Recipient, msg: Message) -> Result<(), Error> {
        event!(Level::DEBUG, "worker message {:?}", msg);
        match msg {
            Message::Req(req) => {
                let request_id = req.request_id;
                let result = self.handle_request(recipient, req).await?;
                if let (Some(result), Some(request_id)) = (result, request_id) {
                    self.send_msg(recipient, Message::Resp(Resp { request_id, result }))
                        .await?;
                }
                Ok(())
            }
            Message::Resp(_resp) => Ok(()),
            Message::Event(event) => self.handle_event(recipient, event).await,
        }
    }

    async fn handle_event(&mut self, _recipient: Recipient, event: Event) -> Result<(), Error> {
        match event {
            Event::WorkerDied(cmd) => {
                println!("worker {} died (status = {:?})", cmd.worker_id, cmd.status);
                self.workers.retain(|x| x.worker_id != cmd.worker_id);
                if !self.shutting_down {
                    self.workers.push(self.spawn_worker().await?);
                }
            }
        }
        Ok(())
    }

    async fn handle_request(
        &mut self,
        recipient: Recipient,
        req: Req,
    ) -> Result<Option<Value>, Error> {
        Ok(match req.cmd {
            Cmd::RegisterWorker(cmd) => {
                let worker_id = require_worker(recipient)?;
                let worker = self.get_worker_mut(worker_id)?;
                worker.state.tasks = cmd.tasks;
                worker.state.workflows = cmd.workflows;
                event!(Level::DEBUG, "worker registered {:?}", worker.state);
                Some(Value::Null)
            }
            Cmd::StoreParams(cmd) => {
                self.storage
                    .params
                    .insert((cmd.workflow_run_id, cmd.params_id), Arc::new(cmd.params));
                Some(Value::Null)
            }
            Cmd::GetParams(cmd) => match self
                .storage
                .params
                .get(&(cmd.workflow_run_id, cmd.params_id))
            {
                Some(params) => Some(Value::Map(
                    params
                        .iter()
                        .map(|(k, v)| (Value::Text(k.to_string()), v.clone()))
                        .collect(),
                )),
                None => Some(Value::Null),
            },
            Cmd::SpawnTask(task) => {
                if !self
                    .storage
                    .has_task_result(task.workflow_run_id, task.task_key)
                {
                    self.enqueue_task(task).await?;
                }
                Some(Value::Null)
            }
            Cmd::GetTaskResult(cmd) => {
                let worker_id = require_worker(recipient)?;
                if let Some(result) = self
                    .storage
                    .get_task_result(cmd.workflow_run_id, cmd.task_key)
                {
                    Some(result)
                } else {
                    if let Some(request_id) = req.request_id {
                        self.storage.notify_task_result(
                            cmd.workflow_run_id,
                            cmd.task_key,
                            Recipient::Worker(worker_id),
                            request_id,
                        );
                        None
                    } else {
                        Some(Value::Null)
                    }
                }
            }
            Cmd::PublishTaskResult(cmd) => {
                if let Some(interests) = self.storage.store_task_result(
                    cmd.workflow_run_id,
                    cmd.task_key,
                    cmd.result.clone(),
                ) {
                    for (recipient, request_id) in interests.into_iter() {
                        self.send_msg(
                            recipient,
                            Message::Resp(Resp {
                                request_id,
                                result: cmd.result.clone(),
                            }),
                        )
                        .await?;
                    }
                }
                Some(Value::Null)
            }
            Cmd::SpawnWorkflow(workflow) => {
                self.enqueue_workflow(workflow).await?;
                Some(Value::Null)
            }
            Cmd::WorkflowEnded(_cmd) => {
                // todo: delete data of workflow
                None
            }
            Cmd::ExecuteTask(_) | Cmd::ExecuteWorkflow(_) => {
                bail!("command cannot be sent to supervisor")
            }
        })
    }

    async fn enqueue_task(&mut self, task: Task) -> Result<(), Error> {
        let queue_name = self.config.queue_name_for_task_name(&task.task_name);
        self.storage.enqueue_task(queue_name, task).await?;
        Ok(())
    }

    async fn enqueue_workflow(&mut self, workflow: Workflow) -> Result<(), Error> {
        let queue_name = self
            .config
            .queue_name_for_workflow_name(&workflow.workflow_name);
        self.storage.enqueue_workflow(queue_name, workflow).await?;
        Ok(())
    }
}

/// Spawns a worker that sends its messages into `worker_tx`.
async fn spawn_worker(
    dir: PathBuf,
    mut cmd: Command,
    mainloop_tx: mpsc::Sender<(Recipient, Result<Message, Error>)>,
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
        tx,
        state: WorkerState::default(),
    };

    {
        let mainloop_tx = mainloop_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = read_msg(&mut rx).await;
                let failed = msg.is_err();
                if mainloop_tx
                    .send((Recipient::Worker(worker_id), msg))
                    .await
                    .is_err()
                    || failed
                {
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
                    Recipient::Worker(worker_id),
                    Ok(Message::Event(Event::WorkerDied(WorkerDied {
                        worker_id,
                        status,
                    }))),
                ))
                .await
                .ok();
        });
    }

    Ok(worker)
}

fn require_worker(recipient: Recipient) -> Result<Uuid, Error> {
    if let Recipient::Worker(worker_id) = recipient {
        Ok(worker_id)
    } else {
        bail!("command requires worker")
    }
}

async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Message, Error> {
    let mut bytes = [0u8; 4];
    r.read_exact(&mut bytes).await?;
    let len = (&bytes[..]).get_u32();
    let mut buf = vec![0; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(ciborium::from_reader(&buf[..])?)
}

fn pack_msg(msg: Message) -> Result<Cursor<Bytes>, Error> {
    let mut buf = Vec::<u8>::new();
    ciborium::into_writer(&msg, &mut buf).unwrap();
    let mut rv = BytesMut::with_capacity(4);
    rv.put_u32(buf.len() as u32);
    rv.extend_from_slice(&buf);
    Ok(Cursor::new(rv.freeze()))
}

impl Listener {
    pub async fn connect(
        addr: SocketAddr,
        mainloop_tx: mpsc::Sender<(Recipient, Result<Message, Error>)>,
    ) -> Result<Listener, Error> {
        Ok(Listener {
            listener: TcpListener::bind(addr).await?,
            clients: HashMap::new(),
            mainloop_tx,
        })
    }

    pub async fn accept(&mut self) -> Result<(), Error> {
        let (stream, addr) = self.listener.accept().await?;
        let (mut read, write) = stream.into_split();
        self.clients.insert(addr, write);
        let mainloop_tx = self.mainloop_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = read_msg(&mut read).await;
                let failed = msg.is_err();
                if mainloop_tx
                    .send((Recipient::Client(addr), msg))
                    .await
                    .is_err()
                    || failed
                {
                    break;
                }
            }
        });
        Ok(())
    }
}

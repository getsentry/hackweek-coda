use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::future::{poll_fn, Future};
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, Error};
use bytes::{Buf, BufMut, BytesMut};
use ciborium::Value;
use nix::libc::ENXIO;
use nix::sys::stat;
use nix::unistd::mkfifo;
use rand::seq::SliceRandom;
use rand::thread_rng;
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

use coda_ipc::{Cmd, Event, Message, Req, Resp, Task, WorkerDied};

use crate::config::Config;

pub struct Controller {
    command: Vec<OsString>,
    target_worker_count: usize,
    workers: Vec<Worker>,
    worker_tx: mpsc::Sender<(Uuid, Result<Message, Error>)>,
    worker_rx: mpsc::Receiver<(Uuid, Result<Message, Error>)>,
    home: TempDir,
    shutting_down: bool,
    storage: Storage,
    config: Config,
}

#[derive(Default, Debug)]
struct WorkerState {
    tasks: HashSet<String>,
    workflows: HashSet<String>,
}

#[derive(Debug, Default)]
struct Storage {
    params: HashMap<Uuid, Arc<BTreeMap<String, Value>>>,
    task_queues: HashMap<String, TaskQueue>,
}

impl Storage {
    pub fn from_config(config: &Config) -> Storage {
        let mut task_queues = HashMap::new();
        for queue in config.iter_queues() {
            task_queues.insert(queue.to_string(), TaskQueue::new());
        }

        Storage {
            params: HashMap::new(),
            task_queues,
        }
    }

    async fn next_queue_item(&mut self) -> (String, Task) {
        poll_fn(move |cx: &mut Context| {
            // TODO: better logic here
            let mut rng = thread_rng();
            let mut queues = self.task_queues.iter_mut().collect::<Vec<_>>();
            queues.shuffle(&mut rng);
            for (queue_name, task_queue) in queues {
                match task_queue.rx.poll_recv(cx) {
                    Poll::Ready(Some(rdy)) => return Poll::Ready((queue_name.to_string(), rdy)),
                    _ => continue,
                }
            }
            Poll::Pending
        })
        .await
    }
}

#[derive(Debug)]
struct TaskQueue {
    tx: mpsc::Sender<Task>,
    rx: mpsc::Receiver<Task>,
}

impl TaskQueue {
    pub fn new() -> TaskQueue {
        let (tx, rx) = mpsc::channel(20);
        TaskQueue { tx, rx }
    }
}

/// Represents a worker process.
pub struct Worker {
    worker_id: Uuid,
    tx: Mutex<pipe::Sender>,
    state: WorkerState,
}

impl Controller {
    /// Creates a fresh controller.
    pub fn new(
        cmd: &[OsString],
        target_worker_count: usize,
        config: Config,
    ) -> Result<Controller, Error> {
        let (worker_tx, worker_rx) = mpsc::channel(20 * target_worker_count);
        Ok(Controller {
            command: cmd.iter().cloned().collect(),
            target_worker_count,
            workers: Vec::new(),
            worker_tx,
            worker_rx,
            home: tempfile::tempdir()?,
            shutting_down: false,
            storage: Storage::from_config(&config),
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
                Some((worker_id, rv)) = self.worker_rx.recv() => {
                    match rv {
                        Ok(msg) => {
                            match self.handle_message(worker_id, msg).await {
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
                (_, ref task) = self.storage.next_queue_item() => {
                    for worker in self.workers.iter() {
                        // XXX: this basically always gives it to the same worker
                        // and even when the worker is busy.
                        if worker.state.tasks.contains(&task.task_name) {
                            self.send_msg(worker.worker_id, Message::Req(Req {
                                request_id: None,
                                cmd: Cmd::ExecuteTask(task.clone()),
                            })).await?;
                            break;
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
            Message::Req(req) => {
                let request_id = req.request_id;
                let result = self.handle_request(worker_id, req).await?;
                if let Some(request_id) = request_id {
                    self.send_msg(worker_id, Message::Resp(Resp { request_id, result }))
                        .await?;
                }
                Ok(())
            }
            Message::Resp(_resp) => Ok(()),
            Message::Event(event) => self.handle_event(worker_id, event).await,
        }
    }

    async fn handle_event(&mut self, worker_id: Uuid, event: Event) -> Result<(), Error> {
        match event {
            Event::WorkerDied(cmd) => {
                println!("worker {} died (status = {:?})", cmd.worker_id, cmd.status);
                self.workers.retain(|x| x.worker_id != worker_id);
                if !self.shutting_down {
                    self.workers.push(self.spawn_worker().await?);
                }
            }
        }
        Ok(())
    }

    async fn handle_request(&mut self, worker_id: Uuid, req: Req) -> Result<Value, Error> {
        Ok(match req.cmd {
            Cmd::StoreParams(cmd) => {
                self.storage
                    .params
                    .insert(cmd.params_id, Arc::new(cmd.params));
                Value::Null
            }
            Cmd::GetParams(cmd) => match self.storage.params.get(&cmd.params_id) {
                Some(params) => Value::Map(
                    params
                        .iter()
                        .map(|(k, v)| (Value::Text(k.to_string()), v.clone()))
                        .collect(),
                ),
                None => Value::Null,
            },
            Cmd::SpawnTask(task) => {
                self.enqueue_task(task).await?;
                Value::Null
            }
            Cmd::RegisterWorker(cmd) => {
                let worker = self.get_worker_mut(worker_id)?;
                worker.state.tasks = cmd.tasks;
                worker.state.workflows = cmd.workflows;
                event!(Level::DEBUG, "worker registered {:?}", worker.state);
                Value::Null
            }
            other => {
                event!(Level::WARN, "unhandled message {:?}", other);
                Value::Null
            }
        })
    }

    async fn enqueue_task(&mut self, task: Task) -> Result<(), Error> {
        let queue_name = self.config.queue_name_for_task_name(&task.task_name);
        let q = self
            .storage
            .task_queues
            .get(queue_name)
            .ok_or_else(|| anyhow!("could not find queue '{}'", queue_name))?;
        q.tx.send(task).await?;
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
        state: WorkerState::default(),
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::{anyhow, Error};
use ciborium::Value;
use coda_ipc::Task;
use coda_ipc::Workflow;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::config::Config;

#[derive(Debug)]
pub struct InMemoryQueue<T> {
    tx: mpsc::Sender<T>,
    rx: mpsc::Receiver<T>,
}

impl<T: fmt::Debug + Sync + Send + 'static> InMemoryQueue<T> {
    pub fn new() -> InMemoryQueue<T> {
        let (tx, rx) = mpsc::channel(20);
        InMemoryQueue { tx, rx }
    }

    pub async fn send(&self, item: T) -> Result<(), Error> {
        self.tx.send(item).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum DequeuedItem {
    Task(Task),
    Workflow(Workflow),
}

#[derive(Debug, Default)]
pub(crate) struct Storage {
    pub(crate) params: HashMap<(Uuid, Uuid), Arc<BTreeMap<String, Value>>>,
    task_queues: HashMap<String, InMemoryQueue<Task>>,
    workflow_queues: HashMap<String, InMemoryQueue<Workflow>>,
    // TODO: add expiration
    task_results: HashMap<(Uuid, Uuid), Value>,
    task_result_interests: HashMap<(Uuid, Uuid), HashSet<(Uuid, Uuid)>>,
}

impl Storage {
    pub fn from_config(config: &Config) -> Storage {
        let mut task_queues = HashMap::new();
        for queue in config.iter_task_queues() {
            task_queues.insert(queue.to_string(), InMemoryQueue::new());
        }

        let mut workflow_queues = HashMap::new();
        for queue in config.iter_workflow_queues() {
            workflow_queues.insert(queue.to_string(), InMemoryQueue::new());
        }

        Storage {
            params: HashMap::new(),
            task_queues,
            workflow_queues,
            task_results: HashMap::new(),
            task_result_interests: HashMap::new(),
        }
    }

    /// Stores a result.
    ///
    /// This returns the registered interests on the value and they must be notified.
    #[must_use]
    pub fn store_result(
        &mut self,
        workflow_run_id: Uuid,
        task_key: Uuid,
        result: Value,
    ) -> Option<HashSet<(Uuid, Uuid)>> {
        self.task_results
            .insert((workflow_run_id, task_key), result);
        self.task_result_interests
            .remove(&(workflow_run_id, task_key))
    }

    /// Retrieves a result.
    pub fn get_task_result(&mut self, workflow_run_id: Uuid, task_key: Uuid) -> Option<Value> {
        self.task_results.get(&(workflow_run_id, task_key)).cloned()
    }

    /// Checks if a result exists.
    pub fn has_task_result(&mut self, workflow_run_id: Uuid, task_key: Uuid) -> bool {
        self.task_results.contains_key(&(workflow_run_id, task_key))
    }

    /// Register a result interest.
    pub fn notify_task_result(
        &mut self,
        workflow_run_id: Uuid,
        task_key: Uuid,
        worker_id: Uuid,
        request_id: Uuid,
    ) {
        self.task_result_interests
            .entry((workflow_run_id, task_key))
            .or_default()
            .insert((worker_id, request_id));
    }

    /// Publishes a task to a specific queue.
    pub async fn enqueue_task(&self, queue: &str, task: Task) -> Result<(), Error> {
        let q = self
            .task_queues
            .get(queue)
            .ok_or_else(|| anyhow!("could not find task queue '{}'", queue))?;
        q.send(task).await?;
        Ok(())
    }

    /// Publishes a workflow to a specific queue.
    pub async fn enqueue_workflow(&self, queue: &str, workflow: Workflow) -> Result<(), Error> {
        let q = self
            .workflow_queues
            .get(queue)
            .ok_or_else(|| anyhow!("could not find workflow queue '{}'", queue))?;
        q.send(workflow).await?;
        Ok(())
    }

    /// Picks up an item from any of the workflow queues.
    pub async fn dequeue(&mut self) -> DequeuedItem {
        enum Queue<'x> {
            Task(&'x mut InMemoryQueue<Task>),
            Workflow(&'x mut InMemoryQueue<Workflow>),
        }

        poll_fn(move |cx: &mut Context| {
            // TODO: better logic here
            let mut rng = thread_rng();
            let mut queues = self
                .workflow_queues
                .values_mut()
                .map(Queue::Workflow)
                .chain(self.task_queues.values_mut().map(Queue::Task))
                .collect::<Vec<_>>();
            queues.shuffle(&mut rng);
            for queue in queues {
                match queue {
                    Queue::Task(task_queue) => match task_queue.rx.poll_recv(cx) {
                        Poll::Ready(Some(rdy)) => return Poll::Ready(DequeuedItem::Task(rdy)),
                        _ => continue,
                    },
                    Queue::Workflow(workflow_queue) => match workflow_queue.rx.poll_recv(cx) {
                        Poll::Ready(Some(rdy)) => return Poll::Ready(DequeuedItem::Workflow(rdy)),
                        _ => continue,
                    },
                }
            }
            Poll::Pending
        })
        .await
    }
}

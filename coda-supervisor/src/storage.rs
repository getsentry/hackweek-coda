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
use coda_ipc::{Outcome, Task, Workflow, WorkflowStatus};
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::config::Config;
use crate::controller::Recipient;

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

#[derive(Debug)]
struct WorkflowState {
    task_results: HashMap<Uuid, Outcome>,
    task_result_interests: HashMap<Uuid, HashSet<(Recipient, Uuid)>>,
    // XXX: i hate it, because active tasks disappear when they are
    // rescheduled on retry :-/
    active_tasks: HashMap<Uuid, Task>,
    status: WorkflowStatus,
}

#[derive(Debug, Default)]
pub(crate) struct Storage {
    config: Arc<Config>,
    pub(crate) params: HashMap<(Uuid, Uuid), Arc<BTreeMap<String, Value>>>,
    task_queues: HashMap<String, InMemoryQueue<Task>>,
    workflow_queues: HashMap<String, InMemoryQueue<Workflow>>,
    workflow_state: HashMap<Uuid, WorkflowState>,
}

impl Storage {
    pub fn from_config(config: Arc<Config>) -> Storage {
        let mut task_queues = HashMap::new();
        for queue in config.iter_task_queues() {
            task_queues.insert(queue.to_string(), InMemoryQueue::new());
        }

        let mut workflow_queues = HashMap::new();
        for queue in config.iter_workflow_queues() {
            workflow_queues.insert(queue.to_string(), InMemoryQueue::new());
        }

        Storage {
            config,
            params: HashMap::new(),
            task_queues,
            workflow_queues,
            workflow_state: HashMap::new(),
        }
    }

    pub fn register_active_task(&mut self, task: &Task) {
        if let Some(workflow_state) = self.workflow_state.get_mut(&task.workflow_run_id) {
            workflow_state
                .active_tasks
                .insert(task.task_id, task.clone());
        }
    }

    pub fn register_workflow(&mut self, workflow_run_id: Uuid) {
        self.workflow_state.insert(
            workflow_run_id,
            WorkflowState {
                task_results: HashMap::new(),
                task_result_interests: HashMap::new(),
                active_tasks: HashMap::new(),
                status: WorkflowStatus::Enqueued,
            },
        );
    }

    pub fn mark_workflow_started(&mut self, workflow_run_id: Uuid) {
        if let Some(workflow_state) = self.workflow_state.get_mut(&workflow_run_id) {
            workflow_state.status = WorkflowStatus::InProgress;
        }
    }

    pub fn finish_workflow(&mut self, workflow_run_id: Uuid) {
        self.workflow_state.remove(&workflow_run_id);
    }

    pub fn workflow_is_live(&self, workflow_run_id: Uuid) -> bool {
        self.workflow_state
            .get(&workflow_run_id)
            .map_or(false, |x| match x.status {
                WorkflowStatus::Enqueued | WorkflowStatus::InProgress => true,
                _ => false,
            })
    }

    /// Stores a result and marks a task as done.
    ///
    /// This returns the registered interests on the value and they must be notified.
    #[must_use]
    pub fn store_task_result(
        &mut self,
        workflow_run_id: Uuid,
        task_key: Uuid,
        task_id: Uuid,
        result: Outcome,
    ) -> Option<HashSet<(Recipient, Uuid)>> {
        if let Some(workflow_state) = self.workflow_state.get_mut(&workflow_run_id) {
            workflow_state.active_tasks.remove(&task_id);
            workflow_state.task_results.insert(task_key, result);
            workflow_state.task_result_interests.remove(&task_key)
        } else {
            None
        }
    }

    /// Marks a task as failed.
    ///
    /// Returns a new task if it needs to be retried.
    #[must_use]
    pub fn handle_task_failed(
        &mut self,
        workflow_run_id: Uuid,
        task_id: Uuid,
        retryable: bool,
    ) -> Option<Task> {
        if let Some(workflow_state) = self.workflow_state.get_mut(&workflow_run_id) {
            if let Some(mut active_task) = workflow_state.active_tasks.remove(&task_id) {
                if retryable && active_task.retries_remaining > 0 {
                    active_task.retries_remaining -= 1;
                    active_task.task_id = Uuid::new_v4();
                    return Some(active_task);
                }
            }
        }
        None
    }

    /// Retrieves a result.
    pub fn get_task_result(&self, workflow_run_id: Uuid, task_key: Uuid) -> Option<Outcome> {
        self.workflow_state
            .get(&workflow_run_id)
            .and_then(|x| x.task_results.get(&task_key))
            .cloned()
    }

    /// Checks if a result exists.
    pub fn has_task_result(&self, workflow_run_id: Uuid, task_key: Uuid) -> bool {
        self.workflow_state
            .get(&workflow_run_id)
            .map_or(false, |x| x.task_results.contains_key(&task_key))
    }

    /// Register a result interest.
    pub fn notify_task_result(
        &mut self,
        workflow_run_id: Uuid,
        task_key: Uuid,
        recipient: Recipient,
        request_id: Uuid,
    ) {
        if let Some(workflow_state) = self.workflow_state.get_mut(&workflow_run_id) {
            workflow_state
                .task_result_interests
                .entry(task_key)
                .or_default()
                .insert((recipient, request_id));
        }
    }

    /// Publishes a task to a specific queue.
    pub async fn enqueue_task(&self, task: Task) -> Result<(), Error> {
        let queue_name = self.config.queue_name_for_task_name(&task.task_name);
        let q = self
            .task_queues
            .get(queue_name)
            .ok_or_else(|| anyhow!("could not find task queue '{}'", queue_name))?;
        q.send(task).await?;
        Ok(())
    }

    /// Publishes a workflow to a specific queue.
    pub async fn enqueue_workflow(&self, workflow: Workflow) -> Result<(), Error> {
        let queue_name = self
            .config
            .queue_name_for_workflow_name(&workflow.workflow_name);
        let q = self
            .workflow_queues
            .get(queue_name)
            .ok_or_else(|| anyhow!("could not find workflow queue '{}'", queue_name))?;
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

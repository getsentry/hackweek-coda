use std::collections::{BTreeMap, HashSet};

use ciborium::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Req(Req),
    Resp(Resp),
    Event(Event),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Req {
    #[serde(flatten)]
    pub cmd: Cmd,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "cmd", content = "args", rename_all = "snake_case")]
pub enum Cmd {
    RegisterWorker(RegisterWorker),
    SpawnTask(Task),
    ExecuteTask(Task),
    TaskFailed(TaskFailed),
    StoreParams(StoreParams),
    GetParams(GetParams),
    PublishTaskResult(PublishTaskResult),
    GetTaskResult(GetTaskResult),
    SpawnWorkflow(Workflow),
    ExecuteWorkflow(Workflow),
    WorkflowFailed(WorkflowFailed),
    WorkflowEnded(WorkflowEnded),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Outcome {
    Success(Value),
    Failure(Value),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Resp {
    pub request_id: Uuid,
    pub result: Outcome,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    WorkerDied(WorkerDied),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorker {
    pub tasks: HashSet<String>,
    pub workflows: HashSet<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerDied {
    pub worker_id: Uuid,
    pub status: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreParams {
    pub workflow_run_id: Uuid,
    pub params_id: Uuid,
    pub params: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetParams {
    pub workflow_run_id: Uuid,
    pub params_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    pub task_name: String,
    pub task_id: Uuid,
    pub task_key: Uuid,
    pub params_id: Uuid,
    pub workflow_run_id: Uuid,
    pub persist_result: bool,
    #[serde(default)]
    pub retries_remaining: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetTaskResult {
    pub workflow_run_id: Uuid,
    pub task_key: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublishTaskResult {
    pub workflow_run_id: Uuid,
    pub task_key: Uuid,
    pub task_id: Uuid,
    pub result: Outcome,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskFailed {
    pub workflow_run_id: Uuid,
    pub task_id: Uuid,
    pub task_key: Uuid,
    pub retryable: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Workflow {
    pub workflow_name: String,
    pub workflow_run_id: Uuid,
    pub params_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkflowFailed {
    pub workflow_run_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkflowEnded {
    pub workflow_run_id: Uuid,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Enqueued,
    InProgress,
    Failed,
    Success,
}

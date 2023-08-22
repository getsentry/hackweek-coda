use std::collections::{BTreeMap, HashSet};

use ciborium::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "cmd", content = "args", rename_all = "snake_case")]
pub enum Message {
    RequestWorkerShutdown(RequestWorkerShutdown),
    WorkerStart(WorkerStart),
    WorkerDied(WorkerDied),
    SpawnTask(Task),
    RunTask(Task),
    StoreParams(StoreParams),
    StartWorkflow(StartWorkflow),
    PublishTaskResult(PublishTaskResult),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerStart {
    pub tasks: HashSet<String>,
    pub workflows: HashSet<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerDied {
    pub worker_id: Uuid,
    pub status: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestWorkerShutdown {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    pub task_name: String,
    pub task_id: Uuid,
    pub task_key: Uuid,
    pub params_id: Uuid,
    pub workflow_run_id: Uuid,
    pub persist_result: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreParams {
    pub workflow_run_id: Uuid,
    pub params_id: Uuid,
    pub params: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartWorkflow {
    pub workflow_name: String,
    pub workflow_run_id: Uuid,
    pub params_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Ok,
    Error,
    Timeout,
    Killed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublishTaskResult {
    pub task_id: Uuid,
    pub task_key: Uuid,
    pub result: Value,
    pub status: TaskStatus,
}

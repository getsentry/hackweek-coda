use std::collections::BTreeMap;

use ciborium::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "cmd", content = "args", rename_all = "snake_case")]
pub enum Message {
    SpawnTask(SpawnTask),
    StoreParams(StoreParams),
    StartWorkflow(StartWorkflow),
    PublishTaskResult(PublishTaskResult),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SpawnTask {
    task_name: String,
    task_id: Uuid,
    task_key: Uuid,
    param_id: Uuid,
    workflow_run_id: Uuid,
    persist_result: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreParams {
    workflow_run_id: Uuid,
    params_id: Uuid,
    params: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartWorkflow {
    workflow_name: String,
    workflow_run_id: Uuid,
    params_id: Uuid,
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
    task_id: Uuid,
    task_key: Uuid,
    result: Value,
    status: TaskStatus,
}

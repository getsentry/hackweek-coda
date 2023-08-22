use std::collections::BTreeMap;
use std::sync::Arc;

use ciborium::Value;
use uuid::Uuid;

pub struct Task {
    task_name: String,
    task_id: Uuid,
    task_key: Uuid,
    params: Arc<BTreeMap<String, Value>>,
    workflow_run_id: Uuid,
    persist_result: bool,
}

impl Task {
    pub fn new(
        task_name: String,
        task_id: Uuid,
        task_key: Uuid,
        params: Arc<BTreeMap<String, Value>>,
        workflow_run_id: Uuid,
        persist_result: bool,
    ) -> Task {
        Task {
            task_name,
            task_id,
            task_key,
            params,
            workflow_run_id,
            persist_result,
        }
    }

    pub fn name(&self) -> &str {
        &self.task_name
    }
}

use std::fmt;
use uuid::Uuid;

pub enum WorkflowState {
    Created,
    Scheduled,
    TaskCreated(Uuid),
    TaskScheduled(Uuid),
    TaskCompleted(Uuid),
    Completed,
}

impl fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let string_repr = match self {
            WorkflowState::Created => String::from("created"),
            WorkflowState::Scheduled => String::from("scheduled"),
            WorkflowState::TaskCreated(task_key) => format!("task-created_{}", task_key),
            WorkflowState::TaskScheduled(task_key) => format!("task-scheduled_{}", task_key),
            WorkflowState::TaskCompleted(task_key) => format!("task-completed_{}", task_key),
            WorkflowState::Completed => String::from("completed"),
        };

        write!(f, "{}", string_repr)
    }
}

pub struct Workflow {
    pub workflow_name: String,
    pub workflow_run_id: Uuid,
    pub workflow_params_id: Option<Uuid>,
}

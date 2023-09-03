use std::collections::HashSet;
use std::hash::Hash;
use uuid::Uuid;
use coda_ipc::Workflow;

struct Supervisor {
    supervisor_id: Uuid,
    workflows: HashSet<String>,
    tasks: HashSet<String>
}
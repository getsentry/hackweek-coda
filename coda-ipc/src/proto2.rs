use std::collections::{BTreeMap, HashSet};

use ciborium::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Request(Request),
    Response(Response),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    #[serde(flatten)]
    pub command: Command,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub result: Value,
    pub error: bool,
    pub request_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "command", content = "args", rename_all = "snake_case")]
pub enum Command {
    RegisterWorker(RegisterWorker),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorker {
    pub tasks: HashSet<String>,
    pub workflows: HashSet<String>,
}

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ConfigValues {
    #[serde(default)]
    task_queues: HashMap<String, HashSet<String>>,
    #[serde(default)]
    workflow_queues: HashMap<String, HashSet<String>>,
}

#[derive(Debug)]
pub struct Config {
    values: ConfigValues,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            values: ConfigValues {
                task_queues: HashMap::new(),
                workflow_queues: HashMap::new(),
            },
        }
    }
}

impl Config {
    /// Loads a config from a given path.
    pub fn from_path<P: AsRef<Path>>(p: P) -> Result<Config, Error> {
        let path = p.as_ref();
        Ok(Config {
            values: toml::from_str(
                &fs::read_to_string(path)
                    .with_context(|| format!("cannot load config file '{}'", path.display()))?,
            )
            .with_context(|| format!("malformed config file '{}'", path.display()))?,
        })
    }

    /// Iterates over all known task queue names.
    pub fn iter_task_queues(&self) -> impl Iterator<Item = &str> {
        if self.values.task_queues.contains_key("default") {
            None
        } else {
            Some("default")
        }
        .into_iter()
        .chain(self.values.task_queues.keys().map(|x| x.as_str()))
    }

    /// Returns the queue name for a task name.
    pub fn queue_name_for_task_name(&self, task_name: &str) -> &str {
        for (queue, tasks) in self.values.task_queues.iter() {
            if tasks.contains(task_name) {
                return queue;
            }
        }
        "default"
    }

    /// Iterates over all known workflow queue names.
    pub fn iter_workflow_queues(&self) -> impl Iterator<Item = &str> {
        if self.values.workflow_queues.contains_key("default") {
            None
        } else {
            Some("default")
        }
        .into_iter()
        .chain(self.values.workflow_queues.keys().map(|x| x.as_str()))
    }

    /// Returns the queue name for a workflow name.
    pub fn queue_name_for_workflow_name(&self, workflow_name: &str) -> &str {
        for (queue, workflows) in self.values.workflow_queues.iter() {
            if workflows.contains(workflow_name) {
                return queue;
            }
        }
        "default"
    }
}

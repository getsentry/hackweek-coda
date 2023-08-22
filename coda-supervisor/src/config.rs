use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ConfigValues {
    queues: HashMap<String, HashSet<String>>,
}

#[derive(Debug)]
pub struct Config {
    values: ConfigValues,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            values: ConfigValues {
                queues: HashMap::new(),
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

    /// Iterates over all known queue names.
    pub fn iter_queues(&self) -> impl Iterator<Item = &str> {
        if self.values.queues.contains_key("default") {
            None
        } else {
            Some("default")
        }
        .into_iter()
        .chain(self.values.queues.keys().map(|x| x.as_str()))
    }

    /// Returns the queue name for a task name.
    pub fn queue_name_for_task_name(&self, task_name: &str) -> &str {
        for (queue, tasks) in self.values.queues.iter() {
            if tasks.contains(task_name) {
                return queue;
            }
        }
        "default"
    }
}
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
}

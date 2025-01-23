use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub service_config_path: String,
    pub zenoh_config_path: Option<String>,
    pub logging_output_path: String,
    pub topics: HashMap<String, String>,
    pub logger_ip: String,
}

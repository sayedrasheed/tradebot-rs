use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub service_config_path: String,
    pub topics: HashMap<String, String>,
    pub pnl_ip: String,
    pub futures_config: HashMap<String, FuturesTickConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct FuturesTickConfig {
    pub tick_size: f32,
    pub tick_value: f32,
}

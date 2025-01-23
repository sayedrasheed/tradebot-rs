use std::collections::HashMap;

use common::config::TopicMtype;
use serde::{Deserialize, Serialize};
use tradebot_protos::messages::start_algo::AlgoId;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct AlgoConfig {
    pub algo_id: AlgoId,
    pub topics: Vec<TopicMtype>,
    pub config: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub service_config_path: String,
    pub topics: HashMap<String, String>,
    pub algo_ip: String,
    pub algos: Vec<AlgoConfig>,
}

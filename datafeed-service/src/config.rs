use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tradebot_protos::messages::DatafeedProvider;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatafeedConfig {
    pub provider: DatafeedProvider,
    pub config: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub service_config_path: String,
    pub datafeed_ip: String,
    pub topics: HashMap<String, String>,
    pub datafeeds: Vec<DatafeedConfig>,
}

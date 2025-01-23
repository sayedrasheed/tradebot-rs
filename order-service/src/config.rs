use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tradebot_protos::messages::start_broker::BrokerId;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub service_config_path: String,
    pub topics: HashMap<String, String>,
}

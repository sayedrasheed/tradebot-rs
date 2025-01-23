use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::start_algo::AlgoId;
use tradebot_protos::messages::start_broker::BrokerId;
use tradebot_protos::messages::{DatafeedProvider, DatafeedType, ExecutionMode};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServiceConfig {
    pub zenoh_config_path: Option<String>,
    pub ip: String,
    pub port: u16,
    pub topics: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatafeedConfig {
    pub enable_ticks: bool,
    pub symbol: String,
    pub datafeed_provider: DatafeedProvider,
    pub candle_streams: Vec<CandleConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Strategy {
    pub strategy_id: Option<String>,
    pub broker_id: BrokerId,
    pub algo_id: AlgoId,
    pub exe_mode: ExecutionMode,
    pub df_type: DatafeedType,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub datafeeds: Vec<DatafeedConfig>,
    pub account_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RunConfig {
    pub service_config_path: String,
    pub zenoh_config_path: Option<String>,
    pub batch_id: String,
    pub stratigies: Vec<Strategy>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TopicConfig {
    pub topics: HashMap<String, String>,
}

impl TopicConfig {
    // Request mapping of publisher topic name. If no mapping exists, this returns `default_topic`.
    pub fn get(&self, default_topic: &str) -> String {
        self.topics
            .get(default_topic)
            .map_or_else(|| default_topic.to_owned(), |topic| topic.clone())
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }
}

impl From<&HashMap<String, String>> for TopicConfig {
    fn from(topics: &HashMap<String, String>) -> Self {
        Self {
            topics: topics.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TopicMtype {
    pub mtype: MessageType,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct HistoricalConfig {
    pub num_days: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CandleConfig {
    pub period_s: u32,
    pub historical_config: Option<HistoricalConfig>,
}

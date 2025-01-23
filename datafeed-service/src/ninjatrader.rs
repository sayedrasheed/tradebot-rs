use std::collections::HashMap;
use std::collections::HashSet;

use crate::datafeed::{DatafeedError, DatafeedJoinHandle, DatafeedSetup, DatafeedSpawn};
use async_trait::async_trait;
use common::config::ServiceConfig;
use common::config::TopicConfig;
use common::config::TopicMtype;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::DatafeedProvider;
use tradebot_protos::messages::Network;
use tradebot_protos::messages::RunStrategy;
use tradebot_protos::messages::StartDatafeed;
use tradebot_protos::messages::{
    Candle, DatafeedComplete, DatafeedStream, ExecutionMode, HistoricalData, Ohlcv, Tick, Topic,
};
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::Subscribe;
use zenoh_node::node::SubscriberError;
use zenoh_node::node::{Node, Publisher};

#[derive(Debug, Snafu)]
pub enum NinjaTraderError {
    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Unable to open {filename}"))]
    SerdeYamlError {
        source: serde_yaml::Error,
        filename: String,
    },

    #[snafu(display("Unable to decode stream from file {filename}"))]
    DecoderStreamError {
        source: databento::dbn::Error,
        filename: String,
    },

    #[snafu(display("Unable to convert date string"))]
    ConvertDateStrError { source: chrono::format::ParseError },

    #[snafu(display("Failed to publish data"))]
    PublishError { source: NodeError },
}

pub struct NinjaTraderService {
    config: NinjaTraderConfig,
    topics: TopicConfig,
    live_node: Node,
    active_feeds: HashSet<String>,
}
impl NinjaTraderService {
    pub async fn new(config_path: &str, live_network: Network) -> Result<Self, SubscriberError> {
        let config = Self::parse_config(config_path)?;
        let topics = TopicConfig::from(&config.topics);
        let mut builder = NodeBuilder::new();
        tracing::debug!(
            "Set up live datafeed network on ip: {}, port: {}",
            live_network.ip,
            live_network.port
        );
        builder.set_network((live_network.ip.clone(), live_network.port as u16));

        tracing::debug!("Set up live datafeed node on port: {}", live_network.port);
        let node = builder.build().await.context(PublishSnafu)?;

        Ok(Self {
            config,
            topics,
            live_node: node,
            active_feeds: HashSet::new(),
        })
    }

    fn get_topic_list(
        topic_config: &TopicConfig,
        provider: Option<&DatafeedProvider>,
        symbol: Option<&str>,
    ) -> Vec<TopicMtype> {
        let provider_str = if let Some(p) = provider {
            p.to_string()
        } else {
            String::new()
        };

        let symbol_str = if let Some(p) = symbol {
            p.to_string()
        } else {
            String::new()
        };

        vec![
            TopicMtype {
                mtype: MessageType::Tick.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("tick"),
                    provider_str,
                    symbol_str
                ),
            },
            TopicMtype {
                mtype: MessageType::Candle.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("candle"),
                    provider_str,
                    symbol_str
                ),
            },
            TopicMtype {
                mtype: MessageType::DatafeedComplete.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("datafeed_complete"),
                    provider_str,
                    symbol_str
                ),
            },
        ]
    }

    fn parse_config(config_path: &str) -> Result<NinjaTraderConfig, DatafeedError> {
        let f = std::fs::read_to_string(&config_path).context(ReadFileToStringSnafu {
            filename: config_path,
        })?;
        let config: NinjaTraderConfig = serde_yaml::from_str(&f).context(SerdeYamlSnafu {
            filename: config_path,
        })?;

        Ok(config)
    }
}
#[async_trait]
impl Subscribe<RunStrategy> for NinjaTraderService {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        for dfs in &msg.start_datafeed.as_ref().unwrap().datafeed {
            let provider: DatafeedProvider = DatafeedProvider::try_from(dfs.provider)?;
            if provider == DatafeedProvider::NinjaTrader && !self.active_feeds.contains(&dfs.symbol)
            {
                let topics = Self::get_topic_list(&self.topics, Some(&provider), Some(&dfs.symbol));
                let mut tick_publisher = None;
                let mut candle_publisher = None;
                let mut complete_publisher = None;

                for t in topics {
                    match t.mtype {
                        MessageType::Tick => {
                            tick_publisher = Some(self.live_node.new_publisher(&t.topic).await?)
                        }
                        MessageType::Candle => {
                            candle_publisher = Some(self.live_node.new_publisher(&t.topic).await?)
                        }
                        MessageType::DatafeedComplete => {
                            complete_publisher = Some(self.live_node.new_publisher(&t.topic).await?)
                        }
                        _ => (),
                    };
                }

                let candle_periods = dfs.candle_streams.iter().map(|c| c.period_s).collect();

                let ninja_trader = NinjaTrader::new(
                    candle_periods,
                    tick_publisher,
                    candle_publisher,
                    complete_publisher,
                )?;
            }
        }
        Ok(())
    }
}

pub struct NinjaTrader {
    candle_periods: Vec<u32>,
    // Publishers
    tick_publisher: Option<Publisher<Tick>>,
    candle_publisher: Option<Publisher<Candle>>,
    complete_publisher: Option<Publisher<DatafeedComplete>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct NinjaTraderConfig {
    pub topics: HashMap<String, String>,
}

impl Default for NinjaTraderConfig {
    fn default() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }
}

impl NinjaTrader {
    pub fn new(
        candle_periods: Vec<u32>,
        tick_publisher: Option<Publisher<Tick>>,
        candle_publisher: Option<Publisher<Candle>>,
        complete_publisher: Option<Publisher<DatafeedComplete>>,
    ) -> Result<Self, DatafeedError> {
        Ok(Self {
            candle_periods,
            tick_publisher,
            candle_publisher,
            complete_publisher,
        })
    }
}

#[async_trait]
impl Subscribe<Tick> for NinjaTrader {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        tracing::debug!("Tick timestamp: {}", msg.timestamp_ns);
        Ok(())
    }
}

#[async_trait]
impl Subscribe<DatafeedComplete> for NinjaTrader {
    async fn on_data(&mut self, msg: DatafeedComplete) -> Result<(), SubscriberError> {
        tracing::debug!("DatafeedComplete timestamp: {}", msg.timestamp_ns);
        Ok(())
    }
}

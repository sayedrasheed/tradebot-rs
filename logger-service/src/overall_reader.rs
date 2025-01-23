use common::config::{TopicConfig, TopicMtype};
use prost::Message;
use snafu::{ResultExt, Snafu};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::order::OrderStatus;
use tradebot_protos::messages::{
    AbortLoggerSubscriber, Advice, AlgoChart, AlgoComplete, Candle, Chart, DatafeedComplete,
    ExecutionMode, HistoricalData, Line, Network, Order, OrderFilled, OrderList, OverallDayStats,
    OverallDayStatsList, OverallFromLogRequest, OverallStats, Point, PositionStats,
    ReadFromDirRequest, ReadFromDirResponse, Realized, Rectangle, Strategy, StrategyFromLogRequest,
    Topic, TotalPnl, TotalPnlRealized, Value,
};
use walkdir::WalkDir;
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
pub enum OverallReaderError {
    #[snafu(display("Unable to open"))]
    JsonParserError { source: std::io::Error },

    #[snafu(display("Unable to open {filename}"))]
    YamlStringError {
        source: serde_yaml::Error,
        filename: String,
    },

    #[snafu(display("Zenoh config error in chart service"))]
    ZenohConfigError { source: NodeError },

    #[snafu(display("Error setting up logging subscriber"))]
    LoggingSubscriberError { source: NodeError },
}

pub struct OverallReader {
    log_dir: String,
    logger_ip: String,
    logger_port: Option<u16>,
    topics: Vec<Topic>,
    overall_stats_publisher: Option<Publisher<OverallDayStatsList>>,
}

impl OverallReader {
    pub async fn new(
        logger_ip: &str,
        logger_port: Option<u16>,
        topic_config: &TopicConfig,
    ) -> Result<Self, SubscriberError> {
        let mut overall_stats_publisher = None;
        let topics = Self::get_log_topic_list(topic_config);
        if let Some(port) = logger_port {
            tracing::debug!(
                "Set up strategy reader on ip: {}, port: {}",
                logger_ip,
                port
            );

            let mut builder = NodeBuilder::new();
            builder.set_network((logger_ip.to_string(), port));

            let node = builder.build().await.context(LoggingSubscriberSnafu)?;

            for topic in &topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::OverallDayStatsList => {
                        overall_stats_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    _ => {}
                }
            }
        } else {
            tracing::warn!("Strategy reader port selection failed, won't be able to read from log");
        }

        Ok(Self {
            log_dir: String::new(),
            logger_ip: logger_ip.to_string(),
            topics,
            logger_port,
            overall_stats_publisher,
        })
    }

    pub async fn overall_from_log(
        &self,
        msg: OverallFromLogRequest,
    ) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Read strategy from log, log dir: {}, batch_id: {}",
            self.log_dir,
            msg.batch_id
        );
        let path: &Path = Path::new(&self.log_dir);
        let metadata_file = path.join("batch_metadata.json");

        if metadata_file.exists() {
            let directory = PathBuf::from(path);
            let overall_stats_bytes: Vec<u8> =
                std::fs::read(directory.join("overall_day_stats.bin"))?;
            let mut iter = 0;

            tracing::debug!(
                "Read overall stats num bytes: {}",
                overall_stats_bytes.len()
            );
            let mut overall_day_stats_list = Vec::new();

            // Read overall stats by day from bin file and publish
            while iter < overall_stats_bytes.len() {
                let overall_day_stats: OverallDayStats =
                    prost::Message::decode_length_delimited(&mut &overall_stats_bytes[iter..])?;
                iter += overall_day_stats.encoded_len()
                    + prost::length_delimiter_len(overall_day_stats.encoded_len());
                overall_day_stats_list.push(overall_day_stats);
            }

            if let Some(overall_stats_publisher) = &self.overall_stats_publisher {
                overall_stats_publisher
                    .publish(OverallDayStatsList {
                        timestamp_ns: 0,
                        overall_day_stats_list,
                    })
                    .await?;
            }
        }

        Ok(())
    }

    pub fn set_log_dir(&mut self, log_dir: &str) {
        self.log_dir = log_dir.to_string();
    }

    pub fn get_log_topic_list(topic_config: &TopicConfig) -> Vec<Topic> {
        vec![Topic {
            mtype: MessageType::OverallDayStatsList.into(),
            topic: topic_config.get("overall_day_stats_list"),
        }]
    }
}

use common::config::{TopicConfig, TopicMtype};
use prost::Message;
use snafu::{ResultExt, Snafu};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::order::OrderStatus;
use tradebot_protos::messages::{
    AbortLoggerSubscriber, Advice, AlgoChart, AlgoComplete, Batch, Candle, Chart, DatafeedComplete,
    ExecutionMode, HistoricalData, Line, Network, Order, OrderFilled, OrderList, PnlValue, Point,
    PositionStats, ReadFromDirRequest, ReadFromDirResponse, Realized, Rectangle, Strategy,
    StrategyFromLogRequest, Topic, TotalPnl, TotalPnlRealized,
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
pub enum StrategyReaderError {
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

pub struct StrategyReader {
    log_dir: String,
    logger_ip: String,
    logger_port: Option<u16>,
    topics: Vec<Topic>,
    algo_chart_publisher: Option<Publisher<AlgoChart>>,
    chart_publisher: Option<Publisher<Chart>>,
    position_stats_publisher: Option<Publisher<PositionStats>>,
    total_pnl_realized_publisher: Option<Publisher<TotalPnlRealized>>,
    total_pnl_publisher: Option<Publisher<TotalPnl>>,
    order_list_publisher: Option<Publisher<OrderList>>,
}

impl StrategyReader {
    pub async fn new(
        logger_ip: &str,
        logger_port: Option<u16>,
        topic_config: &TopicConfig,
    ) -> Result<Self, SubscriberError> {
        let mut algo_chart_publisher = None;
        let mut chart_publisher = None;
        let mut position_stats_publisher = None;
        let mut total_pnl_realized_publisher = None;
        let mut total_pnl_publisher = None;
        let mut order_list_publisher = None;
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
                    MessageType::Chart => {
                        chart_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    MessageType::AlgoChart => {
                        algo_chart_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    MessageType::PositionStats => {
                        position_stats_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    MessageType::TotalPnlRealized => {
                        total_pnl_realized_publisher =
                            Some(node.new_publisher(&topic.topic).await?);
                    }
                    MessageType::TotalPnl => {
                        total_pnl_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    MessageType::OrderList => {
                        order_list_publisher = Some(node.new_publisher(&topic.topic).await?);
                    }
                    _ => {}
                }
            }
        } else {
            tracing::debug!(
                "Strategy reader port selection failed, won't be able to read from log"
            );
        }

        Ok(Self {
            log_dir: String::new(),
            logger_ip: logger_ip.to_string(),
            topics,
            logger_port,
            chart_publisher,
            algo_chart_publisher,
            position_stats_publisher,
            total_pnl_publisher,
            order_list_publisher,
            total_pnl_realized_publisher,
        })
    }

    pub fn strategies_from_dir(
        &mut self,
        msg: ReadFromDirRequest,
    ) -> Result<ReadFromDirResponse, SubscriberError> {
        self.log_dir = msg.log_dir.clone();
        let mut response = ReadFromDirResponse::default();

        if let Some(_) = &self.logger_port {
            response = ReadFromDirResponse {
                timestamp_ns: 0,
                log_dir: msg.log_dir.clone(),
                batches: Vec::new(),
            };

            // Walk the logging dir to find all batch metadata files to accumulate all
            // batches and strategies from the request directory
            for entry in WalkDir::new(Path::new(&msg.log_dir))
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let path = entry.path();
                    match path.file_name() {
                        Some(filename) => {
                            tracing::debug!("Batch folder filename: {:?}", filename);
                            if filename == "batch_metadata.json" {
                                let f =
                                    std::fs::read_to_string(&path).context(JsonParserSnafu {})?;
                                let batch_id: String = serde_json::from_str(&f).unwrap();

                                tracing::debug!("Add batch id to response: {}", batch_id);
                                let mut batch = Batch {
                                    batch_id,
                                    strategies: Vec::new(),
                                };

                                for entry in WalkDir::new(Path::new(path.parent().unwrap()))
                                    .into_iter()
                                    .filter_map(|e| e.ok())
                                {
                                    if entry.file_type().is_file() {
                                        let path = entry.path();
                                        match path.file_name() {
                                            Some(filename) => {
                                                tracing::debug!(
                                                    "Strategy folder filename: {:?}",
                                                    filename
                                                );
                                                if filename == "strategy_metadata.json" {
                                                    let f = std::fs::read_to_string(&path)
                                                        .context(JsonParserSnafu {})?;
                                                    let strategy: Strategy =
                                                        serde_json::from_str(&f).unwrap();

                                                    tracing::debug!(
                                                        "Add strategy id to response: {}",
                                                        strategy.strategy_id
                                                    );
                                                    batch.strategies.push(strategy);
                                                }
                                            }
                                            None => (),
                                        }
                                    }
                                }

                                if batch.strategies.len() > 0 {
                                    response.batches.push(batch);
                                }
                            }
                        }
                        None => (),
                    }
                }
            }
        } else {
            tracing::debug!("Strategy reader port selection failed, unable to read from log");
        }

        Ok((response))
    }

    pub async fn strategy_from_log(
        &self,
        msg: StrategyFromLogRequest,
    ) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Read strategy from log, log dir: {}, strategy_id: {}",
            self.log_dir,
            msg.strategy_id
        );

        // Read all the strategy protobuf bin files and publish for app viewing
        let path: &Path = Path::new(&self.log_dir);
        let metadata_file = path.join("strategy_metadata.json");

        let directory = if metadata_file.exists() {
            PathBuf::from(path)
        } else {
            path.join(msg.strategy_id.clone())
        };

        let mut chart = Chart {
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            current_ohlcv: None,
            ohlcv: Vec::new(),
        };
        let mut algo_chart = AlgoChart {
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            lines: HashMap::new(),
            rectangles: Vec::new(),
            advices: Vec::new(),
        };

        tracing::debug!("Read historical_data");
        let hd_bytes: Vec<u8> = std::fs::read(directory.join("historical_data.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read historical_data num bytes: {}", hd_bytes.len());

        while iter < hd_bytes.len() {
            let hd: HistoricalData =
                prost::Message::decode_length_delimited(&mut &hd_bytes[iter..])?;
            iter += hd.encoded_len() + prost::length_delimiter_len(hd.encoded_len());

            if hd.symbol == msg.symbol && hd.period_s == msg.period_s {
                for ohclv in hd.ohlcv {
                    chart.ohlcv.push(ohclv);
                }
            }
        }

        let candles_bytes: Vec<u8> = std::fs::read(directory.join("candles.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read candles num bytes: {}", candles_bytes.len());

        while iter < candles_bytes.len() {
            let candle: Candle =
                prost::Message::decode_length_delimited(&mut &candles_bytes[iter..])?;
            iter += candle.encoded_len() + prost::length_delimiter_len(candle.encoded_len());

            if candle.symbol == msg.symbol && candle.period_s == msg.period_s {
                if let Some(ohclv) = candle.ohlcv {
                    chart.ohlcv.push(ohclv);
                }
            }
        }

        let lines_bytes: Vec<u8> = std::fs::read(directory.join("lines.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read lines num bytes: {}", lines_bytes.len());

        while iter < lines_bytes.len() {
            let line: Line = prost::Message::decode_length_delimited(&mut &lines_bytes[iter..])?;
            iter += line.encoded_len() + prost::length_delimiter_len(line.encoded_len());

            if line.symbol == msg.symbol && line.period_s == msg.period_s {
                algo_chart.lines.insert(line.description.clone(), line);
            }
        }

        let points_bytes: Vec<u8> = std::fs::read(directory.join("points.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read points num bytes: {}", points_bytes.len());

        while iter < points_bytes.len() {
            let point: Point = prost::Message::decode_length_delimited(&mut &points_bytes[iter..])?;
            iter += point.encoded_len() + prost::length_delimiter_len(point.encoded_len());

            if point.symbol == msg.symbol && point.period_s == msg.period_s {
                let line = algo_chart
                    .lines
                    .entry(point.description.clone())
                    .or_insert(Line {
                        description: point.description.clone(),
                        symbol: point.symbol.clone(),
                        period_s: point.period_s,
                        pane_idx: point.pane_idx,
                        points: vec![],
                        color: None,
                    });

                line.points.push(point.value.unwrap());
            }
        }

        let advice_bytes: Vec<u8> = std::fs::read(directory.join("advices.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read algo orders num bytes: {}", advice_bytes.len());

        while iter < advice_bytes.len() {
            let advice: Advice =
                prost::Message::decode_length_delimited(&mut &advice_bytes[iter..])?;
            iter += advice.encoded_len() + prost::length_delimiter_len(advice.encoded_len());

            if advice.symbol == msg.symbol {
                algo_chart.advices.push(advice);
            }
        }

        let mut order_list = OrderList {
            timestamp_ns: 0,
            orders: vec![],
        };

        let mut order_id_idx_map = HashMap::new();
        let order_bytes: Vec<u8> = std::fs::read(directory.join("orders.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read orders num bytes: {}", order_bytes.len());

        while iter < order_bytes.len() {
            let order: Order = prost::Message::decode_length_delimited(&mut &order_bytes[iter..])?;
            iter += order.encoded_len() + prost::length_delimiter_len(order.encoded_len());

            order_id_idx_map.insert(order.order_id, order_list.orders.len());
            order_list.orders.push(order);
        }

        let order_fills_bytes: Vec<u8> = std::fs::read(directory.join("order_fills.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read order_fills num bytes: {}", order_fills_bytes.len());

        while iter < order_fills_bytes.len() {
            let order_filled: OrderFilled =
                prost::Message::decode_length_delimited(&mut &order_fills_bytes[iter..])?;
            iter += order_filled.encoded_len()
                + prost::length_delimiter_len(order_filled.encoded_len());

            if let Some(order_idx) = order_id_idx_map.get(&order_filled.order_id) {
                order_list.orders[*order_idx].filled_price = order_filled.price;
                order_list.orders[*order_idx].order_status = OrderStatus::Filled.into();
            }
        }

        let realized_bytes: Vec<u8> = std::fs::read(directory.join("realized.bin"))?;
        let mut iter = 0;

        tracing::debug!("Read realized num bytes: {}", realized_bytes.len());

        let mut total_realized = 0.0_f32;
        let mut realized_points = Vec::new();

        while iter < realized_bytes.len() {
            let realized: Realized =
                prost::Message::decode_length_delimited(&mut &realized_bytes[iter..])?;
            iter += realized.encoded_len() + prost::length_delimiter_len(realized.encoded_len());

            total_realized += realized.realized;
            realized_points.push(PnlValue {
                timestamp_ns: realized.timestamp_ns,
                value: total_realized,
            });
        }

        let position_stats_bytes: Vec<u8> = std::fs::read(directory.join("position_stats.bin"))?;
        let mut iter = 0;

        tracing::debug!(
            "Read position_stats num bytes: {}",
            position_stats_bytes.len()
        );

        let mut position_stats = PositionStats::default();

        while iter < position_stats_bytes.len() {
            let ps: PositionStats =
                prost::Message::decode_length_delimited(&mut &position_stats_bytes[iter..])?;
            iter += ps.encoded_len() + prost::length_delimiter_len(ps.encoded_len());

            position_stats = ps;
        }

        // Publish datafeed chart generated from the strategy logs
        if let Some(chart_publisher) = &self.chart_publisher {
            chart_publisher.publish(chart).await?;
        }

        // Publish algofeed chart generated from the strategy logs
        if let Some(algo_chart_publisher) = &self.algo_chart_publisher {
            algo_chart_publisher.publish(algo_chart).await?;
        }

        // Publish the order list generated from the strategy logs
        if let Some(order_list_publisher) = &self.order_list_publisher {
            order_list_publisher.publish(order_list).await?;
        }

        // Publish the pnl data generated from the strategy logs
        if let Some(total_pnl_publisher) = &self.total_pnl_publisher {
            total_pnl_publisher
                .publish(TotalPnl {
                    timestamp_ns: 0,
                    points: realized_points,
                })
                .await?;
        }

        if let Some(total_pnl_realized_publisher) = &self.total_pnl_realized_publisher {
            total_pnl_realized_publisher
                .publish(TotalPnlRealized {
                    timestamp_ns: 0,
                    value: Some(PnlValue {
                        timestamp_ns: 0,
                        value: total_realized,
                    }),
                    position_id: 0,
                })
                .await?;
        }

        if let Some(position_stats_publisher) = &self.position_stats_publisher {
            tracing::debug!("Publish position stats");
            position_stats_publisher.publish(position_stats).await?;
        }
        Ok(())
    }

    pub fn get_log_topic_list(topic_config: &TopicConfig) -> Vec<Topic> {
        vec![
            Topic {
                mtype: MessageType::Chart.into(),
                topic: topic_config.get("chart"),
            },
            Topic {
                mtype: MessageType::AlgoChart.into(),
                topic: topic_config.get("algo_chart"),
            },
            Topic {
                mtype: MessageType::PositionStats.into(),
                topic: topic_config.get("position_stats"),
            },
            Topic {
                mtype: MessageType::TotalPnlRealized.into(),
                topic: topic_config.get("total_pnl_realized"),
            },
            Topic {
                mtype: MessageType::TotalPnl.into(),
                topic: topic_config.get("total_pnl"),
            },
            Topic {
                mtype: MessageType::OrderList.into(),
                topic: topic_config.get("order_list"),
            },
            Topic {
                mtype: MessageType::OverallStats.into(),
                topic: topic_config.get("overall_stats"),
            },
        ]
    }
}

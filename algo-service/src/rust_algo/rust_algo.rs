use crate::algo::{AlgoError, AlgoSetup};
use common::config::TopicMtype;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Advice, AlgoComplete, Candle, DatafeedComplete, HistoricalData, Line, Point, Tick, Value,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;
use std::collections::HashMap;
use ta::indicators::{ExponentialMovingAverage, RelativeStrengthIndex};
use ta::Next;

pub struct RustAlgoSetup {
    config_path: String,
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
}

impl RustAlgoSetup {
    pub fn new() -> Self {
        Self {
            config_path: String::new(),
            advice_publisher: None,
            line_publisher: None,
            point_publisher: None,
            complete_publisher: None,
        }
    }
}

#[async_trait]
impl AlgoSetup for RustAlgoSetup {
    fn set_config_path(&mut self, config_path: &str) {
        self.config_path = config_path.to_string();
    }

    fn set_algo_complete_publisher(&mut self, publisher: Publisher<AlgoComplete>) {
        self.complete_publisher = Some(publisher);
    }

    async fn setup_publishers(
        &mut self,
        node: &Node,
        topics: &Vec<TopicMtype>,
    ) -> Result<(), AlgoError> {
        for topic in topics {
            match topic.mtype {
                MessageType::Advice => {
                    self.advice_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                MessageType::Line => {
                    self.line_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                MessageType::Point => {
                    self.point_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                _ => (),
            }
        }
        Ok(())
    }

    async fn setup_subscribers(
        self,
        subscribe_node: &Node,
        subscribe_topics: &Vec<TopicMtype>,
    ) -> Result<Box<dyn Abort>, AlgoError> {
        let ta = RustAlgo::new(
            None,
            self.advice_publisher,
            self.line_publisher,
            self.point_publisher,
            self.complete_publisher,
        )?;
        let mut subscriber = subscribe_node.new_subscriber(ta).await?;

        for topic in subscribe_topics {
            match topic.mtype {
                MessageType::Tick => {
                    tracing::debug!("Subscribe to tick message, topic: {}", &topic.topic);
                    subscribe_node
                        .subscribe::<Tick>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::Candle => {
                    tracing::debug!("Subscribe to candle message, topic: {}", &topic.topic);
                    subscribe_node
                        .subscribe::<Candle>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::HistoricalData => {
                    tracing::debug!(
                        "Subscribe to historical data message, topic: {}",
                        &topic.topic
                    );
                    subscribe_node
                        .subscribe::<HistoricalData>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::DatafeedComplete => {
                    tracing::debug!(
                        "Subscribe to datafeed complete message, topic: {}",
                        &topic.topic
                    );
                    subscribe_node
                        .subscribe::<DatafeedComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                _ => (),
            }
        }
        Ok(Box::new(subscriber))
    }
}
#[derive(Debug, Snafu)]
pub enum RustAlgoError {
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

    #[snafu(display("Failed to publish data"))]
    PublishError { source: NodeError },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RustAlgoConfig {
    topic_prefix: String,
}

pub struct RustAlgo {
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
    emas: HashMap<u32, ExponentialMovingAverage>,
    rsi: HashMap<u32, RelativeStrengthIndex>,
    reverse: i32,
    last_timestamp: u64,
    count: i32,
}

impl RustAlgo {
    pub fn new(
        config_path: Option<&String>,
        advice_publisher: Option<Publisher<Advice>>,
        line_publisher: Option<Publisher<Line>>,
        point_publisher: Option<Publisher<Point>>,
        complete_publisher: Option<Publisher<AlgoComplete>>,
    ) -> Result<Self, AlgoError> {
        let _config = if let Some(config) = &config_path {
            let f = std::fs::read_to_string(&config)
                .context(ReadFileToStringSnafu { filename: *config })?;
            let config: RustAlgoConfig =
                serde_yaml::from_str(&f).context(SerdeYamlSnafu { filename: *config })?;

            Some(config)
        } else {
            None
        };

        let mut emas = HashMap::new();
        emas.insert(60 as u32, ExponentialMovingAverage::new(14).unwrap());
        emas.insert(300 as u32, ExponentialMovingAverage::new(14).unwrap());

        let mut rsi = HashMap::new();
        rsi.insert(60 as u32, RelativeStrengthIndex::new(14)?);
        rsi.insert(300 as u32, RelativeStrengthIndex::new(14)?);

        Ok(Self {
            advice_publisher,
            line_publisher,
            point_publisher,
            complete_publisher,
            emas,
            rsi,
            reverse: 1,
            last_timestamp: 0,
            count: 0,
        })
    }
}

#[async_trait]
impl Subscribe<Tick> for RustAlgo {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Candle> for RustAlgo {
    async fn on_data(&mut self, msg: Candle) -> Result<(), SubscriberError> {
        // Handle new incoming Candle

        // Calculate new EMA point from new candle and publish
        if let Some(ema) = self.emas.get_mut(&msg.period_s) {
            if let Some(ohlcv) = &msg.ohlcv {
                let close = ohlcv.close as f64;
                let timestamp = ohlcv.timestamp_ns;
                let val = ema.next(close);
                let point = Point {
                    description: format!("ema-14"),
                    symbol: msg.symbol.clone(),
                    period_s: msg.period_s,
                    pane_idx: 0,
                    value: Some(Value {
                        timestamp_ns: timestamp,
                        value: val as f32,
                    }),
                };

                if let Some(publisher) = &self.point_publisher {
                    publisher.publish(point).await?;
                }
            }
        }

        // Calculate new RSI point from new candle and publish
        if let Some(rsi) = self.rsi.get_mut(&msg.period_s) {
            if let Some(ohlcv) = &msg.ohlcv {
                let close = ohlcv.close as f64;
                let timestamp = ohlcv.timestamp_ns;
                let val = rsi.next(close);
                let point = Point {
                    description: format!("rsi"),
                    symbol: msg.symbol.clone(),
                    period_s: msg.period_s,
                    pane_idx: 1,
                    value: Some(Value {
                        timestamp_ns: timestamp,
                        value: val as f32,
                    }),
                };

                if let Some(publisher) = &self.point_publisher {
                    publisher.publish(point).await?;
                }
            }
        }

        // Just to simulate orders
        if self.count >= 400 && self.count % 200 == 0 && self.count < 1200 {
            if let Some(ohlcv) = &msg.ohlcv {
                let close = ohlcv.close;
                if let Some(publisher) = &self.advice_publisher {
                    tracing::debug!(
                        "Publish order timestamp: {} at price: {}",
                        ohlcv.timestamp_ns,
                        close
                    );
                    publisher
                        .publish(Advice {
                            timestamp_ns: ohlcv.timestamp_ns,
                            symbol: msg.symbol,
                            strategy_id: String::new(),
                            order_type: 0,
                            price: close,
                            size: 1 * self.reverse,
                        })
                        .await?;
                    self.reverse /= -1;
                }
            }
        }

        self.count += 1;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<HistoricalData> for RustAlgo {
    async fn on_data(&mut self, msg: HistoricalData) -> Result<(), SubscriberError> {
        tracing::debug!("Historical data message received");

        // Calculate EMA from historical data and publish
        if let Some(ema) = self.emas.get_mut(&msg.period_s) {
            let points: Vec<Value> = msg
                .ohlcv
                .iter()
                .map(|c| {
                    let val = ema.next(c.close as f64);
                    Value {
                        timestamp_ns: c.timestamp_ns,
                        value: val as f32,
                    }
                })
                .collect();

            let line = Line {
                description: format!("ema-14"),
                symbol: msg.symbol.clone(),
                period_s: msg.period_s,
                pane_idx: 0,
                points,
                color: None,
            };

            if let Some(publisher) = &self.line_publisher {
                publisher.publish(line).await?;
            }
        }

        // Calculate RSI from historical data and publish
        if let Some(rsi) = self.rsi.get_mut(&msg.period_s) {
            let points: Vec<Value> = msg
                .ohlcv
                .iter()
                .map(|c| {
                    let val = rsi.next(c.close as f64);
                    Value {
                        timestamp_ns: c.timestamp_ns,
                        value: val as f32,
                    }
                })
                .collect();

            let line = Line {
                description: format!("rsi"),
                symbol: msg.symbol.clone(),
                period_s: msg.period_s,
                pane_idx: 1,
                points,
                color: None,
            };

            if let Some(publisher) = &self.line_publisher {
                publisher.publish(line).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<DatafeedComplete> for RustAlgo {
    async fn on_data(&mut self, msg: DatafeedComplete) -> Result<(), SubscriberError> {
        // Do any shutdown procedure here

        tracing::debug!("Datafeed complete message received");
        let algo_complete = AlgoComplete {
            timestamp_ns: msg.timestamp_ns,
            algo_id: 0, // TODO: remove
        };

        if let Some(publisher) = &self.complete_publisher {
            tracing::debug!("Publish algo complete message");
            publisher.publish(algo_complete).await?;
        }

        Ok(())
    }
}

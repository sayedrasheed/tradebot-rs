use crate::algo::{AlgoError, AlgoSetup};
use common::config::TopicMtype;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Advice, AlgoComplete, Candle, DatafeedComplete, HistoricalData, Line, Point, Tick, Value,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;

// CPP bindings
use cpp_algo::bridge::root as ffi;
use cpp_algo::bridge::root::cpp_algo::CppCandle;
use cpp_algo::bridge::{
    new_algo_interface, new_historical_data_input, AlgoInterface, AlgoParams, CandleInput,
};
use cxx::UniquePtr;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;

pub struct CppAlgoSetup {
    config_path: String,
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
}

impl CppAlgoSetup {
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
impl AlgoSetup for CppAlgoSetup {
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
        let ta = CppAlgo::new(
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
pub enum CppAlgoError {
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

// Yaml config for init parameters
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CppAlgoConfig {
    rsi_period: u32,
    rsi_buy_threshold: f64,
    rsi_sell_threshold: f64,
    candle_periods: Vec<u32>,
}

pub struct CppAlgo {
    algo_interface: UniquePtr<AlgoInterface>,
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
}

impl CppAlgo {
    pub fn new(
        config_path: Option<&String>,
        advice_publisher: Option<Publisher<Advice>>,
        line_publisher: Option<Publisher<Line>>,
        point_publisher: Option<Publisher<Point>>,
        complete_publisher: Option<Publisher<AlgoComplete>>,
    ) -> Result<Self, AlgoError> {
        let algo_interface = if let Some(config) = &config_path {
            // Read config from yaml file for runtime parameters if they exist
            let f = std::fs::read_to_string(&config)
                .context(ReadFileToStringSnafu { filename: *config })?;
            let config: CppAlgoConfig =
                serde_yaml::from_str(&f).context(SerdeYamlSnafu { filename: *config })?;

            // Use algo params cpp struct
            let algo_params = AlgoParams {
                rsi_period: config.rsi_period as i32,
                candle_periods: [60, 300],
                rsi_buy_threshold: config.rsi_buy_threshold,
                rsi_sell_threshold: config.rsi_sell_threshold,
            };

            // create cpp algo interface with algo params
            new_algo_interface(&algo_params)
        } else {
            // use default params
            let algo_params = AlgoParams {
                rsi_period: 14,
                candle_periods: [60, 300],
                rsi_buy_threshold: 20.0,
                rsi_sell_threshold: 75.0,
            };

            new_algo_interface(&algo_params)
        };

        Ok(Self {
            algo_interface,
            advice_publisher,
            line_publisher,
            point_publisher,
            complete_publisher,
        })
    }
}

#[async_trait]
impl Subscribe<Tick> for CppAlgo {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        // Algo doesnt do anything with ticks so do nothing this is just for tutorial
        // In real practice, in the setup_subscribers function we wouldnt subscribe to Ticks
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Candle> for CppAlgo {
    async fn on_data(&mut self, msg: Candle) -> Result<(), SubscriberError> {
        // Set up candle input for cpp handler
        let candle_input = CandleInput {
            timestamp_ns: msg.ohlcv.as_ref().unwrap().timestamp_ns,
            period_s: msg.period_s,
            close: msg.ohlcv.as_ref().unwrap().close,
        };

        let mut candle_output = Box::new(unsafe { std::mem::zeroed() });

        // call cpp algo interface candle handler
        self.algo_interface
            .pin_mut()
            .candle_update(&candle_input, &mut candle_output);

        // Convert cpp algo interface output into app output for visualization.
        // In this case, output is RSI point for line series
        let point = Point {
            description: format!("rsi"),
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            pane_idx: 1,
            value: Some(Value {
                timestamp_ns: msg.ohlcv.as_ref().unwrap().timestamp_ns,
                value: candle_output.rsi.value as f32,
            }),
        };

        if let Some(publisher) = &self.point_publisher {
            publisher.publish(point).await?;
        }

        // Candle output also has an optional advice when algo advises to Buy or Sell
        if candle_output.curr_advice.is_valid {
            // Convert cpp algo interface advice into app advice
            if let Some(publisher) = &self.advice_publisher {
                tracing::debug!(
                    "Publish order timestamp: {} at price: {}",
                    candle_output.curr_advice.timestamp_ns,
                    candle_output.curr_advice.price
                );
                publisher
                    .publish(Advice {
                        timestamp_ns: candle_output.curr_advice.timestamp_ns,
                        symbol: msg.symbol,
                        strategy_id: String::new(),
                        order_type: 0,
                        price: candle_output.curr_advice.price as f32,
                        size: match candle_output.curr_advice.side {
                            ffi::cpp_algo::Side_BUY => candle_output.curr_advice.size,
                            ffi::cpp_algo::Side_SELL => candle_output.curr_advice.size / -1,
                            _ => unreachable!(),
                        },
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<HistoricalData> for CppAlgo {
    async fn on_data(&mut self, msg: HistoricalData) -> Result<(), SubscriberError> {
        // Set up historical data input to cpp handler
        let mut hd_input = new_historical_data_input();
        hd_input.period_s = msg.period_s;
        hd_input.num_candles = msg.ohlcv.len() as u32;

        for (i, ohlcv) in msg.ohlcv.iter().enumerate() {
            hd_input.historical_candles[i] = CppCandle {
                timestamp_ns: ohlcv.timestamp_ns,
                close: ohlcv.close as f64,
            }
        }

        let mut hd_output = Box::new(unsafe { std::mem::zeroed() });

        // call cpp algo interface historical data handler
        self.algo_interface
            .pin_mut()
            .historical_data_update(&hd_input, &mut hd_output);

        // Convert cpp algo interface output into app output for visualization.
        // In this case, output is RSI line series
        let mut points = Vec::new();
        for i in 0..hd_output.num_rsi as usize {
            points.push(Value {
                timestamp_ns: hd_output.historical_rsi[i].timestamp_ns,
                value: hd_output.historical_rsi[i].value as f32,
            });
        }

        let line = Line {
            description: format!("rsi"),
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            pane_idx: 1,
            points,
            color: Some(String::from("#a83291")),
        };

        if let Some(publisher) = &self.line_publisher {
            publisher.publish(line).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<DatafeedComplete> for CppAlgo {
    async fn on_data(&mut self, msg: DatafeedComplete) -> Result<(), SubscriberError> {
        // Do any shutdown procedure here

        tracing::debug!("Datafeed complete message received");
        let algo_complete = AlgoComplete {
            timestamp_ns: msg.timestamp_ns,
            algo_id: 0, // TODO: not needed, remove
        };

        if let Some(publisher) = &self.complete_publisher {
            tracing::debug!("Publish algo complete message");
            publisher.publish(algo_complete).await?;
        }

        Ok(())
    }
}

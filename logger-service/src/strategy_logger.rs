use prost::Message;
use snafu::{ResultExt, Snafu};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortLoggerSubscriber, Advice, AlgoComplete, Candle, DatafeedComplete, ExecutionMode,
    HistoricalData, Line, Order, OrderFilled, OrderfeedComplete, PnlComplete, Point, PositionStats,
    Realized, Rectangle, StartDatafeed, Strategy, SymbolPeriods, Topic,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
pub enum StrategyLoggerError {
    #[snafu(display("Unable to open {filename}"))]
    YamlParserError {
        source: std::io::Error,
        filename: String,
    },

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

pub struct StrategyLogger {
    logger_id: u32,
    files: HashMap<String, File>,
    abort_publisher: Publisher<AbortLoggerSubscriber>,
}

impl StrategyLogger {
    pub fn new(
        logging_path: &str,
        batch_id: &str,
        strategy_id: &str,
        logger_id: u32,
        topics: &Vec<Topic>,
        abort_publisher: Publisher<AbortLoggerSubscriber>,
        df: Option<&StartDatafeed>,
    ) -> Result<Self, SubscriberError> {
        let path = Path::new(logging_path);

        let directory = path
            .join(batch_id.to_string())
            .join(strategy_id.to_string());
        std::fs::create_dir_all(directory.clone())?;

        let mut files = HashMap::new();

        // Strategy logger supports logging from algofeed, datafeed, orderfeed, and strategy pnl feed
        // so if these topics exist then create bin files for them for logging
        for topic in topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::Order => {
                    let file = File::create(directory.join("orders.bin"))?;
                    files.insert(String::from("orders"), file);
                }
                MessageType::OrderFilled => {
                    let file = File::create(directory.join("order_fills.bin"))?;
                    files.insert(String::from("order_fills"), file);
                }
                MessageType::Advice => {
                    let file = File::create(directory.join("advices.bin"))?;
                    files.insert(String::from("advices"), file);
                }
                MessageType::Line => {
                    let file = File::create(directory.join("lines.bin"))?;
                    files.insert(String::from("lines"), file);
                }
                MessageType::Point => {
                    let file = File::create(directory.join("points.bin"))?;
                    files.insert(String::from("points"), file);
                }
                MessageType::Rectangle => {
                    let file = File::create(directory.join("rectangles.bin"))?;
                    files.insert(String::from("rectangles"), file);
                }
                MessageType::HistoricalData => {
                    let file = File::create(directory.join("historical_data.bin"))?;
                    files.insert(String::from("historical_data"), file);
                }
                MessageType::Candle => {
                    let file = File::create(directory.join("candles.bin"))?;
                    files.insert(String::from("candles"), file);
                }
                MessageType::Realized => {
                    let file = File::create(directory.join("realized.bin"))?;

                    files.insert(String::from("realized"), file);
                }
                MessageType::PositionStats => {
                    let file = File::create(directory.join("position_stats.bin"))?;
                    files.insert(String::from("position_stats"), file);
                }
                _ => {}
            }
        }

        // Write out strategies to metadata for easier identification later
        if let Some(start_datafeed) = df {
            let mut strategy = Strategy {
                strategy_id: strategy_id.to_string(),
                symbol_periods: Vec::new(),
            };

            for df in &start_datafeed.datafeed {
                let mut sp = SymbolPeriods::default();
                sp.symbol = df.symbol.clone();
                for c in &df.candle_streams {
                    sp.period_s.push(c.period_s);
                }

                strategy.symbol_periods.push(sp);
            }

            let mut file = File::create(directory.join("strategy_metadata.json"))?;
            file.write(&serde_json::to_vec(&strategy)?)?;
        }

        Ok(Self {
            logger_id,
            files,
            abort_publisher,
        })
    }
}

#[async_trait]
impl Subscribe<Candle> for StrategyLogger {
    async fn on_data(&mut self, msg: Candle) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("candles") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find candles file pointer");
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<HistoricalData> for StrategyLogger {
    async fn on_data(&mut self, msg: HistoricalData) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("historical_data") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find historical_data file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Order> for StrategyLogger {
    async fn on_data(&mut self, msg: Order) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("orders") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find orders file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderFilled> for StrategyLogger {
    async fn on_data(&mut self, msg: OrderFilled) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("order_fills") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find order_fills file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Advice> for StrategyLogger {
    async fn on_data(&mut self, msg: Advice) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("advices") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find advices file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Point> for StrategyLogger {
    async fn on_data(&mut self, msg: Point) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("points") {
            file.write(&buf)?;
        } else {
            tracing::warn!(
                "Can't find points file pointer, ts: {}",
                msg.value.unwrap().timestamp_ns
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Line> for StrategyLogger {
    async fn on_data(&mut self, msg: Line) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("lines") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find lines file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Rectangle> for StrategyLogger {
    async fn on_data(&mut self, msg: Rectangle) -> Result<(), SubscriberError> {
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("rectangles") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find rectangles file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Realized> for StrategyLogger {
    async fn on_data(&mut self, msg: Realized) -> Result<(), SubscriberError> {
        tracing::debug!("Realized message received");
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("realized") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find realized file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<PositionStats> for StrategyLogger {
    async fn on_data(&mut self, msg: PositionStats) -> Result<(), SubscriberError> {
        tracing::debug!("PositionStats message received");
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("position_stats") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find position stats file pointer");
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<DatafeedComplete> for StrategyLogger {
    async fn on_data(&mut self, msg: DatafeedComplete) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Datafeed complete message received, id: {}",
            msg.datafeed_id
        );

        // If we get a datafeed complete message then we are logging datafeed messages only so
        // drain the map so files can close
        self.files.drain();

        let abort_logger = AbortLoggerSubscriber {
            timestamp_ns: msg.timestamp_ns,
            logger_id: self.logger_id,
        };

        tracing::debug!(
            "Publish abort logger subscriber message, id: {}",
            self.logger_id
        );
        self.abort_publisher.publish(abort_logger).await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AlgoComplete> for StrategyLogger {
    async fn on_data(&mut self, msg: AlgoComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Algo complete message received, id: {}", msg.algo_id);
        // If we get a algofeed complete message then we are logging algofeed messages only so
        // drain the map so files can close
        self.files.drain();

        let abort_logger = AbortLoggerSubscriber {
            timestamp_ns: msg.timestamp_ns,
            logger_id: self.logger_id,
        };

        tracing::debug!(
            "Publish abort logger subscriber message, id: {}",
            self.logger_id
        );
        self.abort_publisher.publish(abort_logger).await?;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderfeedComplete> for StrategyLogger {
    async fn on_data(&mut self, msg: OrderfeedComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Order complete message received, id: {}", msg.strategy_id);
        // If we get a orderfeed complete message then we are logging orderfeed messages only so
        // drain the map so files can close
        self.files.drain();

        let abort_logger = AbortLoggerSubscriber {
            timestamp_ns: msg.timestamp_ns,
            logger_id: self.logger_id,
        };

        tracing::debug!(
            "Publish abort logger subscriber message, id: {}",
            self.logger_id
        );
        self.abort_publisher.publish(abort_logger).await?;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<PnlComplete> for StrategyLogger {
    async fn on_data(&mut self, msg: PnlComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Pnl complete message received, id: {}", msg.strategy_id);
        // If we get a pnlfeed complete message then we are logging pnlfeed messages only so
        // drain the map so files can close
        self.files.drain();

        let abort_logger = AbortLoggerSubscriber {
            timestamp_ns: msg.timestamp_ns,
            logger_id: self.logger_id,
        };

        tracing::debug!(
            "Publish abort logger subscriber message, id: {}",
            self.logger_id
        );
        self.abort_publisher.publish(abort_logger).await?;
        Ok(())
    }
}

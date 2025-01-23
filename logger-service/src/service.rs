use crate::config::Config;
use crate::overall_logger::OverallLogger;
use crate::overall_reader::OverallReader;
use crate::strategy_logger::StrategyLogger;
use crate::strategy_reader::StrategyReader;
use common::config::{ServiceConfig, TopicConfig};
use common::utils;
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortLoggerSubscriber, AbortOverallPnlSubscriber, Advice, AlgoComplete, Candle,
    DatafeedComplete, HistoricalData, JoinAlgofeed, JoinAlgofeedSuccess, JoinDatafeed,
    JoinDatafeedSuccess, JoinLoggerfeed, JoinOrderfeed, JoinOrderfeedSuccess, JoinOverallPnlfeed,
    JoinStrategyPnlfeed, Line, Network, Order, OrderFilled, OrderfeedComplete, OverallDayStats,
    OverallFromLogRequest, OverallPnlComplete, OverallStats, PnlComplete, Point, PositionStats,
    ReadFromDirRequest, ReadFromDirResponse, Realized, Rectangle, RunStrategy,
    ServiceAvailableRequest, ServiceAvailableResponse, StartDatafeed, StrategyFromLogRequest,
    Topic,
};
use utils::create_node;
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

use async_trait::async_trait;

const SERVICE_ID: &str = "logging-service";
const SERVICES_LOGGERFEED_LIST: &[&str] = &["app-service"];

#[derive(Debug, Snafu)]
pub enum LoggingServiceError {
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

    #[snafu(display("Error setting up strategy log reader"))]
    StrategyReaderError { source: SubscriberError },
}

pub struct LoggingService {
    subscriber: Subscriber<LoggingSubscriber>,
}

impl LoggingService {
    pub async fn new(config: &Config) -> Result<Self, LoggingServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating logging service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        tracing::debug!("Joining service network on port: {}", port);
        builder.set_network((ip.clone(), port));
        let node = builder.build().await.context(LoggingSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        // Create publishers on the service node. Use topics from service config if they exist, otherwise use hard coded topic
        let join_loggerfeed_publisher = node
            .new_publisher(&topics.get("join_loggerfeed"))
            .await
            .context(LoggingSubscriberSnafu)?;

        let join_df_success_publisher = node
            .new_publisher(&topics.get("join_datafeed_success"))
            .await
            .context(LoggingSubscriberSnafu)?;

        let join_algo_success_publisher = node
            .new_publisher(&topics.get("join_algofeed_success"))
            .await
            .context(LoggingSubscriberSnafu)?;

        let join_order_success_publisher = node
            .new_publisher(&topics.get("join_orderfeed_success"))
            .await
            .context(LoggingSubscriberSnafu)?;

        let strats_from_dir_publisher = node
            .new_publisher(&topics.get("read_from_dir_response"))
            .await
            .context(LoggingSubscriberSnafu)?;

        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(LoggingSubscriberSnafu)?;

        // Create logger subscriber with required publishers
        let ls = LoggingSubscriber::new(
            config,
            ip,
            port,
            join_df_success_publisher,
            join_algo_success_publisher,
            join_order_success_publisher,
            strats_from_dir_publisher,
            service_available_resp_publisher,
            join_loggerfeed_publisher,
        )
        .await?;

        let mut subscriber: Subscriber<LoggingSubscriber> = node
            .new_subscriber(ls)
            .await
            .context(LoggingSubscriberSnafu)?;

        // Subscribe to required service messages on service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Subscribing to service available request message");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to join algo message");
        node.subscribe::<JoinAlgofeed>(&topics.get("join_algofeed"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to join datafeed");
        node.subscribe::<JoinDatafeed>(&topics.get("join_datafeed"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to join orderfeed");
        node.subscribe::<JoinOrderfeed>(&topics.get("join_orderfeed"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to join strategy pnlfeed");
        node.subscribe::<JoinStrategyPnlfeed>(
            &topics.get("join_strategy_pnlfeed"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to join overall pnlfeed");
        node.subscribe::<JoinOverallPnlfeed>(&topics.get("join_overall_pnlfeed"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to strategies from directory request");
        node.subscribe::<ReadFromDirRequest>(&topics.get("read_from_dir_request"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to strategy from log request");
        node.subscribe::<StrategyFromLogRequest>(
            &topics.get("strategy_from_log_request"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to overall from log request");
        node.subscribe::<OverallFromLogRequest>(
            &topics.get("overall_from_log_request"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort logger subscriber");
        node.subscribe::<AbortLoggerSubscriber>(
            &topics.get("abort_logger_subscriber"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort overall pnl subscriber");
        node.subscribe::<AbortOverallPnlSubscriber>(
            &topics.get("abort_overall_pnl_subscriber"),
            &mut subscriber,
        )
        .await
        .context(LoggingSubscriberSnafu)?;

        tracing::debug!("Subscribe to run strategy");
        node.subscribe::<RunStrategy>(&topics.get("run_strategy"), &mut subscriber)
            .await
            .context(LoggingSubscriberSnafu)?;

        // Request for service available discovery
        let service_available_req_publisher = node
            .new_publisher::<ServiceAvailableRequest>(&topics.get("service_available_request"))
            .await
            .context(LoggingSubscriberSnafu)?;

        service_available_req_publisher
            .publish(
                (ServiceAvailableRequest {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                }),
            )
            .await
            .context(LoggingSubscriberSnafu)?;

        Ok(Self { subscriber })
    }

    pub async fn join(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.subscriber.join().await?;
        Ok(())
    }
}

// Zenoh connection feed
struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

pub struct LoggingSubscriber {
    zenoh_config_path: Option<String>,
    logging_output_path: String,
    join_loggerfeed_publisher: Publisher<JoinLoggerfeed>,
    join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
    join_algo_success_publisher: Publisher<JoinAlgofeedSuccess>,
    join_order_success_publisher: Publisher<JoinOrderfeedSuccess>,
    strats_from_dir_publisher: Publisher<ReadFromDirResponse>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    subscribers: HashMap<u32, Box<dyn Abort>>,
    strategy_log_reader: StrategyReader,
    overall_log_reader: OverallReader,
    services_joining_loggerfeed: HashSet<String>,
    batch_buffer: HashMap<String, HashMap<String, StartDatafeed>>,
    logger_id_map: HashMap<String, u32>,
    curr_logger_id: u32,
    service_node: Node,
    logger_ip: String,
    logger_port: Option<u16>,
    topics: TopicConfig,
    algofeed: Option<Feed>,
    datafeed: Option<Feed>,
    orderfeed: Option<Feed>,
    pnlfeed: Option<Feed>,
}

impl LoggingSubscriber {
    pub async fn new(
        config: &Config,
        service_ip: String,
        service_port: u16,
        join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
        join_algo_success_publisher: Publisher<JoinAlgofeedSuccess>,
        join_order_success_publisher: Publisher<JoinOrderfeedSuccess>,
        strats_from_dir_publisher: Publisher<ReadFromDirResponse>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        join_loggerfeed_publisher: Publisher<JoinLoggerfeed>,
    ) -> Result<Self, LoggingServiceError> {
        let mut builder = NodeBuilder::new();
        if let Some(config) = &config.zenoh_config_path {
            builder.set_config_path(config);
        }

        builder.set_network((service_ip, service_port));
        let service_node = builder.build().await.context(LoggingSubscriberSnafu)?;

        let topics = TopicConfig::from(&config.topics);

        // There is a single logger feed so create node on start up
        let logger_port = utils::generate_zenoh_port();
        if let Some(port) = &logger_port {
            join_loggerfeed_publisher
                .publish(JoinLoggerfeed {
                    timestamp_ns: 0,
                    network: Some(Network {
                        ip: config.logger_ip.clone(),
                        port: *port as u32,
                    }),
                    topics: Self::get_logger_topic_list(&topics),
                })
                .await
                .context(LoggingSubscriberSnafu)?;
        }

        let mut services_joining_loggerfeed = HashSet::new();
        for s in SERVICES_LOGGERFEED_LIST {
            let service_str = *s;
            services_joining_loggerfeed.insert(service_str.to_string());
        }

        Ok(Self {
            zenoh_config_path: config.zenoh_config_path.clone(),
            logging_output_path: config.logging_output_path.clone(),
            join_df_success_publisher,
            join_order_success_publisher,
            join_algo_success_publisher,
            strats_from_dir_publisher,
            service_available_resp_publisher,
            subscribers: HashMap::new(),
            strategy_log_reader: StrategyReader::new(&config.logger_ip, logger_port, &topics)
                .await
                .context(StrategyReaderSnafu)?,
            overall_log_reader: OverallReader::new(&config.logger_ip, logger_port, &topics)
                .await
                .context(StrategyReaderSnafu)?,
            batch_buffer: HashMap::new(),
            logger_id_map: HashMap::new(),
            curr_logger_id: 0,
            service_node,
            join_loggerfeed_publisher,
            services_joining_loggerfeed,
            logger_ip: config.logger_ip.clone(),
            logger_port,
            topics,
            algofeed: None,
            datafeed: None,
            orderfeed: None,
            pnlfeed: None,
        })
    }

    // Logger topic list for when there is a read from log request
    fn get_logger_topic_list(topic_config: &TopicConfig) -> Vec<Topic> {
        vec![
            Topic {
                mtype: MessageType::Chart.into(),
                topic: topic_config.get("chart"),
            },
            Topic {
                mtype: MessageType::OrderList.into(),
                topic: topic_config.get("order_list"),
            },
            Topic {
                mtype: MessageType::AlgoChart.into(),
                topic: topic_config.get("algo_chart"),
            },
            Topic {
                mtype: MessageType::TotalPnl.into(),
                topic: topic_config.get("total_pnl"),
            },
            Topic {
                mtype: MessageType::TotalPnlRealized.into(),
                topic: topic_config.get("total_pnl_realized"),
            },
            Topic {
                mtype: MessageType::PositionStats.into(),
                topic: topic_config.get("position_stats"),
            },
            Topic {
                mtype: MessageType::OverallDayStatsList.into(),
                topic: topic_config.get("overall_day_stats_list"),
            },
        ]
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for LoggingSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableRequest) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available request message received from: {}",
            msg.service_id
        );
        self.service_available_resp_publisher
            .publish(ServiceAvailableResponse {
                timestamp_ns: msg.timestamp_ns,
                service_id: SERVICE_ID.to_string(),
            })
            .await?;

        // send loggerfeed to services who subscribe from data that comes from a log
        if self.services_joining_loggerfeed.contains(&msg.service_id) {
            if let Some(port) = &self.logger_port {
                self.join_loggerfeed_publisher
                    .publish(JoinLoggerfeed {
                        timestamp_ns: 0,
                        network: Some(Network {
                            ip: self.logger_ip.clone(),
                            port: *port as u32,
                        }),
                        topics: Self::get_logger_topic_list(&self.topics),
                    })
                    .await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<RunStrategy> for LoggingSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("Start datafeed message received");
        let batch = self
            .batch_buffer
            .entry(msg.start_datafeed.as_ref().unwrap().batch_id.clone())
            .or_insert(HashMap::new());

        batch.insert(
            msg.start_datafeed.as_ref().unwrap().strategy_id.clone(),
            msg.start_datafeed.unwrap(),
        );

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinAlgofeed> for LoggingSubscriber {
    async fn on_data(&mut self, msg: JoinAlgofeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join algofeed message received");
        let af_nw = msg.network.unwrap();

        // Only create a algofeed node when we don't have one already or the join algofeed message has a
        // different ip and port as previous
        if self.algofeed.is_none()
            || self.algofeed.as_ref().unwrap().ip != af_nw.ip
            || self.algofeed.as_ref().unwrap().port != af_nw.port as u16
        {
            let algo_node = create_node(&af_nw.ip, af_nw.port as u16).await?;
            self.algofeed = Some(Feed {
                node: algo_node,
                ip: af_nw.ip.clone(),
                port: af_nw.port as u16,
            });
        }

        tracing::debug!("Set up algo zenoh node");
        let node = &self.algofeed.as_ref().unwrap().node;

        // Set up abort publisher so we can abort this logger once the algo is complete
        let abort_publisher = self
            .service_node
            .new_publisher("abort_logger_subscriber")
            .await?;

        let sl = StrategyLogger::new(
            &self.logging_output_path,
            &msg.batch_id,
            &msg.strategy_id,
            self.curr_logger_id,
            &msg.topics,
            abort_publisher,
            None,
        )?;

        let mut subscriber = node.new_subscriber(sl).await?;

        // Subscribe to the algo messages that we support the logging of
        for topic in &msg.topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::Advice => {
                    tracing::debug!("Subscribe to advice message, topic: {}", topic.topic);
                    node.subscribe::<Advice>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::Line => {
                    tracing::debug!("Subscribe to line message, topic: {}", topic.topic);
                    node.subscribe::<Line>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::Point => {
                    tracing::debug!("Subscribe to point message, topic: {}", topic.topic);
                    node.subscribe::<Point>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::Rectangle => {
                    tracing::debug!("Subscribe to rectangle message, topic: {}", topic.topic);
                    node.subscribe::<Rectangle>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::AlgoComplete => {
                    tracing::debug!("Subscribe to algo complete message, topic: {}", topic.topic);
                    node.subscribe::<AlgoComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                _ => {}
            }
        }

        self.subscribers
            .insert(self.curr_logger_id, Box::new(subscriber));

        self.curr_logger_id += 1;

        self.join_algo_success_publisher
            .publish(JoinAlgofeedSuccess {
                timestamp_ns: msg.timestamp_ns,
                service_id: SERVICE_ID.to_string(),
                strategy_id: msg.strategy_id.clone(),
                batch_id: msg.batch_id.clone(),
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinDatafeed> for LoggingSubscriber {
    async fn on_data(&mut self, msg: JoinDatafeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join datafeed message received");
        if let Some(start_datafeed_buffer) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(start_datafeed) = start_datafeed_buffer.get(&msg.strategy_id) {
                let df_nw = msg.network.as_ref().unwrap();

                // Only create a datafeed node when we don't have one already or the join datafeed message has a
                // different ip and port as previous
                if self.datafeed.is_none()
                    || self.datafeed.as_ref().unwrap().ip != df_nw.ip
                    || self.datafeed.as_ref().unwrap().port != df_nw.port as u16
                {
                    let df_node = create_node(&df_nw.ip, df_nw.port as u16).await?;
                    self.datafeed = Some(Feed {
                        node: df_node,
                        ip: df_nw.ip.clone(),
                        port: df_nw.port as u16,
                    });
                }

                let node = &self.datafeed.as_ref().unwrap().node;

                // Set up abort publisher so we can abort this logger once the datafeed is complete
                let abort_publisher = self
                    .service_node
                    .new_publisher("abort_logger_subscriber")
                    .await?;

                let sl = StrategyLogger::new(
                    &self.logging_output_path,
                    &msg.batch_id,
                    &msg.strategy_id,
                    self.curr_logger_id,
                    &msg.topics,
                    abort_publisher,
                    Some(start_datafeed),
                )?;

                let mut subscriber = node.new_subscriber(sl).await?;

                // Subscribe to the datafeed messages that we support the logging of
                // NOTE: Currently we dont log ticks this may change in the future
                for topic in &msg.topics {
                    let mtype = MessageType::try_from(topic.mtype)?;
                    match mtype {
                        MessageType::HistoricalData => {
                            tracing::debug!(
                                "Subscribe to historical data message, topic: {}",
                                topic.topic
                            );
                            node.subscribe::<HistoricalData>(&topic.topic, &mut subscriber)
                                .await
                                .context(LoggingSubscriberSnafu)?;
                        }
                        MessageType::Candle => {
                            tracing::debug!("Subscribe to candle message, topic: {}", topic.topic);
                            node.subscribe::<Candle>(&topic.topic, &mut subscriber)
                                .await
                                .context(LoggingSubscriberSnafu)?;
                        }
                        MessageType::DatafeedComplete => {
                            tracing::debug!(
                                "Subscribe to datafeed complete message, topic: {}",
                                topic.topic
                            );
                            node.subscribe::<DatafeedComplete>(&topic.topic, &mut subscriber)
                                .await
                                .context(LoggingSubscriberSnafu)?;
                        }
                        _ => {}
                    }
                }

                self.subscribers
                    .insert(self.curr_logger_id, Box::new(subscriber));

                self.curr_logger_id += 1;

                self.join_df_success_publisher
                    .publish(JoinDatafeedSuccess {
                        timestamp_ns: msg.timestamp_ns,
                        service_id: SERVICE_ID.to_string(),
                        strategy_id: msg.strategy_id.clone(),
                        batch_id: msg.batch_id.clone(),
                    })
                    .await?;
            } else {
                tracing::warn!(
                    "Can't find corressponding start datafeed message for strategy id: {}",
                    msg.strategy_id
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOrderfeed> for LoggingSubscriber {
    async fn on_data(&mut self, msg: JoinOrderfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join orderfeed message received");
        let of_nw = msg.network.unwrap();

        // Only create a orderfeed node when we don't have one already or the join algofeed message has a
        // different ip and port as previous
        if self.orderfeed.is_none()
            || self.orderfeed.as_ref().unwrap().ip != of_nw.ip
            || self.orderfeed.as_ref().unwrap().port != of_nw.port as u16
        {
            let order_node = create_node(&of_nw.ip, of_nw.port as u16).await?;
            self.orderfeed = Some(Feed {
                node: order_node,
                ip: of_nw.ip.clone(),
                port: of_nw.port as u16,
            });
        }

        tracing::debug!("Set up orderfeed zenoh node");
        let node = &self.orderfeed.as_ref().unwrap().node;

        // Set up abort publisher so we can abort this logger once the orderfeed is complete
        let abort_publisher = self
            .service_node
            .new_publisher("abort_logger_subscriber")
            .await?;

        let sl = StrategyLogger::new(
            &self.logging_output_path,
            &msg.batch_id,
            &msg.strategy_id,
            self.curr_logger_id,
            &msg.topics,
            abort_publisher,
            None,
        )?;

        let mut subscriber = node.new_subscriber(sl).await?;

        // Subscribe to the orderfeed messages that we support the logging of
        for topic in &msg.topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::Order => {
                    tracing::debug!("Subscribe to order message, topic: {}", topic.topic);
                    node.subscribe::<Order>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::OrderFilled => {
                    tracing::debug!("Subscribe to order filled message, topic: {}", topic.topic);
                    node.subscribe::<OrderFilled>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::OrderfeedComplete => {
                    tracing::debug!(
                        "Subscribe to orderfeed complete message, topic: {}",
                        topic.topic
                    );
                    node.subscribe::<OrderfeedComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                _ => {}
            }
        }

        self.subscribers
            .insert(self.curr_logger_id, Box::new(subscriber));

        self.curr_logger_id += 1;

        self.join_order_success_publisher
            .publish(JoinOrderfeedSuccess {
                timestamp_ns: msg.timestamp_ns,
                strategy_id: msg.strategy_id.clone(),
                service_id: SERVICE_ID.to_string(),
                batch_id: msg.batch_id.clone(),
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinStrategyPnlfeed> for LoggingSubscriber {
    async fn on_data(&mut self, msg: JoinStrategyPnlfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join strategy pnlfeed message received");
        let pnl_nw = msg.network.unwrap();

        // Only create a pnlfeed node when we don't have one already or the join pnlfeed message has a
        // different ip and port as previous
        if self.pnlfeed.is_none()
            || self.pnlfeed.as_ref().unwrap().ip != pnl_nw.ip
            || self.pnlfeed.as_ref().unwrap().port != pnl_nw.port as u16
        {
            tracing::debug!(
                "Set up pnl zenoh node, ip: {}, port: {}",
                pnl_nw.ip,
                pnl_nw.port
            );
            let p_node = create_node(&pnl_nw.ip, pnl_nw.port as u16).await?;
            self.pnlfeed = Some(Feed {
                node: p_node,
                ip: pnl_nw.ip.clone(),
                port: pnl_nw.port as u16,
            });
        }

        let node = &self.pnlfeed.as_ref().unwrap().node;

        // Set up abort publisher so we can abort this logger once the pnlfeed is complete
        let abort_publisher = self
            .service_node
            .new_publisher("abort_logger_subscriber")
            .await?;

        let sl = StrategyLogger::new(
            &self.logging_output_path,
            &msg.batch_id,
            &msg.strategy_id,
            self.curr_logger_id,
            &msg.topics,
            abort_publisher,
            None,
        )?;

        let mut subscriber = node.new_subscriber(sl).await?;

        // Subscribe to the pnl messages that we support the logging of
        for topic in &msg.topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::Realized => {
                    tracing::debug!("Subscribe to realized message, topic: {}", topic.topic);
                    node.subscribe::<Realized>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::PositionStats => {
                    tracing::debug!(
                        "Subscribe to position stats message, topic: {}",
                        topic.topic
                    );
                    node.subscribe::<PositionStats>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::PnlComplete => {
                    tracing::debug!("Subscribe to pnl complete message, topic: {}", topic.topic);
                    node.subscribe::<PnlComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                _ => {}
            }
        }

        self.subscribers
            .insert(self.curr_logger_id, Box::new(subscriber));

        self.curr_logger_id += 1;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOverallPnlfeed> for LoggingSubscriber {
    async fn on_data(&mut self, msg: JoinOverallPnlfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join overall pnlfeed message received");

        // Every overall pnlfeed will have its own ip and port so need to create a node every time
        // TODO: I dont think this is necessary so change in the future for them to be all on one
        let mut builder = NodeBuilder::new();
        if let Some(config) = &self.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        if let Some(network) = &msg.network {
            tracing::debug!(
                "Join overall pnlfeed on ip: {}, port: {}",
                network.ip,
                network.port
            );
            builder.set_network((network.ip.clone(), network.port as u16));
        }

        // Set up abort publisher so we can abort this logger once the pnlfeed is complete
        let abort_publisher = self
            .service_node
            .new_publisher("abort_overall_pnl_subscriber")
            .await?;
        let ol = OverallLogger::new(
            &self.logging_output_path,
            &msg.batch_id,
            &msg.topics,
            abort_publisher,
        )?;

        let node = builder.build().await.context(LoggingSubscriberSnafu)?;
        let mut subscriber = node.new_subscriber(ol).await?;

        // Subscribe to the overall pnl messages that we support the logging of
        for topic in &msg.topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::OverallDayStats => {
                    tracing::debug!(
                        "Subscribe to overall day stats message, topic: {}",
                        topic.topic
                    );
                    node.subscribe::<OverallDayStats>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                MessageType::OverallPnlComplete => {
                    tracing::debug!(
                        "Subscribe to overall pnl complete message, topic: {}",
                        topic.topic
                    );
                    node.subscribe::<OverallPnlComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(LoggingSubscriberSnafu)?;
                }
                _ => {}
            }
        }

        self.subscribers
            .insert(self.curr_logger_id, Box::new(subscriber));

        self.logger_id_map
            .insert(msg.batch_id.clone(), self.curr_logger_id);

        self.curr_logger_id += 1;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<ReadFromDirRequest> for LoggingSubscriber {
    async fn on_data(&mut self, msg: ReadFromDirRequest) -> Result<(), SubscriberError> {
        tracing::debug!("ReadFromDirRequest message received");
        self.overall_log_reader.set_log_dir(&msg.log_dir);

        // Get batch/strategy list from the read from directory request
        let strategies_from_dir_resp = self.strategy_log_reader.strategies_from_dir(msg)?;

        self.strats_from_dir_publisher
            .publish(strategies_from_dir_resp)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<StrategyFromLogRequest> for LoggingSubscriber {
    async fn on_data(&mut self, msg: StrategyFromLogRequest) -> Result<(), SubscriberError> {
        tracing::debug!("StrategyFromLogRequest message received");

        // Get logged strategy data from this log request
        self.strategy_log_reader
            .strategy_from_log(msg.clone())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OverallFromLogRequest> for LoggingSubscriber {
    async fn on_data(&mut self, msg: OverallFromLogRequest) -> Result<(), SubscriberError> {
        tracing::debug!("OverallFromLogRequest message received");

        // Get logged overall data for this batch from this log request
        self.overall_log_reader.overall_from_log(msg).await?;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortLoggerSubscriber> for LoggingSubscriber {
    async fn on_data(&mut self, msg: AbortLoggerSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort Logger subscriber message received, id: {}",
            msg.logger_id
        );
        if let Some(subscriber) = self.subscribers.get(&msg.logger_id) {
            tracing::debug!("Abort logger subscriber, id: {}", msg.logger_id);
            subscriber.abort();
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortOverallPnlSubscriber> for LoggingSubscriber {
    async fn on_data(&mut self, msg: AbortOverallPnlSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort over all pnl subscriber message received, id: {}",
            msg.batch_id
        );

        if let Some(logger_id) = self.logger_id_map.get(&msg.batch_id) {
            if let Some(subscriber) = self.subscribers.get(logger_id) {
                tracing::debug!("Abort overall logger subscriber, id: {}", logger_id);
                subscriber.abort();
            }
        }

        Ok(())
    }
}

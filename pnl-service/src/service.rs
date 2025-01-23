use crate::config::{Config, FuturesTickConfig};
use crate::manager::PnlManager;
use crate::overall::OverallPnlSubscriber;
use crate::position::PositionSubscriber;
use async_trait::async_trait;
use chrono::{DateTime, Timelike};
use chrono_tz::America;
use common::config::{ServiceConfig, TopicConfig, TopicMtype};
use common::utils::{self, adjust_topic};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortOverallPnlSubscriber, AbortStrategyPnlSubscriber, Advice, AlgoComplete, DatafeedType,
    ExecutionMode, JoinAlgofeed, JoinAlgofeedSuccess, JoinDatafeed, JoinOrderfeed,
    JoinOrderfeedSuccess, JoinOverallPnlfeed, JoinStrategyPnlfeed, JoinStrategyPnlfeedSuccess,
    Network, OrderFilled, OrderfeedComplete, PnlComplete, PositionStats, Realized,
    ServiceAvailableRequest, ServiceAvailableResponse, StartBatch, Tick, Topic, Unrealized,
};
use utils::{convert_topics, create_node};
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

const SERVICE_ID: &str = "pnl-service";
const SERVICE_NEEDS_LIST: &[&str] = &["app-service"];

#[derive(Debug, Snafu)]
pub enum PnlServiceError {
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

    #[snafu(display("Zenoh config error in algo service"))]
    ZenohConfigError { source: NodeError },

    #[snafu(display("Error setting up pnl subscriber"))]
    PnlSubscriberError { source: NodeError },

    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Error creating Zenoh node"))]
    CreateNodeError,
}

// Zenoh connection
struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

pub struct PnlService {
    subscriber: Subscriber<PnlSubscriber>,
}

impl PnlService {
    pub async fn new(config: &Config) -> Result<Self, PnlServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = &service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating pnl service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port));
        let node = builder.build().await.context(PnlSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        // Create publishers on the service node. Use topics from service config if they exist, otherwise use hard coded topic
        let join_algofeed_succes_publisher = node
            .new_publisher(&topics.get("join_algofeed_success"))
            .await
            .context(PnlSubscriberSnafu)?;

        let join_orderfeed_succes_publisher = node
            .new_publisher(&topics.get("join_orderfeed_success"))
            .await
            .context(PnlSubscriberSnafu)?;

        let join_overall_pnl_feed_publisher = node
            .new_publisher(&topics.get("join_overall_pnlfeed"))
            .await
            .context(PnlSubscriberSnafu)?;

        let join_strategy_pnl_feed_publisher = node
            .new_publisher(&topics.get("join_strategy_pnlfeed"))
            .await
            .context(PnlSubscriberSnafu)?;

        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(PnlSubscriberSnafu)?;

        let abort_strategy_pnl_publisher = node
            .new_publisher::<AbortStrategyPnlSubscriber>(
                &topics.get("abort_strategy_pnl_subscriber"),
            )
            .await
            .context(PnlSubscriberSnafu)?;

        tracing::debug!("Create pnl subscriber");
        // Create pnl subscriber with required publishers
        let dfs = PnlSubscriber::new(
            config,
            &service_config,
            join_algofeed_succes_publisher,
            join_orderfeed_succes_publisher,
            join_overall_pnl_feed_publisher,
            join_strategy_pnl_feed_publisher,
            service_available_resp_publisher,
            abort_strategy_pnl_publisher,
        )
        .await?;
        let mut subscriber = node.new_subscriber(dfs).await.context(PnlSubscriberSnafu)?;

        // Subscribe to required service messages on service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Subscribe to service available request");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribing to service available response message");
        node.subscribe::<ServiceAvailableResponse>(
            &topics.get("service_available_response"),
            &mut subscriber,
        )
        .await
        .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to start batch");
        node.subscribe::<StartBatch>(&topics.get("start_batch"), &mut subscriber)
            .await
            .context(PnlSubscriberSnafu)?;
        tracing::debug!("Subscribe to join datafeed");
        node.subscribe::<JoinDatafeed>(&topics.get("join_datafeed"), &mut subscriber)
            .await
            .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to join algofeed");
        node.subscribe::<JoinAlgofeed>(&topics.get("join_algofeed"), &mut subscriber)
            .await
            .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to join orderfeed");
        node.subscribe::<JoinOrderfeed>(&topics.get("join_orderfeed"), &mut subscriber)
            .await
            .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to join strategy pnlfeed success");
        node.subscribe::<JoinStrategyPnlfeedSuccess>(
            &topics.get("join_strategy_pnlfeed_success"),
            &mut subscriber,
        )
        .await
        .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort overall pnl subscriber");
        node.subscribe::<AbortOverallPnlSubscriber>(
            &topics.get("abort_overall_pnl_subscriber"),
            &mut subscriber,
        )
        .await
        .context(PnlSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort strategy pnl subscriber");
        node.subscribe::<AbortStrategyPnlSubscriber>(
            &topics.get("abort_strategy_pnl_subscriber"),
            &mut subscriber,
        )
        .await
        .context(PnlSubscriberSnafu)?;

        Ok(Self { subscriber })
    }

    pub async fn join(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.subscriber.join().await?;
        Ok(())
    }
}

struct BatchBufferEntry {
    join_datafeed_buffer: HashMap<String, JoinDatafeed>,
    pnl_nw: Option<(String, u16, Node)>,
    pnlfeed: Option<JoinOverallPnlfeed>,
}

pub struct PnlSubscriber {
    join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
    join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
    join_overall_pnlfeed_publisher: Publisher<JoinOverallPnlfeed>,
    join_strategy_pnlfeed_publisher: Publisher<JoinStrategyPnlfeed>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    abort_strategy_pnl_publisher: Publisher<AbortStrategyPnlSubscriber>,
    batch_buffer: HashMap<String, BatchBufferEntry>,
    strategy_pnl_subscribers: HashMap<(String, String), (Box<dyn Abort>, Box<dyn Abort>)>,
    overall_pnl_subscribers: HashMap<String, Box<dyn Abort>>,
    services_available: HashSet<String>,
    topics: TopicConfig,
    services_required: HashSet<String>,
    futures_config: HashMap<String, FuturesTickConfig>,
    pnl_ip: String,
    service_node: Node,
    service_topics: TopicConfig,
    algofeed: Option<Feed>,
    datafeed: Option<Feed>,
    orderfeed: Option<Feed>,
}

impl PnlSubscriber {
    pub async fn new(
        config: &Config,
        service_config: &ServiceConfig,
        join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
        join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
        join_overall_pnlfeed_publisher: Publisher<JoinOverallPnlfeed>,
        join_strategy_pnlfeed_publisher: Publisher<JoinStrategyPnlfeed>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        abort_strategy_pnl_publisher: Publisher<AbortStrategyPnlSubscriber>,
    ) -> Result<Self, PnlServiceError> {
        let ip = &service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating pnl service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port));
        let service_node = builder.build().await.context(PnlSubscriberSnafu)?;

        let service_topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        let topics = TopicConfig::from(&config.topics);
        let mut services_required = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }
        Ok(Self {
            join_algofeed_success_publisher,
            join_orderfeed_success_publisher,
            service_available_resp_publisher,
            join_overall_pnlfeed_publisher,
            join_strategy_pnlfeed_publisher,
            abort_strategy_pnl_publisher,
            batch_buffer: HashMap::new(),
            strategy_pnl_subscribers: HashMap::new(),
            services_available: HashSet::new(),
            services_required,
            topics,
            futures_config: config.futures_config.clone(),
            pnl_ip: config.pnl_ip.clone(),
            service_node,
            service_topics,
            overall_pnl_subscribers: HashMap::new(),
            algofeed: None,
            datafeed: None,
            orderfeed: None,
        })
    }

    // Overall pnl topic list
    fn get_overall_pnl_topic_list(topics: &TopicConfig) -> Vec<Topic> {
        vec![
            Topic {
                topic: topics.get("overall_day_stats"),
                mtype: MessageType::OverallDayStats.into(),
            },
            Topic {
                topic: topics.get("overall_pnl_complete"),
                mtype: MessageType::OverallPnlComplete.into(),
            },
        ]
    }

    // Individual strategy pnl topic list
    fn get_strategy_pnl_topic_list(
        topics: &TopicConfig,
        batch_id: &str,
        strategy_id: &str,
    ) -> Vec<Topic> {
        vec![
            Topic {
                topic: adjust_topic(&topics.get("realized"), vec![batch_id, strategy_id]),
                mtype: MessageType::Realized.into(),
            },
            Topic {
                topic: adjust_topic(&topics.get("unrealized"), vec![batch_id, strategy_id]),
                mtype: MessageType::Unrealized.into(),
            },
            Topic {
                topic: adjust_topic(&topics.get("position_stats"), vec![batch_id, strategy_id]),
                mtype: MessageType::PositionStats.into(),
            },
            Topic {
                topic: adjust_topic(&topics.get("pnl_complete"), vec![batch_id, strategy_id]),
                mtype: MessageType::PnlComplete.into(),
            },
        ]
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for PnlSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableRequest) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available request message received from: {}",
            msg.service_id
        );

        // Let requesting service know we are available and online
        self.service_available_resp_publisher
            .publish(ServiceAvailableResponse {
                timestamp_ns: msg.timestamp_ns,
                service_id: SERVICE_ID.to_string(),
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableResponse> for PnlSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableResponse) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available response message received from: {}",
            msg.service_id
        );
        self.services_available.insert(msg.service_id);

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinAlgofeed> for PnlSubscriber {
    async fn on_data(&mut self, msg: JoinAlgofeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join algo message received");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(join_datafeed) = batch.join_datafeed_buffer.get(&msg.strategy_id) {
                let exe_type = ExecutionMode::try_from(join_datafeed.exe_mode)?;
                match exe_type {
                    // In backtest mode we are bypassing the broker so instead subscribe directly to advices
                    // from the algo and calculate pnl based on that
                    ExecutionMode::Backtest => {
                        let df_nw = join_datafeed.network.as_ref().unwrap();

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

                        let af_nw = msg.network.as_ref().unwrap();

                        // Only create a datafeed node when we don't have one already or the join datafeed message has a
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

                        let datafeed_topics = convert_topics(&join_datafeed.topics)?;
                        let algo_topics = convert_topics(&msg.topics)?;

                        let overall_pnl_realized_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(pnl_node.new_publisher(&self.topics.get("realized")).await?)
                            } else {
                                None
                            };

                        let overall_pnl_stats_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(
                                    pnl_node
                                        .new_publisher(&self.topics.get("position_stats"))
                                        .await?,
                                )
                            } else {
                                None
                            };

                        let overall_pnl_complete_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(
                                    pnl_node
                                        .new_publisher(&self.topics.get("pnl_complete"))
                                        .await?,
                                )
                            } else {
                                None
                            };

                        let algo_node = &self.algofeed.as_ref().unwrap().node;

                        // We need the datafeed so we can calculate unrealized pnl based on current market price
                        let df_node = &self.datafeed.as_ref().unwrap().node;
                        let df_type = DatafeedType::try_from(join_datafeed.df_type)?;

                        // Advice subscriber to algo node
                        let mut position_subscriber = PositionSubscriber::new(
                            &msg.batch_id,
                            &msg.strategy_id,
                            (algo_node.clone(), &algo_topics),
                            (df_node.clone(), &datafeed_topics),
                            &self.topics,
                            overall_pnl_realized_publisher,
                            overall_pnl_stats_publisher,
                            overall_pnl_complete_publisher,
                        )
                        .await?;

                        match df_type {
                            DatafeedType::Futures => {
                                position_subscriber.set_futures_config(&self.futures_config);
                            }
                            _ => (),
                        }

                        let mut subscriber = algo_node.new_subscriber(position_subscriber).await?;

                        for topic in &algo_topics {
                            let mtype = MessageType::try_from(topic.mtype)?;
                            match mtype {
                                MessageType::Advice => {
                                    tracing::debug!(
                                        "Subscribe to advice message, topic: {}",
                                        topic.topic
                                    );
                                    algo_node
                                        .subscribe::<Advice>(&topic.topic, &mut subscriber)
                                        .await?;
                                }
                                MessageType::AlgoComplete => {
                                    tracing::debug!(
                                        "Subscribe to algo complete message, topic: {}",
                                        topic.topic
                                    );
                                    algo_node
                                        .subscribe::<AlgoComplete>(&topic.topic, &mut subscriber)
                                        .await?;
                                }
                                _ => {}
                            }
                        }

                        self.join_strategy_pnlfeed_publisher
                            .publish(JoinStrategyPnlfeed {
                                timestamp_ns: 0,
                                service_id: SERVICE_ID.to_string(),
                                batch_id: msg.batch_id.clone(),
                                strategy_id: msg.strategy_id.clone(),
                                network: Some(Network {
                                    ip: af_nw.ip.clone(),
                                    port: af_nw.port as u32,
                                }),
                                topics: Self::get_strategy_pnl_topic_list(
                                    &self.topics,
                                    &msg.batch_id,
                                    &msg.strategy_id,
                                ),
                                exe_mode: join_datafeed.exe_mode,
                            })
                            .await?;
                    }
                    _ => (),
                }
            }
        }

        self.join_algofeed_success_publisher
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
impl Subscribe<JoinOrderfeed> for PnlSubscriber {
    async fn on_data(&mut self, msg: JoinOrderfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join orderfeed message received");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(join_datafeed) = batch.join_datafeed_buffer.get(&msg.strategy_id) {
                let exe_mode = ExecutionMode::try_from(join_datafeed.exe_mode)?;
                match exe_mode {
                    // In Resim mode and live mode we are subscribing to order fills to calculate pnl
                    ExecutionMode::Resim | ExecutionMode::Live => {
                        let df_nw = join_datafeed.network.as_ref().unwrap();

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

                        let of_nw = msg.network.as_ref().unwrap();

                        // Only create a datafeed node when we don't have one already or the join datafeed message has a
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

                        let datafeed_topics = convert_topics(&join_datafeed.topics)?;
                        let order_topics = convert_topics(&msg.topics)?;

                        let overall_pnl_realized_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(pnl_node.new_publisher(&self.topics.get("realized")).await?)
                            } else {
                                None
                            };

                        let overall_pnl_stats_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(
                                    pnl_node
                                        .new_publisher(&self.topics.get("position_stats"))
                                        .await?,
                                )
                            } else {
                                None
                            };

                        let overall_pnl_complete_publisher =
                            if let Some((_, _, pnl_node)) = &batch.pnl_nw {
                                Some(
                                    pnl_node
                                        .new_publisher(&self.topics.get("pnl_complete"))
                                        .await?,
                                )
                            } else {
                                None
                            };

                        let order_node = &self.orderfeed.as_ref().unwrap().node;

                        // We need the datafeed so we can calculate unrealized pnl based on current market price
                        let df_node = &self.datafeed.as_ref().unwrap().node;
                        let df_type = DatafeedType::try_from(join_datafeed.df_type)?;

                        // Order filled subscriber to broker node
                        let mut order_filled_subscriber = PositionSubscriber::new(
                            &msg.batch_id,
                            &msg.strategy_id,
                            (order_node.clone(), &order_topics),
                            (df_node.clone(), &datafeed_topics),
                            &self.topics,
                            overall_pnl_realized_publisher,
                            overall_pnl_stats_publisher,
                            overall_pnl_complete_publisher,
                        )
                        .await?;

                        match df_type {
                            DatafeedType::Futures => {
                                order_filled_subscriber.set_futures_config(&self.futures_config)
                            }
                            _ => (),
                        }

                        let mut subscriber =
                            order_node.new_subscriber(order_filled_subscriber).await?;

                        // Subscribe to order fills to calculate pnl
                        for topic in &order_topics {
                            match topic.mtype {
                                MessageType::OrderFilled => {
                                    tracing::debug!(
                                        "Subscribe to order filled topic: {}",
                                        topic.topic
                                    );
                                    order_node
                                        .subscribe::<OrderFilled>(&topic.topic, &mut subscriber)
                                        .await?;
                                }
                                MessageType::OrderfeedComplete => {
                                    tracing::debug!(
                                        "Subscribe to orderfeed complete topic: {}",
                                        topic.topic
                                    );
                                    order_node
                                        .subscribe::<OrderfeedComplete>(
                                            &topic.topic,
                                            &mut subscriber,
                                        )
                                        .await?;
                                }
                                _ => (),
                            }
                        }

                        let pnl_complete =
                            PnlCompleteSubscriber::new(self.abort_strategy_pnl_publisher.clone());
                        let mut pc_subscriber = order_node.new_subscriber(pnl_complete).await?;
                        order_node
                            .subscribe(
                                &adjust_topic(
                                    &self.topics.get("pnl_complete"),
                                    vec![&msg.batch_id, &msg.strategy_id],
                                ),
                                &mut pc_subscriber,
                            )
                            .await?;

                        self.strategy_pnl_subscribers.insert(
                            (msg.batch_id.clone(), msg.strategy_id.clone()),
                            (Box::new(subscriber), Box::new(pc_subscriber)),
                        );

                        self.join_strategy_pnlfeed_publisher
                            .publish(JoinStrategyPnlfeed {
                                timestamp_ns: 0,
                                service_id: SERVICE_ID.to_string(),
                                batch_id: msg.batch_id.clone(),
                                strategy_id: msg.strategy_id.clone(),
                                network: Some(Network {
                                    ip: of_nw.ip.clone(),
                                    port: of_nw.port as u32,
                                }),
                                topics: Self::get_strategy_pnl_topic_list(
                                    &self.topics,
                                    &msg.batch_id,
                                    &msg.strategy_id,
                                ),
                                exe_mode: join_datafeed.exe_mode,
                            })
                            .await?;

                        if !self.services_available.contains("app-service") {
                            self.join_orderfeed_success_publisher
                                .publish(JoinOrderfeedSuccess {
                                    timestamp_ns: msg.timestamp_ns,
                                    service_id: SERVICE_ID.to_string(),
                                    strategy_id: msg.strategy_id.clone(),
                                    batch_id: msg.batch_id.clone(),
                                })
                                .await?;
                        }
                    }
                    _ => (),
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinStrategyPnlfeedSuccess> for PnlSubscriber {
    async fn on_data(&mut self, msg: JoinStrategyPnlfeedSuccess) -> Result<(), SubscriberError> {
        tracing::debug!("JoinStrategyPnlfeedSuccess message received");
        self.join_orderfeed_success_publisher
            .publish(JoinOrderfeedSuccess {
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
impl Subscribe<JoinDatafeed> for PnlSubscriber {
    async fn on_data(&mut self, msg: JoinDatafeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join datafeed message received");

        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            batch
                .join_datafeed_buffer
                .insert(msg.strategy_id.clone(), msg);
        } else {
            self.batch_buffer.insert(
                msg.batch_id.clone(),
                BatchBufferEntry {
                    join_datafeed_buffer: HashMap::from([(msg.strategy_id.clone(), msg)]),
                    pnl_nw: None,
                    pnlfeed: None,
                },
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<StartBatch> for PnlSubscriber {
    async fn on_data(&mut self, msg: StartBatch) -> Result<(), SubscriberError> {
        tracing::debug!("Start batch message received");

        // Anytime we start a batch we start a new overall pnlfeed for this batch
        // TODO: I dont think this is necessary we can use the same feed
        let mut pnlfeed = None;
        let pnl_port = utils::generate_zenoh_port();

        // Set up overall pnl subscriber for this batch
        let pnl_nw = if let Some(port) = pnl_port {
            let pnl_node = create_node(&self.pnl_ip, port)
                .await
                .context(PnlSubscriberSnafu)?;

            let overall_stats_publisher = pnl_node
                .new_publisher(&self.topics.get("overall_day_stats"))
                .await
                .context(PnlSubscriberSnafu)?;

            let overall_pnl_complete_publisher = pnl_node
                .new_publisher(&self.topics.get("overall_pnl_complete"))
                .await?;

            let overall_pnl = OverallPnlSubscriber::new(
                &msg.batch_id,
                msg.strategy_ids.len(),
                overall_stats_publisher,
                overall_pnl_complete_publisher,
            );
            let mut subscriber = pnl_node
                .new_subscriber(overall_pnl)
                .await
                .context(PnlSubscriberSnafu)?;

            pnl_node
                .subscribe::<PositionStats>(&self.topics.get("position_stats"), &mut subscriber)
                .await
                .context(PnlSubscriberSnafu)?;

            pnl_node
                .subscribe::<PnlComplete>(&self.topics.get("pnl_complete"), &mut subscriber)
                .await
                .context(PnlSubscriberSnafu)?;

            pnlfeed = Some(JoinOverallPnlfeed {
                timestamp_ns: 0,
                batch_id: msg.batch_id.clone(),
                service_id: SERVICE_ID.to_string(),
                network: Some(Network {
                    ip: self.pnl_ip.clone(),
                    port: port as u32,
                }),
                topics: Self::get_overall_pnl_topic_list(&self.topics),
            });

            self.overall_pnl_subscribers
                .insert(msg.batch_id.clone(), Box::new(subscriber));

            (Some((self.pnl_ip.clone(), port, pnl_node)))
        } else {
            None
        };

        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            batch.pnl_nw = pnl_nw;
            batch.pnlfeed = pnlfeed;
        } else {
            self.batch_buffer.insert(
                msg.batch_id.clone(),
                BatchBufferEntry {
                    join_datafeed_buffer: HashMap::new(),
                    pnl_nw,
                    pnlfeed,
                },
            );
        }

        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(pnlfeed) = &batch.pnlfeed {
                self.join_overall_pnlfeed_publisher
                    .publish(pnlfeed.clone())
                    .await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortOverallPnlSubscriber> for PnlSubscriber {
    async fn on_data(&mut self, msg: AbortOverallPnlSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!("AbortOverallPnlSubscribermessage received");

        if let Some(subscriber) = self.overall_pnl_subscribers.get_mut(&msg.batch_id) {
            subscriber.abort();
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortStrategyPnlSubscriber> for PnlSubscriber {
    async fn on_data(&mut self, msg: AbortStrategyPnlSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort algo subscriber message received, id: {}",
            msg.strategy_id
        );

        // Abort subscribers for this batch id and strategy id
        if let Some(subscribers) = self
            .strategy_pnl_subscribers
            .get(&(msg.batch_id.clone(), msg.strategy_id.clone()))
        {
            tracing::debug!("Abort algo subscriber, id: {}", msg.strategy_id);
            subscribers.0.abort();
            subscribers.1.abort();
        }

        // When all strategy ids for this batch id are aborted then clean up the buffers so we
        // can reuse this batch id
        let mut all_algos_complete = false;
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            batch.join_datafeed_buffer.remove(&msg.strategy_id);

            if batch.join_datafeed_buffer.len() == 0 {
                all_algos_complete = true;
            }
        }

        if all_algos_complete {
            tracing::debug!("All complete remove batch id, id: {}", msg.batch_id);
            self.batch_buffer.remove(&msg.batch_id);
        }
        Ok(())
    }
}

struct PnlCompleteSubscriber {
    pub abort_publisher: Publisher<AbortStrategyPnlSubscriber>,
}

impl PnlCompleteSubscriber {
    pub fn new(abort_publisher: Publisher<AbortStrategyPnlSubscriber>) -> Self {
        Self { abort_publisher }
    }
}

#[async_trait]
impl Subscribe<PnlComplete> for PnlCompleteSubscriber {
    async fn on_data(&mut self, msg: PnlComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Order complete message received, id: {}", msg.strategy_id);
        self.abort_publisher
            .publish(AbortStrategyPnlSubscriber {
                timestamp_ns: msg.timestamp_ns,
                batch_id: msg.batch_id.clone(),
                strategy_id: msg.strategy_id.clone(),
            })
            .await?;

        Ok(())
    }
}

use crate::advice::AdviceSubscriber;
use crate::config::Config;
use async_trait::async_trait;
use common::config::{ServiceConfig, TopicConfig};
use common::utils::{create_node, ServiceNeedsMet};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortAdviceSubscriber, Advice, AlgoComplete, ExecutionMode, JoinAlgofeed, JoinAlgofeedSuccess,
    JoinOrderfeed, JoinOrderfeedSuccess, OrderfeedComplete, RunStrategy, ServiceAvailableRequest,
    ServiceAvailableResponse, StartAlgo, Topic,
};
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

// Currently the advice subscribers will just subscribe to the Advice message from an algo and convert it to an Order. It
// seems pointless right now since Algos can just publish Orders, but in the future the advice subscribers will also handle
// limit orders and have their own order publishing algos ie splitting a big order into smaller orders etc

const SERVICE_ID: &str = "order-service";
const SERVICE_NEEDS_LIST: &[&str] = &[
    "broker-service",
    "logging-service",
    "pnl-service",
    "app-service",
];

// In backtest mode we only need to wait on the logging service
const SERVICE_NEEDS_LIST_BACKTEST: &[&str] = &["logging-service"];

#[derive(Debug, Snafu)]
pub enum OrderServiceError {
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

    #[snafu(display("Error setting up algo subscriber"))]
    BrokerSubscriberError { source: NodeError },

    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Error creating Zenoh node"))]
    CreateNodeError,
}

pub struct OrderService {
    subscriber: Subscriber<OrderServiceSubscriber>,
}

impl OrderService {
    pub async fn new(config: &Config) -> Result<Self, OrderServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating order service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port));
        let node = builder.build().await.context(BrokerSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        // Create publishers on the service node. Use topics from service config if they exist, otherwise use hard coded topic
        let join_algofeed_succes_publisher = node
            .new_publisher(&topics.get("join_algofeed_success"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let join_orderfeed_publisher = node
            .new_publisher(&topics.get("join_orderfeed"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let abort_advice_publisher = node
            .new_publisher::<AbortAdviceSubscriber>(&topics.get("abort_advice_subscriber"))
            .await
            .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Create order subscriber");
        // Create logger subscriber with required publishers
        let dfs = OrderServiceSubscriber::new(
            config,
            join_algofeed_succes_publisher,
            join_orderfeed_publisher,
            service_available_resp_publisher,
            abort_advice_publisher,
        )
        .await?;
        let mut subscriber = node
            .new_subscriber(dfs)
            .await
            .context(BrokerSubscriberSnafu)?;

        // Subscribe to required service messages on service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Subscribe to service available request");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribing to service available response message");
        node.subscribe::<ServiceAvailableResponse>(
            &topics.get("service_available_response"),
            &mut subscriber,
        )
        .await
        .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribe to run strategy message");
        node.subscribe::<RunStrategy>(&topics.get("run_strategy"), &mut subscriber)
            .await
            .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribe to join algofeed message");
        node.subscribe::<JoinAlgofeed>(&topics.get("join_algofeed"), &mut subscriber)
            .await
            .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribe to join orderfeed message");
        node.subscribe::<JoinOrderfeedSuccess>(
            &topics.get("join_orderfeed_success"),
            &mut subscriber,
        )
        .await
        .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribe to join abort advice subsriber message");
        node.subscribe::<AbortAdviceSubscriber>(
            &topics.get("abort_advice_subscriber"),
            &mut subscriber,
        )
        .await
        .context(BrokerSubscriberSnafu)?;

        sleep(Duration::from_millis(100));

        // Request for service available discovery
        let service_available_req_publisher = node
            .new_publisher::<ServiceAvailableRequest>(&topics.get("service_available_request"))
            .await
            .context(BrokerSubscriberSnafu)?;

        service_available_req_publisher
            .publish(
                (ServiceAvailableRequest {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                }),
            )
            .await
            .context(BrokerSubscriberSnafu)?;

        Ok(Self { subscriber })
    }

    pub async fn join(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.subscriber.join().await?;
        Ok(())
    }
}

// Zenoh connection
struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

struct BatchBufferEntry {
    start_algo_buffer: HashMap<String, StartAlgo>, // Mapped by strategy ID
    service_needs_met: HashMap<String, ServiceNeedsMet>, // Mapped by strategy ID
}

pub struct OrderServiceSubscriber {
    join_orderfeed_publisher: Publisher<JoinOrderfeed>,
    join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    abort_advice_publisher: Publisher<AbortAdviceSubscriber>,
    batch_buffer: HashMap<String, BatchBufferEntry>,
    services_available: HashSet<String>,
    services_required_backtest: HashSet<String>,
    services_required: HashSet<String>,
    subscribers: HashMap<(String, String), (Box<dyn Abort>, Box<dyn Abort>)>, // Holds subscriber thread abort handles
    topics: TopicConfig,
    algofeed: Option<Feed>,
}

impl OrderServiceSubscriber {
    pub async fn new(
        config: &Config,
        join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
        join_orderfeed_publisher: Publisher<JoinOrderfeed>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        abort_advice_publisher: Publisher<AbortAdviceSubscriber>,
    ) -> Result<Self, OrderServiceError> {
        let topics = TopicConfig::from(&config.topics);
        let mut services_required = HashSet::new();
        let mut services_required_backtest = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }
        for s in SERVICE_NEEDS_LIST_BACKTEST {
            let service_str = *s;
            services_required_backtest.insert(service_str.to_string());
        }

        Ok(Self {
            join_algofeed_success_publisher,
            join_orderfeed_publisher,
            abort_advice_publisher,
            services_required,
            services_required_backtest,
            batch_buffer: HashMap::new(),
            services_available: HashSet::new(),
            subscribers: HashMap::new(),
            service_available_resp_publisher,
            topics,
            algofeed: None,
        })
    }

    // Orderfeed topic list
    fn get_order_topic_list(topics: &TopicConfig, start_msg: &StartAlgo) -> Vec<Topic> {
        vec![
            Topic {
                topic: format!(
                    "{}_{}_{}",
                    topics.get("order"),
                    start_msg.batch_id,
                    start_msg.strategy_id,
                ),
                mtype: MessageType::Order.into(),
            },
            Topic {
                topic: format!(
                    "{}_{}_{}",
                    topics.get("order_filled"),
                    start_msg.batch_id,
                    start_msg.strategy_id,
                ),
                mtype: MessageType::OrderFilled.into(),
            },
            Topic {
                topic: format!(
                    "{}_{}_{}",
                    topics.get("orderfeed_complete"),
                    start_msg.batch_id,
                    start_msg.strategy_id,
                ),
                mtype: MessageType::OrderfeedComplete.into(),
            },
        ]
    }

    fn adjust_topic(topic: &str, start_msg: &StartAlgo) -> String {
        format!("{}_{}_{}", topic, start_msg.batch_id, start_msg.strategy_id)
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableRequest) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available request message received from: {}",
            msg.service_id
        );

        // new service is available, this will affect running strategies whether their needs are
        // met. Let all running strategies know a new service is available
        for b in &mut self.batch_buffer {
            for s in &mut b.1.service_needs_met {
                s.1.add_available_service(&msg.service_id);
            }
        }
        self.services_available.insert(msg.service_id);

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
impl Subscribe<ServiceAvailableResponse> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableResponse) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available response message received from: {}",
            msg.service_id
        );

        // new service is available, this will affect running strategies whether their needs are
        // met. Let all running strategies know a new service is available
        for b in &mut self.batch_buffer {
            for s in &mut b.1.service_needs_met {
                s.1.add_available_service(&msg.service_id);
            }
        }

        self.services_available.insert(msg.service_id);

        Ok(())
    }
}

#[async_trait]
impl Subscribe<RunStrategy> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("RunStrategy message received");
        let batch = self
            .batch_buffer
            .entry(msg.start_algo.as_ref().unwrap().batch_id.clone())
            .or_insert(BatchBufferEntry {
                start_algo_buffer: HashMap::new(),
                service_needs_met: HashMap::new(),
            });

        batch.service_needs_met.insert(
            msg.start_algo.as_ref().unwrap().strategy_id.clone(),
            if msg.start_algo.as_ref().unwrap().exe_mode() == ExecutionMode::Backtest {
                ServiceNeedsMet::new(&self.services_required_backtest, &self.services_available)
            } else {
                ServiceNeedsMet::new(&self.services_required, &self.services_available)
            },
        );

        batch.start_algo_buffer.insert(
            msg.start_algo.as_ref().unwrap().strategy_id.clone(),
            msg.start_algo.unwrap(),
        );
        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinAlgofeed> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: JoinAlgofeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join algofeed message received");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(start_algo) = batch.start_algo_buffer.get_mut(&msg.strategy_id) {
                if let Some(service_needs_met) = batch.service_needs_met.get_mut(&msg.strategy_id) {
                    let af_nw = msg.network.as_ref().unwrap();

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
                    let algo_node = &self.algofeed.as_ref().unwrap().node;

                    let order_publisher = algo_node
                        .new_publisher(&Self::adjust_topic(&self.topics.get("order"), start_algo))
                        .await?;
                    let orderfeed_complete_publisher = algo_node
                        .new_publisher(&Self::adjust_topic(
                            &self.topics.get("orderfeed_complete"),
                            start_algo,
                        ))
                        .await?;

                    // Subscribe to Advices from algos
                    let advice_subscriber = AdviceSubscriber::new(
                        &msg.batch_id,
                        &msg.strategy_id,
                        &start_algo.account_ids,
                        order_publisher,
                        orderfeed_complete_publisher,
                    )
                    .await?;

                    let mut subscriber = algo_node.new_subscriber(advice_subscriber).await?;
                    for algo_topic in &msg.topics {
                        let mtype = MessageType::try_from(algo_topic.mtype)?;
                        match mtype {
                            MessageType::Advice => {
                                algo_node
                                    .subscribe::<Advice>(&algo_topic.topic, &mut subscriber)
                                    .await?;
                            }
                            MessageType::AlgoComplete => {
                                algo_node
                                    .subscribe::<AlgoComplete>(&algo_topic.topic, &mut subscriber)
                                    .await?;
                            }
                            _ => (),
                        }
                    }

                    let ofc_complete =
                        OrderfeedCompleteSubscriber::new(self.abort_advice_publisher.clone());

                    // Spawn a new subscriber that will listen for the OrderfeedComplete message from new algo.
                    // When orderfeed is complete, the order service needs to know so it can abort the thread and cleanup
                    let mut ofc_subscriber = algo_node.new_subscriber(ofc_complete).await?;
                    algo_node
                        .subscribe(
                            &Self::adjust_topic(&self.topics.get("orderfeed_complete"), start_algo),
                            &mut ofc_subscriber,
                        )
                        .await?;

                    self.subscribers.insert(
                        (msg.batch_id.clone(), msg.strategy_id.clone()),
                        (Box::new(subscriber), Box::new(ofc_subscriber)),
                    );

                    tracing::debug!("Publish join orderfeed");
                    self.join_orderfeed_publisher
                        .publish(JoinOrderfeed {
                            timestamp_ns: 0,
                            service_id: SERVICE_ID.to_string(),
                            batch_id: msg.batch_id.clone(),
                            strategy_id: msg.strategy_id.to_string(),
                            network: Some(af_nw.clone()),
                            topics: Self::get_order_topic_list(&self.topics, &start_algo),
                            exe_mode: msg.exe_mode,
                        })
                        .await?;

                    // No need to wait for services when in backtest mode
                    if service_needs_met.is_complete() || msg.exe_mode() == ExecutionMode::Backtest
                    {
                        tracing::debug!("Publish join algofeed success");
                        self.join_algofeed_success_publisher
                            .publish(JoinAlgofeedSuccess {
                                timestamp_ns: msg.timestamp_ns,
                                strategy_id: msg.strategy_id.clone(),
                                service_id: SERVICE_ID.to_string(),
                                batch_id: msg.batch_id.clone(),
                            })
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOrderfeedSuccess> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: JoinOrderfeedSuccess) -> Result<(), SubscriberError> {
        tracing::debug!(
            "JoinOrderfeedSuccess message received, service id: {}, strategy id: {}",
            msg.service_id,
            msg.strategy_id
        );
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(service_needs_met) = batch.service_needs_met.get_mut(&msg.strategy_id) {
                service_needs_met.add_service_response(&msg.service_id);
                if service_needs_met.is_complete() {
                    tracing::debug!(
                        "Service wait is complete, publish join algofeed success message"
                    );
                    self.join_algofeed_success_publisher
                        .publish(JoinAlgofeedSuccess {
                            timestamp_ns: msg.timestamp_ns,
                            strategy_id: msg.strategy_id.clone(),
                            service_id: SERVICE_ID.to_string(),
                            batch_id: msg.batch_id.clone(),
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortAdviceSubscriber> for OrderServiceSubscriber {
    async fn on_data(&mut self, msg: AbortAdviceSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort advice subscriber message received, id: {}",
            msg.strategy_id
        );
        if let Some(subscribers) = self
            .subscribers
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
            batch.start_algo_buffer.remove(&msg.strategy_id);
            batch.service_needs_met.remove(&msg.strategy_id);

            if batch.start_algo_buffer.len() == 0 {
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

struct OrderfeedCompleteSubscriber {
    pub abort_publisher: Publisher<AbortAdviceSubscriber>,
}

impl OrderfeedCompleteSubscriber {
    pub fn new(abort_publisher: Publisher<AbortAdviceSubscriber>) -> Self {
        Self { abort_publisher }
    }
}

#[async_trait]
impl Subscribe<OrderfeedComplete> for OrderfeedCompleteSubscriber {
    async fn on_data(&mut self, msg: OrderfeedComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Order complete message received, id: {}", msg.strategy_id);
        self.abort_publisher
            .publish(AbortAdviceSubscriber {
                timestamp_ns: msg.timestamp_ns,
                batch_id: msg.batch_id.clone(),
                strategy_id: msg.strategy_id.clone(),
            })
            .await?;

        Ok(())
    }
}

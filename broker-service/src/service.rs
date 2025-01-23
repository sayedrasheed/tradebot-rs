use crate::broker::{BrokerBuilder, BrokerSetup};
use crate::config::{BrokerConfig, Config};
use crate::paper_trader::PaperTrader;
use async_trait::async_trait;
use common::config::{ServiceConfig, TopicConfig, TopicMtype};
use common::utils::{convert_topics, create_node, ServiceNeedsMet};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::start_broker::BrokerId;
use tradebot_protos::messages::{
    AbortBrokerSubscriber, BrokerComplete, ExecutionMode, JoinAlgofeed, JoinDatafeed,
    JoinDatafeedSuccess, JoinOrderfeed, JoinOrderfeedSuccess, Network, Order, OrderFilled,
    OrderfeedComplete, RunStrategy, ServiceAvailableRequest, ServiceAvailableResponse, StartAlgo,
    StartBroker, StartNinjaOrderfeed, Topic,
};
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{
    Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError, SubscriberImpl,
};

const SERVICE_ID: &str = "broker-service";

// Wait on these services to respond to start a broker, if they are available
const SERVICE_NEEDS_LIST: &[&str] = &["order-service"];

#[derive(Debug, Snafu)]
pub enum BrokerServiceError {
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

struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

pub struct BrokerService {
    subscriber: Subscriber<BrokerSubscriber>,
}

impl BrokerService {
    pub async fn new(config: &Config) -> Result<Self, BrokerServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating broker service");
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
        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let join_orderfeed_success_publisher = node
            .new_publisher::<JoinOrderfeedSuccess>(&topics.get("join_orderfeed_success"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let abort_broker_publisher = node
            .new_publisher::<AbortBrokerSubscriber>(&topics.get("abort_broker_subscriber"))
            .await
            .context(BrokerSubscriberSnafu)?;

        let start_ninja_of_publisher = node
            .new_publisher::<StartNinjaOrderfeed>(&topics.get("start_ninja_orderfeed"))
            .await
            .context(BrokerSubscriberSnafu)?;

        // Create broker subscriber with required publishers
        tracing::debug!("Create broker subscriber");
        let dfs = BrokerSubscriber::new(
            config,
            service_available_resp_publisher,
            join_orderfeed_success_publisher,
            abort_broker_publisher,
            start_ninja_of_publisher,
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
        node.subscribe::<JoinOrderfeed>(&topics.get("join_orderfeed"), &mut subscriber)
            .await
            .context(BrokerSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort broker subscriber message");
        node.subscribe::<AbortBrokerSubscriber>(
            &topics.get("abort_broker_subscriber"),
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

struct BatchBufferEntry {
    start_broker_buffer: HashMap<String, StartBroker>, // Mapped by strategy id
    service_needs_met: HashMap<String, ServiceNeedsMet>, // Mapped by strategy id
}

pub struct BrokerSubscriber {
    broker_config_map: HashMap<BrokerId, BrokerConfig>,
    subscribers: HashMap<u32, (Box<dyn Abort>, Box<dyn Abort>)>,
    batch_buffer: HashMap<String, BatchBufferEntry>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
    abort_broker_publisher: Publisher<AbortBrokerSubscriber>,
    start_ninja_of_publisher: Publisher<StartNinjaOrderfeed>,
    services_available: HashSet<String>,
    services_required: HashSet<String>,
    broker_id_batch_map: HashMap<u32, (String, String)>,
    broker_ip: String,
    topics: TopicConfig,
    broker_curr_id: u32,
    orderfeed: Option<Feed>,
}

impl BrokerSubscriber {
    pub async fn new(
        config: &Config,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
        abort_broker_publisher: Publisher<AbortBrokerSubscriber>,
        start_ninja_of_publisher: Publisher<StartNinjaOrderfeed>,
    ) -> Result<Self, BrokerServiceError> {
        let broker_config_map = config
            .broker
            .iter()
            .map(|ac| (ac.broker_id.clone(), ac.clone()))
            .collect();

        let mut services_required = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }

        Ok(Self {
            broker_config_map,
            subscribers: HashMap::new(),
            batch_buffer: HashMap::new(),
            topics: TopicConfig::from(&config.topics),
            broker_ip: config.broker_ip.clone(),
            services_required,
            services_available: HashSet::new(),
            broker_id_batch_map: HashMap::new(),
            service_available_resp_publisher,
            join_orderfeed_success_publisher,
            abort_broker_publisher,
            start_ninja_of_publisher,
            broker_curr_id: 0,
            orderfeed: None,
        })
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for BrokerSubscriber {
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
impl Subscribe<ServiceAvailableResponse> for BrokerSubscriber {
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
impl Subscribe<RunStrategy> for BrokerSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("Start broker message received");

        // Latch StartBroker information to buffer, no need to start algo until we get a corresponding
        // join datafeed message
        let batch = self
            .batch_buffer
            .entry(msg.start_broker.as_ref().unwrap().batch_id.clone())
            .or_insert(BatchBufferEntry {
                start_broker_buffer: HashMap::new(),
                service_needs_met: HashMap::new(),
            });

        // Batch should map by strategy id as batch id and strategy id are a unique combination
        batch.start_broker_buffer.insert(
            msg.start_broker.as_ref().unwrap().strategy_id.clone(),
            msg.start_broker.unwrap(),
        );

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOrderfeed> for BrokerSubscriber {
    async fn on_data(&mut self, msg: JoinOrderfeed) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Join orderfeed message received: strat id: {}",
            msg.strategy_id
        );
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let service_needs_met = batch
                .service_needs_met
                .entry(msg.strategy_id.clone())
                .or_insert(ServiceNeedsMet::new(
                    &self.services_required,
                    &self.services_available,
                ));

            service_needs_met.add_service_response(&msg.service_id);
            if service_needs_met.is_complete() {
                if let Some(start_broker) = batch.start_broker_buffer.get(&msg.strategy_id) {
                    let nw = msg.network.as_ref().unwrap();

                    // Only create a orderfeed node when we don't have one already or the join orderfeed message has a
                    // different ip and port as previous
                    if self.orderfeed.is_none()
                        || self.orderfeed.as_ref().unwrap().ip != nw.ip
                        || self.orderfeed.as_ref().unwrap().port != nw.port as u16
                    {
                        let order_node = create_node(&nw.ip, nw.port as u16).await?;
                        self.orderfeed = Some(Feed {
                            node: order_node,
                            ip: nw.ip.clone(),
                            port: nw.port as u16,
                        });
                    }

                    let of_node = &self.orderfeed.as_ref().unwrap().node;

                    let broker_id: BrokerId = BrokerId::try_from(start_broker.broker_id)?;
                    match broker_id {
                        BrokerId::PaperTrader => {
                            // Set up paper trader for simulation of order fills
                            tracing::debug!("PaperTrader broker");
                            let config =
                                self.broker_config_map.get(&BrokerId::PaperTrader).unwrap();
                            let mut paper_trader = PaperTrader::new(config.config.as_ref()).await?;

                            paper_trader.set_id(self.broker_curr_id);
                            let of_topics = convert_topics(&msg.topics)?;
                            let of_topic = of_topics
                                .iter()
                                .find(|bt| bt.mtype == MessageType::OrderFilled);

                            if let Some(oft) = of_topic {
                                let of_publisher = of_node.new_publisher(&oft.topic).await?;
                                paper_trader.set_order_filled_publisher(of_publisher);
                            }

                            paper_trader.set_broker_complete_publisher(
                                of_node
                                    .new_publisher(&format!(
                                        "{}_{}",
                                        self.topics.get("broker_complete"),
                                        self.broker_curr_id
                                    ))
                                    .await?,
                            );

                            // Spawn paper trader subscriber to listen for orders
                            let mut subscriber = of_node.new_subscriber(paper_trader).await?;

                            // Subscribe to orders and orderfeed complete from orderfeed
                            for topic in &of_topics {
                                match topic.mtype {
                                    MessageType::Order => {
                                        tracing::debug!(
                                            "Subscribe to order topic: {}",
                                            topic.topic
                                        );
                                        of_node
                                            .subscribe::<Order>(&topic.topic, &mut subscriber)
                                            .await?;
                                    }
                                    MessageType::OrderfeedComplete => {
                                        tracing::debug!(
                                            "Subscribe to orderfeed complete topic: {}",
                                            topic.topic
                                        );
                                        of_node
                                            .subscribe::<OrderfeedComplete>(
                                                &topic.topic,
                                                &mut subscriber,
                                            )
                                            .await?;
                                    }
                                    _ => (),
                                }
                            }

                            // Spawn a new subscriber that will listen for the BrokerComplete message from new broker.
                            // When broker is complete, the broker service needs to know so it can abort the thread and cleanup
                            let broker_complete =
                                BrokerCompleteSubscriber::new(self.abort_broker_publisher.clone());

                            let mut complete_subscriber =
                                of_node.new_subscriber(broker_complete).await?;

                            of_node
                                .subscribe::<BrokerComplete>(
                                    &format!(
                                        "{}_{}",
                                        self.topics.get("broker_complete"),
                                        self.broker_curr_id
                                    ),
                                    &mut complete_subscriber,
                                )
                                .await?;

                            self.join_orderfeed_success_publisher
                                .publish(JoinOrderfeedSuccess {
                                    timestamp_ns: msg.timestamp_ns,
                                    strategy_id: msg.strategy_id.clone(),
                                    service_id: SERVICE_ID.to_string(),
                                    batch_id: msg.batch_id.clone(),
                                })
                                .await?;

                            self.subscribers.insert(
                                self.broker_curr_id,
                                (Box::new(subscriber), Box::new(complete_subscriber)),
                            );
                        }
                        BrokerId::NinjaTrader => {
                            tracing::debug!("NinjaTrader broker");

                            // Ninjatrader is a special case where there is an external server handling
                            // the connection. So publish this message relaying the network and topics
                            // for the server to subscribe to
                            let start_ninja_of = StartNinjaOrderfeed {
                                timestamp_ns: msg.timestamp_ns,
                                batch_id: msg.batch_id.clone(),
                                strategy_id: msg.strategy_id.clone(),
                                network: msg.network.clone(),
                                topics: msg.topics.clone(),
                            };

                            self.start_ninja_of_publisher
                                .publish(start_ninja_of)
                                .await?;

                            self.join_orderfeed_success_publisher
                                .publish(JoinOrderfeedSuccess {
                                    timestamp_ns: msg.timestamp_ns,
                                    strategy_id: msg.strategy_id.clone(),
                                    service_id: SERVICE_ID.to_string(),
                                    batch_id: msg.batch_id.clone(),
                                })
                                .await?;
                        }
                        _ => (),
                    }

                    self.broker_id_batch_map.insert(
                        self.broker_curr_id,
                        (msg.batch_id.clone(), msg.strategy_id.clone()),
                    );
                    self.broker_curr_id += 1;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortBrokerSubscriber> for BrokerSubscriber {
    async fn on_data(&mut self, msg: AbortBrokerSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort broker subscriber message received, id: {}",
            msg.broker_id
        );
        if let Some(subscribers) = self.subscribers.get(&msg.broker_id) {
            tracing::debug!("Abort broker subscriber, id: {}", msg.broker_id);
            subscribers.0.abort();
            subscribers.1.abort();
        }

        if let Some((batch_id, strategy_id)) = &self.broker_id_batch_map.get(&msg.broker_id) {
            let mut all_algos_complete = false;
            if let Some(batch) = self.batch_buffer.get_mut(batch_id) {
                batch.start_broker_buffer.remove(strategy_id);
                batch.service_needs_met.remove(strategy_id);

                if batch.start_broker_buffer.len() == 0 {
                    all_algos_complete = true;
                }
            }

            if all_algos_complete {
                tracing::debug!("All complete remove batch id, id: {}", batch_id);
                self.batch_buffer.remove(batch_id);
            }
        }
        Ok(())
    }
}

struct BrokerCompleteSubscriber {
    pub abort_publisher: Publisher<AbortBrokerSubscriber>,
}

impl BrokerCompleteSubscriber {
    pub fn new(abort_publisher: Publisher<AbortBrokerSubscriber>) -> Self {
        Self { abort_publisher }
    }
}

#[async_trait]
impl Subscribe<BrokerComplete> for BrokerCompleteSubscriber {
    async fn on_data(&mut self, msg: BrokerComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Broker complete message received, id: {}", msg.broker_id);
        self.abort_publisher
            .publish(AbortBrokerSubscriber {
                timestamp_ns: msg.timestamp_ns,
                broker_id: msg.broker_id,
            })
            .await?;

        Ok(())
    }
}

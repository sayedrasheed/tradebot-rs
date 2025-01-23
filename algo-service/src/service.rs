use crate::algo::AlgoBuilder;
use crate::config::{AlgoConfig, Config};
use crate::cpp_algo::cpp_algo::CppAlgoSetup;
use crate::py_algo::py_algo::PyAlgoSetup;
use crate::rust_algo::rust_algo::RustAlgoSetup;
use async_trait::async_trait;
use common::config::{ServiceConfig, TopicConfig, TopicMtype};
use common::utils::{self, ServiceNeedsMet};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::start_algo::AlgoId;
use tradebot_protos::messages::{
    AbortAlgoSubscriber, AlgoComplete, JoinAlgofeed, JoinAlgofeedSuccess, JoinDatafeed,
    JoinDatafeedSuccess, JoinNinjafeed, Network, RunStrategy, ServiceAvailableRequest,
    ServiceAvailableResponse, StartAlgo, Topic,
};
use utils::create_node;
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

const SERVICE_ID: &str = "algo-service";

// Wait on these services to respond to start an algo, if they are available
const SERVICE_NEEDS_LIST: &[&str] = &["order-service", "pnl-service", "app-service"];

#[derive(Debug, Snafu)]
pub enum AlgoServiceError {
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
    AlgoSubscriberError { source: NodeError },

    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Error creating Zenoh node"))]
    CreateNodeError,
}

pub struct AlgoService {
    subscriber: Subscriber<AlgoSubscriber>,
}

impl AlgoService {
    pub async fn new(config: &Config) -> Result<Self, AlgoServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Create service node on ip: {}, port: {}", ip, port);
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port));
        let node = builder.build().await.context(AlgoSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        // Create publishers on the service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Create join algo publisher");
        let join_algo_publisher = node
            .new_publisher::<JoinAlgofeed>(&topics.get("join_algofeed"))
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Create join datafeed success publisher");
        let join_df_success_publisher = node
            .new_publisher::<JoinDatafeedSuccess>(&topics.get("join_datafeed_success"))
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Create service available response publisher");
        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Create abort algo subscriber publisher");
        let abort_algo_subscriber_publisher = node
            .new_publisher::<AbortAlgoSubscriber>(&topics.get("abort_algo_subscriber"))
            .await
            .context(AlgoSubscriberSnafu)?;

        // Create algo subscriber with required publishers
        let dfs = AlgoSubscriber::new(
            config,
            join_algo_publisher,
            join_df_success_publisher,
            service_available_resp_publisher,
            abort_algo_subscriber_publisher,
        )
        .await?;

        let mut subscriber = node
            .new_subscriber(dfs)
            .await
            .context(AlgoSubscriberSnafu)?;

        // Subscribe to required service messages on service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Subscribing to service available request message");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Subscribing to service available response message");
        node.subscribe::<ServiceAvailableResponse>(
            &topics.get("service_available_response"),
            &mut subscriber,
        )
        .await
        .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Subscribe to run strategy message");
        node.subscribe::<RunStrategy>(&topics.get("run_strategy"), &mut subscriber)
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Subscribe to join datafeed");
        node.subscribe::<JoinDatafeed>(&topics.get("join_datafeed"), &mut subscriber)
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Subscribe to abort algo subscriber");
        node.subscribe::<AbortAlgoSubscriber>(
            &topics.get("abort_algo_subscriber"),
            &mut subscriber,
        )
        .await
        .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Subscribe to join algofeed success");
        node.subscribe::<JoinAlgofeedSuccess>(
            &topics.get("join_algofeed_success"),
            &mut subscriber,
        )
        .await
        .context(AlgoSubscriberSnafu)?;

        sleep(Duration::from_millis(100));

        // Request for service available discovery
        let service_available_req_publisher = node
            .new_publisher::<ServiceAvailableRequest>(&topics.get("service_available_request"))
            .await
            .context(AlgoSubscriberSnafu)?;

        tracing::debug!("Publish service available request message for service discovery");
        service_available_req_publisher
            .publish(
                (ServiceAvailableRequest {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                }),
            )
            .await
            .context(AlgoSubscriberSnafu)?;

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

// Buffer entry for this Batch
struct BatchBufferEntry {
    pub start_algo_buffer: HashMap<String, StartAlgo>, // Mapped by strategy ID
    pub service_needs_met: HashMap<String, ServiceNeedsMet>, // Mapped by strategy ID
}

pub struct AlgoSubscriber {
    algo_config_map: HashMap<AlgoId, AlgoConfig>,
    subscribers: HashMap<(String, String), (Box<dyn Abort>, Box<dyn Abort>)>,
    join_algo_publisher: Publisher<JoinAlgofeed>,
    join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    abort_algo_publisher: Publisher<AbortAlgoSubscriber>,
    algo_ip: String,
    topics: TopicConfig,
    batch_buffer: HashMap<String, BatchBufferEntry>,
    services_available: HashSet<String>,
    services_required: HashSet<String>,
    datafeed: Option<Feed>,
    algofeed: Option<Feed>,
    num_join_df: u32,
}

impl AlgoSubscriber {
    pub async fn new(
        config: &Config,
        join_algo_publisher: Publisher<JoinAlgofeed>,
        join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        abort_algo_publisher: Publisher<AbortAlgoSubscriber>,
    ) -> Result<Self, AlgoServiceError> {
        let algo_config_map = config
            .algos
            .iter()
            .map(|ac| (ac.algo_id.clone(), ac.clone()))
            .collect();

        let mut services_required = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }

        Ok(Self {
            algo_config_map,
            subscribers: HashMap::new(),
            join_algo_publisher,
            join_df_success_publisher,
            service_available_resp_publisher,
            abort_algo_publisher,
            algo_ip: config.algo_ip.clone(),
            topics: TopicConfig::from(&config.topics),
            batch_buffer: HashMap::new(),
            services_available: HashSet::new(),
            services_required,
            datafeed: None,
            algofeed: None,
            num_join_df: 0,
        })
    }

    // Common topic list that everyone algo will publish
    fn get_common_topic_list(topics: &TopicConfig) -> Vec<TopicMtype> {
        vec![
            TopicMtype {
                topic: topics.get("algo_complete"),
                mtype: MessageType::AlgoComplete,
            },
            TopicMtype {
                topic: topics.get("advice"),
                mtype: MessageType::Advice,
            },
        ]
    }

    // Adjust topic so it is unique to this strategy. Use batch id and strategy id
    fn adjust_topics(topics: &mut Vec<TopicMtype>, start_algo: &StartAlgo) {
        topics.iter_mut().for_each(|t| {
            t.topic = format!(
                "{}_{}_{}",
                t.topic, start_algo.batch_id, start_algo.strategy_id
            )
        });
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for AlgoSubscriber {
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
        tracing::debug!("Publish service availble response message");
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
impl Subscribe<ServiceAvailableResponse> for AlgoSubscriber {
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
impl Subscribe<RunStrategy> for AlgoSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("Run strategy message received");

        // Latch StartAlgo information to buffer, no need to start algo until we get a corresponding
        // join datafeed message
        let batch = self
            .batch_buffer
            .entry(msg.start_algo.as_ref().unwrap().batch_id.clone())
            .or_insert(BatchBufferEntry {
                start_algo_buffer: HashMap::new(),
                service_needs_met: HashMap::new(),
            });

        // Batch should map by strategy id as batch id and strategy id are a unique combination
        batch.start_algo_buffer.insert(
            msg.start_algo.as_ref().unwrap().strategy_id.clone(),
            msg.start_algo.unwrap(),
        );
        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinDatafeed> for AlgoSubscriber {
    async fn on_data(&mut self, msg: JoinDatafeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join datafeed message received");

        // Get corresponding StartAlgo information to start the algo and join the datafeed
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(start_algo) = batch.start_algo_buffer.get(&msg.strategy_id) {
                self.num_join_df += 1;
                let algo_id = AlgoId::try_from(start_algo.algo_id)?;

                // get algo config for this algo id
                let algo_config = self.algo_config_map.get(&algo_id).unwrap(); // TODO check unwrap here
                let nw = msg.network.unwrap();

                // Only create a datafeed node when we don't have one already or the join datafeed message has a
                // different ip and port as previous
                if self.datafeed.is_none()
                    || self.datafeed.as_ref().unwrap().ip != nw.ip
                    || self.datafeed.as_ref().unwrap().port != nw.port as u16
                {
                    tracing::debug!(
                        "Set up zenoh datafeed node, ip: {}, port: {}",
                        nw.ip,
                        nw.port
                    );
                    let df_node = create_node(&nw.ip, nw.port as u16).await?;
                    self.datafeed = Some(Feed {
                        node: df_node,
                        ip: nw.ip.clone(),
                        port: nw.port as u16,
                    });
                }

                // get publish topics from the algo config for this algo id
                let mut publish_topics: Vec<TopicMtype> = algo_config.topics.clone();

                // add common topics
                publish_topics.extend(Self::get_common_topic_list(&self.topics));

                // adjust topics so they are unique to this batch and strategy id
                Self::adjust_topics(&mut publish_topics, start_algo);
                tracing::debug!("Algo adjusted topics: {:?}", publish_topics);

                let subscribe_topics = msg
                    .topics
                    .iter()
                    .filter_map(|t| {
                        let mtype = MessageType::try_from(t.mtype);
                        match mtype {
                            Ok(mtype) => Some(TopicMtype {
                                mtype,
                                topic: t.topic.clone(),
                            }),
                            Err(_) => None,
                        }
                    })
                    .collect();

                // Create algo node every 100 new running strategies
                // TODO: Is there any benefit to switching port?
                if self.algofeed.is_none() || self.num_join_df % 100 == 0 {
                    let algo_port: Option<u16> = utils::generate_zenoh_port();

                    if let Some(port) = algo_port {
                        tracing::debug!(
                            "Create zenoh algo node, ip: {}, port: {}",
                            self.algo_ip,
                            port
                        );
                        let algo_node = create_node(&self.algo_ip, port as u16).await?;
                        self.algofeed = Some(Feed {
                            node: algo_node,
                            ip: self.algo_ip.clone(),
                            port: port as u16,
                        });
                    } else {
                        tracing::error!("Error generating zenoh port");
                        return Ok(());
                    }
                }

                tracing::debug!("Set up subscribe node to be datafeed node");
                let subscribe_node = &self.datafeed.as_ref().unwrap().node;

                tracing::debug!("Set up publish node to be algofeed node");
                let publish_node = &self.algofeed.as_ref().unwrap().node;

                // Algos subscribe to messages coming from datafeed and will publish its output on
                // its own node
                let algo_builder = AlgoBuilder::new();

                // TODO: DRY principle can be used here with some refactoring. For now leave as is
                let subscriber = match algo_id {
                    AlgoId::RustAlgo => {
                        let ta = RustAlgoSetup::new();
                        let subscriber = algo_builder
                            .build(
                                ta,
                                algo_config.config.clone(),
                                subscribe_node,
                                &subscribe_topics,
                                publish_node,
                                &publish_topics,
                            )
                            .await?;

                        subscriber
                    }
                    AlgoId::PyAlgo => {
                        let ta = PyAlgoSetup::new();
                        let subscriber = algo_builder
                            .build(
                                ta,
                                algo_config.config.clone(),
                                subscribe_node,
                                &subscribe_topics,
                                publish_node,
                                &publish_topics,
                            )
                            .await?;

                        subscriber
                    }
                    AlgoId::CppAlgo => {
                        let ls = CppAlgoSetup::new();
                        let subscriber = algo_builder
                            .build(
                                ls,
                                algo_config.config.clone(),
                                &subscribe_node,
                                &subscribe_topics,
                                publish_node,
                                &publish_topics,
                            )
                            .await?;

                        subscriber
                    }
                };

                // Spawn a new subscriber that will listen for the AlgoComplete message from new algo.
                // When algo is complete, the algo service needs to know so it can abort the thread and cleanup
                let algo_complete_topic = publish_topics
                    .iter()
                    .find(|bt| bt.mtype == MessageType::AlgoComplete)
                    .unwrap();

                let algo_complete_subscriber = AlgoCompleteSubscriber::new(
                    &msg.batch_id,
                    &msg.strategy_id,
                    self.abort_algo_publisher.clone(),
                );

                let mut ac = publish_node
                    .new_subscriber(algo_complete_subscriber)
                    .await?;

                publish_node
                    .subscribe::<AlgoComplete>(&algo_complete_topic.topic, &mut ac)
                    .await?;

                self.subscribers.insert(
                    (msg.batch_id.clone(), msg.strategy_id.clone()),
                    (subscriber, Box::new(ac)),
                );

                let service_needs_met = batch
                    .service_needs_met
                    .entry(msg.strategy_id.clone())
                    .or_insert(ServiceNeedsMet::new(
                        &self.services_required,
                        &self.services_available,
                    ));

                service_needs_met.add_service_response(&msg.service_id);

                // Need to check service needs is_complete here for the case where no other services are available/required
                if service_needs_met.is_complete() {
                    tracing::debug!("Algofeed service needs are met, id: {}", msg.strategy_id);
                    let join_df_success = JoinDatafeedSuccess {
                        timestamp_ns: msg.timestamp_ns,
                        service_id: SERVICE_ID.to_string(),
                        strategy_id: msg.strategy_id.clone(),
                        batch_id: msg.batch_id.clone(),
                    };

                    // Once algo service needs are met for this strategy, tell datafeed service that we successfully joined and it can
                    // start publishing to us
                    tracing::debug!(
                        "Publish join datafeed success message with strategy id: {}",
                        msg.strategy_id
                    );
                    self.join_df_success_publisher
                        .publish(join_df_success)
                        .await?;
                }

                // Publish join algofeed message so downstream nodes can subscribe to output for this strategy
                tracing::debug!(
                    "Publish join algo message on port: {}",
                    self.algofeed.as_ref().unwrap().port
                );
                let join_algo = JoinAlgofeed {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: start_algo.batch_id.clone(),
                    strategy_id: msg.strategy_id.clone(),
                    topics: publish_topics
                        .iter()
                        .map(|t| Topic {
                            mtype: t.mtype.into(),
                            topic: t.topic.clone(),
                        })
                        .collect(),
                    network: Some(Network {
                        ip: self.algo_ip.clone(),
                        port: self.algofeed.as_ref().unwrap().port as u32,
                    }),
                    exe_mode: msg.exe_mode,
                };

                self.join_algo_publisher.publish(join_algo).await?;
            } else {
                tracing::error!("Start algo message not found");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinAlgofeedSuccess> for AlgoSubscriber {
    async fn on_data(&mut self, msg: JoinAlgofeedSuccess) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Join algofeed success message received, service id: {}, strategy id: {}",
            msg.service_id,
            msg.strategy_id
        );
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(service_needs_met) = batch.service_needs_met.get_mut(&msg.strategy_id) {
                service_needs_met.add_service_response(&msg.service_id);

                // Once algo service needs are met for this strategy, tell datafeed service that we successfully joined and it can
                // start publishing to us
                if service_needs_met.is_complete() {
                    tracing::debug!(
                        "Algofeed service needs are complete, id: {}",
                        msg.strategy_id
                    );
                    let join_df_success = JoinDatafeedSuccess {
                        timestamp_ns: msg.timestamp_ns,
                        service_id: SERVICE_ID.to_string(),
                        strategy_id: msg.strategy_id.clone(),
                        batch_id: msg.batch_id.clone(),
                    };

                    tracing::debug!(
                        "Publish join datafeed success message with strategy id: {}",
                        msg.strategy_id
                    );
                    self.join_df_success_publisher
                        .publish(join_df_success)
                        .await?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AbortAlgoSubscriber> for AlgoSubscriber {
    async fn on_data(&mut self, msg: AbortAlgoSubscriber) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Abort algo subscriber message received, id: {}",
            msg.strategy_id
        );

        // Abort subscribers for this batch id and strategy id
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

struct AlgoCompleteSubscriber {
    pub batch_id: String,
    pub strategy_id: String,
    pub abort_publisher: Publisher<AbortAlgoSubscriber>,
}

impl AlgoCompleteSubscriber {
    pub fn new(
        batch_id: &str,
        strategy_id: &str,
        abort_publisher: Publisher<AbortAlgoSubscriber>,
    ) -> Self {
        Self {
            batch_id: batch_id.to_string(),
            strategy_id: strategy_id.to_string(),
            abort_publisher,
        }
    }
}

#[async_trait]
impl Subscribe<AlgoComplete> for AlgoCompleteSubscriber {
    async fn on_data(&mut self, msg: AlgoComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Abort complete message received, id: {}", msg.algo_id);

        // When algo publishes that is is complete, let all services know that this
        // strategy is complete and that it can abort and clean up
        self.abort_publisher
            .publish(AbortAlgoSubscriber {
                timestamp_ns: msg.timestamp_ns,
                batch_id: self.batch_id.clone(),
                strategy_id: self.strategy_id.clone(),
            })
            .await?;

        Ok(())
    }
}

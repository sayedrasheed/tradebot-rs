use crate::config::Config;
use crate::datafeed::{DatafeedBuilder, DatafeedSpawnSetup};
use crate::parquet::ParquetDatafeed;
use crate::s3::S3Datafeed;
use common::config::{ServiceConfig, TopicConfig, TopicMtype};
use common::utils::{self, ServiceNeedsMet};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    DatafeedProvider, DatafeedStream, DatafeedType, ExecutionMode, JoinDatafeed,
    JoinDatafeedSuccess, Network, RunStrategy, ServiceAvailableRequest, ServiceAvailableResponse,
    StartDatafeed, StartNinjaDatafeed, Topic,
};
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Node, Publisher, Subscribe, Subscriber, SubscriberError};

use async_trait::async_trait;
use std::thread::sleep;
use std::time::Duration;
use utils::create_node;

const SERVICE_ID: &str = "datafeed-service";
const SERVICE_NEEDS_LIST: &[&str] = &["algo-service", "logging-service", "app-service"];

#[derive(Debug, Snafu)]
pub enum DatafeedServiceError {
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

    #[snafu(display("Zenoh config error in datafeed service"))]
    ZenohConfigError { source: NodeError },

    #[snafu(display("Error setting up datafeed subscriber"))]
    DatafeedSubscriberError { source: NodeError },
}

pub struct DatafeedService {
    subscriber: Subscriber<DatafeedSubscriber>,
}

impl DatafeedService {
    pub async fn new(config: &Config) -> Result<Self, DatafeedServiceError> {
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating datafeed service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port as u16));
        let node = builder.build().await.context(DatafeedSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        let join_df_publisher = node
            .new_publisher::<JoinDatafeed>(&topics.get("join_datafeed"))
            .await
            .context(DatafeedSubscriberSnafu)?;

        let service_available_response_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(DatafeedSubscriberSnafu)?;

        let start_ninja_df_publisher = node
            .new_publisher::<StartNinjaDatafeed>(&topics.get("start_ninja_datafeed"))
            .await
            .context(DatafeedSubscriberSnafu)?;

        let dfs = DatafeedSubscriber::new(
            config,
            join_df_publisher,
            service_available_response_publisher,
            start_ninja_df_publisher,
        )
        .await?;
        let mut subscriber = node
            .new_subscriber(dfs)
            .await
            .context(DatafeedSubscriberSnafu)?;

        tracing::debug!("Subscribing to service available request message");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(DatafeedSubscriberSnafu)?;

        tracing::debug!("Subscribing to service available response message");
        node.subscribe::<ServiceAvailableResponse>(
            &topics.get("service_available_response"),
            &mut subscriber,
        )
        .await
        .context(DatafeedSubscriberSnafu)?;

        tracing::debug!("Subscribing to run strategy message");
        node.subscribe::<RunStrategy>(&topics.get("run_strategy"), &mut subscriber)
            .await
            .context(DatafeedSubscriberSnafu)?;

        tracing::debug!("Subscribing to join datafeed success message");
        node.subscribe::<JoinDatafeedSuccess>(
            &topics.get("join_datafeed_success"),
            &mut subscriber,
        )
        .await
        .context(DatafeedSubscriberSnafu)?;

        sleep(Duration::from_millis(100));
        let service_available_req_publisher = node
            .new_publisher::<ServiceAvailableRequest>(&topics.get("service_available_request"))
            .await
            .context(DatafeedSubscriberSnafu)?;

        service_available_req_publisher
            .publish(
                (ServiceAvailableRequest {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                }),
            )
            .await
            .context(DatafeedSubscriberSnafu)?;

        Ok(Self { subscriber })
    }

    pub async fn join(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.subscriber.join().await?;
        Ok(())
    }
}

struct DatafeedBatch {
    pub running_strategies: HashSet<String>,
    pub start_df_buffer: HashMap<String, StartDatafeed>,
    pub join_df_buffer: HashMap<String, JoinDatafeed>,
    pub service_needs_met: HashMap<String, ServiceNeedsMet>,
}

struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

pub struct DatafeedSubscriber {
    provider_config_map: HashMap<DatafeedProvider, String>,
    join_df_publisher: Publisher<JoinDatafeed>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    start_ninja_df_publisher: Publisher<StartNinjaDatafeed>,
    datafeed_ip: String,
    datafeed_curr_id: u32,
    batch_buffer: HashMap<String, DatafeedBatch>,
    services_available: HashSet<String>,
    services_required: HashSet<String>,
    topics: TopicConfig,
    live_network: Option<Network>,
    live_node: Option<Node>,
    resim_feed: Option<Feed>,
    num_start_df: u32,
}

impl DatafeedSubscriber {
    pub async fn new(
        config: &Config,
        join_df_publisher: Publisher<JoinDatafeed>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        start_ninja_df_publisher: Publisher<StartNinjaDatafeed>,
    ) -> Result<Self, DatafeedServiceError> {
        let provider_config_map = config
            .datafeeds
            .iter()
            .map(|df| (df.provider.clone(), df.config.clone()))
            .collect();

        let topics = TopicConfig::from(&config.topics);

        let datafeed_ip = config.datafeed_ip.clone();
        let datafeed_port = utils::generate_zenoh_port();
        let mut live_node = None;
        let mut live_network = None;

        let mut services_required = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }

        if let Some(port) = datafeed_port {
            let nw = Network {
                ip: datafeed_ip.clone(),
                port: port as u32,
            };

            let mut builder = NodeBuilder::new();
            tracing::debug!(
                "Set up live datafeed network on ip: {}, port: {}",
                nw.ip,
                nw.port
            );
            builder.set_network((nw.ip.clone(), nw.port as u16));

            tracing::debug!("Set up live datafeed node on port: {}", nw.port);
            let node = builder.build().await.context(DatafeedSubscriberSnafu)?;
            live_node = Some(node);
            live_network = Some(nw);
        }

        Ok(Self {
            provider_config_map,
            join_df_publisher,
            service_available_resp_publisher,
            start_ninja_df_publisher,
            services_available: HashSet::new(),
            services_required,
            batch_buffer: HashMap::new(),
            datafeed_curr_id: 0,
            datafeed_ip,
            live_node,
            live_network,
            topics,
            resim_feed: None,
            num_start_df: 0,
        })
    }

    fn get_topic_list(
        topic_config: &TopicConfig,
        datafeed_stream: &Vec<DatafeedStream>,
        mode: &ExecutionMode,
        batch_id: &str,
        strategy_id: &str,
    ) -> Vec<TopicMtype> {
        if *mode == ExecutionMode::Backtest || *mode == ExecutionMode::Resim {
            Self::get_replay_topic_list(topic_config, batch_id, strategy_id)
        } else {
            let providers: Vec<(DatafeedProvider, String)> = datafeed_stream
                .iter()
                .filter_map(|df| {
                    let p = DatafeedProvider::try_from(df.provider);
                    match p {
                        Ok(provider) => Some((provider, df.symbol.clone())),
                        Err(_) => None,
                    }
                })
                .collect();

            let topics: Vec<Vec<TopicMtype>> = providers
                .iter()
                .map(|p| Self::get_live_topic_list(topic_config, &p.0, &p.1))
                .collect();

            topics.into_iter().flatten().collect()
        }
    }

    fn get_live_topic_list(
        topic_config: &TopicConfig,
        provider: &DatafeedProvider,
        symbol: &str,
    ) -> Vec<TopicMtype> {
        vec![
            TopicMtype {
                mtype: MessageType::Tick.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("tick"),
                    provider.to_string(),
                    symbol
                ),
            },
            TopicMtype {
                mtype: MessageType::Candle.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("candle"),
                    provider.to_string(),
                    symbol
                ),
            },
            TopicMtype {
                mtype: MessageType::HistoricalData.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("historical_data"),
                    provider.to_string(),
                    symbol
                ),
            },
            TopicMtype {
                mtype: MessageType::DatafeedComplete.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("datafeed_complete"),
                    provider.to_string(),
                    symbol
                ),
            },
        ]
    }

    fn get_replay_topic_list(
        topic_config: &TopicConfig,
        batch_id: &str,
        strategy_id: &str,
    ) -> Vec<TopicMtype> {
        vec![
            TopicMtype {
                mtype: MessageType::Tick.into(),
                topic: format!("{}_{}_{}", topic_config.get("tick"), batch_id, strategy_id),
            },
            TopicMtype {
                mtype: MessageType::Candle.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("candle"),
                    batch_id,
                    strategy_id
                ),
            },
            TopicMtype {
                mtype: MessageType::HistoricalData.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("historical_data"),
                    batch_id,
                    strategy_id
                ),
            },
            TopicMtype {
                mtype: MessageType::DatafeedComplete.into(),
                topic: format!(
                    "{}_{}_{}",
                    topic_config.get("datafeed_complete"),
                    batch_id,
                    strategy_id
                ),
            },
        ]
    }

    async fn setup_ninjatrader(
        live_network: &Network,
        batch_id: &str,
        strategy_id: &str,
        topics: &Vec<TopicMtype>,
        dfs: &DatafeedStream,
        publisher: &Publisher<StartNinjaDatafeed>,
    ) -> Result<(), SubscriberError> {
        tracing::debug!("Setup ninjatrader feed");

        let start_ninja_df = StartNinjaDatafeed {
            timestamp_ns: 0,
            batch_id: batch_id.to_string(),
            strategy_id: strategy_id.to_string(),
            symbol: dfs.symbol.clone(),
            candle_periods: dfs.candle_streams.iter().map(|c| c.period_s).collect(),
            network: Some(live_network.clone()),
            topics: topics
                .iter()
                .map(|t| Topic {
                    mtype: t.mtype.into(),
                    topic: t.topic.clone(),
                })
                .collect(),
        };

        publisher.publish(start_ninja_df).await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for DatafeedSubscriber {
    async fn on_data(&mut self, msg: ServiceAvailableRequest) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Service available request message received from: {}",
            msg.service_id
        );
        // let all running strategies know a new service is available
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
impl Subscribe<ServiceAvailableResponse> for DatafeedSubscriber {
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
impl Subscribe<RunStrategy> for DatafeedSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("Run Strategy message received");
        let start_datafeed = msg.start_datafeed.as_ref().unwrap();
        self.num_start_df += 1;
        let mode: ExecutionMode =
            ExecutionMode::try_from(msg.start_datafeed.as_ref().unwrap().exe_mode)?;
        let topics = Self::get_topic_list(
            &self.topics,
            &start_datafeed.datafeed,
            &start_datafeed.exe_mode(),
            &start_datafeed.batch_id,
            &start_datafeed.strategy_id,
        );

        let join_df = match mode {
            ExecutionMode::Backtest | ExecutionMode::Resim => {
                if self.resim_feed.is_none() || self.num_start_df % 100 == 0 {
                    let resim_port = utils::generate_zenoh_port();

                    if let Some(port) = resim_port {
                        let df_node = create_node(&self.datafeed_ip, port).await?;
                        self.resim_feed = Some(Feed {
                            node: df_node,
                            ip: self.datafeed_ip.clone(),
                            port,
                        });
                    } else {
                        tracing::error!("Error selecting zenoh port");
                        return Ok(());
                    }
                }

                Some(JoinDatafeed {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: start_datafeed.batch_id.clone(),
                    datafeed_id: self.datafeed_curr_id,
                    strategy_id: start_datafeed.strategy_id.clone(),
                    exe_mode: start_datafeed.exe_mode,
                    df_type: start_datafeed.df_type,
                    topics: topics
                        .iter()
                        .map(|t| Topic {
                            mtype: t.mtype.into(),
                            topic: t.topic.clone(),
                        })
                        .collect(),
                    network: Some(Network {
                        ip: self.datafeed_ip.clone(),
                        port: self.resim_feed.as_ref().unwrap().port as u32,
                    }),
                })
            }
            ExecutionMode::Live => {
                let topics = Self::get_topic_list(
                    &self.topics,
                    &start_datafeed.datafeed,
                    &start_datafeed.exe_mode(),
                    &start_datafeed.batch_id,
                    &start_datafeed.strategy_id,
                );
                if let Some(live_network) = &self.live_network {
                    let join_df = JoinDatafeed {
                        timestamp_ns: 0,
                        service_id: SERVICE_ID.to_string(),
                        batch_id: start_datafeed.batch_id.clone(),
                        datafeed_id: self.datafeed_curr_id,
                        strategy_id: start_datafeed.strategy_id.clone(),
                        exe_mode: start_datafeed.exe_mode,
                        df_type: start_datafeed.df_type,
                        topics: topics
                            .iter()
                            .map(|t| Topic {
                                mtype: t.mtype.into(),
                                topic: t.topic.clone(),
                            })
                            .collect(),
                        network: Some(live_network.clone()),
                    };

                    Some(join_df)
                } else {
                    None
                }
            }
        };

        if let Some(jdf) = join_df {
            tracing::debug!(
                "Publishing join datafeed message on ip: {}, port, {}",
                jdf.network.as_ref().unwrap().ip,
                jdf.network.as_ref().unwrap().port
            );
            self.join_df_publisher.publish(jdf.clone()).await?;
            self.datafeed_curr_id += 1;

            let batch = self
                .batch_buffer
                .entry(start_datafeed.batch_id.clone())
                .or_insert(DatafeedBatch {
                    running_strategies: HashSet::new(),
                    start_df_buffer: HashMap::new(),
                    join_df_buffer: HashMap::new(),
                    service_needs_met: HashMap::new(),
                });

            batch.service_needs_met.insert(
                start_datafeed.strategy_id.clone(),
                ServiceNeedsMet::new(&self.services_required, &self.services_available),
            );
            batch
                .join_df_buffer
                .insert(start_datafeed.strategy_id.clone(), jdf);
            batch.start_df_buffer.insert(
                start_datafeed.strategy_id.clone(),
                msg.start_datafeed.unwrap(),
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinDatafeedSuccess> for DatafeedSubscriber {
    async fn on_data(&mut self, msg: JoinDatafeedSuccess) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Start join datafeed success message received, service id: {} strategy id: {}",
            msg.service_id,
            msg.strategy_id
        );
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let (Some(service_needs_met), Some(start_df), Some(join_df)) = (
                batch.service_needs_met.get_mut(&msg.strategy_id),
                batch.start_df_buffer.get_mut(&msg.strategy_id),
                batch.join_df_buffer.get_mut(&msg.strategy_id),
            ) {
                service_needs_met.add_service_response(&msg.service_id);
                if service_needs_met.is_complete() {
                    tracing::debug!("Service needs met for strategy id: {}", msg.strategy_id);

                    let mode = ExecutionMode::try_from(start_df.exe_mode)?;
                    let nw = join_df.network.as_ref().unwrap();
                    match mode {
                        ExecutionMode::Backtest | ExecutionMode::Resim => {
                            tracing::debug!("Strategy is in backtest/resim mode");
                            let mut builder = NodeBuilder::new();
                            tracing::debug!(
                                "Set up datafeed resim/backtest network on ip: {}, port: {}",
                                nw.ip,
                                nw.port
                            );
                            builder.set_network((nw.ip.clone(), nw.port as u16));

                            tracing::debug!("Set up datafeed node on port: {}", nw.port);
                            let node = builder.build().await.context(DatafeedSubscriberSnafu)?;

                            // TODO: Only support futures datafeed, but when we support more df_type
                            // will need to be used
                            let _df_type = DatafeedType::try_from(start_df.df_type)?;

                            let exe_mode = ExecutionMode::try_from(start_df.exe_mode)?;
                            let provider: DatafeedProvider =
                                DatafeedProvider::try_from(start_df.datafeed[0].provider)?;

                            let mut df_builder = DatafeedBuilder::new(join_df.datafeed_id);

                            if let Some(config_path) = self.provider_config_map.get(&provider) {
                                df_builder.set_config_path(&config_path);
                            }

                            df_builder.set_exe_mode(&exe_mode);
                            df_builder.set_start_date(start_df.start_date());

                            let topics = Self::get_replay_topic_list(
                                &self.topics,
                                &msg.batch_id,
                                &msg.strategy_id,
                            );

                            match provider {
                                DatafeedProvider::Parquet => {
                                    let parquet = ParquetDatafeed::default();
                                    let mut datafeed = df_builder.build(parquet);
                                    datafeed.setup_datafeeds(&start_df.datafeed);
                                    datafeed.setup_publishers(&node, &topics).await?;
                                    datafeed.run()?;
                                    batch.running_strategies.insert(msg.strategy_id.clone());
                                }
                                DatafeedProvider::S3 => {
                                    let s3 = S3Datafeed::default();
                                    let mut datafeed = df_builder.build(s3);
                                    datafeed.setup_datafeeds(&start_df.datafeed);
                                    datafeed.setup_publishers(&node, &topics).await?;
                                    datafeed.run()?;
                                    batch.running_strategies.insert(msg.strategy_id.clone());
                                }
                                _ => (),
                            };
                        }
                        ExecutionMode::Live => {
                            for dfs in &start_df.datafeed {
                                let provider: DatafeedProvider =
                                    DatafeedProvider::try_from(dfs.provider)?;

                                let mut df_builder = DatafeedBuilder::new(self.datafeed_curr_id);

                                if let Some(config_path) = self.provider_config_map.get(&provider) {
                                    df_builder.set_config_path(config_path);
                                }

                                df_builder.set_exe_mode(&start_df.exe_mode());

                                let topics =
                                    Self::get_live_topic_list(&self.topics, &provider, &dfs.symbol);

                                match provider {
                                    DatafeedProvider::NinjaTrader => {
                                        // Ninjatrader is a special case where the datafeed doesnt come from socket or rest api,
                                        // it comes from an externalNinjatrader server via zenoh connection.
                                        if let Some(live_network) = &self.live_network {
                                            Self::setup_ninjatrader(
                                                live_network,
                                                &msg.batch_id,
                                                &msg.strategy_id,
                                                &topics,
                                                dfs,
                                                &self.start_ninja_df_publisher,
                                            )
                                            .await?;
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

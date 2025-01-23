use crate::config::Config;
use crate::logger::Logger;
use crate::orders::OrderTable;
use crate::overall_pnl::OverallPnlSubscriber;
use crate::strategy_pnl::StrategyPnlSubscriber;
use chrono::{Datelike, Days, NaiveDate, Weekday};
use common::config::{RunConfig, ServiceConfig, TopicConfig, TopicMtype};
use common::utils::{self, ServiceNeedsMet};
use futures::stream;
use futures::StreamExt;
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;
use tradebot_protos::messages::datafeed_stream::{CandleStream, HistoricalDataConfig};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Advice, AlgoChart, AppRequest, AppResponse, Batch, Candle, Chart, ChartRequest, DatafeedStream,
    HistoricalData, JoinAlgofeed, JoinAlgofeedSuccess, JoinDatafeed, JoinDatafeedSuccess,
    JoinLoggerfeed, JoinOrderfeed, JoinOrderfeedSuccess, JoinOverallPnlfeed, JoinStrategyPnlfeed,
    JoinStrategyPnlfeedSuccess, Line, LogRequest, Network, Order, OrderFilled, OrderList,
    OverallDayStats, OverallDayStatsList, OverallRequest, Point, PositionStats, ReadFromDirRequest,
    Realized, Rectangle, RunStrategy, RunYaml, ServiceAvailableRequest, ServiceAvailableResponse,
    StartAlgo, StartBatch, StartBroker, StartDatafeed, Strategy, SymbolPeriods, Tick, Topic,
    TotalPnl, TotalPnlRealized, Unrealized,
};
use utils::create_node;
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError};

use async_trait::async_trait;

use crate::algofeed::ChartAlgofeed;
use crate::datafeed::ChartDatafeed;

const SERVICE_ID: &str = "app-service";

// Wait on these services to respond to start publishing app feed, if they are available
const SERVICE_NEEDS_LIST: &[&str] = &[
    "algo-service",
    "datafeed-service",
    "order-service",
    "pnl-service",
];

#[derive(Debug, Snafu)]
pub enum AppServiceError {
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

    #[snafu(display("Error setting up app subscriber"))]
    AppSubscriberError { source: NodeError },

    #[snafu(display("Unable to convert date string"))]
    ConvertDateStrError { source: chrono::format::ParseError },
}

pub struct AppService {
    subscriber: Subscriber<AppSubscriber>,
}

impl AppService {
    pub async fn new(config: &Config) -> Result<Self, AppServiceError> {
        // Read service config yaml file so we can subscribe to service feed
        let f = std::fs::read_to_string(&config.service_config_path).context(YamlParserSnafu {
            filename: &config.service_config_path,
        })?;
        let service_config: ServiceConfig = serde_yaml::from_str(&f).context(YamlStringSnafu {
            filename: &config.service_config_path,
        })?;

        let ip = service_config.ip;
        let port = service_config.port;

        tracing::debug!("Creating app service");
        let mut builder = NodeBuilder::new();
        if let Some(config) = &service_config.zenoh_config_path {
            tracing::debug!("Setting zenoh config path: {}", config);
            builder.set_config_path(config);
        }

        builder.set_network((ip.clone(), port));
        let node = builder.build().await.context(AppSubscriberSnafu)?;

        let topics = if let Some(topics) = &service_config.topics {
            TopicConfig::from(topics)
        } else {
            TopicConfig::default()
        };

        // Create publishers on the service node. Use topics from service config if they exist, otherwise use hard coded topic
        let join_df_success_publisher = node
            .new_publisher(&topics.get("join_datafeed_success"))
            .await
            .context(AppSubscriberSnafu)?;

        let join_algofeed_success_publisher = node
            .new_publisher(&topics.get("join_algofeed_success"))
            .await
            .context(AppSubscriberSnafu)?;

        let join_orderfeed_success_publisher = node
            .new_publisher(&topics.get("join_orderfeed_success"))
            .await
            .context(AppSubscriberSnafu)?;

        let join_strategy_pnlfeed_success_publisher = node
            .new_publisher(&topics.get("join_strategy_pnlfeed_success"))
            .await
            .context(AppSubscriberSnafu)?;

        let strategies_resp_publisher = node
            .new_publisher(&topics.get("app_response"))
            .await
            .context(AppSubscriberSnafu)?;

        let strategies_from_dir_publisher = node
            .new_publisher(&topics.get("read_from_dir_request"))
            .await
            .context(AppSubscriberSnafu)?;

        let run_strategy_publisher = node
            .new_publisher::<RunStrategy>(&topics.get("run_strategy"))
            .await
            .context(AppSubscriberSnafu)?;

        let start_batch_publisher = node
            .new_publisher::<StartBatch>(&topics.get("start_batch"))
            .await
            .context(AppSubscriberSnafu)?;

        let strategies_req_publisher: Publisher<AppRequest> = node
            .new_publisher::<AppRequest>(&topics.get("app_request"))
            .await
            .context(AppSubscriberSnafu)?;

        let service_available_resp_publisher = node
            .new_publisher::<ServiceAvailableResponse>(&topics.get("service_available_response"))
            .await
            .context(AppSubscriberSnafu)?;

        // Create app subscriber with required publishers
        let apps = AppSubscriber::new(
            config,
            start_batch_publisher,
            run_strategy_publisher,
            strategies_resp_publisher,
            strategies_from_dir_publisher,
            strategies_req_publisher,
            service_available_resp_publisher,
            join_df_success_publisher,
            join_algofeed_success_publisher,
            join_orderfeed_success_publisher,
            join_strategy_pnlfeed_success_publisher,
        )
        .await?;
        let mut subscriber = node
            .new_subscriber(apps)
            .await
            .context(AppSubscriberSnafu)?;

        // Subscribe to required service messages on service node. Use topics from service config if they exist, otherwise use hard coded topic
        tracing::debug!("Subscribe to service available request");
        node.subscribe::<ServiceAvailableRequest>(
            &topics.get("service_available_request"),
            &mut subscriber,
        )
        .await
        .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join datafeed");
        node.subscribe::<JoinDatafeed>(&topics.get("join_datafeed"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join algofeed");
        node.subscribe::<JoinAlgofeed>(&topics.get("join_algofeed"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join orderfeed");
        node.subscribe::<JoinOrderfeed>(&topics.get("join_orderfeed"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join overall pnlfeed");
        node.subscribe::<JoinOverallPnlfeed>(&topics.get("join_overall_pnlfeed"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join strategy pnlfeed");
        node.subscribe::<JoinStrategyPnlfeed>(
            &topics.get("join_strategy_pnlfeed"),
            &mut subscriber,
        )
        .await
        .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to join loggerfeed");
        node.subscribe::<JoinLoggerfeed>(&topics.get("join_loggerfeed"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to app request");
        node.subscribe::<AppRequest>(&topics.get("app_request"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to chart request");
        node.subscribe::<ChartRequest>(&topics.get("chart_request"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to log request");
        node.subscribe::<LogRequest>(&topics.get("log_request"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to overall request");
        node.subscribe::<OverallRequest>(&topics.get("overall_request"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        tracing::debug!("Subscribe to run yaml request");
        node.subscribe::<RunYaml>(&topics.get("run_yaml"), &mut subscriber)
            .await
            .context(AppSubscriberSnafu)?;

        sleep(Duration::from_millis(100));

        // Request for service available discovery
        let service_available_req_publisher: Publisher<_> = node
            .new_publisher::<ServiceAvailableRequest>(&topics.get("service_available_request"))
            .await
            .context(AppSubscriberSnafu)?;

        service_available_req_publisher
            .publish(
                (ServiceAvailableRequest {
                    timestamp_ns: 0,
                    service_id: SERVICE_ID.to_string(),
                }),
            )
            .await
            .context(AppSubscriberSnafu)?;

        Ok(Self { subscriber })
    }

    pub async fn join(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.subscriber.join().await?;
        Ok(())
    }
}

// Helper struct to hold chart request publishers for datafeed and algofeed
struct AppChartReq {
    datafeed_req_publisher: Option<Publisher<ChartRequest>>,
    algofeed_req_publisher: Option<Publisher<ChartRequest>>,
}

impl AppChartReq {
    pub fn new() -> Self {
        Self {
            datafeed_req_publisher: None,
            algofeed_req_publisher: None,
        }
    }

    pub fn set_datafeed_req_publisher(&mut self, publisher: Publisher<ChartRequest>) {
        self.datafeed_req_publisher = Some(publisher);
    }

    pub fn set_algofeed_req_publisher(&mut self, publisher: Publisher<ChartRequest>) {
        self.algofeed_req_publisher = Some(publisher);
    }
}

// Buffer entry for this Batch
struct BatchBufferEntry {
    app_chart_req_map: HashMap<String, AppChartReq>, // Mapped by strategy ID
    start_datafeed_buffer: HashMap<String, StartDatafeed>, // Mapped by strategy ID
    overall_pnl_req_publisher: Option<Publisher<OverallRequest>>, //Overall pnl request publisher for this batch
    num_strategies_requested: i32,
    strategy_ids: Vec<String>,
    service_needs_met: HashMap<String, ServiceNeedsMet>, // Mapped by strategy ID
    subscribers: Vec<Box<dyn Abort>>,
}

// Zenoh connection feed
struct Feed {
    node: Node,
    ip: String,
    port: u16,
}

pub struct AppSubscriber {
    // Required publishers
    start_batch_publisher: Publisher<StartBatch>,
    run_strategy_publisher: Publisher<RunStrategy>,
    strategies_resp_publisher: Publisher<AppResponse>,
    strategies_from_dir_req_publisher: Publisher<ReadFromDirRequest>,
    strategies_req_publisher: Publisher<AppRequest>,
    service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
    join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
    join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
    join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
    join_strategy_pnlfeed_success_publisher: Publisher<JoinStrategyPnlfeedSuccess>,
    topics: TopicConfig,
    batch_buffer: HashMap<String, BatchBufferEntry>,
    logger_subscriber: Option<Box<dyn Abort>>, // Subscriber for logger feed
    services_available: HashSet<String>,
    services_required: HashSet<String>,
    // Current selected batch and strategy by app
    active_strategy: Option<String>,
    active_batch: Option<String>,
    // App connection that app feed gets published to
    app_ip: String,
    app_port: Option<u16>,
    app_node: Option<Node>,
    // Zenoh connections for the different feeds needed by the app
    algofeed: Option<Feed>,
    datafeed: Option<Feed>,
    orderfeed: Option<Feed>,
    pnlfeed: Option<Feed>,
}

impl AppSubscriber {
    pub async fn new(
        config: &Config,
        start_batch_publisher: Publisher<StartBatch>,
        run_strategy_publisher: Publisher<RunStrategy>,
        strategies_resp_publisher: Publisher<AppResponse>,
        strategies_from_dir_req_publisher: Publisher<ReadFromDirRequest>,
        strategies_req_publisher: Publisher<AppRequest>,
        service_available_resp_publisher: Publisher<ServiceAvailableResponse>,
        join_df_success_publisher: Publisher<JoinDatafeedSuccess>,
        join_algofeed_success_publisher: Publisher<JoinAlgofeedSuccess>,
        join_orderfeed_success_publisher: Publisher<JoinOrderfeedSuccess>,
        join_strategy_pnlfeed_success_publisher: Publisher<JoinStrategyPnlfeedSuccess>,
    ) -> Result<Self, AppServiceError> {
        let topics = TopicConfig::from(&config.topics);

        let app_port = utils::generate_zenoh_port();

        let app_node = if let Some(port) = app_port {
            tracing::debug!("Set up app zenoh node");
            let app_node = create_node(&config.app_ip, port)
                .await
                .context(AppSubscriberSnafu)?;
            Some(app_node)
        } else {
            None
        };

        let mut services_required = HashSet::new();

        for s in SERVICE_NEEDS_LIST {
            let service_str = *s;
            services_required.insert(service_str.to_string());
        }

        Ok(Self {
            start_batch_publisher,
            run_strategy_publisher,
            strategies_resp_publisher,
            strategies_from_dir_req_publisher,
            service_available_resp_publisher,
            batch_buffer: HashMap::new(),
            topics,
            app_ip: config.app_ip.clone(),
            app_port,
            active_strategy: None,
            active_batch: None,
            strategies_req_publisher,
            join_df_success_publisher,
            join_algofeed_success_publisher,
            join_orderfeed_success_publisher,
            join_strategy_pnlfeed_success_publisher,
            app_node,
            logger_subscriber: None,
            services_available: HashSet::new(),
            services_required,
            algofeed: None,
            datafeed: None,
            orderfeed: None,
            pnlfeed: None,
        })
    }

    // All the topics that the app feed will publish to the tauri app
    fn get_app_topic_list(topic_config: &TopicConfig) -> Vec<TopicMtype> {
        vec![
            TopicMtype {
                mtype: MessageType::Chart.into(),
                topic: topic_config.get("chart"),
            },
            TopicMtype {
                mtype: MessageType::OrderList.into(),
                topic: topic_config.get("order_list"),
            },
            TopicMtype {
                mtype: MessageType::AlgoChart.into(),
                topic: topic_config.get("algo_chart"),
            },
            TopicMtype {
                mtype: MessageType::Candle.into(),
                topic: topic_config.get("candle_update"),
            },
            TopicMtype {
                mtype: MessageType::ChartRequest.into(),
                topic: topic_config.get("chart_request"),
            },
            TopicMtype {
                mtype: MessageType::Point.into(),
                topic: topic_config.get("point"),
            },
            TopicMtype {
                mtype: MessageType::Rectangle.into(),
                topic: topic_config.get("rectangle"),
            },
            TopicMtype {
                mtype: MessageType::Advice.into(),
                topic: topic_config.get("advice"),
            },
            TopicMtype {
                mtype: MessageType::Order.into(),
                topic: topic_config.get("order"),
            },
            TopicMtype {
                mtype: MessageType::OrderFilled.into(),
                topic: topic_config.get("order_filled"),
            },
            TopicMtype {
                mtype: MessageType::TotalPnl.into(),
                topic: topic_config.get("total_pnl"),
            },
            TopicMtype {
                mtype: MessageType::TotalPnlRealized.into(),
                topic: topic_config.get("total_pnl_realized"),
            },
            TopicMtype {
                mtype: MessageType::TotalPnlUnrealized.into(),
                topic: topic_config.get("total_pnl_unrealized"),
            },
            TopicMtype {
                mtype: MessageType::PositionPnlRealized.into(),
                topic: topic_config.get("pos_pnl_realized"),
            },
            TopicMtype {
                mtype: MessageType::PositionPnlRealizedList.into(),
                topic: topic_config.get("pos_pnl_realized_list"),
            },
            TopicMtype {
                mtype: MessageType::PositionPnlUnrealized.into(),
                topic: topic_config.get("pos_pnl_unrealized"),
            },
            TopicMtype {
                mtype: MessageType::PositionStats.into(),
                topic: topic_config.get("position_stats"),
            },
            TopicMtype {
                mtype: MessageType::OverallStats.into(),
                topic: topic_config.get("overall_stats"),
            },
            TopicMtype {
                mtype: MessageType::PnlCalendar.into(),
                topic: topic_config.get("pnl_calendar"),
            },
            TopicMtype {
                mtype: MessageType::OverallDayStats.into(),
                topic: topic_config.get("overall_day_stats"),
            },
            TopicMtype {
                mtype: MessageType::PnlHour.into(),
                topic: topic_config.get("pnl_hour"),
            },
        ]
    }
}

#[async_trait]
impl Subscribe<ServiceAvailableRequest> for AppSubscriber {
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
        tracing::debug!("Publish response message");
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
impl Subscribe<ServiceAvailableResponse> for AppSubscriber {
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
impl Subscribe<RunStrategy> for AppSubscriber {
    async fn on_data(&mut self, msg: RunStrategy) -> Result<(), SubscriberError> {
        tracing::debug!("Run Strategy message received");

        // Latch StartDatafeed information to buffer, no need to start datafeed app feed until we get a corresponding
        // join datafeed message to subscribe to
        if let Some(batch) = self
            .batch_buffer
            .get_mut(&msg.start_datafeed.as_ref().unwrap().batch_id)
        {
            batch.start_datafeed_buffer.insert(
                msg.start_datafeed.as_ref().unwrap().strategy_id.clone(),
                msg.start_datafeed.unwrap(),
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinDatafeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinDatafeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join datafeed message received");

        // Get batch buffer entry for this batch id
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let app_chart_req = batch
                .app_chart_req_map
                .entry(msg.strategy_id.clone())
                .or_insert(AppChartReq::new());

            let service_needs_met = batch
                .service_needs_met
                .entry(msg.strategy_id.clone())
                .or_insert(ServiceNeedsMet::new(
                    &self.services_required,
                    &self.services_available,
                ));

            service_needs_met.add_service_response(&msg.service_id);
            if service_needs_met.is_complete() {
                // When all strategies for this batch have their needs met, then let the app
                // know so it can stop the loading icon
                if batch.num_strategies_requested == 0 {
                    self.strategies_req_publisher
                        .publish(AppRequest {
                            timestamp_ns: 0,
                            user_id: String::new(),
                        })
                        .await?;
                }
            }

            let df_nw = msg.network.as_ref().unwrap(); // unwrap safe here

            // Only create a datafeed node when we don't have one already or the join datafeed message has a
            // different ip and port as previous. Currently ip and port switch every 100 new strategies but this
            // may change in the future
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

            let df_node = &self.datafeed.as_ref().unwrap().node;
            let df_topics: Vec<TopicMtype> = msg
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

            // This chart request publisher is for when the user selects this strategy id to view. When this happens we need to
            // let the node know that it needs to publish its chart to the tauri app. So we will use this publisher to facilitate
            // that communication
            if app_chart_req.datafeed_req_publisher.is_none() {
                let publisher = df_node
                    .new_publisher::<ChartRequest>(&self.topics.get("chart_request"))
                    .await?;

                app_chart_req.set_datafeed_req_publisher(publisher);
            }

            // Candle update publisher for new candles
            let candle_update_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("candle_update"))
                        .await?,
                )
            } else {
                None
            };

            // Chart publisher for publishing entire datafeed chart
            let chart_publisher = if let Some(app_node) = &self.app_node {
                Some(app_node.new_publisher(&self.topics.get("chart")).await?)
            } else {
                None
            };

            let chart_datafeed =
                ChartDatafeed::new(&msg.strategy_id, candle_update_publisher, chart_publisher);

            let mut df_subscriber = df_node.new_subscriber(chart_datafeed).await?;

            // Subscribe to datafeed topics
            for topic in df_topics {
                match topic.mtype {
                    MessageType::Tick => {
                        df_node
                            .subscribe::<Tick>(&topic.topic, &mut df_subscriber)
                            .await?;
                    }
                    MessageType::Candle => {
                        df_node
                            .subscribe::<Candle>(&topic.topic, &mut df_subscriber)
                            .await?;
                    }
                    MessageType::HistoricalData => {
                        df_node
                            .subscribe::<HistoricalData>(&topic.topic, &mut df_subscriber)
                            .await?;
                    }
                    _ => (),
                }
            }

            // Subscribe to chart request from the tauri app
            df_node
                .subscribe::<ChartRequest>(&self.topics.get("chart_request"), &mut df_subscriber)
                .await?;

            batch.subscribers.push(Box::new(df_subscriber));

            self.join_df_success_publisher
                .publish(JoinDatafeedSuccess {
                    timestamp_ns: msg.timestamp_ns,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: msg.batch_id.clone(),
                    strategy_id: msg.strategy_id.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinAlgofeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinAlgofeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join algofeed message received");

        // Get batch buffer entry for this batch id
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let app_comm = batch
                .app_chart_req_map
                .entry(msg.strategy_id.clone())
                .or_insert(AppChartReq::new());

            let service_needs_met = batch
                .service_needs_met
                .entry(msg.strategy_id.clone())
                .or_insert(ServiceNeedsMet::new(
                    &self.services_required,
                    &self.services_available,
                ));

            service_needs_met.add_service_response(&msg.service_id);
            if service_needs_met.is_complete() {
                batch.num_strategies_requested -= 1;

                // When all strategies for this batch have their needs met, then let the app
                // know so it can stop the loading icon
                if batch.num_strategies_requested == 0 {
                    self.strategies_req_publisher
                        .publish(AppRequest {
                            timestamp_ns: 0,
                            user_id: String::new(),
                        })
                        .await?;
                }
            }

            let af_nw = msg.network.unwrap(); // unwrap safe here

            // Only create a datafeed node when we don't have one already or the join datafeed message has a
            // different ip and port as previous. Currently ip and port switch every 100 new strategies but this
            // may change in the future
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

            // This chart request publisher is for when the user selects this strategy id to view. When this happens we need to
            // let the node know that it needs to publish its chart to the tauri app. So we will use this publisher to facilitate
            // that communication
            if app_comm.algofeed_req_publisher.is_none() {
                let cr_publisher = algo_node
                    .new_publisher::<ChartRequest>(&self.topics.get("chart_request"))
                    .await?;

                app_comm.set_algofeed_req_publisher(cr_publisher);
            }

            // Point publisher for new points on algofeed
            let point_publisher = if let Some(app_node) = &self.app_node {
                Some(app_node.new_publisher(&self.topics.get("point")).await?)
            } else {
                None
            };

            // Rectangle publisher for rectangles on algofeed
            let rectangle_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("rectangle"))
                        .await?,
                )
            } else {
                None
            };

            // Advice publisher for advices on algofeed
            let advice_publisher = if let Some(app_node) = &self.app_node {
                Some(app_node.new_publisher(&self.topics.get("advice")).await?)
            } else {
                None
            };

            // Chart publisher for publishing entire algofeed chart
            let algo_chart_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("algo_chart"))
                        .await?,
                )
            } else {
                None
            };

            let chart_algofeed = ChartAlgofeed::new(
                &msg.strategy_id,
                point_publisher,
                rectangle_publisher,
                advice_publisher,
                algo_chart_publisher,
            );

            let mut algo_subscriber = algo_node.new_subscriber(chart_algofeed).await?;

            // Subscribe to algofeed topics
            for topic in &msg.topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::Point => {
                        algo_node
                            .subscribe::<Point>(&topic.topic, &mut algo_subscriber)
                            .await?;
                    }
                    MessageType::Line => {
                        algo_node
                            .subscribe::<Line>(&topic.topic, &mut algo_subscriber)
                            .await?;
                    }
                    MessageType::Advice => {
                        algo_node
                            .subscribe::<Advice>(&topic.topic, &mut algo_subscriber)
                            .await?;
                    }
                    MessageType::Rectangle => {
                        algo_node
                            .subscribe::<Rectangle>(&topic.topic, &mut algo_subscriber)
                            .await?;
                    }
                    _ => (),
                }
            }

            // Subscribe to chart request from the tauri app
            algo_node
                .subscribe::<ChartRequest>(&self.topics.get("chart_request"), &mut algo_subscriber)
                .await?;

            batch.subscribers.push(Box::new(algo_subscriber));

            self.join_algofeed_success_publisher
                .publish(JoinAlgofeedSuccess {
                    timestamp_ns: msg.timestamp_ns,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: msg.batch_id.clone(),
                    strategy_id: msg.strategy_id.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOrderfeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinOrderfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join orderfeed message received");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let _app_comm = batch
                .app_chart_req_map
                .entry(msg.strategy_id.clone())
                .or_insert(AppChartReq::new());

            let service_needs_met = batch
                .service_needs_met
                .entry(msg.strategy_id.clone())
                .or_insert(ServiceNeedsMet::new(
                    &self.services_required,
                    &self.services_available,
                ));

            service_needs_met.add_service_response(&msg.service_id);
            if service_needs_met.is_complete() {
                batch.num_strategies_requested -= 1;

                // When all strategies for this batch have their needs met, then let the app
                // know so it can stop the loading icon
                if batch.num_strategies_requested == 0 {
                    self.strategies_req_publisher
                        .publish(AppRequest {
                            timestamp_ns: 0,
                            user_id: String::new(),
                        })
                        .await?;
                }
            }

            // order list publisher for current order list
            let order_list_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("order_list"))
                        .await?,
                )
            } else {
                None
            };

            // new order publisher
            let order_publisher = if let Some(app_node) = &self.app_node {
                Some(app_node.new_publisher(&self.topics.get("order")).await?)
            } else {
                None
            };

            // new order filled publisher
            let order_filled_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("order_filled"))
                        .await?,
                )
            } else {
                None
            };

            let of_nw = msg.network.unwrap(); //unwrap safe here

            // Only create a orderfeed node when we don't have one already or the join orderfeed message has a
            // different ip and port as previous. Currently ip and port is mapped to algofeed so it will change when
            // that changes. This may change in the future
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

            tracing::debug!("Set up algo zenoh node");
            let order_node = &self.orderfeed.as_ref().unwrap().node;

            let order_table = OrderTable::new(
                &msg.strategy_id,
                order_publisher,
                order_list_publisher,
                order_filled_publisher,
            );

            let mut ot_subscriber = order_node.new_subscriber(order_table).await?;

            // Subscribe to orderfeed topics
            for topic in &msg.topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::Order => {
                        order_node
                            .subscribe::<Order>(&topic.topic, &mut ot_subscriber)
                            .await?;
                    }
                    MessageType::OrderFilled => {
                        order_node
                            .subscribe::<OrderFilled>(&topic.topic, &mut ot_subscriber)
                            .await?;
                    }
                    _ => (),
                }
            }

            // Subscribe to chart request from the tauri app
            order_node
                .subscribe::<ChartRequest>(&self.topics.get("chart_request"), &mut ot_subscriber)
                .await?;

            batch.subscribers.push(Box::new(ot_subscriber));

            self.join_orderfeed_success_publisher
                .publish(JoinOrderfeedSuccess {
                    timestamp_ns: msg.timestamp_ns,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: msg.batch_id.clone(),
                    strategy_id: msg.strategy_id.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinStrategyPnlfeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinStrategyPnlfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join strategy pnlfeed message received");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let app_comm = batch
                .app_chart_req_map
                .entry(msg.strategy_id.clone())
                .or_insert(AppChartReq::new());

            let service_needs_met = batch
                .service_needs_met
                .entry(msg.strategy_id.clone())
                .or_insert(ServiceNeedsMet::new(
                    &self.services_required,
                    &self.services_available,
                ));

            service_needs_met.add_service_response(&msg.service_id);
            if service_needs_met.is_complete() {
                batch.num_strategies_requested -= 1;

                // When all strategies for this batch have their needs met, then let the app
                // know so it can stop the loading icon
                if batch.num_strategies_requested == 0 {
                    self.strategies_req_publisher
                        .publish(AppRequest {
                            timestamp_ns: 0,
                            user_id: String::new(),
                        })
                        .await?;
                }
            }

            let pnl_nw = msg.network.unwrap();

            // Only create a strategy pnlfeed node when we don't have one already or the join pnlfeed message has a
            // different ip and port as previous. Currently ip and port is mapped to algofeed so it will change when
            // that changes. This may change in the future
            if self.pnlfeed.is_none()
                || self.pnlfeed.as_ref().unwrap().ip != pnl_nw.ip
                || self.pnlfeed.as_ref().unwrap().port != pnl_nw.port as u16
            {
                let p_node = create_node(&pnl_nw.ip, pnl_nw.port as u16).await?;
                self.pnlfeed = Some(Feed {
                    node: p_node,
                    ip: pnl_nw.ip.clone(),
                    port: pnl_nw.port as u16,
                });
            }

            tracing::debug!("Set up algo zenoh node");
            let pnl_node = &self.pnlfeed.as_ref().unwrap().node;

            if app_comm.algofeed_req_publisher.is_none() {
                let cr_publisher = pnl_node
                    .new_publisher::<ChartRequest>(&self.topics.get("chart_request"))
                    .await?;

                app_comm.set_algofeed_req_publisher(cr_publisher);
            }

            // Total pnl publisher
            let pnl_line_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("total_pnl"))
                        .await?,
                )
            } else {
                None
            };

            // New total pnl realized publisher
            let pnl_realized_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("total_pnl_realized"))
                        .await?,
                )
            } else {
                None
            };

            // New total pnl unrealized publisher
            let pnl_unrealized_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("total_pnl_unrealized"))
                        .await?,
                )
            } else {
                None
            };

            // New position pnl realized publisher
            let pos_realized_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("pos_pnl_realized"))
                        .await?,
                )
            } else {
                None
            };

            // New position pnl unrealized publisher
            let pos_unrealized_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("pos_pnl_unrealized"))
                        .await?,
                )
            } else {
                None
            };

            // New position total stats publisher
            let position_stats_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("position_stats"))
                        .await?,
                )
            } else {
                None
            };

            // Realized list publisher
            let realized_list_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("pos_pnl_realized_list"))
                        .await?,
                )
            } else {
                None
            };

            let strategy_pnl = StrategyPnlSubscriber::new(
                &msg.strategy_id,
                pnl_line_publisher,
                pnl_realized_publisher,
                pnl_unrealized_publisher,
                pos_realized_publisher,
                pos_unrealized_publisher,
                position_stats_publisher,
                realized_list_publisher,
            );
            let mut pnl_subscriber = pnl_node.new_subscriber(strategy_pnl).await?;

            // Subscribe to messages for the strategies pnl
            for topic in &msg.topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::Realized => {
                        tracing::debug!("Subscribe to realized message, topic: {}", &topic.topic);
                        pnl_node
                            .subscribe::<Realized>(&topic.topic, &mut pnl_subscriber)
                            .await?;
                    }
                    MessageType::Unrealized => {
                        tracing::debug!("Subscribe to unrealized message, topic: {}", &topic.topic);
                        pnl_node
                            .subscribe::<Unrealized>(&topic.topic, &mut pnl_subscriber)
                            .await?;
                    }
                    MessageType::PositionStats => {
                        tracing::debug!(
                            "Subscribe to position stats message, topic: {}",
                            &topic.topic
                        );
                        pnl_node
                            .subscribe::<PositionStats>(&topic.topic, &mut pnl_subscriber)
                            .await?;
                    }
                    _ => (),
                }
            }

            // Subscribe to chart request from the tauri app
            pnl_node
                .subscribe::<ChartRequest>(&self.topics.get("chart_request"), &mut pnl_subscriber)
                .await?;

            batch.subscribers.push(Box::new(pnl_subscriber));

            self.join_strategy_pnlfeed_success_publisher
                .publish(JoinStrategyPnlfeedSuccess {
                    timestamp_ns: msg.timestamp_ns,
                    service_id: SERVICE_ID.to_string(),
                    batch_id: msg.batch_id.clone(),
                    strategy_id: msg.strategy_id.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinOverallPnlfeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinOverallPnlfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join overall pnlfeed message received");

        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            let pnl_nw = msg.network.as_ref().unwrap();
            let pnl_node: Node = create_node(&pnl_nw.ip, pnl_nw.port as u16).await?;

            if batch.overall_pnl_req_publisher.is_none() {
                let cr_publisher = pnl_node
                    .new_publisher::<OverallRequest>(&self.topics.get("overall_request"))
                    .await?;

                batch.overall_pnl_req_publisher = Some(cr_publisher);
            }

            // Overall stats publisher for this batch
            let batch_overall_stats_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("overall_stats"))
                        .await?,
                )
            } else {
                None
            };

            // Pnl Calendar publisher for this batch
            let pnl_calendar_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("pnl_calendar"))
                        .await?,
                )
            } else {
                None
            };

            // Pnl by hour publisher for this bitch
            let pnl_by_hour_publisher = if let Some(app_node) = &self.app_node {
                Some(app_node.new_publisher(&self.topics.get("pnl_hour")).await?)
            } else {
                None
            };

            // Pnl by day stats publisher for this batch
            let overall_stats_by_day_publisher = if let Some(app_node) = &self.app_node {
                Some(
                    app_node
                        .new_publisher(&self.topics.get("overall_day_stats"))
                        .await?,
                )
            } else {
                None
            };

            let overall_pnl_subscriber = OverallPnlSubscriber::new(
                &msg.batch_id,
                batch_overall_stats_publisher,
                pnl_calendar_publisher,
                overall_stats_by_day_publisher,
                pnl_by_hour_publisher,
            );
            let mut subscriber = pnl_node.new_subscriber(overall_pnl_subscriber).await?;

            // Subscribe to overall pnlfeed topics
            for topic in &msg.topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::OverallDayStats => {
                        tracing::debug!(
                            "Subscribe to overall stats message, topic: {}",
                            &topic.topic
                        );
                        pnl_node
                            .subscribe::<OverallDayStats>(&topic.topic, &mut subscriber)
                            .await?;
                    }
                    _ => (),
                }
            }

            // Subscribe to chart request from the tauri app
            pnl_node
                .subscribe::<OverallRequest>(&self.topics.get("overall_request"), &mut subscriber)
                .await?;

            batch.subscribers.push(Box::new(subscriber));
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<JoinLoggerfeed> for AppSubscriber {
    async fn on_data(&mut self, msg: JoinLoggerfeed) -> Result<(), SubscriberError> {
        tracing::debug!("Join loggerfeed message received");

        // Join loggerfeed for messages publisher when loading a batch/strategy from a log
        if let Some(app_node) = &self.app_node {
            // There should only be one loggerfeed so abort the current one if there is one
            if let Some(logger_subscriber) = &self.logger_subscriber {
                logger_subscriber.abort();
            }
            let logger_nw = msg.network.as_ref().unwrap();
            let logger_node: Node = create_node(&logger_nw.ip, logger_nw.port as u16).await?;

            // Publishers for converting loggerfeed messages to messages the tauri app subscribes to
            let pnl_line_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("total_pnl"))
                    .await?,
            );
            let pnl_realized_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("total_pnl_realized"))
                    .await?,
            );
            let position_stats_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("position_stats"))
                    .await?,
            );

            let overall_stats_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("overall_stats"))
                    .await?,
            );

            let chart_publisher = Some(app_node.new_publisher(&self.topics.get("chart")).await?);

            let algo_chart_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("algo_chart"))
                    .await?,
            );

            let order_list_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("order_list"))
                    .await?,
            );

            let pnl_calendar_publisher = Some(
                app_node
                    .new_publisher(&self.topics.get("pnl_calendar"))
                    .await?,
            );

            let pnl_by_hour_publisher =
                Some(app_node.new_publisher(&self.topics.get("pnl_hour")).await?);

            let logger_subscriber = Logger::new(
                chart_publisher,
                algo_chart_publisher,
                order_list_publisher,
                pnl_line_publisher,
                pnl_realized_publisher,
                position_stats_publisher,
                overall_stats_publisher,
                pnl_calendar_publisher,
                pnl_by_hour_publisher,
            );

            let mut subscriber = logger_node.new_subscriber(logger_subscriber).await.unwrap();

            // Subscribe to loggerfeed topics
            for topic in &msg.topics {
                let mtype = MessageType::try_from(topic.mtype)?;
                match mtype {
                    MessageType::Chart => {
                        println!("Subscribe to chart");
                        logger_node
                            .subscribe::<Chart>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::AlgoChart => {
                        println!("Subscribe to algo chart");
                        logger_node
                            .subscribe::<AlgoChart>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::OrderList => {
                        println!("Subscribe to order list, topic: {}", topic.topic);
                        logger_node
                            .subscribe::<OrderList>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::TotalPnl => {
                        println!("Subscribe to pnl line, topic: {}", topic.topic);
                        logger_node
                            .subscribe::<TotalPnl>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::TotalPnlRealized => {
                        println!("Subscribe to total pnl realized, topic: {}", topic.topic);
                        logger_node
                            .subscribe::<TotalPnlRealized>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::PositionStats => {
                        println!("Subscribe to position stats , topic: {}", topic.topic);
                        logger_node
                            .subscribe::<PositionStats>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    MessageType::OverallDayStatsList => {
                        println!(
                            "Subscribe to overall day stats list , topic: {}",
                            topic.topic
                        );
                        logger_node
                            .subscribe::<OverallDayStatsList>(&topic.topic, &mut subscriber)
                            .await
                            .unwrap();
                    }
                    _ => (),
                }
            }

            self.logger_subscriber = Some(Box::new(subscriber));
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<RunYaml> for AppSubscriber {
    async fn on_data(&mut self, msg: RunYaml) -> Result<(), SubscriberError> {
        tracing::debug!("RunYaml message received");
        // RunYaml message is for when the user selects a run yaml file from the tauri app
        // Read the yaml file to get the run parameters
        let f = std::fs::read_to_string(msg.yaml_path)?;
        let config: RunConfig = serde_yaml::from_str(&f)?;

        // Abort any subscribers if this batch id already exists
        if let Some(batch) = self.batch_buffer.get_mut(&config.batch_id) {
            for sub in &batch.subscribers {
                sub.abort();
            }
        }

        self.batch_buffer.insert(
            config.batch_id.clone(),
            BatchBufferEntry {
                start_datafeed_buffer: HashMap::new(),
                app_chart_req_map: HashMap::new(),
                service_needs_met: HashMap::new(),
                strategy_ids: Vec::new(),
                num_strategies_requested: 0,
                overall_pnl_req_publisher: None,
                subscribers: Vec::new(),
            },
        );

        if let Some(batch) = self.batch_buffer.get_mut(&config.batch_id) {
            // Convert strategies in the run config to the RunStrategy message that all services subscribe to
            for strategy in &config.stratigies {
                // get all dates from start to end for resim/backtest mode
                let dates = get_dates(&strategy.start_date, &strategy.end_date)?;

                let stream = stream::iter(dates);
                futures::pin_mut!(stream);

                // For each date, create a RunStrategy message and publish
                while let Some(date) = stream.next().await {
                    let datafeeds = strategy
                        .datafeeds
                        .iter()
                        .map(|df| DatafeedStream {
                            timestamp_ns: 0,
                            symbol: df.symbol.clone(),
                            enable_ticks: df.enable_ticks,
                            candle_streams: df
                                .candle_streams
                                .iter()
                                .map(|cs| CandleStream {
                                    period_s: cs.period_s,
                                    historical_data: if let Some(hd) = &cs.historical_config {
                                        Some(HistoricalDataConfig {
                                            num_days: hd.num_days,
                                        })
                                    } else {
                                        None
                                    },
                                })
                                .collect(),
                            provider: df.datafeed_provider.into(),
                        })
                        .collect();

                    // StartDatafeed message for this strategy
                    let start_datafeed = StartDatafeed {
                        batch_id: config.batch_id.clone(),
                        strategy_id: if let Some(id) = &strategy.strategy_id {
                            id.clone()
                        } else {
                            format!(
                                "{}_{}",
                                strategy.algo_id.to_string(),
                                date.clone().unwrap_or_default(),
                            )
                        },
                        start_date: date.clone(),
                        df_type: strategy.df_type.into(),
                        exe_mode: strategy.exe_mode.into(),
                        timestamp_ns: 0,
                        datafeed: datafeeds,
                    };

                    batch
                        .start_datafeed_buffer
                        .insert(start_datafeed.strategy_id.clone(), start_datafeed.clone());

                    // StartAlgofeed message for this strategy
                    let start_algo = StartAlgo {
                        timestamp_ns: 0,
                        algo_id: strategy.algo_id.into(),
                        batch_id: config.batch_id.clone(),
                        strategy_id: if let Some(id) = &strategy.strategy_id {
                            id.clone()
                        } else {
                            format!(
                                "{}_{}",
                                strategy.algo_id.to_string(),
                                date.clone().unwrap_or_default(),
                            )
                        },
                        starting_balance: 50000.0,
                        exe_mode: strategy.exe_mode.into(),
                        account_ids: strategy.account_ids.clone().unwrap_or_default(),
                    };

                    // StartBrokerfeed message for this strategy
                    let start_broker = StartBroker {
                        timestamp_ns: 0,
                        broker_id: strategy.broker_id.into(),
                        batch_id: config.batch_id.clone(),
                        strategy_id: if let Some(id) = &strategy.strategy_id {
                            id.clone()
                        } else {
                            format!(
                                "{}_{}",
                                strategy.algo_id.to_string(),
                                date.clone().unwrap_or_default()
                            )
                        },
                        exe_mode: strategy.exe_mode.into(),
                    };

                    batch.num_strategies_requested += 1;
                    batch
                        .strategy_ids
                        .push(strategy.strategy_id.clone().unwrap_or(format!(
                            "{}_{}",
                            strategy.algo_id.to_string(),
                            date.clone().unwrap_or_default(),
                        )));
                    tracing::debug!("start run strategy publish");

                    // StartBrokerfeed message for this strategy
                    self.run_strategy_publisher
                        .publish(RunStrategy {
                            timestamp_ns: msg.timestamp_ns,
                            start_algo: Some(start_algo),
                            start_datafeed: Some(start_datafeed),
                            start_broker: Some(start_broker),
                        })
                        .await?;
                    tracing::debug!("end run strategy publish");
                }
            }

            self.start_batch_publisher
                .publish(StartBatch {
                    timestamp_ns: 0,
                    batch_id: config.batch_id.clone(),
                    strategy_ids: batch.strategy_ids.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AppRequest> for AppSubscriber {
    async fn on_data(&mut self, msg: AppRequest) -> Result<(), SubscriberError> {
        // AppRequest message is for when the user first loads the tauri app. When app is first loaded,
        // the app needs to know which batches and strategies are available for viewing
        tracing::debug!("App request message received");
        if let Some(app_port) = &self.app_port {
            // Create app response message with current batches and strategies
            let mut strategies_response = AppResponse {
                timestamp_ns: msg.timestamp_ns,
                user_id: msg.user_id.clone(),
                batches: Vec::new(),
                network: Some(Network {
                    ip: self.app_ip.clone(),
                    port: *app_port as u32,
                }),
                topics: Self::get_app_topic_list(&self.topics)
                    .iter()
                    .map(|t| Topic {
                        mtype: t.mtype.into(),
                        topic: t.topic.clone(),
                    })
                    .collect(),
            };

            for (batch_id, app_batch) in &self.batch_buffer {
                let mut batch = Batch {
                    batch_id: batch_id.to_string(),
                    strategies: Vec::new(),
                };

                for (strategy_id, start_df) in &app_batch.start_datafeed_buffer {
                    tracing::debug!("Populate strategy");
                    let mut strategy = Strategy {
                        strategy_id: strategy_id.clone(),
                        symbol_periods: Vec::new(),
                    };

                    for df in &start_df.datafeed {
                        let mut sp = SymbolPeriods::default();
                        sp.symbol = df.symbol.clone();
                        for c in &df.candle_streams {
                            sp.period_s.push(c.period_s);
                        }

                        strategy.symbol_periods.push(sp);
                    }

                    batch.strategies.push(strategy);
                }

                strategies_response.batches.push(batch);
            }

            tracing::debug!("Publish app response");
            self.strategies_resp_publisher
                .publish(strategies_response)
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<ChartRequest> for AppSubscriber {
    async fn on_data(&mut self, msg: ChartRequest) -> Result<(), SubscriberError> {
        tracing::debug!("Chart request message received by service");

        // Let active app feed know that new chart request is coming in so it can stop its publishing
        if let (Some(active_batch), Some(active_strategy)) =
            (&self.active_batch, &self.active_strategy)
        {
            if let Some(batch) = self.batch_buffer.get_mut(active_batch) {
                if let Some(app_comm) = batch.app_chart_req_map.get_mut(active_strategy) {
                    if let Some(df_publisher) = &app_comm.datafeed_req_publisher {
                        df_publisher.publish(msg.clone()).await?;
                    } else {
                        tracing::debug!("Datafeed request publisher not set");
                    }

                    if let Some(algo_publisher) = &app_comm.algofeed_req_publisher {
                        algo_publisher.publish(msg.clone()).await?;
                    } else {
                        tracing::debug!("Algofeed request publisher not set");
                    }
                }
            }
        }

        // Let new app feed know that new chart request is coming in so it can start publishing
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(app_comm) = batch.app_chart_req_map.get_mut(&msg.strategy_id) {
                self.active_batch = Some(msg.batch_id.clone());
                self.active_strategy = Some(msg.strategy_id.clone());
                if let Some(df_publisher) = &app_comm.datafeed_req_publisher {
                    df_publisher.publish(msg.clone()).await?;
                } else {
                    tracing::debug!("Datafeed request publisher not set");
                }

                if let Some(algo_publisher) = &app_comm.algofeed_req_publisher {
                    algo_publisher.publish(msg.clone()).await?;
                } else {
                    tracing::debug!("Algofeed request publisher not set");
                }
            } else {
                if msg.strategy_id.len() > 0 {
                    tracing::error!("Failed to find app communication helper");
                // when an empty chart request is being sent, this means the refresh button was pressed to need to
                // tell active strategy to stop publishing
                // TODO: separate out to different message when reset is being requested
                } else {
                    if let Some(active_strategy) = &self.active_strategy {
                        if let Some(app_comm) = batch.app_chart_req_map.get_mut(active_strategy) {
                            if let Some(df_publisher) = &app_comm.datafeed_req_publisher {
                                df_publisher.publish(msg.clone()).await?;
                            } else {
                                tracing::debug!("Datafeed request publisher not set");
                            }

                            if let Some(algo_publisher) = &app_comm.algofeed_req_publisher {
                                algo_publisher.publish(msg.clone()).await?;
                            } else {
                                tracing::debug!("Algofeed request publisher not set");
                            }
                        }
                    }

                    self.active_strategy = None;
                    self.active_batch = None;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<LogRequest> for AppSubscriber {
    async fn on_data(&mut self, msg: LogRequest) -> Result<(), SubscriberError> {
        // User is requesting to load from a log so need to request the logger service to
        // publish all strategies assosicated with the selected logging directory
        tracing::debug!("Log request message received by service");
        self.strategies_from_dir_req_publisher
            .publish(ReadFromDirRequest {
                timestamp_ns: msg.timestamp_ns,
                log_dir: msg.log_dir_path,
            })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Subscribe<OverallRequest> for AppSubscriber {
    async fn on_data(&mut self, msg: OverallRequest) -> Result<(), SubscriberError> {
        // User is requesting to view the overall page for this batch so need to tell the overall pnl
        // nodes to start publishing their feed
        tracing::debug!("OverallRequest request message received by service");
        if let Some(batch) = self.batch_buffer.get_mut(&msg.batch_id) {
            if let Some(overall_publisher) = &batch.overall_pnl_req_publisher {
                overall_publisher.publish(msg.clone()).await?;
            } else {
                tracing::debug!("Overall request publisher not set");
            }
        }

        Ok(())
    }
}

#[inline]
fn get_dates(
    start_date: &Option<String>,
    end_date: &Option<String>,
) -> Result<Vec<Option<String>>, AppServiceError> {
    let mut dates = vec![start_date.clone()];

    if let Some(sd) = &start_date {
        if let Some(ed) = end_date {
            let mut curr_date = sd.clone();

            while curr_date != *ed {
                let dt =
                    NaiveDate::parse_from_str(&curr_date, "%Y%m%d").context(ConvertDateStrSnafu)?;
                if let Some(add_dt) = dt.checked_add_days(Days::new(1)) {
                    let add_str = add_dt.format("%Y%m%d").to_string();
                    if add_dt.weekday() != Weekday::Sat && add_dt.weekday() != Weekday::Sun {
                        let add_str = add_dt.format("%Y%m%d").to_string();
                        dates.push(Some(add_str.clone()));
                    }
                    curr_date = add_str;
                }
            }
        }
    }

    Ok(dates)
}

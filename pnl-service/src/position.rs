use crate::config::FuturesTickConfig;
use crate::manager::PnlManager;
use async_trait::async_trait;
use common::config::{TopicConfig, TopicMtype};
use common::utils::adjust_topic;
use std::collections::HashMap;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Advice, AlgoComplete, OrderFilled, OrderfeedComplete, PnlComplete, PositionStats, Realized,
    Tick, Unrealized,
};
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, SubscriberError};

pub struct PositionSubscriber {
    batch_id: String,
    strategy_id: String,
    algo: (Node, Vec<TopicMtype>),
    datafeed: (Node, Vec<TopicMtype>),
    subscribers: HashMap<String, Box<dyn Abort>>,
    pnl_manager: PnlManager,
    realized_publisher: Publisher<Realized>,
    position_stats_publisher: Publisher<PositionStats>,
    pnl_complete_publisher: Publisher<PnlComplete>,
    overall_pnl_realized_publisher: Option<Publisher<Realized>>,
    overall_pnl_stats_publisher: Option<Publisher<PositionStats>>,
    overall_pnl_complete_publisher: Option<Publisher<PnlComplete>>,
    topics: TopicConfig,
    futures_config: Option<HashMap<String, FuturesTickConfig>>,
    unrealized_topic: String,
}

impl PositionSubscriber {
    pub async fn new(
        batch_id: &str,
        strategy_id: &str,
        algo: (Node, &Vec<TopicMtype>),
        datafeed: (Node, &Vec<TopicMtype>),
        topics: &TopicConfig,
        overall_pnl_realized_publisher: Option<Publisher<Realized>>,
        overall_pnl_stats_publisher: Option<Publisher<PositionStats>>,
        overall_pnl_complete_publisher: Option<Publisher<PnlComplete>>,
    ) -> Result<Self, SubscriberError> {
        let realized_publisher = algo
            .0
            .new_publisher(&adjust_topic(
                &topics.get("realized"),
                vec![batch_id, strategy_id],
            ))
            .await?;
        let position_stats_publisher = algo
            .0
            .new_publisher(&adjust_topic(
                &topics.get("position_stats"),
                vec![batch_id, strategy_id],
            ))
            .await?;
        let pnl_complete_publisher = algo
            .0
            .new_publisher(&adjust_topic(
                &topics.get("pnl_complete"),
                vec![batch_id, strategy_id],
            ))
            .await?;

        let unrealized_topic =
            &adjust_topic(&topics.get("unrealized"), vec![batch_id, strategy_id]);

        Ok(Self {
            batch_id: batch_id.to_string(),
            strategy_id: strategy_id.to_string(),
            datafeed: (datafeed.0, datafeed.1.clone()),
            algo: (algo.0, algo.1.clone()),
            subscribers: HashMap::new(),
            pnl_manager: PnlManager::new(batch_id, strategy_id),
            realized_publisher,
            position_stats_publisher,
            pnl_complete_publisher,
            topics: topics.clone(),
            overall_pnl_realized_publisher,
            overall_pnl_stats_publisher,
            overall_pnl_complete_publisher,
            futures_config: None,
            unrealized_topic: unrealized_topic.to_string(),
        })
    }

    pub fn set_futures_config(&mut self, futures_config: &HashMap<String, FuturesTickConfig>) {
        self.futures_config = Some(futures_config.clone());
        self.pnl_manager.set_futures_config(futures_config);
    }
}

#[async_trait]
impl Subscribe<OrderFilled> for PositionSubscriber {
    async fn on_data(&mut self, msg: OrderFilled) -> Result<(), SubscriberError> {
        tracing::debug!("Order filled message received");
        let position_realized = self.pnl_manager.order_filled(&msg);

        // If position is realized then publish new realized pnl
        if position_realized.0 {
            let (realized_ts, realized) = self.pnl_manager.get_last_realized(&msg.symbol);

            tracing::debug!(
                "Realized position: {}, strategy_id: {}, order id: {}",
                realized,
                self.strategy_id,
                msg.order_id
            );

            self.realized_publisher
                .publish(Realized {
                    order_id: position_realized.1,
                    timestamp_ns: msg.timestamp_ns,
                    realized,
                })
                .await?;

            let position_stats =
                self.pnl_manager
                    .get_position_stats(msg.timestamp_ns, realized_ts, realized);
            self.position_stats_publisher
                .publish(position_stats.clone())
                .await?;

            if let Some(publisher) = &self.overall_pnl_realized_publisher {
                publisher
                    .publish(Realized {
                        order_id: position_realized.1,
                        timestamp_ns: msg.timestamp_ns,
                        realized,
                    })
                    .await?;
            }

            if let Some(publisher) = &self.overall_pnl_stats_publisher {
                tracing::debug!(
                    "Publish position stats to overall pnl subscriber: {}",
                    position_stats.timestamp_ns
                );
                publisher.publish(position_stats).await?;
            }
        }

        // Anytime we get an order fill, we need to stop the unrealized subscriber.
        // We either start a new one with the updated size/avg price or abort when a position is closed
        if let Some(subscriber) = self.subscribers.get(&msg.symbol) {
            subscriber.abort()
        }
        self.subscribers.remove(&msg.symbol);

        // If position is still active, then we need to start a new unrealized subscriber to track the
        // unrealized profit/loss
        if self.pnl_manager.is_active(&msg.symbol) {
            let size = self.pnl_manager.get_size(&msg.symbol);
            let price = self.pnl_manager.get_avg_price(&msg.symbol);

            tracing::debug!(
                "Set up unrealized subscriber, position_id: {}, order id: {}",
                position_realized.1,
                msg.order_id
            );
            let unrealized_publisher = self.algo.0.new_publisher(&self.unrealized_topic).await?;
            let mut unrealized_subscriber = UnrealizedSubscriber::new(
                price,
                size,
                msg.order_id,
                position_realized.1,
                unrealized_publisher,
            );

            if let Some(futures_config) = &self.futures_config {
                unrealized_subscriber.set_futures_config(futures_config);
            }

            let mut subscriber = self
                .datafeed
                .0
                .new_subscriber(unrealized_subscriber)
                .await?;

            let tick_topic = self
                .datafeed
                .1
                .iter()
                .find(|bt| bt.mtype == MessageType::Tick);

            if let Some(topic) = tick_topic {
                tracing::debug!("Subscribe datafeed, id: {}", msg.order_id);
                self.datafeed
                    .0
                    .subscribe(&topic.topic, &mut subscriber)
                    .await?;
            }

            self.subscribers
                .insert(msg.symbol.clone(), Box::new(subscriber));
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Advice> for PositionSubscriber {
    async fn on_data(&mut self, msg: Advice) -> Result<(), SubscriberError> {
        tracing::debug!("Algo order message received");
        let position_realized = self.pnl_manager.order(&msg);

        // If position is realized then publish new realized pnl
        // NOTE: There is no unrealized pnl tracking when using Advices to calculate pnl
        if position_realized.0 {
            let (realized_ts, realized) = self.pnl_manager.get_last_realized(&msg.symbol);

            tracing::debug!("Realized position: {}", realized);

            self.realized_publisher
                .publish(Realized {
                    order_id: position_realized.1,
                    timestamp_ns: msg.timestamp_ns,
                    realized,
                })
                .await?;

            let position_stats =
                self.pnl_manager
                    .get_position_stats(msg.timestamp_ns, realized_ts, realized);

            tracing::debug!("Position stats: {:?}", position_stats);
            self.position_stats_publisher
                .publish(position_stats.clone())
                .await?;

            if let Some(publisher) = &self.overall_pnl_realized_publisher {
                publisher
                    .publish(Realized {
                        order_id: position_realized.1,
                        timestamp_ns: msg.timestamp_ns,
                        realized,
                    })
                    .await?;
            }

            if let Some(publisher) = &self.overall_pnl_stats_publisher {
                publisher.publish(position_stats).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<AlgoComplete> for PositionSubscriber {
    async fn on_data(&mut self, msg: AlgoComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Algo complete message received");
        self.pnl_complete_publisher
            .publish(PnlComplete {
                timestamp_ns: msg.timestamp_ns,
                batch_id: self.batch_id.clone(),
                strategy_id: self.strategy_id.clone(),
            })
            .await?;

        if let Some(overall_pnl_complete) = &self.overall_pnl_complete_publisher {
            overall_pnl_complete
                .publish(PnlComplete {
                    timestamp_ns: msg.timestamp_ns,
                    batch_id: self.batch_id.clone(),
                    strategy_id: self.strategy_id.clone(),
                })
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderfeedComplete> for PositionSubscriber {
    async fn on_data(&mut self, msg: OrderfeedComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Orderfeed complete message received");
        self.pnl_complete_publisher
            .publish(PnlComplete {
                timestamp_ns: msg.timestamp_ns,
                batch_id: self.batch_id.clone(),
                strategy_id: self.strategy_id.clone(),
            })
            .await?;

        if let Some(overall_pnl_complete) = &self.overall_pnl_complete_publisher {
            overall_pnl_complete
                .publish(PnlComplete {
                    timestamp_ns: msg.timestamp_ns,
                    batch_id: self.batch_id.clone(),
                    strategy_id: self.strategy_id.clone(),
                })
                .await?;
        }
        Ok(())
    }
}

struct UnrealizedSubscriber {
    pub order_id: u32,
    pub position_id: u32,
    pub size: i32,
    pub avg_price: f32,
    pub unrealized_publisher: Publisher<Unrealized>,
    pub futures_config: Option<HashMap<String, FuturesTickConfig>>,
}

impl UnrealizedSubscriber {
    pub fn new(
        avg_price: f32,
        size: i32,
        order_id: u32,
        position_id: u32,
        unrealized_publisher: Publisher<Unrealized>,
    ) -> Self {
        Self {
            size,
            avg_price,
            order_id,
            position_id,
            unrealized_publisher,
            futures_config: None,
        }
    }

    pub fn set_futures_config(&mut self, futures_config: &HashMap<String, FuturesTickConfig>) {
        self.futures_config = Some(futures_config.clone());
    }
}

#[async_trait]
impl Subscribe<Tick> for UnrealizedSubscriber {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        // Use current tick price to calculate unrealized pnl
        let mut diff = msg.price - self.avg_price;
        if let Some(futures_config) = &self.futures_config {
            if let Some(tick_config) = futures_config.get(&msg.symbol) {
                diff = (diff / tick_config.tick_size) * tick_config.tick_value;
            }
        }
        let unrealized = diff * self.size as f32;

        self.unrealized_publisher
            .publish(Unrealized {
                timestamp_ns: msg.timestamp_ns,
                unrealized,
                order_id: self.position_id,
            })
            .await?;
        Ok(())
    }
}

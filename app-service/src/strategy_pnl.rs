use std::collections::HashMap;

use async_trait::async_trait;
use tradebot_protos::messages::{
    ChartRequest, PnlValue, PositionPnlRealized, PositionPnlRealizedList, PositionPnlUnrealized,
    PositionStats, Realized, TotalPnl, TotalPnlRealized, TotalPnlUnrealized, Unrealized,
};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

// Strategy pnlfeed converted to position stats, realized and unrealized profit for viewing in the app
pub struct StrategyPnlSubscriber {
    pub strategy_id: String,
    pub publish: bool,
    pub total_pnl: TotalPnl,
    pub position_realized_list: Vec<PositionPnlRealized>,
    pub position_stats: PositionStats,
    pub total_pnl_publisher: Option<Publisher<TotalPnl>>,
    pub pnl_realized_publisher: Option<Publisher<TotalPnlRealized>>,
    pub pnl_unrealized_publisher: Option<Publisher<TotalPnlUnrealized>>,
    pub pos_realized_publisher: Option<Publisher<PositionPnlRealized>>,
    pub pos_unrealized_publisher: Option<Publisher<PositionPnlUnrealized>>,
    pub position_stats_publisher: Option<Publisher<PositionStats>>,
    pub realized_list_publisher: Option<Publisher<PositionPnlRealizedList>>,
    pub total_realized: f32,
    pub position_map: HashMap<u32, bool>,
}

impl StrategyPnlSubscriber {
    pub fn new(
        strategy_id: &str,
        total_pnl_publisher: Option<Publisher<TotalPnl>>,
        pnl_realized_publisher: Option<Publisher<TotalPnlRealized>>,
        pnl_unrealized_publisher: Option<Publisher<TotalPnlUnrealized>>,
        pos_realized_publisher: Option<Publisher<PositionPnlRealized>>,
        pos_unrealized_publisher: Option<Publisher<PositionPnlUnrealized>>,
        position_stats_publisher: Option<Publisher<PositionStats>>,
        realized_list_publisher: Option<Publisher<PositionPnlRealizedList>>,
    ) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            total_pnl: TotalPnl {
                timestamp_ns: 0,
                points: Vec::new(),
            },
            position_stats: PositionStats::default(),
            position_realized_list: Vec::new(),
            publish: false,
            total_pnl_publisher,
            pnl_realized_publisher,
            pnl_unrealized_publisher,
            pos_realized_publisher,
            pos_unrealized_publisher,
            position_stats_publisher,
            realized_list_publisher,
            total_realized: 0.0_f32,
            position_map: HashMap::new(),
        }
    }
}

#[async_trait]
impl Subscribe<ChartRequest> for StrategyPnlSubscriber {
    async fn on_data(&mut self, msg: ChartRequest) -> Result<(), SubscriberError> {
        // An chart request is a request to view strategy stats by strategy id
        tracing::debug!("Chart request message received by strategy pnl");
        tracing::debug!(
            "Strategy pnl strategy id: {}, msg: {}",
            msg.strategy_id,
            self.strategy_id
        );

        // If strategy id matches this one, then publish combined position stats, total pnl, and realized profit list
        if msg.strategy_id == self.strategy_id {
            self.publish = true;

            if let Some(total_pnl_publisher) = &self.total_pnl_publisher {
                total_pnl_publisher.publish(self.total_pnl.clone()).await?;
            }

            if let Some(position_stats_publisher) = &self.position_stats_publisher {
                position_stats_publisher
                    .publish(self.position_stats.clone())
                    .await?;
            }

            if let Some(realized_list_publisher) = &self.realized_list_publisher {
                realized_list_publisher
                    .publish(PositionPnlRealizedList {
                        timestamp_ns: msg.timestamp_ns,
                        realized_list: self.position_realized_list.clone(),
                    })
                    .await?;
            }
        } else {
            tracing::debug!("Set publish to false");
            self.publish = false;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Realized> for StrategyPnlSubscriber {
    async fn on_data(&mut self, msg: Realized) -> Result<(), SubscriberError> {
        // New profit realized so update total pnl, total realized, and current position realized
        tracing::debug!(
            "Realized message received, realized value: {}",
            msg.realized
        );
        self.total_pnl.points.push(PnlValue {
            timestamp_ns: msg.timestamp_ns,
            value: msg.realized + self.total_realized,
        });

        self.total_realized += msg.realized;
        tracing::debug!("Total realized value: {}", self.total_realized);

        let position_pnl_realized = PositionPnlRealized {
            timestamp_ns: msg.timestamp_ns,
            value: Some(PnlValue {
                timestamp_ns: msg.timestamp_ns,
                value: msg.realized,
            }),
            position_id: msg.order_id,
        };

        self.position_realized_list
            .push(position_pnl_realized.clone());

        // Only publish when chart request says to
        if self.publish {
            if let Some(pnl_realized_publisher) = &self.pnl_realized_publisher {
                pnl_realized_publisher
                    .publish(TotalPnlRealized {
                        timestamp_ns: msg.timestamp_ns,
                        value: Some(PnlValue {
                            timestamp_ns: msg.timestamp_ns,
                            value: self.total_realized,
                        }),
                        position_id: msg.order_id,
                    })
                    .await?;
            }

            if let Some(pos_realized_publisher) = &self.pos_realized_publisher {
                pos_realized_publisher
                    .publish(position_pnl_realized)
                    .await?;
            }
        }
        self.position_map.insert(msg.order_id, false);
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Unrealized> for StrategyPnlSubscriber {
    async fn on_data(&mut self, msg: Unrealized) -> Result<(), SubscriberError> {
        // New profit unrealized so update total pnl unrealized and position unrealized
        if self.publish {
            let position_active = self.position_map.entry(msg.order_id).or_insert(true);

            if *position_active {
                tracing::debug!(
                    "Unrealized message received, realized value: {}, position_id: {}",
                    msg.unrealized,
                    msg.order_id,
                );
                if let Some(pnl_unrealized_publisher) = &self.pnl_unrealized_publisher {
                    pnl_unrealized_publisher
                        .publish(TotalPnlUnrealized {
                            timestamp_ns: msg.timestamp_ns,
                            value: Some(PnlValue {
                                timestamp_ns: msg.timestamp_ns,
                                value: self.total_realized + msg.unrealized,
                            }),
                            position_id: msg.order_id,
                        })
                        .await?;
                }

                if let Some(pos_unrealized_publisher) = &self.pos_unrealized_publisher {
                    pos_unrealized_publisher
                        .publish(PositionPnlUnrealized {
                            timestamp_ns: msg.timestamp_ns,
                            value: Some(PnlValue {
                                timestamp_ns: msg.timestamp_ns,
                                value: msg.unrealized,
                            }),
                            position_id: msg.order_id,
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<PositionStats> for StrategyPnlSubscriber {
    async fn on_data(&mut self, msg: PositionStats) -> Result<(), SubscriberError> {
        // New position stats means new position stats realized so need to update the app view if chart request says to
        tracing::debug!("PositionStats received");
        self.position_stats = msg.clone();
        if self.publish {
            if let Some(position_stats_publisher) = &self.position_stats_publisher {
                tracing::debug!("PositionStats publish");
                position_stats_publisher.publish(msg).await?;
            }
        }
        Ok(())
    }
}

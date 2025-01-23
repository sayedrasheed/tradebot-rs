use tradebot_protos::messages::order::OrderType;
use tradebot_protos::messages::{BrokerComplete, Order, OrderFilled, OrderfeedComplete};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;

use crate::broker::BrokerSetup;

#[derive(Debug, Snafu)]
pub enum PaperTraderError {
    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Unable to open {filename}"))]
    SerdeYamlError {
        source: serde_yaml::Error,
        filename: String,
    },

    #[snafu(display("Failed to publish data"))]
    PublishError { source: NodeError },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PaperTraderConfig {
    pub market_order_slippage: f32,
}

pub struct PaperTrader {
    broker_id: u32,
    config: Option<PaperTraderConfig>,
    order_filled_publisher: Option<Publisher<OrderFilled>>,
    broker_complete_publisher: Option<Publisher<BrokerComplete>>,
}

impl PaperTrader {
    pub async fn new(config_path: Option<&String>) -> Result<Self, PaperTraderError> {
        let config = if let Some(config) = &config_path {
            let f = std::fs::read_to_string(&config)
                .context(ReadFileToStringSnafu { filename: *config })?;
            let config: PaperTraderConfig =
                serde_yaml::from_str(&f).context(SerdeYamlSnafu { filename: *config })?;

            Some(config)
        } else {
            None
        };

        Ok(Self {
            broker_id: 0, // TODO: no real need to a broker id we can just use batch id strategy id combo
            config,
            order_filled_publisher: None,
            broker_complete_publisher: None,
        })
    }
}

#[async_trait]
impl Subscribe<Order> for PaperTrader {
    async fn on_data(&mut self, msg: Order) -> Result<(), SubscriberError> {
        // Paper trader will just receive an order and relay an order filled message with some simulation
        // of slippage
        tracing::debug!("Received order message, id: {}", msg.order_id);

        let order_type: OrderType = OrderType::try_from(msg.order_type)?;
        match order_type {
            OrderType::MarketOrder => {
                if let Some(of_publisher) = &self.order_filled_publisher {
                    of_publisher
                        .publish(OrderFilled {
                            timestamp_ns: msg.timestamp_ns,
                            symbol: msg.symbol.clone(),
                            order_id: msg.order_id,
                            strategy_id: msg.strategy_id.clone(),
                            order_type: 0,
                            price: match &self.config {
                                Some(config) => {
                                    msg.price
                                        + if msg.size > 0 {
                                            config.market_order_slippage
                                        } else {
                                            config.market_order_slippage / -1.0
                                        } // simple algo to simulate slippage
                                }
                                None => msg.price,
                            },
                            size: msg.size,
                        })
                        .await?;
                }
            }
            _ => tracing::warn!("Paper trader only supports Market Orders"),
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderfeedComplete> for PaperTrader {
    async fn on_data(&mut self, msg: OrderfeedComplete) -> Result<(), SubscriberError> {
        // When orderfeed is complete then brokerfeed should complete
        tracing::debug!(
            "Received orderfeed complete message, id: {}",
            msg.strategy_id
        );

        if let Some(publisher) = &self.broker_complete_publisher {
            publisher
                .publish(BrokerComplete {
                    timestamp_ns: msg.timestamp_ns,
                    broker_id: self.broker_id,
                })
                .await?;
        }

        Ok(())
    }
}

impl BrokerSetup for PaperTrader {
    fn set_id(&mut self, id: u32) {
        self.broker_id = id;
    }

    fn set_order_filled_publisher(&mut self, publisher: Publisher<OrderFilled>) {
        self.order_filled_publisher = Some(publisher);
    }

    fn set_broker_complete_publisher(&mut self, publisher: Publisher<BrokerComplete>) {
        self.broker_complete_publisher = Some(publisher);
    }
}

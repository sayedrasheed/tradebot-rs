use async_trait::async_trait;
use tradebot_protos::messages::advice::OrderType;
use tradebot_protos::messages::order::OrderStatus;
use tradebot_protos::messages::{Advice, AlgoComplete, Order, OrderfeedComplete};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

// Currently the advice subscribers will just subscribe to the Advice message from an algo and convert it to an Order. It
// seems pointless right now since Algos can just publish Orders, but in the future the order subscribers will also handle
// limit orders and have their own order publishing algos ie splitting a big order into smaller orders etc

pub struct AdviceSubscriber {
    batch_id: String,
    strategy_id: String,
    account_ids: Vec<String>,
    order_publisher: Publisher<Order>,
    orderfeed_complete_publisher: Publisher<OrderfeedComplete>,
    order_id: u32,
}

impl AdviceSubscriber {
    pub async fn new(
        batch_id: &str,
        strategy_id: &str,
        account_ids: &Vec<String>,
        order_publisher: Publisher<Order>,
        orderfeed_complete_publisher: Publisher<OrderfeedComplete>,
    ) -> Result<Self, SubscriberError> {
        Ok(Self {
            batch_id: batch_id.to_string(),
            strategy_id: strategy_id.to_string(),
            order_publisher,
            orderfeed_complete_publisher,
            order_id: 0,
            account_ids: account_ids.clone(),
        })
    }
}

#[async_trait]
impl Subscribe<Advice> for AdviceSubscriber {
    async fn on_data(&mut self, msg: Advice) -> Result<(), SubscriberError> {
        tracing::debug!("Advice message received: {:?}", msg);
        let order_type = OrderType::try_from(msg.order_type);

        // Convert advice into an Order for the broker
        match order_type {
            Ok(ot) => match ot {
                OrderType::MarketOrder => {
                    let order = Order {
                        timestamp_ns: msg.timestamp_ns,
                        timeperiod_s: 0,
                        symbol: msg.symbol.clone(),
                        strategy_id: self.strategy_id.clone(),
                        order_type: msg.order_type,
                        price: msg.price,
                        size: msg.size,
                        order_id: self.order_id,
                        order_status: OrderStatus::Open.into(),
                        filled_price: -1.0,
                        account_ids: self.account_ids.clone(),
                        batch_id: self.batch_id.clone(),
                    };

                    self.order_publisher.publish(order.clone()).await?;
                    self.order_id += 1;
                }
                _ => tracing::warn!("Only market orders currently supported"),
            },
            Err(_) => {
                tracing::warn!("Error decoding order type, ignoring algo advice");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AlgoComplete> for AdviceSubscriber {
    async fn on_data(&mut self, msg: AlgoComplete) -> Result<(), SubscriberError> {
        self.orderfeed_complete_publisher
            .publish(OrderfeedComplete {
                timestamp_ns: msg.timestamp_ns,
                batch_id: self.batch_id.clone(),
                strategy_id: self.strategy_id.clone(),
            })
            .await?;
        Ok(())
    }
}

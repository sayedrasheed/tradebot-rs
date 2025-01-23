use tradebot_protos::messages::{order::OrderStatus, ChartRequest, Order, OrderFilled, OrderList};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use std::collections::HashMap;

use async_trait::async_trait;

// Orderfeed converted to an OrderTable for viewing in the app
pub struct OrderTable {
    pub strategy_id: String,
    pub publish: bool,
    pub order_publisher: Option<Publisher<Order>>,
    pub order_filled_publisher: Option<Publisher<OrderFilled>>,
    pub order_list_publisher: Option<Publisher<OrderList>>,
    pub order_list: OrderList,
    pub order_lookup: HashMap<u32, usize>,
}

impl OrderTable {
    pub fn new(
        strategy_id: &str,
        order_publisher: Option<Publisher<Order>>,
        order_list_publisher: Option<Publisher<OrderList>>,
        order_filled_publisher: Option<Publisher<OrderFilled>>,
    ) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            publish: false,
            order_list: OrderList::default(),
            order_lookup: HashMap::new(),
            order_publisher,
            order_list_publisher,
            order_filled_publisher,
        }
    }
}

#[async_trait]
impl Subscribe<ChartRequest> for OrderTable {
    async fn on_data(&mut self, msg: ChartRequest) -> Result<(), SubscriberError> {
        tracing::debug!("Chart request message received by Order table");
        tracing::debug!(
            "Order Table strategy id: {}, msg: {}",
            msg.strategy_id,
            self.strategy_id
        );

        // If request chart request is this strategy then need to publish current order list for the order table
        if msg.strategy_id == self.strategy_id {
            if let Some(order_list_publisher) = &self.order_list_publisher {
                tracing::debug!("Publish initial order list");
                order_list_publisher
                    .publish(self.order_list.clone())
                    .await?;
                self.publish = true;
            }
        } else {
            self.publish = false;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Order> for OrderTable {
    async fn on_data(&mut self, msg: Order) -> Result<(), SubscriberError> {
        tracing::debug!("Order message received");

        // Only publish the order to the app when the current request says to
        if msg.strategy_id == self.strategy_id {
            self.order_list.orders.push(msg.clone());

            self.order_lookup
                .insert(msg.order_id, self.order_list.orders.len() - 1);

            if self.publish {
                if let Some(order_publisher) = &self.order_publisher {
                    order_publisher.publish(msg).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderFilled> for OrderTable {
    async fn on_data(&mut self, msg: OrderFilled) -> Result<(), SubscriberError> {
        tracing::debug!("OrderFilled message received");

        // Only publish the order filled to the app when the current request says to
        if msg.strategy_id == self.strategy_id {
            if let Some(idx) = self.order_lookup.get(&msg.order_id) {
                self.order_list.orders[*idx].order_status = OrderStatus::Filled.into();
                self.order_list.orders[*idx].filled_price = msg.price;
            }

            if self.publish {
                if let Some(order_filled_publisher) = &self.order_filled_publisher {
                    order_filled_publisher.publish(msg).await?;
                }
            }
        }

        Ok(())
    }
}

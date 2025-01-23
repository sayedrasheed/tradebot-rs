use async_trait::async_trait;
use common::config::{TopicConfig, TopicMtype};
use std::fmt;
use std::{collections::HashMap, error::Error};
use tokio::task::JoinHandle;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortAlgoSubscriber, AlgoComplete, BrokerComplete, DatafeedComplete, Line, Order, OrderFilled,
    Point, Rectangle, Topic,
};
use zenoh_node::node::{
    Abort, Node, Publisher, Subscribe, Subscriber, SubscriberError, SubscriberImpl,
};

// Generic broker builder
pub struct BrokerBuilder {
    id: u32,
}

impl BrokerBuilder {
    pub fn new(id: u32) -> Self {
        Self { id }
    }

    pub async fn build<T>(
        &self,
        mut inner: T,
        node: &Node,
        topics: &TopicConfig,
    ) -> Result<Box<dyn Abort>, BrokerError>
    where
        T: BrokerSetup + 'static + Subscribe<Order>,
    {
        inner.set_id(self.id);
        inner.set_order_filled_publisher(node.new_publisher(&topics.get("order_filled")).await?);

        let mut subscriber = node.new_subscriber(inner).await?;
        node.subscribe::<Order>(&topics.get("order"), &mut subscriber)
            .await?;

        Ok(Box::new(subscriber))
    }
}

// Every broker needs to implement this trait. The BrokerService will supply the order fill publisher and broker complete publisher.
// And the implemented broker will use these publishers to publish order fills and complete message
#[async_trait]
pub trait BrokerSetup: Sized {
    fn set_id(&mut self, id: u32);
    fn set_order_filled_publisher(&mut self, publisher: Publisher<OrderFilled>);
    fn set_broker_complete_publisher(&mut self, publisher: Publisher<BrokerComplete>);
}

// Error stuff
pub struct BrokerError(pub Box<dyn Error + Send + Sync>);

impl snafu::AsErrorSource for BrokerError {
    fn as_error_source(&self) -> &(dyn Error + 'static) {
        &*self.0
    }
}

impl<E> From<E> for BrokerError
where
    E: Error + Send + Sync + 'static,
{
    fn from(inner: E) -> Self {
        Self(Box::new(inner))
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl From<BrokerError> for SubscriberError {
    fn from(error: BrokerError) -> SubscriberError {
        SubscriberError(error.0)
    }
}

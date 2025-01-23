use async_trait::async_trait;
use common::config::TopicMtype;
use std::error::Error;
use std::fmt;
use tokio::task::JoinHandle;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::AlgoComplete;
use zenoh_node::node::{Abort, Node, Publisher, SubscriberError};

pub type AlgoJoinHandle = JoinHandle<Result<(), AlgoError>>;

// Generic algo builder
pub struct AlgoBuilder {}

impl AlgoBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn build<T>(
        &self,
        mut inner: T,
        config_path: Option<String>,
        subscribe_node: &Node,
        subscribe_topics: &Vec<TopicMtype>,
        publish_node: &Node,
        publish_topics: &Vec<TopicMtype>,
    ) -> Result<Box<dyn Abort>, AlgoError>
    where
        T: AlgoSetup,
    {
        if let Some(config) = config_path {
            inner.set_config_path(&config);
        }

        inner.setup_publishers(publish_node, publish_topics).await?;

        for pt in publish_topics {
            match pt.mtype {
                MessageType::AlgoComplete => {
                    inner.set_algo_complete_publisher(publish_node.new_publisher(&pt.topic).await?);
                }
                _ => (),
            }
        }

        let subscriber = inner
            .setup_subscribers(subscribe_node, subscribe_topics)
            .await?;
        Ok(subscriber)
    }
}

// Every algo needs to implement this trait. The AlgoService will give the available publisher and subscriber topics/node
// and the actual algo will chose which ones it will use
#[async_trait]
pub trait AlgoSetup: Sized {
    fn set_config_path(&mut self, config_path: &str); // Path to a optional yaml config if the algo wants to have configurable params
    fn set_algo_complete_publisher(&mut self, publisher: Publisher<AlgoComplete>); // Separate out from other publishers to make it required

    async fn setup_publishers(
        &mut self,
        publish_node: &Node,
        publish_topics: &Vec<TopicMtype>,
    ) -> Result<(), AlgoError>;

    async fn setup_subscribers(
        self,
        subscribe_node: &Node,
        subscribe_topics: &Vec<TopicMtype>,
    ) -> Result<Box<dyn Abort>, AlgoError>;
}

// Error stuff
pub struct AlgoError(pub Box<dyn Error + Send + Sync>);

impl snafu::AsErrorSource for AlgoError {
    fn as_error_source(&self) -> &(dyn Error + 'static) {
        &*self.0
    }
}

impl<E> From<E> for AlgoError
where
    E: Error + Send + Sync + 'static,
{
    fn from(inner: E) -> Self {
        Self(Box::new(inner))
    }
}

impl fmt::Display for AlgoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for AlgoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl From<AlgoError> for SubscriberError {
    fn from(error: AlgoError) -> SubscriberError {
        SubscriberError(error.0)
    }
}

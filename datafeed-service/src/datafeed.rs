use async_trait::async_trait;
use common::config::TopicMtype;
use std::error::Error;
use std::fmt;
use tokio::task::JoinHandle;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Candle, DatafeedComplete, DatafeedStream, ExecutionMode, HistoricalData, Tick,
};
use zenoh_node::node::{Node, Publisher, SubscriberError};

pub type DatafeedJoinHandle = JoinHandle<Result<(), DatafeedError>>;

// Generic datafeed builder
pub struct DatafeedBuilder {
    pub config_path: String,
    pub id: u32,
    pub start_date: Option<String>,
    pub exe_mode: Option<ExecutionMode>,
}

impl DatafeedBuilder {
    pub fn new(id: u32) -> Self {
        Self {
            config_path: String::new(),
            id,
            start_date: None,
            exe_mode: None,
        }
    }

    pub fn build<T>(&self, mut inner: T) -> DatafeedNode<impl DatafeedSpawnSetup>
    where
        T: DatafeedSpawnSetup,
    {
        inner.set_id(self.id);
        inner.set_config_path(&self.config_path);

        if let Some(sd) = &self.start_date {
            inner.set_start_date(&sd);
        }

        if let Some(exe) = &self.exe_mode {
            inner.set_exe_mode(exe);
        }

        DatafeedNode::new(inner)
    }

    pub fn set_config_path(&mut self, config: &str) {
        self.config_path = config.to_string();
    }

    pub fn set_start_date(&mut self, start_date: &str) {
        self.start_date = Some(start_date.to_string());
    }

    pub fn set_exe_mode(&mut self, exe: &ExecutionMode) {
        self.exe_mode = Some(exe.clone());
    }
}

pub struct DatafeedNode<T>
where
    T: DatafeedSpawn + DatafeedSetup,
{
    inner: T,
}

impl<T> DatafeedNode<T>
where
    T: DatafeedSpawn + DatafeedSetup,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn setup_datafeed(&mut self, df: &DatafeedStream) {
        self.inner.set_datafeed_stream(df);
    }

    pub fn setup_datafeeds(&mut self, df: &Vec<DatafeedStream>) {
        self.inner.set_datafeed_streams(df);
    }

    pub async fn setup_publishers(
        &mut self,
        node: &Node,
        topics: &Vec<TopicMtype>,
    ) -> Result<(), DatafeedError> {
        for t in topics {
            match t.mtype {
                MessageType::Tick => self
                    .inner
                    .set_tick_publisher(node.new_publisher(&t.topic).await?),
                MessageType::Candle => self
                    .inner
                    .set_candle_publisher(node.new_publisher(&t.topic).await?),
                MessageType::HistoricalData => self
                    .inner
                    .set_historical_publisher(node.new_publisher(&t.topic).await?),
                MessageType::DatafeedComplete => self
                    .inner
                    .set_datafeed_complete_publisher(node.new_publisher(&t.topic).await?),
                _ => (),
            };
        }

        Ok(())
    }

    pub fn run(self) -> Result<DatafeedJoinHandle, DatafeedError> {
        self.inner.spawn()
    }
}

// Trait for spawning an async datafeed whether its from a socket or storage
#[async_trait]
pub trait DatafeedSpawn: Sized {
    fn spawn(self) -> Result<DatafeedJoinHandle, DatafeedError>;

    async fn datafeed(self) -> Result<(), DatafeedError>;
}

pub trait DatafeedSetup: Sized {
    fn set_id(&mut self, id: u32);
    fn set_config_path(&mut self, config_path: &str);
    fn set_exe_mode(&mut self, exe: &ExecutionMode);
    fn set_datafeed_stream(&mut self, df: &DatafeedStream);

    // use for datafeed that can stream multiple tickers, otherwise stub out
    fn set_datafeed_streams(&mut self, df: &Vec<DatafeedStream>);

    // all datafeeds can only publish these messages
    fn set_tick_publisher(&mut self, publisher: Publisher<Tick>);
    fn set_candle_publisher(&mut self, publisher: Publisher<Candle>);
    fn set_historical_publisher(&mut self, publisher: Publisher<HistoricalData>);
    fn set_datafeed_complete_publisher(&mut self, publisher: Publisher<DatafeedComplete>);

    // Use for datafeeds that will also run their own resim/backtest. This should be rare so
    // stubbing these out by default
    fn set_start_date(&mut self, _start_date: &str) {}
    fn set_end_date(&mut self, _end_date: &str) {}
}

pub trait DatafeedSpawnSetup: Sized + DatafeedSetup + DatafeedSpawn {}
impl<T: DatafeedSetup + DatafeedSpawn + Sized> DatafeedSpawnSetup for T {}

pub struct DatafeedError(pub Box<dyn Error + Send + Sync>);

impl snafu::AsErrorSource for DatafeedError {
    fn as_error_source(&self) -> &(dyn Error + 'static) {
        &*self.0
    }
}

impl<E> From<E> for DatafeedError
where
    E: Error + Send + Sync + 'static,
{
    fn from(inner: E) -> Self {
        Self(Box::new(inner))
    }
}

impl fmt::Display for DatafeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for DatafeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl From<DatafeedError> for SubscriberError {
    fn from(error: DatafeedError) -> SubscriberError {
        SubscriberError(error.0)
    }
}

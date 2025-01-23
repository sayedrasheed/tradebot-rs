use prost::Message;
use snafu::{ResultExt, Snafu};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    AbortOverallPnlSubscriber, OverallDayStats, OverallPnlComplete, Topic,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Snafu)]
pub enum OverallLoggerError {
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

    #[snafu(display("Error setting up logging subscriber"))]
    LoggingSubscriberError { source: NodeError },
}

pub struct OverallLogger {
    files: HashMap<String, File>,
    abort_publisher: Publisher<AbortOverallPnlSubscriber>,
}

impl OverallLogger {
    pub fn new(
        logging_path: &str,
        batch_id: &str,
        topics: &Vec<Topic>,
        abort_publisher: Publisher<AbortOverallPnlSubscriber>,
    ) -> Result<Self, SubscriberError> {
        let path = Path::new(logging_path);

        // Overall logger root dir will be named by the batch id
        let directory = path.join(batch_id.to_string());
        std::fs::create_dir_all(directory.clone())?;

        let mut files = HashMap::new();

        for topic in topics {
            let mtype = MessageType::try_from(topic.mtype)?;
            match mtype {
                MessageType::OverallDayStats => {
                    // create protobuf bin file that logs the overall stats by day since thats all we need to reconstruct the
                    // overall view for the app
                    let file = File::create(directory.join("overall_day_stats.bin"))?;
                    files.insert(String::from("overall_day_stats"), file);
                }
                _ => {}
            }
        }

        // create metadata json file for easier identification
        let mut file = File::create(directory.join("batch_metadata.json"))?;
        file.write(&serde_json::to_vec(&batch_id)?)?;

        Ok(Self {
            files,
            abort_publisher,
        })
    }
}

#[async_trait]
impl Subscribe<OverallDayStats> for OverallLogger {
    async fn on_data(&mut self, msg: OverallDayStats) -> Result<(), SubscriberError> {
        tracing::debug!("OverallDayStats messaged received");

        // Write out protobuf to file
        let mut buf = Vec::new();
        buf.reserve(msg.encoded_len() + prost::length_delimiter_len(msg.encoded_len()));
        let _res = msg.encode_length_delimited(&mut buf)?;

        if let Some(file) = self.files.get_mut("overall_day_stats") {
            file.write(&buf)?;
        } else {
            tracing::warn!("Can't find overall stats file pointer");
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<OverallPnlComplete> for OverallLogger {
    async fn on_data(&mut self, msg: OverallPnlComplete) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Overall Pnl complete message received, id: {}",
            msg.batch_id
        );

        // When pnlfeed is complete then drain the file pointers so they close automatically
        self.files.drain();

        tracing::debug!(
            "Publish abort logger subscriber message, id: {}",
            msg.batch_id
        );
        let abort_logger = AbortOverallPnlSubscriber {
            timestamp_ns: msg.timestamp_ns,
            batch_id: msg.batch_id,
        };

        self.abort_publisher.publish(abort_logger).await?;
        Ok(())
    }
}

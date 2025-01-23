use clap::Parser;
use common::config::RunConfig;
use common::config::{ServiceConfig, TopicConfig};
use common::error::ExitError;
use std::string::ToString;
use tradebot_protos::messages::RunYaml;
use zenoh_node::builder::NodeBuilder;

#[derive(Parser)]
#[command(name = "Trading Bot")]
#[command(author = "Sayed A <sayeedamir72@gmail.com>")]
#[command(version = "1.0")]
struct Cli {
    #[arg(short, long)]
    config: String,
}

// Main is primarily used as a quick way to run a yaml file to launch a strategy/strategies. is provided
// Yaml file is provided via command line with the -c argument
#[tokio::main]
async fn main() -> Result<(), ExitError> {
    let cli = Cli::parse();
    let path = cli.config.as_str();

    let f = std::fs::read_to_string(path)?;
    let config: RunConfig = serde_yaml::from_str(&f)?;

    let f = std::fs::read_to_string(&config.service_config_path)?;
    let service_config: ServiceConfig = serde_yaml::from_str(&f)?;

    // Create service node so we can communicate with the services
    let mut builder = NodeBuilder::new();
    if let Some(config) = &config.zenoh_config_path {
        builder.set_config_path(config);
    }

    let ip = service_config.ip;
    let port = service_config.port;

    builder.set_network((ip.clone(), port));

    let node: zenoh_node::node::Node = builder.build().await?;

    let topics = if let Some(topics) = &service_config.topics {
        TopicConfig::from(topics)
    } else {
        TopicConfig::default()
    };
    let run_strategy_publisher = node
        .new_publisher::<RunYaml>(&topics.get("run_yaml"))
        .await?;

    // Publish RunYaml message to app service and it will run the file
    run_strategy_publisher
        .publish(RunYaml {
            timestamp_ns: 0,
            yaml_path: path.to_string(),
        })
        .await?;

    Ok(())
}

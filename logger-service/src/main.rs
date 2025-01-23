mod config;
mod overall_logger;
mod overall_reader;
mod service;
mod strategy_logger;
mod strategy_reader;

use crate::service::LoggingService;
use clap::Parser;
use common::error::ExitError;
use config::Config;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[derive(Parser)]
#[command(name = "TradeBot")]
#[command(author = "Sayed A <sayeedamir72@gmail.com>")]
#[command(version = "1.0")]
struct Cli {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), ExitError> {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();

    // Set up tracing to use environment variable RUST_LOG
    tracing_subscriber::registry()
        .with(
            stdout_log
                .with_filter(EnvFilter::from_default_env())
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target().starts_with("logger") // filter for only logger service
                })),
        )
        .init();

    // get config path
    let cli = Cli::parse();
    let path = cli.config.as_str();

    // parse yaml config
    let f = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&f)?;

    tracing::info!("Starting Logger Service");
    let logging_service = LoggingService::new(&config).await?;

    logging_service.join().await?;

    Ok(())
}

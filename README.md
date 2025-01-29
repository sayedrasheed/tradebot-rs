# TradeBot
TradeBot is a distributed, service-oriented automated day-trading application built in Rust. It enables the automation of day-trading strategies using both real-time and historical market data. For real-time data, TradeBot currently integrates with NinjaTrader to receive market updates and execute orders, with integration for Interactive Brokers currently in progress. For backtesting, historical data can be sourced from local files or AWS S3, stored in Parquet format.

The platform supports backtesting strategies with historical data and seamlessly integrates Rust, C++ and Python trading strategies through a predefined interface that all algorithms follow. Additionally, TradeBot generates detailed profit and loss reports to assess strategy performance and logs all active strategies in binary format for future analysis.

Communication between services is handled via the Zenoh protocol using the Zenoh Rust API. All messages are serialized and deserialized using Protobuf, leveraging the Prost library.

All running strategies execute asynchronously using the Tokio Runtime. During backtests, each day is processed in parallel, ensuring fast results for longer backtests. In live trading, each trading strategy runs in parallel, enabling for fast trade execution.

Simple examples of Rust, C++, and Python trading strategies integrated into TradeBot can be found here:
1. [Rust](https://github.com/sayedrasheed/tradebot-rs/tree/master/algo-service/src/rust_algo)
2. [C++](https://github.com/sayedrasheed/cpp-algo-example)
3. [Python](https://github.com/sayedrasheed/py-algo-example)

TradeBot is controlled by a React-based Tauri application that can be found [here](https://github.com/sayedrasheed/tradebot-app):

# Demo
## Run resimulation using historical data
[YouTube](https://www.youtube.com/shorts/CoOf7fnGejE)

<img src='./videos/resim_demo.gif'>

## Read results from logged data and view Profit & Loss report
[YouTube](https://www.youtube.com/shorts/FK9qkTfclcQ)
<img src='./videos/read_from_log_demo.gif'>

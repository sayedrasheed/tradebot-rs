# TradeBot
TradeBot is a distributed, service-oriented automated trading application built in Rust. It enables the automation of trading strategies using both real-time and historical market data. For real-time data, TradeBot integrates with NinjaTrader to receive market updates and execute orders. Historical data can be sourced from local files or AWS S3, stored in Parquet format.

The platform supports backtesting strategies with historical data and seamlessly integrates Rust, C++ and Python trading strategies through a predefined interface that all algorithms follow. Additionally, TradeBot generates detailed profit and loss reports to assess strategy performance and logs all active strategies in binary format for future analysis.

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

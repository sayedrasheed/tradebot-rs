use tradebot_protos::messages::{Candle, Chart, ChartRequest, HistoricalData, Ohlcv, Tick};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;
use std::collections::HashMap;

// Datafeed converted to an Chart for viewing in the app
pub struct ChartDatafeed {
    pub strategy_id: String,
    pub active_symbol: String,
    pub active_period_s: u32,
    pub publish: bool,
    pub candle_update_publisher: Option<Publisher<Candle>>,
    pub chart_publisher: Option<Publisher<Chart>>,
    pub charts: HashMap<String, HashMap<u32, Chart>>,
}

impl ChartDatafeed {
    pub fn new(
        strategy_id: &str,
        candle_update_publisher: Option<Publisher<Candle>>,
        chart_publisher: Option<Publisher<Chart>>,
    ) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            candle_update_publisher,
            chart_publisher,
            publish: false,
            charts: HashMap::new(),
            active_symbol: String::new(),
            active_period_s: 0,
        }
    }
}

#[async_trait]
impl Subscribe<Tick> for ChartDatafeed {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        if let Some(symbol_map) = self.charts.get_mut(&msg.symbol) {
            // Use the Tick to update the current candle for all the charts
            for (period_s, chart) in &mut *symbol_map {
                // if current ohlcv is set then just update
                if let Some(current_ohclv) = &mut chart.current_ohlcv {
                    current_ohclv.high = f32::max(current_ohclv.high, msg.price);
                    current_ohclv.low = f32::min(current_ohclv.low, msg.price);
                    current_ohclv.close = msg.price;
                    current_ohclv.volume += msg.size as u64;
                } else {
                    // new candle opening
                    // Round down to nearest period using the tick timestamp
                    let mut timestamp_ns = msg.timestamp_ns;
                    let period_ns = *period_s as u64 * 1000000000;
                    let rem = msg.timestamp_ns % period_ns;

                    if rem > 0 {
                        timestamp_ns -= rem;
                    }

                    chart.current_ohlcv = Some(Ohlcv {
                        timestamp_ns: chart.ohlcv.last().unwrap().timestamp_ns
                            + (*period_s as u64 * 1000000000),
                        open: msg.price,
                        high: msg.price,
                        low: msg.price,
                        close: msg.price,
                        volume: msg.size as u64,
                    })
                }
            }

            // Only publish when chart request says to and symbol and period match
            if self.publish && self.active_symbol == msg.symbol {
                if let Some(chart) = symbol_map.get(&self.active_period_s) {
                    if let Some(ohclv) = &chart.current_ohlcv {
                        if let Some(candle_update_publisher) = &self.candle_update_publisher {
                            candle_update_publisher
                                .publish(Candle {
                                    symbol: msg.symbol.clone(),
                                    period_s: self.active_period_s,
                                    ohlcv: Some(Ohlcv {
                                        timestamp_ns: ohclv.timestamp_ns,
                                        open: ohclv.open,
                                        high: ohclv.high,
                                        low: ohclv.low,
                                        close: ohclv.close,
                                        volume: ohclv.volume,
                                    }),
                                })
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Candle> for ChartDatafeed {
    async fn on_data(&mut self, msg: Candle) -> Result<(), SubscriberError> {
        let period_s = msg.period_s;

        // Only publish the candle to the app when chart request says to and symbol and period match
        if let Some(symbol_map) = self.charts.get_mut(&msg.symbol) {
            if let Some(chart) = symbol_map.get_mut(&period_s) {
                if self.publish
                    && self.active_period_s == msg.period_s
                    && self.active_symbol == msg.symbol
                {
                    if let Some(candle_update_publisher) = &self.candle_update_publisher {
                        candle_update_publisher
                            .publish(Candle {
                                symbol: msg.symbol.clone(),
                                period_s: self.active_period_s,
                                ohlcv: Some(Ohlcv {
                                    timestamp_ns: msg.ohlcv.as_ref().unwrap().timestamp_ns,
                                    open: msg.ohlcv.as_ref().unwrap().open,
                                    high: msg.ohlcv.as_ref().unwrap().high,
                                    low: msg.ohlcv.as_ref().unwrap().low,
                                    close: msg.ohlcv.as_ref().unwrap().close,
                                    volume: msg.ohlcv.as_ref().unwrap().volume,
                                }),
                            })
                            .await?;
                    }
                }

                chart.ohlcv.push(msg.ohlcv.unwrap());
                chart.current_ohlcv.take();
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<HistoricalData> for ChartDatafeed {
    async fn on_data(&mut self, msg: HistoricalData) -> Result<(), SubscriberError> {
        tracing::debug!("Historical data message received, initialize chart object");
        let symbol_map = self.charts.entry(msg.symbol.clone()).or_default();
        let chart = symbol_map.entry(msg.period_s).or_insert(Chart {
            period_s: msg.period_s,
            symbol: msg.symbol.clone(),
            current_ohlcv: None,
            ohlcv: Vec::new(),
        });

        chart.ohlcv = msg.ohlcv;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<ChartRequest> for ChartDatafeed {
    async fn on_data(&mut self, msg: ChartRequest) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Chart request message received by chart datafeed, {}",
            msg.strategy_id
        );

        // If request chart request is this strategy then need to publish current chart
        if msg.strategy_id == self.strategy_id {
            tracing::debug!("Set publish to true");
            self.publish = true;

            if let Some(symbol_map) = self.charts.get(&msg.symbol) {
                if let Some(chart) = symbol_map.get(&msg.period_s) {
                    self.active_period_s = msg.period_s;
                    self.active_symbol = msg.symbol;

                    tracing::debug!("Publish initial chart");
                    if let Some(chart_publisher) = &self.chart_publisher {
                        chart_publisher.publish(chart.clone()).await?;
                    }
                }
            }
        } else {
            tracing::debug!("Set publish to false");
            self.publish = false;
        }

        Ok(())
    }
}

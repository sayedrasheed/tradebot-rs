use tradebot_protos::messages::{Advice, AlgoChart, ChartRequest, Line, Point, Rectangle};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use std::collections::HashMap;

use async_trait::async_trait;

// Algofeed converted to an AlgoChart for viewing in the app
pub struct ChartAlgofeed {
    pub strategy_id: String,
    pub active_symbol: String,
    pub active_period_s: u32,
    pub publish: bool,
    pub last_publish_timestamp_ns: u64,
    pub point_publisher: Option<Publisher<Point>>,
    pub rectangle_publisher: Option<Publisher<Rectangle>>,
    pub advice_publisher: Option<Publisher<Advice>>,
    pub algo_chart_publisher: Option<Publisher<AlgoChart>>,
    pub algo_charts: HashMap<String, HashMap<u32, AlgoChart>>,
}

impl ChartAlgofeed {
    pub fn new(
        strategy_id: &str,
        point_publisher: Option<Publisher<Point>>,
        rectangle_publisher: Option<Publisher<Rectangle>>,
        advice_publisher: Option<Publisher<Advice>>,
        algo_chart_publisher: Option<Publisher<AlgoChart>>,
    ) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            active_symbol: String::new(),
            active_period_s: 0,
            publish: false,
            last_publish_timestamp_ns: 0,
            algo_charts: HashMap::new(),
            point_publisher,
            rectangle_publisher,
            advice_publisher,
            algo_chart_publisher,
        }
    }
}

#[async_trait]
impl Subscribe<ChartRequest> for ChartAlgofeed {
    async fn on_data(&mut self, msg: ChartRequest) -> Result<(), SubscriberError> {
        tracing::debug!("Algo chart request message received by chart algofeed");

        // If request chart request is this strategy then need to publish current chart
        if msg.strategy_id == self.strategy_id {
            tracing::debug!(
                "Algo chart rstrategy id: {}, msg: {}",
                msg.strategy_id,
                self.strategy_id
            );
            if let Some(symbol_map) = self.algo_charts.get(&msg.symbol) {
                tracing::debug!("Algo chart symbol: {}", msg.symbol);
                if let Some(chart) = symbol_map.get(&msg.period_s) {
                    tracing::debug!(
                        "Set active period: {} and active symbol: {}",
                        msg.period_s,
                        msg.symbol
                    );
                    self.publish = true;
                    self.active_period_s = msg.period_s;
                    self.active_symbol = msg.symbol;

                    if let Some(algo_chart_publisher) = &self.algo_chart_publisher {
                        tracing::debug!("Publish initial algo chart");
                        algo_chart_publisher.publish(chart.clone()).await?;
                    }
                }
            }
        } else {
            self.publish = false;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Line> for ChartAlgofeed {
    async fn on_data(&mut self, msg: Line) -> Result<(), SubscriberError> {
        tracing::debug!(
            "Line message received, initialize line chart object, id: {}",
            self.strategy_id
        );
        let symbol_map = self.algo_charts.entry(msg.symbol.clone()).or_default();
        let algo_chart = symbol_map.entry(msg.period_s).or_insert(AlgoChart {
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            lines: HashMap::new(),
            rectangles: Vec::new(),
            advices: Vec::new(),
        });

        algo_chart.lines.insert(msg.description.clone(), msg);

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Point> for ChartAlgofeed {
    async fn on_data(&mut self, msg: Point) -> Result<(), SubscriberError> {
        //tracing::debug!("Point message received");
        let symbol_map = self.algo_charts.entry(msg.symbol.clone()).or_default();
        let algo_chart = symbol_map.entry(msg.period_s).or_insert(AlgoChart {
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            lines: HashMap::new(),
            rectangles: Vec::new(),
            advices: Vec::new(),
        });

        if let Some(line) = algo_chart.lines.get_mut(&msg.description) {
            line.points.push(msg.value.clone().unwrap());
        } else {
            algo_chart.lines.insert(
                msg.description.clone(),
                Line {
                    description: msg.description.clone(),
                    symbol: msg.symbol.clone(),
                    period_s: msg.period_s,
                    pane_idx: msg.pane_idx,
                    points: vec![msg.value.clone().unwrap()],
                    color: None,
                },
            );
        }

        // Only publish the point to the app when the current request says to and the active symbol and period match this point
        if self.publish && self.active_symbol == msg.symbol && self.active_period_s == msg.period_s
        {
            if let Some(point_publisher) = &self.point_publisher {
                point_publisher.publish(msg).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Advice> for ChartAlgofeed {
    async fn on_data(&mut self, msg: Advice) -> Result<(), SubscriberError> {
        //tracing::debug!("Algo order message received");
        let symbol_map = self.algo_charts.entry(msg.symbol.clone()).or_default();

        for (_, chart) in symbol_map {
            chart.advices.push(msg.clone());
        }

        // Only publish the advice to the app when the current request says to and the active symbol matches
        if self.publish && self.active_symbol == msg.symbol {
            if let Some(advice_publisher) = &self.advice_publisher {
                tracing::debug!("Publish advice");
                advice_publisher.publish(msg).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Rectangle> for ChartAlgofeed {
    async fn on_data(&mut self, msg: Rectangle) -> Result<(), SubscriberError> {
        //tracing::debug!("Rectangle message received, initialize chart object");
        let symbol_map = self.algo_charts.entry(msg.symbol.clone()).or_default();

        let algo_chart = symbol_map.entry(msg.period_s).or_insert(AlgoChart {
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            lines: HashMap::new(),
            rectangles: Vec::new(),
            advices: Vec::new(),
        });

        algo_chart.rectangles.push(msg.clone());

        // Only publish the rectangle to the app when the current request says to and the active symbol and period match this rect
        if self.publish && self.active_symbol == msg.symbol && self.active_period_s == msg.period_s
        {
            if let Some(rectangle_publisher) = &self.rectangle_publisher {
                tracing::debug!("Publish rectangle");
                rectangle_publisher.publish(msg).await?;
            }
        }

        Ok(())
    }
}

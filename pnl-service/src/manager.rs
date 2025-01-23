use crate::config::FuturesTickConfig;
use chrono::{DateTime, Timelike};
use chrono_tz::America;
use std::collections::HashMap;
use tradebot_protos::messages::{Advice, OrderFilled, PositionStats};

// Manager to track pnl
pub struct PnlManager {
    pub batch_id: String,
    pub strategy_id: String,
    pub futures_config: Option<HashMap<String, FuturesTickConfig>>,
    pub positions: HashMap<String, Vec<Position>>,
    pub num_closed_positions: u32,
    pub num_wins: u32,
    pub num_losses: u32,
    pub avg_win: f32,
    pub avg_loss: f32,
    pub max_drawdown: f32,
    pub max_drawup: f32,
    pub curr_total: f32,
    pub curr_position_id: u32,
    pub timestamp_ns: u64,
}

#[derive(Debug, Default)]
pub struct Position {
    pub timestamp_ns: u64,
    pub position_id: u32,
    pub size: i32,
    pub avg_price: f32,
    pub realized: f32,
}

impl PnlManager {
    pub fn new(batch_id: &str, strategy_id: &str) -> Self {
        Self {
            batch_id: batch_id.to_string(),
            strategy_id: strategy_id.to_string(),
            positions: HashMap::new(),
            num_closed_positions: 0,
            num_wins: 0,
            num_losses: 0,
            avg_win: 0.0,
            avg_loss: 0.0,
            max_drawdown: 0.0,
            max_drawup: 0.0,
            curr_total: 0.0,
            futures_config: None,
            curr_position_id: 0,
            timestamp_ns: 0,
        }
    }

    // Pricing for futures is different than stocks or options so need this for pnl calculation
    pub fn set_futures_config(&mut self, futures_config: &HashMap<String, FuturesTickConfig>) {
        self.futures_config = Some(futures_config.clone());
    }

    // Handler order fills when in Resim or Live mode
    pub fn order_filled(&mut self, order: &OrderFilled) -> (bool, u32) {
        let mut is_realized = false;
        self.timestamp_ns = order.timestamp_ns;
        let positions = self.positions.entry(order.symbol.clone()).or_default();

        // if its the first position or last position in the list is closed
        if positions.is_empty() || positions.last().unwrap().size == 0 {
            tracing::debug!("New position, price: {}", order.price);
            positions.push(Position {
                timestamp_ns: order.timestamp_ns,
                size: order.size,
                avg_price: order.price,
                realized: 0.0,
                position_id: order.order_id,
            });

            self.curr_position_id += 1;
        } else {
            let position = positions.last_mut().unwrap(); // unwrap safe here, there should always be at least 1

            tracing::debug!("Current position size: {}", position.size);
            // if adding to current position
            if (position.size < 0 && order.size < 0) || (position.size > 0 && order.size > 0) {
                tracing::debug!("Adding to position");
                let mut money_spent = position.size as f32 * position.avg_price;
                money_spent += order.price * order.size as f32;

                position.size += order.size;
                position.avg_price = money_spent / position.size as f32;
            } else {
                // new position
                let mut diff: f32 = position.avg_price - order.price;

                if let Some(futures_config) = &self.futures_config {
                    if let Some(tick_config) = futures_config.get(&order.symbol) {
                        diff = (diff / tick_config.tick_size) * tick_config.tick_value;
                    }
                }

                let realized = diff * order.size as f32;

                position.realized += realized;
                position.size += order.size;

                tracing::debug!("Realized: {}", realized);

                if position.size == 0 {
                    tracing::debug!("Position closed, price: {}", order.price);
                    self.curr_total += position.realized;

                    if position.realized >= 0.0 {
                        // add win
                        self.avg_win = (self.avg_win * self.num_wins as f32 + position.realized)
                            / (self.num_wins + 1) as f32;
                        self.num_wins += 1;

                        self.max_drawup = f32::max(self.max_drawup, self.curr_total);
                    } else {
                        // add loss
                        self.avg_loss = (self.avg_loss * self.num_losses as f32
                            + position.realized)
                            / (self.num_losses + 1) as f32;
                        self.num_losses += 1;

                        self.max_drawdown = f32::min(self.max_drawdown, self.curr_total)
                    }

                    self.num_closed_positions += 1;
                }

                is_realized = true;
            }
        }

        (is_realized, positions.last().unwrap().position_id)
    }

    // Handler advices when in Backtest mode
    pub fn order(&mut self, order: &Advice) -> (bool, u32) {
        let mut is_realized = false;
        self.timestamp_ns = order.timestamp_ns;
        let positions = self.positions.entry(order.symbol.clone()).or_default();

        // if its the first position or last position in the list is closed
        if positions.is_empty() || positions.last().unwrap().size == 0 {
            tracing::debug!("New position, price: {}", order.price);
            positions.push(Position {
                timestamp_ns: order.timestamp_ns,
                size: order.size,
                avg_price: order.price,
                realized: 0.0,
                position_id: self.curr_position_id,
            });
        } else {
            let position = positions.last_mut().unwrap(); // unwrap safe here, there should always be at least 1

            tracing::debug!("Current position size: {}", position.size);
            // if adding to current position
            if (position.size < 0 && order.size < 0) || (position.size > 0 && order.size > 0) {
                tracing::debug!("Adding to position");
                let mut money_spent = position.size as f32 * position.avg_price;
                money_spent += order.price * order.size as f32;

                position.size += order.size;
                position.avg_price = money_spent / position.size as f32;
            } else {
                // new position
                let mut diff: f32 = position.avg_price - order.price;

                if let Some(futures_config) = &self.futures_config {
                    if let Some(tick_config) = futures_config.get(&order.symbol) {
                        diff = (diff / tick_config.tick_size) * tick_config.tick_value;
                    }
                }
                let realized = diff * order.size as f32;

                position.realized += realized;
                position.size += order.size;

                tracing::debug!("Realized: {}", realized);

                if position.size == 0 {
                    tracing::debug!("Position closed, price: {}", order.price);
                    self.curr_total += position.realized;

                    if position.realized >= 0.0 {
                        // add win
                        self.avg_win = (self.avg_win * self.num_wins as f32 + position.realized)
                            / (self.num_wins + 1) as f32;
                        self.num_wins += 1;

                        self.max_drawup = f32::max(self.max_drawup, self.curr_total);
                    } else {
                        // add loss
                        self.avg_loss = (self.avg_loss * self.num_losses as f32
                            + position.realized)
                            / (self.num_losses + 1) as f32;
                        self.num_losses += 1;

                        self.max_drawdown = f32::min(self.max_drawdown, self.curr_total)
                    }

                    self.num_closed_positions += 1;
                }

                is_realized = true;
            }
        }

        self.curr_position_id += 1;

        (is_realized, positions.last().unwrap().position_id)
    }

    pub fn is_active(&self, symbol: &str) -> bool {
        if let Some(position) = self.positions.get(symbol) {
            position.last().unwrap().size != 0
        } else {
            false
        }
    }

    pub fn get_avg_price(&self, symbol: &str) -> f32 {
        self.positions
            .get(symbol)
            .unwrap_or(&Vec::new())
            .last()
            .unwrap_or(&Position {
                timestamp_ns: 0,
                position_id: 0,
                size: 0,
                avg_price: 0.0,
                realized: 0.0,
            })
            .avg_price
    }

    pub fn get_size(&self, symbol: &str) -> i32 {
        self.positions
            .get(symbol)
            .unwrap_or(&Vec::new())
            .last()
            .unwrap_or(&Position {
                timestamp_ns: 0,
                size: 0,
                avg_price: 0.0,
                realized: 0.0,
                position_id: 0,
            })
            .size
    }

    pub fn get_total_realized(&self, symbol: &str) -> f32 {
        if let Some(positions) = self.positions.get(symbol) {
            positions
                .iter()
                .fold(0.0, |realized, pos| realized + pos.realized)
        } else {
            0.0
        }
    }

    pub fn get_last_realized(&self, symbol: &str) -> (u64, f32) {
        if let Some(positions) = self.positions.get(symbol) {
            if let Some(position) = positions.last() {
                (position.timestamp_ns, position.realized)
            } else {
                (0, 0.0)
            }
        } else {
            (0, 0.0)
        }
    }

    pub fn get_position_stats(
        &self,
        timestamp_ns: u64,
        last_realized_timestamp_ns: u64,
        last_realized_pnl: f32,
    ) -> PositionStats {
        let dt = DateTime::from_timestamp_nanos(last_realized_timestamp_ns as i64);
        let est_dt = dt.with_timezone(&America::New_York);

        PositionStats {
            batch_id: self.batch_id.clone(),
            strategy_id: self.strategy_id.clone(),
            timestamp_ns,
            num_closed_positions: self.num_closed_positions,
            total_realized_pnl: self.curr_total,
            win_rate: self.num_wins as f32 / self.num_closed_positions as f32,
            num_wins: self.num_wins,
            num_losses: self.num_losses,
            avg_win: self.avg_win,
            avg_loss: self.avg_loss,
            max_drawdown: self.max_drawdown,
            max_drawup: self.max_drawup,
            avg_pnl: 0.0,
            last_realized_pnl,
            hour: est_dt.hour(),
            minute: est_dt.minute(),
        }
    }
}

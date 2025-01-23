use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use chrono_tz::America;
use std::collections::{BTreeMap, HashMap, HashSet};
use tradebot_protos::messages::{
    AbortOverallPnlSubscriber, OverallDayStats, OverallPnlComplete, PnlComplete, PositionStats,
};

use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

pub struct OverallPnlSubscriber {
    overall_stats_publisher: Publisher<OverallDayStats>,
    overall_pnl_complete_publisher: Publisher<OverallPnlComplete>,
    position_stats_map: BTreeMap<String, HashMap<String, PositionStats>>,
    num_requested_strategies: usize,
    batch_id: String,
}

impl OverallPnlSubscriber {
    pub fn new(
        batch_id: &str,
        num_requested_strategies: usize,
        overall_stats_publisher: Publisher<OverallDayStats>,
        overall_pnl_complete_publisher: Publisher<OverallPnlComplete>,
    ) -> Self {
        Self {
            overall_stats_publisher,
            position_stats_map: BTreeMap::new(),
            num_requested_strategies,
            batch_id: batch_id.to_string(),
            overall_pnl_complete_publisher,
        }
    }
}

#[async_trait]
impl Subscribe<PositionStats> for OverallPnlSubscriber {
    async fn on_data(&mut self, msg: PositionStats) -> Result<(), SubscriberError> {
        tracing::debug!("Position Stats message received");
        tracing::debug!("timestamp: {}", msg.timestamp_ns);
        let dt = DateTime::from_timestamp_nanos(msg.timestamp_ns as i64);
        let est_dt = dt.with_timezone(&America::New_York);
        let date_string = est_dt.format("%Y%m%d").to_string();
        tracing::debug!("date_string: {}", date_string);
        let parsed_date = NaiveDate::parse_from_str(&date_string, "%Y%m%d")?;

        let positions_date_map = self
            .position_stats_map
            .entry(date_string.clone())
            .or_default();

        let mut overall_stats = OverallDayStats {
            timestamp_ns: 0,
            num_closed_positions: 0,
            total_realized_pnl: 0.0,
            max_drawdown: 0.0,
            max_drawup: 0.0,
            win_rate: 0.0,
            num_wins: 0,
            num_losses: 0,
            avg_pnl: 0.0,
            avg_win: 0.0,
            avg_loss: 0.0,
            strategy_ids: Vec::new(),
            date: date_string.clone(),
            day: est_dt.weekday().to_string(),
            day_timestamp_ns: NaiveDateTime::from(parsed_date)
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap() as u64,
            last_realized_pnl: msg.last_realized_pnl,
            position_hour: msg.hour,
            position_minute: msg.minute,
        };

        positions_date_map.insert(msg.strategy_id.clone(), msg);

        let mut total_weighted_loss = 0.0;
        let mut total_weighted_win = 0.0;
        let mut total_profit = 0.0;

        for (strategy_id, pos_stats) in positions_date_map {
            overall_stats.timestamp_ns = pos_stats.timestamp_ns;
            overall_stats.num_closed_positions += pos_stats.num_closed_positions;
            overall_stats.total_realized_pnl += pos_stats.total_realized_pnl;
            overall_stats.num_wins += pos_stats.num_wins;
            overall_stats.num_losses += pos_stats.num_losses;
            overall_stats.win_rate =
                overall_stats.num_wins as f32 / overall_stats.num_closed_positions as f32;
            overall_stats.strategy_ids.push(strategy_id.clone());

            total_weighted_loss += pos_stats.avg_loss * pos_stats.num_losses as f32;
            total_weighted_win += pos_stats.avg_win * pos_stats.num_wins as f32;

            if overall_stats.num_losses > 0 {
                overall_stats.avg_loss = total_weighted_loss / overall_stats.num_losses as f32;
            }

            if overall_stats.num_wins > 0 {
                overall_stats.avg_win = total_weighted_win / overall_stats.num_wins as f32;
            }
            total_profit += pos_stats.total_realized_pnl;

            overall_stats.max_drawdown = f32::min(overall_stats.max_drawdown, total_profit);
            overall_stats.max_drawup = f32::max(overall_stats.max_drawup, total_profit);
        }

        self.overall_stats_publisher.publish(overall_stats).await?;
        Ok(())
    }
}

#[async_trait]
impl Subscribe<PnlComplete> for OverallPnlSubscriber {
    async fn on_data(&mut self, msg: PnlComplete) -> Result<(), SubscriberError> {
        tracing::debug!("Pnl complete message received");

        self.num_requested_strategies -= 1;

        tracing::debug!(
            "self.num_requested_strategies: {}",
            self.num_requested_strategies
        );
        if self.num_requested_strategies == 0 {
            self.overall_pnl_complete_publisher
                .publish(OverallPnlComplete {
                    timestamp_ns: msg.timestamp_ns,
                    batch_id: self.batch_id.clone(),
                })
                .await?;
        }

        Ok(())
    }
}

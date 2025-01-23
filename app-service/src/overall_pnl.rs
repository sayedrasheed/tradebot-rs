use std::collections::{BTreeMap, HashMap};
use tradebot_protos::messages::{
    ChartRequest, OverallDayStats, OverallRequest, OverallStats, PnlCalendar, PnlHour,
};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;

// Overall pnlfeed converted to pnl by hour, pnl by day, and stats for viewing in the app
pub struct OverallPnlSubscriber {
    pub overall_stats_publisher: Option<Publisher<OverallStats>>,
    pub pnl_calendar_publisher: Option<Publisher<PnlCalendar>>,
    pub pnl_hour_publisher: Option<Publisher<PnlHour>>,
    pub overall_day_stats_publisher: Option<Publisher<OverallDayStats>>,
    pub publish: bool,
    pub batch_id: String,
    pub day_stats_manager: DayStatsManager,
}

impl OverallPnlSubscriber {
    pub fn new(
        batch_id: &str,
        overall_stats_publisher: Option<Publisher<OverallStats>>,
        pnl_calendar_publisher: Option<Publisher<PnlCalendar>>,
        overall_day_stats_publisher: Option<Publisher<OverallDayStats>>,
        pnl_hour_publisher: Option<Publisher<PnlHour>>,
    ) -> Self {
        let mut pnl_hour = HashMap::new();

        // 24 hours in a day
        for i in 0..24 as u32 {
            pnl_hour.insert(i, OverallDayStats::default());
        }
        Self {
            batch_id: batch_id.to_string(),
            overall_stats_publisher,
            pnl_calendar_publisher,
            pnl_hour_publisher,
            overall_day_stats_publisher,
            publish: false,
            day_stats_manager: DayStatsManager::new(),
        }
    }
}

#[async_trait]
impl Subscribe<OverallDayStats> for OverallPnlSubscriber {
    async fn on_data(&mut self, msg: OverallDayStats) -> Result<(), SubscriberError> {
        tracing::debug!("OverallDayStats message received");

        // Overall day stats handler, update overall stats with the new stats just received
        self.day_stats_manager.overall_stats(&msg);

        if self.publish {
            if let Some(overall_stats_publisher) = &self.overall_stats_publisher {
                overall_stats_publisher
                    .publish(self.day_stats_manager.get_overall_stats())
                    .await?;
            }

            if let Some(overall_day_stats_publisher) = &self.overall_day_stats_publisher {
                tracing::debug!("publish overall day stats");
                overall_day_stats_publisher.publish(msg).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<OverallRequest> for OverallPnlSubscriber {
    async fn on_data(&mut self, msg: OverallRequest) -> Result<(), SubscriberError> {
        // An overall request is a request to view overall stats by batch id
        tracing::debug!("OverallRequest request message received by overall pnl chart");
        tracing::debug!(
            "OverallPnlSubscriber batch id: {}, msg: {}",
            msg.batch_id,
            self.batch_id
        );

        // If batch id matches this one, then publish combined overall stats, pnl calendar, and pnl by hour
        if msg.batch_id == self.batch_id {
            self.publish = true;

            if let Some(overall_stats_publisher) = &self.overall_stats_publisher {
                overall_stats_publisher
                    .publish(self.day_stats_manager.get_overall_stats())
                    .await?;
            }

            if let Some(pnl_calendar_publisher) = &self.pnl_calendar_publisher {
                tracing::debug!("publish calendar");
                pnl_calendar_publisher
                    .publish(self.day_stats_manager.get_pnl_calendar())
                    .await?;
            }

            if let Some(pnl_hour_publisher) = &self.pnl_hour_publisher {
                tracing::debug!("publish hour");
                pnl_hour_publisher
                    .publish(self.day_stats_manager.get_pnl_hour())
                    .await?;
            }
        } else {
            tracing::debug!("Set publish to false");
            self.publish = false;
        }

        Ok(())
    }
}

// Stats by day manager
pub struct DayStatsManager {
    pnl_calendar: BTreeMap<String, OverallDayStats>, // Mapped by date string
    pnl_hour: HashMap<u32, OverallDayStats>,         // Mapped by hour of the day
    overall_stats: OverallStats,                     // Current overall stats
}

impl DayStatsManager {
    pub fn new() -> Self {
        Self {
            pnl_calendar: BTreeMap::new(),
            pnl_hour: HashMap::new(),
            overall_stats: OverallStats::default(),
        }
    }

    pub fn overall_stats(&mut self, msg: &OverallDayStats) {
        // Update PnlCalendar for this date. New OverallDayStats message means new realized profit for this date
        // so update stats accordingly
        if let Some(curr_position_stats) = self.pnl_calendar.get_mut(&msg.date) {
            self.overall_stats.timestamp_ns = msg.timestamp_ns;
            self.overall_stats.num_closed_positions +=
                msg.num_closed_positions - curr_position_stats.num_closed_positions;
            self.overall_stats.total_realized_pnl -= curr_position_stats.total_realized_pnl;
            self.overall_stats.total_realized_pnl += msg.total_realized_pnl;

            if curr_position_stats.num_wins != msg.num_wins {
                let realized = msg.total_realized_pnl - curr_position_stats.total_realized_pnl;
                self.overall_stats.avg_win =
                    (self.overall_stats.avg_win * self.overall_stats.num_wins as f32 + realized)
                        / (self.overall_stats.num_wins + 1) as f32;
                self.overall_stats.num_wins += msg.num_wins - curr_position_stats.num_wins
            }

            if curr_position_stats.num_losses != msg.num_losses {
                let realized = msg.total_realized_pnl - curr_position_stats.total_realized_pnl;
                self.overall_stats.avg_loss =
                    (self.overall_stats.avg_loss * self.overall_stats.num_losses as f32 + realized)
                        / (self.overall_stats.num_losses + 1) as f32;
                self.overall_stats.num_losses += msg.num_losses - curr_position_stats.num_losses
            }

            self.overall_stats.win_rate =
                self.overall_stats.num_wins as f32 / self.overall_stats.num_closed_positions as f32;

            curr_position_stats.avg_loss = msg.avg_loss;
            curr_position_stats.avg_pnl = msg.avg_pnl;
            curr_position_stats.avg_win = msg.avg_win;
            curr_position_stats.max_drawdown = msg.max_drawdown;
            curr_position_stats.max_drawup = msg.max_drawup;
            curr_position_stats.num_closed_positions = msg.num_closed_positions;
            curr_position_stats.num_losses = msg.num_losses;
            curr_position_stats.num_wins = msg.num_wins;
            curr_position_stats.total_realized_pnl = msg.total_realized_pnl;
            curr_position_stats.win_rate = msg.win_rate;
        } else {
            self.overall_stats.timestamp_ns = msg.timestamp_ns;
            self.overall_stats.num_closed_positions += msg.num_closed_positions;
            self.overall_stats.total_realized_pnl += msg.total_realized_pnl;
            self.overall_stats.num_losses += msg.num_losses;
            self.overall_stats.num_wins += msg.num_wins;
            self.overall_stats.win_rate =
                self.overall_stats.num_wins as f32 / self.overall_stats.num_closed_positions as f32;

            if self.overall_stats.num_wins > 0 {
                self.overall_stats.avg_win =
                    (self.overall_stats.avg_win * self.overall_stats.num_wins as f32 + msg.avg_win)
                        / (self.overall_stats.num_wins + msg.num_wins) as f32;
            } else {
                self.overall_stats.avg_win = msg.avg_win;
            }

            if self.overall_stats.num_losses > 0 {
                self.overall_stats.avg_loss = (self.overall_stats.avg_loss
                    * self.overall_stats.num_losses as f32
                    + msg.avg_loss)
                    / (self.overall_stats.num_losses + msg.num_losses) as f32;
            } else {
                self.overall_stats.avg_loss = msg.avg_loss;
            }

            self.pnl_calendar.insert(msg.date.clone(), msg.clone());
        }

        // To calculate max drawdown and draw up for this day, we need the day befores current realized profit
        let day_before = self.pnl_calendar.range(..msg.date.clone()).next_back();
        if let Some((_, stats_day_before)) = day_before {
            self.overall_stats.max_drawdown = f32::min(
                self.overall_stats.max_drawdown,
                stats_day_before.total_realized_pnl + msg.total_realized_pnl,
            );
            self.overall_stats.max_drawup = f32::max(
                self.overall_stats.max_drawup,
                stats_day_before.total_realized_pnl + msg.total_realized_pnl,
            );
        } else {
            self.overall_stats.max_drawdown =
                f32::min(self.overall_stats.max_drawdown, msg.total_realized_pnl);
            self.overall_stats.max_drawup =
                f32::max(self.overall_stats.max_drawup, msg.total_realized_pnl);
        }

        if let Some(pnl_hour) = self.pnl_hour.get_mut(&msg.position_hour) {
            pnl_hour.total_realized_pnl += msg.last_realized_pnl;
        } else {
            self.pnl_hour.insert(msg.position_hour, msg.clone());
        }
    }

    pub fn get_overall_stats(&self) -> OverallStats {
        self.overall_stats.clone()
    }

    pub fn get_pnl_calendar(&self) -> PnlCalendar {
        PnlCalendar {
            timestamp_ns: 0,
            stats: self
                .pnl_calendar
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    pub fn get_pnl_hour(&self) -> PnlHour {
        PnlHour {
            timestamp_ns: 0,
            stats: self.pnl_hour.clone(),
        }
    }
}

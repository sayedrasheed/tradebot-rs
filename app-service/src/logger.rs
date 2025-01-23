use tradebot_protos::messages::{
    AlgoChart, Chart, OrderList, OverallDayStatsList, OverallStats, PnlCalendar, PnlHour,
    PositionStats, TotalPnl, TotalPnlRealized,
};
use zenoh_node::node::{Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;

use crate::overall_pnl::DayStatsManager;

pub struct Logger {
    pub chart_publisher: Option<Publisher<Chart>>,
    pub algo_chart_publisher: Option<Publisher<AlgoChart>>,
    pub order_list_publisher: Option<Publisher<OrderList>>,
    pub total_pnl_publisher: Option<Publisher<TotalPnl>>,
    pub total_pnl_realized_publisher: Option<Publisher<TotalPnlRealized>>,
    pub position_stats_publisher: Option<Publisher<PositionStats>>,
    pub overall_pnl_publisher: Option<Publisher<OverallStats>>,
    pub pnl_calendar_publisher: Option<Publisher<PnlCalendar>>,
    pub pnl_by_hour_publisher: Option<Publisher<PnlHour>>,
    pub day_stats_manager: DayStatsManager,
}

impl Logger {
    pub fn new(
        chart_publisher: Option<Publisher<Chart>>,
        algo_chart_publisher: Option<Publisher<AlgoChart>>,
        order_list_publisher: Option<Publisher<OrderList>>,
        total_pnl_publisher: Option<Publisher<TotalPnl>>,
        total_pnl_realized_publisher: Option<Publisher<TotalPnlRealized>>,
        position_stats_publisher: Option<Publisher<PositionStats>>,
        overall_pnl_publisher: Option<Publisher<OverallStats>>,
        pnl_calendar_publisher: Option<Publisher<PnlCalendar>>,
        pnl_by_hour_publisher: Option<Publisher<PnlHour>>,
    ) -> Self {
        Self {
            chart_publisher,
            algo_chart_publisher,
            order_list_publisher,
            total_pnl_publisher,
            total_pnl_realized_publisher,
            position_stats_publisher,
            overall_pnl_publisher,
            pnl_calendar_publisher,
            pnl_by_hour_publisher,
            day_stats_manager: DayStatsManager::new(),
        }
    }
}

#[async_trait]
impl Subscribe<OverallDayStatsList> for Logger {
    async fn on_data(&mut self, msg: OverallDayStatsList) -> Result<(), SubscriberError> {
        // Logger reads all OverallStats by day and publishes them in a list. So need to accumlate
        // all these using the day stats manager so we can get the combined overall stats for viewing
        tracing::debug!("OverallDayStatsList message received");
        for overall_day_stats in &msg.overall_day_stats_list {
            self.day_stats_manager.overall_stats(overall_day_stats);
        }

        if let Some(publisher) = &self.overall_pnl_publisher {
            publisher
                .publish(self.day_stats_manager.get_overall_stats())
                .await?;
        }

        if let Some(publisher) = &self.pnl_calendar_publisher {
            publisher
                .publish(self.day_stats_manager.get_pnl_calendar())
                .await?;
        }

        if let Some(publisher) = &self.pnl_by_hour_publisher {
            publisher
                .publish(self.day_stats_manager.get_pnl_hour())
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<Chart> for Logger {
    async fn on_data(&mut self, msg: Chart) -> Result<(), SubscriberError> {
        tracing::debug!("Chart message received");
        if let Some(publisher) = &self.chart_publisher {
            tracing::debug!("Publish chart");
            publisher.publish(msg).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<AlgoChart> for Logger {
    async fn on_data(&mut self, msg: AlgoChart) -> Result<(), SubscriberError> {
        tracing::debug!("AlgoChart received");
        if let Some(publisher) = &self.algo_chart_publisher {
            publisher.publish(msg).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Subscribe<OrderList> for Logger {
    async fn on_data(&mut self, msg: OrderList) -> Result<(), SubscriberError> {
        tracing::debug!("Order List received");
        if let Some(publisher) = &self.order_list_publisher {
            publisher.publish(msg).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<TotalPnl> for Logger {
    async fn on_data(&mut self, msg: TotalPnl) -> Result<(), SubscriberError> {
        tracing::debug!("TotalPnl message received");
        if let Some(publisher) = &self.total_pnl_publisher {
            publisher.publish(msg).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<TotalPnlRealized> for Logger {
    async fn on_data(&mut self, msg: TotalPnlRealized) -> Result<(), SubscriberError> {
        tracing::debug!("TotalPnlRealized message received");
        if let Some(publisher) = &self.total_pnl_realized_publisher {
            publisher.publish(msg).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<PositionStats> for Logger {
    async fn on_data(&mut self, msg: PositionStats) -> Result<(), SubscriberError> {
        tracing::debug!("PositionStats message received");
        if let Some(publisher) = &self.position_stats_publisher {
            publisher.publish(msg).await?;
        }

        Ok(())
    }
}

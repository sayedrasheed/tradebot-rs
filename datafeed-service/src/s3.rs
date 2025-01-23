use crate::datafeed::{DatafeedError, DatafeedJoinHandle, DatafeedSetup, DatafeedSpawn};
use arrow_array::types::UInt32Type;
use arrow_array::{Float32Array, Int8Array, UInt32Array, UInt64Array};
use async_trait::async_trait;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStreamError;
use chrono::{Datelike, Days, Month, NaiveDate};
use futures::stream;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;
use streaming_iterator::StreamingIterator;
use tokio::runtime;
use tokio_util::time::DelayQueue;
use tradebot_protos::messages::{
    Candle, DatafeedComplete, DatafeedStream, ExecutionMode, HistoricalData, Ohlcv, Tick,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Node, Publisher};

#[derive(Debug, Snafu)]
pub enum S3DatafeedError {
    #[snafu(display("Unable to open {filename}"))]
    ReadFileToString {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Unable to open {filename}"))]
    SerdeYamlError {
        source: serde_yaml::Error,
        filename: String,
    },

    #[snafu(display("Unable to convert date string"))]
    ConvertDateStrError { source: chrono::format::ParseError },

    #[snafu(display("Unable to get contract date"))]
    GetContractError,

    #[snafu(display("Failed to publish data"))]
    PublishError { source: NodeError },

    #[snafu(display("Error converting date to chrono"))]
    ConvertDateToChrono { source: chrono::OutOfRange },

    #[snafu(display("Unable to open {filename}"))]
    FileNotFoundError {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Unable to collect body response"))]
    S3BodyResponseError { source: ByteStreamError },
}

struct CandleStreamConfig {
    pub period_s: u32,
    pub num_historical_days: u32,
}
struct DatafeedConfig {
    pub symbol: String,
    pub enable_ticks: bool,
    pub candle_config: Vec<CandleStreamConfig>,
}

enum DatafeedMessage {
    TickMessage(Tick),
    CandleMessage(Candle),
}

pub struct S3Datafeed {
    config_path: String,
    datafeed_id: u32,
    exe_mode: ExecutionMode,
    start_date: String,
    end_date: String,
    datafeeds: Vec<DatafeedConfig>,
    // Publishers
    tick_publisher: Option<Publisher<Tick>>,
    candle_publisher: Option<Publisher<Candle>>,
    historical_publisher: Option<Publisher<HistoricalData>>,
    complete_publisher: Option<Publisher<DatafeedComplete>>,
}

impl Default for S3Datafeed {
    fn default() -> Self {
        Self {
            config_path: String::new(),
            datafeed_id: 0,
            datafeeds: Vec::new(),
            start_date: String::new(),
            end_date: String::new(),
            tick_publisher: None,
            candle_publisher: None,
            historical_publisher: None,
            complete_publisher: None,
            exe_mode: ExecutionMode::Live,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct S3DatafeedConfig {
    bucket_name: String,
    region: String,
    credentials: Option<S3Credentials>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct S3Credentials {
    access_key_id: String,
    secret_access_key: String,
}

impl Default for S3DatafeedConfig {
    fn default() -> Self {
        Self {
            bucket_name: String::new(),
            region: String::new(),
            credentials: None,
        }
    }
}

#[async_trait]
impl DatafeedSpawn for S3Datafeed {
    async fn datafeed(self) -> Result<(), DatafeedError> {
        let s3_config = self.parse_config(&self.config_path)?;

        // Use S3 credentials from config if they are available, otherwise use from aws env
        let client = if let Some(config_credentials) = &s3_config.credentials {
            let credentials = aws_credential_types::Credentials::from_keys(
                &config_credentials.access_key_id,
                &config_credentials.access_key_id,
                None,
            );

            let config = aws_sdk_s3::config::Builder::new()
                .credentials_provider(credentials)
                .region(Region::new(s3_config.region))
                .build();
            aws_sdk_s3::Client::from_conf(config)
        } else {
            let shared_config = aws_config::from_env()
                .region(Region::new(s3_config.region))
                .load()
                .await;
            aws_sdk_s3::Client::new(&shared_config)
        };

        let _ = self
            .publish_historical_data(&client, &s3_config.bucket_name)
            .await;

        let dt =
            NaiveDate::parse_from_str(&self.start_date, "%Y%m%d").context(ConvertDateStrSnafu)?;

        let mut num_messages = 0;
        let mut resim_queue = DelayQueue::new();
        let mut backtest_queue = VecDeque::new();

        // Populate datafeed data for all the requested datafeeds. In resim/backtest mode, all requested
        // datafeeds will come from one simulated datafeed
        let mut df_data: Vec<(Option<VecDeque<Tick>>, Vec<VecDeque<Candle>>)> = Vec::new(); // buffer to store ticks and candle stream lists for a datafeed
        for (i, df) in self.datafeeds.iter().enumerate() {
            df_data.push((None, Vec::new()));
            let contract = Self::get_contract_symbol(&df.symbol, &dt)?;
            let mut data = (None, Vec::new());

            // If tickets are enabled then gather ticks from trades bucket in S3
            data.0 = if df.enable_ticks {
                let mut ticks = VecDeque::new();

                let key = format!(
                    "futures/{}/{}/{}/{}",
                    df.symbol, self.start_date, contract, "trades.parquet"
                );

                let res = client
                    .get_object()
                    .bucket(&s3_config.bucket_name)
                    .key(&key)
                    .send()
                    .await;

                match res {
                    Ok(obj) => {
                        let body: Vec<u8> = obj
                            .body
                            .collect()
                            .await
                            .context(S3BodyResponseSnafu)?
                            .to_vec();
                        let builder =
                            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(body))
                                .unwrap();
                        tracing::debug!("Converted arrow schema is: {}", builder.schema());

                        let mut reader = builder.build().unwrap();

                        // Iterate through parquet file to gather ticks
                        while let Some(Ok(record_batch)) = reader.next() {
                            let timestamp_ns = record_batch.column_by_name("timestamp_ns");
                            let price = record_batch.column_by_name("price");
                            let size = record_batch.column_by_name("size");
                            let side = record_batch.column_by_name("side");

                            if let (Some(ts), Some(p), Some(sz), Some(sd)) =
                                (timestamp_ns, price, size, side)
                            {
                                let ts_arr =
                                    ts.as_any().downcast_ref::<UInt64Array>().unwrap().values();
                                let p_arr =
                                    p.as_any().downcast_ref::<Float32Array>().unwrap().values();
                                let sz_arr =
                                    sz.as_any().downcast_ref::<UInt32Array>().unwrap().values();
                                let sd_arr =
                                    sd.as_any().downcast_ref::<Int8Array>().unwrap().values();

                                for j in 0..ts_arr.len() {
                                    ticks.push_back(Tick {
                                        timestamp_ns: ts_arr[j],
                                        price: p_arr[j],
                                        size: sz_arr[j],
                                        side: sd_arr[j] as i32,
                                        symbol: df.symbol.clone(),
                                    });

                                    num_messages += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading from S3 bucket, {:?}", e);
                        if let Some(publisher) = &self.complete_publisher {
                            tracing::debug!(
                                "Error getting data from S3 so publish datafeed complete message so downstream modules aren't waiting"
                            );
                            publisher
                                .publish(DatafeedComplete {
                                    timestamp_ns: 0,
                                    datafeed_id: self.datafeed_id,
                                })
                                .await?;
                        }
                    }
                }

                Some(ticks)
            } else {
                None
            };

            // For each request candle stream, find corresponding parquet in S3 for resim/backtest
            for (j, c) in df.candle_config.iter().enumerate() {
                data.1.push(VecDeque::new());

                let key = format!(
                    "futures/{}/{}/{}/{}",
                    df.symbol,
                    self.start_date,
                    contract,
                    format!("ohlcv-{}.parquet", c.period_s)
                );

                let res = client
                    .get_object()
                    .bucket(&s3_config.bucket_name)
                    .key(&key)
                    .send()
                    .await;

                match res {
                    Ok(obj) => {
                        let body: Vec<u8> = obj
                            .body
                            .collect()
                            .await
                            .context(S3BodyResponseSnafu)?
                            .to_vec();
                        let builder =
                            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(body))
                                .unwrap();
                        tracing::debug!("Converted arrow schema is: {}", builder.schema());

                        let mut reader = builder.build().unwrap();

                        // Iterate through parquet file to gather candles
                        while let Some(Ok(record_batch)) = reader.next() {
                            let timestamp_ns = record_batch.column_by_name("timestamp_ns");
                            let open = record_batch.column_by_name("open");
                            let high = record_batch.column_by_name("high");
                            let low = record_batch.column_by_name("low");
                            let close = record_batch.column_by_name("close");
                            let volume = record_batch.column_by_name("volume");

                            if let (Some(ts), Some(o), Some(h), Some(l), Some(cl), Some(v)) =
                                (timestamp_ns, open, high, low, close, volume)
                            {
                                let ts_arr =
                                    ts.as_any().downcast_ref::<UInt64Array>().unwrap().values();
                                let o_arr =
                                    o.as_any().downcast_ref::<Float32Array>().unwrap().values();
                                let h_arr =
                                    h.as_any().downcast_ref::<Float32Array>().unwrap().values();
                                let l_arr =
                                    l.as_any().downcast_ref::<Float32Array>().unwrap().values();
                                let c_arr =
                                    cl.as_any().downcast_ref::<Float32Array>().unwrap().values();
                                let v_arr =
                                    v.as_any().downcast_ref::<UInt64Array>().unwrap().values();

                                for k in 0..ts_arr.len() {
                                    data.1[j].push_back(Candle {
                                        symbol: df.symbol.clone(),
                                        period_s: c.period_s,
                                        ohlcv: Some(Ohlcv {
                                            timestamp_ns: ts_arr[k],
                                            open: o_arr[k],
                                            high: h_arr[k],
                                            low: l_arr[k],
                                            close: c_arr[k],
                                            volume: v_arr[k],
                                        }),
                                    });
                                    num_messages += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading from S3 bucket, {:?}", e);
                        if let Some(publisher) = &self.complete_publisher {
                            tracing::debug!(
                                    "Error getting data from S3 so publish datafeed complete message so downstream modules aren't waiting"
                                );
                            publisher
                                .publish(DatafeedComplete {
                                    timestamp_ns: 0,
                                    datafeed_id: self.datafeed_id,
                                })
                                .await?;
                        }
                    }
                }
            }

            df_data[i] = data;
        }

        let mut timer_count = 1; // hard code delay queue to be 1ms

        // Populate delay queue for a simulated datafeed. Loop through number of messages stored in datafeed buffer df_data.
        println!("num_messages: {}", num_messages);
        while num_messages > 0 {
            // Need to publish datafeed data ordered by timestamp. So need to find the minimum timestamp in the the buffers
            let min_timestamp = df_data.iter().fold(std::u64::MAX, |ts, data| {
                if let Some(ticks) = &data.0 {
                    ts.min(
                        ticks
                            .front()
                            .unwrap_or(&Tick {
                                timestamp_ns: std::u64::MAX,
                                ..Default::default()
                            })
                            .timestamp_ns,
                    )
                } else {
                    // if there is no tick data ie ticks arent enabled then use minimum candle timestamp
                    data.1.iter().fold(std::u64::MAX, |cts, cdata| {
                        cts.min(
                            cdata
                                .front()
                                .unwrap_or(&Candle {
                                    ohlcv: Some(Ohlcv {
                                        timestamp_ns: std::u64::MAX,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .ohlcv
                                .as_ref()
                                .unwrap()
                                .timestamp_ns,
                        )
                    })
                }
            });

            // Go through buffer and find the datafeed messages that should be sent based on the timestamp
            for tc in &mut df_data {
                if let Some(tdeq) = &mut tc.0 {
                    if let Some(tick) = tdeq.front() {
                        if tick.timestamp_ns <= min_timestamp {
                            let msg = DatafeedMessage::TickMessage(tick.clone());

                            match self.exe_mode {
                                ExecutionMode::Resim => {
                                    // Use 1ms delay between messages
                                    resim_queue.insert(msg, Duration::from_millis(timer_count));
                                    timer_count += 1;
                                }
                                ExecutionMode::Backtest => {
                                    backtest_queue.push_back(msg);
                                }
                                _ => (),
                            }

                            num_messages -= 1;
                            tdeq.pop_front();
                        }
                    }
                }

                for cdeq in &mut tc.1 {
                    if let Some(candle) = cdeq.front() {
                        // Ohclv timestamps are time the candle opened, so add period to get time candle will be closed since we only
                        // want to publish candles when they are closed
                        if (candle.ohlcv.as_ref().unwrap().timestamp_ns
                            + (candle.period_s as u64 * 1000000000)
                            <= min_timestamp)
                            || tc.0.is_none()
                        {
                            let msg = DatafeedMessage::CandleMessage(candle.clone());

                            match self.exe_mode {
                                ExecutionMode::Resim => {
                                    // Use 1ms delay between messages
                                    resim_queue.insert(msg, Duration::from_millis(timer_count));
                                    timer_count += 1;
                                }
                                ExecutionMode::Backtest => {
                                    backtest_queue.push_back(msg);
                                }
                                _ => (),
                            }
                            num_messages -= 1;
                            cdeq.pop_front();
                        }
                    }
                }
            }
        }

        match self.exe_mode {
            ExecutionMode::Resim => {
                // Create stream from tokio delay queue if in simulated resim mode. Simulated resim mode will just add some delay to make it more realistic
                let stream = stream::unfold(resim_queue, |mut resim_queue| async move {
                    let msg = resim_queue.next().await?.into_inner();
                    Some((msg, resim_queue))
                });

                futures::pin_mut!(stream);
                while let Some(msg) = stream.next().await {
                    match msg {
                        DatafeedMessage::TickMessage(tick) => {
                            if let Some(publisher) = &self.tick_publisher {
                                publisher.publish(tick).await?;
                            }
                        }
                        DatafeedMessage::CandleMessage(candle) => {
                            if let Some(publisher) = &self.candle_publisher {
                                //println!("{:?}", candle);
                                publisher.publish(candle).await?;
                            }
                        }
                    }
                }
            }
            ExecutionMode::Backtest => {
                // Backtest mode we will just iterate through the queue as fast as we can using a futures stream
                let stream = stream::iter(backtest_queue);

                futures::pin_mut!(stream);
                while let Some(msg) = stream.next().await {
                    match msg {
                        DatafeedMessage::TickMessage(tick) => {
                            if let Some(publisher) = &self.tick_publisher {
                                publisher.publish(tick).await?;
                            }
                        }
                        DatafeedMessage::CandleMessage(candle) => {
                            if let Some(publisher) = &self.candle_publisher {
                                //println!("{:?}", candle);
                                publisher.publish(candle).await?;
                            }
                        }
                    }
                }
            }
            _ => (),
        }

        sleep(Duration::from_millis(500));
        if let Some(publisher) = &self.complete_publisher {
            tracing::debug!(
                "Backtest complete, publish datafeed complete message, id: {}",
                self.datafeed_id
            );
            publisher
                .publish(DatafeedComplete {
                    timestamp_ns: 0,
                    datafeed_id: self.datafeed_id,
                })
                .await?;
        }
        Ok(())
    }

    fn spawn(self) -> Result<DatafeedJoinHandle, DatafeedError> {
        let _guard = runtime::Handle::current().enter();
        let handle = tokio::spawn(self.datafeed());
        Ok(handle)
    }
}

impl DatafeedSetup for S3Datafeed {
    fn set_id(&mut self, id: u32) {
        self.datafeed_id = id;
    }

    fn set_start_date(&mut self, start_date: &str) {
        self.start_date = start_date.to_string();
    }

    fn set_end_date(&mut self, end_date: &str) {
        self.end_date = end_date.to_string();
    }

    fn set_config_path(&mut self, config_path: &str) {
        self.config_path = config_path.to_string();
    }

    fn set_exe_mode(&mut self, exe_mode: &ExecutionMode) {
        self.exe_mode = exe_mode.clone();
    }

    fn set_datafeed_stream(&mut self, _df: &DatafeedStream) {}

    fn set_datafeed_streams(&mut self, df: &Vec<DatafeedStream>) {
        self.datafeeds = df
            .iter()
            .map(|d| DatafeedConfig {
                enable_ticks: d.enable_ticks,
                symbol: d.symbol.clone(),
                candle_config: d
                    .candle_streams
                    .iter()
                    .map(|c| CandleStreamConfig {
                        period_s: c.period_s,
                        num_historical_days: c.historical_data.as_ref().unwrap().num_days,
                    })
                    .collect(),
            })
            .collect();
    }
    fn set_tick_publisher(&mut self, publisher: Publisher<Tick>) {
        self.tick_publisher = Some(publisher);
    }

    fn set_candle_publisher(&mut self, publisher: Publisher<Candle>) {
        self.candle_publisher = Some(publisher);
    }

    fn set_historical_publisher(&mut self, publisher: Publisher<HistoricalData>) {
        self.historical_publisher = Some(publisher);
    }

    fn set_datafeed_complete_publisher(&mut self, publisher: Publisher<DatafeedComplete>) {
        self.complete_publisher = Some(publisher);
    }
}

impl S3Datafeed {
    async fn publish_historical_data(
        &self,
        client: &aws_sdk_s3::Client,
        bucket_name: &str,
    ) -> Result<(), S3DatafeedError> {
        for df in &self.datafeeds {
            for c in &df.candle_config {
                let mut historical_data = HistoricalData::default();
                let mut s3_data = VecDeque::new();
                let mut num_days = c.num_historical_days;
                let mut iter_count = 0;
                let mut curr_date = self.start_date.clone();

                historical_data.symbol = df.symbol.clone();
                historical_data.period_s = c.period_s as u32;

                // get historical data for each num_day requested, starting from start date
                // use max iteration count of 7 to look for historical data log
                while num_days > 0 && iter_count < 7 {
                    // assume start date is in YYYYMMDD format
                    let dt = NaiveDate::parse_from_str(&curr_date, "%Y%m%d")
                        .context(ConvertDateStrSnafu)?;

                    // subtract one day from current date and look for log with that date
                    if let Some(sub_dt) = dt.checked_sub_days(Days::new(1)) {
                        let sub_str = sub_dt.format("%Y%m%d").to_string();
                        let contract = Self::get_contract_symbol(&df.symbol, &dt)?;

                        let key = format!(
                            "futures/{}/{}/{}/{}",
                            df.symbol,
                            sub_str,
                            contract,
                            format!("ohlcv-{}.parquet", c.period_s)
                        );

                        let res = client
                            .get_object()
                            .bucket(bucket_name)
                            .key(&key)
                            .send()
                            .await;

                        match res {
                            Ok(object) => {
                                num_days -= 1;
                                // push front so data is in order
                                s3_data.push_front((key, object));
                                curr_date = sub_str;
                                iter_count = 0;
                            }
                            Err(e) => {
                                println!("Err: {:?}", e);
                            }
                        }
                    }
                    iter_count += 1
                }

                // for each date string of historical data, parse each log and publish historical data
                for (key, obj) in s3_data {
                    let body: Vec<u8> = obj
                        .body
                        .collect()
                        .await
                        .context(S3BodyResponseSnafu)?
                        .to_vec();

                    let builder =
                        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(body)).unwrap();
                    tracing::debug!("Converted arrow schema is: {}", builder.schema());

                    let mut reader = builder.build().unwrap();

                    while let Some(Ok(record_batch)) = reader.next() {
                        let timestamp_ns = record_batch.column_by_name("timestamp_ns");
                        let open = record_batch.column_by_name("open");
                        let high = record_batch.column_by_name("high");
                        let low = record_batch.column_by_name("low");
                        let close = record_batch.column_by_name("close");
                        let volume = record_batch.column_by_name("volume");

                        if let (Some(ts), Some(o), Some(h), Some(l), Some(c), Some(v)) =
                            (timestamp_ns, open, high, low, close, volume)
                        {
                            let ts_arr =
                                ts.as_any().downcast_ref::<UInt64Array>().unwrap().values();
                            let o_arr = o.as_any().downcast_ref::<Float32Array>().unwrap().values();
                            let h_arr = h.as_any().downcast_ref::<Float32Array>().unwrap().values();
                            let l_arr = l.as_any().downcast_ref::<Float32Array>().unwrap().values();
                            let c_arr = c.as_any().downcast_ref::<Float32Array>().unwrap().values();
                            let v_arr = v.as_any().downcast_ref::<UInt64Array>().unwrap().values();

                            for i in 0..ts_arr.len() {
                                historical_data.ohlcv.push(Ohlcv {
                                    timestamp_ns: *ts_arr.get(i).unwrap(),
                                    open: *o_arr.get(i).unwrap(),
                                    high: *h_arr.get(i).unwrap(),
                                    low: *l_arr.get(i).unwrap(),
                                    close: *c_arr.get(i).unwrap(),
                                    volume: *v_arr.get(i).unwrap(),
                                })
                            }
                        } else {
                            tracing::error!("Error reading s3 for key: {}", key);
                        }
                    }
                }

                if let Some(hp) = &self.historical_publisher {
                    println!("num candles: {}", historical_data.ohlcv.len());
                    println!("period: {}", historical_data.period_s);
                    hp.publish(historical_data).await.context(PublishSnafu)?;
                }
            }
        }

        Ok(())
    }

    fn parse_config(&self, config_path: &str) -> Result<S3DatafeedConfig, DatafeedError> {
        let f = std::fs::read_to_string(&config_path).context(ReadFileToStringSnafu {
            filename: config_path,
        })?;
        let config: S3DatafeedConfig = serde_yaml::from_str(&f).context(SerdeYamlSnafu {
            filename: config_path,
        })?;

        Ok(config)
    }

    fn get_contract_symbol(symbol: &str, dt: &NaiveDate) -> Result<String, S3DatafeedError> {
        let month = chrono::Month::try_from(dt.month() as u8).context(ConvertDateToChronoSnafu)?;
        let year = dt.year();
        let second_friday =
            NaiveDate::from_weekday_of_month_opt(year, dt.month(), chrono::Weekday::Fri, 2)
                .unwrap();

        let contract_letter = match month {
            Month::January => "H",
            Month::February => "H",
            Month::March => {
                if dt < &second_friday {
                    "H"
                } else {
                    "M"
                }
            }
            Month::April => "M",
            Month::May => "M",
            Month::June => {
                if dt < &second_friday {
                    "M"
                } else {
                    "U"
                }
            }
            Month::July => "U",
            Month::August => "U",
            Month::September => {
                if dt < &second_friday {
                    "U"
                } else {
                    "Z"
                }
            }
            Month::October => "Z",
            Month::November => "Z",
            Month::December => {
                if dt < &second_friday {
                    "Z"
                } else {
                    "H"
                }
            }
        };

        let contract_year = year % 10;

        Ok(format!("{}{}{}", symbol, contract_letter, contract_year))
    }
}

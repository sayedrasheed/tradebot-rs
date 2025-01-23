use crate::algo::{AlgoError, AlgoSetup};
use common::config::TopicMtype;
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::{
    Advice, AlgoComplete, Candle, DatafeedComplete, HistoricalData, Line, Point, Tick, Value,
};
use zenoh_node::error::NodeError;
use zenoh_node::node::{Abort, Node, Publisher, Subscribe, SubscriberError};

use async_trait::async_trait;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use snafu::ResultExt;

pub struct PyAlgoSetup {
    config_path: String,
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
}

impl PyAlgoSetup {
    pub fn new() -> Self {
        Self {
            config_path: String::new(),
            advice_publisher: None,
            line_publisher: None,
            point_publisher: None,
            complete_publisher: None,
        }
    }
}

#[async_trait]
impl AlgoSetup for PyAlgoSetup {
    fn set_config_path(&mut self, config_path: &str) {
        self.config_path = config_path.to_string();
    }

    fn set_algo_complete_publisher(&mut self, publisher: Publisher<AlgoComplete>) {
        self.complete_publisher = Some(publisher);
    }

    async fn setup_publishers(
        &mut self,
        node: &Node,
        topics: &Vec<TopicMtype>,
    ) -> Result<(), AlgoError> {
        for topic in topics {
            match topic.mtype {
                MessageType::Advice => {
                    self.advice_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                MessageType::Line => {
                    self.line_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                MessageType::Point => {
                    self.point_publisher = Some(node.new_publisher(&topic.topic).await?)
                }
                _ => (),
            }
        }
        Ok(())
    }

    async fn setup_subscribers(
        self,
        subscribe_node: &Node,
        subscribe_topics: &Vec<TopicMtype>,
    ) -> Result<Box<dyn Abort>, AlgoError> {
        let ta = PyAlgo::new(
            self.config_path.clone(),
            self.advice_publisher,
            self.line_publisher,
            self.point_publisher,
            self.complete_publisher,
        )?;
        let mut subscriber = subscribe_node.new_subscriber(ta).await?;

        for topic in subscribe_topics {
            match topic.mtype {
                MessageType::Tick => {
                    tracing::debug!("Subscribe to tick message, topic: {}", &topic.topic);
                    subscribe_node
                        .subscribe::<Tick>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::Candle => {
                    tracing::debug!("Subscribe to candle message, topic: {}", &topic.topic);
                    subscribe_node
                        .subscribe::<Candle>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::HistoricalData => {
                    tracing::debug!(
                        "Subscribe to historical data message, topic: {}",
                        &topic.topic
                    );
                    subscribe_node
                        .subscribe::<HistoricalData>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                MessageType::DatafeedComplete => {
                    tracing::debug!(
                        "Subscribe to datafeed complete message, topic: {}",
                        &topic.topic
                    );
                    subscribe_node
                        .subscribe::<DatafeedComplete>(&topic.topic, &mut subscriber)
                        .await
                        .context(PublishSnafu)?;
                }
                _ => (),
            }
        }
        Ok(Box::new(subscriber))
    }
}
#[derive(Debug, Snafu)]
pub enum PyAlgoError {
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

    #[snafu(display("Failed to publish data"))]
    PublishError { source: NodeError },

    #[snafu(display("Failed to read python code from file: {}", path))]
    ReadPythonCodeError {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create python module from source code: {}", path))]
    CreatePythonModuleError { path: String, source: PyErr },

    #[snafu(display("Failed to create python class instance from: {}", class))]
    CreatePythonClassError { class: String, source: PyErr },

    #[snafu(display("Call Historical data setter error"))]
    CreateHistoricalSetterError { source: PyErr },
}

// Yaml config
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PyAlgoConfig {
    py_algo_interface_path: String,
    ema_slow_period: u32,
    ema_fast_period: u32,
    candle_period_s: u32,
}

pub struct PyAlgo {
    module: Py<PyModule>,
    algo_interface: PyObject,
    advice_publisher: Option<Publisher<Advice>>,
    line_publisher: Option<Publisher<Line>>,
    point_publisher: Option<Publisher<Point>>,
    complete_publisher: Option<Publisher<AlgoComplete>>,
}

impl PyAlgo {
    pub fn new(
        config_path: String,
        advice_publisher: Option<Publisher<Advice>>,
        line_publisher: Option<Publisher<Line>>,
        point_publisher: Option<Publisher<Point>>,
        complete_publisher: Option<Publisher<AlgoComplete>>,
    ) -> Result<Self, AlgoError> {
        let f = std::fs::read_to_string(&config_path).context(ReadFileToStringSnafu {
            filename: config_path.clone(),
        })?;
        let config: PyAlgoConfig = serde_yaml::from_str(&f).context(SerdeYamlSnafu {
            filename: config_path,
        })?;

        let code = std::fs::read_to_string(&config.py_algo_interface_path).context(
            ReadPythonCodeSnafu {
                path: &config.py_algo_interface_path,
            },
        )?;

        let module = Python::with_gil(|py| {
            // create python module
            let module = PyModule::from_code(
                py,
                &code,
                config.py_algo_interface_path.as_str(),
                "algo_interface",
            )
            .context(CreatePythonModuleSnafu {
                path: &config.py_algo_interface_path,
            })?;

            Ok::<Py<PyModule>, AlgoError>(module.into())
        })?;

        let algo_interface = Python::with_gil(|py| {
            // create python module
            let module = PyModule::from_code(
                py,
                &code,
                config.py_algo_interface_path.as_str(),
                "algo_interface",
            )
            .context(CreatePythonModuleSnafu {
                path: &config.py_algo_interface_path,
            })?;

            let algo_params = module
                .getattr("AlgoParams")
                .context(CreatePythonClassSnafu {
                    class: "AlgoParams",
                })?
                .call1((
                    config.ema_slow_period,
                    config.ema_fast_period,
                    config.candle_period_s,
                ))
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreatePythonClassSnafu {
                    class: "AlgoParams",
                })?
                .to_object(py);
            // create the object instance
            Ok::<Py<PyAny>, AlgoError>(
                module
                    .getattr("AlgoInterface")
                    .context(CreatePythonClassSnafu {
                        class: "AlgoInterface",
                    })?
                    .call1((algo_params,))
                    .map_err(|err| {
                        err.print_and_set_sys_last_vars(py);
                        err
                    })
                    .context(CreatePythonClassSnafu {
                        class: "AlgoInterface",
                    })?
                    .to_object(py),
            )
        })?;

        Ok(Self {
            advice_publisher,
            line_publisher,
            point_publisher,
            complete_publisher,
            algo_interface,
            module,
        })
    }
}

#[async_trait]
impl Subscribe<Tick> for PyAlgo {
    async fn on_data(&mut self, msg: Tick) -> Result<(), SubscriberError> {
        Ok(())
    }
}

#[async_trait]
impl Subscribe<Candle> for PyAlgo {
    async fn on_data(&mut self, msg: Candle) -> Result<(), SubscriberError> {
        let ohlcv = msg.ohlcv.as_ref().unwrap();
        let (slow_ema_point, fast_ema_point, advice) = Python::with_gil(|py| {
            // Create and set candle input for py handler
            let ci = self
                .module
                .getattr(py, "CandleInput")
                .context(CreatePythonClassSnafu {
                    class: "CandleInput",
                })?
                .call1(py, (ohlcv.timestamp_ns, msg.period_s, ohlcv.close))
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreatePythonClassSnafu {
                    class: "CandleInput",
                })?
                .to_object(py);

            // Create candle output for py handler
            let co = self
                .module
                .getattr(py, "CandleOutput")
                .context(CreatePythonClassSnafu {
                    class: "CandleOutput",
                })?
                .call1(py, ())
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreatePythonClassSnafu {
                    class: "CandleOutput",
                })?
                .to_object(py);

            // Call python candle update handler
            self.algo_interface
                .call_method1(py, "candle_update", (&ci, &co))
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreateHistoricalSetterSnafu)?;

            // Extract candle output and convert into app output for visualization
            // In this case they are points for the two emas
            let py_slow_ema: PyObject = co.getattr(py, "slow_ema")?.extract(py)?;
            let slow_ema_ts = py_slow_ema.getattr(py, "timestamp_ns")?.extract(py)?;
            let slow_ema_val = py_slow_ema.getattr(py, "value")?.extract(py)?;

            let slow_ema_point = Point {
                description: String::from("ema-12"),
                pane_idx: 0,
                period_s: msg.period_s,
                symbol: msg.symbol.clone(),
                value: Some(Value {
                    timestamp_ns: slow_ema_ts,
                    value: slow_ema_val,
                }),
            };

            let py_fast_ema: PyObject = co.getattr(py, "fast_ema")?.extract(py)?;
            let fast_ema_ts = py_fast_ema.getattr(py, "timestamp_ns")?.extract(py)?;
            let fast_ema_val = py_fast_ema.getattr(py, "value")?.extract(py)?;

            let fast_ema_point = Point {
                description: String::from("ema-25"),
                pane_idx: 0,
                period_s: msg.period_s,
                symbol: msg.symbol.clone(),
                value: Some(Value {
                    timestamp_ns: fast_ema_ts,
                    value: fast_ema_val,
                }),
            };

            // Candle output also has an optional advice when algo advises to Buy or Sell
            let py_advice: PyObject = co.getattr(py, "advice")?.extract(py)?;
            let is_valid: bool = py_advice.getattr(py, "is_valid")?.extract(py)?;

            // Convert py algo interface advice into app advice
            let advice = if is_valid {
                let advice_ts: u64 = py_advice.getattr(py, "timestamp_ns")?.extract(py)?;
                let price: f32 = py_advice.getattr(py, "price")?.extract(py)?;
                let size: i32 = py_advice.getattr(py, "size")?.extract(py)?;

                Some(Advice {
                    timestamp_ns: advice_ts,
                    symbol: msg.symbol.clone(),
                    strategy_id: String::new(),
                    order_type: 0,
                    price,
                    size,
                })
            } else {
                None
            };

            Ok::<(Point, Point, Option<Advice>), AlgoError>((
                slow_ema_point,
                fast_ema_point,
                advice,
            ))
        })?;

        // Publish converted app output
        if let Some(publisher) = &self.point_publisher {
            publisher.publish(slow_ema_point).await?;
            publisher.publish(fast_ema_point).await?;
        }

        if let Some(adv) = advice {
            if let Some(publisher) = &self.advice_publisher {
                tracing::debug!(
                    "Publish order timestamp: {} at price: {}",
                    adv.timestamp_ns,
                    adv.price
                );
                publisher.publish(adv).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<HistoricalData> for PyAlgo {
    async fn on_data(&mut self, msg: HistoricalData) -> Result<(), SubscriberError> {
        let (slow_ema_points, fast_ema_points) = Python::with_gil(|py| {
            // Create historical data input for py handler
            let py_dicts: Vec<_> = msg
                .ohlcv
                .into_iter()
                .map(|o| {
                    let dict = PyDict::new(py);
                    dict.set_item("timestamp_ns", o.timestamp_ns).unwrap();
                    dict.set_item("close", o.close).unwrap();
                    dict
                })
                .collect();

            let py_list = PyList::new(py, py_dicts);

            let hdi = self
                .module
                .getattr(py, "HistoricalDataInput")
                .context(CreatePythonClassSnafu {
                    class: "HistoricalDataInput",
                })?
                .call1(py, (msg.period_s, py_list))
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreatePythonClassSnafu {
                    class: "HistoricalDataInput",
                })?
                .to_object(py);

            // Create historical data output for py handler
            let hdo = self
                .module
                .getattr(py, "HistoricalDataOutput")
                .context(CreatePythonClassSnafu {
                    class: "HistoricalDataOutput",
                })?
                .call1(py, (0, PyList::empty(py)))
                .map_err(|err| {
                    err.print_and_set_sys_last_vars(py);
                    err
                })
                .context(CreatePythonClassSnafu {
                    class: "HistoricalDataOutput",
                })?
                .to_object(py);

            // Call python historical data update handler
            let _ = self
                .algo_interface
                .call_method1(py, "historical_data_update", (&hdi, &hdo));

            // Extract historical data output and convert into app output for visualization
            // In this case they are the two EMA lines
            let py_slow_ema = hdo
                .getattr(py, "slow_ema")
                .context(CreateHistoricalSetterSnafu)?;
            let slow_ema_points = if let Ok(slow_ema) = py_slow_ema.cast_as::<PyList>(py) {
                let mut points = Vec::new();

                for ema in slow_ema.iter() {
                    let timestamp_ns: u64 = ema.getattr("timestamp_ns")?.extract()?;
                    let value: f32 = ema.getattr("value")?.extract()?;
                    println!("List element: {}", value);

                    if !value.is_nan() {
                        points.push(Value {
                            timestamp_ns,
                            value,
                        });
                    }
                }

                points
            } else {
                Vec::new()
            };

            let py_fast_ema = hdo
                .getattr(py, "fast_ema")
                .context(CreateHistoricalSetterSnafu)?;
            let fast_ema_points = if let Ok(fast_ema) = py_fast_ema.cast_as::<PyList>(py) {
                let mut points = Vec::new();

                for ema in fast_ema.iter() {
                    let timestamp_ns: u64 = ema.getattr("timestamp_ns")?.extract()?;
                    let value: f32 = ema.getattr("value")?.extract()?;
                    println!("List element: {}", value);

                    if !value.is_nan() {
                        points.push(Value {
                            timestamp_ns,
                            value,
                        });
                    }
                }

                points
            } else {
                Vec::new()
            };

            Ok::<(Vec<Value>, Vec<Value>), AlgoError>((slow_ema_points, fast_ema_points))
        })?;

        let line = Line {
            description: format!("ema-12"),
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            pane_idx: 0,
            points: slow_ema_points,
            color: None,
        };

        if let Some(publisher) = &self.line_publisher {
            publisher.publish(line).await?;
        }

        let line = Line {
            description: format!("ema-25"),
            symbol: msg.symbol.clone(),
            period_s: msg.period_s,
            pane_idx: 0,
            points: fast_ema_points,
            color: None,
        };

        if let Some(publisher) = &self.line_publisher {
            publisher.publish(line).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Subscribe<DatafeedComplete> for PyAlgo {
    async fn on_data(&mut self, msg: DatafeedComplete) -> Result<(), SubscriberError> {
        // Do any shutdown procedure here

        tracing::debug!("Datafeed complete message received");
        let algo_complete = AlgoComplete {
            timestamp_ns: msg.timestamp_ns,
            algo_id: 0, // TODO: not needed
        };

        if let Some(publisher) = &self.complete_publisher {
            tracing::debug!("Publish algo complete message");
            publisher.publish(algo_complete).await?;
        }

        Ok(())
    }
}

use crate::config::TopicMtype;
use rand::Rng;
use std::collections::HashSet;
use std::net::{Ipv4Addr, UdpSocket};
use tradebot_protos::messages::enums::MessageType;
use tradebot_protos::messages::Topic;
use zenoh_node::builder::NodeBuilder;
use zenoh_node::error::NodeError;
use zenoh_node::node::{Node, SubscriberError};

pub struct ServiceNeedsMet {
    services_required: HashSet<String>,
    services_responded: HashSet<String>,
    services_available: HashSet<String>,
}

impl ServiceNeedsMet {
    pub fn new(serviced_required: &HashSet<String>, services_available: &HashSet<String>) -> Self {
        Self {
            services_required: serviced_required.clone(),
            services_available: services_available.clone(),
            services_responded: HashSet::new(),
        }
    }

    pub fn add_available_service(&mut self, service_id: &str) {
        self.services_available.insert(service_id.to_string());
    }

    pub fn add_service_response(&mut self, service_id: &str) {
        self.services_responded.insert(service_id.to_string());
    }

    pub fn is_complete(&mut self) -> bool {
        let mut complete = true;

        tracing::debug!("Services required: {:?}", self.services_required);
        tracing::debug!("Services responded: {:?}", self.services_responded);
        tracing::debug!("Services available: {:?}", self.services_available);
        for service in &self.services_required {
            if self.services_available.len() > 0
                && !self.services_responded.contains(service)
                && self.services_available.contains(service)
            {
                complete = false;
                break;
            }
        }

        complete
    }
}

pub fn generate_zenoh_port() -> Option<u16> {
    const MAX_ITERATIONS: u8 = 20;
    let mut rng = rand::thread_rng();
    let mut result = None;
    let mut iter = 0;

    while result.is_none() && iter < MAX_ITERATIONS {
        let port = rng.gen_range(1024..=49151);
        match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)) {
            Ok(_) => {
                println!("Successfully bound UDP socket to port: {}", port);
                result = Some(port);
            }
            Err(e) => {
                eprintln!("Failed to bind to port {}: {}", port, e);
            }
        }

        iter += 1;
    }

    result
}

pub async fn create_node(ip: &str, port: u16) -> Result<Node, NodeError> {
    let mut node_builder = NodeBuilder::new();

    tracing::debug!("Join network on ip: {}, port: {}", ip, port);
    node_builder.set_network((ip.to_string(), port as u16));

    let node = node_builder.build().await?;

    Ok(node)
}

pub fn convert_topics(topics: &Vec<Topic>) -> Result<Vec<TopicMtype>, SubscriberError> {
    Ok(topics
        .iter()
        .filter_map(|t| {
            let mtype = MessageType::try_from(t.mtype);
            match mtype {
                Ok(mt) => Some(TopicMtype {
                    mtype: mt,
                    topic: t.topic.clone(),
                }),
                Err(_) => None,
            }
        })
        .collect())
}

pub fn adjust_topic(topic: &str, adjust_strs: Vec<&str>) -> String {
    let mut adjusted_topic = topic.to_string();

    for s in adjust_strs {
        adjusted_topic.push('_');
        adjusted_topic.push_str(s);
    }

    adjusted_topic
}

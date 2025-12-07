//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

pub mod amqp;
pub mod file;
pub mod http;
pub mod kafka;
pub mod mqtt;
pub mod memory;
pub mod nats;
pub mod static_endpoint;

use crate::config::{
    ConsumerEndpoint, ConsumerEndpointType, PublisherEndpoint, PublisherEndpointType, Route,
};
use crate::consumers::MessageConsumer;
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::info;

/// Creates a `MessageConsumer` based on the route's "in" configuration.
pub async fn create_consumer_from_route(
    route_name: &str,
    endpoint: &ConsumerEndpoint,
) -> Result<Box<dyn MessageConsumer>> {
    match &endpoint.endpoint_type {
        ConsumerEndpointType::Kafka(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(kafka::KafkaConsumer::new(&cfg.config, topic)?))
        }
        ConsumerEndpointType::Nats(cfg) => {
            let subject = cfg.endpoint.subject.as_deref().unwrap_or(route_name);
            let stream_name = cfg
                .endpoint
                .stream
                .as_deref()
                .or(cfg.config.default_stream.as_deref())
                .ok_or_else(|| {
                    anyhow!(
                        "[route:{}] NATS consumer must specify a 'stream' or have a 'default_stream'",
                        route_name
                    )
                })?;
            Ok(Box::new(
                nats::NatsConsumer::new(&cfg.config, stream_name, subject).await?,
            ))
        }
        ConsumerEndpointType::Amqp(cfg) => {
            let queue = cfg.endpoint.queue.as_deref().unwrap_or(route_name);
            Ok(Box::new(amqp::AmqpConsumer::new(&cfg.config, queue).await?))
        }
        ConsumerEndpointType::Mqtt(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(
                mqtt::MqttConsumer::new(&cfg.config, topic, route_name).await?,
            ))
        }
        ConsumerEndpointType::File(cfg) => {
            Ok(Box::new(file::FileConsumer::new(&cfg.config).await?))
        }
        ConsumerEndpointType::Http(cfg) => {
            Ok(Box::new(http::HttpConsumer::new(&cfg.config).await?))
        }
        ConsumerEndpointType::Static(cfg) => {
            Ok(Box::new(static_endpoint::StaticRequestConsumer::new(cfg)?))
        }
        ConsumerEndpointType::Memory(cfg) => {
            Ok(Box::new(memory::MemoryConsumer::new(&memory::get_or_create_channel(&cfg.config))))
        }
    }
}

/// Creates a `MessagePublisher` based on the route's "out" configuration.
pub async fn create_publisher_from_route(
    route_name: &str,
    endpoint: &PublisherEndpoint,
) -> Result<Arc<dyn MessagePublisher>> {
    match &endpoint.endpoint_type {
        PublisherEndpointType::Kafka(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                kafka::KafkaPublisher::new(&cfg.config, topic).await?,
            ))
        }
        PublisherEndpointType::Nats(cfg) => {
            let subject = cfg.endpoint.subject.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                nats::NatsPublisher::new(&cfg.config, subject, cfg.endpoint.stream.as_deref())
                    .await?,
            ))
        }
        PublisherEndpointType::Amqp(cfg) => {
            let queue = cfg.endpoint.queue.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                amqp::AmqpPublisher::new(&cfg.config, queue).await?,
            ))
        }
        PublisherEndpointType::Mqtt(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                mqtt::MqttPublisher::new(&cfg.config, topic, route_name).await?,
            ))
        }
        PublisherEndpointType::File(cfg) => {
            Ok(Arc::new(file::FilePublisher::new(&cfg.config).await?))
        }
        PublisherEndpointType::Http(cfg) => {
            let mut sink = http::HttpPublisher::new(&cfg.config).await?;
            if let Some(url) = &cfg.endpoint.url {
                sink = sink.with_url(url);
            }
            Ok(Arc::new(sink))
        }
        PublisherEndpointType::Static(cfg) => Ok(Arc::new(
            static_endpoint::StaticEndpointPublisher::new(cfg)?,
        )),
        PublisherEndpointType::Memory(cfg) => {
            Ok(Arc::new(memory::MemoryPublisher::new(&memory::get_or_create_channel(&cfg.config))))
        }
    }
}

/// Creates a `MessagePublisher` for the DLQ if configured.
pub async fn create_dlq_from_route(
    route: &Route,
    name: &str,
) -> Result<Option<Arc<dyn MessagePublisher>>> {
    if let Some(dlq_config) = &route.dlq {
        info!("DLQ configured for route {}", name);
        let topic = dlq_config.kafka.endpoint.topic.as_deref().unwrap_or("dlq");
        let publisher = kafka::KafkaPublisher::new(&dlq_config.kafka.config, topic).await?;
        Ok(Some(Arc::new(publisher)))
    } else {
        Ok(None)
    }
}

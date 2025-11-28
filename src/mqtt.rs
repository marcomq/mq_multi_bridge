use crate::config::MqttConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use rumqttc::{
    tokio_rustls::rustls, AsyncClient, Event, Incoming, MqttOptions, QoS, Transport,
};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};
use fastrand;
pub struct MqttSink {
    client: AsyncClient,
    topic: String,
}

impl MqttSink {
    pub async fn new(config: &MqttConfig, topic: &str) -> anyhow::Result<Self> {
        let (client, _) = create_client_and_eventloop(config).await?;
        Ok(Self {
            client,
            topic: topic.to_string(),
        })
    }

    pub fn with_topic(&self, topic: &str) -> Self {
        Self {
            client: self.client.clone(),
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl MessageSink for MqttSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&message)?;
        self.client
            .publish(&self.topic, QoS::AtLeastOnce, false, payload)
            .await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MqttSource {
    client: AsyncClient,
    eventloop: Arc<Mutex<rumqttc::EventLoop>>,
}

impl MqttSource {
    pub async fn new(config: &MqttConfig, topic: &str) -> anyhow::Result<Self> {
        let (client, eventloop) = create_client_and_eventloop(config).await?;
        client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "MQTT source subscribed");
        Ok(Self {
            client,
            eventloop: Arc::new(Mutex::new(eventloop)),
        })
    }

    pub async fn with_topic(&self, topic: &str) -> anyhow::Result<Self> {
        self.client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "MQTT source subscribed to new topic");
        Ok(self.clone())
    }
}

#[async_trait]
impl MessageSource for MqttSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut eventloop = self.eventloop.lock().await;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    let canonical_message: CanonicalMessage = serde_json::from_slice(&p.payload)?;

                    let commit = Box::new(move || {
                        Box::pin(async move {
                            // With rumqttc, acks are handled internally for QoS 1.
                            // This closure is called after successful processing.
                            info!(topic = %p.topic, "MQTT message processed");
                        }) as BoxFuture<'static, ()>
                    });

                    return Ok((canonical_message, commit));
                }
                Ok(Event::Incoming(Incoming::Disconnect)) => return Err(anyhow!("MQTT disconnected")),
                Err(e) => return Err(anyhow!("MQTT connection error: {}", e)),
                _ => continue, // Ignore other packet types
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn create_client_and_eventloop(
    config: &MqttConfig,
) -> anyhow::Result<(AsyncClient, rumqttc::EventLoop)> {
    let (host, port) = parse_url(&config.url)?;
    let mut mqttoptions = MqttOptions::new(
        format!("mq_multi_bridge_{}", fastrand::u32(..)),
        host,
        port,
    );
    mqttoptions.set_keep_alive(Duration::from_secs(20));
    mqttoptions.set_clean_session(true);

    if config.tls.required {
        let mut root_cert_store = rustls::RootCertStore::empty();
        if let Some(ca_file) = &config.tls.ca_file {
            let mut ca_buf = std::io::BufReader::new(std::fs::File::open(ca_file)?);
            let certs = rustls_pemfile::certs(&mut ca_buf).collect::<Result<Vec<_>, _>>()?;
            for cert in certs {
                root_cert_store.add(cert.into())?;
            }
        }

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        // Note: rumqttc's default rustls integration doesn't easily support client certs.
        // This would require a custom transport implementation if needed.
        if config.tls.accept_invalid_certs {
            warn!("accept_invalid_certs is not supported by rumqttc's default TLS. Certificates will be validated.");
        }
        mqttoptions.set_transport(Transport::tls_with_config(client_config.into()));
    }

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    info!(url = %config.url, "Connected to MQTT broker");
    Ok((client, eventloop))
}

fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let url = url::Url::parse(url)?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in URL"))?.to_string();
    let port = url.port().unwrap_or(if url.scheme() == "mqtts" || url.scheme() == "ssl" { 8883 } else { 1883 });
    Ok((host, port))
}

impl Clone for MqttSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            eventloop: self.eventloop.clone(),
        }
    }
}
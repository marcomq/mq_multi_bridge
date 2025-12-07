use crate::config::MqttConfig;
use crate::consumers::{BoxFuture, CommitFunc, MessageConsumer};
use crate::model::CanonicalMessage;
use crate::publishers::MessagePublisher;
use anyhow::anyhow;
use async_trait::async_trait;
use rumqttc::{tokio_rustls::rustls, AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
pub struct MqttPublisher {
    client: AsyncClient,
    topic: String,
    _eventloop_handle: Arc<JoinHandle<()>>,
}

impl MqttPublisher {
    pub async fn new(config: &MqttConfig, topic: &str, bridge_id: &str) -> anyhow::Result<Self> {
        let (client, mut eventloop) = create_client_and_eventloop(config, bridge_id).await?;
        let topic_clone = topic.to_string();
        // Wait briefly for an initial ConnAck so the caller gets a deterministic connection result.
        let mut connected = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                    if ack.code == rumqttc::ConnectReturnCode::Success {
                        info!("MQTT Publisher connected successfully.");
                        connected = true;
                        break;
                    } else {
                        error!(
                            "MQTT Publisher connection refused: {:?}.",
                            ack.code
                        );
                        return Err(anyhow!("MQTT Publisher connection refused"));
                    }
                }
                Ok(event) => {
                    tracing::trace!("MQTT Publisher received event while waiting for connack: {:?}", event);
                }
                Err(rumqttc::ConnectionError::Tls(e)) => {
                    error!("MQTT Publisher TLS error: {}.", e);
                    return Err(anyhow!("MQTT Publisher TLS error"));
                }
                Err(e) => {
                    error!("MQTT Publisher eventloop error while waiting for connack: {}. retrying...", e);  
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        if !connected {
            return Err(anyhow!("Timeout waiting for MQTT Publisher ConnAck"));
        }

        let eventloop_handle = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        if ack.code == rumqttc::ConnectReturnCode::Success {
                            info!("MQTT Publisher eventloop observed successful ConnAck.");
                        } else {
                            error!(
                                "MQTT Publisher connection refused: {:?}. Halting event loop.",
                                ack.code
                            );
                            break;
                        }
                    }
                    Ok(event) => {
                        tracing::trace!("MQTT Publisher received event: {:?}", event);
                    }
                    Err(rumqttc::ConnectionError::Tls(e)) => {
                        error!("MQTT Publisher TLS error: {}. Halting event loop.", e);
                        break;
                    }
                    Err(e) => {
                        // Log the error and continue the loop. rumqttc will handle reconnection internally.
                        // Adding a sleep here can interfere with its reconnection logic.
                        error!("MQTT Publisher eventloop error: {}. rumqttc will attempt to reconnect.", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            info!(topic = %topic_clone, "MQTT publisher event finished.");
        });

        Ok(Self {
            client,
            topic: topic.to_string(),
            _eventloop_handle: Arc::new(eventloop_handle),
        })
    }
    pub fn with_topic(&self, topic: &str) -> Self {
        Self {
            client: self.client.clone(),
            topic: topic.to_string(),
            _eventloop_handle: self._eventloop_handle.clone(),
        }
    }

    pub async fn disconnect(&self) -> Result<(), rumqttc::ClientError> {
        self.client.disconnect().await
    }
}

#[async_trait]
impl MessagePublisher for MqttPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        tracing::trace!(
            payload = %String::from_utf8_lossy(&message.payload),
            "Publishing MQTT message"
        );
        self
            .client
            .publish(&self.topic, QoS::AtLeastOnce, false, message.payload)
            .await?;
        Ok(None)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MqttConsumer {
    _client: AsyncClient,
    // The receiver for incoming messages from the dedicated eventloop task
    eventloop_handle: Arc<JoinHandle<()>>,
    message_rx: mpsc::Receiver<rumqttc::Publish>,
}

impl MqttConsumer {
    pub async fn new(config: &MqttConfig, topic: &str, bridge_id: &str) -> anyhow::Result<Self> {
        let (client, mut eventloop) = create_client_and_eventloop(config, bridge_id).await?;
        let (message_tx, message_rx) = mpsc::channel(10);

        // The subscription must happen *after* the client is connected.
        // The best place for this is inside the eventloop task itself,
        // which can wait for the connection to be established.
        let topic_clone = topic.to_string();
        let client_clone = client.clone();

        // Wait briefly for an initial ConnAck so the caller gets a deterministic connection result.
        let mut connected = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                    if ack.code == rumqttc::ConnectReturnCode::Success {
                        info!("MQTT Consumer connected.");
                        // Perform initial subscription now that we know we're connected.
                        if let Err(e) = client_clone
                            .subscribe(topic_clone.clone(), QoS::AtLeastOnce)
                            .await
                        {
                            error!("Failed to subscribe to MQTT topic: {}.", e);
                            return Err(anyhow!("Failed to subscribe to topic"));
                        }
                        connected = true;
                        break;
                    } else {
                        error!(
                            "MQTT Consumer connection refused: {:?}.",
                            ack.code
                        );
                        return Err(anyhow!("MQTT Consumer connection refused"));
                    }
                }
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    if message_tx.send(publish).await.is_err() {
                        info!("MQTT consumer's message channel closed while waiting for connack. Exiting.");
                        break;
                    }
                }
                Err(rumqttc::ConnectionError::Tls(e)) => {
                    error!("MQTT Consumer TLS error: {}.", e);
                    return Err(anyhow!("MQTT Consumer TLS error"));
                }
                Err(e) => {
                    error!("MQTT Consumer eventloop error while waiting for connack: {}. retrying...", e);
                }
                _ => {}
            }
        }

        if !connected {
            return Err(anyhow!("Timeout waiting for MQTT Consumer ConnAck"));
        }

        // Spawn a dedicated task to continue polling the eventloop
        let eventloop_handle = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        if message_tx.send(publish).await.is_err() {
                            info!("MQTT consumer's message channel closed. Exiting eventloop.");
                            break;
                        }
                    }
                    Err(rumqttc::ConnectionError::Tls(e)) => {
                        error!("MQTT Consumer TLS error: {}. Halting event loop.", e);
                        break;
                    }
                    Err(e) => {
                        error!("MQTT Consumer eventloop error: {}. rumqttc will attempt to reconnect.", e);
                    }
                    _ => {} // Ignore other events
                }
            }
        });

        Ok(Self {
            _client: client,
            message_rx,
            eventloop_handle: Arc::new(eventloop_handle),
        })
    }
}

impl Drop for MqttConsumer {
    fn drop(&mut self) {
        // When the source is dropped, abort its background eventloop task.
        self.eventloop_handle.abort();
    }
}

#[async_trait]
impl MessageConsumer for MqttConsumer {
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        let p = self
            .message_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("MQTT source channel closed"))?;

        let canonical_message = CanonicalMessage::new(p.payload.to_vec());

        let commit = Box::new(move |_response| {
            Box::pin(async move {
                // With rumqttc, acks are handled internally for QoS 1.
                // This closure is called after successful processing.
                info!(topic = %p.topic, "MQTT message processed");
            }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn create_client_and_eventloop(
    config: &MqttConfig,
    bridge_id: &str,
) -> anyhow::Result<(AsyncClient, rumqttc::EventLoop)> {
    let (host, port) = parse_url(&config.url)?;
    // Use a unique client ID based on the bridge_id to prevent collisions.
    let client_id = sanitize_for_client_id(&format!("mq-multi-bridge-{}", bridge_id));
    let mut mqttoptions = MqttOptions::new(client_id, host, port);

    mqttoptions.set_keep_alive(Duration::from_secs(20));
    mqttoptions.set_clean_session(true);

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        mqttoptions.set_credentials(username, password);
    }

    if config.tls.required {
        let mut root_cert_store = rustls::RootCertStore::empty();
        if let Some(ca_file) = &config.tls.ca_file {
            let mut ca_buf = std::io::BufReader::new(std::fs::File::open(ca_file)?);
            let certs = rustls_pemfile::certs(&mut ca_buf).collect::<Result<Vec<_>, _>>()?;
            for cert in certs {
                root_cert_store.add(cert)?;
            }
        }

        let client_config_builder =
            rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

        let mut client_config = if config.tls.is_mtls_client_configured() {
            let cert_file = config.tls.cert_file.as_ref().unwrap();
            let key_file = config.tls.key_file.as_ref().unwrap();
            let cert_chain = load_certs(cert_file)?;
            let key_der = load_private_key(key_file)?;
            client_config_builder.with_client_auth_cert(cert_chain, key_der)?
        } else {
            client_config_builder.with_no_client_auth()
        };

        if config.tls.accept_invalid_certs {
            warn!("MQTT TLS is configured to accept invalid certificates. This is insecure and should not be used in production.");
            let mut dangerous_config = client_config.dangerous();
            dangerous_config.set_certificate_verifier(Arc::new(NoopServerCertVerifier {}));
        }
        mqttoptions.set_transport(Transport::tls_with_config(client_config.into()));
    }

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    // The connection is established by the eventloop automatically.
    // We don't need a manual health check. If connection fails, the eventloop will error out.
    info!(url = %config.url, "MQTT client created. Eventloop will connect.");
    Ok((client, eventloop))
}

/// Sanitizes a string to be used as part of an MQTT client ID.
/// Replaces non-alphanumeric characters with a hyphen.
fn sanitize_for_client_id(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect()
}

fn load_certs(path: &str) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut cert_buf = std::io::BufReader::new(std::fs::File::open(path)?);
    Ok(rustls_pemfile::certs(&mut cert_buf).collect::<Result<Vec<_>, _>>()?)
}

fn load_private_key(path: &str) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let mut key_buf = std::io::BufReader::new(std::fs::File::open(path)?);
    let key = rustls_pemfile::private_key(&mut key_buf)?
        .ok_or_else(|| anyhow!("No private key found in {}", path))?;
    Ok(key)
}

/// A rustls certificate verifier that does not perform any validation.
#[derive(Debug)]
struct NoopServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoopServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let url = url::Url::parse(url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("No host in URL"))?
        .to_string();
    // Prefer IPv4 localhost to avoid macOS resolving `localhost` to ::1
    // which can bypass Docker Desktop port forwarding in some setups.
    let host = if host == "localhost" { "127.0.0.1".to_string() } else { host };
    let port = url
        .port()
        .unwrap_or(if url.scheme() == "mqtts" || url.scheme() == "ssl" {
            8883
        } else {
            1883
        });
    Ok((host, port))
}

use crate::config::MqttConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use rumqttc::ClientError;
use rumqttc::{tokio_rustls::rustls, AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};
pub struct MqttSink {
    client: AsyncClient,
    topic: String,
}

impl MqttSink {
    pub async fn new(config: &MqttConfig, topic: &str) -> anyhow::Result<Self> {
        let (client, mut eventloop) = create_client_and_eventloop(config).await?;
        // We need to spawn a task to poll the eventloop for the sink client
        tokio::spawn(async move {
            loop {
                if let Err(e) = eventloop.poll().await {
                    error!("MQTT Sink eventloop error: {}", e);
                    break;
                }
            }
        });
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
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let payload = serde_json::to_string(&message)?;
        self.client
            .publish(&self.topic, QoS::AtLeastOnce, false, payload)
            .await?;
        Ok(None)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MqttSource {
    client: AsyncClient,
    // The receiver for incoming messages from the dedicated eventloop task
    message_rx: Arc<Mutex<mpsc::Receiver<rumqttc::Publish>>>,
}

impl MqttSource {
    pub async fn new(config: &MqttConfig, topic: &str) -> anyhow::Result<Self> {
        let (client, mut eventloop) = create_client_and_eventloop(config).await?;
        let (message_tx, message_rx) = mpsc::channel(10);

        // Spawn a dedicated task to poll the eventloop
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        if message_tx.send(p).await.is_err() {
                            // Receiver was dropped, so we can exit
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::Disconnect)) => {
                        error!("MQTT Source disconnected.");
                        break;
                    }
                    Err(e) => {
                        error!("MQTT Source eventloop error: {}", e);
                        break;
                    }
                    _ => {} // Ignore other events
                }
            }
        });

        if !topic.is_empty() {
            client.subscribe(topic, QoS::AtLeastOnce).await?;
            info!(topic = %topic, "MQTT source subscribed");
        }
        Ok(Self {
            client,
            message_rx: Arc::new(Mutex::new(message_rx)),
        })
    }

    pub async fn with_topic(&self, topic: &str) -> anyhow::Result<Self> {
        self.client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "MQTT source subscribed to new topic");
        Ok(self.clone())
    }

    // Add an accessor for the client
    pub fn client(&self) -> AsyncClient {
        self.client.clone()
    }
}

#[async_trait]
impl MessageSource for MqttSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut message_rx = self.message_rx.lock().await;
        let p = message_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("MQTT source channel closed"))?;

        let canonical_message: CanonicalMessage = serde_json::from_slice(&p.payload)?;

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
) -> anyhow::Result<(AsyncClient, rumqttc::EventLoop)> {
    let mut last_error: Option<ClientError> = None;
    for attempt in 1..=5 {
        let (host, port) = parse_url(&config.url)?;
        // Use a UUID to guarantee a unique client ID to prevent the broker from disconnecting one of the clients.
        let client_id = format!("mq_multi_bridge_{}", uuid::Uuid::new_v4());
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
                    root_cert_store.add(cert.into())?;
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
        // The rumqttc client doesn't connect immediately. We check the connection by trying to subscribe.
        match client.subscribe("mq-bridge-health-check", QoS::AtMostOnce).await {
            Ok(_) => {
                info!(url = %config.url, "Connected to MQTT broker");
                client.unsubscribe("mq-bridge-health-check").await?;
                return Ok((client, eventloop));
            }
            Err(e) => {
                last_error = Some(e);
                tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
            }
        }
    }
    Err(anyhow!("Failed to connect to MQTT after multiple attempts: {:?}", last_error.unwrap()))
}

fn load_certs(path: &str) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut cert_buf = std::io::BufReader::new(std::fs::File::open(path)?);
    Ok(rustls_pemfile::certs(&mut cert_buf).collect::<Result<Vec<_>, _>>()?)
}

fn load_private_key(
    path: &str,
) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
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

    fn verify_tls12_signature(&self, _message: &[u8], _cert: &rustls::pki_types::CertificateDer<'_>, _dss: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> { Ok(rustls::client::danger::HandshakeSignatureValid::assertion()) }

    fn verify_tls13_signature(&self, _message: &[u8], _cert: &rustls::pki_types::CertificateDer<'_>, _dss: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> { Ok(rustls::client::danger::HandshakeSignatureValid::assertion()) }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> { rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes() }
}

fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let url = url::Url::parse(url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("No host in URL"))?
        .to_string();
    let port = url
        .port()
        .unwrap_or(if url.scheme() == "mqtts" || url.scheme() == "ssl" {
            8883
        } else {
            1883
        });
    Ok((host, port))
}

impl Clone for MqttSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            message_rx: self.message_rx.clone(),
        }
    }
}

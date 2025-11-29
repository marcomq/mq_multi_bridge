use crate::config::NatsConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_nats::{jetstream, Client, ConnectOptions};
use async_trait::async_trait;
use futures::StreamExt;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring as rustls_ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct NatsSink {
    client: Client,
    subject: String,
}

impl NatsSink {
    pub async fn new(config: &NatsConfig, subject: &str) -> anyhow::Result<Self> {
        let options = build_nats_options(config).await?;
        let client = options.connect(&config.url).await?;

        Ok(Self {
            client,
            subject: subject.to_string(),
        })
    }

    pub fn with_subject(&self, subject: &str) -> Self {
        Self {
            client: self.client.clone(),
            subject: subject.to_string(),
        }
    }
}

#[async_trait]
impl MessageSink for NatsSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let payload = serde_json::to_vec(&message)?;
        self.client
            .publish(self.subject.clone(), payload.into())
            .await?;
        self.client.flush().await?; // Ensures the message is sent
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct NatsSource {
    jetstream: jetstream::Context,
    stream_name: String,
    // The subscription is now optional, as it's created when a subject is specified.
    subscription: Arc<Mutex<Option<jetstream::consumer::pull::Stream>>>,
}
use std::any::Any;

impl NatsSource {
    pub async fn new(config: &NatsConfig, stream_name: &str) -> anyhow::Result<Self> {
        let options = build_nats_options(config).await?;
        let client = options.connect(&config.url).await?;
        let jetstream = jetstream::new(client);

        if !stream_name.is_empty() {
            // Check if the stream exists, but don't create a consumer yet.
            let _stream = jetstream.get_stream(stream_name).await?;
            info!(stream = %stream_name, "NATS source connection established");
        }

        Ok(Self {
            jetstream,
            stream_name: stream_name.to_string(),
            subscription: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn with_subject_and_stream(
        &self,
        subject: &str,
        stream_override: &Option<String>,
        conn_config: &NatsConfig,
    ) -> anyhow::Result<Self> {
        let stream_name = stream_override
            .as_deref()
            .or(conn_config.default_stream.as_deref())
            .ok_or_else(|| anyhow!("NATS route for subject '{}' must have a 'stream' or the connection must have a 'default_stream'", subject))?;

        // Create a new consumer specifically for the given subject.
        let stream = self.jetstream.get_stream(stream_name).await?;
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                // Create a unique, durable consumer name based on stream and subject
                // to allow for multiple routes from the same stream.
                durable_name: Some(format!(
                    "mq-bridge-{}-{}",
                    stream_name,
                    subject.replace('.', "-")
                )),
                filter_subject: subject.to_string(),
                ..Default::default()
            })
            .await?;

        let subscription = consumer.messages().await?;
        info!(stream = %stream_name, subject = %subject, "NATS source subscribed to subject");

        Ok(Self {
            jetstream: self.jetstream.clone(),
            stream_name: stream_name.to_string(),
            subscription: Arc::new(Mutex::new(Some(subscription))),
        })
    }
}

#[async_trait]
impl MessageSource for NatsSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut sub = self.subscription.lock().await;
        let message = sub
            .as_mut()
            .ok_or_else(|| anyhow!("NATS source is not subscribed to a subject"))?
            .next()
            .await
            .ok_or_else(|| anyhow!("NATS subscription stream ended"))??;

        let canonical_message: CanonicalMessage =
            serde_json::from_slice(&message.payload).map_err(anyhow::Error::from)?;

        let commit = Box::new(move |_response| {
            Box::pin(async move {
                message
                    .ack()
                    .await
                    .unwrap_or_else(|e| tracing::error!("Failed to ACK NATS message: {:?}", e))
            }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for NatsSource {
    fn clone(&self) -> Self {
        Self {
            jetstream: self.jetstream.clone(),
            stream_name: self.stream_name.clone(),
            subscription: self.subscription.clone(),
        }
    }
}

// ... rest of the file
async fn build_nats_options(config: &NatsConfig) -> anyhow::Result<ConnectOptions> {
    if !config.tls.required {
        return Ok(ConnectOptions::new());
    }

    let mut root_store = rustls::RootCertStore::empty();
    if let Some(ca_file) = &config.tls.ca_file {
        let mut pem = BufReader::new(std::fs::File::open(ca_file)?);
        for cert in rustls_pemfile::certs(&mut pem) {
            root_store.add(cert?)?;
        }
    }

    let mut client_auth_certs = Vec::new();
    if let Some(cert_file) = &config.tls.cert_file {
        let mut pem = BufReader::new(std::fs::File::open(cert_file)?);
        for cert in rustls_pemfile::certs(&mut pem) {
            client_auth_certs.push(cert?);
        }
    }

    let mut client_auth_key = None;
    if let Some(key_file) = &config.tls.key_file {
        let key_bytes = tokio::fs::read(key_file).await?;
        let mut keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut key_bytes.as_slice())
            .collect::<Result<_, _>>()?;
        if !keys.is_empty() {
            client_auth_key = Some(PrivateKeyDer::Pkcs8(keys.remove(0)));
        }
    }

    let provider = rustls_ring::default_provider();

    let mut tls_config = ClientConfig::builder_with_provider(Arc::new(provider.clone()))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(root_store)
        .with_client_auth_cert(
            client_auth_certs,
            client_auth_key
                .ok_or_else(|| anyhow!("Client key is required but not found or invalid"))?,
        )?;

    if config.tls.accept_invalid_certs {
        #[derive(Debug)]
        struct NoopServerCertVerifier {
            supported_schemes: Vec<SignatureScheme>,
        }
        impl ServerCertVerifier for NoopServerCertVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &CertificateDer<'_>,
                _intermediates: &[CertificateDer<'_>],
                _server_name: &rustls::pki_types::ServerName,
                _ocsp_response: &[u8],
                _now: UnixTime,
            ) -> Result<ServerCertVerified, RustlsError> {
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                self.supported_schemes.clone()
            }
        }
        let schemes = provider
            .signature_verification_algorithms
            .supported_schemes();
        let verifier = NoopServerCertVerifier {
            supported_schemes: schemes,
        };
        tls_config
            .dangerous()
            .set_certificate_verifier(Arc::new(verifier));
    }

    Ok(ConnectOptions::new().tls_client_config(tls_config))
}

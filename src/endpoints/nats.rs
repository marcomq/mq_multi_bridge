use crate::config::NatsConfig;
use crate::consumers::{BoxFuture, BoxedMessageStream, MessageConsumer};
use crate::model::CanonicalMessage;
use crate::publishers::MessagePublisher;
use anyhow::anyhow;
use async_nats::{jetstream, jetstream::stream, ConnectOptions};
use async_trait::async_trait;
use futures::StreamExt;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring as rustls_ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use std::io::BufReader;
use std::sync::Arc;
use tokio::{sync::Mutex, time::Duration};
use tracing::info;

pub struct NatsPublisher {
    jetstream: jetstream::Context,
    subject: String,
}

impl NatsPublisher {
    pub async fn new(
        config: &NatsConfig,
        subject: &str,
        stream_name: Option<&str>,
    ) -> anyhow::Result<Self> {
        let options = build_nats_options(config).await?;
        let client = options.connect(&config.url).await?;
        let jetstream = jetstream::new(client.clone());

        // Ensure the stream exists. This is idempotent.
        // The stream name is now passed in directly.
        if let Some(stream_name) = stream_name {
            info!(stream = %stream_name, "Ensuring NATS stream exists");
            jetstream
                .get_or_create_stream(stream::Config {
                    name: stream_name.to_string(),
                    // The stream must be configured to capture the specific subject,
                    // and optionally any sub-subjects if it's a wildcard.
                    // A subject filter of `foo.>` will match `foo.bar` but not `foo`.
                    // So we need both `subject` and `subject.*` if we want to match both.
                    subjects: vec![subject.to_string(), format!("{}.>", subject)],
                    ..Default::default()
                })
                .await?;
        } else {
            info!("No default_stream configured for NATS sink, skipping stream creation. This may not work with a JetStream source.");
        }

        Ok(Self {
            jetstream,
            subject: subject.to_string(),
        })
    }

    pub fn with_subject(&self, subject: &str) -> Self {
        Self {
            jetstream: self.jetstream.clone(),
            subject: subject.to_string(),
        }
    }
}

#[async_trait]
impl MessagePublisher for NatsPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let payload = serde_json::to_vec(&message)?;
        // Use jetstream.publish and await the ack future to guarantee delivery.
        self.jetstream
            .publish(self.subject.clone(), payload.into())
            .await?
            .await?; // This second await waits for the server acknowledgement.
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct NatsConsumer {
    jetstream: jetstream::Context,
    subscription: Arc<Mutex<jetstream::consumer::pull::Stream>>,
}
use std::any::Any;

impl NatsConsumer {
    pub async fn new(
        config: &NatsConfig,
        stream_name: &str,
        subject: &str,
    ) -> anyhow::Result<Self> {
        let options = build_nats_options(config).await?;
        let client = options.connect(&config.url).await?;
        let jetstream = jetstream::new(client);

        // Create a new consumer specifically for the given subject.
        // Retry getting the stream to handle race conditions where the sink is still creating it.
        let mut stream = None;
        for attempt in 0..20 {
            match jetstream.get_stream(stream_name).await {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => {
                    if attempt < 19 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        let stream = stream.ok_or_else(|| {
            anyhow!(
                "Failed to get NATS stream '{}' after multiple retries",
                stream_name
            )
        })?;
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
                // Start from the beginning of the stream for all messages
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                // Set a reasonable max ack pending to ensure we don't lose messages
                max_ack_pending: 10000,
                ..Default::default()
            })
            .await?;

        // Give the consumer a moment to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        let subscription = consumer.messages().await?;
        info!(stream = %stream_name, subject = %subject, "NATS source subscribed to subject");

        Ok(Self {
            jetstream,
            subscription: Arc::new(Mutex::new(subscription)),
        })
    }
}

#[async_trait]
impl MessageConsumer for NatsConsumer {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut sub = self.subscription.lock().await;
        let message = sub
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

impl Clone for NatsConsumer {
    fn clone(&self) -> Self {
        Self {
            jetstream: self.jetstream.clone(),
            subscription: self.subscription.clone(),
        }
    }
}

// ... rest of the file
async fn build_nats_options(config: &NatsConfig) -> anyhow::Result<ConnectOptions> {
    let mut options = if let Some(token) = &config.token {
        ConnectOptions::with_token(token.clone())
    } else if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        ConnectOptions::with_user_and_password(user.clone(), pass.clone())
    } else {
        ConnectOptions::new()
    };

    if !config.tls.required {
        return Ok(options);
    }

    let mut root_store = rustls::RootCertStore::empty();
    if let Some(ca_file) = &config.tls.ca_file {
        let mut pem = BufReader::new(std::fs::File::open(ca_file)?);
        for cert in rustls_pemfile::certs(&mut pem) {
            root_store.add(cert?)?;
        }
    }

    let tls_config = if config.tls.is_mtls_client_configured() {
        let cert_file = config.tls.cert_file.as_ref().unwrap();
        let key_file = config.tls.key_file.as_ref(); // key_file is optional for some certs
        let mut client_auth_certs = Vec::new();
        let mut pem = BufReader::new(std::fs::File::open(cert_file)?);
        for cert in rustls_pemfile::certs(&mut pem) {
            client_auth_certs.push(cert?);
        }

        let mut client_auth_key = None;
        if let Some(key_file) = key_file {
            let key_bytes = tokio::fs::read(key_file).await?;
            let mut keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut key_bytes.as_slice())
                .collect::<Result<_, _>>()?;
            if !keys.is_empty() {
                client_auth_key = Some(PrivateKeyDer::Pkcs8(keys.remove(0)));
            }
        }

        let provider = rustls_ring::default_provider(); // Corrected line
        let tls_config_builder = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(root_store);

        let tls_config_builder = tls_config_builder.with_client_auth_cert(
            client_auth_certs,
            client_auth_key
                .ok_or_else(|| anyhow!("Client key is required but not found or invalid"))?,
        )?;
        tls_config_builder
    } else {
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

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
        let schemes = rustls_ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes();
        let verifier = NoopServerCertVerifier {
            supported_schemes: schemes,
        };
        let mut new_tls_config = tls_config;
        new_tls_config
            .dangerous()
            .set_certificate_verifier(Arc::new(verifier));
        options = options.tls_client_config(new_tls_config);
    } else {
        options = options.tls_client_config(tls_config);
    }

    Ok(options)
}

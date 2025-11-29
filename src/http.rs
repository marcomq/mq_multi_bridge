//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

use crate::config::HttpConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde_json::Value;
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, instrument};

type HttpSourceMessage = (
    CanonicalMessage,
    oneshot::Sender<anyhow::Result<Option<CanonicalMessage>>>,
);

/// A source that listens for incoming HTTP requests.
#[derive(Clone)]
pub struct HttpSource {
    request_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<HttpSourceMessage>>>,
}

impl HttpSource {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        let (request_tx, request_rx) = mpsc::channel::<HttpSourceMessage>(100);

        let app = Router::new()
            .route("/", post(handle_request))
            .with_state(request_tx);

        let listen_address = config
            .listen_address
            .as_deref()
            .ok_or_else(|| anyhow!("'listen_address' is required for http source connection"))?;
        let addr: SocketAddr = listen_address
            .parse()
            .with_context(|| format!("Invalid listen address: {}", listen_address))?;

        tokio::spawn(async move {
            info!("HTTP source listening on {}", addr);
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        Ok(Self {
            request_rx: Arc::new(tokio::sync::Mutex::new(request_rx)),
        })
    }
}

#[async_trait]
impl MessageSource for HttpSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut rx = self.request_rx.lock().await;
        let (message, response_tx) = rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("HTTP source channel closed"))?;

        // The commit function sends the response back to the HTTP client.
        let commit = Box::new(move |response_from_sink: Option<CanonicalMessage>| {
            Box::pin(async move {
                if response_tx.send(Ok(response_from_sink)).is_err() {
                    error!("Failed to send response back to HTTP source handler");
                }
            }) as BoxFuture<'static, ()>
        });

        Ok((message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[instrument(skip_all, fields(http.method = "POST", http.uri = "/"))]
async fn handle_request(
    State(tx): State<mpsc::Sender<HttpSourceMessage>>,
    Json(payload): Json<Value>,
) -> Response {
    let (response_tx, response_rx) = oneshot::channel();
    let message = CanonicalMessage::deserialized_new(payload);

    if tx.send((message, response_tx)).await.is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to send request to bridge",
        )
            .into_response();
    }

    match response_rx.await {
        Ok(Ok(Some(response_message))) => {
            (StatusCode::OK, Json(response_message.payload)).into_response()
        }
        Ok(Ok(None)) => (StatusCode::ACCEPTED, "Message processed").into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error processing message: {}", e),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to receive response from bridge",
        )
            .into_response(),
    }
}

/// A sink that sends messages to an HTTP endpoint.
#[derive(Clone)]
pub struct HttpSink {
    client: reqwest::Client,
    url: String,
    response_sink: Option<String>,
}

impl HttpSink {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            url: config.url.clone().unwrap_or_default(),
            response_sink: config.response_sink.clone(),
        })
    }

    pub fn with_url(&self, url: &str) -> Self {
        Self {
            client: self.client.clone(),
            url: url.to_string(),
            response_sink: self.response_sink.clone(),
        }
    }
}

#[async_trait]
impl MessageSink for HttpSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let response = self
            .client
            .post(&self.url)
            .json(&message)
            .send()
            .await
            .with_context(|| format!("Failed to send HTTP request to {}", self.url))?;

        let response_status = response.status();
        let response_payload = response
            .json::<Value>()
            .await
            .with_context(|| "Failed to parse JSON response from HTTP sink")?;

        if !response_status.is_success() {
            return Err(anyhow!(
                "HTTP sink request failed with status {}: {:?}",
                response_status,
                response_payload
            ));
        }

        // If a response sink is configured, wrap the response in a CanonicalMessage
        if self.response_sink.is_some() {
            Ok(Some(CanonicalMessage::new(response_payload)))
        } else {
            Ok(None)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

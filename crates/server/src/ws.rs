//! WebSocket handler. One socket per connection, three concurrent tasks:
//! receiver (audio in), pipeline (VAD + scheduling + stabilization), sender
//! (transcript out).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Semaphore};

use crate::config::Config;
use crate::pipeline::Pipeline;
use crate::protocol::{ClientMessage, ServerMessage, StartParams};
use crate::whisper::WhisperEngine;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSCRIPT_QUEUE: usize = 64;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub engine: Arc<WhisperEngine>,
    /// Admission control: capacity is what the inference pool can sustain at
    /// realtime. Beyond it we reject rather than make everyone slow.
    pub slots: Arc<Semaphore>,
}

impl AppState {
    pub fn new(cfg: Arc<Config>, engine: Arc<WhisperEngine>) -> Self {
        let slots = Arc::new(Semaphore::new(cfg.max_connections));
        Self { cfg, engine, slots }
    }
}

pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Ok(permit) = Arc::clone(&state.slots).try_acquire_owned() else {
        tracing::warn!("rejecting connection: at capacity");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "transcription capacity reached",
        )
            .into_response();
    };
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = serve(socket, state).await {
            tracing::warn!(error = %e, "connection ended with error");
        }
        drop(permit);
    })
}

async fn serve(socket: WebSocket, state: AppState) -> anyhow::Result<()> {
    let (mut sink, mut stream) = socket.split();

    // --- handshake ---------------------------------------------------------
    let params = match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_start(&mut stream)).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("bad_handshake", e.to_string()),
            )
            .await;
            return Ok(());
        }
        Err(_) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("handshake_timeout", "no start message"),
            )
            .await;
            return Ok(());
        }
    };

    if params.encoding != "pcm_s16le" {
        let _ = send(
            &mut sink,
            &ServerMessage::error(
                "unsupported_encoding",
                format!("expected pcm_s16le, got {}", params.encoding),
            ),
        )
        .await;
        return Ok(());
    }

    let (out_tx, mut out_rx) = mpsc::channel::<ServerMessage>(TRANSCRIPT_QUEUE);
    let pipeline = match Pipeline::new(
        Arc::clone(&state.cfg),
        Arc::clone(&state.engine),
        &params,
        out_tx.clone(),
    ) {
        Ok(p) => p,
        Err(e) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("init_failed", e.to_string()),
            )
            .await;
            return Ok(());
        }
    };

    send(&mut sink, &ServerMessage::Ready).await?;
    tracing::info!(sample_rate = params.sample_rate, language = %params.language_or_default(), "stream started");

    // --- task 3: sender ----------------------------------------------------
    let sender = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let done = matches!(msg, ServerMessage::Done { .. });
            if send(&mut sink, &msg).await.is_err() {
                break;
            }
            if done {
                let _ = sink.send(Message::Close(None)).await;
                break;
            }
        }
    });

    // --- task 2: pipeline --------------------------------------------------
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(state.cfg.audio_queue);
    let pipeline_task = tokio::spawn(pipeline.run(audio_rx));

    // --- task 1: receiver --------------------------------------------------
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                // Await, don't try_send: backpressure the socket rather than
                // ever dropping input audio.
                if audio_tx.send(data.to_vec()).await.is_err() {
                    break;
                }
            }
            Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Stop) => break,
                Ok(ClientMessage::Start(_)) => {
                    tracing::debug!("ignoring duplicate start");
                }
                Err(e) => {
                    let _ = out_tx
                        .send(ServerMessage::error("bad_control", e.to_string()))
                        .await;
                }
            },
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(error = %e, "websocket receive error");
                break;
            }
        }
    }

    // Closing the audio channel makes the pipeline flush and emit `done`;
    // dropping our sender lets the sender task terminate afterwards.
    drop(audio_tx);
    drop(out_tx);

    let _ = pipeline_task.await;
    let _ = sender.await;
    tracing::info!("stream finished");
    Ok(())
}

async fn read_start(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
) -> anyhow::Result<StartParams> {
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Text(text) => match serde_json::from_str::<ClientMessage>(&text)? {
                ClientMessage::Start(p) => return Ok(p),
                ClientMessage::Stop => anyhow::bail!("stop before start"),
            },
            Message::Binary(_) => anyhow::bail!("audio received before start handshake"),
            Message::Close(_) => anyhow::bail!("closed before start"),
            _ => continue,
        }
    }
    anyhow::bail!("stream ended before start")
}

async fn send(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &ServerMessage,
) -> anyhow::Result<()> {
    let json = serde_json::to_string(msg)?;
    sink.send(Message::Text(Utf8Bytes::from(json))).await?;
    Ok(())
}

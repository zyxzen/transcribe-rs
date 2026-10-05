//! WebSocket handler. One socket per connection, four concurrent tasks:
//! receiver (audio in), pipeline (VAD + scheduling + stabilization), sender
//! (transcript out + keepalive pings) and recorder (persistence).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use sqlx::PgPool;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Config;
use crate::live::LiveRegistry;
use crate::pipeline::Pipeline;
use crate::protocol::{ClientMessage, ServerMessage, StartParams};
use crate::recorder::Recorder;
use crate::sessions::{self, OpenRequest};
use crate::whisper::WhisperEngine;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSCRIPT_QUEUE: usize = 64;
/// How long a resuming connection waits for the previous connection on the
/// same session to finish flushing and release it.
const RESUME_WAIT: Duration = Duration::from_secs(5);
const PING_EVERY: Duration = Duration::from_secs(15);
/// No inbound frame (audio, control or pong) for this long means the peer is
/// gone; closing releases the session's live claim.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub engine: Arc<WhisperEngine>,
    /// Admission control: capacity is what the inference pool can sustain at
    /// realtime. Beyond it we reject rather than make everyone slow.
    pub slots: Arc<Semaphore>,
    pub db: PgPool,
    pub live: LiveRegistry,
    /// Cancelled on Ctrl-C; receivers end their streams as if the client
    /// had sent `stop`.
    pub shutdown: CancellationToken,
    /// Tracks connection futures so shutdown can wait for them to flush.
    pub tracker: TaskTracker,
}

impl AppState {
    pub fn new(cfg: Arc<Config>, engine: Arc<WhisperEngine>, db: PgPool) -> Self {
        let slots = Arc::new(Semaphore::new(cfg.max_connections));
        Self {
            cfg,
            engine,
            slots,
            db,
            live: LiveRegistry::default(),
            shutdown: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
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
    let tracker = state.tracker.clone();
    ws.on_upgrade(move |socket| {
        tracker.track_future(async move {
            if let Err(e) = serve(socket, state).await {
                tracing::warn!(error = %e, "connection ended with error");
            }
            drop(permit);
        })
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
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let pipeline = match Pipeline::new(
        Arc::clone(&state.cfg),
        Arc::clone(&state.engine),
        &params,
        out_tx.clone(),
        events_tx,
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

    // --- session -----------------------------------------------------------
    let language = params.language_or_default();
    let opened = match sessions::open(
        &state.db,
        &state.live,
        &state.cfg.data_dir,
        OpenRequest {
            session_id: params.session_id,
            language: &language,
            sample_rate: params.sample_rate,
        },
        RESUME_WAIT,
    )
    .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::info!(error = ?e, "session open refused");
            let _ = send(&mut sink, &e.to_message()).await;
            return Ok(());
        }
    };
    let session_id = opened.session_id;
    let ready = ServerMessage::Ready {
        session_id,
        resumed: opened.resumed,
        offset_ms: opened.recording.offset_ms,
    };

    // --- task 4: recorder --------------------------------------------------
    let recorder = match Recorder::create(
        state.db.clone(),
        &state.cfg.data_dir,
        opened,
        out_tx.clone(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("init_failed", e.to_string()),
            )
            .await;
            return Ok(());
        }
    };
    let (rec_audio_tx, rec_audio_rx) = mpsc::channel::<Bytes>(state.cfg.audio_queue);
    let recorder_task = tokio::spawn(recorder.run(rec_audio_rx, events_rx));

    send(&mut sink, &ready).await?;
    tracing::info!(%session_id, sample_rate = params.sample_rate, %language, "stream started");

    // --- task 3: sender ----------------------------------------------------
    let sender = tokio::spawn(async move {
        let mut ping = tokio::time::interval_at(Instant::now() + PING_EVERY, PING_EVERY);
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    let Some(msg) = msg else {
                        // Every producer (pipeline, recorder, receiver) is
                        // done, so nothing more can be reported.
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    };
                    if send(&mut sink, &msg).await.is_err() {
                        break;
                    }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // --- task 2: pipeline --------------------------------------------------
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(state.cfg.audio_queue);
    let pipeline_task = tokio::spawn(pipeline.run(audio_rx));

    // --- task 1: receiver --------------------------------------------------
    let mut idle_deadline = Instant::now() + IDLE_TIMEOUT;
    loop {
        let msg = tokio::select! {
            msg = stream.next() => msg,
            () = tokio::time::sleep_until(idle_deadline) => {
                tracing::info!(%session_id, "peer idle; closing");
                break;
            }
            () = state.shutdown.cancelled() => {
                tracing::info!(%session_id, "server shutting down; ending stream");
                break;
            }
        };
        let Some(msg) = msg else { break };
        idle_deadline = Instant::now() + IDLE_TIMEOUT;
        match msg {
            Ok(Message::Binary(data)) => {
                // Await, don't try_send: backpressure the socket rather than
                // ever dropping input audio.
                if audio_tx.send(data.to_vec()).await.is_err() {
                    break;
                }
                // A failed recorder must not end transcription; it has
                // already logged why.
                let _ = rec_audio_tx.send(data).await;
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

    // Order matters: the pipeline's final flush queues its last commits to
    // the recorder and its `done` to the sender; the recorder then writes
    // them, may report a final `persist_failed` through its `notify` clone,
    // and releases the session. Only after that do the last `out_tx` clones
    // drop, so the sender drains everything and then closes the socket.
    drop(audio_tx);
    let _ = pipeline_task.await;
    drop(rec_audio_tx);
    let _ = recorder_task.await;
    drop(out_tx);
    let _ = sender.await;
    tracing::info!(%session_id, "stream finished");
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

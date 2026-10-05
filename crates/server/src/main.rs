mod audio;
mod config;
mod db;
mod export;
mod live;
mod pipeline;
mod protocol;
mod recorder;
mod sessions;
mod stabilizer;
mod vad;
mod whisper;
mod ws;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::routing::get;
use axum::Router;
use clap::Parser;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::whisper::WhisperEngine;
use crate::ws::AppState;

const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    // `.env` is optional; real environment variables take precedence.
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,transcribe_server=debug".into()),
        )
        .init();

    let cfg = Arc::new(Config::parse());

    // Fail fast on a bad database before the multi-second model load.
    let db = db::connect(&cfg.database_url, cfg.db_max_connections).await?;
    tokio::fs::create_dir_all(cfg.data_dir.join("sessions"))
        .await
        .with_context(|| format!("creating {}", cfg.data_dir.display()))?;

    // Blocking work: model load + one warm-up inference per worker. Done before
    // we accept any traffic so the first utterance isn't slow.
    let engine = {
        let cfg = Arc::clone(&cfg);
        tokio::task::spawn_blocking(move || WhisperEngine::spawn(&cfg))
            .await
            .context("engine init panicked")??
    };

    let state = AppState::new(Arc::clone(&cfg), Arc::clone(&engine), db.clone());

    let index = cfg.client_dir.join("index.html");
    let static_files = ServeDir::new(&cfg.client_dir).fallback(ServeFile::new(index));

    let app = Router::new()
        .route("/ws", get(ws::handler))
        .route("/healthz", get(|| async { "ok" }))
        .fallback_service(static_files)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .with_context(|| format!("failed to bind {}", cfg.bind))?;

    tracing::info!(
        addr = %cfg.bind,
        workers = engine.workers(),
        max_connections = cfg.max_connections,
        "listening — open http://{} in a browser",
        cfg.bind
    );

    axum::serve(listener, app)
        .with_graceful_shutdown({
            let token = state.shutdown.clone();
            async move {
                shutdown_signal().await;
                token.cancel();
            }
        })
        .await
        .context("server error")?;

    // Upgraded sockets outlive axum's graceful shutdown; give their pipelines
    // and recorders a bounded window to flush.
    state.tracker.close();
    if tokio::time::timeout(SHUTDOWN_GRACE, state.tracker.wait())
        .await
        .is_err()
    {
        tracing::warn!("shutdown grace elapsed with streams still open");
    }

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}

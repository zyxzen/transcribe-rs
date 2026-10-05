use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Near-realtime Whisper transcription server.
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Config {
    /// Path to a ggml/gguf Whisper model.
    ///
    /// TODO: point this at your model. Recommended for latency:
    ///   ggml-base.en-q5_1.bin  (fast, good enough)
    ///   ggml-small.en-q5_1.bin (slower, more accurate)
    #[arg(long, env = "TR_MODEL", default_value = "models/ggml-base.en-q5_1.bin")]
    pub model: PathBuf,

    #[arg(long, env = "TR_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    /// Directory served at `/` (the browser client).
    #[arg(long, env = "TR_CLIENT_DIR", default_value = "client")]
    pub client_dir: PathBuf,

    /// Postgres connection string for session storage.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Root for persisted audio: `{data_dir}/sessions/{session}/{recording}.pcm`.
    #[arg(long, env = "TR_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    /// Postgres pool size. Writes are batched per connection, so this stays small.
    #[arg(long, env = "TR_DB_MAX_CONNECTIONS", default_value_t = 8)]
    pub db_max_connections: u32,

    /// Number of blocking inference workers (concurrent decodes).
    ///
    /// A GPU serializes decodes, so 1-2 is right there. On CPU, use
    /// cores / threads_per_decode.
    #[arg(long, env = "TR_WORKERS", default_value_t = default_workers())]
    pub workers: usize,

    /// whisper.cpp threads per decode.
    #[arg(long, env = "TR_THREADS", default_value_t = default_threads())]
    pub threads: usize,

    /// Flash attention. Faster on Metal/CUDA; harmless on CPU.
    #[arg(long, env = "TR_FLASH_ATTN", default_value_t = true, action = clap::ArgAction::Set)]
    pub flash_attn: bool,

    /// Encode the padded 30s window every pass instead of sizing the encoder to
    /// the audio present. Slower; kept as an escape hatch if quality regresses.
    #[arg(long)]
    pub no_audio_ctx: bool,

    /// Smallest encoder context used when sizing to the buffer. Too low and
    /// Whisper starts mangling short windows.
    #[arg(long, default_value_t = 256)]
    pub audio_ctx_floor: u32,

    /// Hard cap on concurrent connections.
    ///
    /// TODO: tune. Measure RTF of one decode on your hardware, then set this to
    /// roughly `workers / RTF` with headroom. Beyond this we reject rather than
    /// degrade everyone.
    #[arg(long, env = "TR_MAX_CONNECTIONS", default_value_t = 4)]
    pub max_connections: usize,

    /// Run a decode after this much *new* audio has accumulated.
    ///
    /// This sets steady-state cost, not time-to-first-text: the first pass of an
    /// utterance fires on `min_decode_ms` instead of waiting a full interval, so
    /// this can stay comparatively long without hurting perceived latency.
    #[arg(long, default_value_t = 200)]
    pub decode_interval_ms: u64,

    /// Trailing silence that ends an utterance (immediate flush + commit).
    #[arg(long, default_value_t = 500)]
    pub end_silence_ms: u64,

    /// Audio kept before speech onset so word onsets aren't clipped.
    #[arg(long, default_value_t = 300)]
    pub preroll_ms: u64,

    /// Silero speech probability above which a 32ms frame counts as speech.
    #[arg(long, default_value_t = 0.5)]
    pub vad_threshold: f32,

    /// Trim the active buffer once this much of it has been committed.
    /// Keeps re-decodes cheap.
    #[arg(long, default_value_t = 6_000)]
    pub trim_after_ms: u64,

    /// Committed audio kept after a trim, as acoustic context for the tail.
    /// Below ~1s Whisper starts dropping words at the window edge.
    #[arg(long, default_value_t = 1_000)]
    pub trim_context_ms: u64,

    /// Force a flush if a single segment runs this long (Whisper's window is 30s).
    #[arg(long, default_value_t = 24_000)]
    pub max_segment_ms: u64,

    /// Don't bother decoding buffers shorter than this. Also the trigger for
    /// the first pass of an utterance, so it sets time-to-first-text directly.
    #[arg(long, default_value_t = 120)]
    pub min_decode_ms: u64,

    /// Bounded audio queue depth (chunks) between receiver and pipeline.
    /// This is backpressure, not a drop policy — audio is never discarded.
    #[arg(long, default_value_t = 128)]
    pub audio_queue: usize,
}

impl Config {
    pub fn samples_per_ms(&self) -> usize {
        crate::audio::TARGET_SAMPLE_RATE as usize / 1000
    }

    pub fn decode_interval_samples(&self) -> usize {
        self.decode_interval_ms as usize * self.samples_per_ms()
    }

    pub fn min_decode_samples(&self) -> usize {
        self.min_decode_ms as usize * self.samples_per_ms()
    }

    pub fn preroll_samples(&self) -> usize {
        self.preroll_ms as usize * self.samples_per_ms()
    }

    pub fn max_segment_samples(&self) -> usize {
        self.max_segment_ms as usize * self.samples_per_ms()
    }
}

fn cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn default_workers() -> usize {
    // On GPU backends the device serializes decodes anyway; extra workers only
    // add queueing. On CPU, leave room for the tokio runtime.
    if cfg!(any(feature = "metal", feature = "cuda", feature = "vulkan")) {
        1
    } else {
        (cores() / 4).max(1)
    }
}

fn default_threads() -> usize {
    if cfg!(any(feature = "metal", feature = "cuda", feature = "vulkan")) {
        2
    } else {
        (cores() / 2).clamp(1, 8)
    }
}

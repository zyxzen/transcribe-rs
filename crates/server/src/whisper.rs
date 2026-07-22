//! Blocking Whisper inference pool.
//!
//! CRITICAL: whisper inference is blocking compute. It never runs on a tokio
//! worker — it runs on dedicated OS threads owned by this module. Async callers
//! submit a job and await a oneshot.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;
use whisper_rs::{FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters};

use crate::config::Config;

/// One decoded word with absolute-from-stream-start timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl Word {
    /// Punctuation- and case-insensitive key used for hypothesis agreement.
    pub fn key(&self) -> String {
        self.text
            .chars()
            .filter(|c| c.is_alphanumeric())
            .flat_map(|c| c.to_lowercase())
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Hypothesis {
    pub words: Vec<Word>,
}

pub fn join_words(words: &[Word]) -> String {
    let mut s = String::new();
    for w in words {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str(&w.text);
    }
    s
}

struct Job {
    audio: Arc<Vec<f32>>,
    /// Absolute ms from stream start of `audio[0]`.
    base_ms: u64,
    language: String,
    reply: oneshot::Sender<Result<Hypothesis>>,
}

pub struct WhisperEngine {
    tx: Sender<Job>,
    workers: usize,
}

impl WhisperEngine {
    /// Loads the model once, spawns the worker threads and warms each of them
    /// with a dummy inference before returning.
    pub fn spawn(cfg: &Config) -> Result<Arc<Self>> {
        let path = cfg.model.as_path();
        anyhow::ensure!(
            path.exists(),
            "model not found at {} — see README for download instructions",
            path.display()
        );

        let ctx = Arc::new(load_context(path, cfg.flash_attn)?);
        tracing::info!(model = %path.display(), "whisper model loaded");

        let (tx, rx) = crossbeam_channel::unbounded::<Job>();
        let (warm_tx, warm_rx) = crossbeam_channel::bounded::<Result<()>>(cfg.workers);
        let tuning = Tuning::from_config(cfg);

        for id in 0..cfg.workers {
            let ctx = Arc::clone(&ctx);
            let rx = rx.clone();
            let warm_tx = warm_tx.clone();
            std::thread::Builder::new()
                .name(format!("whisper-{id}"))
                .spawn(move || worker_main(id, ctx, rx, warm_tx, tuning))
                .with_context(|| format!("failed to spawn whisper worker {id}"))?;
        }
        drop(warm_tx);

        for _ in 0..cfg.workers {
            warm_rx
                .recv()
                .context("whisper worker died during warmup")??;
        }
        tracing::info!(
            workers = cfg.workers,
            threads = cfg.threads,
            flash_attn = cfg.flash_attn,
            audio_ctx_floor = ?tuning.audio_ctx_floor,
            "inference pool warm"
        );

        Ok(Arc::new(Self {
            tx,
            workers: cfg.workers,
        }))
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Submit a decode and await the result. Called from async code; the work
    /// itself happens on a dedicated OS thread.
    pub async fn decode(
        &self,
        audio: Arc<Vec<f32>>,
        base_ms: u64,
        language: String,
    ) -> Result<Hypothesis> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job {
                audio,
                base_ms,
                language,
                reply,
            })
            .map_err(|_| anyhow!("inference pool has shut down"))?;
        rx.await
            .map_err(|_| anyhow!("inference worker dropped job"))?
    }
}

/// Decode-time knobs that every job shares.
#[derive(Debug, Clone, Copy)]
pub struct Tuning {
    pub threads: i32,
    /// Size the encoder to the audio actually present instead of the padded
    /// 30 s window. `None` uses whisper's default (full window).
    pub audio_ctx_floor: Option<i32>,
}

impl Tuning {
    fn from_config(cfg: &Config) -> Self {
        Self {
            threads: cfg.threads as i32,
            audio_ctx_floor: (!cfg.no_audio_ctx).then_some(cfg.audio_ctx_floor as i32),
        }
    }
}

fn load_context(path: &Path, flash_attn: bool) -> Result<WhisperContext> {
    let mut params = WhisperContextParameters::default();
    // `use_gpu` defaults to true and is honoured when a GPU feature is enabled.
    params.flash_attn(flash_attn);
    WhisperContext::new_with_params(
        path.to_str()
            .ok_or_else(|| anyhow!("model path is not valid UTF-8"))?,
        params,
    )
    .context("failed to load whisper model")
}

/// Whisper's encoder always runs over a 30 s window padded from whatever audio
/// it was given, and its cost scales with `audio_ctx`. Sizing that to the real
/// buffer length is the single largest latency win for short streaming windows.
///
/// 30 s = 3000 mel frames of 10 ms = 1500 encoder positions, so one position is
/// 20 ms of audio. A little headroom past the end avoids clipping the tail.
fn audio_ctx_for(samples: usize, floor: i32) -> i32 {
    const SAMPLES_PER_CTX: usize = 320; // 20 ms at 16 kHz
    const HEADROOM: i32 = 32;
    const MAX: i32 = 1500;
    ((samples / SAMPLES_PER_CTX) as i32 + HEADROOM).clamp(floor.min(MAX), MAX)
}

fn worker_main(
    id: usize,
    ctx: Arc<WhisperContext>,
    rx: Receiver<Job>,
    warm_tx: Sender<Result<()>>,
    tuning: Tuning,
) {
    // Each worker owns scratch state. With `no_context = true` there is no
    // cross-call carry-over, so states are interchangeable between streams.
    let mut state = match ctx.create_state().context("failed to create whisper state") {
        Ok(s) => s,
        Err(e) => {
            let _ = warm_tx.send(Err(e));
            return;
        }
    };

    // Warm up: one dummy inference so the first real utterance doesn't pay for
    // kernel compilation / lazy allocation.
    let warmup = vec![0.0f32; 16_000];
    let warm_result = run_decode(&ctx, &mut state, &warmup, 0, "en", tuning).map(|_| ());
    let failed = warm_result.is_err();
    let _ = warm_tx.send(warm_result);
    if failed {
        return;
    }

    while let Ok(job) = rx.recv() {
        if job.reply.is_closed() {
            // Connection went away while the job was queued — skip the compute.
            continue;
        }
        let started = Instant::now();
        let audio_ms = (job.audio.len() * 1000 / 16_000) as u64;
        let result = run_decode(
            &ctx,
            &mut state,
            &job.audio,
            job.base_ms,
            &job.language,
            tuning,
        );
        let elapsed = started.elapsed();
        if audio_ms > 0 {
            tracing::debug!(
                worker = id,
                audio_ms,
                took_ms = elapsed.as_millis(),
                rtf = elapsed.as_secs_f64() / (audio_ms as f64 / 1000.0),
                "decode"
            );
        }
        let _ = job.reply.send(result);
    }
}

fn run_decode(
    ctx: &WhisperContext,
    state: &mut whisper_rs::WhisperState,
    audio: &[f32],
    base_ms: u64,
    language: &str,
    tuning: Tuning,
) -> Result<Hypothesis> {
    let mut params = FullParams::new(SamplingStrategy::Greedy { best_of: 1 });

    // --- latency-first decoding -------------------------------------------
    params.set_n_threads(tuning.threads);
    params.set_language(Some(language));
    params.set_translate(false);
    // One segment per pass: we re-decode a short rolling window every tick.
    params.set_single_segment(true);
    // No carry-over between windows. Without this, streaming re-decodes feed
    // their own output back as prompt and spiral into hallucination.
    params.set_no_context(true);
    // Pin one temperature — temperature fallback re-runs the decoder and is the
    // single biggest latency spike in whisper.cpp.
    params.set_temperature(0.0);
    params.set_temperature_inc(0.0);
    params.set_suppress_blank(true);
    params.set_token_timestamps(true);
    // Speech tops out around 4-5 tokens/second. A budget well above that still
    // caps the greedy decoder's repetition loops, which otherwise emit hundreds
    // of tokens for a two-second window and blow the latency budget.
    let audio_secs = (audio.len() / 16_000) as i32;
    params.set_max_tokens((audio_secs * 8 + 16).clamp(16, 224));
    if let Some(floor) = tuning.audio_ctx_floor {
        params.set_audio_ctx(audio_ctx_for(audio.len(), floor));
    }
    params.set_print_special(false);
    params.set_print_progress(false);
    params.set_print_realtime(false);
    params.set_print_timestamps(false);

    state.full(params, audio).context("whisper decode failed")?;

    let eot = ctx.token_eot();
    let mut words: Vec<Word> = Vec::new();

    for i in 0..state.full_n_segments() {
        let Some(segment) = state.get_segment(i) else {
            continue;
        };
        let seg_start = base_ms + cs_to_ms(segment.start_timestamp());
        let seg_end = base_ms + cs_to_ms(segment.end_timestamp());

        for j in 0..segment.n_tokens() {
            let Some(token) = segment.get_token(j) else {
                continue;
            };
            // Special / timestamp tokens carry no text.
            if token.token_id() >= eot {
                continue;
            }
            let piece = token.to_str_lossy().unwrap_or_default().to_string();
            if piece.is_empty() {
                continue;
            }
            let data = token.token_data();
            let start = base_ms + cs_to_ms(data.t0);
            let end = base_ms + cs_to_ms(data.t1);

            let starts_word = piece.starts_with(' ') || words.is_empty();
            let trimmed = piece.trim();
            if trimmed.is_empty() {
                continue;
            }

            if starts_word {
                words.push(Word {
                    text: trimmed.to_string(),
                    start_ms: start.max(seg_start),
                    end_ms: end.max(start).min(seg_end.max(start)),
                });
            } else if let Some(last) = words.last_mut() {
                last.text.push_str(trimmed);
                last.end_ms = last.end_ms.max(end);
            }
        }
    }

    // Whisper likes to emit these on silence. Drop them rather than show them.
    words.retain(|w| !is_noise_token(&w.text));
    truncate_degenerate(&mut words);

    Ok(Hypothesis { words })
}

/// Longest plausible English word, with room for hyphenation.
const MAX_WORD_LEN: usize = 24;
/// Consecutive identical words beyond this are a decoder loop, not speech.
const MAX_WORD_REPEATS: usize = 3;

/// Cut a hypothesis at the first sign of a greedy repetition loop.
///
/// Two shapes show up in practice: the same word repeated ("the the the …") and
/// a single run-on token where the loop happened inside one word
/// ("real-time-time-time-…"). Both are hallucination, and everything after the
/// onset is worthless, so the hypothesis is truncated rather than repaired.
fn truncate_degenerate(words: &mut Vec<Word>) {
    let mut run = 1usize;
    for i in 0..words.len() {
        if words[i].text.chars().count() > MAX_WORD_LEN {
            words.truncate(i);
            return;
        }
        if i > 0 {
            let key = words[i].key();
            if !key.is_empty() && key == words[i - 1].key() {
                run += 1;
                if run > MAX_WORD_REPEATS {
                    words.truncate(i + 1 - run);
                    return;
                }
            } else {
                run = 1;
            }
        }
    }
}

/// whisper.cpp timestamps are centiseconds.
fn cs_to_ms(cs: i64) -> u64 {
    (cs.max(0) as u64) * 10
}

fn is_noise_token(text: &str) -> bool {
    let t = text.trim();
    (t.starts_with('[') && t.ends_with(']'))
        || (t.starts_with('(') && t.ends_with(')'))
        || (t.starts_with('*') && t.ends_with('*'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_key_ignores_case_and_punctuation() {
        let a = Word {
            text: "Hello,".into(),
            start_ms: 0,
            end_ms: 1,
        };
        let b = Word {
            text: "hello".into(),
            start_ms: 5,
            end_ms: 9,
        };
        assert_eq!(a.key(), b.key());
    }

    fn words(texts: &[&str]) -> Vec<Word> {
        texts
            .iter()
            .enumerate()
            .map(|(i, t)| Word {
                text: (*t).to_string(),
                start_ms: i as u64 * 100,
                end_ms: i as u64 * 100 + 100,
            })
            .collect()
    }

    #[test]
    fn truncates_repeated_word_loops() {
        let mut w = words(&["and", "then", "the", "the", "the", "the", "end"]);
        truncate_degenerate(&mut w);
        assert_eq!(
            w.iter().map(|x| x.text.as_str()).collect::<Vec<_>>(),
            ["and", "then"]
        );
    }

    #[test]
    fn truncates_run_on_hallucinated_token() {
        let mut w = words(&["real", &"time-".repeat(20), "streaming"]);
        truncate_degenerate(&mut w);
        assert_eq!(
            w.iter().map(|x| x.text.as_str()).collect::<Vec<_>>(),
            ["real"]
        );
    }

    #[test]
    fn leaves_ordinary_speech_alone() {
        let mut w = words(&["the", "quick", "brown", "fox", "the", "lazy", "dog"]);
        truncate_degenerate(&mut w);
        assert_eq!(w.len(), 7);
    }

    #[test]
    fn filters_bracketed_noise() {
        assert!(is_noise_token("[BLANK_AUDIO]"));
        assert!(is_noise_token("(music)"));
        assert!(!is_noise_token("hello"));
    }
}

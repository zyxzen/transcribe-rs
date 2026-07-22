//! Per-connection audio pipeline.
//!
//! resample-if-needed -> Silero VAD (32 ms frames) -> rolling active buffer
//! -> latest-wins decode scheduling -> LocalAgreement-2 -> transcript JSON.
//!
//! Scheduling policy: at most one decode in flight per connection. New audio
//! arriving while a decode runs never queues a second pass — it just marks the
//! buffer dirty, and the next pass picks up *everything* accumulated. Redundant
//! inference passes are dropped; input audio never is.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::audio::{self, Resampler, TARGET_SAMPLE_RATE};
use crate::config::Config;
use crate::protocol::{ServerMessage, StartParams};
use crate::stabilizer::{Outcome, Stabilizer};
use crate::vad::{Vad, VAD_CHUNK, VAD_CHUNK_MS};
use crate::whisper::{Hypothesis, WhisperEngine};

/// Result of one completed decode, routed back into the pipeline's select loop.
struct DecodeDone {
    hypothesis: Result<Hypothesis>,
    /// Length of the active buffer at submit time.
    submitted_len: usize,
    /// Whether this pass was the one that should finalize the segment.
    finalizing: bool,
}

pub struct Pipeline {
    cfg: Arc<Config>,
    engine: Arc<WhisperEngine>,
    language: String,
    out: mpsc::Sender<ServerMessage>,

    resampler: Option<Resampler>,
    odd_byte: Option<u8>,
    convert_scratch: Vec<f32>,
    resample_scratch: Vec<f32>,

    vad: Vad,
    /// Samples not yet aligned into a full VAD frame.
    vad_carry: Vec<f32>,
    /// Absolute ms of audio that has passed through the VAD framer.
    frames_ms: u64,

    /// Ring of recent non-speech audio, prepended on speech onset.
    preroll: VecDeque<f32>,
    /// Audio since the last commit-trim or segment reset.
    active: Vec<f32>,
    active_start_ms: u64,

    in_speech: bool,
    silence_run_ms: u64,

    samples_since_submit: usize,
    /// No decode has run yet for the current utterance.
    awaiting_first_pass: bool,
    finalize_requested: bool,
    /// Active-buffer length that a decode must cover to finalize the segment.
    finalize_len: usize,

    stab: Stabilizer,
    seq: u64,
    last_pending: String,
}

impl Pipeline {
    pub fn new(
        cfg: Arc<Config>,
        engine: Arc<WhisperEngine>,
        params: &StartParams,
        out: mpsc::Sender<ServerMessage>,
    ) -> Result<Self> {
        let resampler = Resampler::new(params.sample_rate, TARGET_SAMPLE_RATE)?;
        if resampler.is_some() {
            tracing::debug!(
                from = params.sample_rate,
                to = TARGET_SAMPLE_RATE,
                "resampling input"
            );
        }
        let vad = Vad::new(cfg.vad_threshold)?;
        let preroll_cap = cfg.preroll_samples();

        Ok(Self {
            language: params.language_or_default(),
            resampler,
            odd_byte: None,
            convert_scratch: Vec::with_capacity(8192),
            resample_scratch: Vec::with_capacity(8192),
            vad,
            vad_carry: Vec::with_capacity(VAD_CHUNK * 2),
            frames_ms: 0,
            preroll: VecDeque::with_capacity(preroll_cap + VAD_CHUNK),
            active: Vec::with_capacity(cfg.max_segment_samples()),
            active_start_ms: 0,
            in_speech: false,
            silence_run_ms: 0,
            samples_since_submit: 0,
            awaiting_first_pass: true,
            finalize_requested: false,
            finalize_len: 0,
            stab: Stabilizer::new(),
            seq: 0,
            last_pending: String::new(),
            cfg,
            engine,
            out,
        })
    }

    /// Drives the connection until the audio channel closes, then emits `done`.
    pub async fn run(mut self, mut audio_rx: mpsc::Receiver<Vec<u8>>) {
        // Decode results come back over their own channel so the select loop
        // owns two independent receivers (no borrow gymnastics on an in-flight
        // future).
        let (done_tx, mut done_rx) = mpsc::channel::<DecodeDone>(2);
        let mut in_flight = false;
        let mut dirty = false;

        loop {
            tokio::select! {
                biased;

                // Prefer draining results: committing text is what the user sees.
                Some(done) = done_rx.recv() => {
                    in_flight = false;
                    self.on_decode_done(done).await;
                    if self.should_submit(dirty) {
                        dirty = false;
                        in_flight = self.submit(&done_tx);
                    }
                }

                chunk = audio_rx.recv() => {
                    let Some(chunk) = chunk else { break };
                    if let Err(e) = self.ingest(&chunk) {
                        tracing::warn!(error = %e, "audio ingest failed");
                        let _ = self.out.send(ServerMessage::error("audio", e.to_string())).await;
                        break;
                    }
                    dirty = true;
                    if !in_flight && self.should_submit(true) {
                        dirty = false;
                        in_flight = self.submit(&done_tx);
                    }
                }
            }
        }

        self.finish(in_flight, done_rx, done_tx).await;
    }

    // ---- ingest -----------------------------------------------------------

    fn ingest(&mut self, bytes: &[u8]) -> Result<()> {
        self.convert_scratch.clear();

        // Re-attach a sample split across two WebSocket frames.
        let carry = self.odd_byte.take();
        let mut joined;
        let bytes = if let Some(b) = carry {
            joined = Vec::with_capacity(bytes.len() + 1);
            joined.push(b);
            joined.extend_from_slice(bytes);
            joined.as_slice()
        } else {
            bytes
        };
        self.odd_byte = audio::pcm_s16le_to_f32(bytes, &mut self.convert_scratch);

        let samples: &[f32] = if let Some(rs) = self.resampler.as_mut() {
            self.resample_scratch.clear();
            rs.push(&self.convert_scratch, &mut self.resample_scratch)?;
            &self.resample_scratch
        } else {
            &self.convert_scratch
        };

        // Frame the stream into fixed 32 ms VAD chunks.
        self.vad_carry.extend_from_slice(samples);
        let mut offset = 0;
        while self.vad_carry.len() - offset >= VAD_CHUNK {
            let frame_start = offset;
            offset += VAD_CHUNK;
            // Split the borrow: predict needs &mut self.vad, the frame is in
            // self.vad_carry.
            let speech = {
                let frame = &self.vad_carry[frame_start..offset];
                self.vad.is_speech(frame)
            };
            self.handle_frame(frame_start, offset, speech);
        }
        if offset > 0 {
            self.vad_carry.drain(..offset);
        }
        Ok(())
    }

    fn handle_frame(&mut self, from: usize, to: usize, speech: bool) {
        let frame_len = to - from;
        self.frames_ms += VAD_CHUNK_MS;

        if speech {
            if !self.in_speech {
                self.in_speech = true;
                if self.active.is_empty() {
                    // Roll the pre-speech audio in so word onsets aren't clipped.
                    let preroll_ms = (self.preroll.len() as u64 * 1000) / TARGET_SAMPLE_RATE as u64;
                    let frame_start_ms = self.frames_ms.saturating_sub(VAD_CHUNK_MS);
                    self.active_start_ms = frame_start_ms.saturating_sub(preroll_ms);
                    self.active.extend(self.preroll.drain(..));
                }
            }
            self.silence_run_ms = 0;
            self.preroll.clear();
            self.active.extend_from_slice(&self.vad_carry[from..to]);
            self.samples_since_submit += frame_len;
        } else if self.active.is_empty() {
            // Not speaking and nothing buffered: keep only the pre-roll tail.
            // VAD-gating means Whisper never sees this audio at all.
            self.preroll
                .extend(self.vad_carry[from..to].iter().copied());
            let cap = self.cfg.preroll_samples();
            while self.preroll.len() > cap {
                self.preroll.pop_front();
            }
        } else {
            // Trailing silence inside an utterance: keep it (it carries the word
            // tail) but count towards end-of-utterance.
            self.active.extend_from_slice(&self.vad_carry[from..to]);
            self.samples_since_submit += frame_len;
            self.silence_run_ms += VAD_CHUNK_MS;

            if self.in_speech && self.silence_run_ms >= self.cfg.end_silence_ms {
                self.in_speech = false;
                self.request_finalize();
            }
        }

        // Whisper's receptive field is 30 s. Force a flush well before that.
        if self.active.len() >= self.cfg.max_segment_samples() {
            self.request_finalize();
        }
    }

    fn request_finalize(&mut self) {
        if !self.finalize_requested {
            self.finalize_requested = true;
        }
        // Always extend to the newest audio so the final pass sees everything.
        self.finalize_len = self.active.len();
    }

    // ---- scheduling -------------------------------------------------------

    fn should_submit(&self, dirty: bool) -> bool {
        if self.active.len() < self.cfg.min_decode_samples() {
            // Too short to be worth a pass — unless we're finalizing and there
            // is genuinely something buffered.
            return self.finalize_requested && !self.active.is_empty();
        }
        // Finalize beats the interval: flush immediately on VAD silence.
        if self.finalize_requested {
            return true;
        }
        if !dirty {
            return false;
        }
        // The first pass of an utterance fires as soon as there is enough audio
        // to decode at all, rather than waiting a full interval. Time-to-first-
        // text is what the user perceives, and one extra decode per utterance
        // is cheap; later passes wait for the interval to bound sustained load.
        let threshold = if self.awaiting_first_pass {
            self.cfg.min_decode_samples()
        } else {
            self.cfg.decode_interval_samples()
        };
        self.samples_since_submit >= threshold
    }

    /// Returns true if a decode is now in flight.
    fn submit(&mut self, done_tx: &mpsc::Sender<DecodeDone>) -> bool {
        if self.active.is_empty() {
            return false;
        }
        let audio = Arc::new(self.active.clone());
        let submitted_len = audio.len();
        let finalizing = self.finalize_requested && submitted_len >= self.finalize_len;
        self.samples_since_submit = 0;
        self.awaiting_first_pass = false;

        let engine = Arc::clone(&self.engine);
        let base_ms = self.active_start_ms;
        let language = self.language.clone();
        let done_tx = done_tx.clone();

        tokio::spawn(async move {
            let hypothesis = engine.decode(audio, base_ms, language).await;
            let _ = done_tx
                .send(DecodeDone {
                    hypothesis,
                    submitted_len,
                    finalizing,
                })
                .await;
        });
        true
    }

    async fn on_decode_done(&mut self, done: DecodeDone) {
        let hyp = match done.hypothesis {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "decode failed");
                let _ = self
                    .out
                    .send(ServerMessage::error("decode_failed", e.to_string()))
                    .await;
                return;
            }
        };

        if done.finalizing {
            let outcome = self.stab.flush(Some(hyp));
            self.emit(outcome, true).await;
            self.reset_segment(done.submitted_len);
        } else {
            let outcome = self.stab.update(hyp);
            self.emit(outcome, false).await;
            self.maybe_trim();
        }
    }

    /// Drop audio that has already been committed, so re-decodes stay cheap.
    fn maybe_trim(&mut self) {
        // A finalize pass is about to run over this buffer. Trimming now would
        // hand it a fragment, and Whisper drops words on sub-second windows.
        if self.finalize_requested {
            return;
        }
        let Some(end_ms) = self.stab.committed_end_ms() else {
            return;
        };
        if end_ms <= self.active_start_ms {
            return;
        }
        let committed_ms = end_ms - self.active_start_ms;
        if committed_ms < self.cfg.trim_after_ms {
            return;
        }

        // Leave a margin of already-committed audio in the buffer. Whisper needs
        // acoustic context to decode the uncommitted tail well, and cutting flush
        // to the commit boundary loses whatever word straddles it.
        let new_start_ms = end_ms.saturating_sub(self.cfg.trim_context_ms);
        if new_start_ms <= self.active_start_ms {
            return;
        }
        let drop_samples = ((new_start_ms - self.active_start_ms) as usize
            * self.cfg.samples_per_ms())
        .min(self.active.len());
        if drop_samples == 0 {
            return;
        }
        self.active.drain(..drop_samples);
        self.active_start_ms = new_start_ms;
        self.stab.on_trim(new_start_ms);
        tracing::trace!(
            trimmed_ms = new_start_ms,
            remaining_ms = (self.active.len() / self.cfg.samples_per_ms()) as u64,
            "trimmed active buffer"
        );
    }

    /// Segment finished: clear the window, keeping any audio recorded after the
    /// final decode was submitted (the user may already be talking again).
    fn reset_segment(&mut self, consumed: usize) {
        let keep = self.active.len().saturating_sub(consumed);
        if keep > 0 {
            self.active.drain(..consumed);
            self.active_start_ms = self
                .frames_ms
                .saturating_sub((keep as u64 * 1000) / TARGET_SAMPLE_RATE as u64);
        } else {
            self.active.clear();
            self.active_start_ms = self.frames_ms;
        }
        self.finalize_requested = false;
        self.finalize_len = 0;
        self.silence_run_ms = 0;
        self.awaiting_first_pass = true;
        self.samples_since_submit = self.active.len();
        self.last_pending.clear();
    }

    // ---- emit -------------------------------------------------------------

    async fn emit(&mut self, outcome: Outcome, end_of_segment: bool) {
        let changed = outcome.commit.is_some() || outcome.pending != self.last_pending;
        if !changed && !end_of_segment {
            return;
        }
        if end_of_segment && outcome.commit.is_none() && self.last_pending.is_empty() {
            // Nothing was said in this segment — don't emit an empty paragraph
            // break.
            return;
        }

        self.seq += 1;
        self.last_pending = outcome.pending.clone();
        let msg = ServerMessage::Transcript {
            seq: self.seq,
            commit: outcome.commit,
            pending: outcome.pending,
            end_of_segment,
            language: self.language.clone(),
        };
        if self.out.send(msg).await.is_err() {
            tracing::debug!("transcript sink closed");
        }
    }

    // ---- shutdown ---------------------------------------------------------

    async fn finish(
        mut self,
        in_flight: bool,
        mut done_rx: mpsc::Receiver<DecodeDone>,
        done_tx: mpsc::Sender<DecodeDone>,
    ) {
        // Let an in-flight pass land so its text isn't lost.
        if in_flight {
            if let Some(done) = done_rx.recv().await {
                self.on_decode_done(done).await;
            }
        }

        // One last pass over whatever audio remains, then flush everything.
        if !self.active.is_empty() {
            self.request_finalize();
            if self.submit(&done_tx) {
                if let Some(done) = done_rx.recv().await {
                    self.on_decode_done(done).await;
                }
            }
        } else if self.stab.has_window_state() {
            let outcome = self.stab.flush(None);
            self.emit(outcome, true).await;
        }

        let _ = self
            .out
            .send(ServerMessage::Done {
                text: self.stab.full_text().to_string(),
            })
            .await;
    }
}

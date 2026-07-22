//! Silero VAD wrapper. Operates on fixed 32 ms frames at 16 kHz.

use anyhow::{Context, Result};
use voice_activity_detector::VoiceActivityDetector;

/// Silero v5 at 16 kHz wants 512-sample chunks (32 ms).
pub const VAD_CHUNK: usize = 512;
pub const VAD_CHUNK_MS: u64 = (VAD_CHUNK as u64 * 1000) / 16_000;

pub struct Vad {
    detector: VoiceActivityDetector,
    threshold: f32,
}

impl Vad {
    pub fn new(threshold: f32) -> Result<Self> {
        let detector = VoiceActivityDetector::builder()
            .sample_rate(16_000i64)
            .chunk_size(VAD_CHUNK)
            .build()
            .context("failed to build Silero VAD")?;
        Ok(Self {
            detector,
            threshold,
        })
    }

    /// `frame` must be exactly [`VAD_CHUNK`] samples.
    pub fn is_speech(&mut self, frame: &[f32]) -> bool {
        debug_assert_eq!(frame.len(), VAD_CHUNK);
        self.detector.predict(frame.iter().copied()) >= self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_is_not_speech() {
        let mut vad = Vad::new(0.5).expect("silero model bundled with crate");
        let frame = [0.0f32; VAD_CHUNK];
        assert!(!vad.is_speech(&frame));
    }
}

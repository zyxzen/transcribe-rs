//! Sample format conversion and (optional) resampling to Whisper's 16 kHz mono.

use anyhow::{Context, Result};
use rubato::{FastFixedIn, PolynomialDegree, Resampler as _};

pub const TARGET_SAMPLE_RATE: u32 = 16_000;

/// Decode little-endian interleaved-free (mono) Int16 PCM into f32 in [-1, 1].
///
/// A trailing odd byte is returned so the caller can prepend it to the next
/// frame — WebSocket frame boundaries do not have to align to sample boundaries.
pub fn pcm_s16le_to_f32(bytes: &[u8], out: &mut Vec<f32>) -> Option<u8> {
    let (pairs, tail) = bytes.split_at(bytes.len() & !1);
    out.reserve(pairs.len() / 2);
    for pair in pairs.chunks_exact(2) {
        let v = i16::from_le_bytes([pair[0], pair[1]]);
        out.push(v as f32 / 32768.0);
    }
    tail.first().copied()
}

/// Fixed-input-chunk polynomial resampler, fed from an accumulating buffer so
/// callers can push arbitrary-length slices.
pub struct Resampler {
    inner: FastFixedIn<f32>,
    pending: Vec<f32>,
    scratch_in: Vec<Vec<f32>>,
}

impl Resampler {
    /// `None` when no conversion is needed.
    pub fn new(from_rate: u32, to_rate: u32) -> Result<Option<Self>> {
        if from_rate == to_rate {
            return Ok(None);
        }
        anyhow::ensure!(
            (8_000..=192_000).contains(&from_rate),
            "unsupported input sample rate {from_rate}"
        );

        let chunk = 1024;
        let inner = FastFixedIn::<f32>::new(
            to_rate as f64 / from_rate as f64,
            1.0,
            PolynomialDegree::Septic,
            chunk,
            1,
        )
        .context("failed to construct resampler")?;

        Ok(Some(Self {
            inner,
            pending: Vec::with_capacity(chunk * 2),
            scratch_in: vec![Vec::with_capacity(chunk)],
        }))
    }

    /// Append `input` (at the source rate) and drain everything convertible into
    /// `out` (at the target rate).
    pub fn push(&mut self, input: &[f32], out: &mut Vec<f32>) -> Result<()> {
        self.pending.extend_from_slice(input);

        loop {
            let needed = self.inner.input_frames_next().max(1);
            if self.pending.len() < needed {
                break;
            }
            self.scratch_in[0].clear();
            self.scratch_in[0].extend_from_slice(&self.pending[..needed]);
            self.pending.drain(..needed);

            let resampled = self
                .inner
                .process(&self.scratch_in, None)
                .context("resample failed")?;
            out.extend_from_slice(&resampled[0]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_pcm_and_carries_odd_byte() {
        let mut out = Vec::new();
        let tail = pcm_s16le_to_f32(&[0x00, 0x40, 0x11], &mut out);
        assert_eq!(out.len(), 1);
        assert!((out[0] - 0.5).abs() < 1e-6);
        assert_eq!(tail, Some(0x11));
    }

    #[test]
    fn identity_rate_needs_no_resampler() {
        assert!(Resampler::new(16_000, 16_000).unwrap().is_none());
    }

    #[test]
    fn downsamples_48k_to_16k_roughly_one_third() {
        let mut r = Resampler::new(48_000, 16_000).unwrap().unwrap();
        let input = vec![0.0f32; 48_000];
        let mut out = Vec::new();
        r.push(&input, &mut out).unwrap();
        // Allow for the resampler's internal latency / partial trailing chunk.
        assert!(
            out.len() > 15_000 && out.len() <= 16_100,
            "got {}",
            out.len()
        );
    }
}

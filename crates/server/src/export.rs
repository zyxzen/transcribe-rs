//! Session export formats. Pure functions over persisted chunks, no I/O, so
//! every format is unit tested without a database.

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::db::Chunk;

const CUE_MAX_MS: i64 = 7_000;
const CUE_MAX_CHARS: usize = 84;
const LINE_MAX_CHARS: usize = 42;
const FILENAME_MAX_CHARS: usize = 60;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChunkView {
    pub text: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Paragraph {
    pub index: i32,
    pub start_ms: i64,
    pub end_ms: i64,
    /// Chunks joined with single spaces, exactly as the live client shows them.
    pub text: String,
    pub chunks: Vec<ChunkView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cue {
    pub start_ms: i64,
    pub end_ms: i64,
    pub text: String,
}

/// Groups chunks (ordered by id) into paragraphs.
pub fn paragraphs(chunks: &[Chunk]) -> Vec<Paragraph> {
    let mut out: Vec<Paragraph> = Vec::new();
    for c in chunks {
        let view = ChunkView {
            text: c.text.clone(),
            start_ms: c.start_ms,
            end_ms: c.end_ms,
        };
        match out.last_mut() {
            Some(p) if p.index == c.paragraph => {
                p.text.push(' ');
                p.text.push_str(&c.text);
                p.end_ms = p.end_ms.max(c.end_ms);
                p.chunks.push(view);
            }
            _ => out.push(Paragraph {
                index: c.paragraph,
                start_ms: c.start_ms,
                end_ms: c.end_ms,
                text: c.text.clone(),
                chunks: vec![view],
            }),
        }
    }
    out
}

/// Subtitle cues: a paragraph can run 24 s, far too long for one cue, so
/// chunks are packed into cues of at most 7 s / 84 characters.
pub fn build_cues(paragraphs: &[Paragraph]) -> Vec<Cue> {
    let mut cues: Vec<Cue> = Vec::new();
    for p in paragraphs {
        let mut current: Option<Cue> = None;
        for c in &p.chunks {
            match current.as_mut() {
                Some(cue)
                    if c.end_ms - cue.start_ms <= CUE_MAX_MS
                        && cue.text.chars().count() + 1 + c.text.chars().count()
                            <= CUE_MAX_CHARS =>
                {
                    cue.text.push(' ');
                    cue.text.push_str(&c.text);
                    cue.end_ms = cue.end_ms.max(c.end_ms);
                }
                _ => {
                    cues.extend(current.take());
                    current = Some(Cue {
                        start_ms: c.start_ms,
                        end_ms: c.end_ms,
                        text: c.text.clone(),
                    });
                }
            }
        }
        cues.extend(current);
    }
    for i in 0..cues.len() {
        if let Some(next_start) = cues.get(i + 1).map(|c| c.start_ms) {
            if cues[i].end_ms > next_start {
                cues[i].end_ms = next_start;
            }
        }
        if cues[i].end_ms <= cues[i].start_ms {
            cues[i].end_ms = cues[i].start_ms + 1;
        }
    }
    cues
}

/// Greedy word wrap to 42-character lines, at most two; overflow stays on
/// the second line rather than being dropped.
pub fn wrap(text: &str) -> String {
    let mut lines: Vec<String> = Vec::new();
    for word in text.split_whitespace() {
        match lines.last_mut() {
            Some(line) if line.chars().count() + 1 + word.chars().count() <= LINE_MAX_CHARS => {
                line.push(' ');
                line.push_str(word);
            }
            _ => lines.push(word.to_string()),
        }
    }
    if lines.len() > 2 {
        let rest = lines.split_off(1).join(" ");
        lines.push(rest);
    }
    lines.join("\n")
}

pub fn to_txt(paragraphs: &[Paragraph]) -> String {
    if paragraphs.is_empty() {
        return String::new();
    }
    let mut out = paragraphs
        .iter()
        .map(|p| p.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    out.push('\n');
    out
}

pub fn to_srt(cues: &[Cue]) -> String {
    let mut out = String::new();
    for (i, c) in cues.iter().enumerate() {
        out.push_str(&format!(
            "{}\n{} --> {}\n{}\n\n",
            i + 1,
            timestamp(c.start_ms, ','),
            timestamp(c.end_ms, ','),
            wrap(&c.text)
        ));
    }
    out
}

pub fn to_vtt(cues: &[Cue]) -> String {
    let mut out = String::from("WEBVTT\n\n");
    for c in cues {
        out.push_str(&format!(
            "{} --> {}\n{}\n\n",
            timestamp(c.start_ms, '.'),
            timestamp(c.end_ms, '.'),
            wrap(&c.text)
        ));
    }
    out
}

fn timestamp(ms: i64, sep: char) -> String {
    let ms = ms.max(0);
    let (h, rem) = (ms / 3_600_000, ms % 3_600_000);
    let (m, rem) = (rem / 60_000, rem % 60_000);
    let (s, milli) = (rem / 1_000, rem % 1_000);
    format!("{h:02}:{m:02}:{s:02}{sep}{milli:03}")
}

/// Canonical 44-byte header for mono 16-bit PCM.
pub fn wav_header(sample_rate: u32, data_len: u32) -> [u8; 44] {
    let mut h = [0u8; 44];
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&36u32.saturating_add(data_len).to_le_bytes());
    h[8..12].copy_from_slice(b"WAVE");
    h[12..16].copy_from_slice(b"fmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes()); // PCM
    h[22..24].copy_from_slice(&1u16.to_le_bytes()); // mono
    h[24..28].copy_from_slice(&sample_rate.to_le_bytes());
    h[28..32].copy_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
    h[32..34].copy_from_slice(&2u16.to_le_bytes()); // block align
    h[34..36].copy_from_slice(&16u16.to_le_bytes()); // bits per sample
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&data_len.to_le_bytes());
    h
}

/// ASCII-only slug of the title, so it is always safe inside a quoted
/// `Content-Disposition` filename; falls back to the creation time.
pub fn filename(title: Option<&str>, created_at: DateTime<Utc>, ext: &str) -> String {
    let mut slug = String::new();
    for ch in title.unwrap_or("").chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.truncate(FILENAME_MAX_CHARS);
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        format!("session-{}.{ext}", created_at.format("%Y-%m-%d-%H%M"))
    } else {
        format!("{slug}.{ext}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn chunk(paragraph: i32, text: &str, start_ms: i64, end_ms: i64) -> Chunk {
        Chunk {
            paragraph,
            text: text.into(),
            start_ms,
            end_ms,
        }
    }

    #[test]
    fn paragraphs_group_consecutive_chunks() {
        let ps = paragraphs(&[
            chunk(0, "hello there", 0, 900),
            chunk(0, "general", 900, 1_500),
            chunk(1, "kenobi", 2_000, 2_600),
        ]);
        assert_eq!(ps.len(), 2);
        assert_eq!((ps[0].index, ps[0].start_ms, ps[0].end_ms), (0, 0, 1_500));
        assert_eq!(ps[0].text, "hello there general");
        assert_eq!(ps[0].chunks.len(), 2);
        assert_eq!(ps[1].text, "kenobi");
    }

    #[test]
    fn cues_split_on_duration() {
        let ps = paragraphs(&[
            chunk(0, "a", 0, 3_000),
            chunk(0, "b", 3_000, 6_000),
            chunk(0, "c", 6_000, 7_500),
        ]);
        let cues = build_cues(&ps);
        assert_eq!(
            cues,
            vec![
                Cue {
                    start_ms: 0,
                    end_ms: 6_000,
                    text: "a b".into()
                },
                Cue {
                    start_ms: 6_000,
                    end_ms: 7_500,
                    text: "c".into()
                },
            ]
        );
    }

    #[test]
    fn cues_split_on_length() {
        let word = "a".repeat(40);
        let ps = paragraphs(&[
            chunk(0, &word, 0, 10),
            chunk(0, &word, 10, 20),
            chunk(0, &word, 20, 30),
        ]);
        let cues = build_cues(&ps);
        assert_eq!(cues.len(), 2);
        assert_eq!(cues[0].text.len(), 81);
    }

    #[test]
    fn cues_never_cross_paragraphs_and_never_overlap() {
        let ps = paragraphs(&[chunk(0, "one", 0, 2_500), chunk(1, "two", 2_000, 3_000)]);
        let cues = build_cues(&ps);
        assert_eq!(cues.len(), 2);
        assert_eq!(cues[0].end_ms, 2_000, "clamped to the next cue's start");
    }

    #[test]
    fn zero_length_cue_gets_one_millisecond() {
        let cues = build_cues(&paragraphs(&[chunk(0, "x", 500, 500)]));
        assert_eq!((cues[0].start_ms, cues[0].end_ms), (500, 501));
    }

    #[test]
    fn wrap_limits_lines() {
        let a = "a".repeat(40);
        let b = "b".repeat(40);
        assert_eq!(wrap(&format!("{a} {b}")), format!("{a}\n{b}"));
        let x = "x".repeat(30);
        let y = "y".repeat(30);
        let z = "z".repeat(30);
        assert_eq!(wrap(&format!("{x} {y} {z}")), format!("{x}\n{y} {z}"));
        assert_eq!(wrap("short line"), "short line");
    }

    #[test]
    fn srt_and_vtt_formatting() {
        let cues = vec![
            Cue {
                start_ms: 0,
                end_ms: 1_500,
                text: "hello there general".into(),
            },
            Cue {
                start_ms: 3_723_045,
                end_ms: 3_724_000,
                text: "kenobi".into(),
            },
        ];
        assert_eq!(
            to_srt(&cues),
            "1\n00:00:00,000 --> 00:00:01,500\nhello there general\n\n\
             2\n01:02:03,045 --> 01:02:04,000\nkenobi\n\n"
        );
        assert_eq!(
            to_vtt(&cues),
            "WEBVTT\n\n00:00:00.000 --> 00:00:01.500\nhello there general\n\n\
             01:02:03.045 --> 01:02:04.000\nkenobi\n\n"
        );
    }

    #[test]
    fn txt_separates_paragraphs_with_blank_lines() {
        let ps = paragraphs(&[
            chunk(0, "a", 0, 1),
            chunk(0, "b", 1, 2),
            chunk(1, "c", 3, 4),
        ]);
        assert_eq!(to_txt(&ps), "a b\n\nc\n");
    }

    #[test]
    fn empty_session_exports_are_valid() {
        let ps = paragraphs(&[]);
        assert_eq!(to_txt(&ps), "");
        assert_eq!(to_srt(&build_cues(&ps)), "");
        assert_eq!(to_vtt(&build_cues(&ps)), "WEBVTT\n\n");
        assert_eq!(&wav_header(16_000, 0)[40..44], &0u32.to_le_bytes());
    }

    #[test]
    fn wav_header_fields() {
        let h = wav_header(16_000, 32_000);
        assert_eq!(&h[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(h[4..8].try_into().unwrap()), 36 + 32_000);
        assert_eq!(&h[8..16], b"WAVEfmt ");
        assert_eq!(u16::from_le_bytes(h[20..22].try_into().unwrap()), 1, "PCM");
        assert_eq!(u16::from_le_bytes(h[22..24].try_into().unwrap()), 1, "mono");
        assert_eq!(u32::from_le_bytes(h[24..28].try_into().unwrap()), 16_000);
        assert_eq!(
            u32::from_le_bytes(h[28..32].try_into().unwrap()),
            32_000,
            "byte rate"
        );
        assert_eq!(
            u16::from_le_bytes(h[34..36].try_into().unwrap()),
            16,
            "bits"
        );
        assert_eq!(&h[36..40], b"data");
        assert_eq!(u32::from_le_bytes(h[40..44].try_into().unwrap()), 32_000);
    }

    #[test]
    fn filename_is_ascii_safe() {
        let at = Utc.with_ymd_and_hms(2026, 10, 3, 14, 32, 0).unwrap();
        assert_eq!(
            filename(Some("Team Standup: Q4!"), at, "srt"),
            "team-standup-q4.srt"
        );
        assert_eq!(filename(Some(r#"say "hi""#), at, "txt"), "say-hi.txt");
        assert_eq!(
            filename(Some("会議"), at, "wav"),
            "session-2026-10-03-1432.wav"
        );
        assert_eq!(filename(None, at, "json"), "session-2026-10-03-1432.json");
        let long = filename(Some(&"word ".repeat(30)), at, "txt");
        assert!(long.len() <= 60 + ".txt".len());
        assert!(!long.contains("-.txt"));
    }
}

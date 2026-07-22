//! LocalAgreement-2 stabilizer.
//!
//! Two consecutive hypotheses over the same rolling window are compared; the
//! longest common prefix beyond what is already committed is promoted to
//! committed text. Everything after it is the unstable `pending` tail.
//!
//! Commit and pending always come from the SAME decode tick, so the client can
//! append the delta and replace the tail without any risk of duplication at the
//! boundary.

use crate::protocol::Commit;
use crate::whisper::{join_words, Hypothesis, Word};

/// Token timestamps are approximate; require a word to end clearly before the
/// committed boundary before treating it as already-committed.
const TIMESTAMP_SLACK_MS: u64 = 60;

/// Longest committed-text suffix used to re-anchor a hypothesis. Longer anchors
/// are tried first — a multi-word match is essentially unambiguous, while a
/// single common word ("the") could match in several places.
const ANCHOR_MAX_WORDS: usize = 6;

#[derive(Debug, Default)]
pub struct Outcome {
    pub commit: Option<Commit>,
    pub pending: String,
}

#[derive(Debug, Default)]
pub struct Stabilizer {
    /// Words committed within the *current* audio window.
    committed: Vec<Word>,
    /// The previous hypothesis over the current window.
    prev: Vec<Word>,
    /// Everything committed since the stream began.
    full_text: String,
}

impl Stabilizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a fresh hypothesis. Returns the newly committed delta (if any) and
    /// the current unstable tail.
    pub fn update(&mut self, hyp: Hypothesis) -> Outcome {
        let words = hyp.words;
        let base = self.skip_committed(&words);
        let prev_base = self.skip_committed(&self.prev);

        let new_tail = &words[base..];
        let prev_tail = &self.prev[prev_base..];
        let agreed = common_prefix(new_tail, prev_tail);

        tracing::trace!(
            base,
            prev_base,
            agreed,
            hyp = ?words.iter().map(|w| (&w.text, w.start_ms, w.end_ms)).collect::<Vec<_>>(),
            committed = ?self.committed.iter().map(|w| (&w.text, w.end_ms)).collect::<Vec<_>>(),
            "stabilize"
        );

        let newly: Vec<Word> = new_tail[..agreed].to_vec();
        let pending = join_words(&new_tail[agreed..]);
        let commit = self.push_committed(newly);

        self.prev = words;
        Outcome { commit, pending }
    }

    /// How many leading words of `hyp` are already committed.
    ///
    /// Two things rule out simpler answers. Word counts drift, because each pass
    /// re-decodes the window from scratch and may re-tokenize it ("the laser" ->
    /// "the lazy dog"), so an index into the committed list swallows real words.
    /// And the hypothesis does not necessarily *start* at committed text: after
    /// a trim the window opens with retained context, so anchoring at `hyp[0]`
    /// fails and every word gets committed a second time.
    ///
    /// So: search for the longest suffix of the committed text inside the
    /// hypothesis and resume just past it. Timestamps are only a fallback —
    /// they shift by more than a word's width between passes.
    fn skip_committed(&self, hyp: &[Word]) -> usize {
        if self.committed.is_empty() || hyp.is_empty() {
            return 0;
        }
        let hyp_keys: Vec<String> = hyp.iter().map(Word::key).collect();
        let committed_keys: Vec<String> = self.committed.iter().map(Word::key).collect();

        let longest = ANCHOR_MAX_WORDS
            .min(committed_keys.len())
            .min(hyp_keys.len());
        for n in (1..=longest).rev() {
            let anchor = &committed_keys[committed_keys.len() - n..];
            if anchor.iter().any(String::is_empty) {
                continue;
            }
            if let Some(pos) = find_last(&hyp_keys, anchor) {
                return pos + n;
            }
        }

        let end = self.committed_end_ms().unwrap_or(0);
        hyp.iter()
            .take_while(|w| w.end_ms + TIMESTAMP_SLACK_MS <= end)
            .count()
    }

    /// End of utterance: everything in the latest hypothesis becomes committed
    /// immediately, without waiting for a second agreeing pass.
    ///
    /// `hyp` is the final decode of the segment; pass `None` to flush whatever
    /// the last hypothesis was.
    pub fn flush(&mut self, hyp: Option<Hypothesis>) -> Outcome {
        let words = match hyp {
            Some(h) => h.words,
            None => std::mem::take(&mut self.prev),
        };
        let base = self.skip_committed(&words);
        tracing::trace!(
            base,
            hyp = ?words.iter().map(|w| (&w.text, w.start_ms, w.end_ms)).collect::<Vec<_>>(),
            committed = ?self.committed.iter().map(|w| (&w.text, w.end_ms)).collect::<Vec<_>>(),
            "flush"
        );
        let newly: Vec<Word> = words[base..].to_vec();
        let commit = self.push_committed(newly);

        self.committed.clear();
        self.prev.clear();
        Outcome {
            commit,
            pending: String::new(),
        }
    }

    fn push_committed(&mut self, newly: Vec<Word>) -> Option<Commit> {
        if newly.is_empty() {
            return None;
        }
        let start = newly.first().unwrap().start_ms;
        let end = newly.last().unwrap().end_ms.max(start);
        let text = join_words(&newly);

        if !self.full_text.is_empty() {
            self.full_text.push(' ');
        }
        self.full_text.push_str(&text);
        self.committed.extend(newly);

        Some(Commit { text, start, end })
    }

    /// Absolute ms at which the committed text within the current window ends.
    pub fn committed_end_ms(&self) -> Option<u64> {
        self.committed.last().map(|w| w.end_ms)
    }

    pub fn has_window_state(&self) -> bool {
        !self.committed.is_empty() || !self.prev.is_empty()
    }

    /// The audio window was trimmed to start at `new_start_ms`.
    ///
    /// Committed words still inside the retained window are kept: that audio
    /// gets re-decoded as context, and keeping them is what stops those words
    /// from being committed a second time. Everything older is gone for good.
    /// The previous hypothesis is dropped — it described a different window.
    pub fn on_trim(&mut self, new_start_ms: u64) {
        self.committed.retain(|w| w.start_ms >= new_start_ms);
        self.prev.clear();
    }

    pub fn full_text(&self) -> &str {
        &self.full_text
    }
}

/// Index of the last occurrence of `needle` within `hay`.
fn find_last(hay: &[String], needle: &[String]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    (0..=hay.len() - needle.len())
        .rev()
        .find(|&i| hay[i..i + needle.len()] == *needle)
}

fn common_prefix(a: &[Word], b: &[Word]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(x, y)| x.key() == y.key() && !x.key().is_empty())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hyp(words: &[(&str, u64, u64)]) -> Hypothesis {
        Hypothesis {
            words: words
                .iter()
                .map(|(t, s, e)| Word {
                    text: (*t).to_string(),
                    start_ms: *s,
                    end_ms: *e,
                })
                .collect(),
        }
    }

    #[test]
    fn first_hypothesis_commits_nothing() {
        let mut s = Stabilizer::new();
        let out = s.update(hyp(&[("hello", 0, 300), ("world", 300, 600)]));
        assert!(out.commit.is_none());
        assert_eq!(out.pending, "hello world");
    }

    #[test]
    fn agreement_commits_the_common_prefix() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("hello", 0, 300), ("word", 300, 600)]));
        let out = s.update(hyp(&[("hello", 0, 300), ("world", 300, 650)]));

        let commit = out.commit.expect("prefix should commit");
        assert_eq!(commit.text, "hello");
        assert_eq!(commit.start, 0);
        assert_eq!(commit.end, 300);
        assert_eq!(out.pending, "world");
        assert_eq!(s.full_text(), "hello");
    }

    #[test]
    fn committed_text_is_not_re_emitted() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("a", 0, 100), ("b", 100, 200)]));
        s.update(hyp(&[("a", 0, 100), ("b", 100, 200)]));
        let out = s.update(hyp(&[("a", 0, 100), ("b", 100, 200), ("c", 200, 300)]));
        assert!(out.commit.is_none(), "a and b were already committed");
        assert_eq!(out.pending, "c");
    }

    #[test]
    fn punctuation_differences_still_agree() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("Hello", 0, 300), ("there", 300, 600)]));
        let out = s.update(hyp(&[("hello,", 0, 300), ("there", 300, 610)]));
        assert_eq!(out.commit.map(|c| c.text), Some("hello, there".to_string()));
        assert_eq!(out.pending, "");
    }

    #[test]
    fn flush_commits_the_remainder_atomically() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("one", 0, 100), ("two", 100, 200)]));
        s.update(hyp(&[("one", 0, 100), ("two", 100, 200)]));
        let out = s.flush(Some(hyp(&[
            ("one", 0, 100),
            ("two", 100, 200),
            ("three", 200, 400),
        ])));
        assert_eq!(out.commit.map(|c| c.text), Some("three".to_string()));
        assert_eq!(out.pending, "");
        assert_eq!(s.full_text(), "one two three");
        assert!(!s.has_window_state());
    }

    /// Regression: the decoder re-tokenized "the laser" as "the lazy dog" on the
    /// final pass. Indexing committed words by count skipped "lazy" entirely.
    #[test]
    fn retokenized_tail_does_not_lose_words() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[
            ("jumps", 0, 200),
            ("over", 200, 400),
            ("the", 400, 500),
        ]));
        s.update(hyp(&[
            ("jumps", 0, 200),
            ("over", 200, 400),
            ("the", 400, 500),
            ("laser.", 500, 900),
        ]));
        assert_eq!(s.full_text(), "jumps over the");

        let out = s.flush(Some(hyp(&[
            ("jumps", 0, 200),
            ("over", 200, 400),
            ("the", 400, 500),
            ("lazy", 500, 700),
            ("dog.", 700, 900),
        ])));
        assert_eq!(out.commit.map(|c| c.text), Some("lazy dog.".to_string()));
        assert_eq!(s.full_text(), "jumps over the lazy dog.");
    }

    /// A word that vanishes from a later hypothesis must not cause the words
    /// after it to be re-committed.
    #[test]
    fn dropped_committed_word_does_not_duplicate_the_tail() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("alpha", 0, 200), ("bravo", 200, 400)]));
        s.update(hyp(&[("alpha", 0, 200), ("bravo", 200, 400)]));
        assert_eq!(s.full_text(), "alpha bravo");

        // "alpha" disappears; the timestamp backstop still rules out "bravo".
        let out = s.update(hyp(&[("bravo", 200, 400), ("charlie", 400, 600)]));
        assert!(out.commit.is_none());
        assert_eq!(out.pending, "charlie");
    }

    /// After a trim the retained context is re-decoded. Those words are already
    /// committed and must not be emitted twice.
    #[test]
    fn retained_context_after_trim_is_not_recommitted() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("alpha", 0, 500), ("bravo", 500, 1000)]));
        s.update(hyp(&[("alpha", 0, 500), ("bravo", 500, 1000)]));
        assert_eq!(s.full_text(), "alpha bravo");

        // Window trimmed to 500 ms: "alpha" is gone, "bravo" is retained context.
        s.on_trim(500);

        let out = s.update(hyp(&[("bravo", 500, 1000), ("charlie", 1000, 1400)]));
        assert!(out.commit.is_none(), "bravo was already committed");
        assert_eq!(out.pending, "charlie");

        let out = s.update(hyp(&[("bravo", 500, 1000), ("charlie", 1000, 1400)]));
        assert_eq!(out.commit.map(|c| c.text), Some("charlie".to_string()));
        assert_eq!(s.full_text(), "alpha bravo charlie");
    }

    /// Regression: after a trim the window opens with retained context, so the
    /// hypothesis no longer starts at committed text. Anchoring at `hyp[0]`
    /// re-committed every word ("waiting waiting waiting").
    #[test]
    fn hypothesis_not_starting_at_committed_text_is_realigned() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("ranch", 0, 300), ("waiting", 300, 700)]));
        s.update(hyp(&[("ranch", 0, 300), ("waiting", 300, 700)]));
        assert_eq!(s.full_text(), "ranch waiting");

        // Trim drops "ranch"; the next window still opens with un-committed
        // context ("everybody") before the committed tail.
        s.on_trim(300);
        let out = s.update(hyp(&[
            ("everybody", 150, 300),
            ("waiting", 300, 720),
            ("patiently", 720, 1100),
        ]));
        assert!(out.commit.is_none(), "waiting was already committed");
        assert_eq!(out.pending, "patiently");
    }

    #[test]
    fn shrinking_hypothesis_does_not_panic_or_recommit() {
        let mut s = Stabilizer::new();
        s.update(hyp(&[("a", 0, 100), ("b", 100, 200)]));
        s.update(hyp(&[("a", 0, 100), ("b", 100, 200)]));
        let out = s.update(hyp(&[("a", 0, 100)]));
        assert!(out.commit.is_none());
        assert_eq!(out.pending, "");
    }
}

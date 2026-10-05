//! Per-connection persistence. Runs beside the pipeline so neither Postgres
//! nor the disk ever sits on the decode path: audio frames are appended to the
//! recording's PCM file, commits are batched into `transcript_chunks`.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use sqlx::PgPool;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

use crate::db::{self, Chunk};
use crate::live::LiveGuard;
use crate::protocol::ServerMessage;
use crate::sessions::{self, OpenedSession};

const FLUSH_AFTER: Duration = Duration::from_millis(500);
const FLUSH_ROWS: usize = 32;
const RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(400),
    Duration::from_millis(1_600),
];
/// While Postgres is failing, try again this often (one attempt each).
const RETRY_FAILING_EVERY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderEvent {
    /// A wire commit; timestamps are stream-relative.
    Commit {
        text: String,
        start_ms: u64,
        end_ms: u64,
    },
    EndOfSegment,
}

pub struct Recorder {
    pool: PgPool,
    session_id: Uuid,
    recording_id: Uuid,
    offset_ms: i64,
    sample_rate: u32,
    paragraph: i32,
    paragraph_has_text: bool,
    file: Option<File>,
    rows: Vec<Chunk>,
    failing: bool,
    reported: bool,
    notify: mpsc::Sender<ServerMessage>,
    /// Dropped last, when `close` returns: releasing it lets a waiting
    /// reconnect resume this session.
    _guard: LiveGuard,
}

impl Recorder {
    pub async fn create(
        pool: PgPool,
        data_dir: &Path,
        opened: OpenedSession,
        notify: mpsc::Sender<ServerMessage>,
    ) -> anyhow::Result<Self> {
        let path = sessions::recording_path(data_dir, opened.session_id, opened.recording.id);
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            pool,
            session_id: opened.session_id,
            recording_id: opened.recording.id,
            offset_ms: opened.recording.offset_ms,
            sample_rate: opened.recording.sample_rate as u32,
            paragraph: opened.next_paragraph,
            paragraph_has_text: false,
            file: Some(file),
            rows: Vec::new(),
            failing: false,
            reported: false,
            notify,
            _guard: opened.guard,
        })
    }

    /// Runs until both inputs close, then flushes and finishes the recording.
    pub async fn run(
        mut self,
        audio: mpsc::Receiver<Bytes>,
        mut events: mpsc::UnboundedReceiver<RecorderEvent>,
    ) {
        // Audio is drained by its own task so a slow or failing Postgres can
        // never stall capture (and, through backpressure, transcription).
        let file = self
            .file
            .take()
            .expect("recording file is opened by create");
        let writer = tokio::spawn(write_audio(BufWriter::new(file), audio));

        let mut flush_at: Option<Instant> = None;
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Some(RecorderEvent::Commit { text, start_ms, end_ms }) => {
                        self.rows.push(Chunk {
                            paragraph: self.paragraph,
                            text,
                            start_ms: self.offset_ms + start_ms as i64,
                            end_ms: self.offset_ms + end_ms as i64,
                        });
                        self.paragraph_has_text = true;
                        // While failing, only the retry timer touches Postgres.
                        if self.rows.len() >= FLUSH_ROWS && !self.failing {
                            flush_at = self.flush().await;
                        } else if flush_at.is_none() {
                            flush_at = Some(Instant::now() + FLUSH_AFTER);
                        }
                    }
                    Some(RecorderEvent::EndOfSegment) => {
                        if self.paragraph_has_text {
                            self.paragraph += 1;
                            self.paragraph_has_text = false;
                        }
                        if !self.failing {
                            flush_at = self.flush().await;
                        }
                    }
                    None => break,
                },
                () = sleep_until(flush_at) => {
                    flush_at = self.flush().await;
                }
            }
        }
        self.close(writer).await;
    }

    /// Writes buffered rows. Returns when to try again if they could not be
    /// written, `None` when nothing is left.
    async fn flush(&mut self) -> Option<Instant> {
        if self.rows.is_empty() {
            return None;
        }
        let attempts = if self.failing {
            1
        } else {
            RETRY_BACKOFF.len() + 1
        };
        for attempt in 0..attempts {
            match db::insert_chunks(&self.pool, self.session_id, self.recording_id, &self.rows)
                .await
            {
                Ok(()) => {
                    self.rows.clear();
                    self.failing = false;
                    return None;
                }
                Err(e) => {
                    tracing::warn!(error = %e, attempt, "persisting transcript chunks failed");
                    if let Some(backoff) = RETRY_BACKOFF
                        .get(attempt)
                        .filter(|_| attempt + 1 < attempts)
                    {
                        tokio::time::sleep(*backoff).await;
                    }
                }
            }
        }
        self.failing = true;
        if !self.reported {
            self.reported = true;
            // try_send: never block on a slow or departed client.
            let _ = self.notify.try_send(ServerMessage::error(
                "persist_failed",
                "transcript is not being saved; retrying",
            ));
        }
        Some(Instant::now() + RETRY_FAILING_EVERY)
    }

    async fn close(mut self, writer: JoinHandle<u64>) {
        // The final flush gets the full retry budget again.
        self.failing = false;
        if self.flush().await.is_some() {
            tracing::error!(rows = self.rows.len(), "dropping unsaved transcript chunks");
        }
        // Both inputs are closed; wait for the audio to hit the disk.
        let bytes_written = match writer.await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "recording audio writer failed");
                0
            }
        };
        let duration_ms = sessions::pcm_duration_ms(bytes_written, self.sample_rate);
        if let Err(e) = db::finish_recording(&self.pool, self.recording_id, duration_ms).await {
            tracing::warn!(error = %e, "finishing recording failed; it will be repaired on next resume");
        }
        // `_guard` drops here, after everything is persisted.
    }
}

/// Appends every frame to the recording, never waiting on anything but the
/// disk. Returns the bytes written once the channel closes.
async fn write_audio(mut file: BufWriter<File>, mut audio: mpsc::Receiver<Bytes>) -> u64 {
    let mut written = 0u64;
    let mut warned = false;
    while let Some(bytes) = audio.recv().await {
        match file.write_all(&bytes).await {
            Ok(()) => written += bytes.len() as u64,
            Err(e) if !warned => {
                warned = true;
                tracing::warn!(error = %e, "writing recording audio failed");
            }
            Err(_) => {}
        }
    }
    if let Err(e) = file.flush().await {
        tracing::warn!(error = %e, "flushing recording audio failed");
    }
    if let Err(e) = file.get_ref().sync_all().await {
        tracing::warn!(error = %e, "syncing recording audio failed");
    }
    written
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::LiveRegistry;
    use crate::sessions::{self, OpenRequest};

    async fn start(
        pool: &PgPool,
        live: &LiveRegistry,
        dir: &Path,
        session_id: Option<Uuid>,
    ) -> (Uuid, Uuid, Recorder, mpsc::Receiver<ServerMessage>) {
        let opened = sessions::open(
            pool,
            live,
            dir,
            OpenRequest {
                session_id,
                language: "en",
                sample_rate: 16_000,
            },
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let ids = (opened.session_id, opened.recording.id);
        let (notify_tx, notify_rx) = mpsc::channel(4);
        let recorder = Recorder::create(pool.clone(), dir, opened, notify_tx)
            .await
            .unwrap();
        (ids.0, ids.1, recorder, notify_rx)
    }

    fn commit(text: &str, start_ms: u64, end_ms: u64) -> RecorderEvent {
        RecorderEvent::Commit {
            text: text.into(),
            start_ms,
            end_ms,
        }
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn persists_audio_and_chunks_on_the_session_timeline(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();

        // A first, already-finished 1 s recording so the offset is visible.
        let sid = Uuid::now_v7();
        db::create_session(&pool, sid, "en").await.unwrap();
        let r0 = db::begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        db::finish_recording(&pool, r0.recording.id, 1_000)
            .await
            .unwrap();

        let (_, rec_id, recorder, _notify) = start(&pool, &live, dir.path(), Some(sid)).await;
        let (audio_tx, audio_rx) = mpsc::channel(8);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(recorder.run(audio_rx, events_rx));

        audio_tx.send(Bytes::from(vec![1u8; 3_200])).await.unwrap();
        audio_tx.send(Bytes::from(vec![2u8; 3_200])).await.unwrap();
        events_tx.send(commit("hello", 0, 120)).unwrap();
        events_tx.send(commit("world", 120, 180)).unwrap();
        events_tx.send(RecorderEvent::EndOfSegment).unwrap();
        // An empty segment must not leave a gap in paragraph numbering.
        events_tx.send(RecorderEvent::EndOfSegment).unwrap();
        events_tx.send(commit("again", 190, 200)).unwrap();
        assert!(live.is_live(sid));

        drop(audio_tx);
        drop(events_tx);
        task.await.unwrap();

        assert!(!live.is_live(sid), "guard released after close");
        assert_eq!(
            db::list_chunks(&pool, sid).await.unwrap(),
            vec![
                db::Chunk {
                    paragraph: 0,
                    text: "hello".into(),
                    start_ms: 1_000,
                    end_ms: 1_120
                },
                db::Chunk {
                    paragraph: 0,
                    text: "world".into(),
                    start_ms: 1_120,
                    end_ms: 1_180
                },
                db::Chunk {
                    paragraph: 1,
                    text: "again".into(),
                    start_ms: 1_190,
                    end_ms: 1_200
                },
            ]
        );
        let recs = db::list_recordings(&pool, sid).await.unwrap();
        assert_eq!(recs[1].duration_ms, Some(200));
        assert!(recs[1].ended_at.is_some());
        assert_eq!(
            db::get_session(&pool, sid)
                .await
                .unwrap()
                .unwrap()
                .duration_ms,
            1_200
        );
        let audio = tokio::fs::read(sessions::recording_path(dir.path(), sid, rec_id))
            .await
            .unwrap();
        assert_eq!(audio.len(), 6_400);
        assert_eq!((audio[0], audio[6_399]), (1, 2));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn flushes_every_32_rows_without_waiting_for_close(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let (sid, _, recorder, _notify) = start(&pool, &live, dir.path(), None).await;
        let (audio_tx, audio_rx) = mpsc::channel(8);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(recorder.run(audio_rx, events_rx));

        for i in 0..32u64 {
            events_tx.send(commit("w", i * 10, i * 10 + 5)).unwrap();
        }
        // Poll for the batch while the recorder is still running. The window
        // is well under the 500 ms timer, so only the row-count trigger can
        // satisfy it.
        let mut seen = 0;
        for _ in 0..30 {
            seen = db::list_chunks(&pool, sid).await.unwrap().len();
            if seen == 32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(seen, 32);
        drop(audio_tx);
        drop(events_tx);
        task.await.unwrap();
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn db_outage_reports_once_and_still_releases(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let (sid, rec_id, recorder, mut notify) = start(&pool, &live, dir.path(), None).await;
        let (audio_tx, audio_rx) = mpsc::channel(8);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(recorder.run(audio_rx, events_rx));

        pool.close().await; // every query from here on fails
        audio_tx.send(Bytes::from(vec![0u8; 3_200])).await.unwrap();
        events_tx.send(commit("lost", 0, 100)).unwrap();
        events_tx.send(RecorderEvent::EndOfSegment).unwrap();
        events_tx.send(commit("also lost", 100, 200)).unwrap();
        events_tx.send(RecorderEvent::EndOfSegment).unwrap();
        drop(audio_tx);
        drop(events_tx);
        task.await.unwrap();

        let mut codes = Vec::new();
        while let Ok(ServerMessage::Error { code, .. }) = notify.try_recv() {
            codes.push(code);
        }
        assert_eq!(codes, vec!["persist_failed".to_string()]);
        assert!(!live.is_live(sid));
        let audio = tokio::fs::read(sessions::recording_path(dir.path(), sid, rec_id))
            .await
            .unwrap();
        assert_eq!(
            audio.len(),
            3_200,
            "audio is kept even when Postgres is down"
        );
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn audio_keeps_draining_while_postgres_is_failing(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let (sid, rec_id, recorder, _notify) = start(&pool, &live, dir.path(), None).await;
        let (audio_tx, audio_rx) = mpsc::channel(2);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(recorder.run(audio_rx, events_rx));

        pool.close().await;
        // Puts the recorder into its ~2.1 s retry path.
        events_tx.send(commit("lost", 0, 100)).unwrap();
        events_tx.send(RecorderEvent::EndOfSegment).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Far more frames than the channel holds: each send only completes if
        // audio is being drained while the retries run.
        tokio::time::timeout(Duration::from_millis(1_000), async {
            for _ in 0..20 {
                audio_tx.send(Bytes::from(vec![7u8; 3_200])).await.unwrap();
            }
        })
        .await
        .expect("audio must not wait on Postgres retries");

        drop(audio_tx);
        drop(events_tx);
        task.await.unwrap();
        let audio = tokio::fs::read(sessions::recording_path(dir.path(), sid, rec_id))
            .await
            .unwrap();
        assert_eq!(audio.len(), 20 * 3_200);
    }
}

//! Session lifecycle: opening (create or resume) a session for a connection,
//! the on-disk audio layout, and repair of recordings a crash left unclosed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::db;
use crate::live::{LiveGuard, LiveRegistry};
use crate::protocol::ServerMessage;

pub fn session_dir(data_dir: &Path, session_id: Uuid) -> PathBuf {
    data_dir.join("sessions").join(session_id.to_string())
}

pub fn recording_path(data_dir: &Path, session_id: Uuid, recording_id: Uuid) -> PathBuf {
    session_dir(data_dir, session_id).join(format!("{recording_id}.pcm"))
}

/// Duration of raw mono Int16 PCM. A trailing odd byte is half a sample and
/// does not count.
pub fn pcm_duration_ms(bytes: u64, sample_rate: u32) -> i64 {
    if sample_rate == 0 {
        return 0;
    }
    ((bytes / 2) * 1000 / u64::from(sample_rate)) as i64
}

pub async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Closes recordings a crash left open, using the audio actually on disk.
/// Callers must hold the session's live claim, or they could "repair" a
/// recording that is still being written.
pub async fn repair(pool: &PgPool, data_dir: &Path, session_id: Uuid) -> anyhow::Result<()> {
    for rec in db::unclosed_recordings(pool, session_id).await? {
        let len = file_len(&recording_path(data_dir, session_id, rec.id)).await;
        let duration_ms = pcm_duration_ms(len, rec.sample_rate as u32);
        tracing::info!(recording = %rec.id, duration_ms, "repairing unclosed recording");
        db::finish_recording(pool, rec.id, duration_ms).await?;
    }
    Ok(())
}

pub async fn delete_files(data_dir: &Path, session_id: Uuid) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(session_dir(data_dir, session_id)).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

pub struct OpenRequest<'a> {
    pub session_id: Option<Uuid>,
    pub language: &'a str,
    pub sample_rate: u32,
}

pub struct OpenedSession {
    pub session_id: Uuid,
    pub resumed: bool,
    pub recording: db::Recording,
    pub next_paragraph: i32,
    pub guard: LiveGuard,
}

#[derive(Debug)]
pub enum OpenError {
    NotFound,
    Busy,
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for OpenError {
    fn from(e: anyhow::Error) -> Self {
        OpenError::Internal(e)
    }
}

impl OpenError {
    pub fn to_message(&self) -> ServerMessage {
        match self {
            OpenError::NotFound => {
                ServerMessage::error("session_not_found", "no session with that id")
            }
            OpenError::Busy => {
                ServerMessage::error("session_busy", "session is live on another connection")
            }
            OpenError::Internal(e) => ServerMessage::error("init_failed", e.to_string()),
        }
    }
}

/// Creates a new session, or claims and resumes an existing one, and opens
/// its next recording.
pub async fn open(
    pool: &PgPool,
    live: &LiveRegistry,
    data_dir: &Path,
    req: OpenRequest<'_>,
    wait: Duration,
) -> Result<OpenedSession, OpenError> {
    let (session_id, resumed, guard) = match req.session_id {
        None => {
            let id = Uuid::now_v7();
            let guard = live
                .try_claim(id)
                .expect("a fresh UUIDv7 cannot already be live");
            db::create_session(pool, id, req.language).await?;
            (id, false, guard)
        }
        Some(id) => {
            if db::get_session(pool, id).await?.is_none() {
                return Err(OpenError::NotFound);
            }
            let guard = live.claim(id, wait).await.map_err(|_| OpenError::Busy)?;
            repair(pool, data_dir, id).await?;
            (id, true, guard)
        }
    };
    let start =
        db::begin_recording(pool, session_id, Uuid::now_v7(), req.sample_rate as i32).await?;
    Ok(OpenedSession {
        session_id,
        resumed,
        recording: start.recording,
        next_paragraph: start.next_paragraph,
        guard,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::live::LiveRegistry;
    use crate::protocol::ServerMessage;
    use sqlx::PgPool;
    use std::time::Duration;
    use uuid::Uuid;

    fn req(session_id: Option<Uuid>) -> OpenRequest<'static> {
        OpenRequest {
            session_id,
            language: "en",
            sample_rate: 16_000,
        }
    }

    #[test]
    fn pcm_duration_counts_whole_samples() {
        assert_eq!(pcm_duration_ms(32_000, 16_000), 1_000);
        assert_eq!(pcm_duration_ms(32_001, 16_000), 1_000);
        assert_eq!(pcm_duration_ms(0, 16_000), 0);
        assert_eq!(pcm_duration_ms(100, 0), 0);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn new_session_is_created_and_claimed(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let opened = open(
            &pool,
            &live,
            dir.path(),
            req(None),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        assert!(!opened.resumed);
        assert_eq!(
            (opened.recording.ordinal, opened.recording.offset_ms),
            (0, 0)
        );
        assert!(live.is_live(opened.session_id));
        assert!(db::get_session(&pool, opened.session_id)
            .await
            .unwrap()
            .is_some());
        drop(opened);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_repairs_unclosed_recording_from_file_size(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let first = open(
            &pool,
            &live,
            dir.path(),
            req(None),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let sid = first.session_id;
        // Simulate a crash: 1 s of audio on disk, recording never finished.
        let path = recording_path(dir.path(), sid, first.recording.id);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, vec![0u8; 32_000]).await.unwrap();
        drop(first);

        let second = open(
            &pool,
            &live,
            dir.path(),
            req(Some(sid)),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        assert!(second.resumed);
        assert_eq!(
            (second.recording.ordinal, second.recording.offset_ms),
            (1, 1_000)
        );
        let recs = db::list_recordings(&pool, sid).await.unwrap();
        assert_eq!(recs[0].duration_ms, Some(1_000));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_of_unknown_session_is_not_found(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let err = open(
            &pool,
            &live,
            dir.path(),
            req(Some(Uuid::now_v7())),
            Duration::from_millis(10),
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(err, OpenError::NotFound));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_of_live_session_is_busy_after_waiting(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let first = open(
            &pool,
            &live,
            dir.path(),
            req(None),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let err = open(
            &pool,
            &live,
            dir.path(),
            req(Some(first.session_id)),
            Duration::from_millis(50),
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(err, OpenError::Busy));
        // The refused resume must not have created a recording.
        assert_eq!(
            db::list_recordings(&pool, first.session_id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn delete_files_tolerates_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        delete_files(dir.path(), Uuid::now_v7()).await.unwrap();
    }

    #[test]
    fn open_errors_map_to_wire_codes() {
        let code = |e: OpenError| match e.to_message() {
            ServerMessage::Error { code, .. } => code,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(code(OpenError::NotFound), "session_not_found");
        assert_eq!(code(OpenError::Busy), "session_busy");
        assert_eq!(
            code(OpenError::Internal(anyhow::anyhow!("x"))),
            "init_failed"
        );
    }
}

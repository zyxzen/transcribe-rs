//! Postgres access. All SQL lives here so the persistence surface is
//! reviewable in one place; callers get plain structs back.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .context("connecting to Postgres (is it running, and is DATABASE_URL right?)")?;
    MIGRATOR.run(&pool).await.context("running migrations")?;
    Ok(pool)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub id: Uuid,
    pub title: Option<String>,
    pub language: String,
    pub duration_ms: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListItem {
    pub id: Uuid,
    pub title: Option<String>,
    pub language: String,
    pub duration_ms: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub preview: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Recording {
    pub id: Uuid,
    #[serde(skip)]
    pub session_id: Uuid,
    pub ordinal: i32,
    pub offset_ms: i64,
    pub sample_rate: i32,
    pub duration_ms: Option<i64>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// One persisted wire `commit`, timestamps on the session timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub paragraph: i32,
    pub text: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

pub struct RecordingStart {
    pub recording: Recording,
    /// Paragraph index the new recording's first commit belongs to.
    pub next_paragraph: i32,
}

pub async fn create_session(pool: &PgPool, id: Uuid, language: &str) -> Result<Session> {
    let session = sqlx::query_as!(
        Session,
        "INSERT INTO sessions (id, language) VALUES ($1, $2)
         RETURNING id, title, language, duration_ms, created_at, updated_at",
        id,
        language
    )
    .fetch_one(pool)
    .await?;
    Ok(session)
}

pub async fn get_session(pool: &PgPool, id: Uuid) -> Result<Option<Session>> {
    let session = sqlx::query_as!(
        Session,
        "SELECT id, title, language, duration_ms, created_at, updated_at
         FROM sessions WHERE id = $1",
        id
    )
    .fetch_optional(pool)
    .await?;
    Ok(session)
}

/// Newest first. `before` is the last id of the previous page (UUIDv7 sorts
/// by creation time).
pub async fn list_sessions(
    pool: &PgPool,
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<SessionListItem>> {
    let items = sqlx::query_as!(
        SessionListItem,
        r#"SELECT s.id, s.title, s.language, s.duration_ms, s.created_at, s.updated_at,
                  coalesce((SELECT left(string_agg(t.text, ' ' ORDER BY t.id), 160)
                            FROM (SELECT c.id, c.text FROM transcript_chunks c
                                  WHERE c.session_id = s.id ORDER BY c.id LIMIT 40) t),
                           '') AS "preview!"
           FROM sessions s
           WHERE $1::uuid IS NULL OR s.id < $1
           ORDER BY s.id DESC
           LIMIT $2"#,
        before,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(items)
}

pub async fn update_title(pool: &PgPool, id: Uuid, title: Option<&str>) -> Result<Option<Session>> {
    let session = sqlx::query_as!(
        Session,
        "UPDATE sessions SET title = $2, updated_at = now() WHERE id = $1
         RETURNING id, title, language, duration_ms, created_at, updated_at",
        id,
        title
    )
    .fetch_optional(pool)
    .await?;
    Ok(session)
}

pub async fn delete_session(pool: &PgPool, id: Uuid) -> Result<bool> {
    let done = sqlx::query!("DELETE FROM sessions WHERE id = $1", id)
        .execute(pool)
        .await?;
    Ok(done.rows_affected() > 0)
}

/// Opens the next recording of a session. Its offset is the session's
/// duration so far, which keeps recordings contiguous on one timeline.
pub async fn begin_recording(
    pool: &PgPool,
    session_id: Uuid,
    recording_id: Uuid,
    sample_rate: i32,
) -> Result<RecordingStart> {
    let mut tx = pool.begin().await?;
    let offset_ms = sqlx::query_scalar!(
        "SELECT duration_ms FROM sessions WHERE id = $1 FOR UPDATE",
        session_id
    )
    .fetch_one(&mut *tx)
    .await?;
    let ordinal = sqlx::query_scalar!(
        r#"SELECT coalesce(max(ordinal) + 1, 0) AS "n!" FROM recordings WHERE session_id = $1"#,
        session_id
    )
    .fetch_one(&mut *tx)
    .await?;
    let next_paragraph = sqlx::query_scalar!(
        r#"SELECT coalesce(max(paragraph) + 1, 0) AS "n!" FROM transcript_chunks WHERE session_id = $1"#,
        session_id
    )
    .fetch_one(&mut *tx)
    .await?;
    let recording = sqlx::query_as!(
        Recording,
        "INSERT INTO recordings (id, session_id, ordinal, offset_ms, sample_rate)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, session_id, ordinal, offset_ms, sample_rate, duration_ms, started_at, ended_at",
        recording_id,
        session_id,
        ordinal,
        offset_ms,
        sample_rate
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(RecordingStart {
        recording,
        next_paragraph,
    })
}

/// Records a recording's final duration and recomputes the session total as
/// the sum, so it is idempotent and doubles as crash repair.
pub async fn finish_recording(pool: &PgPool, recording_id: Uuid, duration_ms: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    let session_id = sqlx::query_scalar!(
        "UPDATE recordings SET duration_ms = $2, ended_at = coalesce(ended_at, now())
         WHERE id = $1 RETURNING session_id",
        recording_id,
        duration_ms
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "UPDATE sessions
         SET duration_ms = (SELECT coalesce(sum(duration_ms), 0)::int8 FROM recordings WHERE session_id = $1),
             updated_at = now()
         WHERE id = $1",
        session_id
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn unclosed_recordings(pool: &PgPool, session_id: Uuid) -> Result<Vec<Recording>> {
    let rows = sqlx::query_as!(
        Recording,
        "SELECT id, session_id, ordinal, offset_ms, sample_rate, duration_ms, started_at, ended_at
         FROM recordings WHERE session_id = $1 AND duration_ms IS NULL ORDER BY ordinal",
        session_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn list_recordings(pool: &PgPool, session_id: Uuid) -> Result<Vec<Recording>> {
    let rows = sqlx::query_as!(
        Recording,
        "SELECT id, session_id, ordinal, offset_ms, sample_rate, duration_ms, started_at, ended_at
         FROM recordings WHERE session_id = $1 ORDER BY ordinal",
        session_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One round trip for a whole batch.
pub async fn insert_chunks(
    pool: &PgPool,
    session_id: Uuid,
    recording_id: Uuid,
    chunks: &[Chunk],
) -> Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }
    let paragraphs: Vec<i32> = chunks.iter().map(|c| c.paragraph).collect();
    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
    let starts: Vec<i64> = chunks.iter().map(|c| c.start_ms).collect();
    let ends: Vec<i64> = chunks.iter().map(|c| c.end_ms).collect();
    sqlx::query!(
        "INSERT INTO transcript_chunks (session_id, recording_id, paragraph, text, start_ms, end_ms)
         SELECT $1::uuid, $2::uuid, * FROM UNNEST($3::int4[], $4::text[], $5::int8[], $6::int8[])",
        session_id,
        recording_id,
        &paragraphs[..],
        &texts[..],
        &starts[..],
        &ends[..]
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_chunks(pool: &PgPool, session_id: Uuid) -> Result<Vec<Chunk>> {
    let rows = sqlx::query_as!(
        Chunk,
        "SELECT paragraph, text, start_ms, end_ms FROM transcript_chunks
         WHERE session_id = $1 ORDER BY id",
        session_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(paragraph: i32, text: &str, start_ms: i64, end_ms: i64) -> Chunk {
        Chunk {
            paragraph,
            text: text.into(),
            start_ms,
            end_ms,
        }
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_continues_the_timeline(pool: PgPool) {
        let sid = Uuid::now_v7();
        create_session(&pool, sid, "en").await.unwrap();

        let first = begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        assert_eq!(
            (
                first.recording.ordinal,
                first.recording.offset_ms,
                first.next_paragraph
            ),
            (0, 0, 0)
        );
        insert_chunks(
            &pool,
            sid,
            first.recording.id,
            &[chunk(0, "hello", 0, 400), chunk(1, "again", 900, 1_200)],
        )
        .await
        .unwrap();
        finish_recording(&pool, first.recording.id, 1_500)
            .await
            .unwrap();

        let second = begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        assert_eq!(
            (
                second.recording.ordinal,
                second.recording.offset_ms,
                second.next_paragraph
            ),
            (1, 1_500, 2)
        );
        assert_eq!(
            get_session(&pool, sid).await.unwrap().unwrap().duration_ms,
            1_500
        );
        assert_eq!(unclosed_recordings(&pool, sid).await.unwrap().len(), 1);
        assert_eq!(list_recordings(&pool, sid).await.unwrap().len(), 2);
        assert_eq!(
            list_chunks(&pool, sid).await.unwrap(),
            vec![chunk(0, "hello", 0, 400), chunk(1, "again", 900, 1_200)]
        );
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn session_duration_is_the_sum_of_recordings(pool: PgPool) {
        let sid = Uuid::now_v7();
        create_session(&pool, sid, "en").await.unwrap();
        let a = begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        finish_recording(&pool, a.recording.id, 1_000)
            .await
            .unwrap();
        let b = begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        finish_recording(&pool, b.recording.id, 250).await.unwrap();
        // Finishing twice (repair after a clean close) must not double count.
        finish_recording(&pool, b.recording.id, 250).await.unwrap();
        assert_eq!(
            get_session(&pool, sid).await.unwrap().unwrap().duration_ms,
            1_250
        );
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn list_is_newest_first_with_keyset_paging(pool: PgPool) {
        let mut ids = Vec::new();
        for _ in 0..3 {
            let id = Uuid::now_v7();
            create_session(&pool, id, "en").await.unwrap();
            ids.push(id);
            // UUIDv7 is millisecond-ordered; keep ids strictly increasing.
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let r = begin_recording(&pool, ids[2], Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        insert_chunks(
            &pool,
            ids[2],
            r.recording.id,
            &[chunk(0, "hello", 0, 1), chunk(1, "world", 2, 3)],
        )
        .await
        .unwrap();

        let page = list_sessions(&pool, None, 2).await.unwrap();
        assert_eq!(
            page.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![ids[2], ids[1]]
        );
        assert_eq!(page[0].preview, "hello world");
        assert_eq!(page[1].preview, "");

        let next = list_sessions(&pool, Some(ids[1]), 2).await.unwrap();
        assert_eq!(next.iter().map(|s| s.id).collect::<Vec<_>>(), vec![ids[0]]);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn title_update_and_cascade_delete(pool: PgPool) {
        let sid = Uuid::now_v7();
        create_session(&pool, sid, "en").await.unwrap();
        let r = begin_recording(&pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        insert_chunks(&pool, sid, r.recording.id, &[chunk(0, "x", 0, 1)])
            .await
            .unwrap();

        let renamed = update_title(&pool, sid, Some("Standup"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renamed.title.as_deref(), Some("Standup"));
        assert!(update_title(&pool, Uuid::now_v7(), None)
            .await
            .unwrap()
            .is_none());

        assert!(delete_session(&pool, sid).await.unwrap());
        assert!(!delete_session(&pool, sid).await.unwrap());
        assert!(list_chunks(&pool, sid).await.unwrap().is_empty());
        assert!(list_recordings(&pool, sid).await.unwrap().is_empty());
    }
}

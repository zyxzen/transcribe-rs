# Postgres Sessions + React Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist transcription sessions in Postgres (transcript) and on disk (audio) so they can be browsed, resumed and exported, and rewrite the browser client in React + TypeScript.

**Architecture:** Each WebSocket connection gains a fourth task, the *recorder*, fed by the receiver (audio `Bytes`, bounded channel) and the pipeline (commit events, unbounded channel). It appends raw PCM to `data/sessions/{session}/{recording}.pcm` and batches commits into `transcript_chunks`. An in-memory `LiveRegistry` ensures one live socket per session. A REST API under `/api/sessions` serves browse/rename/delete/export. The client is a Vite + React app served from `client/dist`.

**Tech Stack:** Rust 1.90, axum 0.8, tokio, sqlx 0.8.6 (Postgres 17, compile-checked `query!`), uuid v7, chrono, tokio-util; React 19.3, React Router 8, TanStack Query 5, Vite 8, TypeScript 6.0, Vitest 5, ESLint 10, pnpm.

**Spec:** `docs/superpowers/specs/2026-10-03-postgres-sessions-design.md`

## Deviations from the spec (decided while planning)

| Spec says | Plan does | Why |
|---|---|---|
| sqlx (unversioned; 0.9 is current) | **sqlx 0.8.6** | sqlx 0.9 needs rustc ≥ 1.94; this machine has 1.90. Upgrade later with the toolchain. |
| React Router v7 | **React Router 8.4** | Current major. `RouterProvider` is imported from `react-router/dom`, everything else from `react-router`. |
| TypeScript (unversioned; 7.0 is current) | **TypeScript 6.0** | typescript-eslint 8.71 supports `typescript <6.1`. |
| `GET /api/sessions/:id/export.{ext}` | **`GET /api/sessions/{id}/export/{format}`** | axum 0.8.4's router (matchit 0.8.4) cannot capture part of a path segment. The download filename still comes from `Content-Disposition`. |
| `createdb transcribe_rust_test` | **not needed** | `#[sqlx::test]` creates and drops a throwaway database per test on the server `DATABASE_URL` points at. |
| `.sqlx/` committed + `cargo build` offline | Same, **plus** `DATABASE_URL` must be exported when running `cargo test` | `#[sqlx::test]` reads `DATABASE_URL` from the process environment at run time. |

## Global Constraints

- Latency first: no DB or disk I/O on the pipeline hot path; the pipeline never awaits the recorder (commit events go over an **unbounded** channel).
- Audio is never dropped: receiver → recorder audio channel is **bounded** (`--audio-queue`) and the receiver **awaits** it.
- Wire protocol changes are backward compatible: `start.sessionId` optional; `ready` gains `sessionId`, `resumed`, `offsetMs`. Wire timestamps stay **stream-relative**.
- Stored timestamps are on the **session timeline**: `recording.offset_ms + stream ms`.
- One live socket per session; a resume waits up to **5 s** (`RESUME_WAIT`) for the lock, then `session_busy`.
- Keepalive: server Ping every **15 s**; close after **45 s** with no inbound frame.
- Recorder flush: **500 ms** after the first buffered row, or **32** rows, or on `EndOfSegment`. DB retry **100/400/1600 ms**, then `persist_failed` once, then retry every **5 s**.
- Cues: ≤ **7000 ms**, ≤ **84** chars, never cross paragraphs, lines ≤ **42** chars, max **2** lines.
- Titles ≤ **200** chars (trimmed; empty → null). List `limit` default **50**, max **200**. Preview **160** chars. Filenames ≤ **60** chars slug.
- Client offline buffer: **960 000** bytes (30 s at 16 kHz Int16). Backoff: **0.5, 1, 2, 4, 8, 10, 10…** s.
- `localStorage` key for the current session: `transcribe.currentSessionId`. Transcript text is no longer stored in `localStorage`.
- Commits: conventional style, **no Claude attribution lines** (user's global rule).
- Bind stays `127.0.0.1` by default (no auth).

## Review Focus

1. **Malformed `sessionId` in `start`** (not a UUID) → `bad_handshake` error and close; no session row created. Test: Task 5 `rejects_malformed_session_id`.
2. **Server crash leaves a recording with `duration_ms IS NULL`** → the next resume or (non-live) export recomputes it from the `.pcm` size and the session duration is the sum. Tests: Task 3 `resume_repairs_unclosed_recording_from_file_size`, Task 7 `wav_export_repairs_unclosed_recording`.
3. **Postgres becomes unreachable mid-session** → client gets exactly one `persist_failed`, audio file still complete, recorder still terminates and releases the live lock. Test: Task 4 `db_outage_reports_once_and_still_releases`.
4. **Exporting a session with no speech** (no chunks, empty or missing `.pcm`) → empty `.txt`, `WEBVTT\n\n`, empty `.srt`, header-only 44-byte WAV. Tests: Task 6 `empty_session_exports_are_valid`, Task 7 `empty_session_wav_is_header_only`.
5. **Title with quotes / only non-ASCII characters** → filename stays ASCII-safe (`Content-Disposition` cannot be broken), falling back to `session-YYYY-MM-DD-HHMM`. Test: Task 6 `filename_is_ascii_safe`.

---

## File map

**Server (`crates/server/`)**

| File | Status | Responsibility |
|---|---|---|
| `Cargo.toml` | modify | new deps |
| `migrations/0001_sessions.sql` | create | schema |
| `src/config.rs` | modify | `database_url`, `data_dir`, `db_max_connections`; `client_dir` default → `client/dist` (Task 10) |
| `src/db.rs` | create | pool + migrations + every SQL query |
| `src/live.rs` | create | `LiveRegistry`, `LiveGuard`, `Busy` |
| `src/sessions.rs` | create | open (create/resume) a session for a connection, file layout, repair, file deletion |
| `src/recorder.rs` | create | per-connection persistence task |
| `src/export.rs` | create | pure export functions |
| `src/api.rs` | create | REST handlers + `ApiError` |
| `src/protocol.rs` | modify | `StartParams.session_id`, `ServerMessage::Ready { .. }` |
| `src/pipeline.rs` | modify | `recorder` sender; `emit()` forwards commits/EOS |
| `src/ws.rs` | modify | handshake opens session, spawns recorder, keepalive, idle timeout, shutdown token |
| `src/main.rs` | modify | dotenv, DB connect, data dir, `/api` nest, tracked graceful shutdown |
| `/.sqlx/` | create | offline query cache (workspace root) |
| `/.env.example`, `/.gitignore` | create/modify | config template; ignore `.env`, `data/`, client build output |
| `/scripts/smoke-ws.mjs` | create | end-to-end WebSocket smoke test (Node 24 global `WebSocket`) |

**Client (`client/`)** — replaces `client/index.html` (vanilla) in Task 10.

| File | Responsibility |
|---|---|
| `package.json`, `tsconfig.json`, `vite.config.ts`, `eslint.config.js`, `index.html` | tooling |
| `public/audio-worklet.js` | unchanged worklet (moved) |
| `src/main.tsx`, `src/App.tsx` | bootstrap, router, query client |
| `src/protocol.ts` | wire types + `parseServerMessage` |
| `src/api.ts` | REST types + fetch wrappers + `ApiError` |
| `src/format.ts` | `formatClock`, `formatDate`, `sessionLabel` |
| `src/sessionStore.ts` | `localStorage` current-session id |
| `src/transcript/reducer.ts` | pure transcript state machine |
| `src/transcript/reconnect.ts` | `backoffDelay`, `FrameBuffer` |
| `src/transcript/liveSession.ts` | mic + worklet + WebSocket + reconnect (plain class, no React) |
| `src/hooks/useTranscriber.ts` | React wrapper over `LiveSession` + reducer |
| `src/hooks/useLevelMeter.ts`, `src/hooks/useAutoScroll.ts` | UI behaviours ported from the vanilla client |
| `src/components/*` | `icons.tsx`, `Transcript.tsx`, `TitleEditor.tsx`, `ExportMenu.tsx` |
| `src/pages/*` | `RecorderPage.tsx`, `SessionsPage.tsx`, `SessionPage.tsx` |
| `src/styles/app.css` | ported styles + page styles |

---

### Task 1: Postgres foundation (deps, config, schema, `db.rs`)

**Files:**
- Modify: `crates/server/Cargo.toml`, `crates/server/src/config.rs`, `crates/server/src/main.rs`, `.gitignore`
- Create: `crates/server/migrations/0001_sessions.sql`, `crates/server/src/db.rs`, `.env.example`, `.env` (not committed), `.sqlx/`

**Interfaces:**
- Produces (all in `crate::db`):
  - `pub static MIGRATOR: sqlx::migrate::Migrator`
  - `pub async fn connect(url: &str, max_connections: u32) -> anyhow::Result<PgPool>`
  - `pub struct Session { id: Uuid, title: Option<String>, language: String, duration_ms: i64, created_at: DateTime<Utc>, updated_at: DateTime<Utc> }` (Serialize camelCase, all fields `pub`)
  - `pub struct SessionListItem { id, title, language, duration_ms, created_at, updated_at, preview: String }` (Serialize camelCase)
  - `pub struct Recording { id: Uuid, session_id: Uuid (serde skip), ordinal: i32, offset_ms: i64, sample_rate: i32, duration_ms: Option<i64>, started_at: DateTime<Utc>, ended_at: Option<DateTime<Utc>> }` (Serialize camelCase)
  - `pub struct Chunk { paragraph: i32, text: String, start_ms: i64, end_ms: i64 }` (`Debug, Clone, PartialEq, Eq`)
  - `pub struct RecordingStart { recording: Recording, next_paragraph: i32 }`
  - `create_session(pool, id: Uuid, language: &str) -> Result<Session>`
  - `get_session(pool, id) -> Result<Option<Session>>`
  - `list_sessions(pool, before: Option<Uuid>, limit: i64) -> Result<Vec<SessionListItem>>`
  - `update_title(pool, id, title: Option<&str>) -> Result<Option<Session>>`
  - `delete_session(pool, id) -> Result<bool>`
  - `begin_recording(pool, session_id, recording_id, sample_rate: i32) -> Result<RecordingStart>`
  - `finish_recording(pool, recording_id, duration_ms: i64) -> Result<()>`
  - `unclosed_recordings(pool, session_id) -> Result<Vec<Recording>>`
  - `list_recordings(pool, session_id) -> Result<Vec<Recording>>`
  - `insert_chunks(pool, session_id, recording_id, chunks: &[Chunk]) -> Result<()>`
  - `list_chunks(pool, session_id) -> Result<Vec<Chunk>>`
  - Config: `cfg.database_url: String`, `cfg.data_dir: PathBuf`, `cfg.db_max_connections: u32`

- [ ] **Step 1: Create the database and local env**

```bash
createdb transcribe_rust
cat > .env.example <<'EOF'
# Postgres for sessions. `createdb transcribe_rust` first.
DATABASE_URL=postgres://localhost/transcribe_rust
# Where per-recording PCM audio is written.
TR_DATA_DIR=data
EOF
cp .env.example .env
printf '\n/.env\n/data\n/client/dist\n/client/node_modules\n' >> .gitignore
export DATABASE_URL=postgres://localhost/transcribe_rust
```

- [ ] **Step 2: Install sqlx-cli matching the library version**

```bash
cargo install sqlx-cli --version 0.8.6 --locked --no-default-features --features postgres,rustls
cargo sqlx --version
```
Expected: `sqlx-cli 0.8.6`.

- [ ] **Step 3: Add dependencies**

In `crates/server/Cargo.toml`, change the `tokio` and `futures-util` lines and add the new crates under `[dependencies]`, then add a `[dev-dependencies]` section:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "net", "time", "signal", "fs", "io-util"] }
futures-util = { version = "0.3", default-features = false, features = ["sink", "std"] }

sqlx = { version = "0.8.6", default-features = false, features = ["runtime-tokio", "postgres", "uuid", "chrono", "migrate", "macros"] }
uuid = { version = "1", features = ["v7", "serde"] }
chrono = { version = "0.4", default-features = false, features = ["clock", "serde"] }
tokio-util = { version = "0.7", features = ["rt", "io"] }
bytes = "1"
dotenvy = "0.15"

[dev-dependencies]
tokio = { version = "1", features = ["test-util", "macros", "rt-multi-thread"] }
tempfile = "3"
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
```

- [ ] **Step 4: Write the migration**

Create `crates/server/migrations/0001_sessions.sql`:

```sql
create table sessions (
  id           uuid primary key,
  title        text,
  language     text not null,
  duration_ms  bigint not null default 0,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

create table recordings (
  id           uuid primary key,
  session_id   uuid not null references sessions(id) on delete cascade,
  ordinal      int not null,
  offset_ms    bigint not null,
  sample_rate  int not null,
  duration_ms  bigint,
  started_at   timestamptz not null default now(),
  ended_at     timestamptz,
  unique (session_id, ordinal)
);

create table transcript_chunks (
  id           bigserial primary key,
  session_id   uuid not null references sessions(id) on delete cascade,
  recording_id uuid not null references recordings(id) on delete cascade,
  paragraph    int not null,
  text         text not null,
  start_ms     bigint not null,
  end_ms       bigint not null
);
create index transcript_chunks_session_idx on transcript_chunks (session_id, id);
```

Apply it to the dev database (the `query!` macros type-check against it at compile time):

```bash
cargo sqlx migrate run --source crates/server/migrations
```
Expected: `Applied 1/migrate sessions`.

- [ ] **Step 5: Add config fields**

In `crates/server/src/config.rs`, add after the `client_dir` field:

```rust
    /// Postgres connection string for session storage.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Root for persisted audio: `{data_dir}/sessions/{session}/{recording}.pcm`.
    #[arg(long, env = "TR_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    /// Postgres pool size. Writes are batched per connection, so this stays small.
    #[arg(long, env = "TR_DB_MAX_CONNECTIONS", default_value_t = 8)]
    pub db_max_connections: u32,
```

- [ ] **Step 6: Write the failing `db.rs` tests**

Create `crates/server/src/db.rs` containing only the test module for now, and add `mod db;` to the module list at the top of `crates/server/src/main.rs` (alphabetical, after `mod config;`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(paragraph: i32, text: &str, start_ms: i64, end_ms: i64) -> Chunk {
        Chunk { paragraph, text: text.into(), start_ms, end_ms }
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_continues_the_timeline(pool: PgPool) {
        let sid = Uuid::now_v7();
        create_session(&pool, sid, "en").await.unwrap();

        let first = begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        assert_eq!(
            (first.recording.ordinal, first.recording.offset_ms, first.next_paragraph),
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
        finish_recording(&pool, first.recording.id, 1_500).await.unwrap();

        let second = begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        assert_eq!(
            (second.recording.ordinal, second.recording.offset_ms, second.next_paragraph),
            (1, 1_500, 2)
        );
        assert_eq!(get_session(&pool, sid).await.unwrap().unwrap().duration_ms, 1_500);
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
        let a = begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        finish_recording(&pool, a.recording.id, 1_000).await.unwrap();
        let b = begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        finish_recording(&pool, b.recording.id, 250).await.unwrap();
        // Finishing twice (repair after a clean close) must not double count.
        finish_recording(&pool, b.recording.id, 250).await.unwrap();
        assert_eq!(get_session(&pool, sid).await.unwrap().unwrap().duration_ms, 1_250);
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
        let r = begin_recording(&pool, ids[2], Uuid::now_v7(), 16_000).await.unwrap();
        insert_chunks(
            &pool,
            ids[2],
            r.recording.id,
            &[chunk(0, "hello", 0, 1), chunk(1, "world", 2, 3)],
        )
        .await
        .unwrap();

        let page = list_sessions(&pool, None, 2).await.unwrap();
        assert_eq!(page.iter().map(|s| s.id).collect::<Vec<_>>(), vec![ids[2], ids[1]]);
        assert_eq!(page[0].preview, "hello world");
        assert_eq!(page[1].preview, "");

        let next = list_sessions(&pool, Some(ids[1]), 2).await.unwrap();
        assert_eq!(next.iter().map(|s| s.id).collect::<Vec<_>>(), vec![ids[0]]);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn title_update_and_cascade_delete(pool: PgPool) {
        let sid = Uuid::now_v7();
        create_session(&pool, sid, "en").await.unwrap();
        let r = begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        insert_chunks(&pool, sid, r.recording.id, &[chunk(0, "x", 0, 1)]).await.unwrap();

        let renamed = update_title(&pool, sid, Some("Standup")).await.unwrap().unwrap();
        assert_eq!(renamed.title.as_deref(), Some("Standup"));
        assert!(update_title(&pool, Uuid::now_v7(), None).await.unwrap().is_none());

        assert!(delete_session(&pool, sid).await.unwrap());
        assert!(!delete_session(&pool, sid).await.unwrap());
        assert!(list_chunks(&pool, sid).await.unwrap().is_empty());
        assert!(list_recordings(&pool, sid).await.unwrap().is_empty());
    }
}
```

- [ ] **Step 7: Run the tests to verify they fail**

Run: `cargo test db::`
Expected: compile errors — `cannot find function create_session`, `cannot find type Chunk`, etc.

- [ ] **Step 8: Implement `db.rs`**

Put this above the test module in `crates/server/src/db.rs`:

```rust
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
    Ok(RecordingStart { recording, next_paragraph })
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
```

- [ ] **Step 9: Connect at startup**

In `crates/server/src/main.rs`, make these edits:

1. First line of `main()` body (before tracing init):
```rust
    // `.env` is optional; real environment variables take precedence.
    let _ = dotenvy::dotenv();
```
2. Immediately after `let cfg = Arc::new(Config::parse());`:
```rust
    // Fail fast on a bad database before the multi-second model load.
    let db = db::connect(&cfg.database_url, cfg.db_max_connections).await?;
    tokio::fs::create_dir_all(cfg.data_dir.join("sessions"))
        .await
        .with_context(|| format!("creating {}", cfg.data_dir.display()))?;
```
`db` is unused until Task 5; prefix with `_` if the warning bothers you: `let _db = ...` — Task 5 renames it back.

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cargo test db::`
Expected: 4 passed. (Dead-code warnings for functions used by later tasks are expected until Task 7.)

Run: `cargo test`
Expected: all 25 tests pass (21 existing + 4 new).

- [ ] **Step 11: Generate the offline query cache**

```bash
cargo sqlx prepare --workspace
ls .sqlx | head
```
Expected: a `.sqlx/` directory at the repo root with one `query-*.json` per query.

- [ ] **Step 12: Commit**

```bash
git add .gitignore .env.example .sqlx crates/server/Cargo.toml Cargo.lock crates/server/migrations crates/server/src/db.rs crates/server/src/config.rs crates/server/src/main.rs
git commit -m "feat(server): add Postgres schema and data access for sessions"
```

---

### Task 2: Live-session registry

**Files:**
- Create: `crates/server/src/live.rs`
- Modify: `crates/server/src/main.rs` (add `mod live;`)

**Interfaces:**
- Produces (`crate::live`):
  - `#[derive(Clone, Default)] pub struct LiveRegistry`
  - `LiveRegistry::is_live(&self, id: Uuid) -> bool`
  - `LiveRegistry::try_claim(&self, id: Uuid) -> Option<LiveGuard>`
  - `LiveRegistry::claim(&self, id: Uuid, wait: Duration) -> impl Future<Output = Result<LiveGuard, Busy>>`
  - `#[must_use] pub struct LiveGuard` — releases on `Drop`
  - `#[derive(Debug, PartialEq, Eq)] pub struct Busy;`

- [ ] **Step 1: Write the failing tests**

Create `crates/server/src/live.rs` with only the test module, and add `mod live;` to `main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claim_is_exclusive_until_the_guard_drops() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let guard = reg.try_claim(id).expect("first claim");
        assert!(reg.is_live(id));
        assert!(reg.try_claim(id).is_none());
        drop(guard);
        assert!(!reg.is_live(id));
        assert!(reg.try_claim(id).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_claim_acquires_once_released() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let guard = reg.try_claim(id).unwrap();
        let waiter = {
            let reg = reg.clone();
            tokio::spawn(async move { reg.claim(id, Duration::from_secs(5)).await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(guard);
        assert_eq!(waiter.await.unwrap(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_claim_times_out_as_busy() {
        let reg = LiveRegistry::default();
        let id = Uuid::now_v7();
        let _guard = reg.try_claim(id).unwrap();
        assert!(matches!(reg.claim(id, Duration::from_secs(5)).await, Err(Busy)));
    }

    #[tokio::test]
    async fn other_sessions_are_independent() {
        let reg = LiveRegistry::default();
        let _a = reg.try_claim(Uuid::now_v7()).unwrap();
        assert!(reg.try_claim(Uuid::now_v7()).is_some());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test live::`
Expected: compile error `cannot find type LiveRegistry`.

- [ ] **Step 3: Implement**

Put above the tests in `live.rs`:

```rust
//! Which sessions currently have a connected socket. Deliberately in memory,
//! never a DB column: a crash must not leave a session stuck "live".

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct LiveRegistry {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    live: Mutex<HashSet<Uuid>>,
    released: Notify,
}

/// The session stayed live for the whole wait.
#[derive(Debug, PartialEq, Eq)]
pub struct Busy;

impl LiveRegistry {
    pub fn is_live(&self, id: Uuid) -> bool {
        self.inner.live.lock().unwrap().contains(&id)
    }

    pub fn try_claim(&self, id: Uuid) -> Option<LiveGuard> {
        let fresh = self.inner.live.lock().unwrap().insert(id);
        fresh.then(|| LiveGuard { registry: self.clone(), id })
    }

    /// Claims `id`, waiting up to `wait` for a previous holder to release it.
    /// A reconnect usually races the old connection's final flush, which
    /// takes about one decode.
    pub async fn claim(&self, id: Uuid, wait: Duration) -> Result<LiveGuard, Busy> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Register for the wake-up *before* checking, so a release
            // between the check and the await is not missed.
            let released = self.inner.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(guard) = self.try_claim(id) {
                return Ok(guard);
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return Err(Busy);
            }
        }
    }
}

#[must_use = "dropping the guard immediately releases the session"]
pub struct LiveGuard {
    registry: LiveRegistry,
    id: Uuid,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.registry.inner.live.lock().unwrap().remove(&self.id);
        self.registry.inner.released.notify_waiters();
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test live::`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/live.rs crates/server/src/main.rs
git commit -m "feat(server): add in-memory live session registry"
```

---

### Task 3: Session lifecycle and audio file layout (`sessions.rs`)

**Files:**
- Create: `crates/server/src/sessions.rs`
- Modify: `crates/server/src/main.rs` (add `mod sessions;`)

**Interfaces:**
- Consumes: `db::*` (Task 1), `LiveRegistry`, `LiveGuard`, `Busy` (Task 2), `ServerMessage::error` (existing).
- Produces (`crate::sessions`):
  - `pub fn session_dir(data_dir: &Path, session_id: Uuid) -> PathBuf`
  - `pub fn recording_path(data_dir: &Path, session_id: Uuid, recording_id: Uuid) -> PathBuf`
  - `pub fn pcm_duration_ms(bytes: u64, sample_rate: u32) -> i64`
  - `pub async fn file_len(path: &Path) -> u64` (0 if missing)
  - `pub async fn repair(pool: &PgPool, data_dir: &Path, session_id: Uuid) -> anyhow::Result<()>`
  - `pub async fn delete_files(data_dir: &Path, session_id: Uuid) -> std::io::Result<()>`
  - `pub struct OpenRequest<'a> { pub session_id: Option<Uuid>, pub language: &'a str, pub sample_rate: u32 }`
  - `pub struct OpenedSession { pub session_id: Uuid, pub resumed: bool, pub recording: db::Recording, pub next_paragraph: i32, pub guard: LiveGuard }`
  - `#[derive(Debug)] pub enum OpenError { NotFound, Busy, Internal(anyhow::Error) }` with `fn to_message(&self) -> ServerMessage`
  - `pub async fn open(pool: &PgPool, live: &LiveRegistry, data_dir: &Path, req: OpenRequest<'_>, wait: Duration) -> Result<OpenedSession, OpenError>`

- [ ] **Step 1: Write the failing tests**

Create `crates/server/src/sessions.rs` with only this test module; add `mod sessions;` to `main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn req(session_id: Option<Uuid>) -> OpenRequest<'static> {
        OpenRequest { session_id, language: "en", sample_rate: 16_000 }
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
        let opened = open(&pool, &live, dir.path(), req(None), Duration::from_millis(10))
            .await
            .unwrap();
        assert!(!opened.resumed);
        assert_eq!((opened.recording.ordinal, opened.recording.offset_ms), (0, 0));
        assert!(live.is_live(opened.session_id));
        assert!(db::get_session(&pool, opened.session_id).await.unwrap().is_some());
        drop(opened);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_repairs_unclosed_recording_from_file_size(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let first = open(&pool, &live, dir.path(), req(None), Duration::from_millis(10))
            .await
            .unwrap();
        let sid = first.session_id;
        // Simulate a crash: 1 s of audio on disk, recording never finished.
        let path = recording_path(dir.path(), sid, first.recording.id);
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        tokio::fs::write(&path, vec![0u8; 32_000]).await.unwrap();
        drop(first);

        let second = open(&pool, &live, dir.path(), req(Some(sid)), Duration::from_millis(10))
            .await
            .unwrap();
        assert!(second.resumed);
        assert_eq!((second.recording.ordinal, second.recording.offset_ms), (1, 1_000));
        let recs = db::list_recordings(&pool, sid).await.unwrap();
        assert_eq!(recs[0].duration_ms, Some(1_000));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_of_unknown_session_is_not_found(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let err = open(&pool, &live, dir.path(), req(Some(Uuid::now_v7())), Duration::from_millis(10))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, OpenError::NotFound));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn resume_of_live_session_is_busy_after_waiting(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let first = open(&pool, &live, dir.path(), req(None), Duration::from_millis(10))
            .await
            .unwrap();
        let err = open(&pool, &live, dir.path(), req(Some(first.session_id)), Duration::from_millis(50))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, OpenError::Busy));
        // The refused resume must not have created a recording.
        assert_eq!(db::list_recordings(&pool, first.session_id).await.unwrap().len(), 1);
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
        assert_eq!(code(OpenError::Internal(anyhow::anyhow!("x"))), "init_failed");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test sessions::`
Expected: compile errors (`cannot find function open`, …).

- [ ] **Step 3: Implement**

Put above the tests:

```rust
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
    tokio::fs::metadata(path).await.map(|m| m.len()).unwrap_or(0)
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
            OpenError::NotFound => ServerMessage::error("session_not_found", "no session with that id"),
            OpenError::Busy => ServerMessage::error(
                "session_busy",
                "session is live on another connection",
            ),
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
            let guard = live.try_claim(id).expect("a fresh UUIDv7 cannot already be live");
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
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test sessions::`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/sessions.rs crates/server/src/main.rs
git commit -m "feat(server): open and resume sessions with crash repair"
```

---

### Task 4: Recorder task

**Files:**
- Create: `crates/server/src/recorder.rs`
- Modify: `crates/server/src/main.rs` (add `mod recorder;`)

**Interfaces:**
- Consumes: `db::{insert_chunks, finish_recording, Chunk}`, `sessions::{recording_path, pcm_duration_ms, OpenedSession}`, `LiveGuard`, `ServerMessage::error`.
- Produces (`crate::recorder`):
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub enum RecorderEvent { Commit { text: String, start_ms: u64, end_ms: u64 }, EndOfSegment }`
  - `pub struct Recorder`
  - `Recorder::create(pool: PgPool, data_dir: &Path, opened: OpenedSession, notify: mpsc::Sender<ServerMessage>) -> anyhow::Result<Recorder>`
  - `Recorder::run(self, audio: mpsc::Receiver<bytes::Bytes>, events: mpsc::UnboundedReceiver<RecorderEvent>)` — async, returns `()` once **both** channels close and everything is flushed; drops the live guard last.

- [ ] **Step 1: Write the failing tests**

Create `crates/server/src/recorder.rs` with only the tests; add `mod recorder;` to `main.rs`:

```rust
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
            OpenRequest { session_id, language: "en", sample_rate: 16_000 },
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let ids = (opened.session_id, opened.recording.id);
        let (notify_tx, notify_rx) = mpsc::channel(4);
        let recorder = Recorder::create(pool.clone(), dir, opened, notify_tx).await.unwrap();
        (ids.0, ids.1, recorder, notify_rx)
    }

    fn commit(text: &str, start_ms: u64, end_ms: u64) -> RecorderEvent {
        RecorderEvent::Commit { text: text.into(), start_ms, end_ms }
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn persists_audio_and_chunks_on_the_session_timeline(pool: PgPool) {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();

        // A first, already-finished 1 s recording so the offset is visible.
        let sid = Uuid::now_v7();
        db::create_session(&pool, sid, "en").await.unwrap();
        let r0 = db::begin_recording(&pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        db::finish_recording(&pool, r0.recording.id, 1_000).await.unwrap();

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
                db::Chunk { paragraph: 0, text: "hello".into(), start_ms: 1_000, end_ms: 1_120 },
                db::Chunk { paragraph: 0, text: "world".into(), start_ms: 1_120, end_ms: 1_180 },
                db::Chunk { paragraph: 1, text: "again".into(), start_ms: 1_190, end_ms: 1_200 },
            ]
        );
        let recs = db::list_recordings(&pool, sid).await.unwrap();
        assert_eq!(recs[1].duration_ms, Some(200));
        assert!(recs[1].ended_at.is_some());
        assert_eq!(db::get_session(&pool, sid).await.unwrap().unwrap().duration_ms, 1_200);
        let audio = tokio::fs::read(sessions::recording_path(dir.path(), sid, rec_id)).await.unwrap();
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
        // Poll for the batch while the recorder is still running.
        let mut seen = 0;
        for _ in 0..100 {
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
        let audio = tokio::fs::read(sessions::recording_path(dir.path(), sid, rec_id)).await.unwrap();
        assert_eq!(audio.len(), 3_200, "audio is kept even when Postgres is down");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test recorder::`
Expected: compile errors (`cannot find type Recorder`, …).

- [ ] **Step 3: Implement**

Put above the tests:

```rust
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
    Commit { text: String, start_ms: u64, end_ms: u64 },
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
    file: BufWriter<File>,
    bytes_written: u64,
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
        let file = OpenOptions::new().create(true).append(true).open(&path).await?;
        Ok(Self {
            pool,
            session_id: opened.session_id,
            recording_id: opened.recording.id,
            offset_ms: opened.recording.offset_ms,
            sample_rate: opened.recording.sample_rate as u32,
            paragraph: opened.next_paragraph,
            paragraph_has_text: false,
            file: BufWriter::new(file),
            bytes_written: 0,
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
        mut audio: mpsc::Receiver<Bytes>,
        mut events: mpsc::UnboundedReceiver<RecorderEvent>,
    ) {
        let mut audio_open = true;
        let mut events_open = true;
        let mut flush_at: Option<Instant> = None;

        while audio_open || events_open {
            tokio::select! {
                frame = audio.recv(), if audio_open => match frame {
                    Some(bytes) => self.write_audio(&bytes).await,
                    None => audio_open = false,
                },
                event = events.recv(), if events_open => match event {
                    Some(RecorderEvent::Commit { text, start_ms, end_ms }) => {
                        self.rows.push(Chunk {
                            paragraph: self.paragraph,
                            text,
                            start_ms: self.offset_ms + start_ms as i64,
                            end_ms: self.offset_ms + end_ms as i64,
                        });
                        self.paragraph_has_text = true;
                        if self.rows.len() >= FLUSH_ROWS {
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
                        flush_at = self.flush().await;
                    }
                    None => events_open = false,
                },
                () = sleep_until(flush_at) => {
                    flush_at = self.flush().await;
                }
            }
        }
        self.close().await;
    }

    async fn write_audio(&mut self, bytes: &[u8]) {
        match self.file.write_all(bytes).await {
            Ok(()) => self.bytes_written += bytes.len() as u64,
            Err(e) => tracing::warn!(error = %e, "writing recording audio failed"),
        }
    }

    /// Writes buffered rows. Returns when to try again if they could not be
    /// written, `None` when nothing is left.
    async fn flush(&mut self) -> Option<Instant> {
        if self.rows.is_empty() {
            return None;
        }
        let attempts = if self.failing { 1 } else { RETRY_BACKOFF.len() + 1 };
        for attempt in 0..attempts {
            match db::insert_chunks(&self.pool, self.session_id, self.recording_id, &self.rows).await
            {
                Ok(()) => {
                    self.rows.clear();
                    self.failing = false;
                    return None;
                }
                Err(e) => {
                    tracing::warn!(error = %e, attempt, "persisting transcript chunks failed");
                    if attempt + 1 < attempts {
                        tokio::time::sleep(RETRY_BACKOFF[attempt]).await;
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

    async fn close(mut self) {
        // The final flush gets the full retry budget again.
        self.failing = false;
        if self.flush().await.is_some() {
            tracing::error!(rows = self.rows.len(), "dropping unsaved transcript chunks");
        }
        if let Err(e) = self.file.flush().await {
            tracing::warn!(error = %e, "flushing recording audio failed");
        }
        if let Err(e) = self.file.get_ref().sync_all().await {
            tracing::warn!(error = %e, "syncing recording audio failed");
        }
        let duration_ms = sessions::pcm_duration_ms(self.bytes_written, self.sample_rate);
        if let Err(e) = db::finish_recording(&self.pool, self.recording_id, duration_ms).await {
            tracing::warn!(error = %e, "finishing recording failed; it will be repaired on next resume");
        }
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test recorder::`
Expected: 3 passed. `db_outage_reports_once_and_still_releases` takes ~4–5 s (two full retry budgets); that is expected.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/recorder.rs crates/server/src/main.rs
git commit -m "feat(server): add per-connection recorder for audio and transcript"
```

---

### Task 5: Wire sessions into the protocol, pipeline and WebSocket handler

**Files:**
- Modify: `crates/server/src/protocol.rs`, `crates/server/src/pipeline.rs`, `crates/server/src/ws.rs`, `crates/server/src/main.rs`
- Create: `scripts/smoke-ws.mjs`

**Interfaces:**
- Consumes: `sessions::{open, OpenRequest}`, `Recorder`, `RecorderEvent`, `LiveRegistry`, `db::connect`.
- Produces:
  - `StartParams.session_id: Option<Uuid>` (wire `sessionId`)
  - `ServerMessage::Ready { session_id: Uuid, resumed: bool, offset_ms: i64 }` (wire `{"type":"ready","sessionId":…,"resumed":…,"offsetMs":…}`)
  - `Pipeline::new(cfg, engine, &params, out, recorder: mpsc::UnboundedSender<RecorderEvent>)`
  - `AppState { cfg, engine, slots, db: PgPool, live: LiveRegistry, shutdown: CancellationToken, tracker: TaskTracker }`, `AppState::new(cfg, engine, db)`

- [ ] **Step 1: Write the failing protocol tests**

Append to the `tests` module in `crates/server/src/protocol.rs`:

```rust
    #[test]
    fn start_accepts_optional_session_id() {
        let with: ClientMessage = serde_json::from_str(
            r#"{"type":"start","sessionId":"0192f0c1-0000-7000-8000-000000000001"}"#,
        )
        .unwrap();
        let ClientMessage::Start(p) = with else { panic!("expected start") };
        assert_eq!(
            p.session_id.map(|u| u.to_string()).as_deref(),
            Some("0192f0c1-0000-7000-8000-000000000001")
        );

        let without: ClientMessage = serde_json::from_str(r#"{"type":"start"}"#).unwrap();
        let ClientMessage::Start(p) = without else { panic!("expected start") };
        assert!(p.session_id.is_none());
    }

    #[test]
    fn rejects_malformed_session_id() {
        assert!(serde_json::from_str::<ClientMessage>(r#"{"type":"start","sessionId":"nope"}"#).is_err());
    }

    #[test]
    fn ready_carries_session_fields() {
        let id: uuid::Uuid = "0192f0c1-0000-7000-8000-000000000001".parse().unwrap();
        let json = serde_json::to_string(&ServerMessage::Ready {
            session_id: id,
            resumed: true,
            offset_ms: 1_500,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"ready","sessionId":"0192f0c1-0000-7000-8000-000000000001","resumed":true,"offsetMs":1500}"#
        );
    }
```

Run: `cargo test protocol::`
Expected: compile errors (`no field session_id`, `Ready` is a unit variant).

- [ ] **Step 2: Update `protocol.rs`**

Add `use uuid::Uuid;` under the serde import. In `StartParams` add:

```rust
    /// Resume this session instead of creating a new one.
    #[serde(default)]
    pub session_id: Option<Uuid>,
```
In `impl Default for StartParams` add `session_id: None,`. Replace the `Ready` variant with:

```rust
    /// Handshake accepted, model warm, start sending audio.
    Ready {
        session_id: Uuid,
        /// True when this connection continues an existing session.
        resumed: bool,
        /// Where this connection starts on the session timeline. Wire
        /// timestamps stay relative to this connection's stream.
        offset_ms: i64,
    },
```

Run: `cargo test protocol::` — `ws.rs` will not compile yet (`ServerMessage::Ready` usage). Continue to Step 3 before running.

- [ ] **Step 3: Forward commits from the pipeline**

In `crates/server/src/pipeline.rs`:

1. Add `use crate::recorder::RecorderEvent;` to the imports.
2. Add a field to `Pipeline` after `out`:
```rust
    /// Persistence feed. Unbounded on purpose: the pipeline must never wait
    /// on the database.
    recorder: mpsc::UnboundedSender<RecorderEvent>,
```
3. Add a parameter to `Pipeline::new` after `out: mpsc::Sender<ServerMessage>,`:
```rust
        recorder: mpsc::UnboundedSender<RecorderEvent>,
```
   and add `recorder,` to the struct literal after `out,`.
4. In `emit()`, insert right after the second early `return` block (before `self.seq += 1;`):
```rust
        if let Some(commit) = &outcome.commit {
            let _ = self.recorder.send(RecorderEvent::Commit {
                text: commit.text.clone(),
                start_ms: commit.start,
                end_ms: commit.end,
            });
        }
        if end_of_segment {
            let _ = self.recorder.send(RecorderEvent::EndOfSegment);
        }
```

- [ ] **Step 4: Rewrite `ws.rs`**

Replace the whole of `crates/server/src/ws.rs` with:

```rust
//! WebSocket handler. One socket per connection, four concurrent tasks:
//! receiver (audio in), pipeline (VAD + scheduling + stabilization), sender
//! (transcript out + keepalive pings) and recorder (persistence).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use sqlx::PgPool;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Config;
use crate::live::LiveRegistry;
use crate::pipeline::Pipeline;
use crate::protocol::{ClientMessage, ServerMessage, StartParams};
use crate::recorder::Recorder;
use crate::sessions::{self, OpenRequest};
use crate::whisper::WhisperEngine;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSCRIPT_QUEUE: usize = 64;
/// How long a resuming connection waits for the previous connection on the
/// same session to finish flushing and release it.
const RESUME_WAIT: Duration = Duration::from_secs(5);
const PING_EVERY: Duration = Duration::from_secs(15);
/// No inbound frame (audio, control or pong) for this long means the peer is
/// gone; closing releases the session's live claim.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub engine: Arc<WhisperEngine>,
    /// Admission control: capacity is what the inference pool can sustain at
    /// realtime. Beyond it we reject rather than make everyone slow.
    pub slots: Arc<Semaphore>,
    pub db: PgPool,
    pub live: LiveRegistry,
    /// Cancelled on Ctrl-C; receivers end their streams as if the client
    /// had sent `stop`.
    pub shutdown: CancellationToken,
    /// Tracks connection futures so shutdown can wait for them to flush.
    pub tracker: TaskTracker,
}

impl AppState {
    pub fn new(cfg: Arc<Config>, engine: Arc<WhisperEngine>, db: PgPool) -> Self {
        let slots = Arc::new(Semaphore::new(cfg.max_connections));
        Self {
            cfg,
            engine,
            slots,
            db,
            live: LiveRegistry::default(),
            shutdown: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
    }
}

pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Ok(permit) = Arc::clone(&state.slots).try_acquire_owned() else {
        tracing::warn!("rejecting connection: at capacity");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "transcription capacity reached",
        )
            .into_response();
    };
    let tracker = state.tracker.clone();
    ws.on_upgrade(move |socket| {
        tracker.track_future(async move {
            if let Err(e) = serve(socket, state).await {
                tracing::warn!(error = %e, "connection ended with error");
            }
            drop(permit);
        })
    })
}

async fn serve(socket: WebSocket, state: AppState) -> anyhow::Result<()> {
    let (mut sink, mut stream) = socket.split();

    // --- handshake ---------------------------------------------------------
    let params = match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_start(&mut stream)).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("bad_handshake", e.to_string()),
            )
            .await;
            return Ok(());
        }
        Err(_) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("handshake_timeout", "no start message"),
            )
            .await;
            return Ok(());
        }
    };

    if params.encoding != "pcm_s16le" {
        let _ = send(
            &mut sink,
            &ServerMessage::error(
                "unsupported_encoding",
                format!("expected pcm_s16le, got {}", params.encoding),
            ),
        )
        .await;
        return Ok(());
    }

    let (out_tx, mut out_rx) = mpsc::channel::<ServerMessage>(TRANSCRIPT_QUEUE);
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let pipeline = match Pipeline::new(
        Arc::clone(&state.cfg),
        Arc::clone(&state.engine),
        &params,
        out_tx.clone(),
        events_tx,
    ) {
        Ok(p) => p,
        Err(e) => {
            let _ = send(
                &mut sink,
                &ServerMessage::error("init_failed", e.to_string()),
            )
            .await;
            return Ok(());
        }
    };

    // --- session -----------------------------------------------------------
    let language = params.language_or_default();
    let opened = match sessions::open(
        &state.db,
        &state.live,
        &state.cfg.data_dir,
        OpenRequest {
            session_id: params.session_id,
            language: &language,
            sample_rate: params.sample_rate,
        },
        RESUME_WAIT,
    )
    .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::info!(error = ?e, "session open refused");
            let _ = send(&mut sink, &e.to_message()).await;
            return Ok(());
        }
    };
    let session_id = opened.session_id;
    let ready = ServerMessage::Ready {
        session_id,
        resumed: opened.resumed,
        offset_ms: opened.recording.offset_ms,
    };

    // --- task 4: recorder --------------------------------------------------
    let recorder =
        match Recorder::create(state.db.clone(), &state.cfg.data_dir, opened, out_tx.clone()).await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = send(
                    &mut sink,
                    &ServerMessage::error("init_failed", e.to_string()),
                )
                .await;
                return Ok(());
            }
        };
    let (rec_audio_tx, rec_audio_rx) = mpsc::channel::<Bytes>(state.cfg.audio_queue);
    let recorder_task = tokio::spawn(recorder.run(rec_audio_rx, events_rx));

    send(&mut sink, &ready).await?;
    tracing::info!(%session_id, sample_rate = params.sample_rate, %language, "stream started");

    // --- task 3: sender ----------------------------------------------------
    let sender = tokio::spawn(async move {
        let mut ping = tokio::time::interval_at(Instant::now() + PING_EVERY, PING_EVERY);
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    let Some(msg) = msg else { break };
                    let done = matches!(msg, ServerMessage::Done { .. });
                    if send(&mut sink, &msg).await.is_err() {
                        break;
                    }
                    if done {
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // --- task 2: pipeline --------------------------------------------------
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(state.cfg.audio_queue);
    let pipeline_task = tokio::spawn(pipeline.run(audio_rx));

    // --- task 1: receiver --------------------------------------------------
    let mut idle_deadline = Instant::now() + IDLE_TIMEOUT;
    loop {
        let msg = tokio::select! {
            msg = stream.next() => msg,
            () = tokio::time::sleep_until(idle_deadline) => {
                tracing::info!(%session_id, "peer idle; closing");
                break;
            }
            () = state.shutdown.cancelled() => {
                tracing::info!(%session_id, "server shutting down; ending stream");
                break;
            }
        };
        let Some(msg) = msg else { break };
        idle_deadline = Instant::now() + IDLE_TIMEOUT;
        match msg {
            Ok(Message::Binary(data)) => {
                // Await, don't try_send: backpressure the socket rather than
                // ever dropping input audio.
                if audio_tx.send(data.to_vec()).await.is_err() {
                    break;
                }
                // A failed recorder must not end transcription; it has
                // already logged why.
                let _ = rec_audio_tx.send(data).await;
            }
            Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Stop) => break,
                Ok(ClientMessage::Start(_)) => {
                    tracing::debug!("ignoring duplicate start");
                }
                Err(e) => {
                    let _ = out_tx
                        .send(ServerMessage::error("bad_control", e.to_string()))
                        .await;
                }
            },
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(error = %e, "websocket receive error");
                break;
            }
        }
    }

    // Order matters: the pipeline's final flush queues its last commits to
    // the recorder, the recorder then writes them and releases the session,
    // and only then does the sender run dry and close the socket.
    drop(audio_tx);
    let _ = pipeline_task.await;
    drop(rec_audio_tx);
    let _ = recorder_task.await;
    drop(out_tx);
    let _ = sender.await;
    tracing::info!(%session_id, "stream finished");
    Ok(())
}

async fn read_start(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
) -> anyhow::Result<StartParams> {
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Text(text) => match serde_json::from_str::<ClientMessage>(&text)? {
                ClientMessage::Start(p) => return Ok(p),
                ClientMessage::Stop => anyhow::bail!("stop before start"),
            },
            Message::Binary(_) => anyhow::bail!("audio received before start handshake"),
            Message::Close(_) => anyhow::bail!("closed before start"),
            _ => continue,
        }
    }
    anyhow::bail!("stream ended before start")
}

async fn send(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &ServerMessage,
) -> anyhow::Result<()> {
    let json = serde_json::to_string(msg)?;
    sink.send(Message::Text(Utf8Bytes::from(json))).await?;
    Ok(())
}
```

- [ ] **Step 5: Wire state and graceful shutdown in `main.rs`**

1. If Task 1 named it `_db`, rename back to `db`.
2. Replace `let state = AppState::new(Arc::clone(&cfg), Arc::clone(&engine));` with:
```rust
    let state = AppState::new(Arc::clone(&cfg), Arc::clone(&engine), db.clone());
```
3. Change `.with_state(state);` on the router to `.with_state(state.clone());`.
4. Replace the `axum::serve(...)...;` statement and the trailing `Ok(())` with:
```rust
    axum::serve(listener, app)
        .with_graceful_shutdown({
            let token = state.shutdown.clone();
            async move {
                shutdown_signal().await;
                token.cancel();
            }
        })
        .await
        .context("server error")?;

    // Upgraded sockets outlive axum's graceful shutdown; give their pipelines
    // and recorders a bounded window to flush.
    state.tracker.close();
    if tokio::time::timeout(SHUTDOWN_GRACE, state.tracker.wait())
        .await
        .is_err()
    {
        tracing::warn!("shutdown grace elapsed with streams still open");
    }

    Ok(())
```
5. Add near the imports: `const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);`

- [ ] **Step 6: Run all tests**

Run: `cargo test`
Expected: all pass (protocol now 5 tests). Then `cargo sqlx prepare --workspace` (no SQL changed, but keeps `.sqlx` honest) — expect no diff in `git status .sqlx`.

- [ ] **Step 7: Write the smoke script**

Create `scripts/smoke-ws.mjs`:

```js
// End-to-end check of the session handshake against a running server.
//   node scripts/smoke-ws.mjs            -> new session
//   node scripts/smoke-ws.mjs <uuid>     -> resume that session
// Streams 2.048 s of silence, then stops. Prints every server message.
const url = process.env.WS_URL ?? 'ws://127.0.0.1:8080/ws';
const sessionId = process.argv[2];

const ws = new WebSocket(url);
ws.binaryType = 'arraybuffer';

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'start',
    sampleRate: 16000,
    encoding: 'pcm_s16le',
    language: 'en',
    ...(sessionId ? { sessionId } : {}),
  }));
};

ws.onmessage = async (ev) => {
  const msg = JSON.parse(ev.data);
  console.log(JSON.stringify(msg));
  if (msg.type === 'ready') {
    const frame = new Uint8Array(4096); // 128 ms of 16 kHz Int16 silence
    for (let i = 0; i < 16; i++) {
      ws.send(frame);
      await new Promise((r) => setTimeout(r, 128));
    }
    ws.send(JSON.stringify({ type: 'stop' }));
  }
};

ws.onclose = (ev) => {
  console.log(`closed ${ev.code}`);
  process.exit(0);
};
ws.onerror = (ev) => {
  console.error('websocket error', ev.message ?? ev);
  process.exit(1);
};
```

- [ ] **Step 8: Smoke-test end to end**

```bash
cargo run --release --features metal -- --model models/ggml-base.en-q5_1.bin   # terminal 1
node scripts/smoke-ws.mjs                                                       # terminal 2
```
Expected output in terminal 2:
```
{"type":"ready","sessionId":"<uuid>","resumed":false,"offsetMs":0}
{"type":"done","text":""}
closed 1005
```
(The close code may be 1000 or 1005.) Then resume it:
```bash
node scripts/smoke-ws.mjs <uuid>
```
Expected: `{"type":"ready","sessionId":"<uuid>","resumed":true,"offsetMs":2048}`. Check the database and files:
```bash
psql transcribe_rust -Atc "select ordinal, offset_ms, duration_ms from recordings where session_id = '<uuid>' order by ordinal"
ls -l data/sessions/<uuid>/
node scripts/smoke-ws.mjs not-a-uuid
```
Expected: `0|0|2048` and `1|2048|2048`; two 65536-byte `.pcm` files; the last command prints `{"type":"error","code":"bad_handshake",...}`. Stop the server with Ctrl-C and confirm it exits within 5 s.

- [ ] **Step 9: Commit**

```bash
git add crates/server/src scripts/smoke-ws.mjs .sqlx
git commit -m "feat(server): persist sessions from the WebSocket handler with resume and keepalive"
```

---

### Task 6: Export formats (`export.rs`)

**Files:**
- Create: `crates/server/src/export.rs`
- Modify: `crates/server/src/main.rs` (add `mod export;`)

**Interfaces:**
- Consumes: `db::Chunk`.
- Produces (`crate::export`):
  - `pub struct ChunkView { text: String, start_ms: i64, end_ms: i64 }` (Serialize camelCase)
  - `pub struct Paragraph { index: i32, start_ms: i64, end_ms: i64, text: String, chunks: Vec<ChunkView> }` (Serialize camelCase)
  - `pub struct Cue { start_ms: i64, end_ms: i64, text: String }`
  - `paragraphs(&[Chunk]) -> Vec<Paragraph>`, `build_cues(&[Paragraph]) -> Vec<Cue>`, `wrap(&str) -> String`, `to_txt(&[Paragraph]) -> String`, `to_srt(&[Cue]) -> String`, `to_vtt(&[Cue]) -> String`, `wav_header(sample_rate: u32, data_len: u32) -> [u8; 44]`, `filename(title: Option<&str>, created_at: DateTime<Utc>, ext: &str) -> String`

- [ ] **Step 1: Write the failing tests**

Create `crates/server/src/export.rs` with only the tests; add `mod export;` to `main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn chunk(paragraph: i32, text: &str, start_ms: i64, end_ms: i64) -> Chunk {
        Chunk { paragraph, text: text.into(), start_ms, end_ms }
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
                Cue { start_ms: 0, end_ms: 6_000, text: "a b".into() },
                Cue { start_ms: 6_000, end_ms: 7_500, text: "c".into() },
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
            Cue { start_ms: 0, end_ms: 1_500, text: "hello there general".into() },
            Cue { start_ms: 3_723_045, end_ms: 3_724_000, text: "kenobi".into() },
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
        let ps = paragraphs(&[chunk(0, "a", 0, 1), chunk(0, "b", 1, 2), chunk(1, "c", 3, 4)]);
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
        assert_eq!(u32::from_le_bytes(h[28..32].try_into().unwrap()), 32_000, "byte rate");
        assert_eq!(u16::from_le_bytes(h[34..36].try_into().unwrap()), 16, "bits");
        assert_eq!(&h[36..40], b"data");
        assert_eq!(u32::from_le_bytes(h[40..44].try_into().unwrap()), 32_000);
    }

    #[test]
    fn filename_is_ascii_safe() {
        let at = Utc.with_ymd_and_hms(2026, 10, 3, 14, 32, 0).unwrap();
        assert_eq!(filename(Some("Team Standup: Q4!"), at, "srt"), "team-standup-q4.srt");
        assert_eq!(filename(Some(r#"say "hi""#), at, "txt"), "say-hi.txt");
        assert_eq!(filename(Some("会議"), at, "wav"), "session-2026-10-03-1432.wav");
        assert_eq!(filename(None, at, "json"), "session-2026-10-03-1432.json");
        let long = filename(Some(&"word ".repeat(30)), at, "txt");
        assert!(long.len() <= 60 + ".txt".len());
        assert!(!long.contains("-.txt"));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test export::`
Expected: compile errors.

- [ ] **Step 3: Implement**

Put above the tests:

```rust
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
        let view = ChunkView { text: c.text.clone(), start_ms: c.start_ms, end_ms: c.end_ms };
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
                    current = Some(Cue { start_ms: c.start_ms, end_ms: c.end_ms, text: c.text.clone() });
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
    let mut out = paragraphs.iter().map(|p| p.text.as_str()).collect::<Vec<_>>().join("\n\n");
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
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test export::`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/export.rs crates/server/src/main.rs
git commit -m "feat(server): add txt, srt, vtt and wav export formatting"
```

---

### Task 7: REST API

**Files:**
- Create: `crates/server/src/api.rs`
- Modify: `crates/server/src/main.rs` (add `mod api;`, nest router)

**Interfaces:**
- Consumes: `db::*`, `export::*`, `sessions::{repair, delete_files, recording_path}`, `LiveRegistry`.
- Produces:
  - `#[derive(Clone)] pub struct ApiState { pub db: PgPool, pub live: LiveRegistry, pub data_dir: PathBuf }`
  - `pub fn router<S: Clone + Send + Sync + 'static>(state: ApiState) -> Router<S>` with routes (relative to the `/api` nest):
    - `GET /sessions?limit&before` → `[{id,title,language,durationMs,createdAt,updatedAt,preview,live}]`
    - `GET /sessions/{id}` → `{…session, live, recordings:[…], paragraphs:[{index,startMs,endMs,text}]}`
    - `PATCH /sessions/{id}` body `{title}` → `{…session, live}`
    - `DELETE /sessions/{id}` → 204 / 404 / 409 `session_live`
    - `GET /sessions/{id}/export/{format}` (`txt|srt|vtt|json|wav`)
  - Error body: `{"error":{"code":"…","message":"…"}}`

- [ ] **Step 1: Write the failing tests**

Create `crates/server/src/api.rs` with only the tests; add `mod api;` to `main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{HeaderMap, Request};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::path::Path as FsPath;
    use tower::ServiceExt;

    struct Fixture {
        app: Router,
        pool: PgPool,
        live: LiveRegistry,
        dir: tempfile::TempDir,
    }

    fn fixture(pool: PgPool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let app = router(ApiState { db: pool.clone(), live: live.clone(), data_dir: dir.path().into() });
        Fixture { app, pool, live, dir }
    }

    async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, HeaderMap, Bytes) {
        let mut req = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, headers, bytes)
    }

    fn json(bytes: &Bytes) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    /// Session with one closed 3 s recording and two paragraphs.
    async fn seed(pool: &PgPool, dir: &FsPath, title: Option<&str>) -> Uuid {
        let sid = Uuid::now_v7();
        db::create_session(pool, sid, "en").await.unwrap();
        if let Some(t) = title {
            db::update_title(pool, sid, Some(t)).await.unwrap();
        }
        let r = db::begin_recording(pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        db::insert_chunks(
            pool,
            sid,
            r.recording.id,
            &[
                db::Chunk { paragraph: 0, text: "hello there".into(), start_ms: 0, end_ms: 900 },
                db::Chunk { paragraph: 0, text: "general".into(), start_ms: 900, end_ms: 1_500 },
                db::Chunk { paragraph: 1, text: "kenobi".into(), start_ms: 2_000, end_ms: 2_600 },
            ],
        )
        .await
        .unwrap();
        let path = sessions::recording_path(dir, sid, r.recording.id);
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        tokio::fs::write(&path, vec![0u8; 96_000]).await.unwrap();
        db::finish_recording(pool, r.recording.id, 3_000).await.unwrap();
        sid
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn list_pages_newest_first(pool: PgPool) {
        let f = fixture(pool);
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(seed(&f.pool, f.dir.path(), None).await);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let (status, _, body) = call(&f.app, "GET", "/sessions?limit=2", None).await;
        assert_eq!(status, StatusCode::OK);
        let page = json(&body);
        assert_eq!(page[0]["id"], ids[2].to_string());
        assert_eq!(page[1]["id"], ids[1].to_string());
        assert_eq!(page[0]["preview"], "hello there general kenobi");
        assert_eq!(page[0]["live"], false);
        assert_eq!(page[0]["durationMs"], 3_000);

        let (_, _, body) = call(&f.app, "GET", &format!("/sessions?limit=2&before={}", ids[1]), None).await;
        let next = json(&body);
        assert_eq!(next.as_array().unwrap().len(), 1);
        assert_eq!(next[0]["id"], ids[0].to_string());
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn detail_groups_paragraphs(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let (status, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}"), None).await;
        assert_eq!(status, StatusCode::OK);
        let d = json(&body);
        assert_eq!(d["paragraphs"][0]["text"], "hello there general");
        assert_eq!(d["paragraphs"][0]["startMs"], 0);
        assert_eq!(d["paragraphs"][0]["endMs"], 1_500);
        assert_eq!(d["paragraphs"][1]["index"], 1);
        assert!(d["paragraphs"][0].get("chunks").is_none());
        assert_eq!(d["recordings"].as_array().unwrap().len(), 1);
        assert!(d["recordings"][0].get("sessionId").is_none());

        let (status, _, body) = call(&f.app, "GET", &format!("/sessions/{}", Uuid::now_v7()), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json(&body)["error"]["code"], "not_found");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn rename_trims_clears_and_validates(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let uri = format!("/sessions/{sid}");

        let (status, _, body) = call(&f.app, "PATCH", &uri, Some(serde_json::json!({"title": "  Standup  "}))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json(&body)["title"], "Standup");

        let (_, _, body) = call(&f.app, "PATCH", &uri, Some(serde_json::json!({"title": "   "}))).await;
        assert_eq!(json(&body)["title"], Value::Null);

        let (status, _, body) = call(&f.app, "PATCH", &uri, Some(serde_json::json!({"title": "x".repeat(201)}))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json(&body)["error"]["code"], "title_too_long");

        let (status, _, _) = call(&f.app, "PATCH", &format!("/sessions/{}", Uuid::now_v7()), Some(serde_json::json!({"title": "x"}))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn delete_refuses_live_then_removes_rows_and_files(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let uri = format!("/sessions/{sid}");

        let guard = f.live.try_claim(sid).unwrap();
        let (status, _, body) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(json(&body)["error"]["code"], "session_live");
        drop(guard);

        let (status, _, _) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(!sessions::session_dir(f.dir.path(), sid).exists());
        let (status, _, _) = call(&f.app, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _, _) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn exports_each_format(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), Some("Standup")).await;
        let get = |format: &'static str| {
            let app = f.app.clone();
            async move { call(&app, "GET", &format!("/sessions/{sid}/export/{format}"), None).await }
        };

        let (status, headers, body) = get("txt").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        assert_eq!(headers["content-disposition"], r#"attachment; filename="standup.txt""#);
        assert_eq!(&body[..], b"hello there general\n\nkenobi\n");

        let (_, headers, body) = get("srt").await;
        assert_eq!(headers["content-type"], "application/x-subrip; charset=utf-8");
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "1\n00:00:00,000 --> 00:00:01,500\nhello there general\n\n2\n00:00:02,000 --> 00:00:02,600\nkenobi\n\n"
        );

        let (_, headers, body) = get("vtt").await;
        assert_eq!(headers["content-type"], "text/vtt; charset=utf-8");
        assert!(body.starts_with(b"WEBVTT\n\n00:00:00.000 --> 00:00:01.500\n"));

        let (_, headers, body) = get("json").await;
        assert_eq!(headers["content-type"], "application/json");
        let doc = json(&body);
        assert_eq!(doc["session"]["title"], "Standup");
        assert_eq!(doc["paragraphs"][0]["chunks"].as_array().unwrap().len(), 2);
        assert_eq!(doc["recordings"][0]["durationMs"], 3_000);

        let (_, headers, body) = get("wav").await;
        assert_eq!(headers["content-type"], "audio/wav");
        assert_eq!(body.len(), 44 + 96_000);
        assert_eq!(&body[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(body[40..44].try_into().unwrap()), 96_000);

        let (status, _, _) = get("mp3").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn wav_export_repairs_unclosed_recording(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        // A crash during a second recording: 100 ms on disk, never finished.
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        tokio::fs::write(sessions::recording_path(f.dir.path(), sid, r.recording.id), vec![0u8; 3_200])
            .await
            .unwrap();

        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(body.len(), 44 + 96_000 + 3_200);
        assert_eq!(db::get_session(&f.pool, sid).await.unwrap().unwrap().duration_ms, 3_100);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn live_export_does_not_repair_and_drops_half_samples(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000).await.unwrap();
        tokio::fs::write(sessions::recording_path(f.dir.path(), sid, r.recording.id), vec![0u8; 3_201])
            .await
            .unwrap();
        let _guard = f.live.try_claim(sid).unwrap();

        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(body.len(), 44 + 96_000 + 3_200);
        let recs = db::list_recordings(&f.pool, sid).await.unwrap();
        assert_eq!(recs[1].duration_ms, None, "live recording left alone");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn empty_session_wav_is_header_only(pool: PgPool) {
        let f = fixture(pool);
        let sid = Uuid::now_v7();
        db::create_session(&f.pool, sid, "en").await.unwrap();
        let (status, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 44);
        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/txt"), None).await;
        assert!(body.is_empty());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test api::`
Expected: compile errors.

- [ ] **Step 3: Implement**

Put above the tests:

```rust
//! REST API over persisted sessions: browse, rename, delete, export.

use std::path::PathBuf;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::db;
use crate::export::{self, Paragraph};
use crate::live::LiveRegistry;
use crate::sessions;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;
const MAX_TITLE_CHARS: usize = 200;

#[derive(Clone)]
pub struct ApiState {
    pub db: PgPool,
    pub live: LiveRegistry,
    pub data_dir: PathBuf,
}

pub fn router<S>(state: ApiState) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/sessions", get(list_sessions))
        .route(
            "/sessions/{id}",
            get(get_session).patch(rename_session).delete(delete_session),
        )
        .route("/sessions/{id}/export/{format}", get(export_session))
        .with_state(state)
}

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    Conflict(&'static str, &'static str),
    Unprocessable(&'static str, String),
    Internal(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError::Internal(e.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not_found", "no such session".to_string()),
            ApiError::Conflict(code, message) => (StatusCode::CONFLICT, code, message.to_string()),
            ApiError::Unprocessable(code, message) => (StatusCode::UNPROCESSABLE_ENTITY, code, message),
            ApiError::Internal(e) => {
                tracing::error!(error = ?e, "api request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error".to_string())
            }
        };
        (status, Json(serde_json::json!({ "error": { "code": code, "message": message } })))
            .into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(Serialize)]
struct SessionSummary {
    #[serde(flatten)]
    item: db::SessionListItem,
    live: bool,
}

#[derive(Serialize)]
struct SessionView {
    #[serde(flatten)]
    session: db::Session,
    live: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ParagraphView {
    index: i32,
    start_ms: i64,
    end_ms: i64,
    text: String,
}

#[derive(Serialize)]
struct SessionDetail {
    #[serde(flatten)]
    session: SessionView,
    recordings: Vec<db::Recording>,
    paragraphs: Vec<ParagraphView>,
}

#[derive(Serialize)]
struct ExportDocument<'a> {
    session: &'a SessionView,
    recordings: &'a [db::Recording],
    paragraphs: &'a [Paragraph],
}

#[derive(Deserialize)]
struct ListParams {
    limit: Option<i64>,
    before: Option<Uuid>,
}

#[derive(Deserialize)]
struct RenameBody {
    title: Option<String>,
}

async fn list_sessions(
    State(st): State<ApiState>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<Vec<SessionSummary>>> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let items = db::list_sessions(&st.db, params.before, limit).await?;
    Ok(Json(
        items
            .into_iter()
            .map(|item| SessionSummary { live: st.live.is_live(item.id), item })
            .collect(),
    ))
}

async fn load(st: &ApiState, id: Uuid) -> ApiResult<SessionView> {
    let session = db::get_session(&st.db, id).await?.ok_or(ApiError::NotFound)?;
    Ok(SessionView { live: st.live.is_live(id), session })
}

async fn get_session(
    State(st): State<ApiState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<SessionDetail>> {
    let session = load(&st, id).await?;
    let recordings = db::list_recordings(&st.db, id).await?;
    let paragraphs = export::paragraphs(&db::list_chunks(&st.db, id).await?)
        .into_iter()
        .map(|p| ParagraphView { index: p.index, start_ms: p.start_ms, end_ms: p.end_ms, text: p.text })
        .collect();
    Ok(Json(SessionDetail { session, recordings, paragraphs }))
}

async fn rename_session(
    State(st): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(body): Json<RenameBody>,
) -> ApiResult<Json<SessionView>> {
    let title = body.title.as_deref().map(str::trim).filter(|t| !t.is_empty());
    if title.is_some_and(|t| t.chars().count() > MAX_TITLE_CHARS) {
        return Err(ApiError::Unprocessable(
            "title_too_long",
            format!("titles are limited to {MAX_TITLE_CHARS} characters"),
        ));
    }
    let session = db::update_title(&st.db, id, title).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(SessionView { live: st.live.is_live(id), session }))
}

async fn delete_session(State(st): State<ApiState>, Path(id): Path<Uuid>) -> ApiResult<StatusCode> {
    // Holding the claim while deleting stops a reconnect from resuming a
    // session that is halfway gone.
    let Some(_guard) = st.live.try_claim(id) else {
        return Err(ApiError::Conflict("session_live", "stop recording before deleting this session"));
    };
    if !db::delete_session(&st.db, id).await? {
        return Err(ApiError::NotFound);
    }
    sessions::delete_files(&st.data_dir, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn export_session(
    State(st): State<ApiState>,
    Path((id, format)): Path<(Uuid, String)>,
) -> ApiResult<Response> {
    if !matches!(format.as_str(), "txt" | "srt" | "vtt" | "json" | "wav") {
        return Err(ApiError::NotFound);
    }
    // Repair only when nobody is recording; a live recording's missing
    // duration is not damage.
    if let Some(_guard) = st.live.try_claim(id) {
        sessions::repair(&st.db, &st.data_dir, id).await?;
    }
    let session = load(&st, id).await?;
    let filename = export::filename(session.session.title.as_deref(), session.session.created_at, &format);

    let (content_type, body) = if format == "wav" {
        ("audio/wav", wav_body(&st, id).await?)
    } else {
        let paragraphs = export::paragraphs(&db::list_chunks(&st.db, id).await?);
        match format.as_str() {
            "txt" => ("text/plain; charset=utf-8", Body::from(export::to_txt(&paragraphs))),
            "srt" => (
                "application/x-subrip; charset=utf-8",
                Body::from(export::to_srt(&export::build_cues(&paragraphs))),
            ),
            "vtt" => (
                "text/vtt; charset=utf-8",
                Body::from(export::to_vtt(&export::build_cues(&paragraphs))),
            ),
            _ => {
                let recordings = db::list_recordings(&st.db, id).await?;
                let doc = ExportDocument { session: &session, recordings: &recordings, paragraphs: &paragraphs };
                ("application/json", Body::from(serde_json::to_vec_pretty(&doc)?))
            }
        }
    };

    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{filename}\"")),
        ],
        body,
    )
        .into_response())
}

/// Header plus every recording's PCM, streamed in order without buffering
/// the session in memory. Lengths are fixed up front so a live recording
/// growing during the download cannot desync the header.
async fn wav_body(st: &ApiState, id: Uuid) -> ApiResult<Body> {
    let recordings = db::list_recordings(&st.db, id).await?;
    let sample_rate = recordings.first().map_or(16_000, |r| r.sample_rate);
    if recordings.iter().any(|r| r.sample_rate != sample_rate) {
        return Err(ApiError::Unprocessable(
            "mixed_sample_rates",
            "recordings in this session use different sample rates".into(),
        ));
    }

    let mut parts: Vec<BoxStream<'static, std::io::Result<Bytes>>> = Vec::new();
    let mut total: u64 = 0;
    for rec in &recordings {
        let path = sessions::recording_path(&st.data_dir, id, rec.id);
        let len = match tokio::fs::metadata(&path).await {
            // Even length: a live recording may be mid-sample.
            Ok(meta) => meta.len() & !1,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "recording audio missing from export");
                continue;
            }
        };
        if len == 0 {
            continue;
        }
        let file = tokio::fs::File::open(&path).await?;
        total += len;
        parts.push(ReaderStream::new(file.take(len)).boxed());
    }

    let header = Bytes::copy_from_slice(&export::wav_header(
        sample_rate as u32,
        u32::try_from(total).unwrap_or(u32::MAX),
    ));
    let body = stream::once(async move { Ok::<_, std::io::Error>(header) })
        .chain(stream::iter(parts).flatten());
    Ok(Body::from_stream(body))
}
```

- [ ] **Step 4: Mount the API in `main.rs`**

After `let state = AppState::new(...)`, add:

```rust
    let api = api::router(api::ApiState {
        db: db.clone(),
        live: state.live.clone(),
        data_dir: cfg.data_dir.clone(),
    });
```
and add `.nest("/api", api)` to the router chain right after `.route("/healthz", get(|| async { "ok" }))`.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test api::`
Expected: 8 passed.

Run: `cargo test && cargo clippy --all-targets -- -D warnings`
Expected: all tests pass; clippy clean (no dead code left — every `db`/`sessions`/`export` function is now used). Fix any clippy findings in place.

Run: `cargo sqlx prepare --workspace` and include `.sqlx` changes if any.

- [ ] **Step 6: Smoke-test the API**

With the server running and the session from Task 5's smoke test:
```bash
curl -s localhost:8080/api/sessions | head -c 400; echo
curl -s -o /tmp/s.wav -D - localhost:8080/api/sessions/<uuid>/export/wav | grep -i -E 'content-(type|disposition)'
```
Expected: JSON array with the session; `content-type: audio/wav` and an `attachment; filename="session-…wav"` header; `/tmp/s.wav` is 131116 bytes (44 + 2 × 65536).

- [ ] **Step 7: Commit**

```bash
git add crates/server/src/api.rs crates/server/src/main.rs .sqlx
git commit -m "feat(server): add sessions REST API with exports"
```

---

### Task 8: Client scaffold and pure modules

**Files:**
- Create: `client/package.json`, `client/tsconfig.json`, `client/vite.config.ts`, `client/eslint.config.js`, `client/src/protocol.ts`, `client/src/protocol.test.ts`, `client/src/format.ts`, `client/src/format.test.ts`, `client/src/transcript/reducer.ts`, `client/src/transcript/reducer.test.ts`, `client/src/transcript/reconnect.ts`, `client/src/transcript/reconnect.test.ts`, `client/src/sessionStore.ts`
- Leave untouched for now: `client/index.html`, `client/audio-worklet.js` (the running server still serves them until Task 10)

**Interfaces:**
- Produces:
  - `protocol.ts`: `StartMessage`, `StopMessage`, `ClientMessage`, `Commit`, `ReadyMessage`, `TranscriptMessage`, `ErrorMessage`, `DoneMessage`, `ServerMessage`, `parseServerMessage(data: string): ServerMessage | null`
  - `format.ts`: `formatClock(ms: number): string`, `formatDate(iso: string): string`, `sessionLabel(s: { title: string | null; createdAt: string }): string`
  - `transcript/reducer.ts`: `COLORS`, `Paragraph { text; color }`, `TranscriptState { paragraphs; pending; open; lastSeq; nextColor }`, `TranscriptAction` (`seed` | `connected` | `message` | `reset`), `initialTranscript`, `transcriptReducer`, `seededTranscript(paragraphs: string[]): TranscriptState`
  - `transcript/reconnect.ts`: `BACKOFF_MS`, `backoffDelay(attempt: number): number`, `OFFLINE_BUFFER_BYTES = 960_000`, `class FrameBuffer { push(frame: ArrayBuffer); drain(): ArrayBuffer[]; get bytes(); get dropped() }`
  - `sessionStore.ts`: `readStoredSessionId()`, `storeSessionId(id)`, `clearStoredSessionId()`

- [ ] **Step 1: Create `package.json` and install dependencies**

Create `client/package.json`:

```json
{
  "name": "transcribe-client",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "preview": "vite preview",
    "typecheck": "tsc --noEmit",
    "lint": "eslint .",
    "test": "vitest run"
  }
}
```

```bash
cd client
pnpm add react@^19.3.0 react-dom@^19.3.0 react-router@^8.4.0 @tanstack/react-query@^5.104.1
pnpm add -D vite@^8.3.2 @vitejs/plugin-react@^6.1.1 typescript@~6.0.3 vitest@^5.0.3 \
  @types/react@^19.3.0 @types/react-dom@^19.3.0 @types/node@^24 \
  eslint@^10.11.0 @eslint/js@^10.0.1 typescript-eslint@^8.71.0 eslint-plugin-react-hooks@^7.1.1 globals@^17.13.0
cd ..
```
Expected: `client/pnpm-lock.yaml` created, no peer-dependency errors.

- [ ] **Step 2: Tooling config**

`client/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "skipLibCheck": true,
    "noEmit": true,
    "types": ["vite/client", "node"]
  },
  "include": ["src", "vite.config.ts"]
}
```

`client/vite.config.ts`:
```ts
/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// `pnpm dev` serves the UI on :5173 and forwards the API and the transcription
// socket to the Rust server on :8080.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8080',
      '/ws': { target: 'ws://127.0.0.1:8080', ws: true },
    },
  },
  test: { environment: 'node' },
});
```

`client/eslint.config.js`:
```js
import js from '@eslint/js';
import { defineConfig, globalIgnores } from 'eslint/config';
import reactHooks from 'eslint-plugin-react-hooks';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default defineConfig([
  globalIgnores(['dist', 'public', 'audio-worklet.js']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [js.configs.recommended, tseslint.configs.recommended, reactHooks.configs.flat.recommended],
    languageOptions: { globals: globals.browser },
  },
]);
```

- [ ] **Step 3: Write the failing tests**

`client/src/protocol.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { parseServerMessage } from './protocol';

describe('parseServerMessage', () => {
  it('parses a ready message', () => {
    const msg = parseServerMessage('{"type":"ready","sessionId":"abc","resumed":false,"offsetMs":0}');
    expect(msg).toEqual({ type: 'ready', sessionId: 'abc', resumed: false, offsetMs: 0 });
  });

  it('returns null for garbage or untyped JSON', () => {
    expect(parseServerMessage('not json')).toBeNull();
    expect(parseServerMessage('{"hello":1}')).toBeNull();
    expect(parseServerMessage('null')).toBeNull();
  });
});
```

`client/src/format.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { formatClock, sessionLabel } from './format';

describe('formatClock', () => {
  it('formats minutes and hours', () => {
    expect(formatClock(0)).toBe('0:00');
    expect(formatClock(61_999)).toBe('1:01');
    expect(formatClock(3_723_000)).toBe('1:02:03');
    expect(formatClock(-5)).toBe('0:00');
  });
});

describe('sessionLabel', () => {
  it('prefers the title and falls back to the date', () => {
    expect(sessionLabel({ title: 'Standup', createdAt: '2026-10-03T14:32:00Z' })).toBe('Standup');
    expect(sessionLabel({ title: null, createdAt: '2026-10-03T14:32:00Z' })).toMatch(/^Session /);
  });
});
```

`client/src/transcript/reducer.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import type { TranscriptMessage } from '../protocol';
import { initialTranscript, seededTranscript, transcriptReducer, type TranscriptState } from './reducer';

const tx = (seq: number, over: Partial<TranscriptMessage> = {}): TranscriptMessage => ({
  type: 'transcript',
  seq,
  pending: '',
  endOfSegment: false,
  language: 'en',
  ...over,
});
const commit = (text: string) => ({ text, start: 0, end: 0 });
const apply = (state: TranscriptState, ...msgs: TranscriptMessage[]) =>
  msgs.reduce((s, msg) => transcriptReducer(s, { type: 'message', msg }), state);

describe('transcriptReducer', () => {
  it('appends commits and replaces pending from the same tick', () => {
    let s = apply(initialTranscript, tx(1, { commit: commit('hello'), pending: 'wor' }));
    expect(s.paragraphs).toEqual([{ text: 'hello', color: 0 }]);
    expect(s.pending).toBe('wor');
    expect(s.open).toBe(true);
    s = apply(s, tx(2, { commit: commit('world'), pending: 'how' }));
    expect(s.paragraphs).toEqual([{ text: 'hello world', color: 0 }]);
    expect(s.pending).toBe('how');
  });

  it('closes the paragraph on endOfSegment and starts the next in a new colour', () => {
    let s = apply(initialTranscript, tx(1, { commit: commit('one') }), tx(2, { commit: commit('two'), endOfSegment: true, pending: 'x' }));
    expect(s.open).toBe(false);
    expect(s.pending).toBe('');
    s = apply(s, tx(3, { commit: commit('three') }));
    expect(s.paragraphs).toEqual([
      { text: 'one two', color: 0 },
      { text: 'three', color: 1 },
    ]);
  });

  it('keeps pending visible before any commit', () => {
    const s = apply(initialTranscript, tx(1, { pending: 'hel' }));
    expect(s.paragraphs).toEqual([]);
    expect(s.pending).toBe('hel');
    expect(s.open).toBe(false);
  });

  it('ignores stale and duplicate seq', () => {
    const s = apply(initialTranscript, tx(2, { commit: commit('a') }), tx(2, { commit: commit('dup') }), tx(1, { commit: commit('old') }));
    expect(s.paragraphs).toEqual([{ text: 'a', color: 0 }]);
  });

  it('seeds history and continues the colour cycle after reconnecting', () => {
    let s = seededTranscript(['first', 'second']);
    expect(s.paragraphs.map((p) => p.color)).toEqual([0, 1]);
    s = apply(s, tx(5, { commit: commit('x') }));
    s = transcriptReducer(s, { type: 'connected' });
    expect(s.lastSeq).toBe(0);
    s = apply(s, tx(1, { commit: commit('third') }));
    expect(s.paragraphs.at(-1)).toEqual({ text: 'third', color: 3 });
  });

  it('cycles colours modulo the palette', () => {
    const s = seededTranscript(['a', 'b', 'c', 'd', 'e']);
    expect(s.paragraphs.map((p) => p.color)).toEqual([0, 1, 2, 3, 0]);
    expect(s.nextColor).toBe(1);
  });

  it('reset clears everything', () => {
    expect(transcriptReducer(seededTranscript(['a']), { type: 'reset' })).toEqual(initialTranscript);
  });
});
```

`client/src/transcript/reconnect.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { backoffDelay, FrameBuffer } from './reconnect';

describe('backoffDelay', () => {
  it('follows 0.5, 1, 2, 4, 8, then 10 s forever', () => {
    expect([0, 1, 2, 3, 4, 5, 6, 20].map(backoffDelay)).toEqual([500, 1000, 2000, 4000, 8000, 10000, 10000, 10000]);
  });
});

describe('FrameBuffer', () => {
  it('drops the oldest frames past capacity and reports it once drained', () => {
    const buf = new FrameBuffer(10);
    const a = new ArrayBuffer(4);
    const b = new ArrayBuffer(4);
    const c = new ArrayBuffer(4);
    buf.push(a);
    buf.push(b);
    expect([buf.bytes, buf.dropped]).toEqual([8, false]);
    buf.push(c);
    expect([buf.bytes, buf.dropped]).toEqual([8, true]);
    expect(buf.drain()).toEqual([b, c]);
    expect([buf.bytes, buf.dropped]).toEqual([0, false]);
  });

  it('keeps everything under capacity', () => {
    const buf = new FrameBuffer(100);
    buf.push(new ArrayBuffer(40));
    buf.push(new ArrayBuffer(60));
    expect(buf.drain()).toHaveLength(2);
  });
});
```

- [ ] **Step 4: Run to verify failure**

Run: `cd client && pnpm test`
Expected: FAIL — modules `./protocol`, `./format`, `./reducer`, `./reconnect` not found.

- [ ] **Step 5: Implement the modules**

`client/src/protocol.ts`:
```ts
// Wire protocol, mirroring crates/server/src/protocol.rs. Keep the two in step.

export interface StartMessage {
  type: 'start';
  sampleRate: number;
  encoding: 'pcm_s16le';
  /** null lets the model detect the language. */
  language: string | null;
  /** Resume this session instead of creating a new one. */
  sessionId?: string;
}

export interface StopMessage {
  type: 'stop';
}

export type ClientMessage = StartMessage | StopMessage;

export interface Commit {
  text: string;
  /** ms from the start of this connection's stream */
  start: number;
  end: number;
}

export interface ReadyMessage {
  type: 'ready';
  sessionId: string;
  resumed: boolean;
  offsetMs: number;
}

export interface TranscriptMessage {
  type: 'transcript';
  seq: number;
  /** Append-only delta; absent on ticks with no new stable text. */
  commit?: Commit;
  /** Replace semantics; always present. */
  pending: string;
  endOfSegment: boolean;
  language: string;
}

export interface ErrorMessage {
  type: 'error';
  code: string;
  message: string;
}

export interface DoneMessage {
  type: 'done';
  text: string;
}

export type ServerMessage = ReadyMessage | TranscriptMessage | ErrorMessage | DoneMessage;

export function parseServerMessage(data: string): ServerMessage | null {
  try {
    const value: unknown = JSON.parse(data);
    if (value && typeof value === 'object' && typeof (value as { type?: unknown }).type === 'string') {
      return value as ServerMessage;
    }
  } catch {
    // fall through
  }
  return null;
}
```

`client/src/format.ts`:
```ts
export function formatClock(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = String(total % 60).padStart(2, '0');
  return h ? `${h}:${String(m).padStart(2, '0')}:${s}` : `${m}:${s}`;
}

export function formatDate(iso: string): string {
  return new Date(iso).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

export function sessionLabel(s: { title: string | null; createdAt: string }): string {
  return s.title ?? `Session ${formatDate(s.createdAt)}`;
}
```

`client/src/transcript/reducer.ts`:
```ts
import type { TranscriptMessage } from '../protocol';

export const COLORS = ['#4285f4', '#f4a340', '#34a853', '#a855f7'] as const;

export interface Paragraph {
  text: string;
  /** Index into COLORS. */
  color: number;
}

export interface TranscriptState {
  paragraphs: Paragraph[];
  /** Unstable hypothesis tail. */
  pending: string;
  /** Whether the last paragraph still receives commits. */
  open: boolean;
  /** Highest seq applied on the current connection. */
  lastSeq: number;
  nextColor: number;
}

export type TranscriptAction =
  | { type: 'seed'; paragraphs: string[] }
  | { type: 'connected' }
  | { type: 'message'; msg: TranscriptMessage }
  | { type: 'reset' };

export const initialTranscript: TranscriptState = {
  paragraphs: [],
  pending: '',
  open: false,
  lastSeq: 0,
  nextColor: 0,
};

export function seededTranscript(paragraphs: string[]): TranscriptState {
  return {
    ...initialTranscript,
    paragraphs: paragraphs.map((text, i) => ({ text, color: i % COLORS.length })),
    nextColor: paragraphs.length % COLORS.length,
  };
}

export function transcriptReducer(state: TranscriptState, action: TranscriptAction): TranscriptState {
  switch (action.type) {
    case 'seed':
      return seededTranscript(action.paragraphs);
    case 'reset':
      return initialTranscript;
    case 'connected':
      // Each connection restarts seq at 1 and opens a new paragraph.
      return { ...state, lastSeq: 0, pending: '', open: false };
    case 'message': {
      const { msg } = action;
      if (msg.seq <= state.lastSeq) return state;
      let { paragraphs, open, nextColor } = state;
      if (msg.commit) {
        const last = paragraphs.at(-1);
        if (open && last) {
          const text = last.text ? `${last.text} ${msg.commit.text}` : msg.commit.text;
          paragraphs = [...paragraphs.slice(0, -1), { ...last, text }];
        } else {
          paragraphs = [...paragraphs, { text: msg.commit.text, color: nextColor }];
          nextColor = (nextColor + 1) % COLORS.length;
          open = true;
        }
      }
      let pending = msg.pending;
      if (msg.endOfSegment) {
        open = false;
        pending = '';
      }
      return { paragraphs, pending, open, lastSeq: msg.seq, nextColor };
    }
  }
}
```

`client/src/transcript/reconnect.ts`:
```ts
export const BACKOFF_MS = [500, 1000, 2000, 4000, 8000, 10000] as const;

export function backoffDelay(attempt: number): number {
  return BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)] ?? 10000;
}

/** 30 s of 16 kHz Int16 mono. */
export const OFFLINE_BUFFER_BYTES = 960_000;

/** Audio captured while the socket is down, oldest dropped first. */
export class FrameBuffer {
  private frames: ArrayBuffer[] = [];
  private size = 0;
  private overflowed = false;

  constructor(private readonly maxBytes: number) {}

  get bytes(): number {
    return this.size;
  }

  get dropped(): boolean {
    return this.overflowed;
  }

  push(frame: ArrayBuffer): void {
    this.frames.push(frame);
    this.size += frame.byteLength;
    while (this.size > this.maxBytes && this.frames.length > 0) {
      this.size -= this.frames.shift()?.byteLength ?? 0;
      this.overflowed = true;
    }
  }

  drain(): ArrayBuffer[] {
    const out = this.frames;
    this.frames = [];
    this.size = 0;
    this.overflowed = false;
    return out;
  }
}
```

`client/src/sessionStore.ts`:
```ts
// The session the recorder continues by default. Storage can throw in private
// windows; losing the id only means the next recording starts a new session.
const KEY = 'transcribe.currentSessionId';

export function readStoredSessionId(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null;
  }
}

export function storeSessionId(id: string): void {
  try {
    localStorage.setItem(KEY, id);
  } catch {
    // ignore
  }
}

export function clearStoredSessionId(): void {
  try {
    localStorage.removeItem(KEY);
  } catch {
    // ignore
  }
}
```

- [ ] **Step 6: Run to verify pass**

Run: `cd client && pnpm test && pnpm typecheck && pnpm lint`
Expected: 4 test files, 14 tests pass; typecheck and lint clean.

- [ ] **Step 7: Commit**

```bash
git add client/package.json client/pnpm-lock.yaml client/tsconfig.json client/vite.config.ts client/eslint.config.js client/src
git commit -m "feat(client): scaffold React + TypeScript client with transcript reducer"
```

---

### Task 9: Client API layer and live-session hooks

**Files:**
- Create: `client/src/api.ts`, `client/src/transcript/liveSession.ts`, `client/src/hooks/useTranscriber.ts`, `client/src/hooks/useLevelMeter.ts`, `client/src/hooks/useAutoScroll.ts`

**Interfaces:**
- Consumes: everything from Task 8.
- Produces:
  - `api.ts`: `PAGE_SIZE = 50`, `EXPORT_FORMATS`, `type ExportFormat`, `SessionSummary`, `Recording`, `ParagraphView`, `SessionDetail`, `class ApiError extends Error { status; code }`, `api.listSessions(before?: string): Promise<SessionSummary[]>`, `api.getSession(id)`, `api.renameSession(id, title: string | null)`, `api.deleteSession(id)`, `api.exportUrl(id, format)`
  - `liveSession.ts`: `type TranscriberStatus = 'idle' | 'starting' | 'connecting' | 'live' | 'reconnecting' | 'stopping'`, `interface LiveSessionCallbacks`, `class LiveSession { paused: boolean; analyser: AnalyserNode | null; start(); stop(); setMuted(muted) }`
  - `useTranscriber()` → `{ transcript, status, sessionId, paused, muted, notice, firstTextMs, language, start({ sessionId, autoDetect }), stop(), togglePause(), toggleMute(), newSession(), seed(id, paragraphs), getAnalyser() }`
  - `useLevelMeter(ref, getAnalyser, active, silent)`
  - `useAutoScroll(ref, content)` → `{ autoScroll, toggle }`

There is no unit test for these modules (they drive `getUserMedia`, `AudioContext` and `WebSocket`); their pure parts were tested in Task 8 and they are exercised manually in Task 10. The gate here is `typecheck` + `lint`.

- [ ] **Step 1: `api.ts`**

```ts
export const PAGE_SIZE = 50;
export const EXPORT_FORMATS = ['txt', 'srt', 'vtt', 'json', 'wav'] as const;
export type ExportFormat = (typeof EXPORT_FORMATS)[number];

export interface SessionSummary {
  id: string;
  title: string | null;
  language: string;
  durationMs: number;
  createdAt: string;
  updatedAt: string;
  live: boolean;
  preview: string;
}

export interface Recording {
  id: string;
  ordinal: number;
  offsetMs: number;
  sampleRate: number;
  durationMs: number | null;
  startedAt: string;
  endedAt: string | null;
}

export interface ParagraphView {
  index: number;
  startMs: number;
  endMs: number;
  text: string;
}

export interface SessionDetail extends Omit<SessionSummary, 'preview'> {
  recordings: Recording[];
  paragraphs: ParagraphView[];
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { 'content-type': 'application/json', ...init?.headers },
  });
  if (!res.ok) {
    let code = `http_${res.status}`;
    let message = res.statusText;
    try {
      const body = (await res.json()) as { error?: { code?: string; message?: string } };
      code = body.error?.code ?? code;
      message = body.error?.message ?? message;
    } catch {
      // not a JSON error body
    }
    throw new ApiError(res.status, code, message);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  listSessions(before?: string): Promise<SessionSummary[]> {
    const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
    if (before) params.set('before', before);
    return request(`/api/sessions?${params}`);
  },
  getSession(id: string): Promise<SessionDetail> {
    return request(`/api/sessions/${id}`);
  },
  renameSession(id: string, title: string | null): Promise<Omit<SessionSummary, 'preview'>> {
    return request(`/api/sessions/${id}`, { method: 'PATCH', body: JSON.stringify({ title }) });
  },
  deleteSession(id: string): Promise<void> {
    return request(`/api/sessions/${id}`, { method: 'DELETE' });
  },
  exportUrl(id: string, format: ExportFormat): string {
    return `/api/sessions/${id}/export/${format}`;
  },
};
```

- [ ] **Step 2: `transcript/liveSession.ts`**

```ts
import { parseServerMessage, type ReadyMessage, type StartMessage, type TranscriptMessage } from '../protocol';
import { backoffDelay, FrameBuffer, OFFLINE_BUFFER_BYTES } from './reconnect';

const TARGET_RATE = 16_000;
/** How long Stop waits for the server's `done` before closing anyway. */
const STOP_TIMEOUT_MS = 5_000;

export type TranscriberStatus = 'idle' | 'starting' | 'connecting' | 'live' | 'reconnecting' | 'stopping';

export interface LiveSessionCallbacks {
  status(status: TranscriberStatus): void;
  ready(msg: ReadyMessage): void;
  transcript(msg: TranscriptMessage): void;
  /** Transient UI notice; null clears it. */
  notice(text: string | null): void;
  /** `terminal` means the session will not reconnect. */
  serverError(code: string, message: string, terminal: boolean): void;
  /** Fully stopped; no further callbacks. */
  ended(): void;
}

/**
 * One recording run: microphone → worklet → WebSocket, with automatic
 * reconnect into the same session. Plain class so React only sees callbacks.
 */
export class LiveSession {
  paused = false;
  analyser: AnalyserNode | null = null;

  private ws: WebSocket | null = null;
  private ctx: AudioContext | null = null;
  private node: AudioWorkletNode | null = null;
  private stream: MediaStream | null = null;
  private ready = false;
  private everReady = false;
  private stopped = false;
  private terminal = false;
  private finished = false;
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private readonly buffer = new FrameBuffer(OFFLINE_BUFFER_BYTES);

  constructor(
    private sessionId: string | null,
    private readonly autoDetect: boolean,
    private readonly cb: LiveSessionCallbacks,
  ) {}

  async start(): Promise<void> {
    this.cb.status('starting');
    this.stream = await navigator.mediaDevices.getUserMedia({
      audio: { channelCount: 1, echoCancellation: false, noiseSuppression: false, autoGainControl: true },
    });
    // Ask for 16 kHz directly; the browser resamples the mic better than we
    // can. The worklet falls back to its own downsampler otherwise.
    try {
      this.ctx = new AudioContext({ sampleRate: TARGET_RATE });
    } catch {
      this.ctx = new AudioContext();
    }
    await this.ctx.resume();
    await this.ctx.audioWorklet.addModule('/audio-worklet.js');
    if (this.stopped) {
      this.teardownAudio();
      return;
    }

    const source = this.ctx.createMediaStreamSource(this.stream);
    this.analyser = this.ctx.createAnalyser();
    this.analyser.fftSize = 1024;
    source.connect(this.analyser);

    this.node = new AudioWorkletNode(this.ctx, 'capture-processor', {
      numberOfInputs: 1,
      numberOfOutputs: 0,
      processorOptions: { targetRate: TARGET_RATE, frameSamples: 2048 }, // ~128 ms
    });
    this.node.port.onmessage = (ev: MessageEvent<Int16Array<ArrayBuffer>>) => this.onFrame(ev.data.buffer);
    source.connect(this.node);
    this.connect();
  }

  setMuted(muted: boolean): void {
    this.stream?.getAudioTracks().forEach((t) => {
      t.enabled = !muted;
    });
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.teardownAudio();
    const ws = this.ws;
    if (ws && ws.readyState === WebSocket.OPEN && this.ready) {
      ws.send(JSON.stringify({ type: 'stop' }));
      this.cb.status('stopping');
      // The server answers with `done` and closes; don't hang if it never does.
      setTimeout(() => ws.close(), STOP_TIMEOUT_MS);
    } else {
      if (ws) {
        ws.onclose = null;
        ws.close();
      }
      this.finish();
    }
  }

  private connect(): void {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;
    this.ready = false;
    this.cb.status(this.attempt === 0 ? 'connecting' : 'reconnecting');

    ws.onopen = () => {
      const start: StartMessage = {
        type: 'start',
        sampleRate: TARGET_RATE,
        encoding: 'pcm_s16le',
        language: this.autoDetect ? null : 'en',
        ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      };
      ws.send(JSON.stringify(start));
    };

    ws.onmessage = (ev: MessageEvent<string>) => {
      const msg = parseServerMessage(ev.data);
      if (!msg) return;
      switch (msg.type) {
        case 'ready':
          this.ready = true;
          this.everReady = true;
          this.attempt = 0;
          this.sessionId = msg.sessionId;
          this.cb.ready(msg);
          this.flushBuffer();
          this.cb.status('live');
          break;
        case 'transcript':
          this.cb.transcript(msg);
          break;
        case 'error': {
          // A missing session never comes back; a busy one is worth retrying
          // only if we were already recording into it (we raced our own
          // previous connection's flush).
          const terminal = msg.code === 'session_not_found' || (msg.code === 'session_busy' && !this.everReady);
          if (terminal) this.terminal = true;
          this.cb.serverError(msg.code, msg.message, terminal);
          break;
        }
        case 'done':
          break;
      }
    };

    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      this.ready = false;
      if (this.stopped || this.terminal) {
        this.teardownAudio();
        this.finish();
        return;
      }
      const delay = backoffDelay(this.attempt++);
      this.cb.status('reconnecting');
      this.cb.notice(`Connection lost, retrying in ${delay / 1000}s`);
      this.timer = setTimeout(() => {
        this.timer = null;
        this.connect();
      }, delay);
    };
  }

  private onFrame(frame: ArrayBuffer): void {
    if (this.paused || this.stopped) return;
    if (this.ws && this.ready && this.ws.readyState === WebSocket.OPEN) this.ws.send(frame);
    else this.buffer.push(frame);
  }

  private flushBuffer(): void {
    const dropped = this.buffer.dropped;
    for (const frame of this.buffer.drain()) this.ws?.send(frame);
    this.cb.notice(dropped ? 'Some audio was lost while reconnecting.' : null);
  }

  private teardownAudio(): void {
    if (this.node) {
      this.node.port.postMessage('stop');
      this.node.port.onmessage = null;
      this.node.disconnect();
      this.node = null;
    }
    this.stream?.getTracks().forEach((t) => t.stop());
    this.stream = null;
    void this.ctx?.close();
    this.ctx = null;
    this.analyser = null;
  }

  private finish(): void {
    if (this.finished) return;
    this.finished = true;
    this.ws = null;
    this.cb.status('idle');
    this.cb.ended();
  }
}
```

- [ ] **Step 3: `hooks/useTranscriber.ts`**

```ts
import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { clearStoredSessionId, readStoredSessionId, storeSessionId } from '../sessionStore';
import { LiveSession, type TranscriberStatus } from '../transcript/liveSession';
import { initialTranscript, transcriptReducer } from '../transcript/reducer';

export interface StartOptions {
  sessionId: string | null;
  autoDetect: boolean;
}

export function useTranscriber() {
  const [transcript, dispatch] = useReducer(transcriptReducer, initialTranscript);
  const [status, setStatus] = useState<TranscriberStatus>('idle');
  const [sessionId, setSessionId] = useState<string | null>(readStoredSessionId);
  const [paused, setPaused] = useState(false);
  const [muted, setMuted] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [firstTextMs, setFirstTextMs] = useState<number | null>(null);
  const [language, setLanguage] = useState<string | null>(null);
  const live = useRef<LiveSession | null>(null);
  const mutedRef = useRef(false);

  // Leaving the page ends the recording; the server flushes what it has.
  useEffect(
    () => () => {
      live.current?.stop();
      live.current = null;
    },
    [],
  );

  const start = useCallback(async ({ sessionId: resumeId, autoDetect }: StartOptions) => {
    if (live.current) return;
    const startedAt = performance.now();
    let sawText = false;
    mutedRef.current = false;
    setMuted(false);
    setPaused(false);
    setNotice(null);
    setFirstTextMs(null);

    const session: LiveSession = new LiveSession(resumeId, autoDetect, {
      status: setStatus,
      ready: (msg) => {
        storeSessionId(msg.sessionId);
        setSessionId(msg.sessionId);
        dispatch({ type: 'connected' });
      },
      transcript: (msg) => {
        dispatch({ type: 'message', msg });
        if (!sawText && (msg.commit || msg.pending)) {
          sawText = true;
          setFirstTextMs(Math.round(performance.now() - startedAt));
        }
        setLanguage(msg.language);
      },
      notice: setNotice,
      serverError: (code, message, terminal) => {
        if (code === 'session_not_found') {
          clearStoredSessionId();
          setSessionId(null);
          setNotice('That session no longer exists. Press start for a new one.');
        } else if (code === 'session_busy') {
          if (terminal) setNotice('This session is recording in another tab.');
        } else if (code === 'persist_failed') {
          setNotice('Transcript is not being saved right now; retrying.');
        } else {
          setNotice(`Error: ${message || code}`);
        }
      },
      ended: () => {
        if (live.current === session) live.current = null;
        setPaused(false);
      },
    });
    live.current = session;
    try {
      await session.start();
    } catch (e) {
      session.stop();
      setNotice(`Microphone error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, []);

  const stop = useCallback(() => live.current?.stop(), []);

  const togglePause = useCallback(() => {
    const session = live.current;
    if (!session) return;
    session.paused = !session.paused;
    setPaused(session.paused);
  }, []);

  const toggleMute = useCallback(() => {
    const session = live.current;
    if (!session) return;
    mutedRef.current = !mutedRef.current;
    session.setMuted(mutedRef.current);
    setMuted(mutedRef.current);
  }, []);

  const newSession = useCallback(() => {
    if (live.current) return;
    clearStoredSessionId();
    setSessionId(null);
    setNotice(null);
    setFirstTextMs(null);
    dispatch({ type: 'reset' });
  }, []);

  const seed = useCallback((id: string, paragraphs: string[]) => {
    if (live.current) return;
    storeSessionId(id);
    setSessionId(id);
    dispatch({ type: 'seed', paragraphs });
  }, []);

  const getAnalyser = useCallback(() => live.current?.analyser ?? null, []);

  return {
    transcript,
    status,
    sessionId,
    paused,
    muted,
    notice,
    firstTextMs,
    language,
    start,
    stop,
    togglePause,
    toggleMute,
    newSession,
    seed,
    getAnalyser,
  };
}
```

- [ ] **Step 4: `hooks/useLevelMeter.ts`**

```ts
import { useEffect, useRef, type RefObject } from 'react';

/**
 * Exponentially smoothed RMS written to the element's `--level` CSS variable
 * every frame. Rises fast, falls slowly, so the meter tracks speech without
 * flickering. Never touches React state.
 */
export function useLevelMeter(
  ref: RefObject<HTMLElement | null>,
  getAnalyser: () => AnalyserNode | null,
  active: boolean,
  silent: boolean,
): void {
  const silentRef = useRef(silent);
  useEffect(() => {
    silentRef.current = silent;
  }, [silent]);

  useEffect(() => {
    const el = ref.current;
    if (!el || !active) return;
    let raf = 0;
    let value = 0;
    let buf: Float32Array<ArrayBuffer> | null = null;
    const tick = () => {
      let target = 0;
      const analyser = getAnalyser();
      if (analyser && !silentRef.current) {
        if (!buf || buf.length !== analyser.fftSize) buf = new Float32Array(analyser.fftSize);
        analyser.getFloatTimeDomainData(buf);
        let sum = 0;
        for (const v of buf) sum += v * v;
        // Perceptual-ish curve: quiet speech should still move the needle.
        target = Math.min(1, Math.sqrt(Math.sqrt(sum / buf.length)) * 2.6);
      }
      value += (target - value) * (target > value ? 0.35 : 0.08);
      el.style.setProperty('--level', value.toFixed(3));
      raf = requestAnimationFrame(tick);
    };
    tick();
    return () => {
      cancelAnimationFrame(raf);
      el.style.setProperty('--level', '0');
    };
  }, [ref, getAnalyser, active]);
}
```

- [ ] **Step 5: `hooks/useAutoScroll.ts`**

```ts
import { useCallback, useEffect, useLayoutEffect, useState, type RefObject } from 'react';

const STICK_PX = 48;

/**
 * Follows the tail until the reader scrolls away from the bottom, and
 * re-engages once they scroll back down to it.
 */
export function useAutoScroll(ref: RefObject<HTMLElement | null>, content: unknown) {
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onScroll = () => setAutoScroll(el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_PX);
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [ref]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (el && autoScroll) el.scrollTop = el.scrollHeight;
  }, [ref, content, autoScroll]);

  const toggle = useCallback(() => setAutoScroll((on) => !on), []);
  return { autoScroll, toggle };
}
```

- [ ] **Step 6: Typecheck and lint**

Run: `cd client && pnpm typecheck && pnpm lint && pnpm test`
Expected: clean; 14 tests still pass. If `react-hooks` lint flags a pattern, fix the code to satisfy the rule (do not disable the rule).

- [ ] **Step 7: Commit**

```bash
git add client/src
git commit -m "feat(client): add sessions API client and live transcription hooks"
```

---

### Task 10: Recorder page, styles and serving the built client

**Files:**
- Create: `client/index.html` (replaces the vanilla page), `client/public/audio-worklet.js` (moved), `client/src/main.tsx`, `client/src/App.tsx`, `client/src/styles/app.css`, `client/src/components/icons.tsx`, `client/src/components/Transcript.tsx`, `client/src/components/TitleEditor.tsx`, `client/src/pages/RecorderPage.tsx`, `client/src/pages/SessionsPage.tsx`, `client/src/pages/SessionPage.tsx` (the last two as minimal real pages here, completed in Task 11)
- Delete: `client/audio-worklet.js` (moved)
- Modify: `crates/server/src/config.rs` (`client_dir` default)

**Interfaces:**
- Consumes: Task 8 and Task 9 modules.
- Produces: `TitleEditor({ sessionId })`, `Transcript({ state, listening })`, icon components `MicIcon, StopIcon, PauseIcon, PlayIcon, LevelIcon, AutoScrollIcon, PlusIcon, ListIcon, CopyIcon, ExpandIcon, TrashIcon`; routes `/`, `/sessions`, `/sessions/:id`.

- [ ] **Step 1: Move the worklet and port the styles**

```bash
mkdir -p client/public client/src/styles client/src/components client/src/pages
git mv client/audio-worklet.js client/public/audio-worklet.js
git show develop:client/index.html \
  | awk '/<style>/{f=1;next} /<\/style>/{f=0} f' \
  | sed -e 's/^  //' \
        -e 's/body:not(\[data-state="idle"\])/.recorder:not([data-state="idle"])/g' \
        -e 's/body\[data-state=/.recorder[data-state=/g' \
  > client/src/styles/app.css
grep -c 'body\[data-state' client/src/styles/app.css
```
Expected: the final grep prints `0`.

Append to `client/src/styles/app.css`:

```css
/* ---------- React shell ---------- */

#root { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.recorder { flex: 1; min-height: 0; display: flex; flex-direction: column; }
a { color: var(--accent); }
a.icon-btn, a.pill { text-decoration: none; }

.title-btn {
  font-weight: 600; font-size: .95rem;
  max-width: 22ch; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  border-radius: 6px; padding: .1rem .3rem; margin: 0 -.3rem;
}
.title-btn:hover { background: var(--tint); }
.title-input {
  font: inherit; font-weight: 600; font-size: .95rem; color: var(--fg);
  background: var(--tint); border: 1px solid var(--line); border-radius: 6px;
  padding: .1rem .3rem; width: 22ch;
}
#status[data-notice="true"] { color: var(--danger); }

/* ---------- sessions pages ---------- */

.page {
  flex: 1; min-height: 0; overflow-y: auto;
  width: 100%; max-width: 860px; margin: 0 auto;
  padding: 2rem clamp(1rem, 5vw, 2.5rem) 3rem;
}
.page-head { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1rem; }
.page-head h1 { font-size: 1.4rem; margin: 0; }
.page-head h1 .title-btn, .page-head h1 .title-input { font-size: 1.4rem; max-width: none; width: auto; }
.back { display: inline-block; margin-bottom: 1rem; font-size: .9rem; }
.muted { color: var(--muted); }
.error { color: var(--danger); }

.session-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: .5rem; }
.session-row {
  display: flex; align-items: center; gap: .75rem;
  border: 1px solid var(--line); border-radius: 12px; padding: .75rem .75rem .75rem 1rem;
}
.session-row:hover { background: var(--tint); }
.session-link { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: .15rem; color: inherit; text-decoration: none; }
.session-title { font-weight: 600; display: flex; align-items: center; gap: .5rem; }
.session-preview { color: var(--muted); font-size: .9rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.session-meta { color: var(--muted); font-size: .8rem; font-variant-numeric: tabular-nums; }
.live-badge { color: var(--danger); font-size: .8rem; font-weight: 600; }

.actions { display: flex; flex-wrap: wrap; align-items: center; gap: .6rem; margin: 1rem 0 1.5rem; }
.export-menu { display: flex; flex-wrap: wrap; gap: .4rem; }
.export-menu .pill { height: 36px; padding: 0 .9rem; font-size: .85rem; }
.pill.primary-pill { background: var(--accent); color: #fff; }
.pill.danger-pill { color: var(--danger); }
.load-more { margin-top: 1rem; }

.session-transcript { display: flex; flex-direction: column; gap: .5rem; line-height: 1.55; }
.session-transcript .stamp {
  color: var(--muted); font-size: .8rem; font-variant-numeric: tabular-nums; margin-right: .6rem;
}
```

- [ ] **Step 2: Vite entry, bootstrap and router**

`client/index.html` (overwrite):
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Near-realtime Whisper</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`client/src/main.tsx`:
```tsx
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './styles/app.css';

const root = document.getElementById('root');
if (!root) throw new Error('missing #root');
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

`client/src/App.tsx`:
```tsx
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createBrowserRouter } from 'react-router';
import { RouterProvider } from 'react-router/dom';
import { RecorderPage } from './pages/RecorderPage';
import { SessionPage } from './pages/SessionPage';
import { SessionsPage } from './pages/SessionsPage';

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } },
});

const router = createBrowserRouter([
  { path: '/', Component: RecorderPage },
  { path: '/sessions', Component: SessionsPage },
  { path: '/sessions/:id', Component: SessionPage },
]);

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}
```

- [ ] **Step 3: Icons**

`client/src/components/icons.tsx` (SVGs copied from the vanilla page; three new ones for plus, list, trash):
```tsx
export function MicIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
      <rect x="9" y="2.5" width="6" height="11" rx="3" fill="currentColor" stroke="none" />
      <path d="M5.5 11a6.5 6.5 0 0 0 13 0M12 17.5V21M8.5 21h7" />
    </svg>
  );
}

export function LevelIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
      <rect x="9" y="2.5" width="6" height="11" rx="3" fill="currentColor" stroke="none" />
      <path d="M5.5 11a6.5 6.5 0 0 0 13 0M12 17.5V21" />
    </svg>
  );
}

export function StopIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor">
      <rect x="6" y="6" width="12" height="12" rx="2.5" />
    </svg>
  );
}

export function PauseIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor">
      <rect x="6" y="5" width="4" height="14" rx="1.2" />
      <rect x="14" y="5" width="4" height="14" rx="1.2" />
    </svg>
  );
}

export function PlayIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor">
      <path d="M8 5.6c0-.9 1-1.5 1.8-1L18.3 11c.7.5.7 1.5 0 2l-8.5 5.4c-.8.5-1.8-.1-1.8-1V5.6Z" />
    </svg>
  );
}

export function AutoScrollIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M6 6.5 12 12l6-5.5M6 13.5 12 19l6-5.5" />
    </svg>
  );
}

export function PlusIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
      <path d="M12 5v14M5 12h14" />
    </svg>
  );
}

export function ListIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
      <path d="M9 6h11M9 12h11M9 18h11M4.5 6h.01M4.5 12h.01M4.5 18h.01" />
    </svg>
  );
}

export function CopyIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinejoin="round">
      <rect x="9" y="9" width="11" height="11" rx="2.5" />
      <path d="M15 5.5A1.5 1.5 0 0 0 13.5 4H6a2 2 0 0 0-2 2v7.5A1.5 1.5 0 0 0 5.5 15" />
    </svg>
  );
}

export function ExpandIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 4h6v6M20 4l-7 7M10 20H4v-6M4 20l7-7" />
    </svg>
  );
}

export function TrashIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 7h16M9.5 7V5.5A1.5 1.5 0 0 1 11 4h2a1.5 1.5 0 0 1 1.5 1.5V7M6.5 7l.8 12a1.5 1.5 0 0 0 1.5 1.4h6.4a1.5 1.5 0 0 0 1.5-1.4l.8-12" />
    </svg>
  );
}
```

- [ ] **Step 4: Transcript and title components**

`client/src/components/Transcript.tsx`:
```tsx
import type { CSSProperties } from 'react';
import { COLORS, type TranscriptState } from '../transcript/reducer';

const accent = (color: number) => ({ '--c': COLORS[color % COLORS.length] }) as CSSProperties;

export function Transcript({ state, listening }: { state: TranscriptState; listening: boolean }) {
  const { paragraphs, pending, open, nextColor } = state;
  const last = paragraphs.length - 1;
  return (
    <>
      <div id="transcript">
        {paragraphs.map((p, i) => (
          <div key={i} className="line" style={accent(p.color)}>
            <span className="committed">{p.text}</span>
            {open && i === last && pending ? <span className="pending"> {pending}</span> : null}
          </div>
        ))}
        {!open && pending ? (
          <div className="line" style={accent(nextColor)}>
            <span className="pending">{pending}</span>
          </div>
        ) : null}
      </div>
      <div id="listening" hidden={!listening}>
        listening<i>.</i>
        <i>.</i>
        <i>.</i>
      </div>
    </>
  );
}
```

`client/src/components/TitleEditor.tsx`:
```tsx
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { api } from '../api';
import { sessionLabel } from '../format';

/** Click to rename; Enter or blur saves, Escape cancels, empty clears. */
export function TitleEditor({ sessionId }: { sessionId: string }) {
  const qc = useQueryClient();
  const { data } = useQuery({ queryKey: ['session', sessionId], queryFn: () => api.getSession(sessionId) });
  const [draft, setDraft] = useState<string | null>(null);
  const rename = useMutation({
    mutationFn: (title: string | null) => api.renameSession(sessionId, title),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['session', sessionId] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
    },
  });

  const label = data ? sessionLabel(data) : 'Untitled session';
  if (draft === null) {
    return (
      <button type="button" className="title-btn" title="Rename session" onClick={() => setDraft(data?.title ?? '')}>
        {label}
      </button>
    );
  }

  const commit = () => {
    const value = draft.trim();
    setDraft(null);
    if (value !== (data?.title ?? '')) rename.mutate(value || null);
  };
  return (
    <input
      className="title-input"
      aria-label="Session title"
      autoFocus
      maxLength={200}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Enter') commit();
        if (e.key === 'Escape') setDraft(null);
      }}
    />
  );
}
```

- [ ] **Step 5: Recorder page**

`client/src/pages/RecorderPage.tsx`:
```tsx
import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { api, ApiError } from '../api';
import {
  AutoScrollIcon, CopyIcon, ExpandIcon, LevelIcon, ListIcon, MicIcon, PauseIcon, PlayIcon, PlusIcon, StopIcon,
} from '../components/icons';
import { TitleEditor } from '../components/TitleEditor';
import { Transcript } from '../components/Transcript';
import { useAutoScroll } from '../hooks/useAutoScroll';
import { useLevelMeter } from '../hooks/useLevelMeter';
import { useTranscriber } from '../hooks/useTranscriber';
import { seededTranscript } from '../transcript/reducer';

export function RecorderPage() {
  const [params, setParams] = useSearchParams();
  const t = useTranscriber();
  const [autoDetect, setAutoDetect] = useState(false);
  const [flash, setFlash] = useState<string | null>(null);
  const stageRef = useRef<HTMLElement>(null);
  const levelRef = useRef<HTMLButtonElement>(null);

  const isLive = t.status !== 'idle';
  const requested = params.get('session');
  const target = requested ?? t.sessionId;

  // History is shown (not yet seeded) until recording starts, so nothing
  // here needs an effect to copy server data into the reducer.
  const showHistory = !isLive && t.transcript.paragraphs.length === 0 && target !== null;
  const history = useQuery({
    queryKey: ['session', target],
    queryFn: () => api.getSession(target as string),
    enabled: showHistory,
  });
  const missing = history.error instanceof ApiError && history.error.status === 404;
  const historyParagraphs = useMemo(() => history.data?.paragraphs.map((p) => p.text) ?? [], [history.data]);
  const view = useMemo(
    () => (showHistory && historyParagraphs.length ? seededTranscript(historyParagraphs) : t.transcript),
    [showHistory, historyParagraphs, t.transcript],
  );

  const { autoScroll, toggle: toggleAutoScroll } = useAutoScroll(stageRef, view);
  useLevelMeter(levelRef, t.getAnalyser, isLive, t.muted || t.paused);

  const hasText = view.paragraphs.length > 0 || view.pending !== '';
  const uiState = isLive ? 'live' : hasText ? 'ended' : 'idle';
  const busy = t.status === 'starting' || t.status === 'stopping';

  const begin = async () => {
    const id = missing ? null : target;
    if (missing) t.newSession();
    else if (id && t.transcript.paragraphs.length === 0 && historyParagraphs.length) t.seed(id, historyParagraphs);
    if (requested) setParams({}, { replace: true });
    await t.start({ sessionId: id, autoDetect });
  };

  const say = (text: string) => {
    setFlash(text);
    setTimeout(() => setFlash((f) => (f === text ? null : f)), 2500);
  };

  const copy = async () => {
    const text = view.paragraphs.map((p) => p.text).join('\n');
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      say('copied');
    } catch {
      say('copy blocked');
    }
  };

  const statusText =
    t.notice ??
    flash ??
    (missing
      ? 'That session no longer exists.'
      : t.status === 'starting' || t.status === 'connecting'
        ? 'connecting…'
        : t.status === 'reconnecting'
          ? 'reconnecting…'
          : t.status === 'stopping'
            ? 'stopping…'
            : t.status === 'live'
              ? t.paused
                ? 'paused'
                : t.firstTextMs !== null
                  ? `first text in ${t.firstTextMs} ms`
                  : 'listening…'
              : '');

  // Space toggles pause while live, start otherwise (as in the old client).
  const beginRef = useRef(begin);
  useEffect(() => {
    beginRef.current = begin;
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.code !== 'Space' || e.target !== document.body) return;
      e.preventDefault();
      if (isLive) t.togglePause();
      else void beginRef.current();
    };
    addEventListener('keydown', onKey);
    return () => removeEventListener('keydown', onKey);
  }, [isLive, t.togglePause]);

  const titleSession = isLive ? t.sessionId : missing ? null : target;

  return (
    <div className="recorder" data-state={uiState}>
      <main id="splash">
        <div className="bubble">{showHistory && history.isLoading ? 'Loading session…' : 'Press and start talking'}</div>
        <button className="mic-hero" id="hero" aria-label="Start transcribing" disabled={busy} onClick={() => void begin()}>
          <MicIcon />
        </button>
        <p className="hint">Solid text is committed · grey text is the live hypothesis</p>
        <Link className="hint" to="/sessions">
          Past sessions
        </Link>
      </main>

      <button
        className="icon-btn"
        id="autoscroll"
        aria-pressed={autoScroll}
        aria-label="Toggle auto-scroll"
        title={autoScroll ? 'Auto-scroll is on' : 'Auto-scroll is off — click to follow'}
        onClick={toggleAutoScroll}
      >
        <AutoScrollIcon />
      </button>

      <section id="stage" ref={stageRef}>
        <Transcript state={view} listening={t.status === 'live' && t.firstTextMs === null} />
      </section>

      <footer id="bar">
        <div id="brand">
          <span className="dot" />
          {titleSession ? <TitleEditor sessionId={titleSession} /> : <span>Whisper live</span>}
          <span id="status" data-notice={t.notice !== null || missing}>
            {statusText}
          </span>
        </div>

        <div id="controls">
          <button
            className="icon-btn"
            id="pause"
            aria-label={t.paused ? 'Resume' : 'Pause'}
            title={t.paused ? 'Resume capture' : 'Pause capture'}
            disabled={t.status !== 'live'}
            onClick={t.togglePause}
          >
            {t.paused ? <PlayIcon /> : <PauseIcon />}
          </button>
          <button
            className="primary"
            id="primary"
            disabled={busy}
            aria-label={isLive ? 'Stop' : hasText ? 'Continue' : 'Start'}
            onClick={isLive ? t.stop : () => void begin()}
          >
            {isLive ? <StopIcon /> : <MicIcon />}
          </button>
          <button
            className="icon-btn"
            id="level"
            ref={levelRef}
            data-muted={String(t.muted)}
            aria-label="Microphone level"
            title={t.muted ? 'Unmute microphone' : 'Input level — click to mute'}
            onClick={t.toggleMute}
          >
            <LevelIcon />
          </button>
        </div>

        <div id="tools">
          <button
            className="icon-btn"
            aria-label="New session"
            title="New session"
            disabled={isLive || !hasText}
            onClick={() => {
              t.newSession();
              if (requested) setParams({}, { replace: true });
            }}
          >
            <PlusIcon />
          </button>
          {isLive ? (
            <button className="icon-btn" aria-label="Sessions" title="Stop recording to browse sessions" disabled>
              <ListIcon />
            </button>
          ) : (
            <Link className="icon-btn" to="/sessions" aria-label="Sessions" title="Sessions">
              <ListIcon />
            </Link>
          )}
          <button className="icon-btn" aria-label="Copy transcript" title="Copy transcript" onClick={() => void copy()}>
            <CopyIcon />
          </button>
          <button
            className="icon-btn"
            aria-label="Fullscreen"
            title="Fullscreen"
            onClick={() => (document.fullscreenElement ? void document.exitFullscreen() : void document.documentElement.requestFullscreen?.())}
          >
            <ExpandIcon />
          </button>
          <button
            className="pill"
            id="detect"
            aria-pressed={autoDetect}
            title="Let the model detect the language"
            onClick={() => {
              setAutoDetect((on) => !on);
              if (isLive) say('applies on next start');
            }}
          >
            <span>{autoDetect ? (t.language && isLive ? t.language.toUpperCase() : 'Auto') : 'Detect Language'}</span>
          </button>
        </div>
      </footer>
    </div>
  );
}
```

- [ ] **Step 6: Minimal sessions pages (completed in Task 11)**

`client/src/pages/SessionsPage.tsx`:
```tsx
import { Link } from 'react-router';

export function SessionsPage() {
  return (
    <div className="page">
      <header className="page-head">
        <h1>Sessions</h1>
        <Link className="pill" to="/">
          New recording
        </Link>
      </header>
    </div>
  );
}
```

`client/src/pages/SessionPage.tsx`:
```tsx
import { Link } from 'react-router';

export function SessionPage() {
  return (
    <div className="page">
      <Link to="/sessions" className="back">
        ← All sessions
      </Link>
    </div>
  );
}
```

- [ ] **Step 7: Serve the built client**

In `crates/server/src/config.rs`, change the `client_dir` attribute's default and doc:
```rust
    /// Built browser client served at `/` (`cd client && pnpm build`).
    #[arg(long, env = "TR_CLIENT_DIR", default_value = "client/dist")]
    pub client_dir: PathBuf,
```

- [ ] **Step 8: Verify**

```bash
cd client && pnpm typecheck && pnpm lint && pnpm test && pnpm build && cd ..
cargo test
cargo run --release --features metal -- --model models/ggml-base.en-q5_1.bin
```
Expected: `client/dist/index.html` and `client/dist/audio-worklet.js` exist. Open <http://127.0.0.1:8080> and check, in this order:
1. The splash looks as before (red mic, bubble). Click it, allow the mic, speak: grey pending text then solid committed text; status shows `first text in N ms`; the level disc moves.
2. Pause stops text; resume continues. Stop → transcript stays; the title button shows `Session <date>`; click it, rename to `Test`, press Enter.
3. Reload the page: the transcript is shown again (from the API). Click the mic → speech appends below the old text in a new paragraph.
4. While recording, stop the server (Ctrl-C) and start it again: status shows `reconnecting…`, then text continues in the same session (check `psql transcribe_rust -Atc "select ordinal, offset_ms from recordings order by started_at desc limit 3"` shows consecutive ordinals with growing offsets).
5. **New session** clears the screen; the next recording gets a new session id.

- [ ] **Step 9: Commit**

```bash
git add client crates/server/src/config.rs
git commit -m "feat(client): port the recorder UI to React with session resume"
```

---

### Task 11: Sessions list and detail pages

**Files:**
- Create: `client/src/components/ExportMenu.tsx`
- Modify: `client/src/pages/SessionsPage.tsx`, `client/src/pages/SessionPage.tsx` (replace the minimal versions)

**Interfaces:**
- Consumes: `api`, `ApiError`, `PAGE_SIZE`, `EXPORT_FORMATS`, `TitleEditor`, `TrashIcon`, `formatClock`, `formatDate`, `sessionLabel`, `COLORS`, `readStoredSessionId`, `clearStoredSessionId`.
- Produces: `ExportMenu({ sessionId })`.

- [ ] **Step 1: Export menu**

`client/src/components/ExportMenu.tsx`:
```tsx
import { api, EXPORT_FORMATS } from '../api';

export function ExportMenu({ sessionId }: { sessionId: string }) {
  return (
    <div className="export-menu" role="group" aria-label="Export">
      {EXPORT_FORMATS.map((format) => (
        <a key={format} className="pill" href={api.exportUrl(sessionId, format)} download>
          .{format}
        </a>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: Sessions list**

Replace `client/src/pages/SessionsPage.tsx`:
```tsx
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router';
import { api, PAGE_SIZE } from '../api';
import { TrashIcon } from '../components/icons';
import { formatClock, formatDate, sessionLabel } from '../format';
import { clearStoredSessionId, readStoredSessionId } from '../sessionStore';

export function SessionsPage() {
  const qc = useQueryClient();
  const sessions = useInfiniteQuery({
    queryKey: ['sessions'],
    queryFn: ({ pageParam }) => api.listSessions(pageParam),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => (last.length === PAGE_SIZE ? last.at(-1)?.id : undefined),
    // Keep live badges and growing durations current.
    refetchInterval: (query) => (query.state.data?.pages.some((p) => p.some((s) => s.live)) ? 3000 : false),
  });
  const remove = useMutation({
    mutationFn: (id: string) => api.deleteSession(id),
    onSuccess: (_, id) => {
      if (readStoredSessionId() === id) clearStoredSessionId();
      qc.removeQueries({ queryKey: ['session', id] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
    },
  });
  const rows = sessions.data?.pages.flat() ?? [];

  return (
    <div className="page">
      <header className="page-head">
        <h1>Sessions</h1>
        <Link className="pill" to="/">
          New recording
        </Link>
      </header>

      {sessions.isPending ? (
        <p className="muted">Loading…</p>
      ) : sessions.isError ? (
        <p className="error">Couldn’t load sessions: {sessions.error.message}</p>
      ) : rows.length === 0 ? (
        <p className="muted">
          No sessions yet. <Link to="/">Record one</Link>.
        </p>
      ) : (
        <ul className="session-list">
          {rows.map((s) => (
            <li key={s.id} className="session-row">
              <Link to={`/sessions/${s.id}`} className="session-link">
                <span className="session-title">
                  {sessionLabel(s)}
                  {s.live ? <span className="live-badge">● live</span> : null}
                </span>
                <span className="session-preview">{s.preview || 'No transcript yet'}</span>
                <span className="session-meta">
                  {formatClock(s.durationMs)} · {formatDate(s.createdAt)}
                </span>
              </Link>
              <button
                className="icon-btn"
                aria-label={`Delete ${sessionLabel(s)}`}
                title={s.live ? 'Stop recording before deleting' : 'Delete session'}
                disabled={s.live || remove.isPending}
                onClick={() => {
                  if (confirm(`Delete “${sessionLabel(s)}”? Its transcript and audio are removed.`)) remove.mutate(s.id);
                }}
              >
                <TrashIcon />
              </button>
            </li>
          ))}
        </ul>
      )}

      {sessions.hasNextPage ? (
        <button
          className="pill load-more"
          disabled={sessions.isFetchingNextPage}
          onClick={() => void sessions.fetchNextPage()}
        >
          {sessions.isFetchingNextPage ? 'Loading…' : 'Load more'}
        </button>
      ) : null}
      {remove.isError ? <p className="error">Delete failed: {remove.error.message}</p> : null}
    </div>
  );
}
```

- [ ] **Step 3: Session detail**

Replace `client/src/pages/SessionPage.tsx`:
```tsx
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CSSProperties } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { api, ApiError } from '../api';
import { ExportMenu } from '../components/ExportMenu';
import { TitleEditor } from '../components/TitleEditor';
import { formatClock, formatDate, sessionLabel } from '../format';
import { clearStoredSessionId, readStoredSessionId } from '../sessionStore';
import { COLORS } from '../transcript/reducer';

export function SessionPage() {
  const { id = '' } = useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const session = useQuery({
    queryKey: ['session', id],
    queryFn: () => api.getSession(id),
    // Show text as it is saved while the session is recording elsewhere.
    refetchInterval: (query) => (query.state.data?.live ? 3000 : false),
  });
  const remove = useMutation({
    mutationFn: () => api.deleteSession(id),
    onSuccess: () => {
      if (readStoredSessionId() === id) clearStoredSessionId();
      qc.removeQueries({ queryKey: ['session', id] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
      void navigate('/sessions');
    },
  });

  if (session.isPending) {
    return (
      <div className="page">
        <p className="muted">Loading…</p>
      </div>
    );
  }
  if (session.isError) {
    const notFound = session.error instanceof ApiError && session.error.status === 404;
    return (
      <div className="page">
        <Link to="/sessions" className="back">
          ← All sessions
        </Link>
        <p className="error">{notFound ? 'This session does not exist.' : `Couldn’t load session: ${session.error.message}`}</p>
      </div>
    );
  }

  const s = session.data;
  return (
    <div className="page">
      <Link to="/sessions" className="back">
        ← All sessions
      </Link>
      <header className="page-head">
        <h1>
          <TitleEditor sessionId={s.id} />
        </h1>
        {s.live ? <span className="live-badge">● live</span> : null}
      </header>
      <p className="session-meta">
        {formatClock(s.durationMs)} · {formatDate(s.createdAt)} · {s.recordings.length}{' '}
        {s.recordings.length === 1 ? 'recording' : 'recordings'}
      </p>

      <div className="actions">
        <button
          className="pill primary-pill"
          disabled={s.live}
          title={s.live ? 'This session is recording in another tab' : 'Continue recording into this session'}
          onClick={() => void navigate(`/?session=${s.id}`)}
        >
          Continue recording
        </button>
        <ExportMenu sessionId={s.id} />
        <button
          className="pill danger-pill"
          disabled={s.live || remove.isPending}
          onClick={() => {
            if (confirm(`Delete “${sessionLabel(s)}”? Its transcript and audio are removed.`)) remove.mutate();
          }}
        >
          Delete
        </button>
      </div>
      {remove.isError ? <p className="error">Delete failed: {remove.error.message}</p> : null}

      {s.paragraphs.length === 0 ? (
        <p className="muted">No transcript yet.</p>
      ) : (
        <div className="session-transcript">
          {s.paragraphs.map((p, i) => (
            <div key={p.index} className="line" style={{ '--c': COLORS[i % COLORS.length] } as CSSProperties}>
              <span className="stamp">{formatClock(p.startMs)}</span>
              {p.text}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 4: Verify**

```bash
cd client && pnpm typecheck && pnpm lint && pnpm test && pnpm build && cd ..
```
With the server running, open <http://127.0.0.1:8080/sessions>:
1. Sessions are listed newest first with preview, duration and date; a session being recorded in another tab shows **● live** and its delete button is disabled.
2. Open one: paragraphs have `m:ss` stamps; rename works and the list reflects it.
3. Each export link downloads a file named after the title (`test.srt`, …). Play the `.wav` in VLC with the `.srt` loaded: cues line up with speech.
4. **Continue recording** opens the recorder with the history shown; recording appends to it.
5. Open the same session's Continue link in a second tab while the first is recording: after ~5 s the second tab shows “This session is recording in another tab.”
6. Delete a session from the detail page → back on the list, it is gone, and `data/sessions/<id>` no longer exists.
7. Deep-link reload: refreshing `/sessions/<id>` in the browser still loads the page (SPA fallback).

- [ ] **Step 5: Commit**

```bash
git add client/src
git commit -m "feat(client): add session list and detail pages with export and delete"
```

---

### Task 12: Docs, final verification and latency check

**Files:**
- Modify: `README.md`, `CLAUDE.md`

- [ ] **Step 1: Update `README.md`**

1. Replace the `### 2. Build and run` section's code block with:

````markdown
### 2. Database

Sessions are stored in Postgres (transcript) and under `data/` (audio).

```sh
createdb transcribe_rust
cp .env.example .env        # DATABASE_URL=postgres://localhost/transcribe_rust
```

Migrations run automatically when the server starts.

### 3. Build and run

```sh
(cd client && pnpm install && pnpm build)   # the server serves client/dist

# macOS (Metal)
cargo run --release --features metal -- --model models/ggml-base.en-q5_1.bin

# NVIDIA
cargo run --release --features cuda -- --model models/ggml-base.en-q5_1.bin

# CPU only
cargo run --release -- --model models/ggml-base.en-q5_1.bin
```

For client development, run the server as above and `cd client && pnpm dev`,
then open <http://localhost:5173> (Vite proxies `/api` and `/ws` to :8080).
````

2. In the "Wire protocol" section, change the `start` example and the `ready` line to:

````markdown
```json
{"type":"start","sampleRate":16000,"encoding":"pcm_s16le","language":"en","sessionId":"0192…"}
```
````
and
```json
{"type":"ready","sessionId":"0192…","resumed":true,"offsetMs":84210}
```
and add these bullets to the server→client list:

```markdown
- `sessionId` in `start` is optional. Without it the server creates a session;
  with it the server resumes that session (waiting up to 5 s for a previous
  connection on it to finish), or replies `session_not_found` / `session_busy`.
- `ready.offsetMs` is where this connection starts on the session timeline.
  Wire timestamps stay relative to the connection's own stream.
- `{"type":"error","code":"persist_failed"}` means Postgres is unreachable;
  transcription continues and audio is still written to disk.
- The server pings every 15 s and closes a socket that has been silent for 45 s.
```

3. Add a new section before "## Architecture":

````markdown
## Sessions

Every connection records into a session. A session has one or more
*recordings* (one per connection: the first, plus one per resume), laid end to
end on a single timeline, so exported subtitles line up with exported audio.

| Store | What |
|---|---|
| Postgres `sessions`, `recordings`, `transcript_chunks` | metadata and every committed chunk, with session-timeline timestamps |
| `data/sessions/{session}/{recording}.pcm` | raw 16 kHz Int16 mono, exactly as received; append-only, so crash-safe |

A recording left open by a crash is repaired from its file size the next time
the session is resumed or exported.

### REST API

| Method | Path | |
|---|---|---|
| GET | `/api/sessions?limit=50&before=<id>` | newest first, keyset paging |
| GET | `/api/sessions/{id}` | metadata, recordings, paragraphs |
| PATCH | `/api/sessions/{id}` | `{"title": "…" \| null}` |
| DELETE | `/api/sessions/{id}` | `409 session_live` while recording |
| GET | `/api/sessions/{id}/export/{txt\|srt\|vtt\|json\|wav}` | download |

> There is no authentication. The server binds to `127.0.0.1` by default; if
> you bind it to another interface, anyone who can reach it can read and
> delete every session.
````

4. In "## Architecture", replace the file tree with:

```
crates/server/src/
  main.rs         wiring, DB connect, static file serving, graceful shutdown
  ws.rs           axum handler; 4 tasks per connection + admission control
  pipeline.rs     VAD, rolling buffer, latest-wins scheduling, emission
  recorder.rs     per-connection persistence: PCM file + batched chunk inserts
  sessions.rs     open/resume a session, audio file layout, crash repair
  live.rs         in-memory "which sessions have a socket" registry
  db.rs           every SQL query (sqlx, compile-checked)
  api.rs          REST API
  export.rs       txt / srt / vtt / json / wav formatting
  vad.rs          Silero VAD over fixed 32 ms frames
  stabilizer.rs   LocalAgreement-2
  whisper.rs      blocking inference pool
  audio.rs        Int16 -> f32, optional resampling
  protocol.rs     serde types for the wire protocol
  config.rs       CLI / env configuration
crates/server/migrations/   SQL migrations, applied on boot
client/                     Vite + React + TypeScript
  src/transcript/   reducer, reconnect helpers, LiveSession (mic + socket)
  src/pages/        recorder, session list, session detail
  public/audio-worklet.js   capture, downsample to 16 kHz, Int16 framing
```

and replace the "Per connection, three concurrent tasks:" list with:

```markdown
Per connection, four concurrent tasks:

1. **Receiver** — WebSocket binary frames → `mpsc` to the pipeline *and* to the
   recorder. Both channels are bounded and awaited, so a slow consumer
   backpressures the socket rather than dropping audio.
2. **Pipeline** — resample if needed → VAD → maintain the active buffer (audio
   since the last commit) → schedule decodes latest-wins → LocalAgreement-2 →
   emit transcript JSON, and forward each commit to the recorder over an
   unbounded channel, so persistence can never stall decoding.
3. **Sender** — `mpsc` of transcript messages → WebSocket text frames, plus a
   ping every 15 s.
4. **Recorder** — appends audio to the recording's PCM file and writes commits
   to Postgres in batches (every 500 ms, 32 rows, or end of utterance). On
   close it records the duration and releases the session for the next
   connection.
```

5. In "## Tests", replace the code block and paragraph with:

````markdown
```sh
export DATABASE_URL=postgres://localhost/transcribe_rust
cargo test                       # DB tests create a throwaway database each
(cd client && pnpm test)
```

Server unit tests cover protocol serialization, the LocalAgreement-2 state
machine, PCM conversion and resampling, the live registry and every export
format. `#[sqlx::test]` integration tests cover the recorder, session resume and
crash repair, and the REST API. Client tests cover the transcript reducer and
reconnect helpers.
````

- [ ] **Step 2: Update `CLAUDE.md`**

1. Replace the `## Commands` code block with:

````markdown
```sh
createdb transcribe_rust && cp .env.example .env   # once
export DATABASE_URL=postgres://localhost/transcribe_rust   # needed by cargo test
cargo run --release --features metal -- --model models/ggml-base.en-q5_1.bin
cargo test                                      # includes #[sqlx::test] DB tests
cargo clippy --all-targets -- -D warnings
cargo fmt
cargo sqlx prepare --workspace                  # after ANY SQL change; commit .sqlx/
cargo sqlx migrate add -r <name> --source crates/server/migrations   # new migration
cd client && pnpm dev                           # :5173, proxies /api and /ws to :8080
cd client && pnpm build                         # server serves client/dist
cd client && pnpm typecheck && pnpm lint && pnpm test
```
````

2. Replace the `## Layout` tree with the same tree as README's Architecture section (Step 1.4).

3. Add to `## Invariants — do not break`:

```markdown
- **Persistence never blocks decoding.** The pipeline sends commits to the
  recorder over an *unbounded* channel and never awaits it. DB and disk I/O live
  only in `recorder.rs` (and the API).
- **One live socket per session**, enforced by `live.rs` (in memory, never a DB
  column). Anything that mutates a session's recordings or files — resume
  repair, delete, export repair — must hold a `LiveGuard`.
- **Stored timestamps are on the session timeline** (`recording.offset_ms +
  stream ms`); wire timestamps stay stream-relative.
- **All SQL lives in `db.rs`** and uses the `query!`/`query_as!` macros. After
  changing SQL run `cargo sqlx prepare --workspace` and commit `.sqlx/`.
```

4. Replace the "Client is plain HTML/JS" convention bullet with:

```markdown
- Client is Vite + React 19 + TypeScript (strict) + React Router 8 + TanStack
  Query. Wire types in `client/src/protocol.ts` mirror `protocol.rs` by hand;
  change both together. Browser/audio/socket logic stays in plain classes
  (`transcript/liveSession.ts`); components see callbacks and reducer state.
- Versions pinned below current majors on purpose: sqlx 0.8 (0.9 needs rustc
  1.94) and TypeScript 6.0 (typescript-eslint supports <6.1).
```

- [ ] **Step 3: Full verification**

```bash
export DATABASE_URL=postgres://localhost/transcribe_rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
SQLX_OFFLINE=true cargo build --release --features metal   # proves .sqlx is complete
cd client && pnpm typecheck && pnpm lint && pnpm test && pnpm build && cd ..
```
Expected: every command succeeds. Record the test counts in the commit message body.

- [ ] **Step 4: Latency regression check**

Run the server with `RUST_LOG=transcribe_server=debug`, speak three sentences in the React client, and read `took_ms` from the log and `first text in N ms` from the status line. Compare against README "Measured" (first text ≈ 149 ms, median decode ≈ 64 ms on this hardware). Expected: within ~10 % (noise). If first-text regressed, check that `RecorderPage` is not re-rendering per animation frame (React DevTools profiler) before suspecting the server.

- [ ] **Step 5: Manual acceptance (spec §10)**

Walk through the five checks in the spec's "Manual acceptance" list; each was exercised in Tasks 10–11 — repeat them on the final build and note any failure as a bug to fix before finishing.

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: document sessions, REST API, client workflow and new invariants"
```

# Postgres-backed sessions + React client — design

Date: 2026-10-03
Status: approved in conversation, pending written-spec review
Branch: `feat/postgres-sessions` (off `develop`)

## 1. Goal

Persist transcription sessions server-side so that a user can:

1. **Browse** past transcripts (list, open, rename, delete).
2. **Resume** a session — automatically after a dropped socket or page reload,
   and manually by picking any past session and continuing to record into it.
3. **Archive / export** a session as `.txt`, `.srt`, `.vtt`, `.json` or `.wav`.

At the same time, rewrite the browser client as a React + TypeScript app.

### Constraints

- **Latency is the product.** Persistence must add nothing measurable to
  time-to-first-text or decode time. No DB or disk I/O on the pipeline hot path;
  the pipeline never blocks on persistence.
- **Audio is never dropped** (existing invariant) — this extends to the audio
  file: it receives every frame the server receives.
- Single user, **no authentication**. Default bind stays `127.0.0.1`.
- Wire protocol changes are backward compatible (new fields optional).

### Non-goals (YAGNI)

Accounts/auth, full-text search, tags, soft delete, persisting `pending` text,
multi-node deployment, docker-compose, re-transcription of stored audio.

## 2. Decisions

| Topic | Decision |
|---|---|
| Storage split | Transcript in Postgres; audio as files on disk |
| DB library | `sqlx` (postgres, compile-checked `query!`, built-in migrations), offline cache `.sqlx/` committed |
| Database | Local Homebrew Postgres 17; databases `transcribe_rust` and `transcribe_rust_test`. The pre-existing `realtime_transcription` DB is unrelated and untouched |
| Persistence architecture | One **recorder task** per connection (4th task beside receiver/pipeline/sender) |
| Audio format on disk | Raw `s16le` PCM per recording; WAV header synthesized at export time |
| Resume modes | Both automatic (socket drop / reload) and manual (from session list) |
| Concurrency per session | At most one live socket; reconnect waits up to 5 s for the lock |
| DB down at startup | Refuse to start |
| DB down mid-session | Retry, then emit `persist_failed` once; transcription and audio capture continue |
| Client | Full rewrite: Vite + React 19 + TypeScript (strict) + React Router v7 + TanStack Query, pnpm, plain CSS |

## 3. Data model

Migration `crates/server/migrations/0001_sessions.sql`:

```sql
create table sessions (
  id           uuid primary key,                 -- UUIDv7, generated server-side (time-sortable)
  title        text,                             -- null → UI shows created date
  language     text not null,                    -- language of the first recording
  duration_ms  bigint not null default 0,        -- total recorded audio across recordings
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

create table recordings (                          -- one per WebSocket connection
  id           uuid primary key,                 -- UUIDv7
  session_id   uuid not null references sessions(id) on delete cascade,
  ordinal      int not null,                     -- 0, 1, 2, … in session order
  offset_ms    bigint not null,                  -- session duration_ms when this recording began
  sample_rate  int not null,
  duration_ms  bigint,                           -- null until closed (or repaired)
  started_at   timestamptz not null default now(),
  ended_at     timestamptz,
  unique (session_id, ordinal)
);

create table transcript_chunks (                   -- one row per wire `commit`
  id           bigserial primary key,
  session_id   uuid not null references sessions(id) on delete cascade,
  recording_id uuid not null references recordings(id) on delete cascade,
  paragraph    int not null,                     -- session-wide paragraph index
  text         text not null,
  start_ms     bigint not null,                  -- session timeline
  end_ms       bigint not null
);
create index transcript_chunks_session_idx on transcript_chunks (session_id, id);
```

### Timeline

- Every stored timestamp is on the **session timeline**:
  `recording.offset_ms + <stream-relative ms from the wire commit>`.
- `recording.offset_ms` = `sessions.duration_ms` at the moment the recording
  starts. Recordings are therefore contiguous on the timeline, and concatenating
  their audio in `ordinal` order reproduces it exactly.
- The wire protocol keeps **stream-relative** timestamps (unchanged).

### Paragraphs

- A paragraph is the run of commits between `endOfSegment: true` events.
- `paragraph` is session-wide. It increments after each `EndOfSegment` and at the
  start of every new recording (a resumed recording always opens a new
  paragraph). On resume the recorder starts at `coalesce(max(paragraph), -1) + 1`.
- Paragraph text = its chunks ordered by `id`, joined with a single space —
  identical to how `Stabilizer::full_text` and the client join commits.

### Liveness

Whether a session is live is **in memory only** (`LiveRegistry` in `AppState`),
never a DB column, so a crash cannot leave a session stuck "live".

### Audio files

- `--data-dir` / `TR_DATA_DIR`, default `data/` (gitignored).
- Path: `{data_dir}/sessions/{session_id}/{recording_id}.pcm`.
- Raw little-endian Int16 mono at `recording.sample_rate` — exactly the bytes
  the client sent. Append-only, so crash-safe.
- A recording's true duration is `file_len / 2 / sample_rate`. On close the
  recorder writes this to `recordings.duration_ms` and adds it to
  `sessions.duration_ms`. A recording left with `duration_ms IS NULL` (server
  crash) is **repaired** when the session is next resumed or exported: duration
  is computed from the file size and the session duration recomputed as the sum.
- Deleting a session deletes its rows (cascade) and then its directory.

## 4. Wire protocol changes

```jsonc
// client → server (sessionId optional)
{"type":"start","sampleRate":16000,"encoding":"pcm_s16le","language":"en","sessionId":"0192…"}

// server → client
{"type":"ready","sessionId":"0192…","resumed":true,"offsetMs":84210}
```

- `start` without `sessionId` → create session + recording ordinal 0;
  `ready.resumed = false`, `offsetMs = 0`.
- `start` with `sessionId`:
  1. Session must exist → else `error{code:"session_not_found"}` and close.
  2. Claim the live lock, waiting up to **5 s** → else
     `error{code:"session_busy"}` and close.
  3. Repair any unclosed recordings (§3), create recording `max(ordinal)+1`
     with `offset_ms = sessions.duration_ms`.
- New error codes: `session_not_found`, `session_busy`, `persist_failed`.
- `transcript`, `error`, `done` unchanged. `protocol.rs` tests and README
  updated.

### Keepalive

The server sends a WebSocket Ping every **15 s** and closes the connection if no
inbound frame of any kind (audio, text, pong) arrives for **45 s**. Browsers
answer pings automatically, so a paused client stays connected while a
half-open socket releases its live lock.

## 5. Server architecture

### New modules

```
crates/server/
  migrations/0001_sessions.sql
  .sqlx/                 offline query cache (committed)
  src/db.rs              pool setup, migrate on boot, all SQL
  src/live.rs            LiveRegistry: claim(id, timeout) -> Result<LiveGuard, Busy>; guard releases on Drop
  src/recorder.rs        per-connection persistence task
  src/api.rs             REST handlers
  src/export.rs          pure export functions
```

### Recorder task

Inputs:

| Channel | Payload | Kind | Rationale |
|---|---|---|---|
| `audio` | `bytes::Bytes` (the binary frame) | bounded (`--audio-queue`), sender awaits | never drop audio; backpressure the socket like the pipeline channel |
| `events` | `RecorderEvent::{Commit{text,start_ms,end_ms}, EndOfSegment}` | **unbounded** | a few tiny messages/s; the pipeline must never block on persistence |

Behaviour:

- Audio frames are appended via `tokio::io::BufWriter<tokio::fs::File>`.
- Commits are buffered and flushed as one multi-row `INSERT … SELECT * FROM
  UNNEST(…)` when any of: 500 ms elapsed since the first buffered row, 32 rows
  buffered, or an `EndOfSegment` arrives.
- `offset_ms` is added to `start_ms`/`end_ms` before insert.
- DB write failure: retry 3× with 100/400/1600 ms backoff, keeping the rows
  buffered. If still failing, send `persist_failed` to the client once (via the
  outbound message channel) and keep buffering/retrying on subsequent flushes;
  audio continues to be written regardless.
- On close (both input channels closed): final flush, `flush()` + `sync_all()`
  the file, set `recordings.duration_ms`/`ended_at`, add to
  `sessions.duration_ms`, bump `sessions.updated_at`, then drop the `LiveGuard`
  (waking any waiting reconnect).

### Changes to existing code

- **`ws.rs`**
  - Handshake: after parsing `start`, resolve/create the session, claim the
    lock, create the recording, spawn the recorder, then send `ready`.
  - Receiver: each binary frame goes to the pipeline (as today) and, as a
    `Bytes` clone, to the recorder's audio channel. Any inbound frame resets the
    idle deadline.
  - Sender also emits Pings on a 15 s interval.
  - Teardown order: receiver ends → drop pipeline audio sender → await pipeline
    (its final commits are already queued to the recorder) → drop recorder
    senders → await recorder → guard released → await sender.
- **`pipeline.rs`**: one new field `recorder: mpsc::UnboundedSender<RecorderEvent>`.
  `emit()` forwards `commit` (if any) and `end_of_segment` to it. Scheduling,
  VAD, trimming and the stabilizer are untouched.
- **`AppState`**: adds `db: PgPool`, `live: LiveRegistry`, `shutdown:
  CancellationToken`, `tracker: TaskTracker`.
- **`main.rs`**: load `.env` (`dotenvy`), connect pool, run migrations, create
  `data_dir`, then bind. Graceful shutdown: on Ctrl-C cancel the token (receiver
  loops exit as if the client stopped), stop accepting, and wait up to 5 s on the
  tracker for pipelines and recorders to finish.
- **`config.rs`**: `--database-url` (`DATABASE_URL`, required),
  `--data-dir` (`TR_DATA_DIR`, default `data`), `--db-max-connections`
  (`TR_DB_MAX_CONNECTIONS`, default 8); `--client-dir` default becomes
  `client/dist`.

### New dependencies

`sqlx` (features: `runtime-tokio`, `tls-rustls`, `postgres`, `uuid`, `chrono`,
`migrate`, `macros`), `uuid` (`v7`, `serde`), `chrono` (`serde`), `tokio-util`
(`rt` for `TaskTracker`/`CancellationToken`, `io` for `ReaderStream`), `bytes`,
`dotenvy`. Dev: `tower` (`util`) for handler tests, `tempfile`.

## 6. REST API

JSON is camelCase. Errors: `{"error":{"code":"…","message":"…"}}` with 404 /
409 / 422.

| Method | Path | Response |
|---|---|---|
| GET | `/api/sessions?limit=50&before=<id>` | `[{id, title, language, durationMs, createdAt, updatedAt, live, preview}]`, newest first; keyset on `id` (UUIDv7); `limit` ≤ 200; `preview` = first 160 chars of session text |
| GET | `/api/sessions/:id` | `{id, title, language, durationMs, createdAt, updatedAt, live, recordings:[{id, ordinal, offsetMs, durationMs, sampleRate, startedAt, endedAt}], paragraphs:[{index, startMs, endMs, text}]}` |
| PATCH | `/api/sessions/:id` | body `{title: string \| null}` (trimmed, ≤ 200 chars, empty → null) → updated session summary |
| DELETE | `/api/sessions/:id` | 204; **409 `session_live`** if live |
| GET | `/api/sessions/:id/export.{txt,srt,vtt,json,wav}` | file download with `Content-Disposition: attachment; filename="…"` |

Exports work on live sessions and reflect what has been persisted so far.

## 7. Export formats (`export.rs`, pure functions)

- **Filename**: slugified title (`[a-z0-9-]`, ≤ 60 chars), or
  `session-YYYY-MM-DD-HHMM` (UTC created time) when untitled; plus extension.
- **txt**: paragraphs separated by a blank line, trailing newline.
- **Cues (srt/vtt)** — `build_cues(paragraphs_with_chunks)`:
  - Accumulate consecutive chunks of one paragraph into a cue; start a new cue
    when adding the next chunk would make the cue exceed **7000 ms**
    (`end - start`) or **84 characters**. Never cross a paragraph boundary.
    A single chunk exceeding the limits becomes its own cue.
  - Cue start = first chunk `start_ms`, end = last chunk `end_ms`, then clamp
    `end ≤ next cue start` and ensure `end > start` (min 1 ms).
  - Text wrapped greedily at word boundaries to ≤ 42 chars per line, max 2
    lines (overflow kept on line 2 rather than dropped).
- **srt**: `N\nHH:MM:SS,mmm --> HH:MM:SS,mmm\ntext\n\n`, numbered from 1.
- **vtt**: `WEBVTT\n\n` then `HH:MM:SS.mmm --> HH:MM:SS.mmm\ntext\n\n`.
- **json**: `{session:{…as GET…}, recordings:[…], paragraphs:[{index, startMs,
  endMs, text, chunks:[{text, startMs, endMs}]}]}`.
- **wav**: 44-byte PCM WAV header (mono, 16-bit, `sample_rate`) with data length
  = sum of the recordings' current file sizes (each rounded down to an even
  byte count), followed by the files streamed in `ordinal` order via
  `ReaderStream`, each truncated to its counted length. If recordings have
  differing sample rates → 422 `mixed_sample_rates`. Missing file → treated as
  0 bytes and logged.

## 8. React client

### Layout

```
client/
  index.html  vite.config.ts  tsconfig.json  package.json  eslint.config.js
  public/audio-worklet.js            unchanged worklet, loaded via audioWorklet.addModule('/audio-worklet.js')
  src/
    main.tsx  App.tsx                router + QueryClientProvider
    protocol.ts                      wire types (discriminated unions) mirroring protocol.rs
    api.ts                           typed fetch wrappers for /api/sessions*
    transcript/reducer.ts            pure transcript state machine (+ reducer.test.ts)
    hooks/useTranscriber.ts          mic, AudioContext, worklet, WebSocket, reconnect, offline buffer
    hooks/useLevelMeter.ts           smoothed RMS (attack 0.35, release 0.08, as today)
    hooks/useAutoScroll.ts           stick within 48 px, disengage on scroll-up
    pages/RecorderPage.tsx           `/` and `/?session=:id`
    pages/SessionsPage.tsx           `/sessions`
    pages/SessionPage.tsx            `/sessions/:id`
    components/…                     Transcript, Controls, ExportMenu, TitleEditor, ConnectionBadge
    styles/app.css                   ported from today's index.html <style>
```

### Transcript reducer

State: `{paragraphs: {text, color}[], pending: string, open: boolean, lastSeq}`.
Actions: `seed(paragraphs)`, `transcript(msg)`, `reset()`.
- `commit.text` appended to the open paragraph (space-joined), opening one if
  none is open; `pending` replaced; `endOfSegment` closes the paragraph and
  clears `pending`. Messages with `seq ≤ lastSeq` are ignored; `lastSeq` resets
  on each new connection (`ready`).
- Colour cycles through the existing 4-colour palette per paragraph.

### `useTranscriber`

Exposes `{status, state, sessionId, firstTextMs, start(sessionId?), stop,
pause, resume, setMuted, setAutoDetect}` where `status` ∈ `idle | connecting |
live | paused | reconnecting | stopping | error`.

- Sends `start` with `sessionId` when resuming; stores `ready.sessionId` in
  `localStorage['transcribe.currentSessionId']`.
- **Auto-reconnect**: on a socket close that was not preceded by `done` or a
  user Stop, reconnect with the same `sessionId`, backoff 0.5, 1, 2, 4, 8, 10,
  10… s. `session_busy` during reconnect is retried; `session_not_found` is
  terminal.
- **Offline buffer**: mic capture continues while reconnecting; frames are
  queued up to 30 s of audio (960 000 bytes at 16 kHz) and flushed after
  `ready`. On overflow the oldest frames are dropped and a gap notice is shown.
- Pause gates sending only (as today). Level meter and scroll state live in
  refs / rAF, not React state.

### Pages

- **Recorder** — faithful port of the current UI (splash, bottom-anchored
  transcript, committed solid / pending grey italic, pause, level-meter mute,
  auto-scroll, copy, fullscreen, language-detect pill, first-text status).
  Changes: **Clear → New session** (clears the stored id; old session stays in
  the DB); transcript text no longer stored in `localStorage`. Additions:
  inline title editor, Sessions link, connection badge (reconnecting / gap),
  **Continue** when opened on a session with history (history loaded via
  `GET /api/sessions/:id` and used to `seed` the reducer).
- **Sessions** — list with title-or-date, preview, duration, created time, live
  badge; "Load more" via `before`; delete with confirm (disabled when live);
  row → detail.
- **Session detail** — paragraphs with `mm:ss` start stamps, title rename,
  **Continue recording** → `/?session=:id`, Export menu (`<a download>` links
  to the five export URLs), delete. Refetch every 3 s while `live`.

### Tooling

- `pnpm dev`: Vite on :5173 proxying `/api` and `/ws` (ws: true) to :8080.
- `pnpm build` → `client/dist`, served by axum (existing SPA fallback to
  `index.html`).
- Scripts: `typecheck`, `lint`, `test` (Vitest).
- Versions confirmed against current docs (Context7) at scaffold time.

## 9. Dev setup

```sh
createdb transcribe_rust && createdb transcribe_rust_test
cp .env.example .env               # DATABASE_URL=postgres://localhost/transcribe_rust
cargo install sqlx-cli --no-default-features --features postgres,rustls
cargo run --release --features metal
cd client && pnpm install && pnpm dev
```

`.gitignore` adds `.env`, `data/`, `client/dist/`, `client/node_modules/`.
`cargo sqlx prepare` must be re-run whenever SQL changes (documented in
CLAUDE.md). README and CLAUDE.md updated with setup, the new modules, the
security note about binding beyond localhost, and the persistence invariants.

## 10. Testing

### Server

| Kind | Coverage | DB |
|---|---|---|
| Unit | `export.rs`: cue limits (7 s / 84 chars), paragraph boundaries, overlap clamp, SRT/VTT formatting, wrapping, WAV header bytes, filename slugs | no |
| Unit | `live.rs`: claim, release on drop, wait-then-acquire, busy after timeout (paused tokio time) | no |
| Unit | `protocol.rs`: `start` with/without `sessionId`; `ready` serialization | no |
| Integration | `#[sqlx::test(migrations = "./migrations")]` against `transcribe_rust_test`: recorder writes chunks with offset; resume → ordinal 1, correct `offset_ms`, paragraph continuation; repair of unclosed recording; cascade delete + directory removal; API handlers via `tower::ServiceExt::oneshot` (list paging, get, patch, delete 409 when live, each export content-type) | yes |
| Existing | stabilizer, pipeline, VAD, audio tests stay green | no |

The recorder is constructed from a `PgPool`, a directory and channels, so it is
tested without Whisper.

### Client (Vitest)

Reducer (same-tick commit+pending, endOfSegment, seed, stale `seq`), reconnect
backoff schedule and offline-buffer overflow (fake timers).

### Manual acceptance

1. Record → session appears in list → all five exports download; `.srt` aligns
   with `.wav` in a player.
2. Kill and restart the server mid-sentence → client reconnects into the same
   session; paragraphs and timeline continue.
3. Reload → Continue → appends to the same session.
4. Same session in two tabs → second gets `session_busy` after 5 s.
5. First-text and `took_ms` unchanged within noise vs README numbers.

## 11. Delivery

Feature branch `feat/postgres-sessions`, roughly four commits:
1. DB, live registry, recorder, protocol + ws wiring.
2. REST API and exports.
3. React client.
4. Docs (README, CLAUDE.md, `.env.example`).

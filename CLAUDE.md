# CLAUDE.md

Near-realtime speech transcription: a Rust/axum server streams microphone PCM
over one WebSocket, gates it with Silero VAD, decodes with whisper.cpp
(`whisper-rs`), stabilizes with LocalAgreement-2, and streams `commit`/`pending`
text back to a Vite/React browser client. Sessions persist to Postgres
(transcript) and `data/` (audio). `README.md` is the authoritative deep
dive (wire protocol, latency levers, measured numbers) — read it before
changing pipeline behavior.

**Design priority: low perceived latency (time-to-first-visible-text) over
maximum accuracy.** Any change that adds work to the hot path must justify its
latency cost.

## Commands

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
node scripts/smoke-ws.mjs                       # WebSocket smoke test against a running server (Node 24)
RUST_LOG=transcribe_server=debug cargo run ...  # per-decode `took_ms` for capacity tuning
```

First build compiles whisper.cpp (needs `cmake` + C++ toolchain, several
minutes). Models live in `models/` (gitignored); the server opens at
<http://127.0.0.1:8080>. `DATABASE_URL` is required (no default; `.env` is
loaded if present). `--client-dir` defaults to the relative path `client/dist`,
so run the server from the repo root and `cd client && pnpm build` first.

## Layout

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

## Invariants — do not break

- **Never run blocking work on a tokio worker.** Whisper inference goes through
  the `whisper.rs` pool (crossbeam channel → OS threads → oneshot back). Any new
  blocking or slow I/O must stay off the pipeline task's hot path too.
- **Audio is never dropped.** The receiver `await`s on a bounded channel so a
  slow pipeline backpressures the socket. Only redundant decode *passes* are
  dropped (latest-wins, at most one decode in flight per connection).
- **`commit` and `pending` come from the same decode tick.** `commit` is an
  append-only delta; `pending` has replace semantics. The client relies on this
  to avoid duplicated text at the boundary.
- **Wire protocol changes must be backward compatible** (or update client +
  README + `protocol.rs` tests together). Field names are camelCase on the wire.
- **Stabilizer alignment is lexical, not by word index** — the decoder
  re-tokenizes between passes. Regression tests in `stabilizer.rs` cover dropped
  and re-committed words; keep them passing and add one for any new edge case.
- **Persistence never blocks decoding.** The pipeline sends commits to the
  recorder over an *unbounded* channel and never awaits it. DB and disk I/O live
  only in `recorder.rs` (and the API). The recorder drains audio in its own
  internal writer task, so Postgres retries never hold up audio either.
- **One live socket per session**, enforced by `live.rs` (in memory, never a DB
  column). Anything that mutates a session's recordings or files — resume
  repair, delete, export repair — must hold a `LiveGuard`. The server closes the
  socket only after the recorder has released the session.
- **Stored timestamps are on the session timeline** (`recording.offset_ms +
  stream ms`); wire timestamps stay stream-relative.
- **All SQL lives in `db.rs`** and uses the `query!`/`query_as!` macros. After
  changing SQL run `cargo sqlx prepare --workspace` and commit `.sqlx/`.
- Admission control (`--max-connections`) rejects with 503 rather than degrading
  existing streams.

## Conventions

- Rust 2021, workspace with a single crate (`crates/server`). `anyhow` for
  errors at the edges, `tracing` for logs (structured fields, not format
  strings).
- New tunables go in `config.rs` as clap args with a doc comment explaining the
  latency/cost trade-off; add a `TR_*` env var for anything deploy-relevant.
- Tests live next to the code in `#[cfg(test)] mod tests`.
- Client is Vite + React 19 + TypeScript (strict) + React Router 8 + TanStack
  Query. Wire types in `client/src/protocol.ts` mirror `protocol.rs` by hand;
  change both together. Browser/audio/socket logic stays in plain classes
  (`transcript/liveSession.ts`); components see callbacks and reducer state.
- Versions pinned below current majors on purpose: sqlx 0.8 (0.9 needs rustc
  1.94) and TypeScript 6.0 (typescript-eslint supports <6.1).
- Commits: conventional style (`feat:`, `fix:`, `chore:`, `feat(client):`). No
  Claude attribution lines.

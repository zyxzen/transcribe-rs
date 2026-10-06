# transcribe-rust

Near-realtime speech transcription over a single WebSocket: Rust + axum on the
server, whisper.cpp for inference, Silero VAD for gating, and a browser client
that streams microphone audio in and renders text out as it firms up.

The design target is **low perceived latency**, not maximum accuracy. An
unstable hypothesis is streamed the moment the decoder produces one; confirmed
text lags behind it invisibly.

## How it feels realtime

| Lever | Where |
|---|---|
| **First pass of an utterance fires on `min_decode_ms`, not a full interval** | `pipeline.rs::should_submit` |
| **Encoder sized to the audio present instead of the padded 30 s window** | `whisper.rs::audio_ctx_for` |
| Decode every ~200 ms of new audio, stream the result as `pending` | `pipeline.rs` |
| Commit *immediately* on VAD end-of-utterance, don't wait for the next tick | `pipeline.rs::request_finalize` |
| Never run Whisper on silence (saves compute, prevents hallucination) | `vad.rs` + the active-buffer gate |
| Latest-wins scheduling: at most one decode in flight; redundant *passes* are dropped, audio never is | `pipeline.rs::run` |
| Trim committed audio out of the buffer so re-decodes stay cheap | `pipeline.rs::maybe_trim` |
| Flash attention, warm-up inference per worker at startup | `whisper.rs` |
| Greedy decode, single segment, no temperature fallback, `no_context` | `whisper.rs::run_decode` |

The top two are worth spelling out, because both were worth more than any
threshold tweak:

- **`audio_ctx`.** whisper.cpp pads every input to 30 s and runs its encoder over
  the whole thing, so decoding a 2 s window costs the same as decoding 30 s.
  Sizing the encoder to the buffer (20 ms of audio per encoder position, with a
  floor) cut median decode time from 142 ms to 64 ms.
- **Early first pass.** `decode_interval_ms` used to gate the *first* decode of
  an utterance too, so nothing appeared until a full interval of speech had
  accumulated — `min_decode_ms` had no effect on perceived latency at all. The
  first pass now fires as soon as there is enough audio to decode. This decouples
  time-to-first-text from steady-state cost: 149 ms to first text while still
  only decoding every 200 ms.

Perceived latency is time-to-first-visible-text. `commit` and `pending` are
always produced by the **same decode tick**, so the client can append the delta
and replace the tail with no risk of duplicated text at the boundary.

## Wire protocol

One WebSocket at `/ws`, bidirectional.

**Client → server.** A JSON handshake first, then binary frames:

```json
{"type":"start","sampleRate":16000,"encoding":"pcm_s16le","language":"en","sessionId":"0192…"}
```

followed by raw little-endian Int16 mono PCM, ~100–250 ms per frame. Frame
boundaries do not have to align to sample boundaries. `{"type":"stop"}` ends the
stream cleanly.

**Server → client.** Text frames only:

```json
{"type":"ready","sessionId":"0192…","resumed":true,"offsetMs":84210}
{"type":"transcript","seq":7,"commit":{"text":"hello there","start":120,"end":940},"pending":"how are","endOfSegment":false,"language":"en"}
{"type":"transcript","seq":8,"pending":"how are you","endOfSegment":false,"language":"en"}
{"type":"error","code":"decode_failed","message":"..."}
{"type":"done","text":"hello there how are you"}
```

- `commit` is an **append-only delta**, omitted on ticks that produced no new
  stable text.
- `pending` is always present and has **replace** semantics.
- `endOfSegment: true` means the utterance ended — render a paragraph break.
- Timestamps are absolute milliseconds from stream start.
- `sessionId` in `start` is optional. Without it the server creates a session;
  with it the server resumes that session (waiting up to 5 s for a previous
  connection on it to finish), or replies `session_not_found` / `session_busy`.
- `ready.offsetMs` is where this connection starts on the session timeline.
  Wire timestamps stay relative to the connection's own stream.
- Error codes:

  | Code | Meaning | Closes socket |
  |---|---|---|
  | `bad_handshake` / `handshake_timeout` | first frame wasn't a valid `start` | yes |
  | `unsupported_encoding` | `encoding` other than `pcm_s16le` | yes |
  | `session_not_found` / `session_busy` | resume target missing, or still live elsewhere | yes |
  | `init_failed` | server couldn't open the session or its audio file | yes |
  | `bad_control` | unparseable control message after `start` | no |
  | `decode_failed` | one decode pass failed and was dropped; streaming continues | no |
  | `persist_failed` | Postgres unreachable; transcription continues and audio is still written to disk | no |
- The server pings every 15 s and closes a socket that has been silent for 45 s.
- The server closes the socket only after the recorder has released the
  session, so `done` followed by close happens after the final save.

## Running it

### 1. Get a model

```sh
mkdir -p models
curl -L -o models/ggml-base.en-q5_1.bin \
  https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en-q5_1.bin
```

`base.en-q5_1` is the latency-first default. `small.en-q5_1` is noticeably more
accurate and roughly 3× the compute — swap it in with `--model` once you've
measured RTF on your hardware.

### 2. Database

Sessions are stored in Postgres (transcript) and under `data/` (audio).

```sh
createdb transcribe_rust
cp .env.example .env        # DATABASE_URL=postgres://localhost/transcribe_rust
```

`DATABASE_URL` is required (there is no default); `.env` is loaded if present.
This build has no Postgres TLS support: use a local (or otherwise trusted-network)
Postgres; `sslmode=require` URLs are not supported.
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

Run the server from the repo root: `--client-dir` defaults to the relative path
`client/dist`, so the client must be built before it will be served.

For client development, run the server as above and `cd client && pnpm dev`,
then open <http://localhost:5173> (Vite proxies `/api` and `/ws` to :8080).

Building whisper-rs compiles whisper.cpp, so the first build needs `cmake` and a
C++ toolchain and takes a few minutes.

Then open <http://127.0.0.1:8080> and press the mic. Committed text renders
solid; the pending hypothesis renders grey and italic. The status line shows
measured time-to-first-text. Stopping keeps the session on screen; pressing
again continues it as a new recording. **Past sessions** (`/sessions`) lists
everything recorded, with rename, export and delete on each session's page.

To check the socket without a browser, run `node scripts/smoke-ws.mjs` (Node 24)
against a running server: it opens a session, streams 2 s of silence and prints
every server message. Pass a session id to exercise resume; `WS_URL` overrides
the endpoint. `GET /healthz` returns `ok`.

> The browser only grants microphone access on a secure origin. `localhost`
> counts. To reach the server from another machine, put it behind TLS.

### Tuning

Every latency knob is a CLI flag (`--help` for the full list). The ones worth
touching first:

| Flag | Default | Effect |
|---|---|---|
| `--min-decode-ms` | 120 | **Sets time-to-first-text.** Fires the first pass |
| `--decode-interval-ms` | 200 | Steady-state cadence and cost |
| `--end-silence-ms` | 500 | Trailing silence that ends an utterance |
| `--vad-threshold` | 0.5 | Silero speech probability cutoff |
| `--audio-ctx-floor` | 256 | Smallest encoder context; `--no-audio-ctx` disables sizing |
| `--flash-attn` | true | `--flash-attn false` to disable |
| `--trim-after-ms` | 6000 | Committed audio that accumulates before a trim |
| `--trim-context-ms` | 1000 | Committed audio kept after a trim, as context |
| `--workers` | 1 on GPU | Concurrent decodes |
| `--threads` | 2 on GPU | whisper.cpp threads per decode |
| `--preroll-ms` | 300 | Audio kept before speech onset so word onsets aren't clipped |
| `--max-segment-ms` | 24000 | Forces a flush on a run-on utterance (Whisper's window is 30 s) |
| `--max-connections` | 4 | Admission control; see below |

Don't take `--trim-context-ms` below ~1s. Whisper drops words near the edge of a
short window, and cutting the buffer flush to the commit boundary loses whatever
word straddles it.

Pushing `--decode-interval-ms` below ~150 buys nothing — first-text is already
governed by `--min-decode-ms` — and it starts fragmenting the hypothesis
(`"this is a- test- of near-"`), because each pass sees too little new audio to
extend a word cleanly.

Deployment settings also read from the environment (`.env` included):

| Flag | Env | Default |
|---|---|---|
| `--model` | `TR_MODEL` | `models/ggml-base.en-q5_1.bin` |
| `--bind` | `TR_BIND` | `127.0.0.1:8080` |
| `--allowed-hosts` | `TR_ALLOWED_HOSTS` | (none; see [REST API](#rest-api)) |
| `--client-dir` | `TR_CLIENT_DIR` | `client/dist` |
| `--database-url` | `DATABASE_URL` | required |
| `--data-dir` | `TR_DATA_DIR` | `data` |
| `--db-max-connections` | `TR_DB_MAX_CONNECTIONS` | 8 |
| `--workers`, `--threads`, `--flash-attn`, `--max-connections` | `TR_WORKERS`, `TR_THREADS`, `TR_FLASH_ATTN`, `TR_MAX_CONNECTIONS` | as above |

### Measured

M-series Mac, Metal, `base.en-q5_1`, one worker, audio fed at realtime pace.
Median of 3 runs, measured from speech onset by a realtime-paced benchmark
harness (not checked in):

| Config | First text | Median decode |
|---|---|---|
| 400 ms interval, no `audio_ctx`, no flash attn | 472 ms | 142 ms |
| + flash attention | 447 ms | 122 ms |
| + `audio_ctx` sizing | 408 ms | 64 ms |
| + early first pass, 200 ms interval, 120 ms min | **149 ms** | 64 ms |

Over the 19.5 s three-utterance clip with the tuned defaults: 70 decodes, mean
window 3.3 s, mean decode 60 ms, max 129 ms.

**Capacity.** The number that matters for scaling is duty cycle —
`decode_ms / decode_interval_ms` — not RTF. At the defaults that is
**60/200 ≈ 30% of one worker per connected stream**, so this hardware sustains
roughly 3 concurrent streams per worker. Measure your own: run with
`RUST_LOG=transcribe_server=debug` and read the `took_ms` field. On CPU-only
builds decodes are far slower and `--max-connections` should come down
accordingly.

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

Errors are JSON: `{"error":{"code":"…","message":"…"}}`. That includes
`400 bad_request` for malformed ids or bodies, `422 title_too_long` (titles are
capped at 200 characters), `422 mixed_sample_rates` when a session's recordings
can't be concatenated into one WAV, and a JSON `404` for unknown `/api` paths.

> There is no authentication. The server binds to `127.0.0.1` by default; if
> you bind it to another interface, anyone who can reach it can read and
> delete every session.
>
> Requests are also checked against a `Host` allowlist (loopback names, the
> `--bind` address, plus `--allowed-hosts` / `TR_ALLOWED_HOSTS`, comma-separated)
> and `/ws` against `Origin` when the browser sends one; anything else gets
> `403` (`forbidden_host` / `forbidden_origin`). This blocks DNS rebinding and
> cross-site WebSockets. You only need to set it when serving under another
> name (LAN hostname, reverse proxy).

## Architecture

```
crates/server/src/
  main.rs         wiring, DB connect, static file serving, graceful shutdown
  hosts.rs        Host / Origin allowlist middleware
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
  src/protocol.ts   wire types, mirrored by hand from protocol.rs
  src/api.ts        REST client (TanStack Query)
  src/transcript/   reducer, reconnect helpers, LiveSession (mic + socket)
  src/hooks/        useTranscriber, level meter, auto-scroll
  src/pages/        recorder, session list, session detail
  src/components/   transcript view, title editor, export menu
  public/audio-worklet.js   capture, downsample to 16 kHz, Int16 framing
scripts/smoke-ws.mjs        WebSocket smoke test against a running server
```

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
   to Postgres in batches (every 500 ms, 32 rows, or end of utterance). It
   drains audio in its own internal writer task, so Postgres retries never hold
   up audio and therefore never stall transcription. On close it records the
   duration and releases the session for the next connection; the server closes
   the socket only after that.

### Stabilization: LocalAgreement-2

Each decode produces a hypothesis over the current window. The longest common
prefix between the new hypothesis and the previous one — beyond what is already
committed — is promoted to committed text; the remainder is `pending`. Word
comparison is case- and punctuation-insensitive so cosmetic re-punctuation
doesn't block a commit.

On VAD end-of-utterance the whole hypothesis is committed at once without
waiting for a second agreeing pass, `endOfSegment: true` is emitted, the audio
buffer is reset and the hypothesis state is cleared.

Each pass re-decodes the window from scratch, so hypotheses cannot be aligned by
word index — the decoder re-tokenizes ("the laser" → "the lazy dog") and, after a
trim, the window no longer even starts at committed text. Alignment instead
searches for the longest suffix of the committed text inside the new hypothesis
and resumes just past it, falling back to timestamps only when no lexical anchor
matches. Both failure modes this prevents — dropped words and re-committed ones —
have regression tests in `stabilizer.rs`.

Greedy decoding occasionally falls into a repetition loop. `whisper.rs` caps
`max_tokens` against the window duration and truncates a hypothesis at the first
sign of degeneracy (a repeated word run, or a run-on token like
`real-time-time-time-…`), so hallucinated text never reaches the client.

### Threading

Whisper inference is blocking compute and **never** runs on a tokio worker — it
would stall the entire runtime. `whisper.rs` owns a pool of dedicated OS threads
fed by a crossbeam channel; async callers submit a job and await a oneshot.

The model is loaded once as `Arc<WhisperContext>`. Each worker owns its own
`WhisperState` (scratch buffers). Because `no_context = true` there is no
cross-call carry-over, so states are interchangeable between streams — sharing
them per-worker rather than per-stream keeps memory flat as connections scale.

### Scaling

Compute is the bottleneck, and a GPU serializes decodes regardless of how many
jobs you throw at it. So:

- `--workers` bounds concurrent decodes (1–2 per GPU; `cores / threads-per-decode`
  on CPU).
- Every connection has at most one decode in flight, so pool queue depth is
  bounded by connection count.
- `--max-connections` is admission control, and the right value comes from the
  duty cycle measured above (`workers / duty`, with headroom) — not from RTF,
  which flatters you because each decode covers a window far longer than the
  interval that triggered it. Connections beyond the cap get `503` rather than
  degrading everyone already connected.

## Tests

```sh
export DATABASE_URL=postgres://localhost/transcribe_rust
cargo test                       # DB tests create a throwaway database each
(cd client && pnpm typecheck && pnpm lint && pnpm test)
```

Server unit tests cover protocol serialization, the LocalAgreement-2 state
machine, PCM conversion and resampling, the live registry and every export
format. `#[sqlx::test]` integration tests cover the recorder, session resume and
crash repair, and the REST API. Client tests cover the transcript reducer,
reconnect helpers, wire-protocol parsing and formatting. The VAD test exercises the bundled Silero model.

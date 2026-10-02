# CLAUDE.md

Near-realtime speech transcription: a Rust/axum server streams microphone PCM
over one WebSocket, gates it with Silero VAD, decodes with whisper.cpp
(`whisper-rs`), stabilizes with LocalAgreement-2, and streams `commit`/`pending`
text back to a vanilla-JS browser client. `README.md` is the authoritative deep
dive (wire protocol, latency levers, measured numbers) — read it before
changing pipeline behavior.

**Design priority: low perceived latency (time-to-first-visible-text) over
maximum accuracy.** Any change that adds work to the hot path must justify its
latency cost.

## Commands

```sh
cargo build --release --features metal          # macOS; cuda / vulkan elsewhere, none = CPU
cargo run --release --features metal -- --model models/ggml-base.en-q5_1.bin
cargo test                                      # unit tests; VAD test uses the bundled Silero model
cargo clippy --all-targets -- -D warnings
cargo fmt
RUST_LOG=transcribe_server=debug cargo run ...  # per-decode `took_ms` for capacity tuning
```

First build compiles whisper.cpp (needs `cmake` + C++ toolchain, several
minutes). Models live in `models/` (gitignored); the server opens at
<http://127.0.0.1:8080>.

## Layout

```
crates/server/src/
  main.rs        wiring, static serving of client/, graceful shutdown
  config.rs      every tunable as a clap flag (most also TR_* env vars)
  ws.rs          axum handler: admission control + receiver/pipeline/sender tasks
  pipeline.rs    VAD, rolling buffer, latest-wins decode scheduling, emission
  stabilizer.rs  LocalAgreement-2 (commit = agreed prefix, rest = pending)
  whisper.rs     blocking inference pool on dedicated OS threads
  vad.rs         Silero over fixed 32 ms frames
  audio.rs       Int16 -> f32, resampling to 16 kHz
  protocol.rs    serde types for the wire protocol
client/
  index.html        single-file UI + WebSocket client (no build step)
  audio-worklet.js  capture, downsample to 16 kHz, Int16 framing
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
- Admission control (`--max-connections`) rejects with 503 rather than degrading
  existing streams.

## Conventions

- Rust 2021, workspace with a single crate (`crates/server`). `anyhow` for
  errors at the edges, `tracing` for logs (structured fields, not format
  strings).
- New tunables go in `config.rs` as clap args with a doc comment explaining the
  latency/cost trade-off; add a `TR_*` env var for anything deploy-relevant.
- Tests live next to the code in `#[cfg(test)] mod tests`.
- Client is plain HTML/JS with no bundler — keep it that way unless asked.
- Commits: conventional style (`feat:`, `fix:`, `chore:`, `feat(client):`). No
  Claude attribution lines.

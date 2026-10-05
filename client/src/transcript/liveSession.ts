import { parseServerMessage, type ReadyMessage, type StartMessage, type TranscriptMessage } from '../protocol';
import { backoffDelay, FrameBuffer, OFFLINE_BUFFER_BYTES } from './reconnect';

const TARGET_RATE = 16_000;
/** How long Stop waits for the server's `done` before closing anyway. */
const STOP_TIMEOUT_MS = 5_000;
/** Server errors that retrying cannot fix (bad handshake, refused origin/host). */
const TERMINAL_CODES = new Set([
  'session_not_found',
  'init_failed',
  'bad_handshake',
  'unsupported_encoding',
  'forbidden_origin',
  'forbidden_host',
]);

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
  private stopTimer: ReturnType<typeof setTimeout> | null = null;
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
      this.stopTimer = setTimeout(() => {
        this.stopTimer = null;
        if (this.finished) return;
        ws.onclose = null;
        ws.onmessage = null;
        ws.close();
        this.finish();
      }, STOP_TIMEOUT_MS);
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
          // These never succeed on retry. A busy session is worth retrying
          // only if we were already recording into it (we raced our own
          // previous connection's flush).
          const terminal =
            TERMINAL_CODES.has(msg.code) || (msg.code === 'session_busy' && !this.everReady);
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
      // Never reached `ready`: the server is down or refused the upgrade (a
      // 403 for a bad Origin/Host surfaces only as a bare close). Don't loop.
      if (!this.everReady && !this.stopped && !this.terminal) {
        this.terminal = true;
        this.cb.serverError('connect_failed', 'Could not connect to the server.', true);
      }
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
    if (this.stopTimer) {
      clearTimeout(this.stopTimer);
      this.stopTimer = null;
    }
    this.ws = null;
    this.cb.status('idle');
    this.cb.ended();
  }
}

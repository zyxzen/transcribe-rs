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

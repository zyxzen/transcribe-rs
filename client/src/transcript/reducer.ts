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

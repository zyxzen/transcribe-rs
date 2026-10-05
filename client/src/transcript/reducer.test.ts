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

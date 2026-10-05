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

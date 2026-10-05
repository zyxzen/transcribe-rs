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

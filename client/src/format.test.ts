import { describe, expect, it } from 'vitest';
import { formatClock, sessionLabel } from './format';

describe('formatClock', () => {
  it('formats minutes and hours', () => {
    expect(formatClock(0)).toBe('0:00');
    expect(formatClock(61_999)).toBe('1:01');
    expect(formatClock(3_723_000)).toBe('1:02:03');
    expect(formatClock(-5)).toBe('0:00');
  });
});

describe('sessionLabel', () => {
  it('prefers the title and falls back to the date', () => {
    expect(sessionLabel({ title: 'Standup', createdAt: '2026-10-03T14:32:00Z' })).toBe('Standup');
    expect(sessionLabel({ title: null, createdAt: '2026-10-03T14:32:00Z' })).toMatch(/^Session /);
  });
});

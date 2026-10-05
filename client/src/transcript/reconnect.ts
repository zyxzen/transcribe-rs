export const BACKOFF_MS = [500, 1000, 2000, 4000, 8000, 10000] as const;

export function backoffDelay(attempt: number): number {
  return BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)] ?? 10000;
}

/** 30 s of 16 kHz Int16 mono. */
export const OFFLINE_BUFFER_BYTES = 960_000;

/** Audio captured while the socket is down, oldest dropped first. */
export class FrameBuffer {
  private frames: ArrayBuffer[] = [];
  private size = 0;
  private overflowed = false;

  constructor(private readonly maxBytes: number) {}

  get bytes(): number {
    return this.size;
  }

  get dropped(): boolean {
    return this.overflowed;
  }

  push(frame: ArrayBuffer): void {
    this.frames.push(frame);
    this.size += frame.byteLength;
    while (this.size > this.maxBytes && this.frames.length > 0) {
      this.size -= this.frames.shift()?.byteLength ?? 0;
      this.overflowed = true;
    }
  }

  drain(): ArrayBuffer[] {
    const out = this.frames;
    this.frames = [];
    this.size = 0;
    this.overflowed = false;
    return out;
  }
}

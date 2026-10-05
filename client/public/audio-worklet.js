/**
 * Capture worklet: runs on the audio thread, downsamples to 16 kHz, converts to
 * Int16 and posts fixed-size frames to the main thread.
 *
 * AudioWorklet, not ScriptProcessorNode — ScriptProcessorNode runs on the main
 * thread and its jitter shows up directly as transcription latency.
 */
class CaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const opts = options.processorOptions || {};
    this.targetRate = opts.targetRate || 16000;
    // `sampleRate` is a global inside AudioWorkletGlobalScope.
    this.ratio = sampleRate / this.targetRate;

    // Frame size sent to the server, in target-rate samples.
    // 2048 @ 16 kHz = 128 ms.
    this.frameSamples = opts.frameSamples || 2048;
    this.out = new Int16Array(this.frameSamples);
    this.outLen = 0;

    // Fractional read position into `tail`, carried across process() calls so
    // the resampled stream has no discontinuities at block boundaries.
    this.pos = 0;
    this.tail = new Float32Array(0);

    this.running = true;
    this.port.onmessage = (e) => {
      if (e.data === 'stop') {
        this.flush();
        this.running = false;
      }
    };
  }

  flush() {
    if (this.outLen > 0) {
      const frame = this.out.slice(0, this.outLen);
      this.port.postMessage(frame, [frame.buffer]);
      this.outLen = 0;
    }
  }

  emit(sample) {
    const clamped = Math.max(-1, Math.min(1, sample));
    this.out[this.outLen++] = clamped < 0 ? clamped * 0x8000 : clamped * 0x7fff;
    if (this.outLen === this.frameSamples) {
      const frame = this.out.slice(0);
      this.port.postMessage(frame, [frame.buffer]);
      this.outLen = 0;
    }
  }

  process(inputs) {
    if (!this.running) return false;

    const channel = inputs[0] && inputs[0][0];
    if (!channel || channel.length === 0) return true;

    // Concatenate leftover input with this block.
    const buf = new Float32Array(this.tail.length + channel.length);
    buf.set(this.tail, 0);
    buf.set(channel, this.tail.length);

    // Linear interpolation. Cheap, and Whisper's mel front-end is far more
    // forgiving than a listener would be.
    let pos = this.pos;
    while (pos + 1 < buf.length) {
      const i = Math.floor(pos);
      const frac = pos - i;
      this.emit(buf[i] * (1 - frac) + buf[i + 1] * frac);
      pos += this.ratio;
    }

    const consumed = Math.floor(pos);
    this.tail = buf.slice(consumed);
    this.pos = pos - consumed;
    return true;
  }
}

registerProcessor('capture-processor', CaptureProcessor);

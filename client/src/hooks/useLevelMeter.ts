import { useEffect, useRef, type RefObject } from 'react';

/**
 * Exponentially smoothed RMS written to the element's `--level` CSS variable
 * every frame. Rises fast, falls slowly, so the meter tracks speech without
 * flickering. Never touches React state.
 */
export function useLevelMeter(
  ref: RefObject<HTMLElement | null>,
  getAnalyser: () => AnalyserNode | null,
  active: boolean,
  silent: boolean,
): void {
  const silentRef = useRef(silent);
  useEffect(() => {
    silentRef.current = silent;
  }, [silent]);

  useEffect(() => {
    const el = ref.current;
    if (!el || !active) return;
    let raf = 0;
    let value = 0;
    let buf: Float32Array<ArrayBuffer> | null = null;
    const tick = () => {
      let target = 0;
      const analyser = getAnalyser();
      if (analyser && !silentRef.current) {
        if (!buf || buf.length !== analyser.fftSize) buf = new Float32Array(analyser.fftSize);
        analyser.getFloatTimeDomainData(buf);
        let sum = 0;
        for (const v of buf) sum += v * v;
        // Perceptual-ish curve: quiet speech should still move the needle.
        target = Math.min(1, Math.sqrt(Math.sqrt(sum / buf.length)) * 2.6);
      }
      value += (target - value) * (target > value ? 0.35 : 0.08);
      el.style.setProperty('--level', value.toFixed(3));
      raf = requestAnimationFrame(tick);
    };
    tick();
    return () => {
      cancelAnimationFrame(raf);
      el.style.setProperty('--level', '0');
    };
  }, [ref, getAnalyser, active]);
}

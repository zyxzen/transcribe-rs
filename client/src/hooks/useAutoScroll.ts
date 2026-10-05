import { useCallback, useEffect, useLayoutEffect, useState, type RefObject } from 'react';

const STICK_PX = 48;

/**
 * Follows the tail until the reader scrolls away from the bottom, and
 * re-engages once they scroll back down to it.
 */
export function useAutoScroll(ref: RefObject<HTMLElement | null>, content: unknown) {
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onScroll = () => setAutoScroll(el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_PX);
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [ref]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (el && autoScroll) el.scrollTop = el.scrollHeight;
  }, [ref, content, autoScroll]);

  const toggle = useCallback(() => setAutoScroll((on) => !on), []);
  return { autoScroll, toggle };
}

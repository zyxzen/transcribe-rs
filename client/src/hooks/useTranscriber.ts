import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { clearStoredSessionId, readStoredSessionId, storeSessionId } from '../sessionStore';
import { LiveSession, type TranscriberStatus } from '../transcript/liveSession';
import { initialTranscript, transcriptReducer } from '../transcript/reducer';

export interface StartOptions {
  sessionId: string | null;
  autoDetect: boolean;
}

export function useTranscriber() {
  const [transcript, dispatch] = useReducer(transcriptReducer, initialTranscript);
  const [status, setStatus] = useState<TranscriberStatus>('idle');
  const [sessionId, setSessionId] = useState<string | null>(readStoredSessionId);
  const [paused, setPaused] = useState(false);
  const [muted, setMuted] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [firstTextMs, setFirstTextMs] = useState<number | null>(null);
  const [language, setLanguage] = useState<string | null>(null);
  const live = useRef<LiveSession | null>(null);
  const mutedRef = useRef(false);

  // Leaving the page ends the recording; the server flushes what it has.
  useEffect(
    () => () => {
      live.current?.stop();
      live.current = null;
    },
    [],
  );

  const start = useCallback(async ({ sessionId: resumeId, autoDetect }: StartOptions) => {
    if (live.current) return;
    const startedAt = performance.now();
    let sawText = false;
    mutedRef.current = false;
    setMuted(false);
    setPaused(false);
    setNotice(null);
    setFirstTextMs(null);

    const session: LiveSession = new LiveSession(resumeId, autoDetect, {
      status: setStatus,
      ready: (msg) => {
        storeSessionId(msg.sessionId);
        setSessionId(msg.sessionId);
        dispatch({ type: 'connected' });
      },
      transcript: (msg) => {
        dispatch({ type: 'message', msg });
        if (!sawText && (msg.commit || msg.pending)) {
          sawText = true;
          setFirstTextMs(Math.round(performance.now() - startedAt));
        }
        setLanguage(msg.language);
      },
      notice: setNotice,
      serverError: (code, message, terminal) => {
        if (code === 'session_not_found') {
          clearStoredSessionId();
          setSessionId(null);
          setNotice('That session no longer exists. Press start for a new one.');
        } else if (code === 'session_busy') {
          if (terminal) setNotice('This session is recording in another tab.');
        } else if (code === 'persist_failed') {
          setNotice('Transcript is not being saved right now; retrying.');
        } else {
          setNotice(`Error: ${message || code}`);
        }
      },
      ended: () => {
        if (live.current === session) live.current = null;
        setPaused(false);
      },
    });
    live.current = session;
    try {
      await session.start();
    } catch (e) {
      session.stop();
      setNotice(`Microphone error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, []);

  const stop = useCallback(() => live.current?.stop(), []);

  const togglePause = useCallback(() => {
    const session = live.current;
    if (!session) return;
    session.paused = !session.paused;
    setPaused(session.paused);
  }, []);

  const toggleMute = useCallback(() => {
    const session = live.current;
    if (!session) return;
    mutedRef.current = !mutedRef.current;
    session.setMuted(mutedRef.current);
    setMuted(mutedRef.current);
  }, []);

  const newSession = useCallback(() => {
    if (live.current) return;
    clearStoredSessionId();
    setSessionId(null);
    setNotice(null);
    setFirstTextMs(null);
    dispatch({ type: 'reset' });
  }, []);

  const seed = useCallback((id: string, paragraphs: string[]) => {
    if (live.current) return;
    storeSessionId(id);
    setSessionId(id);
    dispatch({ type: 'seed', paragraphs });
  }, []);

  const getAnalyser = useCallback(() => live.current?.analyser ?? null, []);

  return {
    transcript,
    status,
    sessionId,
    paused,
    muted,
    notice,
    firstTextMs,
    language,
    start,
    stop,
    togglePause,
    toggleMute,
    newSession,
    seed,
    getAnalyser,
  };
}

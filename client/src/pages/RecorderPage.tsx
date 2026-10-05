import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { api, ApiError } from '../api';
import {
  AutoScrollIcon, CopyIcon, ExpandIcon, LevelIcon, ListIcon, MicIcon, PauseIcon, PlayIcon, PlusIcon, StopIcon,
} from '../components/icons';
import { TitleEditor } from '../components/TitleEditor';
import { Transcript } from '../components/Transcript';
import { useAutoScroll } from '../hooks/useAutoScroll';
import { useLevelMeter } from '../hooks/useLevelMeter';
import { useTranscriber } from '../hooks/useTranscriber';
import { seededTranscript } from '../transcript/reducer';

export function RecorderPage() {
  const [params, setParams] = useSearchParams();
  const t = useTranscriber();
  const { togglePause } = t;
  const [autoDetect, setAutoDetect] = useState(false);
  const [flash, setFlash] = useState<string | null>(null);
  const stageRef = useRef<HTMLElement>(null);
  const levelRef = useRef<HTMLButtonElement>(null);

  const isLive = t.status !== 'idle';
  const requested = params.get('session');
  const target = requested ?? t.sessionId;

  // History is shown (not yet seeded) until recording starts, so nothing
  // here needs an effect to copy server data into the reducer.
  const showHistory = !isLive && t.transcript.paragraphs.length === 0 && target !== null;
  const history = useQuery({
    queryKey: ['session', target],
    queryFn: () => api.getSession(target as string),
    enabled: showHistory,
  });
  const missing = history.error instanceof ApiError && history.error.status === 404;
  const historyParagraphs = useMemo(() => history.data?.paragraphs.map((p) => p.text) ?? [], [history.data]);
  const view = useMemo(
    () => (showHistory && historyParagraphs.length ? seededTranscript(historyParagraphs) : t.transcript),
    [showHistory, historyParagraphs, t.transcript],
  );

  const { autoScroll, toggle: toggleAutoScroll } = useAutoScroll(stageRef, view);
  useLevelMeter(levelRef, t.getAnalyser, isLive, t.muted || t.paused);

  const hasText = view.paragraphs.length > 0 || view.pending !== '';
  const uiState = isLive ? 'live' : hasText ? 'ended' : 'idle';
  const busy = t.status === 'starting' || t.status === 'stopping';

  const begin = async () => {
    const id = missing ? null : target;
    if (missing) t.newSession();
    else if (id && t.transcript.paragraphs.length === 0 && historyParagraphs.length) t.seed(id, historyParagraphs);
    if (requested) setParams({}, { replace: true });
    await t.start({ sessionId: id, autoDetect });
  };

  const say = (text: string) => {
    setFlash(text);
    setTimeout(() => setFlash((f) => (f === text ? null : f)), 2500);
  };

  const copy = async () => {
    const text = view.paragraphs.map((p) => p.text).join('\n');
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      say('copied');
    } catch {
      say('copy blocked');
    }
  };

  const statusText =
    t.notice ??
    flash ??
    (missing
      ? 'That session no longer exists.'
      : t.status === 'starting' || t.status === 'connecting'
        ? 'connecting…'
        : t.status === 'reconnecting'
          ? 'reconnecting…'
          : t.status === 'stopping'
            ? 'stopping…'
            : t.status === 'live'
              ? t.paused
                ? 'paused'
                : t.firstTextMs !== null
                  ? `first text in ${t.firstTextMs} ms`
                  : 'listening…'
              : '');

  // Space toggles pause while live, start otherwise (as in the old client).
  const beginRef = useRef(begin);
  useEffect(() => {
    beginRef.current = begin;
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.code !== 'Space' || e.target !== document.body) return;
      e.preventDefault();
      if (isLive) togglePause();
      else void beginRef.current();
    };
    addEventListener('keydown', onKey);
    return () => removeEventListener('keydown', onKey);
  }, [isLive, togglePause]);

  const titleSession = isLive ? t.sessionId : missing ? null : target;

  return (
    <div className="recorder" data-state={uiState}>
      <main id="splash">
        <div className="bubble">{showHistory && history.isLoading ? 'Loading session…' : 'Press and start talking'}</div>
        <button className="mic-hero" id="hero" aria-label="Start transcribing" disabled={busy} onClick={() => void begin()}>
          <MicIcon />
        </button>
        <p className="hint">Solid text is committed · grey text is the live hypothesis</p>
        <Link className="hint" to="/sessions">
          Past sessions
        </Link>
      </main>

      <button
        className="icon-btn"
        id="autoscroll"
        aria-pressed={autoScroll}
        aria-label="Toggle auto-scroll"
        title={autoScroll ? 'Auto-scroll is on' : 'Auto-scroll is off — click to follow'}
        onClick={toggleAutoScroll}
      >
        <AutoScrollIcon />
      </button>

      <section id="stage" ref={stageRef}>
        <Transcript state={view} listening={t.status === 'live' && t.firstTextMs === null} />
      </section>

      <footer id="bar">
        <div id="brand">
          <span className="dot" />
          {titleSession ? <TitleEditor sessionId={titleSession} /> : <span>Whisper live</span>}
          <span id="status" data-notice={t.notice !== null || missing}>
            {statusText}
          </span>
        </div>

        <div id="controls">
          <button
            className="icon-btn"
            id="pause"
            aria-label={t.paused ? 'Resume' : 'Pause'}
            title={t.paused ? 'Resume capture' : 'Pause capture'}
            disabled={t.status !== 'live'}
            onClick={t.togglePause}
          >
            {t.paused ? <PlayIcon /> : <PauseIcon />}
          </button>
          <button
            className="primary"
            id="primary"
            disabled={busy}
            aria-label={isLive ? 'Stop' : hasText ? 'Continue' : 'Start'}
            onClick={isLive ? t.stop : () => void begin()}
          >
            {isLive ? <StopIcon /> : <MicIcon />}
          </button>
          <button
            className="icon-btn"
            id="level"
            ref={levelRef}
            data-muted={String(t.muted)}
            aria-label="Microphone level"
            title={t.muted ? 'Unmute microphone' : 'Input level — click to mute'}
            onClick={t.toggleMute}
          >
            <LevelIcon />
          </button>
        </div>

        <div id="tools">
          <button
            className="icon-btn"
            aria-label="New session"
            title="New session"
            disabled={isLive || !hasText}
            onClick={() => {
              t.newSession();
              if (requested) setParams({}, { replace: true });
            }}
          >
            <PlusIcon />
          </button>
          {isLive ? (
            <button className="icon-btn" aria-label="Sessions" title="Stop recording to browse sessions" disabled>
              <ListIcon />
            </button>
          ) : (
            <Link className="icon-btn" to="/sessions" aria-label="Sessions" title="Sessions">
              <ListIcon />
            </Link>
          )}
          <button className="icon-btn" aria-label="Copy transcript" title="Copy transcript" onClick={() => void copy()}>
            <CopyIcon />
          </button>
          <button
            className="icon-btn"
            aria-label="Fullscreen"
            title="Fullscreen"
            onClick={() => (document.fullscreenElement ? void document.exitFullscreen() : void document.documentElement.requestFullscreen?.())}
          >
            <ExpandIcon />
          </button>
          <button
            className="pill"
            id="detect"
            aria-pressed={autoDetect}
            title="Let the model detect the language"
            onClick={() => {
              setAutoDetect((on) => !on);
              if (isLive) say('applies on next start');
            }}
          >
            <span>{autoDetect ? (t.language && isLive ? t.language.toUpperCase() : 'Auto') : 'Detect Language'}</span>
          </button>
        </div>
      </footer>
    </div>
  );
}

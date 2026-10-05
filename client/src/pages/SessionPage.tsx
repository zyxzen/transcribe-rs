import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CSSProperties } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { api, ApiError } from '../api';
import { ExportMenu } from '../components/ExportMenu';
import { TitleEditor } from '../components/TitleEditor';
import { formatClock, formatDate, sessionLabel } from '../format';
import { clearStoredSessionId, readStoredSessionId } from '../sessionStore';
import { COLORS } from '../transcript/reducer';

export function SessionPage() {
  const { id = '' } = useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const session = useQuery({
    queryKey: ['session', id],
    queryFn: () => api.getSession(id),
    // Show text as it is saved while the session is recording elsewhere.
    refetchInterval: (query) => (query.state.data?.live ? 3000 : false),
  });
  const remove = useMutation({
    mutationFn: () => api.deleteSession(id),
    onSuccess: () => {
      if (readStoredSessionId() === id) clearStoredSessionId();
      qc.removeQueries({ queryKey: ['session', id] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
      void navigate('/sessions');
    },
  });

  if (session.isPending) {
    return (
      <div className="page">
        <p className="muted">Loading…</p>
      </div>
    );
  }
  if (session.isError) {
    const notFound = session.error instanceof ApiError && session.error.status === 404;
    return (
      <div className="page">
        <Link to="/sessions" className="back">
          ← All sessions
        </Link>
        <p className="error">{notFound ? 'This session does not exist.' : `Couldn’t load session: ${session.error.message}`}</p>
      </div>
    );
  }

  const s = session.data;
  return (
    <div className="page">
      <Link to="/sessions" className="back">
        ← All sessions
      </Link>
      <header className="page-head">
        <h1>
          <TitleEditor sessionId={s.id} />
        </h1>
        {s.live ? <span className="live-badge">● live</span> : null}
      </header>
      <p className="session-meta">
        {formatClock(s.durationMs)} · {formatDate(s.createdAt)} · {s.recordings.length}{' '}
        {s.recordings.length === 1 ? 'recording' : 'recordings'}
      </p>

      <div className="actions">
        <button
          className="pill primary-pill"
          disabled={s.live}
          title={s.live ? 'This session is recording in another tab' : 'Continue recording into this session'}
          onClick={() => void navigate(`/?session=${s.id}`)}
        >
          Continue recording
        </button>
        <ExportMenu sessionId={s.id} />
        <button
          className="pill danger-pill"
          disabled={s.live || remove.isPending}
          onClick={() => {
            if (confirm(`Delete “${sessionLabel(s)}”? Its transcript and audio are removed.`)) remove.mutate();
          }}
        >
          Delete
        </button>
      </div>
      {remove.isError ? <p className="error">Delete failed: {remove.error.message}</p> : null}

      {s.paragraphs.length === 0 ? (
        <p className="muted">No transcript yet.</p>
      ) : (
        <div className="session-transcript">
          {s.paragraphs.map((p, i) => (
            <div key={p.index} className="line" style={{ '--c': COLORS[i % COLORS.length] } as CSSProperties}>
              <span className="stamp">{formatClock(p.startMs)}</span>
              {p.text}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router';
import { api, PAGE_SIZE } from '../api';
import { TrashIcon } from '../components/icons';
import { formatClock, formatDate, sessionLabel } from '../format';
import { clearStoredSessionId, readStoredSessionId } from '../sessionStore';

export function SessionsPage() {
  const qc = useQueryClient();
  const sessions = useInfiniteQuery({
    queryKey: ['sessions'],
    queryFn: ({ pageParam }) => api.listSessions(pageParam),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => (last.length === PAGE_SIZE ? last.at(-1)?.id : undefined),
    // Keep live badges and growing durations current.
    refetchInterval: (query) => (query.state.data?.pages.some((p) => p.some((s) => s.live)) ? 3000 : false),
  });
  const remove = useMutation({
    mutationFn: (id: string) => api.deleteSession(id),
    onSuccess: (_, id) => {
      if (readStoredSessionId() === id) clearStoredSessionId();
      qc.removeQueries({ queryKey: ['session', id] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
    },
  });
  const rows = sessions.data?.pages.flat() ?? [];

  return (
    <div className="page">
      <header className="page-head">
        <h1>Sessions</h1>
        <Link className="pill" to="/">
          New recording
        </Link>
      </header>

      {sessions.isPending ? (
        <p className="muted">Loading…</p>
      ) : sessions.isError ? (
        <p className="error">Couldn’t load sessions: {sessions.error.message}</p>
      ) : rows.length === 0 ? (
        <p className="muted">
          No sessions yet. <Link to="/">Record one</Link>.
        </p>
      ) : (
        <ul className="session-list">
          {rows.map((s) => (
            <li key={s.id} className="session-row">
              <Link to={`/sessions/${s.id}`} className="session-link">
                <span className="session-title">
                  {sessionLabel(s)}
                  {s.live ? <span className="live-badge">● live</span> : null}
                </span>
                <span className="session-preview">{s.preview || 'No transcript yet'}</span>
                <span className="session-meta">
                  {formatClock(s.durationMs)} · {formatDate(s.createdAt)}
                </span>
              </Link>
              <button
                className="icon-btn"
                aria-label={`Delete ${sessionLabel(s)}`}
                title={s.live ? 'Stop recording before deleting' : 'Delete session'}
                disabled={s.live || remove.isPending}
                onClick={() => {
                  if (confirm(`Delete “${sessionLabel(s)}”? Its transcript and audio are removed.`)) remove.mutate(s.id);
                }}
              >
                <TrashIcon />
              </button>
            </li>
          ))}
        </ul>
      )}

      {sessions.hasNextPage ? (
        <button
          className="pill load-more"
          disabled={sessions.isFetchingNextPage}
          onClick={() => void sessions.fetchNextPage()}
        >
          {sessions.isFetchingNextPage ? 'Loading…' : 'Load more'}
        </button>
      ) : null}
      {remove.isError ? <p className="error">Delete failed: {remove.error.message}</p> : null}
    </div>
  );
}

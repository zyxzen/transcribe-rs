import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { api } from '../api';
import { sessionLabel } from '../format';

/** Click to rename; Enter or blur saves, Escape cancels, empty clears. */
export function TitleEditor({ sessionId }: { sessionId: string }) {
  const qc = useQueryClient();
  const { data } = useQuery({ queryKey: ['session', sessionId], queryFn: () => api.getSession(sessionId) });
  const [draft, setDraft] = useState<string | null>(null);
  const rename = useMutation({
    mutationFn: (title: string | null) => api.renameSession(sessionId, title),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['session', sessionId] });
      void qc.invalidateQueries({ queryKey: ['sessions'] });
    },
  });

  const label = data ? sessionLabel(data) : 'Untitled session';
  if (draft === null) {
    return (
      <button type="button" className="title-btn" title="Rename session" onClick={() => setDraft(data?.title ?? '')}>
        {label}
      </button>
    );
  }

  const commit = () => {
    const value = draft.trim();
    setDraft(null);
    if (value !== (data?.title ?? '')) rename.mutate(value || null);
  };
  return (
    <input
      className="title-input"
      aria-label="Session title"
      autoFocus
      maxLength={200}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Enter') commit();
        if (e.key === 'Escape') setDraft(null);
      }}
    />
  );
}

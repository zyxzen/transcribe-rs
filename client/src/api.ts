export const PAGE_SIZE = 50;
export const EXPORT_FORMATS = ['txt', 'srt', 'vtt', 'json', 'wav'] as const;
export type ExportFormat = (typeof EXPORT_FORMATS)[number];

export interface SessionSummary {
  id: string;
  title: string | null;
  language: string;
  durationMs: number;
  createdAt: string;
  updatedAt: string;
  live: boolean;
  preview: string;
}

export interface Recording {
  id: string;
  ordinal: number;
  offsetMs: number;
  sampleRate: number;
  durationMs: number | null;
  startedAt: string;
  endedAt: string | null;
}

export interface ParagraphView {
  index: number;
  startMs: number;
  endMs: number;
  text: string;
}

export interface SessionDetail extends Omit<SessionSummary, 'preview'> {
  recordings: Recording[];
  paragraphs: ParagraphView[];
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { 'content-type': 'application/json', ...init?.headers },
  });
  if (!res.ok) {
    let code = `http_${res.status}`;
    let message = res.statusText;
    try {
      const body = (await res.json()) as { error?: { code?: string; message?: string } };
      code = body.error?.code ?? code;
      message = body.error?.message ?? message;
    } catch {
      // not a JSON error body
    }
    throw new ApiError(res.status, code, message);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  listSessions(before?: string): Promise<SessionSummary[]> {
    const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
    if (before) params.set('before', before);
    return request(`/api/sessions?${params}`);
  },
  getSession(id: string): Promise<SessionDetail> {
    return request(`/api/sessions/${id}`);
  },
  renameSession(id: string, title: string | null): Promise<Omit<SessionSummary, 'preview'>> {
    return request(`/api/sessions/${id}`, { method: 'PATCH', body: JSON.stringify({ title }) });
  },
  deleteSession(id: string): Promise<void> {
    return request(`/api/sessions/${id}`, { method: 'DELETE' });
  },
  exportUrl(id: string, format: ExportFormat): string {
    return `/api/sessions/${id}/export/${format}`;
  },
};

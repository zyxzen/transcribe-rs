// The session the recorder continues by default. Storage can throw in private
// windows; losing the id only means the next recording starts a new session.
const KEY = 'transcribe.currentSessionId';

export function readStoredSessionId(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null;
  }
}

export function storeSessionId(id: string): void {
  try {
    localStorage.setItem(KEY, id);
  } catch {
    // ignore
  }
}

export function clearStoredSessionId(): void {
  try {
    localStorage.removeItem(KEY);
  } catch {
    // ignore
  }
}

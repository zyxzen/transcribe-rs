import { api, EXPORT_FORMATS } from '../api';

export function ExportMenu({ sessionId }: { sessionId: string }) {
  return (
    <div className="export-menu" role="group" aria-label="Export">
      {EXPORT_FORMATS.map((format) => (
        <a key={format} className="pill" href={api.exportUrl(sessionId, format)} download>
          .{format}
        </a>
      ))}
    </div>
  );
}

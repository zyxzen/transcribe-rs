import type { CSSProperties } from 'react';
import { COLORS, type TranscriptState } from '../transcript/reducer';

const accent = (color: number) => ({ '--c': COLORS[color % COLORS.length] }) as CSSProperties;

export function Transcript({ state, listening }: { state: TranscriptState; listening: boolean }) {
  const { paragraphs, pending, open, nextColor } = state;
  const last = paragraphs.length - 1;
  return (
    <>
      <div id="transcript">
        {paragraphs.map((p, i) => (
          <div key={i} className="line" style={accent(p.color)}>
            <span className="committed">{p.text}</span>
            {open && i === last && pending ? <span className="pending"> {pending}</span> : null}
          </div>
        ))}
        {!open && pending ? (
          <div className="line" style={accent(nextColor)}>
            <span className="pending">{pending}</span>
          </div>
        ) : null}
      </div>
      <div id="listening" hidden={!listening}>
        listening<i>.</i>
        <i>.</i>
        <i>.</i>
      </div>
    </>
  );
}

import { Link } from 'react-router';

export function SessionPage() {
  return (
    <div className="page">
      <Link to="/sessions" className="back">
        ← All sessions
      </Link>
    </div>
  );
}

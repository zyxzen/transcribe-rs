import { Link } from 'react-router';

export function SessionsPage() {
  return (
    <div className="page">
      <header className="page-head">
        <h1>Sessions</h1>
        <Link className="pill" to="/">
          New recording
        </Link>
      </header>
    </div>
  );
}

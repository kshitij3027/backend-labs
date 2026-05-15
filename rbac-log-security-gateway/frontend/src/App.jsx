import { useEffect, useState } from "react";

export default function App() {
  const [healthy, setHealthy] = useState(null);
  const [healthError, setHealthError] = useState(null);

  useEffect(() => {
    // Backend exposes /health (not /api/health). Both nginx (prod) and vite (dev)
    // proxy this path through to backend:8000, so it works in either environment.
    fetch("/health")
      .then((r) => (r.ok ? r.json() : Promise.reject(`status ${r.status}`)))
      .then((data) => setHealthy(data.status === "ok"))
      .catch((err) => setHealthError(String(err)));
  }, []);

  return (
    <main className="container">
      <h1>RBAC Log Security Gateway</h1>
      <p>JWT auth + role-based authorization + audit logging for log queries.</p>
      <section>
        <h2>Backend health</h2>
        {healthError && <p className="error">unreachable: {healthError}</p>}
        {!healthError && healthy === null && <p>checking…</p>}
        {healthy === true && <p className="ok">backend is healthy ✓</p>}
        {healthy === false && <p className="error">backend reported a non-ok status</p>}
      </section>
      <section>
        <h2>Scaffold note</h2>
        <p>Login, dashboard, log search, and admin pages are wired up in subsequent commits.</p>
      </section>
    </main>
  );
}

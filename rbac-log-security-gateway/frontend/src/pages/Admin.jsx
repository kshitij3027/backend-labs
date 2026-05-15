import { useEffect, useState } from "react";
import api from "../api/client";

function useAdminData(path) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    setLoading(true);
    api
      .get(path)
      .then((r) => {
        if (active) setData(r.data);
      })
      .catch((err) => {
        if (active) setError(String(err.message || err));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [path]);

  return { data, error, loading };
}

export default function Admin() {
  const summary = useAdminData("/admin/audit-summary");
  const status = useAdminData("/admin/system-status");
  const events = useAdminData("/admin/security-events?limit=20");
  const policies = useAdminData("/admin/rbac-policies");

  return (
    <main className="container">
      <h1>Admin</h1>

      <section>
        <h2>System status</h2>
        {status.loading && <p>Loading…</p>}
        {status.error && <p className="error" data-testid="status-error">{status.error}</p>}
        {status.data && (
          <ul data-testid="system-status">
            <li>Status: <strong>{status.data.status}</strong></li>
            <li>Uptime: <strong>{status.data.uptime_seconds.toFixed(1)}s</strong></li>
            <li>Audit entries: <strong data-testid="audit-entry-count">{status.data.audit_entry_count}</strong></li>
            <li>Security events: <strong data-testid="security-event-count">{status.data.security_event_count}</strong></li>
            <li>Roles: {status.data.known_roles.map((r) => (
              <span key={r} className="role-chip">{r}</span>
            ))}</li>
            <li>Resources: {status.data.known_resources.length}</li>
          </ul>
        )}
      </section>

      <section>
        <h2>Audit summary</h2>
        {summary.loading && <p>Loading…</p>}
        {summary.error && <p className="error">{summary.error}</p>}
        {summary.data && (
          <table className="data-table" data-testid="audit-summary-table">
            <tbody>
              <tr><th>Total entries</th><td>{summary.data.total_entries}</td></tr>
              <tr><th>Allow decisions</th><td>{summary.data.allow_decisions}</td></tr>
              <tr><th>Deny decisions</th><td>{summary.data.deny_decisions}</td></tr>
              <tr><th>Security events</th><td>{summary.data.security_events}</td></tr>
              <tr>
                <th>By status</th>
                <td>{Object.entries(summary.data.by_status).map(([s, n]) => (
                  <span key={s} className="role-chip">{s}: {n}</span>
                ))}</td>
              </tr>
              <tr>
                <th>By user</th>
                <td>{Object.entries(summary.data.by_user).map(([u, n]) => (
                  <span key={u} className="role-chip">{u}: {n}</span>
                ))}</td>
              </tr>
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>Recent security events</h2>
        {events.loading && <p>Loading…</p>}
        {events.error && <p className="error">{events.error}</p>}
        {events.data && events.data.length === 0 && <p>No security events recorded.</p>}
        {events.data && events.data.length > 0 && (
          <table className="data-table" data-testid="security-events-table">
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Event</th>
                <th>Status</th>
                <th>Path</th>
                <th>User</th>
                <th>Source IP</th>
              </tr>
            </thead>
            <tbody>
              {events.data.map((e, i) => (
                <tr key={i}>
                  <td>{new Date(e.timestamp).toLocaleString()}</td>
                  <td><code>{e.event_type}</code></td>
                  <td>{e.status}</td>
                  <td>{e.path}</td>
                  <td>{e.username || "—"}</td>
                  <td>{e.source_ip || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>RBAC policies</h2>
        {policies.loading && <p>Loading…</p>}
        {policies.error && <p className="error">{policies.error}</p>}
        {policies.data && (
          <div data-testid="rbac-policies">
            {Object.entries(policies.data.roles).map(([role, perms]) => (
              <div key={role} className="policy-block">
                <h3>{role} <span className="meta">(default scope: <code>{policies.data.default_scopes[role]}</code>)</span></h3>
                <ul>
                  {perms.map((p) => (
                    <li key={p}>
                      <code className={p.startsWith("!") ? "deny" : "allow"}>{p}</code>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}

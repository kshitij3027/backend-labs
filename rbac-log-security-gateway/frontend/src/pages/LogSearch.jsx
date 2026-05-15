import { useState } from "react";
import api from "../api/client";

const RESOURCES = [
  "application.auth",
  "application.api",
  "application.worker",
  "business.metrics",
  "business.financial",
  "business.customer",
  "system.kernel",
  "system.audit",
];

export default function LogSearch() {
  const [resource, setResource] = useState("application.auth");
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);
    try {
      const r = await api.get(`/logs/search?resource=${encodeURIComponent(resource)}&limit=100`);
      setData(r.data);
    } catch (err) {
      const detail = err.response && err.response.data && err.response.data.detail;
      if (err.response && err.response.status === 403) {
        const reason =
          detail && typeof detail === "object" ? detail.reason : "forbidden";
        const rule = detail && typeof detail === "object" ? detail.rule : null;
        setError({
          kind: "forbidden",
          message: `Access denied (${reason})${rule ? ` — matched rule: ${rule}` : ""}`,
        });
      } else if (err.response && err.response.status === 401) {
        setError({ kind: "unauthorized", message: "Your session expired — please sign in again." });
      } else {
        setError({ kind: "error", message: String((detail && detail.error) || err.message) });
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="container">
      <h1>Log search</h1>
      <p>
        Pick a resource and query its logs. Your role determines what you see — denied resources
        return a 403, aggregated-only views show counts not rows, and PII-sensitive resources are masked.
      </p>
      <form onSubmit={onSubmit} className="row search-form" data-testid="log-search-form">
        <label>
          Resource
          <select
            value={resource}
            onChange={(e) => setResource(e.target.value)}
            data-testid="resource-select"
          >
            {RESOURCES.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </label>
        <button type="submit" disabled={loading} data-testid="log-search-submit">
          {loading ? "Searching…" : "Search"}
        </button>
      </form>

      {error && (
        <div className={`alert alert-${error.kind}`} role="alert" data-testid="log-search-error">
          {error.message}
        </div>
      )}

      {data && (
        <section data-testid="log-search-results">
          <h2>{data.resource} <span className="meta">({data.count} records)</span></h2>
          {data.rbac_rule && (
            <p className="meta">
              Matched RBAC rule: <code data-testid="rbac-rule">{data.rbac_rule}</code>
            </p>
          )}
          {data.aggregated && (
            <section data-testid="aggregated-block">
              <h3>Aggregated view (your role only allows summaries)</h3>
              <ul>
                <li>Total: <strong>{data.aggregated.total}</strong></li>
                <li>By level: {Object.entries(data.aggregated.by_level || {}).map(([level, n]) => (
                  <span key={level} className="role-chip">{level}: {n}</span>
                ))}</li>
                <li>Window: {data.aggregated.earliest} → {data.aggregated.latest}</li>
              </ul>
            </section>
          )}
          {data.masked && (
            <p data-testid="masked-banner" className="banner banner-mask">
              ⚠ PII fields are masked for your role.
            </p>
          )}
          {data.records && (
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Time</th>
                  <th>Level</th>
                  <th>Message</th>
                  <th>Fields</th>
                </tr>
              </thead>
              <tbody>
                {data.records.map((rec) => (
                  <tr key={rec.id} data-testid={`log-row-${rec.id}`}>
                    <td><code>{rec.id}</code></td>
                    <td>{new Date(rec.timestamp).toLocaleString()}</td>
                    <td>
                      <span className={`level level-${rec.level}`}>{rec.level}</span>
                    </td>
                    <td>{rec.message}</td>
                    <td>
                      {Object.entries(rec.fields).map(([k, v]) => (
                        <div key={k} className="kv">
                          <code>{k}</code>: {v}
                        </div>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </main>
  );
}

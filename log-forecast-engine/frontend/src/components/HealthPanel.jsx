import { fmt, pct } from "../util.js";

// System health / performance panel. Combines GET /health (status, deployed
// models, db/redis booleans, perf snapshot) with GET /metrics app metrics
// (per-model recent accuracy, prediction counts, resource usage).
export default function HealthPanel({ health, appMetrics }) {
  const status = health?.status || "unknown";
  const statusTier =
    status === "ok" ? "green" : status === "degraded" ? "yellow" : "neutral";

  const subs = health?.subsystems || {};
  const perf = health?.performance || {};
  const rss = perf.rss_mb ?? appMetrics?.resource_usage?.rss_mb;
  const uptime = perf.uptime_seconds;

  const counts = appMetrics?.counts || {};
  const accuracy = appMetrics?.prediction_accuracy || {};
  const accEntries = Object.entries(accuracy);

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">System Health &amp; Performance</h2>
        <span className={`badge badge--${statusTier}`}>{status}</span>
      </div>

      <div className="stats">
        <div className="stat">
          <div className="stat__label">Deployed models</div>
          <div className="stat__value">{health?.deployed_models ?? "—"}</div>
        </div>
        <div className="stat">
          <div className="stat__label">RSS (MB)</div>
          <div className="stat__value">{rss == null ? "—" : fmt(rss, 1)}</div>
        </div>
        <div className="stat">
          <div className="stat__label">Compute samples</div>
          <div className="stat__value">{counts.compute_samples ?? "—"}</div>
        </div>
        <div className="stat">
          <div className="stat__label">Uptime (s)</div>
          <div className="stat__value">{uptime == null ? "—" : fmt(uptime, 0)}</div>
        </div>
      </div>

      <div className="row" style={{ marginTop: 14 }}>
        <span className="muted">Subsystems:</span>
        <span className="row" style={{ gap: 6 }}>
          <span className={`dot ${subs.database ? "dot--up" : "dot--down"}`} />
          database
        </span>
        <span className="row" style={{ gap: 6 }}>
          <span className={`dot ${subs.redis ? "dot--up" : "dot--down"}`} />
          redis
        </span>
      </div>

      <div style={{ marginTop: 14 }}>
        <div className="card__hint" style={{ marginBottom: 6 }}>
          recent per-model accuracy
        </div>
        {accEntries.length === 0 ? (
          <div className="muted">No scored forecasts yet.</div>
        ) : (
          <table>
            <tbody>
              {accEntries.map(([name, acc]) => (
                <tr key={name}>
                  <td>{name}</td>
                  <td className="num">{pct(acc)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

import { pct, fmt, modelAgreement } from "../util.js";

// Drill-down on the current alert: surfaces the factors behind the forecast so
// an operator can judge it — aggregate confidence, cross-model agreement
// (derived from the spread of individual_forecasts), the weights actually used,
// any failed models, and data freshness (forecast generation time). Satisfies
// "drill-down analysis on alerts to inspect prediction factors".
export default function AlertDrillDown({ forecast }) {
  if (!forecast) {
    return (
      <section className="card">
        <div className="card__head">
          <h2 className="card__title">Alert Drill-Down</h2>
        </div>
        <div className="empty">No active forecast to analyse.</div>
      </section>
    );
  }

  const alert = forecast.alert_level || "—";
  const agreement = modelAgreement(forecast.individual_forecasts);
  const weights = forecast.weights_used || {};
  const failed = forecast.failed_models || [];
  const generatedAt = forecast.timestamp ? new Date(forecast.timestamp) : null;
  const ageMin =
    generatedAt && !Number.isNaN(generatedAt.getTime())
      ? Math.max(0, (Date.now() - generatedAt.getTime()) / 60000)
      : null;

  const weightEntries = Object.entries(weights);

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">Alert Drill-Down</h2>
        <span
          className={`badge badge--${
            alert === "high" ? "red" : alert === "medium" ? "yellow" : "green"
          }`}
        >
          {alert}
        </span>
      </div>

      <div className="factors">
        <div className="factor">
          <span className="factor__label">Aggregate confidence</span>
          <span className="factor__val">{pct(forecast.confidence)}</span>
        </div>
        <div className="factor">
          <span className="factor__label">Model agreement</span>
          <span className="factor__val">
            {agreement == null ? "—" : pct(agreement)}
          </span>
        </div>
        <div className="factor">
          <span className="factor__label">Data freshness</span>
          <span className="factor__val">
            {ageMin == null ? "—" : `${fmt(ageMin, 1)} min ago`}
          </span>
        </div>
        <div className="factor">
          <span className="factor__label">Cached</span>
          <span className="factor__val">{forecast.cached ? "yes" : "no"}</span>
        </div>
      </div>

      <div style={{ marginTop: 14 }}>
        <div className="card__hint" style={{ marginBottom: 6 }}>
          weights used
        </div>
        {weightEntries.length === 0 ? (
          <div className="muted">No weights reported.</div>
        ) : (
          <div className="tags">
            {weightEntries.map(([name, w]) => (
              <span key={name} className="tag">
                {name}: {fmt(w, 3)}
              </span>
            ))}
          </div>
        )}
      </div>

      <div style={{ marginTop: 12 }}>
        <div className="card__hint" style={{ marginBottom: 6 }}>
          failed models
        </div>
        {failed.length === 0 ? (
          <span className="badge badge--green">none — full ensemble</span>
        ) : (
          <div className="tags">
            {failed.map((name) => (
              <span key={name} className="tag" style={{ color: "var(--red)" }}>
                {name}
              </span>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

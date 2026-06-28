import {
  LineChart,
  Line,
  ResponsiveContainer,
  YAxis,
} from "recharts";
import { tierFor, pct } from "../util.js";

// Color-coded confidence indicator (green / yellow / red) for the current
// forecast's aggregate confidence, plus the alert-level badge and a per-step
// confidence sparkline. Tier thresholds come from the live /config so the
// colours track runtime changes.
export default function ConfidencePanel({ forecast, config }) {
  const high = Number(config?.high_confidence_threshold);
  const medium = Number(config?.medium_confidence_threshold);
  const highT = Number.isFinite(high) ? high : 0.85;
  const medT = Number.isFinite(medium) ? medium : 0.65;

  const confidence = Number(forecast?.confidence);
  const hasConf = Number.isFinite(confidence);
  const tier = hasConf ? tierFor(confidence, highT, medT) : "neutral";
  const alert = forecast?.alert_level || "—";

  const spark = (forecast?.ensemble_confidence || [])
    .map((v, i) => ({ i, c: Number(v) }))
    .filter((p) => Number.isFinite(p.c));

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">Forecast Confidence</h2>
        <span className={`badge badge--${tier === "neutral" ? "neutral" : tier}`}>
          alert: {alert}
        </span>
      </div>

      {!hasConf ? (
        <div className="empty">No forecast confidence available.</div>
      ) : (
        <div className="conf">
          <div className="conf__gauge">
            <span className={`conf__value conf__value--${tier}`}>
              {pct(confidence)}
            </span>
            <span className="muted">aggregate confidence</span>
          </div>

          <div className="conf__bar">
            <div
              className={`conf__bar-fill fill--${tier}`}
              style={{ width: `${Math.max(0, Math.min(1, confidence)) * 100}%` }}
            />
          </div>

          <div className="muted">
            Thresholds — green ≥ {pct(highT)}, yellow ≥ {pct(medT)}, red below.
          </div>

          {spark.length > 1 && (
            <div>
              <div className="card__hint" style={{ marginBottom: 4 }}>
                per-step confidence
              </div>
              <ResponsiveContainer width="100%" height={56}>
                <LineChart data={spark} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                  <YAxis domain={[0, 1]} hide />
                  <Line
                    type="monotone"
                    dataKey="c"
                    stroke={
                      tier === "green"
                        ? "var(--green)"
                        : tier === "yellow"
                        ? "var(--yellow)"
                        : "var(--red)"
                    }
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}
    </section>
  );
}

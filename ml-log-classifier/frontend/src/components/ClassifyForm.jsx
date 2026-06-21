import { useState } from "react";
import { classifyService } from "../api.js";
import { severityColor } from "./severityColors.js";

// Interactive classify form.
//
// A textarea + "Classify" button POSTs to /api/classify/service (the hierarchical
// model) so the result surfaces service routing + the cross-service anomaly score
// in addition to severity/category/confidence. Submitting also feeds the live
// metrics (the backend records every classification), so the charts/table react.

const SAMPLE =
  "Database connection failed: timeout after 30s connecting to primary replica";

function pct(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return `${(Number(value) * 100).toFixed(1)}%`;
}

// anomaly_score is a 0..1 risk-ish signal — tint it green→amber→red by band.
function anomalyColor(score) {
  const s = Number(score);
  if (Number.isNaN(s)) return "#94a3b8";
  if (s >= 0.66) return "#ef4444";
  if (s >= 0.33) return "#f59e0b";
  return "#22c55e";
}

function ResultCard({ label, value, color }) {
  return (
    <div className="result-card">
      <div className="result-card__label">{label}</div>
      <div className="result-card__value" style={color ? { color } : undefined}>
        {value}
      </div>
    </div>
  );
}

export default function ClassifyForm() {
  const [text, setText] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e) {
    e.preventDefault();
    const raw = text.trim();
    if (!raw || loading) return;
    setLoading(true);
    setError(null);
    try {
      const res = await classifyService(raw);
      setResult(res);
    } catch (err) {
      setError(err.message || "Classification failed");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card form-card">
      <h3 className="card__title">Classify a Log</h3>

      <form onSubmit={onSubmit} className="classify-form">
        <textarea
          className="classify-form__input"
          rows={3}
          placeholder="Paste a raw log line, e.g. 'Connection refused on port 5432'"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
        <div className="classify-form__actions">
          <button
            type="button"
            className="btn btn--ghost"
            onClick={() => setText(SAMPLE)}
            disabled={loading}
          >
            Use sample
          </button>
          <button
            type="submit"
            className="btn btn--primary"
            disabled={loading || text.trim() === ""}
          >
            {loading ? "Classifying…" : "Classify"}
          </button>
        </div>
      </form>

      {error ? <div className="form-card__error">{error}</div> : null}

      {result ? (
        <div className="result-grid">
          <ResultCard label="Service" value={result.service ?? "—"} />
          <ResultCard
            label="Severity"
            value={result.severity ?? "—"}
            color={severityColor(result.severity)}
          />
          <ResultCard label="Category" value={result.category ?? "—"} />
          <ResultCard label="Confidence" value={pct(result.confidence)} />
          <ResultCard
            label="Anomaly Score"
            value={
              result.anomaly_score === undefined || result.anomaly_score === null
                ? "—"
                : Number(result.anomaly_score).toFixed(3)
            }
            color={anomalyColor(result.anomaly_score)}
          />
        </div>
      ) : null}
    </div>
  );
}

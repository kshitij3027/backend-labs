import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Bar } from "react-chartjs-2";
// Side-effect import: registers BarElement + CategoryScale/LinearScale once.
import "./chartSetup.js";
import { getFeatureImportance } from "../api.js";

const TOP_N = 15;

// Horizontal bar of the model's top engineered features by RandomForest
// importance, fetched from GET /api/feature-importance on mount and on a manual
// "Refresh" (handy right after a retrain). Loading/empty/error states are all
// handled so it renders cleanly before (and without) data.

export default function FeatureImportance() {
  const [features, setFeatures] = useState([]);
  const [version, setVersion] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  // Guards setState after unmount (the fetch may resolve late).
  const mountedRef = useRef(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getFeatureImportance(TOP_N);
      if (!mountedRef.current) return;
      setFeatures(Array.isArray(data.features) ? data.features : []);
      setVersion(data.model_version ?? null);
    } catch (e) {
      if (!mountedRef.current) return;
      setError(e.message || "Failed to load feature importance");
    } finally {
      if (mountedRef.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    load();
    return () => {
      mountedRef.current = false;
    };
  }, [load]);

  // Already sorted desc by the backend; chart.js draws the first item at the
  // bottom of a horizontal bar, so reverse for top-down visual order.
  const ordered = useMemo(() => features.slice().reverse(), [features]);

  const data = useMemo(
    () => ({
      labels: ordered.map((f) => f.name),
      datasets: [
        {
          label: "Importance",
          data: ordered.map((f) => f.importance),
          backgroundColor: "#38bdf8", // --accent
          borderRadius: 4,
        },
      ],
    }),
    [ordered],
  );

  const options = useMemo(
    () => ({
      indexAxis: "y", // horizontal bars
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: { label: (ctx) => ` ${Number(ctx.parsed.x).toFixed(4)}` },
        },
      },
      scales: {
        x: {
          beginAtZero: true,
          grid: { color: "rgba(51, 65, 85, 0.4)" },
        },
        y: { grid: { display: false } },
      },
    }),
    [],
  );

  return (
    <div className="card chart-card">
      <h3 className="card__title">
        Feature Importance
        {version ? <span className="card__title-sub"> ({version})</span> : null}
        <button
          type="button"
          className="btn btn--ghost card__title-action"
          onClick={load}
          disabled={loading}
        >
          {loading ? "…" : "Refresh"}
        </button>
      </h3>

      <div className="chart-card__canvas chart-card__canvas--tall">
        {error ? (
          <div className="empty-state empty-state--error">{error}</div>
        ) : loading && features.length === 0 ? (
          <div className="empty-state">Loading…</div>
        ) : features.length === 0 ? (
          <div className="empty-state">No importances available</div>
        ) : (
          <Bar data={data} options={options} />
        )}
      </div>
    </div>
  );
}

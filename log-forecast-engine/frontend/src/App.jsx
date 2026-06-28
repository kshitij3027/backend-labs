import { useCallback, useEffect, useRef, useState } from "react";
import {
  getHealth,
  getAppMetrics,
  getMetricData,
  getForecastSteps,
  getModels,
  getConfig,
  DEFAULT_POLL_MS,
} from "./api.js";
import { METRICS, HORIZONS } from "./util.js";
import ForecastChart from "./components/ForecastChart.jsx";
import ConfidencePanel from "./components/ConfidencePanel.jsx";
import ModelComparison from "./components/ModelComparison.jsx";
import HealthPanel from "./components/HealthPanel.jsx";
import ConfigPanel from "./components/ConfigPanel.jsx";
import AlertDrillDown from "./components/AlertDrillDown.jsx";

// Top-level dashboard shell. Holds the selected metric + horizon and a single
// polling loop that refreshes every panel on the configured cadence (default
// 30s, overridable from /config's prediction_interval). Each child is defensive
// against null / loading / empty / degraded data, so the whole tree paints on
// first render before any fetch resolves.
export default function App() {
  const [metric, setMetric] = useState(METRICS[0]);
  // Default horizon: 1 hr (12 steps), matching the backend default horizon.
  const [horizonSteps, setHorizonSteps] = useState(12);

  const [forecast, setForecast] = useState(null);
  const [actual, setActual] = useState(null);
  const [models, setModels] = useState(null);
  const [health, setHealth] = useState(null);
  const [appMetrics, setAppMetrics] = useState(null);
  const [config, setConfig] = useState(null);

  const [pollMs, setPollMs] = useState(DEFAULT_POLL_MS);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pulsing, setPulsing] = useState(false);
  const [error, setError] = useState(null);

  // Refetch everything. Each call is isolated so one failing endpoint never
  // blanks the rest of the dashboard.
  const refreshAll = useCallback(async () => {
    setPulsing(true);
    const tasks = [
      getForecastSteps(horizonSteps, metric)
        .then(setForecast)
        .catch(() => setForecast(null)),
      getMetricData(metric, 120)
        .then(setActual)
        .catch(() => setActual(null)),
      getModels()
        .then(setModels)
        .catch(() => setModels(null)),
      getHealth()
        .then(setHealth)
        .catch(() => setHealth(null)),
      getAppMetrics()
        .then(setAppMetrics)
        .catch(() => setAppMetrics(null)),
    ];
    await Promise.allSettled(tasks);
    setLastUpdated(new Date());
    setError(null);
    setTimeout(() => setPulsing(false), 1200);
  }, [metric, horizonSteps]);

  // Load runtime config once (and adopt its poll cadence). Config is also
  // re-read by ConfigPanel after a save; here we just seed the poll interval.
  const loadConfig = useCallback(async () => {
    try {
      const cfg = await getConfig();
      setConfig(cfg);
      const minutes = Number(cfg?.prediction_interval_min);
      if (Number.isFinite(minutes) && minutes > 0) {
        setPollMs(minutes * 60_000);
      }
    } catch {
      setConfig(null);
    }
  }, []);

  useEffect(() => {
    loadConfig();
  }, [loadConfig]);

  // Polling loop: fire immediately on metric/horizon change, then on interval.
  const savedRefresh = useRef(refreshAll);
  savedRefresh.current = refreshAll;
  useEffect(() => {
    savedRefresh.current();
    const id = setInterval(() => savedRefresh.current(), pollMs);
    return () => clearInterval(id);
  }, [pollMs, metric, horizonSteps]);

  const horizonLabel =
    HORIZONS.find((h) => h.steps === horizonSteps)?.label || `${horizonSteps} steps`;

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">Log Forecast Engine</h1>
            <p className="app__subtitle">
              Live metric forecasting · ensemble confidence · alert drill-down
            </p>
          </div>
        </div>

        <div className="controls">
          <div className="control">
            <label htmlFor="metric-select">Metric</label>
            <select
              id="metric-select"
              value={metric}
              onChange={(e) => setMetric(e.target.value)}
            >
              {METRICS.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </div>

          <div className="control">
            <label htmlFor="horizon-select">Horizon</label>
            <select
              id="horizon-select"
              value={horizonSteps}
              onChange={(e) => setHorizonSteps(Number(e.target.value))}
            >
              {HORIZONS.map((h) => (
                <option key={h.steps} value={h.steps}>
                  {h.label}
                </option>
              ))}
            </select>
          </div>

          <div className="refresh" role="status" aria-live="polite">
            <span
              className={`refresh__dot ${pulsing ? "pulsing" : ""}`}
              aria-hidden="true"
            />
            <span>
              {lastUpdated
                ? `Updated ${lastUpdated.toLocaleTimeString()}`
                : "Loading…"}
              {` · every ${Math.round(pollMs / 1000)}s`}
            </span>
          </div>
        </div>
      </header>

      <main className="app__main">
        {error && <div className="banner">{error}</div>}

        {/* Centerpiece: predicted vs actual with confidence band. */}
        <ForecastChart
          forecast={forecast}
          actual={actual}
          metric={metric}
          horizonLabel={horizonLabel}
        />

        {/* Confidence (color-coded) + alert drill-down side by side. */}
        <div className="grid-2">
          <ConfidencePanel forecast={forecast} config={config} />
          <AlertDrillDown forecast={forecast} />
        </div>

        {/* Side-by-side model comparison (lines + weights/accuracy table). */}
        <ModelComparison forecast={forecast} models={models} />

        {/* System health/perf + runtime config controls. */}
        <div className="grid-2">
          <HealthPanel health={health} appMetrics={appMetrics} />
          <ConfigPanel
            config={config}
            metric={metric}
            onSaved={(cfg) => {
              setConfig(cfg);
              refreshAll();
            }}
          />
        </div>
      </main>
    </div>
  );
}

import { useCallback, useEffect, useRef, useState } from "react";
import { getHealth, getStats, DEFAULT_POLL_MS } from "./api.js";

// Poll cadence for the status strip. A constant for now; C17 may adopt a value
// supplied by GET /config.
const POLL_MS = DEFAULT_POLL_MS;

// Human labels for the per-subsystem component booleans in HealthResponse.components.
const COMPONENT_LABELS = [
  ["database", "Database"],
  ["vector_extension", "pgvector"],
  ["redis", "Redis"],
  ["embedding_model", "Embedding model"],
];

// One component readiness pill (green when true, red when false/absent).
function ComponentPill({ label, ok }) {
  return (
    <span className={`pill ${ok ? "pill--ok" : "pill--bad"}`}>
      <span className="pill__dot" aria-hidden="true" />
      {label}
    </span>
  );
}

// Top-level dashboard SHELL (C15). Proves the wiring end-to-end: a single polling
// loop fetches GET /health + GET /stats through nginx's /api proxy and renders
// service status, per-component readiness, and corpus size. The recommend form +
// suggestion cards arrive in C16; feedback + runtime controls in C17. Every field
// read is defensive so the tree paints on first render, before any fetch resolves.
export default function App() {
  const [health, setHealth] = useState(null);
  const [stats, setStats] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pulsing, setPulsing] = useState(false);
  const [reachable, setReachable] = useState(true);

  // Refetch health + stats. Isolated with allSettled so one failing endpoint
  // never blanks the other, and a total outage flips `reachable` without throwing.
  const refresh = useCallback(async () => {
    setPulsing(true);
    const [h, s] = await Promise.allSettled([getHealth(), getStats()]);

    if (h.status === "fulfilled") setHealth(h.value);
    else setHealth(null);

    if (s.status === "fulfilled") setStats(s.value);
    else setStats(null);

    setReachable(h.status === "fulfilled" || s.status === "fulfilled");
    setLastUpdated(new Date());
    setTimeout(() => setPulsing(false), 1000);
  }, []);

  // Polling loop: fire immediately on mount, then on interval.
  const saved = useRef(refresh);
  saved.current = refresh;
  useEffect(() => {
    saved.current();
    const id = setInterval(() => saved.current(), POLL_MS);
    return () => clearInterval(id);
  }, []);

  const status = health?.status ?? (reachable ? "unknown" : "unreachable");
  const statusOk = status === "ok";
  const components = health?.components ?? {};
  // Prefer stats.corpus_size (authoritative rollup), fall back to health's best-effort count.
  const corpusSize =
    stats?.corpus_size ?? health?.corpus_size ?? null;
  const embeddedCount = stats?.embedded_count ?? null;

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">Log Recommendation Engine</h1>
            <p className="app__subtitle">
              Similar-incident recommendations · semantic + contextual ranking ·
              feedback-tuned
            </p>
          </div>
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
            {` · every ${Math.round(POLL_MS / 1000)}s`}
          </span>
        </div>
      </header>

      <main className="app__main">
        {/* Status strip: overall service status + per-subsystem readiness + corpus size. */}
        <section className="card status">
          <div className="status__row">
            <span
              className={`badge ${
                statusOk ? "badge--ok" : reachable ? "badge--warn" : "badge--bad"
              }`}
            >
              <span className="badge__dot" aria-hidden="true" />
              {statusOk
                ? "Service OK"
                : reachable
                ? `Service ${status}`
                : "API unreachable"}
            </span>

            {health?.service && (
              <span className="status__meta">
                {health.service}
                {health.version ? ` v${health.version}` : ""}
              </span>
            )}
          </div>

          <div className="status__components">
            {COMPONENT_LABELS.map(([key, label]) => (
              <ComponentPill key={key} label={label} ok={Boolean(components[key])} />
            ))}
          </div>

          <div className="status__metrics">
            <div className="metric">
              <span className="metric__value">
                {corpusSize == null ? "—" : corpusSize.toLocaleString()}
              </span>
              <span className="metric__label">Incidents in corpus</span>
            </div>
            <div className="metric">
              <span className="metric__value">
                {embeddedCount == null ? "—" : embeddedCount.toLocaleString()}
              </span>
              <span className="metric__label">Embedded (searchable)</span>
            </div>
          </div>
        </section>

        {/* Placeholder for the recommend form + suggestion cards (C16). */}
        <section className="card placeholder">
          <h2 className="placeholder__title">Recommend UI</h2>
          <p className="placeholder__text">
            Query form and ranked suggestion cards arrive in C16; feedback and
            runtime controls in C17. This shell verifies the dashboard → nginx →
            API wiring via <code>/api/health</code> and <code>/api/stats</code>.
          </p>
        </section>
      </main>
    </div>
  );
}

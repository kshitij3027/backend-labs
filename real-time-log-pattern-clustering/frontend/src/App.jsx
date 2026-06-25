import { useClusterStream } from "./hooks/useWebSocket.js";
import StatCards from "./components/StatCards.jsx";
import ClusterScatter from "./components/ClusterScatter.jsx";
import PatternTimeline from "./components/PatternTimeline.jsx";
import QualityMetrics from "./components/QualityMetrics.jsx";
import AnomalyAlerts from "./components/AnomalyAlerts.jsx";

// Top-level dashboard shell: header + live connection indicator, then the
// headline stat cards driven by the shared `/ws/stream` snapshot. The cluster
// scatter, pattern timeline, anomaly alerts and drill-down panels mount into
// the placeholder section below in commits C15–C17. Every child is null-safe /
// has a loading state, so the whole tree renders on first paint before any data
// arrives.
export default function App() {
  const { snapshot, connected } = useClusterStream();

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">Real-Time Log Pattern Clustering</h1>
            <p className="app__subtitle">
              Live streaming clustering, pattern discovery &amp; anomaly detection
            </p>
          </div>
        </div>
        <div
          className="conn"
          role="status"
          aria-live="polite"
          title={connected ? "WebSocket connected" : "WebSocket disconnected"}
        >
          <span
            className={`conn__dot ${connected ? "conn__dot--up" : "conn__dot--down"}`}
            aria-hidden="true"
          />
          <span className="conn__label">
            {connected ? "Live" : "Reconnecting…"}
          </span>
        </div>
      </header>

      <main className="app__main">
        {/* Headline metrics — live from the WS snapshot. */}
        <StatCards snapshot={snapshot} />

        {/* Live cluster scatter — PCA-2D projection coloured by cluster, with
            algorithm tabs. Refetches as `stats.total_processed` advances. */}
        <ClusterScatter snapshot={snapshot} />

        {/* Pattern evolution timeline (wide) beside the cluster-quality tiles.
            Collapses to a single column on narrow viewports. */}
        <div className="grid-2">
          <PatternTimeline snapshot={snapshot} />
          <QualityMetrics snapshot={snapshot} />
        </div>

        {/* Live anomaly alerts feed — full width below the timeline/quality row. */}
        <AnomalyAlerts snapshot={snapshot} />

        {/* C17 panels (cluster drill-down + manual ingest form) mount below
            here next. */}
      </main>
    </div>
  );
}

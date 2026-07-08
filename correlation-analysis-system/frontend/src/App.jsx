import { useDashboard } from "./hooks/useDashboard.js";
import ErrorBanner from "./components/ErrorBanner.jsx";
import StatsCards from "./components/StatsCards.jsx";
import TimelineChart from "./components/TimelineChart.jsx";
import ScatterPlot from "./components/ScatterPlot.jsx";
import MatrixHeatmap from "./components/MatrixHeatmap.jsx";
import AlertsFeed from "./components/AlertsFeed.jsx";
import CorrelationsTable from "./components/CorrelationsTable.jsx";
import LogsTable from "./components/LogsTable.jsx";

// Poll cadence for the whole dashboard — one GET /api/v1/dashboard every 5s.
const POLL_MS = 5000;

// Top-level dashboard. A single polling loop (useDashboard) fetches the fat GET
// /api/v1/dashboard payload through nginx's /api proxy and fans it out to the stats
// grid, the visual analytics (timeline, scatter, matrix heatmap, alerts feed) and
// the two data tables. Every slice is read defensively so first render and a
// degraded payload never crash.
//
// Graceful degradation: the hook keeps the last good snapshot on a failed poll and
// raises `error`/`stale`, so the ErrorBanner explains the outage while the panels
// keep showing the last data instead of blanking out.
export default function App() {
  const { data, error, lastUpdated, loading, stale } = useDashboard(POLL_MS);

  // Defensive reads so first render (data === null) and a degraded payload never crash.
  const status = data?.status ?? {};
  const stats = data?.stats ?? {};
  const timeline = data?.timeline ?? [];
  const scatter = data?.scatter ?? [];
  const matrix = data?.matrix ?? { sources: [], cells: [] };
  const alerts = data?.alerts ?? [];
  const correlations = data?.recent_correlations ?? [];
  const logs = data?.recent_logs ?? [];

  const connecting = loading && !data;
  const connected = !error;
  const healthy = connected && Boolean(status.healthy);

  // Live indicator state: Connecting (first load) → Live (reachable) → Stale (error
  // but we still have a prior snapshot) → Offline (error, never got data).
  let liveTone = "bad";
  let liveText = "Offline";
  if (connecting) {
    liveTone = "wait";
    liveText = "Connecting…";
  } else if (healthy) {
    liveTone = "ok";
    liveText = "Live";
  } else if (stale) {
    liveTone = "warn";
    liveText = "Stale";
  }

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">Correlation Analysis Dashboard</h1>
            <p className="app__subtitle">
              Real-time multi-source log correlation · 5-second live poll
            </p>
          </div>
        </div>

        <div className="live" role="status" aria-live="polite">
          <span className={`live__dot live__dot--${liveTone}`} aria-hidden="true" />
          <span className="live__text">
            {liveText}
            {lastUpdated
              ? ` · updated ${lastUpdated.toLocaleTimeString([], { hour12: false })}`
              : ""}
            {` · every ${Math.round(POLL_MS / 1000)}s`}
          </span>
        </div>
      </header>

      <ErrorBanner error={error} lastUpdated={lastUpdated} />

      {connecting ? (
        <div className="connecting">
          <span className="spinner" aria-hidden="true" />
          <span>Connecting to backend…</span>
        </div>
      ) : (
        <main className="app__main">
          <StatsCards stats={stats} status={status} />

          {/* Visual analytics. Two responsive rows of paired panels (each collapses
              to a single column ≤900px), then the two full-width tables. Every slice
              is null-guarded above so first render + a degraded payload never crash. */}
          <section className="dashgrid-2">
            <TimelineChart timeline={timeline} />
            <ScatterPlot scatter={scatter} />
          </section>

          <section className="dashgrid-2">
            <MatrixHeatmap matrix={matrix} />
            <AlertsFeed alerts={alerts} />
          </section>

          <CorrelationsTable correlations={correlations} />
          <LogsTable logs={logs} />
        </main>
      )}
    </div>
  );
}

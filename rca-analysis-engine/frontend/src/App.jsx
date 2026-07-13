import { useIncidents } from "./hooks/useIncidents.js";
import ErrorBanner from "./components/ErrorBanner.jsx";
import IncidentsList from "./components/IncidentsList.jsx";
import TimelinePanel from "./components/TimelinePanel.jsx";
import RootCausesPanel from "./components/RootCausesPanel.jsx";
import { shortId, formatDateTime } from "./util.js";

// Top-level RCA dashboard shell (C11).
//
// `useIncidents` loads the existing incident history over REST and then live-updates it
// from the `/ws` WebSocket. The layout is a two-column split: a left incidents rail and
// a right detail pane (timeline + ranked root causes) for the selected incident, plus a
// reserved placeholder for the C12 interactive Plotly causal-graph / impact panels.
//
// Graceful degradation: the WebSocket status drives a live indicator and, when the feed
// drops or the initial fetch fails, an ErrorBanner — while the last-good incidents stay
// on screen instead of blanking out.
export default function App() {
  const { incidents, status, error, selectedId, setSelectedId } = useIncidents();

  // Resolve the selected incident, falling back to the newest so the detail pane is
  // populated even before the user clicks (and if the selection scrolled off the cap).
  const selected =
    incidents.find((i) => i.incident_id === selectedId) ?? incidents[0] ?? null;

  // Live indicator: Connecting (first connect) → Live (socket open) → Offline (dropped).
  let liveTone = "wait";
  let liveText = "Connecting…";
  if (status === "live") {
    liveTone = "ok";
    liveText = "Live";
  } else if (status === "offline") {
    liveTone = "bad";
    liveText = "Offline";
  }

  // Banner message: an initial-load failure takes priority (no data yet), otherwise a
  // dropped socket. Null when healthy (banner hidden).
  let bannerMessage = null;
  if (error) {
    bannerMessage = `${error} — showing last-known incidents.`;
  } else if (status === "offline") {
    bannerMessage = "Live connection lost — reconnecting. Showing last-known incidents.";
  }

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">RCA Analysis Dashboard</h1>
            <p className="app__subtitle">
              Causal root-cause analysis · live incident feed over WebSocket
            </p>
          </div>
        </div>

        <div className="live" role="status" aria-live="polite">
          <span className={`live__dot live__dot--${liveTone}`} aria-hidden="true" />
          <span className="live__text">{liveText}</span>
        </div>
      </header>

      <ErrorBanner message={bannerMessage} />

      <main className="app__main layout">
        <IncidentsList
          incidents={incidents}
          selectedId={selected?.incident_id ?? null}
          onSelect={setSelectedId}
        />

        <div className="detail">
          {selected ? (
            <>
              <section className="panel detail__head">
                <div>
                  <h2 className="detail__id">{shortId(selected.incident_id)}</h2>
                  <p className="detail__meta">
                    {formatDateTime(selected.timestamp)} ·{" "}
                    {selected.impact_analysis?.blast_radius ?? 0} downstream ·{" "}
                    {selected.impact_analysis?.affected_services?.length ?? 0} services
                    affected
                  </p>
                </div>
              </section>

              <TimelinePanel incident={selected} />
              <RootCausesPanel incident={selected} />

              {/* Reserved for C12: the interactive Plotly causal-graph network plot and
                  the richer impact / blast-radius panels. */}
              <section className="panel panel--placeholder">
                <div className="panel__head">
                  <h2 className="panel__title">Causal Graph</h2>
                  <span className="panel__soon">C12</span>
                </div>
                <p className="placeholder__note">
                  Interactive causal-graph network plot (severity-keyed nodes, weighted
                  edges, click-to-highlight blast radius) arrives in C12.
                </p>
              </section>
            </>
          ) : (
            <section className="panel panel--placeholder">
              <p className="placeholder__note">
                Select an incident to see its timeline and ranked root causes. New
                incidents stream in live as they are analyzed.
              </p>
            </section>
          )}
        </div>
      </main>
    </div>
  );
}

import { useCallback, useEffect, useState } from "react";
import { useIncidents } from "./hooks/useIncidents.js";
import ErrorBanner from "./components/ErrorBanner.jsx";
import IncidentsList from "./components/IncidentsList.jsx";
import TimelinePanel from "./components/TimelinePanel.jsx";
import RootCausesPanel from "./components/RootCausesPanel.jsx";
import CausalGraphPanel from "./components/CausalGraphPanel.jsx";
import ImpactPanel from "./components/ImpactPanel.jsx";
import { shortId, formatDateTime } from "./util.js";

// Top-level RCA dashboard shell.
//
// `useIncidents` loads the existing incident history over REST and then live-updates it
// from the `/ws` WebSocket. The layout is a two-column split: a left incidents rail and a
// right detail pane for the selected incident. The detail pane is a responsive grid — the
// interactive Plotly causal-graph plot is the prominent element (full width), with the
// ranked root causes, the impact / blast-radius panel, and the reconstructed timeline
// arranged around it; it collapses to a single column at ≤900px.
//
// A single `focusNodeId` is shared across the causal graph and the root-causes panel:
// clicking a graph node OR a root-cause row highlights that node's downstream blast
// radius in the plot and marks the matching root cause active. Selecting a different
// incident clears the focus.
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

  // Shared highlight, lifted here so the causal graph and the root-causes panel stay in
  // sync. Toggling the same node id clears it.
  const [focusNodeId, setFocusNodeId] = useState(null);
  const onFocusNode = useCallback((id) => {
    setFocusNodeId((cur) => (cur === id ? null : id));
  }, []);

  // Reset the highlight whenever the selected incident changes — a node id from one
  // incident is meaningless in another.
  const selectedIncidentId = selected?.incident_id ?? null;
  useEffect(() => {
    setFocusNodeId(null);
  }, [selectedIncidentId]);

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

              <div className="detailgrid">
                <div className="detailgrid__cell detailgrid__cell--wide">
                  <CausalGraphPanel
                    incident={selected}
                    focusNodeId={focusNodeId}
                    onFocusNode={onFocusNode}
                  />
                </div>
                <div className="detailgrid__cell">
                  <RootCausesPanel
                    incident={selected}
                    focusNodeId={focusNodeId}
                    onFocusNode={onFocusNode}
                  />
                </div>
                <div className="detailgrid__cell">
                  <ImpactPanel incident={selected} />
                </div>
                <div className="detailgrid__cell detailgrid__cell--wide">
                  <TimelinePanel incident={selected} />
                </div>
              </div>
            </>
          ) : (
            <section className="panel panel--placeholder">
              <p className="placeholder__note">
                Select an incident to see its causal graph, timeline, ranked root causes,
                and impact. New incidents stream in live as they are analyzed.
              </p>
            </section>
          )}
        </div>
      </main>
    </div>
  );
}

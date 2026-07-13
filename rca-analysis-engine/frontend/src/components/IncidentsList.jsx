import { levelClass, shortId, formatClock } from "../util.js";

// Left-rail list of analyzed incidents (newest-first). Each row shows the short
// incident id, its clock time, the top ranked root-cause service, and the root-cause
// count. Clicking a row selects it; the selected row is highlighted. Selection drives
// the detail pane (timeline + root causes) — and, in C12, the causal-graph panel.
//
// Props:
//   incidents  — array of IncidentReport (newest-first)
//   selectedId — currently selected incident_id (or null)
//   onSelect   — (incident_id) => void
export default function IncidentsList({ incidents, selectedId, onSelect }) {
  return (
    <section className="panel incidents">
      <div className="panel__head">
        <h2 className="panel__title">Incidents</h2>
        <span className="panel__count">{incidents.length}</span>
      </div>

      {incidents.length === 0 ? (
        <p className="placeholder__note">
          No incidents yet. Post a batch to <code>/api/analyze-incident</code> and it
          will appear here live.
        </p>
      ) : (
        <ul className="incidents__list">
          {incidents.map((inc) => {
            const top = inc.root_causes?.[0];
            const count = inc.root_causes?.length ?? 0;
            const selected = inc.incident_id === selectedId;
            return (
              <li key={inc.incident_id}>
                <button
                  type="button"
                  className={`incidentrow${selected ? " incidentrow--active" : ""}`}
                  onClick={() => onSelect(inc.incident_id)}
                  aria-pressed={selected}
                >
                  <div className="incidentrow__top">
                    <span className="incidentrow__id">{shortId(inc.incident_id)}</span>
                    <span className="incidentrow__time">
                      {formatClock(inc.timestamp)}
                    </span>
                  </div>
                  <div className="incidentrow__bottom">
                    {top ? (
                      <span className="incidentrow__cause">
                        <span
                          className={`dot dot--${levelClass(top.level)}`}
                          aria-hidden="true"
                        />
                        {top.service}
                      </span>
                    ) : (
                      <span className="incidentrow__cause incidentrow__cause--none">
                        no root cause
                      </span>
                    )}
                    <span className="incidentrow__badge">
                      {count} cause{count === 1 ? "" : "s"}
                    </span>
                  </div>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

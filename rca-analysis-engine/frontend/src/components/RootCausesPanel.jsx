import { levelClass, clamp01, pct, fmt, shortId, truncate } from "../util.js";

// Ranked root-cause candidates for the selected incident (C12 enrichment of the C11
// panel). Each candidate shows its rank, service, severity, `T+M:SS` position in the
// incident, a calibrated-confidence bar (with the pre-calibration raw value when it
// differs), and its base-rate anomaly score. The concurrent multi-hypothesis shortlist
// renders below as chips coloured by lifecycle state (tentative / confirmed / pruned).
//
// Each row is clickable and drives the shared `focusNodeId`: selecting a root cause
// highlights its downstream blast radius in the CausalGraphPanel, and the row matching
// the graph's currently-focused node is marked active — so the two panels stay in sync.
// Reads every field defensively so a partial report never crashes render.
//
// Props:
//   incident    — the selected IncidentReport (or null)
//   focusNodeId — the shared highlighted node id (or null)
//   onFocusNode — (event_id) => void; toggles the shared highlight
export default function RootCausesPanel({ incident, focusNodeId, onFocusNode }) {
  const rootCauses = incident?.root_causes ?? [];
  const hypotheses = incident?.hypotheses ?? [];
  const anomalyScores = incident?.anomaly_scores ?? {};

  // event_id -> relative_time ("T+M:SS") from the timeline, so each cause shows when it
  // fired relative to the incident start (root causes carry only an absolute timestamp).
  const relTime = new Map();
  for (const entry of incident?.timeline ?? []) {
    if (entry?.event_id) relTime.set(entry.event_id, entry.relative_time);
  }

  const select = (id) => {
    if (id != null && typeof onFocusNode === "function") onFocusNode(id);
  };

  return (
    <section className="panel" data-testid="root-causes-panel">
      <div className="panel__head">
        <h2 className="panel__title">Root Causes</h2>
        <span className="panel__count">{rootCauses.length}</span>
      </div>

      {rootCauses.length === 0 ? (
        <p className="placeholder__note">No root-cause candidates identified.</p>
      ) : (
        <ol className="rootcauses">
          {rootCauses.map((rc, i) => {
            const conf = clamp01(rc.confidence);
            const raw = rc.raw_confidence;
            const showRaw =
              raw != null && Number.isFinite(Number(raw)) && Math.abs(raw - conf) >= 0.005;
            const anomaly = anomalyScores[rc.event_id];
            const hasAnomaly = anomaly != null && Number.isFinite(Number(anomaly));
            const when = relTime.get(rc.event_id);
            const active = rc.event_id != null && rc.event_id === focusNodeId;

            return (
              <li
                key={rc.event_id ?? i}
                className={`rootcause rootcause--clickable${
                  active ? " rootcause--active" : ""
                }`}
                role="button"
                tabIndex={0}
                aria-pressed={active}
                title="Highlight this cause's blast radius in the graph"
                onClick={() => select(rc.event_id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    select(rc.event_id);
                  }
                }}
              >
                <div className="rootcause__head">
                  <span className="rootcause__rank">#{i + 1}</span>
                  <span
                    className={`dot dot--${levelClass(rc.level)}`}
                    aria-hidden="true"
                  />
                  <span className="rootcause__service">{rc.service}</span>
                  <span className={`level level--${levelClass(rc.level)}`}>
                    {String(rc.level ?? "").toUpperCase()}
                  </span>
                  <span className="rootcause__conf">{pct(conf)}</span>
                </div>

                <div className="rootcause__meta">
                  {when && <span className="rootcause__time">{when}</span>}
                  {showRaw && (
                    <span className="rootcause__raw" title="pre-calibration confidence">
                      raw {pct(raw)}
                    </span>
                  )}
                  {hasAnomaly && (
                    <span
                      className="rootcause__anomaly"
                      title="base-rate anomaly score (0–1)"
                    >
                      anomaly {fmt(anomaly, 2)}
                    </span>
                  )}
                </div>

                <div
                  className="confbar"
                  role="meter"
                  aria-valuenow={Math.round(conf * 100)}
                  aria-valuemin={0}
                  aria-valuemax={100}
                >
                  <div className="confbar__fill" style={{ width: `${conf * 100}%` }} />
                </div>

                <p className="rootcause__msg" title={rc.message}>
                  {truncate(rc.message, 130)}
                </p>
              </li>
            );
          })}
        </ol>
      )}

      {hypotheses.length > 0 && (
        <div className="hypotheses">
          <div className="hypotheses__label">Hypotheses</div>
          <div className="chips">
            {hypotheses.map((h) => (
              <span
                key={h.hypothesis_id}
                className={`chip chip--hyp chip--${h.state}`}
                title={`${h.state} · target ${h.root_cause_event_id} · ${pct(h.confidence)}`}
              >
                <span className="chip__dot" aria-hidden="true" />
                {shortId(h.root_cause_event_id)}
                <span className="chip__state">{h.state}</span>
                <span className="chip__conf">{pct(h.confidence)}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

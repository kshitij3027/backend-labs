import { levelClass, clamp01, pct, shortId, truncate } from "../util.js";

// Ranked root-cause candidates for the selected incident, each with a confidence bar,
// plus the concurrent multi-hypothesis chips (tentative / confirmed / pruned). This is
// the basic-but-real C11 version; C12 enriches it (and adds the impact / causal-graph
// panels). Reads every field defensively so a partial report never crashes render.
//
// Props:
//   incident — the selected IncidentReport (or null)
export default function RootCausesPanel({ incident }) {
  const rootCauses = incident?.root_causes ?? [];
  const hypotheses = incident?.hypotheses ?? [];

  return (
    <section className="panel">
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
            return (
              <li key={rc.event_id ?? i} className="rootcause">
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
                <div className="confbar" role="meter" aria-valuenow={Math.round(conf * 100)}>
                  <div
                    className="confbar__fill"
                    style={{ width: `${conf * 100}%` }}
                  />
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

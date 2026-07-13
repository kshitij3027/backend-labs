import { fmt, truncate } from "../util.js";

// Impact / blast-radius panel (C12). Surfaces the selected incident's
// `impact_analysis` — blast radius, affected services, total events and the weighted
// reachability scalar — plus the post-mortem `recovery_points` (interior choke points
// where an intervention truncates the largest downstream subtree) and a summary of the
// `event_classifications` (how many events were primary triggers vs propagation-path vs
// contributing factors). Reads every field defensively so a partial report never crashes.
//
// Props:
//   incident — the selected IncidentReport (or null)

// event_classifications values are the EventClass enum values (lowercase_underscore).
const CLASS_META = [
  { key: "primary_trigger", label: "Primary trigger", cls: "primary" },
  { key: "propagation_path", label: "Propagation path", cls: "propagation" },
  { key: "contributing_factor", label: "Contributing factor", cls: "contributing" },
];

export default function ImpactPanel({ incident }) {
  const impact = incident?.impact_analysis ?? {};
  const details = impact.details ?? {};
  const services = impact.affected_services ?? [];
  const recovery = incident?.recovery_points ?? [];
  const classifications = incident?.event_classifications ?? {};

  const blastRadius = Number(impact.blast_radius ?? 0);
  const totalEvents = Number(impact.total_events ?? 0);
  const weighted = details.weighted_impact;

  // Tally event classes for the summary row.
  const counts = { primary_trigger: 0, propagation_path: 0, contributing_factor: 0 };
  for (const value of Object.values(classifications)) {
    if (value in counts) counts[value] += 1;
  }
  const hasClasses = Object.keys(classifications).length > 0;

  return (
    <section className="panel" data-testid="impact-panel">
      <div className="panel__head">
        <h2 className="panel__title">Impact / Blast Radius</h2>
        <span className="panel__count">{services.length} svc</span>
      </div>

      <div className="impactstats">
        <div className="impactstat">
          <span className="impactstat__value">{blastRadius}</span>
          <span className="impactstat__label">Blast radius</span>
        </div>
        <div className="impactstat">
          <span className="impactstat__value">{services.length}</span>
          <span className="impactstat__label">Affected services</span>
        </div>
        <div className="impactstat">
          <span className="impactstat__value">{totalEvents}</span>
          <span className="impactstat__label">Total events</span>
        </div>
        <div className="impactstat">
          <span className="impactstat__value">{fmt(weighted, 2)}</span>
          <span className="impactstat__label">Weighted impact</span>
        </div>
      </div>

      <div className="impactblock">
        <div className="impactblock__label">Affected services</div>
        {services.length === 0 ? (
          <p className="placeholder__note">No downstream services affected.</p>
        ) : (
          <div className="chips">
            {services.map((svc) => (
              <span key={svc} className="chip chip--service">
                {svc}
              </span>
            ))}
          </div>
        )}
      </div>

      <div className="impactblock">
        <div className="impactblock__label">Recovery points</div>
        {recovery.length === 0 ? (
          <p className="placeholder__note">
            No interior recovery point — the trigger propagated straight to leaf effects.
          </p>
        ) : (
          <ul className="recovery">
            {recovery.map((rp, i) => (
              <li key={rp.event_id ?? i} className="recovery__item">
                <div className="recovery__top">
                  <span className="recovery__service">{rp.service}</span>
                  <span
                    className="recovery__gated"
                    title="events in the downstream subtree this choke point gates"
                  >
                    gates {rp.gated_subtree_size}
                  </span>
                </div>
                <p className="recovery__why" title={rp.rationale}>
                  {truncate(rp.rationale, 140)}
                </p>
              </li>
            ))}
          </ul>
        )}
      </div>

      {hasClasses && (
        <div className="impactblock">
          <div className="impactblock__label">Event classification</div>
          <div className="classcounts">
            {CLASS_META.map((c) => (
              <div key={c.key} className={`classcount classcount--${c.cls}`}>
                <span className="classcount__value">{counts[c.key]}</span>
                <span className="classcount__label">{c.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

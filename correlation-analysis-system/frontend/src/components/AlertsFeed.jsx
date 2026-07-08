import { hhmmss, relativeTime, typeClass, typeLabel } from "../util.js";

// Cap the feed so a long-running session doesn't render an unbounded list.
const MAX_ROWS = 20;

const SEV_LABELS = { critical: "Critical", warning: "Warning", info: "Info" };

/** Normalise an alert severity to one of critical|warning|info (default info). */
function sevClass(severity) {
  const s = String(severity ?? "").toLowerCase();
  return s === "critical" || s === "warning" || s === "info" ? s : "info";
}

// Live operator alerts, newest first. Each row carries a severity-coloured left
// border + dot, the alert title/message, the originating correlation-type chip and
// a relative timestamp. The backend already returns these newest-first, but we sort
// defensively so a reordered payload can't scramble the feed.
//
// Props:
//   alerts — dashboard.alerts, or [] while loading / degraded
export default function AlertsFeed({ alerts = [] }) {
  const all = Array.isArray(alerts) ? alerts : [];
  const rows = [...all]
    .sort((a, b) => (Number(b?.created_at) || 0) - (Number(a?.created_at) || 0))
    .slice(0, MAX_ROWS);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Alerts</h2>
        <span className="panel__count">{all.length}</span>
      </div>

      {rows.length === 0 ? (
        <div className="chart-empty" style={{ minHeight: 120 }}>
          No alerts
        </div>
      ) : (
        <ul className="alerts">
          {rows.map((a, i) => {
            const sev = sevClass(a?.severity);
            return (
              <li key={a?.id ?? i} className={`alert alert--${sev}`}>
                <div className="alert__head">
                  <span className="alert__sev">
                    <span className="alert__dot" aria-hidden="true" />
                    {SEV_LABELS[sev]}
                  </span>
                  <span className="alert__time" title={hhmmss(a?.created_at)}>
                    {relativeTime(Number(a?.created_at) * 1000)}
                  </span>
                </div>
                <div className="alert__title">{a?.title ?? "Alert"}</div>
                {a?.message ? <div className="alert__msg">{a.message}</div> : null}
                <div className="alert__meta">
                  <span className={`typechip type--${typeClass(a?.correlation_type)}`}>
                    <span className="typechip__dot" aria-hidden="true" />
                    <span className="typechip__label">
                      {typeLabel(a?.correlation_type)}
                    </span>
                  </span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

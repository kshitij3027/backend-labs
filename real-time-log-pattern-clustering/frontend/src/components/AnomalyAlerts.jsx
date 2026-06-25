// Live anomaly alerts feed (C16).
//
// Renders `snapshot.anomalies` (already newest-first, up to 20) as a scrollable
// feed. Each row surfaces the originating service, the (truncated) log message,
// the algorithms that flagged it as chips, the anomaly score and a local
// HH:MM:SS timestamp. Fully null-safe: it renders a calm empty state before any
// data and never throws on missing fields or unparseable dates.

/** Format an ISO/epoch timestamp as local HH:MM:SS, or "" if unparseable. */
function formatTime(ts) {
  if (ts === null || ts === undefined) {
    return "";
  }
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return "";
  }
  return d.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

/** Format a numeric score to 2dp, or "—" when it's missing/non-numeric. */
function formatScore(score) {
  return Number.isFinite(score) ? score.toFixed(2) : "—";
}

/**
 * @param {{ snapshot: (object|null) }} props the shared WS snapshot; reads
 *   `snapshot.anomalies` (list of `{timestamp, message, service, algorithms,
 *   score}`, newest first).
 */
export default function AnomalyAlerts({ snapshot }) {
  const anomalies = Array.isArray(snapshot?.anomalies) ? snapshot.anomalies : [];
  const count = anomalies.length;

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Anomaly Alerts</h3>
        <span
          className="count-badge"
          title={`${count} anomalies in the current window`}
        >
          {count}
        </span>
      </div>

      {count === 0 ? (
        <div className="alert-empty">No anomalies detected yet.</div>
      ) : (
        <ul className="alert-list" aria-label="Recent anomaly alerts">
          {anomalies.map((a, i) => {
            const service = a?.service || "—";
            const message = a?.message || "(no message)";
            const algorithms = Array.isArray(a?.algorithms)
              ? a.algorithms
              : [];
            const time = formatTime(a?.timestamp);
            // Anomalies have no stable id in the frame; index is fine for a
            // capped, newest-first feed that fully re-renders each snapshot.
            return (
              <li className="alert-row" key={i}>
                <div className="alert-row__top">
                  <span className="alert-row__service">{service}</span>
                  <span className="alert-row__score" title="Anomaly score">
                    {formatScore(a?.score)}
                  </span>
                </div>
                <div className="alert-row__msg" title={message}>
                  {message}
                </div>
                <div className="alert-row__meta">
                  <span className="alert-row__chips">
                    {algorithms.map((algo, j) => (
                      <span className="chip" key={j}>
                        {algo}
                      </span>
                    ))}
                  </span>
                  {time ? (
                    <time className="alert-row__time">{time}</time>
                  ) : null}
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

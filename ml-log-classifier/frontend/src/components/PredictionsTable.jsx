import { severityColor } from "./severityColors.js";

// Table of the most-recent classifications from the live snapshot.
//
// `snapshot.recent_predictions` is a ring buffer ordered newest-LAST (see
// src/metrics.py), so we reverse a copy to show most-recent first. Null-safe:
// an absent/empty list renders an empty-state row.

const RAW_LOG_MAX = 80; // chars before we truncate the raw_log cell

function truncate(text, max) {
  const s = String(text ?? "");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function fmtPct(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function SeverityBadge({ severity }) {
  const color = severityColor(severity);
  return (
    <span
      className="badge"
      style={{
        color,
        borderColor: color,
        backgroundColor: `${color}1f`, // ~12% alpha tint
      }}
    >
      {severity || "—"}
    </span>
  );
}

export default function PredictionsTable({ snapshot }) {
  const recent = (snapshot && snapshot.recent_predictions) || [];
  // Copy then reverse so we don't mutate the snapshot; newest first.
  const rows = recent.slice().reverse();

  return (
    <div className="card table-card">
      <h3 className="card__title">
        Recent Predictions
        {rows.length > 0 ? (
          <span className="card__title-sub"> ({rows.length})</span>
        ) : null}
      </h3>

      {rows.length === 0 ? (
        <div className="empty-state">No predictions yet</div>
      ) : (
        <div className="table-wrap">
          <table className="pred-table">
            <thead>
              <tr>
                <th className="pred-table__log">Log</th>
                <th>Severity</th>
                <th>Category</th>
                <th className="pred-table__num">Confidence</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={`${r.ts ?? ""}-${i}`}>
                  <td className="pred-table__log" title={r.raw_log || ""}>
                    <code>{truncate(r.raw_log, RAW_LOG_MAX)}</code>
                  </td>
                  <td>
                    <SeverityBadge severity={r.severity} />
                  </td>
                  <td>{r.category || "—"}</td>
                  <td className="pred-table__num">{fmtPct(r.confidence)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

import { fmt, hhmmss, typeClass, typeLabel } from "../util.js";

// A strength cell: a small proportional bar plus the 2dp value. Width is clamped
// to [0, 1] so an out-of-range strength can't overflow the cell.
function StrengthBar({ value }) {
  const x = Number(value);
  const pct = Number.isFinite(x) ? Math.max(0, Math.min(1, x)) * 100 : 0;
  return (
    <div className="strengthbar" title={fmt(value, 3)}>
      <div className="strengthbar__track">
        <div className="strengthbar__fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="strengthbar__val">{fmt(value, 2)}</span>
    </div>
  );
}

// One event endpoint (source over service) for the A / B columns.
function Endpoint({ event }) {
  const ev = event ?? {};
  return (
    <div className="endpoint">
      <span className="endpoint__source">{ev.source ?? "—"}</span>
      {ev.service ? <span className="endpoint__service">{ev.service}</span> : null}
    </div>
  );
}

// Recent detected correlations, newest first (the backend returns them already
// ordered). Basic, non-sortable table for C9 — sortable headers arrive in C10.
// Every field is read defensively so a partial row never crashes the render.
//
// Props:
//   correlations — dashboard.recent_correlations, or [] while loading / degraded
export default function CorrelationsTable({ correlations = [] }) {
  const rows = Array.isArray(correlations) ? correlations : [];

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Recent correlations</h2>
        <span className="panel__count">{rows.length}</span>
      </div>
      <div className="tablewrap">
        <table className="datatable">
          <thead>
            <tr>
              <th>Time</th>
              <th>Type</th>
              <th>A</th>
              <th>B</th>
              <th>Strength</th>
              <th>Conf.</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td className="datatable__empty" colSpan={6}>
                  No correlations detected yet
                </td>
              </tr>
            ) : (
              rows.map((c, i) => (
                <tr key={c?.id ?? i}>
                  <td className="datatable__time">{hhmmss(c?.detected_at)}</td>
                  <td>
                    <span className={`typechip type--${typeClass(c?.correlation_type)}`}>
                      <span className="typechip__dot" aria-hidden="true" />
                      <span className="typechip__label">
                        {typeLabel(c?.correlation_type)}
                      </span>
                    </span>
                  </td>
                  <td>
                    <Endpoint event={c?.event_a} />
                  </td>
                  <td>
                    <Endpoint event={c?.event_b} />
                  </td>
                  <td>
                    <StrengthBar value={c?.strength} />
                  </td>
                  <td className="datatable__num">{fmt(c?.confidence, 2)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

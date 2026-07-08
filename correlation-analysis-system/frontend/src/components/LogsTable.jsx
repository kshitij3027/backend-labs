import { hhmmss, levelClass, shortId, truncate } from "../util.js";

// Recent raw log events, newest first (backend-ordered). Level is colour-coded
// (ERROR red / WARN amber / INFO grey) and error rows get a subtle tint so an
// operator can eyeball where the noise is. Non-sortable for C9.
//
// Props:
//   logs — dashboard.recent_logs, or [] while loading / degraded
export default function LogsTable({ logs = [] }) {
  const rows = Array.isArray(logs) ? logs : [];

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Recent logs</h2>
        <span className="panel__count">{rows.length}</span>
      </div>
      <div className="tablewrap">
        <table className="datatable">
          <thead>
            <tr>
              <th>Time</th>
              <th>Source</th>
              <th>Service</th>
              <th>Level</th>
              <th>Message</th>
              <th>Corr ID</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td className="datatable__empty" colSpan={6}>
                  No logs yet
                </td>
              </tr>
            ) : (
              rows.map((ev, i) => {
                const lvl = levelClass(ev?.level);
                return (
                  <tr
                    key={ev?.id ?? i}
                    className={lvl === "error" ? "datatable__row--error" : undefined}
                  >
                    <td className="datatable__time">{hhmmss(ev?.timestamp)}</td>
                    <td>{ev?.source ?? "—"}</td>
                    <td>{ev?.service ?? "—"}</td>
                    <td>
                      <span className={`level level--${lvl}`}>
                        {String(ev?.level ?? "—").toUpperCase()}
                      </span>
                    </td>
                    <td className="datatable__msg" title={ev?.message ?? ""}>
                      {truncate(ev?.message, 90)}
                    </td>
                    <td className="datatable__corr" title={ev?.correlation_id ?? ""}>
                      {shortId(ev?.correlation_id)}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

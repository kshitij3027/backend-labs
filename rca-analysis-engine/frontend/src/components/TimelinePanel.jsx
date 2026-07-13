import { levelClass, truncate } from "../util.js";

// Reconstructed incident timeline: one row per event in chronological order, showing
// the sequence number, the `T+M:SS` relative offset from incident start, the service,
// the severity-coloured level, and the message. Scrollable so a long incident stays
// contained. Reads every field defensively so a partial report never crashes render.
//
// Props:
//   incident — the selected IncidentReport (or null)
export default function TimelinePanel({ incident }) {
  const timeline = incident?.timeline ?? [];

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Timeline</h2>
        <span className="panel__count">{timeline.length}</span>
      </div>

      {timeline.length === 0 ? (
        <p className="placeholder__note">No timeline entries for this incident.</p>
      ) : (
        <div className="tablewrap timeline__scroll">
          <table className="datatable">
            <thead>
              <tr>
                <th>#</th>
                <th>Offset</th>
                <th>Service</th>
                <th>Level</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {timeline.map((e) => (
                <tr key={e.event_id ?? e.sequence_id}>
                  <td className="datatable__num">{e.sequence_id}</td>
                  <td className="datatable__time">{e.relative_time}</td>
                  <td className="timeline__service">{e.service}</td>
                  <td>
                    <span className={`level level--${levelClass(e.level)}`}>
                      {String(e.level ?? "").toUpperCase()}
                    </span>
                  </td>
                  <td className="datatable__msg" title={e.message}>
                    {truncate(e.message, 140)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

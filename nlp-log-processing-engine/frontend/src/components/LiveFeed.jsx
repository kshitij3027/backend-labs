import {
  entityClass,
  intentClass,
  intentLabel,
  sentimentClass,
  truncate,
  formatClock,
} from "../util.js";

// Scrolling list of the most recent analyses streamed over the WebSocket (newest first).
// Each row shows a severity dot, a truncated message, an intent badge, a couple of entity
// mini-chips and the client receive-time. The panel header carries the WS connection
// status so the user can see when the live feed is connecting / live / dropped.
//
// Props:
//   feed   — array of analysis results (each stamped with `_key` and `_ts` by the hook),
//            already capped and newest-first.
//   status — "connecting" | "live" | "offline".

const STATUS_TEXT = {
  live: "Live",
  connecting: "Connecting…",
  offline: "Offline",
};
const STATUS_TONE = { live: "ok", connecting: "wait", offline: "bad" };

export default function LiveFeed({ feed = [], status = "connecting" }) {
  const tone = STATUS_TONE[status] || "wait";
  const text = STATUS_TEXT[status] || "Connecting…";

  return (
    <section className="panel livefeed">
      <div className="panel__head">
        <h2 className="panel__title">Live feed</h2>
        <span className="live" role="status" aria-live="polite">
          <span className={`live__dot live__dot--${tone}`} aria-hidden="true" />
          <span className="live__text">{text}</span>
        </span>
      </div>

      {feed.length === 0 ? (
        <p className="muted">
          Waiting for analyses… run one above, or POST to <code>/api/analyze</code>. Every
          analyzed line streams in here live.
        </p>
      ) : (
        <ul className="feed">
          {feed.map((item) => {
            const entities = Array.isArray(item.entities) ? item.entities : [];
            const sentLabel = item.sentiment?.label;
            return (
              <li key={item._key} className="feedrow">
                <div className="feedrow__top">
                  <span
                    className={`dot dot--${sentimentClass(sentLabel)}`}
                    title={sentimentClass(sentLabel)}
                    aria-hidden="true"
                  />
                  <span className="feedrow__msg" title={item.message}>
                    {truncate(item.message, 96)}
                  </span>
                  <span className="feedrow__time">{formatClock(item._ts)}</span>
                </div>
                <div className="feedrow__bottom">
                  <span
                    className={`badge badge--sm intent intent--${intentClass(
                      item.intent?.label
                    )}`}
                  >
                    {intentLabel(item.intent?.label)}
                  </span>
                  {entities.slice(0, 3).map((e, i) => (
                    <span
                      key={i}
                      className={`minichip ent--${entityClass(e.label)}`}
                      title={`${e.text} · ${e.label}`}
                    >
                      {truncate(e.text, 18)}
                    </span>
                  ))}
                  {entities.length > 3 ? (
                    <span className="feedrow__more">+{entities.length - 3}</span>
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

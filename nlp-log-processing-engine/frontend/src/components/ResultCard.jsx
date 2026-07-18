import {
  entityClass,
  intentClass,
  intentLabel,
  sentimentClass,
  fmt,
  pct,
} from "../util.js";

// Renders ONE analysis result (AnalysisResponse):
//   * the original message with recognised entities highlighted inline, colour-coded by
//     label (SERVICE / HOST / IP / USER_ID / ERROR_CODE / PATH / URL / PORT, plus a
//     "general" fallback for spaCy's own ORG/GPE/DATE/... entities);
//   * an intent badge with its confidence %; the "other" reject bucket renders as "Other";
//   * a sentiment badge colour-coded by severity (positive=green, neutral=grey,
//     negative=orange, critical=red) with the raw VADER compound score;
//   * a labelled entity chip list (so entities are legible even when offsets are missing);
//   * keyword chips.
// Empty entities / keywords degrade to a muted "none" note rather than an empty gap.
//
// Props:
//   result — an AnalysisResponse, or null/undefined (renders nothing).

/**
 * Slice `message` into ordered segments, each either plain text or a labelled entity span,
 * using the entities' char offsets. Entities without usable integer offsets (or that fall
 * outside the message, or overlap an already-emitted span) are skipped here — the chip list
 * below still shows them. Returns `[{text, label?}]` covering the whole message in order.
 */
function highlightSegments(message, entities) {
  const msg = typeof message === "string" ? message : "";
  const valid = (Array.isArray(entities) ? entities : [])
    .filter(
      (e) =>
        e &&
        Number.isInteger(e.start) &&
        Number.isInteger(e.end) &&
        e.start >= 0 &&
        e.end <= msg.length &&
        e.start < e.end
    )
    .sort((a, b) => a.start - b.start);

  const segments = [];
  let cursor = 0;
  for (const e of valid) {
    if (e.start < cursor) continue; // overlaps a span we already emitted — skip
    if (e.start > cursor) segments.push({ text: msg.slice(cursor, e.start) });
    segments.push({ text: msg.slice(e.start, e.end), label: e.label });
    cursor = e.end;
  }
  if (cursor < msg.length) segments.push({ text: msg.slice(cursor) });
  // A message with no highlightable entities still renders as a single plain segment.
  if (segments.length === 0) segments.push({ text: msg });
  return segments;
}

export default function ResultCard({ result }) {
  if (!result || typeof result !== "object") return null;

  const message = typeof result.message === "string" ? result.message : "";
  const entities = Array.isArray(result.entities) ? result.entities : [];
  const keywords = Array.isArray(result.keywords) ? result.keywords : [];
  const intent = result.intent || {};
  const sentiment = result.sentiment || {};

  const segments = highlightSegments(message, entities);

  return (
    <section className="panel resultcard">
      <div className="panel__head">
        <h2 className="panel__title">Result</h2>
        <div className="badges">
          <span className={`badge intent intent--${intentClass(intent.label)}`}>
            {intentLabel(intent.label)}
            <span className="badge__meta">{pct(intent.confidence)}</span>
          </span>
          <span className={`badge sentiment sentiment--${sentimentClass(sentiment.label)}`}>
            {sentimentClass(sentiment.label)}
            <span className="badge__meta">{fmt(sentiment.score, 2)}</span>
          </span>
        </div>
      </div>

      {/* Message with inline, colour-coded entity highlights. */}
      <p className="resultcard__message">
        {segments.map((seg, i) =>
          seg.label ? (
            <mark
              key={i}
              className={`ent ent--${entityClass(seg.label)}`}
              title={seg.label}
            >
              {seg.text}
              <span className="ent__tag">{seg.label}</span>
            </mark>
          ) : (
            <span key={i}>{seg.text}</span>
          )
        )}
      </p>

      {/* Entities as a labelled chip list (legible even when offsets are absent). */}
      <div className="resultcard__section">
        <div className="resultcard__label">
          Entities <span className="resultcard__count">{entities.length}</span>
        </div>
        {entities.length === 0 ? (
          <p className="muted">No entities detected.</p>
        ) : (
          <ul className="entchips">
            {entities.map((e, i) => (
              <li key={i} className={`entchip ent--${entityClass(e.label)}`}>
                <span className="entchip__dot" aria-hidden="true" />
                <span className="entchip__text">{e.text}</span>
                <span className="entchip__label">{e.label}</span>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Trending / salient keywords. */}
      <div className="resultcard__section">
        <div className="resultcard__label">
          Keywords <span className="resultcard__count">{keywords.length}</span>
        </div>
        {keywords.length === 0 ? (
          <p className="muted">No keywords extracted.</p>
        ) : (
          <ul className="chips">
            {keywords.map((kw, i) => (
              <li key={i} className="kw">
                {kw}
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

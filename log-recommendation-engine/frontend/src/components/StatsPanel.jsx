import { fmt } from "../util.js";

// Turn a {key: count} map into a stable, sorted (desc by count) list of entries,
// capped so a huge cardinality doesn't blow up the panel. Non-object -> [].
function topEntries(map, limit = 8) {
  if (!map || typeof map !== "object") return [];
  return Object.entries(map)
    .filter(([, v]) => Number.isFinite(Number(v)))
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .slice(0, limit);
}

function num(v) {
  const x = Number(v);
  return Number.isFinite(x) ? x.toLocaleString() : "—";
}

// One labelled count for the compact breakdown rows (by_service / by_severity).
function DistRow({ items, empty }) {
  if (items.length === 0) return <div className="stats__none">{empty}</div>;
  return (
    <div className="stats__dist">
      {items.map(([k, v]) => (
        <span className="stats__distitem" key={k}>
          <span className="stats__distkey">{k}</span>
          <span className="stats__distval">{num(v)}</span>
        </span>
      ))}
    </div>
  );
}

// Corpus + feedback rollup panel (C17). Purely presentational: it renders the
// `stats` object App already polls on the shared ~15s loop (GET /stats), so it
// auto-refreshes for free and the "helpful/unhelpful/served" counters tick up live
// as votes land and recommendations are served. Every field is read defensively so
// the panel paints before the first poll resolves.
//
// Props:
//   stats       — the latest StatsResponse, or null while loading / on outage
//   lastUpdated — Date of the last successful poll (for the freshness line)
//   pollMs      — poll cadence in ms (shown as "· every Ns")
export default function StatsPanel({ stats, lastUpdated, pollMs }) {
  const byService = topEntries(stats?.by_service);
  const bySeverity = topEntries(stats?.by_severity, 6);
  const topPatterns = Array.isArray(stats?.top_patterns)
    ? stats.top_patterns.slice(0, 6)
    : [];

  const helpful = Number(stats?.feedback_helpful);
  const unhelpful = Number(stats?.feedback_unhelpful);
  const total = Number.isFinite(Number(stats?.feedback_total))
    ? Number(stats.feedback_total)
    : (Number.isFinite(helpful) ? helpful : 0) +
      (Number.isFinite(unhelpful) ? unhelpful : 0);

  // Helpful share of all votes — a quick read on whether the corpus is trusted.
  const helpfulPct =
    total > 0 && Number.isFinite(helpful)
      ? Math.round((helpful / total) * 100)
      : null;

  return (
    <section className="card stats">
      <div className="card__head">
        <h2 className="card__title">Corpus &amp; feedback</h2>
        <span className="card__hint">
          {lastUpdated
            ? `updated ${lastUpdated.toLocaleTimeString()}`
            : "loading…"}
          {pollMs ? ` · every ${Math.round(pollMs / 1000)}s` : ""}
        </span>
      </div>

      {!stats ? (
        <div className="empty">Stats unavailable.</div>
      ) : (
        <>
          {/* Headline counters. */}
          <div className="stats__grid">
            <div className="statbox">
              <span className="statbox__val">{num(stats.corpus_size)}</span>
              <span className="statbox__lab">Corpus</span>
            </div>
            <div className="statbox">
              <span className="statbox__val">{num(stats.embedded_count)}</span>
              <span className="statbox__lab">Embedded</span>
            </div>
            <div className="statbox">
              <span className="statbox__val">
                {num(stats.recommendations_served)}
              </span>
              <span className="statbox__lab">Served</span>
            </div>
            <div className="statbox">
              <span className="statbox__val">{num(total)}</span>
              <span className="statbox__lab">Votes</span>
            </div>
          </div>

          {/* Feedback split: helpful vs unhelpful, with a share bar. */}
          <div className="stats__section">
            <div className="stats__secthead">
              <span className="stats__secttitle">Feedback</span>
              {helpfulPct != null && (
                <span className="stats__pct">{helpfulPct}% helpful</span>
              )}
            </div>
            <div className="fbbar" role="img" aria-label="helpful vs unhelpful share">
              <span
                className="fbbar__seg fbbar__seg--up"
                style={{ width: `${helpfulPct == null ? 0 : helpfulPct}%` }}
              />
              <span
                className="fbbar__seg fbbar__seg--down"
                style={{
                  width: `${helpfulPct == null ? 0 : 100 - helpfulPct}%`,
                }}
              />
            </div>
            <div className="fbbar__legend">
              <span className="fb-up">👍 {num(helpful)} helpful</span>
              <span className="fb-down">👎 {num(unhelpful)} not helpful</span>
            </div>
          </div>

          {/* Corpus distributions. */}
          <div className="stats__section">
            <span className="stats__secttitle">By service</span>
            <DistRow items={byService} empty="no service breakdown yet" />
          </div>

          <div className="stats__section">
            <span className="stats__secttitle">By severity</span>
            <DistRow items={bySeverity} empty="no severity breakdown yet" />
          </div>

          {/* Learned query patterns with their helpful / unhelpful tallies. */}
          <div className="stats__section">
            <span className="stats__secttitle">Top query patterns</span>
            {topPatterns.length === 0 ? (
              <div className="stats__none">no feedback patterns yet</div>
            ) : (
              <ul className="patterns">
                {topPatterns.map((p, i) => (
                  <li className="pattern" key={`${p?.query_pattern ?? i}`}>
                    <span className="pattern__name" title={p?.query_pattern}>
                      {p?.query_pattern || "(default)"}
                    </span>
                    <span className="pattern__counts">
                      <span className="fb-up">+{fmt(p?.helpful, 0)}</span>
                      <span className="fb-down">−{fmt(p?.unhelpful, 0)}</span>
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </>
      )}
    </section>
  );
}

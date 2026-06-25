// A responsive grid of headline metric cards.
//
// Reads straight from the live snapshot's `stats` object. Everything is
// null-safe: before any data (or any WS tick) `snapshot` is null and each card
// shows a 0 / em-dash placeholder, so the dashboard renders cleanly on first
// paint and never crashes on a missing field.

const DASH = "—"; // em dash placeholder for "no data yet"

/** Format an integer count; falls back to "0" so counters read sensibly. */
function fmtInt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0";
  }
  return Number(value).toLocaleString();
}

/** Format a throughput rate to one decimal + unit, or the dash placeholder. */
function fmtRate(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return DASH;
  }
  return `${Number(value).toFixed(1)} logs/s`;
}

function StatCard({ label, value, accent, hint }) {
  return (
    <div className="stat-card" data-accent={accent || undefined}>
      <div className="stat-card__label">{label}</div>
      <div className="stat-card__value">{value}</div>
      {hint ? <div className="stat-card__hint">{hint}</div> : null}
    </div>
  );
}

export default function StatCards({ snapshot }) {
  const s = (snapshot && snapshot.stats) || {};
  const algos = Array.isArray(s.algorithms) ? s.algorithms : [];

  return (
    <section className="stat-cards" aria-label="Live clustering metrics">
      <StatCard
        label="Throughput"
        value={fmtRate(s.throughput_per_sec)}
        accent="throughput"
      />
      <StatCard
        label="Total Clusters"
        value={fmtInt(s.total_clusters)}
        accent="clusters"
        hint={algos.length ? `${algos.length} algorithms` : undefined}
      />
      <StatCard
        label="Patterns Discovered"
        value={fmtInt(s.patterns_discovered)}
        accent="patterns"
      />
      <StatCard
        label="Anomalies"
        value={fmtInt(s.anomalies_detected)}
        accent="anomalies"
      />
      <StatCard
        label="Processed"
        value={fmtInt(s.total_processed)}
        accent="processed"
      />
    </section>
  );
}

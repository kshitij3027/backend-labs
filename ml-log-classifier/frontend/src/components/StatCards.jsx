// A responsive grid of headline metric cards.
//
// Reads straight from the live metrics snapshot. Everything is null-safe: before
// any data (or any WS tick) `snapshot` is null and each card shows an em dash,
// so the dashboard renders cleanly on first paint and never crashes on a missing
// field.

const DASH = "—"; // em dash placeholder for "no data yet"

/** Format an integer count, or the dash placeholder when absent. */
function fmtInt(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return DASH;
  }
  return Number(value).toLocaleString();
}

/** Format a number to a fixed number of decimals, or the dash placeholder. */
function fmtFixed(value, digits) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return DASH;
  }
  return Number(value).toFixed(digits);
}

/** Format a 0..1 confidence as a whole-number percent, or the dash placeholder. */
function fmtPct(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return DASH;
  }
  return `${(Number(value) * 100).toFixed(1)}%`;
}

/** Show a string value, or the dash placeholder when empty/missing. */
function fmtStr(value) {
  if (value === null || value === undefined || value === "") {
    return DASH;
  }
  return String(value);
}

function StatCard({ label, value, accent }) {
  return (
    <div className="stat-card">
      <div className="stat-card__label">{label}</div>
      <div
        className="stat-card__value"
        data-accent={accent || undefined}
        title={typeof value === "string" ? value : undefined}
      >
        {value}
      </div>
    </div>
  );
}

export default function StatCards({ snapshot }) {
  const s = snapshot || {};

  return (
    <section className="stat-cards" aria-label="Live classification metrics">
      <StatCard label="Total Classified" value={fmtInt(s.total_classified)} />
      <StatCard
        label="Throughput (per sec)"
        value={fmtFixed(s.throughput_per_sec, 1)}
      />
      <StatCard label="Avg Confidence" value={fmtPct(s.avg_confidence)} />
      <StatCard
        label="Model Status"
        value={fmtStr(s.model_status)}
        accent={s.model_status || undefined}
      />
      <StatCard label="Current Version" value={fmtStr(s.current_version)} />
    </section>
  );
}

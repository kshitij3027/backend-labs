// Small shared helpers used across the correlation dashboard components.

// The five correlation families the engine emits (backend CorrelationType enum
// values). Order here drives the per-type chip order in StatsCards. Each maps to
// a CSS accent class `type--<value>` defined in styles.css.
export const CORRELATION_TYPES = [
  "temporal",
  "session_based",
  "user_based",
  "error_cascade",
  "metric_based",
];

// Short human labels for the correlation-type chips (the raw enum values are
// snake_case and a touch long for a dense table cell).
export const TYPE_LABELS = {
  temporal: "Temporal",
  session_based: "Session",
  user_based: "User",
  error_cascade: "Cascade",
  metric_based: "Metric",
};

/** Display label for a correlation type; unknown types echo back verbatim. */
export function typeLabel(type) {
  return TYPE_LABELS[type] ?? String(type ?? "—");
}

/** Stable CSS modifier suffix for a correlation type; unknown -> "unknown". */
export function typeClass(type) {
  return CORRELATION_TYPES.includes(String(type)) ? String(type) : "unknown";
}

/** Format a number to `n` decimals; "—" for non-finite input. */
export function fmt(v, n = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(n);
}

/** Format an integer with thousands separators; "—" for non-finite input. */
export function num(v) {
  const x = Number(v);
  return Number.isFinite(x) ? Math.round(x).toLocaleString() : "—";
}

/**
 * Format an epoch-seconds timestamp as a local HH:MM:SS clock string. Accepts the
 * float epoch-seconds the backend emits (detected_at / timestamp / created_at).
 * "—" for non-finite input.
 */
export function hhmmss(epochSeconds) {
  const x = Number(epochSeconds);
  if (!Number.isFinite(x)) return "—";
  return new Date(x * 1000).toLocaleTimeString([], { hour12: false });
}

/**
 * Coarse relative-time string ("just now", "12s ago", "3m ago", "1h ago") from a
 * Date (or epoch-ms number). Used by the error banner to say how stale the last
 * good update is. Returns "just now" for null/invalid input's caller to guard.
 */
export function relativeTime(from, now = Date.now()) {
  const then = from instanceof Date ? from.getTime() : Number(from);
  if (!Number.isFinite(then)) return "just now";
  const secs = Math.max(0, Math.round((now - then) / 1000));
  if (secs < 5) return "just now";
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ago`;
}

/** Truncate a string to `max` chars with an ellipsis; passthrough for short/empty. */
export function truncate(s, max = 90) {
  const str = String(s ?? "");
  return str.length > max ? `${str.slice(0, max - 1)}…` : str;
}

/** Short form of a correlation/session id for a dense cell (last 8 chars). */
export function shortId(id) {
  const s = String(id ?? "");
  if (!s) return "—";
  return s.length > 8 ? `…${s.slice(-8)}` : s;
}

/** Stable CSS modifier for a log level; unknown levels get "other". */
export function levelClass(level) {
  const l = String(level ?? "").toUpperCase();
  if (l === "ERROR" || l === "CRITICAL" || l === "FATAL") return "error";
  if (l === "WARN" || l === "WARNING") return "warn";
  if (l === "INFO") return "info";
  if (l === "DEBUG") return "debug";
  return "other";
}

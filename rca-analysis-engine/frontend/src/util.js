// Small shared helpers used across the RCA dashboard components. Self-contained (no
// external deps) so the bundle stays lean and CSP-safe behind nginx.

/** The four log levels the engine emits, in ascending severity order. */
export const LOG_LEVELS = ["INFO", "WARNING", "ERROR", "CRITICAL"];

/**
 * Stable CSS modifier suffix for a log level -> `.level--<suffix>` in styles.css.
 * CRITICAL/ERROR/WARNING/INFO each get their own severity accent; anything else
 * falls back to "other".
 */
export function levelClass(level) {
  const l = String(level ?? "").toUpperCase();
  if (l === "CRITICAL" || l === "FATAL") return "critical";
  if (l === "ERROR") return "error";
  if (l === "WARNING" || l === "WARN") return "warning";
  if (l === "INFO") return "info";
  return "other";
}

/** Sort rank for a log level (higher = more severe). */
export function levelRank(level) {
  const l = String(level ?? "").toUpperCase();
  if (l === "CRITICAL" || l === "FATAL") return 4;
  if (l === "ERROR") return 3;
  if (l === "WARNING" || l === "WARN") return 2;
  if (l === "INFO") return 1;
  return 0;
}

/** Clamp a value into [0, 1]; 0 for non-finite input. */
export function clamp01(v) {
  const x = Number(v);
  return Number.isFinite(x) ? Math.max(0, Math.min(1, x)) : 0;
}

/** Format a 0..1 confidence as a whole-number percent string (e.g. "72%"). */
export function pct(v) {
  return `${Math.round(clamp01(v) * 100)}%`;
}

/** Format a number to `n` decimals; "—" for non-finite input. */
export function fmt(v, n = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(n);
}

/**
 * Short form of an incident/event id for a dense cell. Strips a known prefix
 * (`inc-`, `evt-`) for readability and keeps it compact.
 */
export function shortId(id) {
  const s = String(id ?? "");
  if (!s) return "—";
  return s.length > 14 ? `${s.slice(0, 13)}…` : s;
}

/**
 * Format a backend timestamp (ISO 8601 string, or epoch seconds/ms) as a local
 * HH:MM:SS clock string. Falls back to the raw value when it can't be parsed, so a
 * non-ISO timestamp the client happened to post still renders something sensible.
 */
export function formatClock(value) {
  const d = toDate(value);
  if (!d) return String(value ?? "—");
  return d.toLocaleTimeString([], { hour12: false });
}

/** Format a backend timestamp as a local date + time; raw fallback when unparseable. */
export function formatDateTime(value) {
  const d = toDate(value);
  if (!d) return String(value ?? "—");
  return `${d.toLocaleDateString()} ${d.toLocaleTimeString([], { hour12: false })}`;
}

/** Best-effort Date from an ISO string or epoch seconds/ms; null when unparseable. */
function toDate(value) {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    // Heuristic: seconds vs. milliseconds.
    return new Date(value < 1e12 ? value * 1000 : value);
  }
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Coarse relative-time string ("just now", "12s ago", "3m ago", "1h ago") from a
 * Date (or epoch-ms number). Used by the error banner to say how stale the last
 * good snapshot is.
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
export function truncate(s, max = 120) {
  const str = String(s ?? "");
  return str.length > max ? `${str.slice(0, max - 1)}…` : str;
}

// Small shared helpers used across the NLP dashboard components. Self-contained (no
// external deps) so the bundle stays lean and CSP-safe behind nginx.
//
// The vocabulary constants below mirror the backend BY VALUE (see src/generators.py and
// src/nlp/*): the dashboard never imports Python, so the intent / sentiment / entity label
// sets are duplicated here on purpose. They drive the colour-coding class helpers.

// -- vocabulary ---------------------------------------------------------------------

/** The eight intents the classifier emits (src/generators.INTENTS). The analyzer also
 *  returns the "other" low-confidence reject bucket, which is NOT in this list. */
export const INTENTS = [
  "authentication",
  "deployment",
  "error_report",
  "health_check",
  "resource_warning",
  "network",
  "database",
  "config_change",
];

/** The four severity/sentiment classes, ascending in alarm (src/nlp/sentiment). */
export const SENTIMENT_LABELS = ["positive", "neutral", "negative", "critical"];

/** The eight log-specific entity labels the NER layer targets (src/nlp/entity.LOG_LABELS).
 *  General spaCy entities (ORG, GPE, DATE, ...) are additive and fall back to "general". */
export const LOG_ENTITY_LABELS = [
  "SERVICE",
  "HOST",
  "IP",
  "USER_ID",
  "ERROR_CODE",
  "PATH",
  "URL",
  "PORT",
];

const _INTENT_SET = new Set(INTENTS);
const _SENTIMENT_SET = new Set(SENTIMENT_LABELS);
const _LOG_ENTITY_SET = new Set(LOG_ENTITY_LABELS);

// -- class-suffix helpers (map a label -> a stable CSS modifier in styles.css) ------

/** Stable CSS suffix for an intent -> `.intent--<suffix>`; unknown / reject -> "other". */
export function intentClass(label) {
  const l = String(label ?? "").toLowerCase();
  return _INTENT_SET.has(l) ? l : "other";
}

/** Human-readable intent label: "error_report" -> "Error report"; empty/other -> "Other". */
export function intentLabel(label) {
  const l = String(label ?? "").trim();
  if (!l || l.toLowerCase() === "other") return "Other";
  const spaced = l.replace(/_/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

/** Stable CSS suffix for a sentiment -> `.sentiment--<suffix>` / `.dot--<suffix>`;
 *  anything unexpected falls back to "neutral". */
export function sentimentClass(label) {
  const l = String(label ?? "").toLowerCase();
  return _SENTIMENT_SET.has(l) ? l : "neutral";
}

/**
 * Stable CSS suffix for an entity label -> `.ent--<suffix>`. Log-specific labels get their
 * own colour (lower-cased, e.g. USER_ID -> "user_id"); any general spaCy entity (ORG, GPE,
 * DATE, ...) falls back to "general".
 */
export function entityClass(label) {
  const raw = String(label ?? "");
  return _LOG_ENTITY_SET.has(raw.toUpperCase()) ? raw.toLowerCase() : "general";
}

// -- formatting ---------------------------------------------------------------------

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
  return Number.isFinite(x) ? x.toFixed(n) : "—";
}

/** Truncate a string to `max` chars with an ellipsis; passthrough for short/empty. */
export function truncate(s, max = 120) {
  const str = String(s ?? "");
  return str.length > max ? `${str.slice(0, max - 1)}…` : str;
}

/**
 * Format an epoch (seconds or ms) or ISO timestamp as a local HH:MM:SS clock string.
 * Falls back to the raw value when it can't be parsed.
 */
export function formatClock(value) {
  const d = toDate(value);
  return d ? d.toLocaleTimeString([], { hour12: false }) : String(value ?? "—");
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

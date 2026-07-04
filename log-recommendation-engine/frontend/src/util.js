// Small shared helpers used across the recommendation dashboard components.

// The severities the backend understands (project contract). Rendered as the
// options of the RecommendForm's severity <select> and used to colour chips.
export const SEVERITIES = ["critical", "high", "medium", "low"];

/** Format a 0..1 (or any) number to `n` decimals; "—" for non-finite input. */
export function fmt(v, n = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(n);
}

/** Format a 0..1 relevance as a whole-number percent string ("73%"). */
export function pct(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return `${Math.round(x * 100)}%`;
}

/**
 * Split a comma-separated tag string into a clean array: trim each entry, drop
 * blanks, de-duplicate while preserving order. Mirrors the backend's tag
 * normalisation so what the user types is what gets queried.
 */
export function parseTags(raw) {
  const seen = new Set();
  const out = [];
  for (const part of String(raw || "").split(",")) {
    const t = part.trim();
    if (t && !seen.has(t)) {
      seen.add(t);
      out.push(t);
    }
  }
  return out;
}

/** Stable CSS modifier for a severity chip; unknown severities get "muted". */
export function severityTier(sev) {
  return SEVERITIES.includes(String(sev)) ? String(sev) : "muted";
}

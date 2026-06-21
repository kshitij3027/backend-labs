// Shared severity → color mapping so the doughnut chart, the table badges, and
// the classify-form result card all agree on what (say) ERROR looks like.
//
// Keys are matched case-insensitively. Anything unknown falls back to a neutral
// slate. These hexes line up with the status tokens in styles.css.

const SEVERITY_COLORS = {
  DEBUG: "#64748b", // slate-500
  INFO: "#38bdf8", // sky-400 (accent)
  WARN: "#f59e0b", // amber-500
  WARNING: "#f59e0b",
  ERROR: "#ef4444", // red-500
  CRITICAL: "#b91c1c", // red-700
  FATAL: "#7f1d1d", // red-900
};

const FALLBACK = "#94a3b8"; // slate-400

/** Return a hex color for a severity label (case-insensitive), or a neutral default. */
export function severityColor(label) {
  if (!label) return FALLBACK;
  return SEVERITY_COLORS[String(label).toUpperCase()] || FALLBACK;
}

/** Build a parallel array of colors for an array of severity labels. */
export function severityColors(labels) {
  return (labels || []).map(severityColor);
}

// A small categorical palette for non-severity charts (categories, services).
// Cycled by index; readable on the dark theme.
const CATEGORICAL = [
  "#38bdf8", // sky
  "#a78bfa", // violet
  "#34d399", // emerald
  "#fbbf24", // amber
  "#f472b6", // pink
  "#f87171", // red
  "#60a5fa", // blue
  "#2dd4bf", // teal
  "#c084fc", // purple
  "#fb923c", // orange
];

/** Pick `count` colors from the categorical palette, cycling as needed. */
export function categoricalColors(count) {
  const out = [];
  for (let i = 0; i < count; i += 1) {
    out.push(CATEGORICAL[i % CATEGORICAL.length]);
  }
  return out;
}

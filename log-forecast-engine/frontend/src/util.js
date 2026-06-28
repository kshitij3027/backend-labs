// Small shared helpers used across dashboard components.

export const METRICS = ["response_time", "error_rate", "throughput"];

// Horizon selector: label -> number of future steps. The backend interval is
// 5 minutes per step (project_requirements.md §7 prediction_interval), so
// steps = minutes / 5. These all fall within the 1..288 (24h) horizon bounds.
export const HORIZONS = [
  { label: "15 min", steps: 3 },
  { label: "30 min", steps: 6 },
  { label: "1 hr", steps: 12 },
  { label: "2 hr", steps: 24 },
  { label: "4 hr", steps: 48 },
  { label: "8 hr", steps: 96 },
  { label: "12 hr", steps: 144 },
  { label: "24 hr", steps: 288 },
];

const PALETTE = [
  "var(--m0)",
  "var(--m1)",
  "var(--m2)",
  "var(--m3)",
  "var(--m4)",
];

/** Stable colour for a model line by index. */
export function modelColor(i) {
  return PALETTE[i % PALETTE.length];
}

/** Map a confidence (0..1) to a tier given thresholds; returns "green"|"yellow"|"red". */
export function tierFor(confidence, high, medium) {
  const c = Number(confidence);
  if (!Number.isFinite(c)) return "red";
  if (c >= high) return "green";
  if (c >= medium) return "yellow";
  return "red";
}

/** Format an ISO timestamp into a short HH:MM (local) label for chart axes. */
export function shortTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

/** Round a number to `n` decimals, returning "—" for non-finite input. */
export function fmt(v, n = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(n);
}

/** Format a 0..1 confidence as a whole-number percent string. */
export function pct(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "—";
  return `${Math.round(x * 100)}%`;
}

/**
 * Model-agreement score in [0,1] derived from the spread of per-step individual
 * forecasts: tight clustering across models -> high agreement. Returns null when
 * there are fewer than two models or no data.
 */
export function modelAgreement(individualForecasts) {
  const series = Object.values(individualForecasts || {}).filter(
    (a) => Array.isArray(a) && a.length,
  );
  if (series.length < 2) return null;
  const steps = Math.min(...series.map((a) => a.length));
  if (steps === 0) return null;

  let totalCv = 0;
  let counted = 0;
  for (let i = 0; i < steps; i++) {
    const col = series.map((a) => Number(a[i])).filter(Number.isFinite);
    if (col.length < 2) continue;
    const mean = col.reduce((s, v) => s + v, 0) / col.length;
    const variance =
      col.reduce((s, v) => s + (v - mean) * (v - mean), 0) / col.length;
    const std = Math.sqrt(variance);
    // Coefficient of variation, guarded against near-zero means.
    const denom = Math.abs(mean) > 1e-9 ? Math.abs(mean) : 1;
    totalCv += std / denom;
    counted += 1;
  }
  if (counted === 0) return null;
  const avgCv = totalCv / counted;
  // Squash CV into a 0..1 agreement score (CV=0 -> 1; large CV -> ~0).
  return 1 / (1 + avgCv);
}

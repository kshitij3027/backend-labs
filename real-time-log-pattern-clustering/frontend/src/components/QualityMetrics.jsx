// Cluster quality metrics panel (C16).
//
// Surfaces the three clustering-quality scores from `snapshot.quality` (falling
// back to the matching `snapshot.stats` fields). Each is shown as a tile with a
// 3dp value; silhouette and coherence also render a thin bar mapped into [0,1].
// Davies–Bouldin (lower is better) shows no bar. Fully null-safe: a missing
// metric renders "—" and never produces NaN.

/** First finite number among the args, else null. */
function firstFinite(...vals) {
  for (const v of vals) {
    if (Number.isFinite(v)) {
      return v;
    }
  }
  return null;
}

/** Format a metric value to 3dp, or "—" when null/non-finite. */
function fmt(v) {
  return Number.isFinite(v) ? v.toFixed(3) : "—";
}

/** Clamp n into [lo, hi]. */
function clamp(n, lo, hi) {
  return Math.min(hi, Math.max(lo, n));
}

/** Silhouette ∈ [-1, 1] → fill percent [0, 100]. */
function silhouettePct(v) {
  if (!Number.isFinite(v)) {
    return 0;
  }
  return clamp((v + 1) / 2, 0, 1) * 100;
}

/** Coherence ∈ [0, 1] → fill percent [0, 100]. */
function coherencePct(v) {
  if (!Number.isFinite(v)) {
    return 0;
  }
  return clamp(v, 0, 1) * 100;
}

/**
 * @param {{ snapshot: (object|null) }} props the shared WS snapshot; reads
 *   `snapshot.quality` ({silhouette, davies_bouldin, coherence}) with a fallback
 *   to the same-named `snapshot.stats` fields.
 */
export default function QualityMetrics({ snapshot }) {
  const quality = snapshot?.quality;
  const stats = snapshot?.stats;

  const silhouette = firstFinite(quality?.silhouette, stats?.silhouette);
  const coherence = firstFinite(quality?.coherence, stats?.coherence);
  const daviesBouldin = firstFinite(
    quality?.davies_bouldin,
    stats?.davies_bouldin,
  );

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Cluster Quality</h3>
      </div>

      <div className="metric-tiles">
        <div className="metric-tile" data-accent="patterns">
          <div className="metric-tile__label">Silhouette</div>
          <div className="metric-tile__value">{fmt(silhouette)}</div>
          <div
            className="metric-bar"
            role="img"
            aria-label={`Silhouette ${fmt(silhouette)}`}
          >
            <span
              className="metric-bar__fill metric-bar__fill--patterns"
              style={{ width: `${silhouettePct(silhouette)}%` }}
            />
          </div>
          <div className="metric-tile__hint">separation · higher better</div>
        </div>

        <div className="metric-tile" data-accent="throughput">
          <div className="metric-tile__label">Coherence</div>
          <div className="metric-tile__value">{fmt(coherence)}</div>
          <div
            className="metric-bar"
            role="img"
            aria-label={`Coherence ${fmt(coherence)}`}
          >
            <span
              className="metric-bar__fill metric-bar__fill--throughput"
              style={{ width: `${coherencePct(coherence)}%` }}
            />
          </div>
          <div className="metric-tile__hint">intra-cluster · higher better</div>
        </div>

        <div className="metric-tile" data-accent="clusters">
          <div className="metric-tile__label">Davies–Bouldin</div>
          <div className="metric-tile__value">{fmt(daviesBouldin)}</div>
          <div className="metric-tile__hint">lower is better</div>
        </div>
      </div>
    </section>
  );
}

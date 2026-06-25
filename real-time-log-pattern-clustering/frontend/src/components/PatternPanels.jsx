import { useCallback, useEffect, useRef, useState } from "react";
import {
  getTemporalPatterns,
  getPerformancePatterns,
  getBehavioralPatterns,
  getSequencePatterns,
} from "../api.js";

// The four batch-mining views, in tab order. Each maps a stable key to its label
// and the api.js getter that fetches it. Keeping the fetcher on the tab object
// means the fetch effect stays a single generic path for all four views.
const TABS = [
  { key: "temporal", label: "Temporal", fetch: getTemporalPatterns },
  { key: "performance", label: "Performance", fetch: getPerformancePatterns },
  { key: "behavioral", label: "Behavioral", fetch: getBehavioralPatterns },
  { key: "sequence", label: "Sequence", fetch: getSequencePatterns },
];

/** Format a number to `dp` decimals, or "—" when it's missing / non-numeric. */
function fmt(v, dp = 1) {
  return Number.isFinite(v) ? v.toFixed(dp) : "—";
}

/** Coerce to a finite number for display, defaulting to 0. */
function num(v) {
  return Number.isFinite(v) ? v : 0;
}

// --------------------------------------------------------------- view renderers
// Each renderer is null-safe: it defends against a missing/!array payload and
// returns its own empty state, so a partial backend response never throws.

/** Temporal: rows of `{kind, description, window, metric, count}` (kind chipped). */
function TemporalView({ data }) {
  const rows = Array.isArray(data) ? data : [];
  if (rows.length === 0) {
    return <div className="empty-inline">No temporal patterns found.</div>;
  }
  return (
    <div className="ptable" role="table" aria-label="Temporal patterns">
      {rows.map((p, i) => (
        <div className="prow prow--temporal" role="row" key={p?.pattern_id ?? i}>
          <span className="pcell pcell--kind">
            <span className="chip">{p?.kind ?? "pattern"}</span>
          </span>
          <span className="pcell pcell--desc" title={p?.description || ""}>
            {p?.description || "—"}
          </span>
          <span className="pcell pcell--window">{p?.window || "—"}</span>
          <span className="pcell pcell--num" title="severity metric">
            {fmt(num(p?.metric), 2)}×
          </span>
          <span className="pcell pcell--num" title="supporting log count">
            {num(p?.count)}
          </span>
        </div>
      ))}
    </div>
  );
}

/** Performance: latency bands table + top bottleneck signatures. */
function PerformanceView({ data }) {
  const bands = Array.isArray(data?.bands) ? data.bands : [];
  const signatures = Array.isArray(data?.signatures) ? data.signatures : [];
  if (bands.length === 0 && signatures.length === 0) {
    return <div className="empty-inline">No performance patterns found.</div>;
  }
  return (
    <div className="pstack">
      <div className="psub">
        <div className="psub__title">
          Latency bands
          {Number.isFinite(data?.total_with_latency) ? (
            <span className="psub__meta">
              {data.total_with_latency} logs with latency
            </span>
          ) : null}
        </div>
        {bands.length === 0 ? (
          <div className="empty-inline">No latency samples.</div>
        ) : (
          <div className="ptable" role="table" aria-label="Latency bands">
            <div className="prow prow--perf prow--head" role="row">
              <span className="pcell">band</span>
              <span className="pcell pcell--num">count</span>
              <span className="pcell pcell--num">mean ms</span>
              <span className="pcell pcell--num">p95 ms</span>
            </div>
            {bands.map((b, i) => (
              <div className="prow prow--perf" role="row" key={b?.band ?? i}>
                <span className="pcell">
                  <span className="chip">{b?.band ?? "band"}</span>
                </span>
                <span className="pcell pcell--num">{num(b?.count)}</span>
                <span className="pcell pcell--num">{fmt(num(b?.mean_ms))}</span>
                <span className="pcell pcell--num">{fmt(num(b?.p95_ms))}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {signatures.length > 0 ? (
        <div className="psub">
          <div className="psub__title">Bottleneck signatures</div>
          <div className="ptable" role="table" aria-label="Bottleneck signatures">
            <div className="prow prow--sig prow--head" role="row">
              <span className="pcell">service / endpoint</span>
              <span className="pcell pcell--num">p95 ms</span>
              <span className="pcell pcell--num">count</span>
            </div>
            {signatures.map((s, i) => {
              const target = s?.endpoint
                ? `${s?.service ?? "—"} ${s.endpoint}`
                : s?.service ?? "—";
              return (
                <div className="prow prow--sig" role="row" key={i}>
                  <span className="pcell pcell--target" title={target}>
                    {target}
                  </span>
                  <span className="pcell pcell--num">{fmt(num(s?.p95_ms))}</span>
                  <span className="pcell pcell--num">{num(s?.count)}</span>
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}

/** Behavioral: cohort rows (label chip, counts, error rate, example entities). */
function BehavioralView({ data }) {
  const groups = Array.isArray(data?.groups) ? data.groups : [];
  if (groups.length === 0) {
    return <div className="empty-inline">No behavior cohorts found.</div>;
  }
  return (
    <div className="pstack">
      {Number.isFinite(data?.entities) ? (
        <div className="psub__meta psub__meta--lead">
          {data.entities} entities profiled
        </div>
      ) : null}
      <div className="ptable" role="table" aria-label="Behavior cohorts">
        <div className="prow prow--behav prow--head" role="row">
          <span className="pcell">cohort</span>
          <span className="pcell pcell--num">entities</span>
          <span className="pcell pcell--num">avg req</span>
          <span className="pcell pcell--num">err rate</span>
          <span className="pcell">examples</span>
        </div>
        {groups.map((g, i) => {
          const examples = Array.isArray(g?.example_entities)
            ? g.example_entities
            : [];
          const suspect =
            g?.label === "security-suspect" || g?.label === "error-heavy";
          return (
            <div className="prow prow--behav" role="row" key={g?.group ?? i}>
              <span className="pcell">
                <span className={`chip ${suspect ? "chip--bad" : ""}`}>
                  {g?.label ?? "group"}
                </span>
              </span>
              <span className="pcell pcell--num">{num(g?.count)}</span>
              <span className="pcell pcell--num">{fmt(num(g?.mean_requests))}</span>
              <span className="pcell pcell--num">
                {(num(g?.mean_error_rate) * 100).toFixed(0)}%
              </span>
              <span className="pcell pcell--examples" title={examples.join(", ")}>
                {examples.length ? examples.join(", ") : "—"}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/** Sequence: summary line + flagged-anomaly rows (entity, score, sample events). */
function SequenceView({ data }) {
  const anomalies = Array.isArray(data?.anomalies) ? data.anomalies : [];
  const summary = [
    Number.isFinite(data?.analyzed) ? `${data.analyzed} entities` : null,
    Number.isFinite(data?.window) ? `window n=${data.window}` : null,
    Number.isFinite(data?.model_ngrams) ? `${data.model_ngrams} n-grams` : null,
  ].filter(Boolean);

  return (
    <div className="pstack">
      {summary.length ? (
        <div className="psub__meta psub__meta--lead">{summary.join(" · ")}</div>
      ) : null}
      {anomalies.length === 0 ? (
        <div className="empty-inline">No anomalous sequences detected.</div>
      ) : (
        <div className="ptable" role="table" aria-label="Sequence anomalies">
          <div className="prow prow--seq prow--head" role="row">
            <span className="pcell">entity</span>
            <span className="pcell pcell--num">score</span>
            <span className="pcell">sample events</span>
          </div>
          {anomalies.map((a, i) => {
            const events = Array.isArray(a?.sample_events) ? a.sample_events : [];
            return (
              <div className="prow prow--seq" role="row" key={a?.entity ?? i}>
                <span className="pcell pcell--target" title={a?.entity || ""}>
                  {a?.entity || "—"}
                </span>
                <span className="pcell pcell--num pcell--score">
                  {fmt(num(a?.score), 2)}
                </span>
                <span className="pcell pcell--events">
                  {events.length
                    ? events.map((ev, j) => (
                        <span className="chip chip--ev" key={j}>
                          {ev}
                        </span>
                      ))
                    : "—"}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/** Dispatch to the right view renderer for the active tab. */
function renderView(tabKey, data) {
  switch (tabKey) {
    case "temporal":
      return <TemporalView data={data} />;
    case "performance":
      return <PerformanceView data={data} />;
    case "behavioral":
      return <BehavioralView data={data} />;
    case "sequence":
      return <SequenceView data={data} />;
    default:
      return null;
  }
}

/**
 * Discovered Patterns panel (C19).
 *
 * Surfaces the four batch-mining views — Temporal, Performance, Behavioral,
 * Sequence — behind a tab bar, each backed by its `/patterns/*` endpoint. Results
 * are cached per tab (in a ref) so re-clicking a previously-loaded tab paints
 * instantly without a refetch; a Refresh button forces a re-fetch of the active
 * tab. Overlapping / stale fetches are guarded by a monotonic request id so an
 * out-of-order response never overwrites fresher data, and unmount is guarded so
 * no state update fires after teardown.
 *
 * Static panel (mined from the warm-up corpus), so it does not subscribe to the
 * WS snapshot — it fetches once per tab on demand.
 */
export default function PatternPanels() {
  const [active, setActive] = useState("temporal");
  // Per-tab result cache: { [tabKey]: payload }. Lives in a ref so populating it
  // doesn't itself trigger a render loop; we mirror "is this tab cached" into
  // state via `cachedTick` only to force a repaint when a fetch resolves.
  const cacheRef = useRef({});
  const [, setCachedTick] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const reqIdRef = useRef(0);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // Fetch one tab's data. `force` bypasses the per-tab cache (Refresh button).
  const load = useCallback((tabKey, force) => {
    if (!force && cacheRef.current[tabKey] !== undefined) {
      // Already cached — nothing to do; clear any stale error from another tab.
      setError(null);
      return;
    }
    const tab = TABS.find((t) => t.key === tabKey);
    if (!tab) return;

    const reqId = ++reqIdRef.current;
    setLoading(true);
    setError(null);

    tab
      .fetch()
      .then((payload) => {
        if (!mountedRef.current || reqId !== reqIdRef.current) return;
        cacheRef.current[tabKey] = payload;
        setLoading(false);
        setCachedTick((t) => t + 1);
      })
      .catch((err) => {
        if (!mountedRef.current || reqId !== reqIdRef.current) return;
        setError(err?.message || "Failed to load patterns");
        setLoading(false);
      });
  }, []);

  // Load the active tab on mount and whenever it changes (cache hit = no-op).
  useEffect(() => {
    load(active, false);
  }, [active, load]);

  const cached = cacheRef.current[active];
  const hasData = cached !== undefined;

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Discovered Patterns</h3>
        <div className="panel__head-actions">
          <div className="tab-bar" role="tablist" aria-label="Pattern view">
            {TABS.map((t) => (
              <button
                key={t.key}
                type="button"
                role="tab"
                aria-selected={active === t.key}
                className={`tab ${active === t.key ? "tab--active" : ""}`}
                onClick={() => setActive(t.key)}
              >
                {t.label}
              </button>
            ))}
          </div>
          <button
            type="button"
            className="btn-refresh"
            onClick={() => load(active, true)}
            disabled={loading}
            title="Re-fetch the active view"
          >
            {loading ? "…" : "Refresh"}
          </button>
        </div>
      </div>

      {error ? <div className="panel__error">{error}</div> : null}

      <div className="pattern-body">
        {hasData ? (
          renderView(active, cached)
        ) : (
          <div className="empty-inline">
            {loading ? "Loading patterns…" : "No data."}
          </div>
        )}
      </div>
    </section>
  );
}

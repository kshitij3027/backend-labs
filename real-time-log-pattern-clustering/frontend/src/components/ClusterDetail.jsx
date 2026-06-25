import { useEffect, useMemo, useRef, useState } from "react";
import { getClusters, getClusterDetail } from "../api.js";

// Algorithms the backend clusters with. Order = tab order. Matches the scatter.
const ALGORITHMS = ["kmeans", "dbscan", "hdbscan"];

// Sentinel cluster id for noise / too-new-to-belong points.
const NOISE_ID = -1;

/** Human label for a cluster id (noise sentinel gets a friendly name). */
function labelForCluster(clusterId) {
  return clusterId === NOISE_ID ? "noise/new" : `Cluster ${clusterId}`;
}

/** Coerce an unknown value to a finite number, or null. */
function asNumber(v) {
  return Number.isFinite(v) ? v : null;
}

/** Format a possibly-fractional stat to a short, tabular-friendly string. */
function fmtNum(v) {
  const n = asNumber(v);
  if (n === null) return "—";
  // Whole numbers stay whole; otherwise 2dp.
  return Number.isInteger(n) ? String(n) : n.toFixed(2);
}

/**
 * Cluster Drill-Down (C17).
 *
 * Pick an algorithm → browse its clusters in a scrollable left list → select one
 * → inspect its size, pattern type, representative line and example log lines on
 * the right. Refetches the cluster list whenever the active algorithm changes or
 * the stream's `total_processed` counter advances (passed via `snapshot`), and
 * fetches richer per-cluster detail (member count, confidence stats) lazily when
 * a cluster is selected. Overlapping / stale fetches are guarded by monotonic
 * request ids so out-of-order responses never clobber fresher data. Null-safe
 * throughout: renders loading / empty states before any data arrives.
 *
 * @param {{ snapshot: (object|null) }} props the shared WS snapshot; its
 *   `stats.total_processed` is used purely as a "new data arrived" trigger.
 */
export default function ClusterDetail({ snapshot }) {
  const [algorithm, setAlgorithm] = useState("kmeans");
  const [clusters, setClusters] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Monotonic request counters: only the newest in-flight fetch of each kind is
  // allowed to commit, so stale responses are dropped.
  const listReqRef = useRef(0);
  const detailReqRef = useRef(0);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const processed = snapshot?.stats?.total_processed ?? 0;

  // Fetch the cluster list on algorithm change / when processed advances. The
  // largest cluster is auto-selected once the list lands (only if nothing is
  // selected yet, so a live refresh doesn't yank the user's current selection).
  useEffect(() => {
    const reqId = ++listReqRef.current;
    setLoading(true);

    getClusters(algorithm)
      .then((data) => {
        if (!mountedRef.current || reqId !== listReqRef.current) {
          return;
        }
        const list = Array.isArray(data) ? data : [];
        setClusters(list);
        setError(null);
        setLoading(false);

        // Default-select the largest cluster the first time a list loads (or
        // after an algorithm switch reset selection to null).
        setSelectedId((prev) => {
          if (prev !== null && list.some((c) => c?.cluster_id === prev)) {
            return prev; // keep a still-valid selection across refreshes
          }
          if (list.length === 0) return null;
          let best = list[0];
          for (const c of list) {
            if ((c?.size ?? -Infinity) > (best?.size ?? -Infinity)) best = c;
          }
          return best?.cluster_id ?? null;
        });
      })
      .catch((err) => {
        if (!mountedRef.current || reqId !== listReqRef.current) {
          return;
        }
        setError(err?.message || "Failed to load clusters");
        setLoading(false);
      });
    // Reset selection on algorithm switch so we re-pick the largest there.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [algorithm, processed]);

  // When the algorithm changes, drop any selection/detail so the right pane
  // re-defaults to the new algorithm's largest cluster.
  useEffect(() => {
    setSelectedId(null);
    setDetail(null);
  }, [algorithm]);

  // Fetch richer detail for the selected cluster (best-effort: if the endpoint
  // is unavailable we silently fall back to the row data from the list).
  useEffect(() => {
    if (selectedId === null) {
      setDetail(null);
      return;
    }
    const reqId = ++detailReqRef.current;
    getClusterDetail(algorithm, selectedId)
      .then((data) => {
        if (!mountedRef.current || reqId !== detailReqRef.current) {
          return;
        }
        setDetail(data && typeof data === "object" ? data : null);
      })
      .catch(() => {
        if (!mountedRef.current || reqId !== detailReqRef.current) {
          return;
        }
        setDetail(null); // fall back to list row data
      });
  }, [algorithm, selectedId, processed]);

  // The currently-selected row from the list (always available once selected).
  const selectedRow = useMemo(
    () => clusters.find((c) => c?.cluster_id === selectedId) || null,
    [clusters, selectedId],
  );

  // Merge: prefer the richer detail payload, fall back to the list row per-field.
  const view = useMemo(() => {
    if (!selectedRow && !detail) return null;
    const d = detail || {};
    const r = selectedRow || {};
    const examples =
      (Array.isArray(d.examples) && d.examples.length ? d.examples : null) ||
      (Array.isArray(d.example_lines) && d.example_lines.length
        ? d.example_lines
        : null) ||
      (Array.isArray(r.examples) ? r.examples : []);
    return {
      cluster_id: r.cluster_id ?? d.cluster_id ?? selectedId,
      size: asNumber(d.members) ?? asNumber(d.size) ?? asNumber(r.size),
      pattern_type: d.pattern_type ?? r.pattern_type ?? null,
      representative: d.representative ?? r.representative ?? null,
      examples,
      // Optional confidence stats (only present on the detail payload).
      confidence_mean: asNumber(d.confidence_mean) ?? asNumber(d.mean_confidence),
      confidence_min: asNumber(d.confidence_min) ?? asNumber(d.min_confidence),
      confidence_max: asNumber(d.confidence_max) ?? asNumber(d.max_confidence),
    };
  }, [detail, selectedRow, selectedId]);

  const hasClusters = clusters.length > 0;

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Cluster Drill-Down</h3>
        <div className="tab-bar" role="tablist" aria-label="Clustering algorithm">
          {ALGORITHMS.map((algo) => (
            <button
              key={algo}
              type="button"
              role="tab"
              aria-selected={algorithm === algo}
              className={`tab ${algorithm === algo ? "tab--active" : ""}`}
              onClick={() => setAlgorithm(algo)}
            >
              {algo}
            </button>
          ))}
        </div>
      </div>

      {error ? <div className="panel__error">{error}</div> : null}

      <div className="drill">
        {/* LEFT: scrollable cluster list. */}
        <div className="cluster-list" role="listbox" aria-label="Clusters">
          {hasClusters ? (
            clusters.map((c) => {
              const id = c?.cluster_id;
              const active = id === selectedId;
              return (
                <button
                  key={String(id)}
                  type="button"
                  role="option"
                  aria-selected={active}
                  className={`cluster-row ${active ? "cluster-row--active" : ""}`}
                  onClick={() => setSelectedId(id)}
                >
                  <span className="cluster-row__name">{labelForCluster(id)}</span>
                  {c?.pattern_type ? (
                    <span className="chip cluster-row__type">{c.pattern_type}</span>
                  ) : null}
                  <span className="cluster-row__size">{fmtNum(c?.size)}</span>
                </button>
              );
            })
          ) : (
            <div className="drill-empty">
              {loading ? "Loading clusters…" : "No clusters yet"}
            </div>
          )}
        </div>

        {/* RIGHT: detail for the selected cluster. */}
        <div className="cluster-detail">
          {view ? (
            <>
              <div className="cluster-detail__head">
                <span className="cluster-detail__title">
                  {labelForCluster(view.cluster_id)}
                </span>
                {view.pattern_type ? (
                  <span className="chip">{view.pattern_type}</span>
                ) : null}
              </div>

              <div className="cluster-detail__stats">
                <div className="kv">
                  <span className="kv__label">Size</span>
                  <span className="kv__value">{fmtNum(view.size)}</span>
                </div>
                {view.confidence_mean !== null ? (
                  <div className="kv">
                    <span className="kv__label">Mean confidence</span>
                    <span className="kv__value">
                      {(view.confidence_mean * 100).toFixed(0)}%
                    </span>
                  </div>
                ) : null}
                {view.confidence_min !== null && view.confidence_max !== null ? (
                  <div className="kv">
                    <span className="kv__label">Confidence range</span>
                    <span className="kv__value">
                      {(view.confidence_min * 100).toFixed(0)}–
                      {(view.confidence_max * 100).toFixed(0)}%
                    </span>
                  </div>
                ) : null}
              </div>

              <div className="cluster-detail__rep">
                <span className="cluster-detail__rep-label">Representative</span>
                <code className="cluster-detail__rep-line">
                  {view.representative || "—"}
                </code>
              </div>

              <div className="cluster-detail__examples">
                <span className="cluster-detail__rep-label">
                  Example log lines
                </span>
                {view.examples && view.examples.length ? (
                  <ul className="example-list">
                    {view.examples.map((ex, i) => (
                      <li key={i} className="example-line">
                        <code>{typeof ex === "string" ? ex : String(ex)}</code>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <div className="muted">No example lines.</div>
                )}
              </div>
            </>
          ) : (
            <div className="drill-empty drill-empty--detail">
              {hasClusters ? "Select a cluster" : "Loading clusters…"}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

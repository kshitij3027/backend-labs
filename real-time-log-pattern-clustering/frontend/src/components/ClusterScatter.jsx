import { useEffect, useMemo, useRef, useState } from "react";
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  Tooltip,
  Legend,
} from "chart.js";
import { Scatter } from "react-chartjs-2";
import { getScatter } from "../api.js";

// react-chartjs-2 v5 does not auto-register Chart.js pieces, so register the
// exact set this scatter needs (numeric axes + point markers + tooltip/legend)
// once at module load. Doing it here keeps the component self-contained.
ChartJS.register(LinearScale, PointElement, Tooltip, Legend);

// Algorithms the backend can colour the scatter by. Order = tab order.
const ALGORITHMS = ["kmeans", "dbscan", "hdbscan"];

// Sentinel cluster id the backend uses for points that are noise / too new to
// belong to a cluster yet. Rendered muted + labelled "noise/new".
const NOISE_ID = -1;
const NOISE_COLOR = "rgba(120, 130, 160, 0.45)";

// Deterministic palette keyed by cluster id. A fixed list of vivid hues keeps
// the same cluster the same colour across refetches (clusters are stable ids),
// and we wrap with modulo so an unbounded number of clusters still gets a
// colour. Kept in sync with the dark theme's accent family.
const PALETTE = [
  "#6ea8ff", // blue
  "#34d399", // emerald
  "#fbbf24", // amber
  "#fb7185", // rose
  "#818cf8", // indigo
  "#38bdf8", // sky
  "#f472b6", // pink
  "#a3e635", // lime
  "#fb923c", // orange
  "#2dd4bf", // teal
  "#c084fc", // purple
  "#facc15", // yellow
];

/** Colour for a cluster id: muted grey for noise, else a stable palette pick. */
function colorForCluster(clusterId) {
  if (clusterId === NOISE_ID) {
    return NOISE_COLOR;
  }
  // Non-negative modulo so even unexpected ids map into the palette.
  const idx = ((clusterId % PALETTE.length) + PALETTE.length) % PALETTE.length;
  return PALETTE[idx];
}

/** Human label for a cluster id (noise sentinel gets a friendly name). */
function labelForCluster(clusterId) {
  return clusterId === NOISE_ID ? "noise/new" : `Cluster ${clusterId}`;
}

/**
 * Group raw `{x, y, cluster_id}` points into one Chart.js dataset per cluster.
 * Clusters are ordered numerically with the noise group (-1) pushed last so it
 * sits at the end of the legend.
 */
function buildDatasets(points) {
  const groups = new Map();
  for (const p of points) {
    if (!p || typeof p.x !== "number" || typeof p.y !== "number") {
      continue;
    }
    const id = Number.isFinite(p.cluster_id) ? p.cluster_id : NOISE_ID;
    let bucket = groups.get(id);
    if (!bucket) {
      bucket = [];
      groups.set(id, bucket);
    }
    bucket.push({ x: p.x, y: p.y });
  }

  const ids = [...groups.keys()].sort((a, b) => {
    // Noise always last; otherwise ascending by id.
    if (a === NOISE_ID) return 1;
    if (b === NOISE_ID) return -1;
    return a - b;
  });

  return ids.map((id) => {
    const color = colorForCluster(id);
    return {
      label: labelForCluster(id),
      data: groups.get(id),
      backgroundColor: color,
      borderColor: color,
      pointRadius: 3,
      pointHoverRadius: 5,
    };
  });
}

// Dark-theme chart colours (explicit so the canvas is legible on the slate bg).
const GRID_COLOR = "rgba(36, 48, 86, 0.55)"; // --border, softened
const TICK_COLOR = "#6b7799"; // --text-faint
const LEGEND_COLOR = "#9aa6cc"; // --text-dim
const TOOLTIP_BG = "#141c38"; // --card

/**
 * Live cluster scatter (C15).
 *
 * Renders the PCA-2D projection of recent log feature vectors as a Chart.js
 * scatter, one colour per cluster, with a tab bar to switch between the three
 * clustering algorithms. Refetches whenever the active algorithm changes or the
 * stream's `total_processed` counter advances, so the plot tracks ingestion
 * live. Overlapping / stale fetches are guarded by a monotonic request id.
 *
 * @param {{ snapshot: (object|null) }} props the shared WS snapshot; its
 *   `stats.total_processed` is used purely as a "new data arrived" trigger.
 */
export default function ClusterScatter({ snapshot }) {
  const [algorithm, setAlgorithm] = useState("kmeans");
  const [points, setPoints] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Monotonic request counter: only the newest in-flight fetch is allowed to
  // commit its result, so out-of-order responses never overwrite fresher data.
  const reqIdRef = useRef(0);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // Re-fetch on algorithm change and whenever the processed counter advances.
  // Using the counter (not a timer) means the scatter only re-queries when the
  // backend has actually ingested more logs.
  const processed = snapshot?.stats?.total_processed ?? 0;

  useEffect(() => {
    const reqId = ++reqIdRef.current;
    setLoading(true);

    getScatter(algorithm, 500)
      .then((data) => {
        // Ignore if unmounted or a newer request has since been issued.
        if (!mountedRef.current || reqId !== reqIdRef.current) {
          return;
        }
        setPoints(Array.isArray(data) ? data : []);
        setError(null);
        setLoading(false);
      })
      .catch((err) => {
        if (!mountedRef.current || reqId !== reqIdRef.current) {
          return;
        }
        setError(err?.message || "Failed to load scatter");
        setLoading(false);
      });
  }, [algorithm, processed]);

  const data = useMemo(
    () => ({ datasets: buildDatasets(points) }),
    [points],
  );

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      // The axes are an arbitrary 2-D projection — hide the numeric clutter but
      // keep a faint grid for spatial reference.
      scales: {
        x: {
          grid: { color: GRID_COLOR },
          ticks: { color: TICK_COLOR, display: false },
          border: { color: GRID_COLOR },
        },
        y: {
          grid: { color: GRID_COLOR },
          ticks: { color: TICK_COLOR, display: false },
          border: { color: GRID_COLOR },
        },
      },
      plugins: {
        legend: {
          position: "right",
          labels: {
            color: LEGEND_COLOR,
            boxWidth: 10,
            boxHeight: 10,
            usePointStyle: true,
            pointStyle: "circle",
            padding: 12,
            font: { size: 11 },
          },
        },
        tooltip: {
          backgroundColor: TOOLTIP_BG,
          borderColor: GRID_COLOR,
          borderWidth: 1,
          titleColor: "#eef2ff",
          bodyColor: LEGEND_COLOR,
          callbacks: {
            // Show which cluster the hovered point belongs to.
            label: (ctx) => ` ${ctx.dataset.label}`,
          },
        },
      },
    }),
    [],
  );

  const hasPoints = points.length > 0;

  return (
    <section className="panel chart-card">
      <div className="panel__head">
        <h3 className="section__title panel__title">
          Cluster Map — 2D projection
        </h3>
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

      <div className="chart-area">
        {hasPoints ? (
          <Scatter data={data} options={options} />
        ) : (
          <div className="empty-state">
            {loading
              ? "Loading cluster points…"
              : "Waiting for cluster points…"}
          </div>
        )}
      </div>
    </section>
  );
}

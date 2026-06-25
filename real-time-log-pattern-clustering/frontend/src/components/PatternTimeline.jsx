import { useEffect, useMemo, useRef, useState } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import { Line } from "react-chartjs-2";

// react-chartjs-2 v5 does not auto-register Chart.js pieces, so register the
// exact set this line chart needs (category x-axis + linear y-axis + line/point
// elements + tooltip/legend + area fill) once at module load.
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler,
);

// Cap on how many snapshots we retain in the rolling timeline. At ~1.5s/frame
// this is ~90s of history — enough to read a trend without unbounded growth.
const MAX_POINTS = 60;

// Line theme colours, kept in sync with the dark theme accents so the three
// series read as the same metrics shown elsewhere on the dashboard.
const SERIES = [
  { key: "patterns", label: "Patterns Discovered", color: "#34d399" }, // emerald
  { key: "anomalies", label: "Anomalies", color: "#fb7185" }, // rose
  { key: "clusters", label: "Total Clusters", color: "#818cf8" }, // indigo
];

// Dark-theme chart colours (explicit so the canvas is legible on the slate bg).
const GRID_COLOR = "rgba(36, 48, 86, 0.55)"; // --border, softened
const TICK_COLOR = "#6b7799"; // --text-faint
const LEGEND_COLOR = "#9aa6cc"; // --text-dim
const TOOLTIP_BG = "#141c38"; // --card

/** Coerce a possibly-missing numeric stat to a finite number (default 0). */
function num(v) {
  return Number.isFinite(v) ? v : 0;
}

/**
 * Pattern Evolution timeline (C16).
 *
 * A Chart.js line chart of the three headline counts — patterns discovered,
 * anomalies detected and total clusters — accumulated CLIENT-SIDE from the live
 * WS snapshot stream. This is a true live TIME-series: the broadcaster pushes a
 * frame every ~1.5s and each distinct frame appends one point (capped at 60), so
 * the chart shows a rolling line that is flat while the backend is idle and
 * steps up as logs are ingested. Crucially it advances on TIME, not on data
 * change — flat metric values still produce points, so an idle system shows a
 * live flat line rather than sitting on a perpetual empty state.
 *
 * History lives in a ref (so it survives re-renders without re-triggering the
 * effect); a tick counter forces the re-render after each append. The only
 * guard is against re-processing the exact same `snapshot` object reference
 * (e.g. an unrelated re-render), which would otherwise double-append a frame.
 *
 * @param {{ snapshot: (object|null) }} props the shared WS snapshot; reads
 *   `snapshot.stats.{patterns_discovered, anomalies_detected, total_clusters}`.
 */
export default function PatternTimeline({ snapshot }) {
  // Rolling history of metric points. Lives in a ref, not state, so appends are
  // cheap and don't depend on the previous render closing over stale data.
  const historyRef = useRef([]);
  // The last snapshot reference we recorded, so an unrelated re-render that
  // re-runs the effect with the SAME frame can't double-append. Two DISTINCT
  // frames with identical metrics still yield two points (live time axis).
  const lastSnapshotRef = useRef(null);
  // Bumped on every accepted append purely to trigger a re-render.
  const [, setTick] = useState(0);

  useEffect(() => {
    // Advance on TIME: append one point per distinct frame. Only guard against
    // a null/absent frame or the exact same object reference seen twice.
    if (!snapshot || snapshot === lastSnapshotRef.current) {
      return;
    }
    lastSnapshotRef.current = snapshot;

    const stats = snapshot.stats || {};
    historyRef.current.push({
      patterns: num(stats.patterns_discovered),
      anomalies: num(stats.anomalies_detected),
      clusters: num(stats.total_clusters),
    });
    // Trim oldest points beyond the cap.
    if (historyRef.current.length > MAX_POINTS) {
      historyRef.current.splice(0, historyRef.current.length - MAX_POINTS);
    }
    setTick((t) => t + 1);
  }, [snapshot]);

  const history = historyRef.current;
  const pointCount = history.length;

  const data = useMemo(() => {
    // Relative time labels: oldest = t-(n-1), newest = t-0.
    const labels = history.map((_, i) => `t-${pointCount - 1 - i}`);
    return {
      labels,
      datasets: SERIES.map((s) => ({
        label: s.label,
        data: history.map((pt) => pt[s.key]),
        borderColor: s.color,
        backgroundColor: `${s.color}22`, // light translucent fill
        fill: true,
        tension: 0.3,
        borderWidth: 2,
        pointRadius: 2,
        pointHoverRadius: 4,
        pointBackgroundColor: s.color,
      })),
    };
    // pointCount changes whenever history does (via the tick re-render).
  }, [pointCount]);

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: {
          grid: { color: GRID_COLOR },
          ticks: {
            color: TICK_COLOR,
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8,
          },
          border: { color: GRID_COLOR },
        },
        y: {
          beginAtZero: true,
          grid: { color: GRID_COLOR },
          ticks: { color: TICK_COLOR, precision: 0 },
          border: { color: GRID_COLOR },
        },
      },
      plugins: {
        legend: {
          position: "top",
          align: "end",
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
        },
      },
    }),
    [],
  );

  // Need at least two points before a line is meaningful.
  const hasTrend = pointCount >= 2;

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Pattern Evolution</h3>
      </div>

      <div className="chart-area chart-area--timeline">
        {hasTrend ? (
          <Line data={data} options={options} />
        ) : (
          <div className="empty-state">Collecting timeline…</div>
        )}
      </div>
    </section>
  );
}

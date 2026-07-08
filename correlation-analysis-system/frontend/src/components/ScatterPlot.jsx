import { useMemo, useState } from "react";
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { CORRELATION_TYPES, cssVar, fmt, relativeTime, typeLabel } from "../util.js";

const CHART_H = 240;

// Themed point tooltip. `colors` is threaded through by Recharts (it clones the
// element and injects active/payload while preserving our extra props).
function ScatterTip({ active, payload, colors = {} }) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;
  const p = payload[0]?.payload ?? {};
  return (
    <div className="charttip">
      <div className="charttip__title">
        <span
          className="charttip__swatch"
          style={{ background: colors[p.type] || "var(--type-unknown)" }}
          aria-hidden="true"
        />
        {typeLabel(p.type)}
      </div>
      <div className="charttip__row">
        <span className="charttip__key">Strength</span>
        <span className="charttip__val">{fmt(p.strength, 2)}</span>
      </div>
      <div className="charttip__row">
        <span className="charttip__key">Confidence</span>
        <span className="charttip__val">{fmt(p.confidence, 2)}</span>
      </div>
      <div className="charttip__row">
        <span className="charttip__key">Detected</span>
        <span className="charttip__val">{relativeTime(Number(p.detected_at) * 1000)}</span>
      </div>
    </div>
  );
}

// Strength × confidence distribution of recent correlations, one colour per type.
// Each type is its own <Scatter> series so the legend can toggle it independently
// (hidden types are tracked in local state and the series is `hide`-flagged).
//
// Props:
//   scatter — dashboard.scatter, or [] while loading / degraded
export default function ScatterPlot({ scatter = [] }) {
  const data = Array.isArray(scatter) ? scatter : [];
  const [hidden, setHidden] = useState(() => new Set());

  const colors = useMemo(
    () => ({
      temporal: cssVar("--type-temporal", "#6ba3ff"),
      session_based: cssVar("--type-session", "#4ecb8d"),
      user_based: cssVar("--type-user", "#c58af9"),
      error_cascade: cssVar("--type-cascade", "#ff6b6b"),
      metric_based: cssVar("--type-metric", "#ffb454"),
    }),
    [],
  );
  const axis = useMemo(() => cssVar("--text-faint", "#6b7785"), []);
  const grid = useMemo(() => cssVar("--border-soft", "#232b36"), []);

  // Bucket points into per-type series (skipping unknown types defensively).
  const series = useMemo(() => {
    const groups = {};
    for (const t of CORRELATION_TYPES) groups[t] = [];
    for (const pt of data) {
      const t = pt?.type;
      if (CORRELATION_TYPES.includes(t)) groups[t].push(pt);
    }
    return groups;
  }, [data]);

  const toggle = (t) =>
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return next;
    });

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Strength × confidence</h2>
        <span className="panel__count">{data.length}</span>
      </div>

      <ul className="chart-legend" role="group" aria-label="Toggle correlation types">
        {CORRELATION_TYPES.map((t) => {
          const off = hidden.has(t);
          return (
            <li key={t}>
              <button
                type="button"
                className={`chart-legend__item${off ? " chart-legend__item--off" : ""}`}
                onClick={() => toggle(t)}
                aria-pressed={!off}
              >
                <span
                  className="chart-legend__dot"
                  style={{ background: colors[t] }}
                  aria-hidden="true"
                />
                {typeLabel(t)}
                <span className="chart-legend__n">{series[t].length}</span>
              </button>
            </li>
          );
        })}
      </ul>

      {data.length === 0 ? (
        <div className="chart-empty" style={{ height: CHART_H }}>
          Waiting for data…
        </div>
      ) : (
        <div className="chart" style={{ height: CHART_H }}>
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 8, right: 12, bottom: 16, left: -6 }}>
              <CartesianGrid stroke={grid} />
              <XAxis
                type="number"
                dataKey="strength"
                name="Strength"
                domain={[0, 1]}
                ticks={[0, 0.25, 0.5, 0.75, 1]}
                stroke={axis}
                tick={{ fontSize: 10, fill: axis }}
                tickLine={false}
                label={{
                  value: "Strength",
                  position: "insideBottom",
                  offset: -6,
                  fontSize: 11,
                  fill: axis,
                }}
              />
              <YAxis
                type="number"
                dataKey="confidence"
                name="Confidence"
                domain={[0, 1]}
                ticks={[0, 0.25, 0.5, 0.75, 1]}
                stroke={axis}
                tick={{ fontSize: 10, fill: axis }}
                tickLine={false}
                width={40}
                label={{
                  value: "Confidence",
                  angle: -90,
                  position: "insideLeft",
                  offset: 16,
                  fontSize: 11,
                  fill: axis,
                }}
              />
              <ZAxis range={[38, 38]} />
              <Tooltip
                content={<ScatterTip colors={colors} />}
                cursor={{ strokeDasharray: "3 3", stroke: axis }}
              />
              {CORRELATION_TYPES.map((t) => (
                <Scatter
                  key={t}
                  name={typeLabel(t)}
                  data={series[t]}
                  fill={colors[t]}
                  fillOpacity={0.8}
                  hide={hidden.has(t)}
                  isAnimationActive={false}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

import { useMemo } from "react";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { cssVar, fmt, hhmmss } from "../util.js";

// Chart plot height. The panel chrome sits above this; ResponsiveContainer needs a
// definite height on its parent, so it's fixed here (stays readable on mobile).
const CHART_H = 240;

// Themed tooltip. Recharts positions this HTML div for us; theming is via the
// .charttip CSS classes (CSS vars resolve fine in HTML, unlike SVG attributes).
function TimelineTip({ active, payload }) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;
  const p = payload[0]?.payload ?? {};
  return (
    <div className="charttip">
      <div className="charttip__title">{hhmmss(p.t)}</div>
      <div className="charttip__row">
        <span className="charttip__key">Correlations</span>
        <span className="charttip__val">{Number(p.count) || 0}</span>
      </div>
      <div className="charttip__row">
        <span className="charttip__key">Avg strength</span>
        <span className="charttip__val">{fmt(p.avg_strength, 2)}</span>
      </div>
    </div>
  );
}

// Detection throughput over time. Bars = correlations detected per 10s bucket
// (left axis); the line = mean strength of those detections (right axis, 0..1).
// Data arrives oldest-first already, so it's rendered as-is.
//
// Props:
//   timeline — dashboard.timeline, or [] while loading / degraded
export default function TimelineChart({ timeline = [] }) {
  const data = Array.isArray(timeline) ? timeline : [];

  // SVG presentation attributes don't resolve var(--x); resolve once at mount.
  const c = useMemo(
    () => ({
      bar: cssVar("--accent", "#6ba3ff"),
      line: cssVar("--ok", "#4ecb8d"),
      grid: cssVar("--border-soft", "#232b36"),
      axis: cssVar("--text-faint", "#6b7785"),
    }),
    [],
  );

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Detection timeline</h2>
        <span className="panel__count">{data.length}</span>
      </div>

      {data.length === 0 ? (
        <div className="chart-empty" style={{ height: CHART_H }}>
          Waiting for data…
        </div>
      ) : (
        <div className="chart" style={{ height: CHART_H }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 8, right: 6, bottom: 2, left: -12 }}>
              <CartesianGrid stroke={c.grid} vertical={false} />
              <XAxis
                dataKey="t"
                tickFormatter={hhmmss}
                stroke={c.axis}
                tick={{ fontSize: 10, fill: c.axis }}
                tickLine={false}
                minTickGap={26}
              />
              <YAxis
                yAxisId="left"
                allowDecimals={false}
                stroke={c.axis}
                tick={{ fontSize: 10, fill: c.axis }}
                tickLine={false}
                width={34}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                domain={[0, 1]}
                stroke={c.axis}
                tick={{ fontSize: 10, fill: c.axis }}
                tickLine={false}
                width={34}
              />
              <Tooltip
                content={<TimelineTip />}
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
              />
              <Legend wrapperStyle={{ fontSize: 11, paddingTop: 4 }} />
              <Bar
                yAxisId="left"
                dataKey="count"
                name="Correlations"
                fill={c.bar}
                radius={[3, 3, 0, 0]}
                maxBarSize={26}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="avg_strength"
                name="Avg strength"
                stroke={c.line}
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

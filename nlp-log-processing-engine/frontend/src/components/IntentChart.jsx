import { useMemo } from "react";
import {
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { intentClass, intentLabel, truncate } from "../util.js";

// Intent distribution as a vertical bar chart, tallest first. Each bar is coloured by its
// intent hue via the shared `--intent-*` CSS tokens (resolved by name through
// `intentClass`), so the chart matches the intent badges elsewhere. X labels are rotated so
// the longer names stay readable at any width. Empty until the first line is classified.
//
// Data source: `stats.intent_distribution` ({label: count}) from useStats().

const CHART_HEIGHT = 250;

// Dark-theme styling for Recharts' default tooltip (kept inline so no stylesheet is touched).
const TOOLTIP_CONTENT_STYLE = {
  background: "var(--card-2)",
  border: "1px solid var(--border)",
  borderRadius: "9px",
  boxShadow: "var(--shadow)",
  fontSize: "12px",
};
const TOOLTIP_LABEL_STYLE = { color: "var(--text-dim)", fontWeight: 700, marginBottom: 2 };
const TOOLTIP_ITEM_STYLE = { color: "var(--text)" };
const AXIS_TICK = { fill: "var(--text-dim)", fontSize: 11 };

/** Rotated, truncated X-axis tick so long intent names don't collide or overflow. */
function RotatedTick({ x, y, payload }) {
  return (
    <g transform={`translate(${x},${y})`}>
      <text
        dy={10}
        textAnchor="end"
        transform="rotate(-32)"
        fill="var(--text-dim)"
        fontSize={11}
      >
        {truncate(String(payload?.value ?? ""), 14)}
      </text>
    </g>
  );
}

export default function IntentChart({ stats }) {
  const dist = stats?.intent_distribution;

  // Recompute only when the distribution object identity changes (stable between snapshots).
  const data = useMemo(() => {
    const src = dist && typeof dist === "object" ? dist : {};
    return Object.entries(src)
      .map(([label, count]) => ({
        label,
        name: intentLabel(label),
        count: Number(count) || 0,
        hue: `var(--intent-${intentClass(label)})`,
      }))
      .filter((d) => d.count > 0)
      .sort((a, b) => b.count - a.count);
  }, [dist]);

  const total = data.reduce((s, d) => s + d.count, 0);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Intent distribution</h2>
        <span className="panel__count">{total.toLocaleString()}</span>
      </div>

      {data.length === 0 ? (
        <p className="muted">
          No intents classified yet. Analyze a log line to populate this chart.
        </p>
      ) : (
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <BarChart data={data} margin={{ top: 6, right: 8, left: -14, bottom: 26 }}>
            <CartesianGrid vertical={false} stroke="var(--border-soft)" />
            <XAxis
              dataKey="name"
              interval={0}
              height={54}
              tick={<RotatedTick />}
              tickLine={false}
              axisLine={{ stroke: "var(--border)" }}
            />
            <YAxis
              allowDecimals={false}
              width={34}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={{ stroke: "var(--border)" }}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              contentStyle={TOOLTIP_CONTENT_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              itemStyle={TOOLTIP_ITEM_STYLE}
              formatter={(value) => [value, "Count"]}
            />
            <Bar
              dataKey="count"
              radius={[4, 4, 0, 0]}
              maxBarSize={54}
              isAnimationActive={false}
            >
              {data.map((d) => (
                <Cell key={d.label} fill={d.hue} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}

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
import { entityClass, truncate } from "../util.js";

// Entity-type distribution as a vertical bar chart — the top ~10 labels by count. Bars are
// coloured by the entity-label hue (`--ent-*`, via `entityClass`), so SERVICE / HOST / IP /
// USER_ID / … match the inline highlights and chips in the ResultCard. Labels are shown
// verbatim (spaCy's own ORG/GPE/DATE/… fold to the "general" hue). Empty until data exists.
//
// Data source: `stats.entity_type_distribution` ({label: count}) from useStats().

const CHART_HEIGHT = 250;
const TOP_N = 10;

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

/** Rotated, truncated X-axis tick so entity labels (ERROR_CODE, USER_ID, …) stay readable. */
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
        {truncate(String(payload?.value ?? ""), 12)}
      </text>
    </g>
  );
}

export default function EntityTypeChart({ stats }) {
  const dist = stats?.entity_type_distribution;

  const data = useMemo(() => {
    const src = dist && typeof dist === "object" ? dist : {};
    return Object.entries(src)
      .map(([label, count]) => ({
        label,
        count: Number(count) || 0,
        hue: `var(--ent-${entityClass(label)})`,
      }))
      .filter((d) => d.count > 0)
      .sort((a, b) => b.count - a.count)
      .slice(0, TOP_N);
  }, [dist]);

  const total = data.reduce((s, d) => s + d.count, 0);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Entity types</h2>
        <span className="panel__count">{total.toLocaleString()}</span>
      </div>

      {data.length === 0 ? (
        <p className="muted">
          No entities recognised yet. Analyze a log line to populate this chart.
        </p>
      ) : (
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <BarChart data={data} margin={{ top: 6, right: 8, left: -14, bottom: 26 }}>
            <CartesianGrid vertical={false} stroke="var(--border-soft)" />
            <XAxis
              dataKey="label"
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
              maxBarSize={48}
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

import { useMemo } from "react";
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { SENTIMENT_LABELS, sentimentClass } from "../util.js";

// Sentiment / severity mix as a donut, using the four severity tokens:
//   positive = green · neutral = grey · negative = orange · critical = red.
// Slices are ordered by ascending alarm (SENTIMENT_LABELS order) so the legend always reads
// positive → critical regardless of counts. Empty until the first line is analysed.
//
// Data source: `stats.sentiment_distribution` ({label: count}) from useStats().

const CHART_HEIGHT = 250;

const TOOLTIP_CONTENT_STYLE = {
  background: "var(--card-2)",
  border: "1px solid var(--border)",
  borderRadius: "9px",
  boxShadow: "var(--shadow)",
  fontSize: "12px",
};
const TOOLTIP_LABEL_STYLE = { color: "var(--text-dim)", fontWeight: 700, marginBottom: 2 };
const TOOLTIP_ITEM_STYLE = { color: "var(--text)" };
const LEGEND_STYLE = { fontSize: "12px", color: "var(--text-dim)" };

/** Rank a sentiment label by severity for stable slice/legend ordering; unknowns sort last. */
const SEVERITY_RANK = new Map(SENTIMENT_LABELS.map((l, i) => [l, i]));

/** "critical" -> "Critical". */
function titleCase(s) {
  const str = String(s ?? "");
  return str ? str.charAt(0).toUpperCase() + str.slice(1) : str;
}

export default function SentimentChart({ stats }) {
  const dist = stats?.sentiment_distribution;

  const data = useMemo(() => {
    const src = dist && typeof dist === "object" ? dist : {};
    return Object.entries(src)
      .map(([label, count]) => ({
        label,
        name: titleCase(label),
        count: Number(count) || 0,
        hue: `var(--sent-${sentimentClass(label)})`,
      }))
      .filter((d) => d.count > 0)
      .sort(
        (a, b) => (SEVERITY_RANK.get(a.label) ?? 99) - (SEVERITY_RANK.get(b.label) ?? 99)
      );
  }, [dist]);

  const total = data.reduce((s, d) => s + d.count, 0);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Sentiment mix</h2>
        <span className="panel__count">{total.toLocaleString()}</span>
      </div>

      {data.length === 0 ? (
        <p className="muted">
          No sentiment scored yet. Analyze a log line to populate this chart.
        </p>
      ) : (
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <PieChart>
            <Pie
              data={data}
              dataKey="count"
              nameKey="name"
              innerRadius={58}
              outerRadius={92}
              paddingAngle={2}
              stroke="var(--card)"
              strokeWidth={2}
              isAnimationActive={false}
            >
              {data.map((d) => (
                <Cell key={d.label} fill={d.hue} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={TOOLTIP_CONTENT_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              itemStyle={TOOLTIP_ITEM_STYLE}
              formatter={(value, name) => [value, name]}
            />
            <Legend
              iconType="circle"
              wrapperStyle={LEGEND_STYLE}
              formatter={(value) => (
                <span style={{ color: "var(--text-dim)" }}>{value}</span>
              )}
            />
          </PieChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}

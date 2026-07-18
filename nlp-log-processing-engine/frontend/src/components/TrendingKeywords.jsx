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
import { truncate } from "../util.js";

// Trending keywords as a horizontal bar chart (highest count at the top). Bars share the
// accent hue, fading slightly down the ranking so the leaders read strongest. Keyword text
// on the Y axis is truncated to keep the plot area usable at narrow widths. Empty until the
// YAKE extractor has produced trending terms.
//
// Data source: `stats.trending_keywords` ([[keyword, count], ...], already ranked and capped
// server-side to trending_top_k) from useStats().

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

export default function TrendingKeywords({ stats }) {
  const trending = stats?.trending_keywords;

  const data = useMemo(() => {
    const src = Array.isArray(trending) ? trending : [];
    const rows = src
      .map((pair) => {
        // Each entry is a [keyword, count] tuple; tolerate anything malformed.
        if (!Array.isArray(pair)) return null;
        const kw = pair[0];
        const count = Number(pair[1]) || 0;
        if (typeof kw !== "string" || !kw || count <= 0) return null;
        return { kw, label: truncate(kw, 22), count };
      })
      .filter(Boolean)
      .sort((a, b) => b.count - a.count)
      .slice(0, TOP_N);
    return rows;
  }, [trending]);

  // Fade opacity from 1.0 (rank 1) down to ~0.5 (last) so the ranking reads at a glance.
  const opacityFor = (i) =>
    data.length > 1 ? 1 - (i / (data.length - 1)) * 0.5 : 1;

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Trending keywords</h2>
        <span className="panel__count">{data.length.toLocaleString()}</span>
      </div>

      {data.length === 0 ? (
        <p className="muted">
          No trending keywords yet. Analyze a few log lines to populate this chart.
        </p>
      ) : (
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <BarChart
            layout="vertical"
            data={data}
            margin={{ top: 4, right: 16, left: 4, bottom: 4 }}
          >
            <CartesianGrid horizontal={false} stroke="var(--border-soft)" />
            <XAxis
              type="number"
              allowDecimals={false}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={{ stroke: "var(--border)" }}
            />
            <YAxis
              type="category"
              dataKey="label"
              width={118}
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
              radius={[0, 4, 4, 0]}
              maxBarSize={22}
              isAnimationActive={false}
            >
              {data.map((d, i) => (
                <Cell key={d.kw} fill="var(--accent)" fillOpacity={opacityFor(i)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}

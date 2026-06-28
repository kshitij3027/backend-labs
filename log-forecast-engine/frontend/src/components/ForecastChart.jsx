import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { shortTime } from "../util.js";

// Centerpiece "live predictions vs actual" chart.
//
// Builds a single time-ordered series spanning recent ACTUAL observations
// (GET /metrics/{name}) followed by the ensemble forecast (GET /forecast/{steps}),
// with a shaded prediction-interval band (lower..upper) over the forecast region.
// A reference line marks "now" — the boundary between observed and predicted.
export default function ForecastChart({ forecast, actual, metric, horizonLabel }) {
  const actualPoints = (actual && Array.isArray(actual.points) ? actual.points : [])
    .map((p) => ({ ts: p.timestamp, actual: Number(p.value) }))
    .filter((p) => p.ts && Number.isFinite(p.actual));

  const steps =
    forecast && Array.isArray(forecast.step_timestamps)
      ? forecast.step_timestamps
      : [];
  const pred = forecast?.ensemble_prediction || [];
  const lower = forecast?.lower || [];
  const upper = forecast?.upper || [];

  const forecastPoints = steps.map((ts, i) => {
    const lo = Number(lower[i]);
    const up = Number(upper[i]);
    const point = {
      ts,
      prediction: Number.isFinite(Number(pred[i])) ? Number(pred[i]) : null,
    };
    // Recharts stacks the band as [base, height]; only include when both bounds exist.
    if (Number.isFinite(lo) && Number.isFinite(up)) {
      point.bandBase = lo;
      point.bandSpan = Math.max(0, up - lo);
    }
    return point;
  });

  const data = [...actualPoints, ...forecastPoints];
  const boundaryTs = forecastPoints.length ? forecastPoints[0].ts : null;

  const hasData = data.length > 0;

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">
          Predictions vs Actual · {metric}
        </h2>
        <span className="card__hint">horizon {horizonLabel}</span>
      </div>

      {forecast?.note && <div className="banner" style={{ marginBottom: 12 }}>{forecast.note}</div>}

      {!hasData ? (
        <div className="empty">
          No data yet — ingest metrics and wait for a forecast cycle.
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <ComposedChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid stroke="var(--border-soft)" strokeDasharray="3 3" />
            <XAxis
              dataKey="ts"
              tickFormatter={shortTime}
              stroke="var(--text-faint)"
              fontSize={11}
              minTickGap={28}
            />
            <YAxis stroke="var(--text-faint)" fontSize={11} width={56} />
            <Tooltip
              labelFormatter={(v) => shortTime(v)}
              formatter={(val, name) => [
                typeof val === "number" ? val.toFixed(3) : val,
                name,
              ]}
            />
            <Legend />

            {/* Confidence band: invisible base + visible span stacked on top. */}
            <Area
              type="monotone"
              dataKey="bandBase"
              stackId="band"
              stroke="none"
              fill="none"
              isAnimationActive={false}
              legendType="none"
              name="lower"
            />
            <Area
              type="monotone"
              dataKey="bandSpan"
              stackId="band"
              stroke="none"
              fill="var(--band)"
              fillOpacity={0.18}
              isAnimationActive={false}
              name="confidence band"
            />

            {boundaryTs && (
              <ReferenceLine
                x={boundaryTs}
                stroke="var(--text-faint)"
                strokeDasharray="4 4"
                label={{ value: "now", fill: "var(--text-faint)", fontSize: 11 }}
              />
            )}

            <Line
              type="monotone"
              dataKey="actual"
              name="actual"
              stroke="var(--actual)"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="prediction"
              name="ensemble prediction"
              stroke="var(--pred)"
              strokeWidth={2}
              strokeDasharray="5 4"
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}

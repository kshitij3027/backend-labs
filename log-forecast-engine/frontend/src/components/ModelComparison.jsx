import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { modelColor, fmt, pct, shortTime } from "../util.js";

// Side-by-side individual model comparison: one line per ensemble member from
// forecast.individual_forecasts, plus a table of each model's weight, accuracy
// and deploy flag from GET /models. Satisfies the "side-by-side individual
// model comparison" requirement.
export default function ModelComparison({ forecast, models }) {
  const individual = forecast?.individual_forecasts || {};
  const names = Object.keys(individual);
  const steps = forecast?.step_timestamps || [];

  // Build a per-step row keyed by timestamp index with each model's value.
  const data = steps.map((ts, i) => {
    const row = { ts };
    for (const name of names) {
      const v = Number(individual[name]?.[i]);
      row[name] = Number.isFinite(v) ? v : null;
    }
    return row;
  });

  const roster = models && Array.isArray(models.models) ? models.models : [];

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">Individual Model Comparison</h2>
        <span className="card__hint">
          {names.length} forecasting · {roster.length} registered
        </span>
      </div>

      {names.length === 0 ? (
        <div className="empty">No per-model forecasts available.</div>
      ) : (
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
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
            {names.map((name, i) => (
              <Line
                key={name}
                type="monotone"
                dataKey={name}
                name={name}
                stroke={modelColor(i)}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}

      {roster.length > 0 && (
        <table style={{ marginTop: 14 }}>
          <thead>
            <tr>
              <th>Model</th>
              <th className="num">Weight</th>
              <th className="num">Accuracy</th>
              <th>Deployed</th>
            </tr>
          </thead>
          <tbody>
            {roster.map((m) => (
              <tr key={m.model_name}>
                <td>{m.model_name}</td>
                <td className="num">{fmt(m.weight, 3)}</td>
                <td className="num">
                  {m.accuracy == null ? "—" : pct(m.accuracy)}
                </td>
                <td>
                  <span className={`badge badge--${m.is_deployed ? "green" : "neutral"}`}>
                    {m.is_deployed ? "yes" : "no"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

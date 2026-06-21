import { useMemo } from "react";
import { Doughnut } from "react-chartjs-2";
// Side-effect import: registers ArcElement + scales exactly once (see chartSetup).
import "./chartSetup.js";
import { severityColors } from "./severityColors.js";

// Doughnut of the live severity distribution from the metrics snapshot.
//
// Null-safe: before any data, `snapshot.severity_distribution` is absent/empty and
// we render an empty-state placeholder instead of an empty (or crashing) chart.

export default function SeverityChart({ snapshot }) {
  const dist = (snapshot && snapshot.severity_distribution) || {};
  // Sort labels by count desc so the legend/segments are stable + readable.
  const labels = useMemo(
    () => Object.keys(dist).sort((a, b) => (dist[b] || 0) - (dist[a] || 0)),
    [dist],
  );
  const values = labels.map((l) => dist[l] || 0);
  const total = values.reduce((a, b) => a + b, 0);

  const data = useMemo(
    () => ({
      labels,
      datasets: [
        {
          label: "Severity",
          data: values,
          backgroundColor: severityColors(labels),
          borderColor: "#1e293b", // --bg-elev: thin separators between arcs
          borderWidth: 2,
          hoverOffset: 6,
        },
      ],
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [labels.join("|"), values.join("|")],
  );

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      cutout: "62%",
      plugins: {
        legend: { position: "bottom", labels: { boxWidth: 12, padding: 14 } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const v = ctx.parsed || 0;
              const pct = total ? ((v / total) * 100).toFixed(1) : "0.0";
              return ` ${ctx.label}: ${v} (${pct}%)`;
            },
          },
        },
      },
    }),
    [total],
  );

  return (
    <div className="card chart-card">
      <h3 className="card__title">Severity Distribution</h3>
      <div className="chart-card__canvas">
        {total > 0 ? (
          <Doughnut data={data} options={options} />
        ) : (
          <div className="empty-state">No classifications yet</div>
        )}
      </div>
    </div>
  );
}

import { useMemo } from "react";
import { Bar } from "react-chartjs-2";
// Side-effect import: registers BarElement + CategoryScale/LinearScale once.
import "./chartSetup.js";
import { categoricalColors } from "./severityColors.js";

// Vertical bar of the live category distribution from the metrics snapshot.
// Null-safe: empty/absent distribution renders an empty-state placeholder.

export default function CategoryChart({ snapshot }) {
  const dist = (snapshot && snapshot.category_distribution) || {};
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
          label: "Count",
          data: values,
          backgroundColor: categoricalColors(labels.length),
          borderRadius: 6,
          maxBarThickness: 56,
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
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (ctx) => ` ${ctx.parsed.y}` } },
      },
      scales: {
        x: { grid: { display: false }, ticks: { autoSkip: false } },
        y: {
          beginAtZero: true,
          ticks: { precision: 0 },
          grid: { color: "rgba(51, 65, 85, 0.4)" },
        },
      },
    }),
    [],
  );

  return (
    <div className="card chart-card">
      <h3 className="card__title">Category Distribution</h3>
      <div className="chart-card__canvas">
        {total > 0 ? (
          <Bar data={data} options={options} />
        ) : (
          <div className="empty-state">No classifications yet</div>
        )}
      </div>
    </div>
  );
}

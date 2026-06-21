import { useMetricsSocket } from "./hooks/useWebSocket.js";
import StatCards from "./components/StatCards.jsx";
import ClassifyForm from "./components/ClassifyForm.jsx";
import SeverityChart from "./components/SeverityChart.jsx";
import CategoryChart from "./components/CategoryChart.jsx";
import FeatureImportance from "./components/FeatureImportance.jsx";
import PredictionsTable from "./components/PredictionsTable.jsx";
import ModelPanel from "./components/ModelPanel.jsx";

// Top-level dashboard shell: header + live connection indicator, then the full
// dashboard. The live charts/table read the shared metrics snapshot from the
// WebSocket; the classify form, feature-importance chart and model panel use REST
// (and own their own fetch/poll state). Every child is null-safe / has a loading
// state, so the whole tree renders on first paint before any data arrives.
export default function App() {
  const { snapshot, connected } = useMetricsSocket();

  return (
    <div className="app">
      <header className="app__header">
        <h1 className="app__title">ML Log Classifier &mdash; Live Dashboard</h1>
        <div
          className="conn"
          role="status"
          aria-live="polite"
          title={connected ? "WebSocket connected" : "WebSocket disconnected"}
        >
          <span
            className={`conn__dot ${connected ? "conn__dot--up" : "conn__dot--down"}`}
            aria-hidden="true"
          />
          <span className="conn__label">
            {connected ? "Connected" : "Disconnected"}
          </span>
        </div>
      </header>

      <main className="app__main">
        {/* Headline metrics */}
        <StatCards snapshot={snapshot} />

        {/* Interactive classification */}
        <section className="section">
          <ClassifyForm />
        </section>

        {/* Distributions side-by-side (responsive: stacks on narrow screens) */}
        <section className="section">
          <h2 className="section__title">Distributions</h2>
          <div className="grid grid--2">
            <SeverityChart snapshot={snapshot} />
            <CategoryChart snapshot={snapshot} />
          </div>
        </section>

        {/* Model explainability */}
        <section className="section">
          <h2 className="section__title">Model Explainability</h2>
          <FeatureImportance />
        </section>

        {/* Live recent predictions */}
        <section className="section">
          <h2 className="section__title">Live Feed</h2>
          <PredictionsTable snapshot={snapshot} />
        </section>

        {/* Model registry / A-B / adaptive / services */}
        <section className="section">
          <h2 className="section__title">Models &amp; Serving</h2>
          <ModelPanel />
        </section>
      </main>
    </div>
  );
}

import { useMetricsSocket } from "./hooks/useWebSocket.js";
import StatCards from "./components/StatCards.jsx";

// Top-level dashboard shell: header + live connection indicator + stat cards.
//
// This commit is the SCAFFOLD — stat cards only. Charts, the recent-predictions
// table, and the classify form arrive in the next commit and slot in below the
// cards. The whole tree renders before any data/WS thanks to null-safe children.
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
        <StatCards snapshot={snapshot} />
      </main>
    </div>
  );
}

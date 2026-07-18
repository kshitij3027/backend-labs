import { useEffect, useState } from "react";
import { useWebSocket } from "./hooks/useWebSocket.js";
import { getHealth, getStats } from "./api.js";
import AnalyzeBox from "./components/AnalyzeBox.jsx";
import ResultCard from "./components/ResultCard.jsx";
import LiveFeed from "./components/LiveFeed.jsx";

// Top-level NLP dashboard shell (C11).
//
// `useWebSocket` opens the `/ws` live feed: it surfaces the connection `status`, the latest
// `stats` snapshot, and a capped, newest-first `feed` of analysed lines. On mount we also
// fetch `/api/health` and `/api/stats` once over REST so the header shows a count and the
// analyzer-ready state immediately, before the first live frame arrives; each subsequent
// `stats` WS frame then refreshes the count in lockstep with the feed.
//
// Layout: a header (title + live status + analyzer health + "analyzed N"), a top row with
// the AnalyzeBox and the latest manual ResultCard, a clearly-marked placeholder where the
// C12 charts row will mount, and the LiveFeed. Posting a line to /api/analyze also causes
// the backend to broadcast it, so a manual analysis appears BOTH as the ResultCard and as
// the newest LiveFeed row.
export default function App() {
  const { status, lastStats, feed } = useWebSocket();

  // The most recent manual analysis (from the AnalyzeBox), shown in the ResultCard.
  const [manualResult, setManualResult] = useState(null);

  // Header telemetry. `stats` is seeded by the initial REST fetch and then overwritten by
  // each live `stats` frame; `health` is a one-shot analyzer-ready probe.
  const [stats, setStats] = useState(null);
  const [health, setHealth] = useState(null);

  // One-shot REST bootstrap so the header isn't blank until the first WS frame.
  useEffect(() => {
    let alive = true;
    getHealth()
      .then((h) => alive && setHealth(h))
      .catch(() => {});
    getStats()
      .then((s) => alive && setStats(s))
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, []);

  // Fold each live stats snapshot into the header state.
  useEffect(() => {
    if (lastStats) setStats(lastStats);
  }, [lastStats]);

  const analyzedCount = Number(stats?.total_analyzed ?? 0);

  // Live indicator: Connecting (first connect) -> Live (socket open) -> Offline (dropped).
  let liveTone = "wait";
  let liveText = "Connecting…";
  if (status === "live") {
    liveTone = "ok";
    liveText = "Live";
  } else if (status === "offline") {
    liveTone = "bad";
    liveText = "Offline";
  }

  // Analyzer readiness from the health probe (defaults to unknown/grey until it resolves).
  const analyzerReady = health?.analyzer_ready === true;
  const healthTone = health == null ? "wait" : analyzerReady ? "ok" : "bad";
  const healthText =
    health == null ? "Analyzer…" : analyzerReady ? "Analyzer ready" : "Analyzer down";

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">NLP Log Processing Engine</h1>
            <p className="app__subtitle">
              Entities · intent · sentiment · keywords — live over WebSocket
            </p>
          </div>
        </div>

        <div className="headmeta">
          <span className="headstat">
            <span className="headstat__value">{analyzedCount.toLocaleString()}</span>
            <span className="headstat__label">analyzed</span>
          </span>
          <span className="live" role="status" aria-live="polite">
            <span className={`live__dot live__dot--${healthTone}`} aria-hidden="true" />
            <span className="live__text">{healthText}</span>
          </span>
          <span className="live" role="status" aria-live="polite">
            <span className={`live__dot live__dot--${liveTone}`} aria-hidden="true" />
            <span className="live__text">{liveText}</span>
          </span>
        </div>
      </header>

      <main className="app__main">
        {/* Top row: the analyze box and the latest manual result, side by side. */}
        <div className="app__top">
          <AnalyzeBox onResult={setManualResult} />
          {manualResult ? (
            <ResultCard result={manualResult} />
          ) : (
            <section className="panel panel--placeholder resultcard--empty">
              <p className="placeholder__note">
                Analyze a log line to see its <strong>entities</strong>,{" "}
                <strong>intent</strong>, <strong>sentiment</strong> and{" "}
                <strong>keywords</strong> here. Results also stream into the live feed below.
              </p>
            </section>
          )}
        </div>

        {/* ================= C12 CHARTS ROW (placeholder) =================
            C12 mounts the recharts visualisations here — intent & sentiment
            distributions, trending keywords and a throughput sparkline — all fed by
            `stats` (the /api/stats bootstrap + live `stats` WS frames already wired
            above). `recharts` is already a dependency in package.json; nothing in the
            C11 shell imports it yet. Keep this region as the single insertion point. */}
        <section className="panel panel--placeholder charts-placeholder">
          <span className="panel__soon">CHARTS · C12</span>
          <p className="placeholder__note">
            Intent &amp; sentiment distributions, trending keywords and throughput charts
            land here in C12, driven by the same live stats feed.
          </p>
        </section>

        <LiveFeed feed={feed} status={status} />
      </main>
    </div>
  );
}

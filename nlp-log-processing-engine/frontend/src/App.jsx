import { useEffect, useState } from "react";
import { useWebSocket } from "./hooks/useWebSocket.js";
import { useStats } from "./hooks/useStats.js";
import { getHealth, getStats } from "./api.js";
import AnalyzeBox from "./components/AnalyzeBox.jsx";
import ResultCard from "./components/ResultCard.jsx";
import LiveFeed from "./components/LiveFeed.jsx";
import StatsCards from "./components/StatsCards.jsx";
import IntentChart from "./components/IntentChart.jsx";
import SentimentChart from "./components/SentimentChart.jsx";
import EntityTypeChart from "./components/EntityTypeChart.jsx";
import TrendingKeywords from "./components/TrendingKeywords.jsx";

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

  // Freshest stats for the C12 charts row: REST bootstrap + ~5s fallback poll, updated live
  // from each WS `stats` frame (freshest source wins), with a `stale` flag when the poll is
  // failing. Independent of the header's one-shot `stats` below.
  const { stats: chartStats, stale: chartStale } = useStats(lastStats);

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

        {/* ===================== C12 CHARTS ROW =====================
            KPI tiles across the top, then a responsive 2-up grid of the intent, sentiment,
            entity-type and trending-keyword charts (Recharts). All are fed by
            `useStats(lastStats)` — REST bootstrap + ~5s poll, refreshed live on every WS
            `stats` frame — so they populate from zero-data empty-states as lines are
            analysed. The grid uses auto-fit/minmax to stack to a single column on narrow
            and mobile widths with no media query. */}
        <section
          className="charts"
          aria-label="Live statistics"
          style={{ display: "flex", flexDirection: "column", gap: "var(--gap)" }}
        >
          <StatsCards stats={chartStats} status={status} stale={chartStale} />
          <div
            className="charts__grid"
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 26rem), 1fr))",
              gap: "var(--gap)",
            }}
          >
            <IntentChart stats={chartStats} />
            <SentimentChart stats={chartStats} />
            <EntityTypeChart stats={chartStats} />
            <TrendingKeywords stats={chartStats} />
          </div>
        </section>

        <LiveFeed feed={feed} status={status} />
      </main>
    </div>
  );
}

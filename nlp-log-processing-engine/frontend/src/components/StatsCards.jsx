import { fmt } from "../util.js";

// Compact KPI row sitting above the C12 charts grid. Four tiles:
//   * Analyzed        — total_analyzed, thousands-separated.
//   * Throughput      — throughput_per_sec, e.g. "12.3 /s".
//   * Intents seen    — number of DISTINCT intent labels observed so far.
//   * Live feed       — the WebSocket connection status (Live / Connecting / Offline),
//                       with a small note that flips to "stale" when the REST poll is
//                       failing yet we're still showing the last-good snapshot.
//
// Everything degrades gracefully to zero/empty (the dashboard opens with no data). Styling
// is self-contained (inline styles over the existing CSS tokens) so no shared stylesheet is
// touched; the connection dot reuses the shell's `.live__dot` classes for a consistent look.
//
// Props:
//   stats  — a normalised /api/stats snapshot from useStats().
//   status — "connecting" | "live" | "offline" (from useWebSocket()).
//   stale  — true when the poll is erroring and `stats` is last-good (from useStats()).

const STATUS_META = {
  live: { tone: "ok", text: "Live" },
  connecting: { tone: "wait", text: "Connecting…" },
  offline: { tone: "bad", text: "Offline" },
};

const GRID_STYLE = {
  display: "grid",
  // auto-fit + minmax keeps the row responsive with no media query: 4-up on wide screens,
  // wrapping to 2-up then 1-up as the viewport narrows.
  gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 160px), 1fr))",
  gap: "var(--gap)",
};

const CARD_STYLE = {
  background: "linear-gradient(180deg, var(--card), var(--card-2))",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  boxShadow: "var(--shadow)",
  padding: "13px 15px",
  display: "flex",
  flexDirection: "column",
  gap: "3px",
  minWidth: 0,
};

const LABEL_STYLE = {
  fontSize: "10.5px",
  fontWeight: 700,
  textTransform: "uppercase",
  letterSpacing: "0.05em",
  color: "var(--text-faint)",
};

const VALUE_STYLE = {
  fontSize: "23px",
  fontWeight: 800,
  color: "var(--text)",
  fontVariantNumeric: "tabular-nums",
  lineHeight: 1.15,
};

const HINT_STYLE = { fontSize: "11px", color: "var(--text-faint)" };

function StatCard({ label, value, hint }) {
  return (
    <div style={CARD_STYLE}>
      <span style={LABEL_STYLE}>{label}</span>
      <span style={VALUE_STYLE}>{value}</span>
      <span style={HINT_STYLE}>{hint}</span>
    </div>
  );
}

export default function StatsCards({ stats, status = "connecting", stale = false }) {
  const total = Number(stats?.total_analyzed ?? 0);
  const throughput = Number(stats?.throughput_per_sec ?? 0);

  const intents = stats?.intent_distribution;
  const distinctIntents =
    intents && typeof intents === "object"
      ? Object.values(intents).filter((c) => Number(c) > 0).length
      : 0;

  const conn = STATUS_META[status] || STATUS_META.connecting;

  return (
    <section style={GRID_STYLE} aria-label="Summary statistics">
      <StatCard label="Analyzed" value={total.toLocaleString()} hint="log lines" />
      <StatCard label="Throughput" value={`${fmt(throughput, 1)} /s`} hint="approx." />
      <StatCard
        label="Intents seen"
        value={distinctIntents.toLocaleString()}
        hint="distinct classes"
      />
      <div style={CARD_STYLE}>
        <span style={LABEL_STYLE}>Live feed</span>
        <span
          style={{
            ...VALUE_STYLE,
            fontSize: "18px",
            display: "inline-flex",
            alignItems: "center",
            gap: "9px",
          }}
        >
          <span className={`live__dot live__dot--${conn.tone}`} aria-hidden="true" />
          {conn.text}
        </span>
        <span style={HINT_STYLE}>{stale ? "stats stale — reconnecting" : "websocket"}</span>
      </div>
    </section>
  );
}

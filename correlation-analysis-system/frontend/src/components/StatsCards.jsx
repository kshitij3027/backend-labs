import { CORRELATION_TYPES, fmt, num, typeClass, typeLabel } from "../util.js";

// One metric card. `tone` ("ok" | "bad" | "warn" | undefined) tints the value for
// the status cards (Redis / Pipeline). `hint` is an optional sub-label line.
function Card({ label, value, tone, hint }) {
  return (
    <div className="statcard">
      <span className={`statcard__value${tone ? ` statcard__value--${tone}` : ""}`}>
        {value}
      </span>
      <span className="statcard__label">{label}</span>
      {hint ? <span className="statcard__hint">{hint}</span> : null}
    </div>
  );
}

// Live stats + status grid. Reads the dashboard `stats` block (correlation counts,
// throughput, memory, alerts) and the `status` block (redis / pipeline / active
// scenario). Every field is read defensively so the grid paints before the first
// poll resolves and never crashes on a degraded/empty payload.
//
// Props:
//   stats  — dashboard.stats, or {} while loading / degraded
//   status — dashboard.status, or {} while loading / degraded
export default function StatsCards({ stats = {}, status = {} }) {
  const redisOk = Boolean(status.redis);
  const pipelineOn = Boolean(status.pipeline_running);
  const scenario = status.active_scenario || "normal";
  const scenarioActive = Boolean(status.active_scenario);

  // Per-type breakdown chips. Show the full fixed taxonomy in canonical order so
  // the layout is stable; a type with no detections yet reads 0.
  const types = stats.types && typeof stats.types === "object" ? stats.types : {};

  return (
    <section className="statcards" aria-label="Live statistics">
      <div className="statcards__grid">
        <Card label="Total correlations" value={num(stats.total)} />
        <Card label="Avg strength" value={fmt(stats.avg_strength, 2)} />
        <Card label="Recent (60s)" value={num(stats.recent_count)} />
        <Card label="Events / sec" value={fmt(stats.events_per_sec, 1)} />
        <Card label="Events processed" value={num(stats.events_processed)} />
        <Card label="Memory (MB)" value={fmt(stats.memory_mb, 1)} />
        <Card label="Alerts total" value={num(stats.alerts_total)} />
        <Card
          label="Active scenario"
          value={scenario}
          tone={scenarioActive ? "warn" : undefined}
        />
        <Card
          label="Redis"
          value={redisOk ? "connected" : "down"}
          tone={redisOk ? "ok" : "bad"}
        />
        <Card
          label="Pipeline"
          value={pipelineOn ? "running" : "stopped"}
          tone={pipelineOn ? "ok" : "bad"}
        />
      </div>

      <div className="typechips" aria-label="Correlations by type">
        {CORRELATION_TYPES.map((t) => (
          <span key={t} className={`typechip type--${typeClass(t)}`}>
            <span className="typechip__dot" aria-hidden="true" />
            <span className="typechip__label">{typeLabel(t)}</span>
            <span className="typechip__count">{num(types[t] ?? 0)}</span>
          </span>
        ))}
      </div>
    </section>
  );
}

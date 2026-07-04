import { useCallback, useEffect, useRef, useState } from "react";
import {
  getHealth,
  getStats,
  postRecommend,
  postFeedback,
  getConfig,
  putConfig,
  DEFAULT_POLL_MS,
} from "./api.js";
import RecommendForm from "./components/RecommendForm.jsx";
import SuggestionList from "./components/SuggestionList.jsx";
import ControlsPanel from "./components/ControlsPanel.jsx";
import StatsPanel from "./components/StatsPanel.jsx";

// Poll cadence for the status strip. A constant for now; C17 may adopt a value
// supplied by GET /config.
const POLL_MS = DEFAULT_POLL_MS;

// Human labels for the per-subsystem component booleans in HealthResponse.components.
const COMPONENT_LABELS = [
  ["database", "Database"],
  ["vector_extension", "pgvector"],
  ["redis", "Redis"],
  ["embedding_model", "Embedding model"],
];

// One component readiness pill (green when true, red when false/absent).
function ComponentPill({ label, ok }) {
  return (
    <span className={`pill ${ok ? "pill--ok" : "pill--bad"}`}>
      <span className="pill__dot" aria-hidden="true" />
      {label}
    </span>
  );
}

// Top-level dashboard (C15 shell → C16 recommend → C17 feedback loop + controls).
// A single polling loop fetches GET /health + GET /stats through nginx's /api proxy
// and renders service status, per-component readiness, corpus size, and (C17) a live
// stats panel. C16 adds the recommend form + ranked suggestion cards. C17 closes the
// loop: 👍/👎 votes on a suggestion POST /feedback then RE-RUN the last query so the
// re-rank is visible, and runtime sliders PUT /config then re-run so the new weighting
// takes effect. Every field read is defensive so the tree paints before any fetch.
export default function App() {
  const [health, setHealth] = useState(null);
  const [stats, setStats] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pulsing, setPulsing] = useState(false);
  const [reachable, setReachable] = useState(true);

  // Recommend cycle (C16): the current POST /recommend result, whether a request
  // is in flight, and the last error. `result` is the raw RecommendResponse; its
  // `recommendation_id` is kept here because C17's feedback votes reference it.
  const [result, setResult] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [recError, setRecError] = useState(null);

  // C17: the last submitted query body, kept so a vote or a config change can
  // silently RE-RUN the identical query and surface the re-rank / new weighting.
  const [lastQuery, setLastQuery] = useState(null);

  // C17: current effective runtime config + its version (seeded on mount from
  // GET /config, re-seeded after each successful PUT). Drives the ControlsPanel.
  const [config, setConfig] = useState(null);
  const [configVersion, setConfigVersion] = useState(null);

  // Handle a form submission: assembly happens in the form; here we own the fetch
  // lifecycle. We remember the body as `lastQuery` so votes / config edits can
  // re-run it. On failure we keep the prior result visible but surface the error,
  // so a bad query doesn't wipe out a good previous answer.
  const handleRecommend = useCallback(async (body) => {
    setSubmitting(true);
    setRecError(null);
    setLastQuery(body);
    try {
      const res = await postRecommend(body);
      setResult(res);
    } catch (e) {
      setRecError(e?.message || "Recommendation request failed.");
    } finally {
      setSubmitting(false);
    }
  }, []);

  // Refetch health + stats. Isolated with allSettled so one failing endpoint
  // never blanks the other, and a total outage flips `reachable` without throwing.
  // Defined before the vote handler so that handler can call it to tick the stats
  // panel the instant a vote lands, rather than waiting for the next poll.
  const refresh = useCallback(async () => {
    setPulsing(true);
    const [h, s] = await Promise.allSettled([getHealth(), getStats()]);

    if (h.status === "fulfilled") setHealth(h.value);
    else setHealth(null);

    if (s.status === "fulfilled") setStats(s.value);
    else setStats(null);

    setReachable(h.status === "fulfilled" || s.status === "fulfilled");
    setLastUpdated(new Date());
    setTimeout(() => setPulsing(false), 1000);
  }, []);

  // Silent re-run of the last query (no spinner-blanking, no lastQuery churn). Used
  // after a vote or a config change so the re-rank / new weighting appears in place.
  // A feedback-epoch / config-version bump makes the response `cached:false` with the
  // new order. Errors bubble to the caller; the current result is left intact.
  const rerunLastQuery = useCallback(async () => {
    if (!lastQuery) return;
    setSubmitting(true);
    try {
      const res = await postRecommend(lastQuery);
      setResult(res);
      setRecError(null);
    } finally {
      setSubmitting(false);
    }
  }, [lastQuery]);

  // C17 vote handler passed to each SuggestionCard. Record the vote against the
  // CURRENT recommendation_id + the card's incident_id, then re-run the last query
  // so the live re-rank shows. Returns the FeedbackResponse so the card can display
  // the updated helpful/unhelpful tallies. A re-run failure is swallowed here (the
  // vote itself succeeded and stats will still refresh) but leaves recError set so
  // the user knows the list may be stale; the vote ack still renders.
  const handleVote = useCallback(
    async (incidentId, helpful) => {
      const recId = result?.recommendation_id;
      if (recId == null) {
        throw new Error("No active recommendation to attach feedback to.");
      }
      const fb = await postFeedback({
        recommendation_id: recId,
        incident_id: incidentId,
        helpful,
      });
      // Re-rank: re-run the same query. Don't let a transient re-run error mask the
      // fact that the vote itself was recorded — surface it without throwing.
      try {
        await rerunLastQuery();
      } catch (e) {
        setRecError(
          (e?.message || "Re-run after vote failed.") +
            " (your vote was still recorded)",
        );
      }
      // Refresh the stats panel so the vote counters tick immediately, not on the
      // next poll tick.
      refresh();
      return fb;
    },
    [result, rerunLastQuery, refresh],
  );

  // C17 config-apply handler passed to ControlsPanel. PUT only the changed fields,
  // re-seed the panel from the new effective config (bumps the version pill), then
  // re-run the last query (if any) so the new weighting is visible. Throws on a 422
  // so the panel can show the backend `detail`.
  const handleApplyConfig = useCallback(
    async (updates) => {
      const res = await putConfig(updates); // may throw (422 detail in message)
      if (res && res.config) setConfig(res.config);
      if (res && res.version != null) setConfigVersion(res.version);
      // Reflect the new weighting in the visible results.
      try {
        await rerunLastQuery();
      } catch {
        /* keep the applied config even if the re-run hiccups */
      }
      return res;
    },
    [rerunLastQuery],
  );

  // Polling loop: fire immediately on mount, then on interval.
  const saved = useRef(refresh);
  saved.current = refresh;
  useEffect(() => {
    saved.current();
    const id = setInterval(() => saved.current(), POLL_MS);
    return () => clearInterval(id);
  }, []);

  // C17: seed the runtime config once on mount so the ControlsPanel sliders reflect
  // the live values. A failure just leaves the panel in its "unavailable" state —
  // the rest of the dashboard is unaffected (no throw escapes).
  useEffect(() => {
    let alive = true;
    getConfig()
      .then((res) => {
        if (!alive || !res) return;
        if (res.config) setConfig(res.config);
        if (res.version != null) setConfigVersion(res.version);
      })
      .catch(() => {
        /* config panel shows "unavailable"; dashboard keeps working */
      });
    return () => {
      alive = false;
    };
  }, []);

  const status = health?.status ?? (reachable ? "unknown" : "unreachable");
  const statusOk = status === "ok";
  const components = health?.components ?? {};
  // Prefer stats.corpus_size (authoritative rollup), fall back to health's best-effort count.
  const corpusSize =
    stats?.corpus_size ?? health?.corpus_size ?? null;
  const embeddedCount = stats?.embedded_count ?? null;

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__logo" aria-hidden="true" />
          <div>
            <h1 className="app__title">Log Recommendation Engine</h1>
            <p className="app__subtitle">
              Similar-incident recommendations · semantic + contextual ranking ·
              feedback-tuned
            </p>
          </div>
        </div>

        <div className="refresh" role="status" aria-live="polite">
          <span
            className={`refresh__dot ${pulsing ? "pulsing" : ""}`}
            aria-hidden="true"
          />
          <span>
            {lastUpdated
              ? `Updated ${lastUpdated.toLocaleTimeString()}`
              : "Loading…"}
            {` · every ${Math.round(POLL_MS / 1000)}s`}
          </span>
        </div>
      </header>

      <main className="app__main">
        {/* Status strip: overall service status + per-subsystem readiness + corpus size. */}
        <section className="card status">
          <div className="status__row">
            <span
              className={`badge ${
                statusOk ? "badge--ok" : reachable ? "badge--warn" : "badge--bad"
              }`}
            >
              <span className="badge__dot" aria-hidden="true" />
              {statusOk
                ? "Service OK"
                : reachable
                ? `Service ${status}`
                : "API unreachable"}
            </span>

            {health?.service && (
              <span className="status__meta">
                {health.service}
                {health.version ? ` v${health.version}` : ""}
              </span>
            )}
          </div>

          <div className="status__components">
            {COMPONENT_LABELS.map(([key, label]) => (
              <ComponentPill key={key} label={label} ok={Boolean(components[key])} />
            ))}
          </div>

          <div className="status__metrics">
            <div className="metric">
              <span className="metric__value">
                {corpusSize == null ? "—" : corpusSize.toLocaleString()}
              </span>
              <span className="metric__label">Incidents in corpus</span>
            </div>
            <div className="metric">
              <span className="metric__value">
                {embeddedCount == null ? "—" : embeddedCount.toLocaleString()}
              </span>
              <span className="metric__label">Embedded (searchable)</span>
            </div>
          </div>
        </section>

        {/* Recommend UI (C16): incident form (left) + ranked suggestions (right).
            The list gets the C17 vote handler so 👍/👎 records feedback and re-ranks.
            Collapses to a single column on narrow viewports. */}
        <div className="recommend-grid">
          <RecommendForm onSubmit={handleRecommend} submitting={submitting} />
          <SuggestionList
            result={result}
            submitting={submitting}
            error={recError}
            onVote={handleVote}
          />
        </div>

        {/* Insights row (C17): runtime ranking controls + the live corpus/feedback
            stats panel side by side, collapsing to one column when narrow. */}
        <div className="insights-grid">
          <ControlsPanel
            config={config}
            version={configVersion}
            onApply={handleApplyConfig}
          />
          <StatsPanel stats={stats} lastUpdated={lastUpdated} pollMs={POLL_MS} />
        </div>
      </main>
    </div>
  );
}

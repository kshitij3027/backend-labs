// Backend API client for the Log Recommendation Engine dashboard.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to
//     the `api` service (a `rewrite ... break` strips the `/api` prefix, so
//     `/api/health` -> `http://api:8000/health`).
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy.

export const API_BASE = "/api";

// Fallback poll interval (ms) for the health/stats status strip. C17 may later
// adopt a cadence supplied by GET /config; until then this constant governs it.
export const DEFAULT_POLL_MS = 15_000;

/** Internal: GET `path` under the API base and return parsed JSON, or throw. */
async function getJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`GET ${path} failed: ${res.status}`);
  }
  return res.json();
}

/** Internal: send a JSON body with `method` to `path`, return parsed JSON. */
async function postJSON(path, method, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body == null ? undefined : { "Content-Type": "application/json" },
    body: body == null ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    // Surface the backend's validation detail when present (e.g. 422 from POST /recommend).
    let detail = "";
    try {
      const j = await res.json();
      detail =
        j && j.detail
          ? ` — ${typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail)}`
          : "";
    } catch {
      /* ignore non-JSON error bodies */
    }
    throw new Error(`${method} ${path} failed: ${res.status}${detail}`);
  }
  return res.json();
}

/**
 * GET /health — deep readiness: overall status, per-subsystem component booleans
 * (database, vector_extension, redis, embedding_model), service, version, corpus_size.
 * Always HTTP 200 while the process is alive; a degraded stack is reported in the body.
 * @returns {Promise<object>} HealthResponse
 */
export function getHealth() {
  return getJSON("/health");
}

/**
 * GET /stats — corpus + feedback rollup: corpus_size, embedded_count, by_service,
 * by_severity, feedback_total/helpful/unhelpful, recommendations_served, top_patterns.
 * @returns {Promise<object>} StatsResponse
 */
export function getStats() {
  return getJSON("/stats");
}

/**
 * POST /recommend — rank incident recommendations for a query (wired in C16).
 * @param {object} body {title, description, service?, severity?, tags?, top_k?, ...}
 * @returns {Promise<object>} RecommendResponse — {recommendation_id, count, cached, suggestions}
 */
export function postRecommend(body) {
  return postJSON("/recommend", "POST", body);
}

// --------------------------------------------------------------------------- //
// Feedback + runtime config (C17). These close the loop in the dashboard: a vote
// records feedback and a config edit retunes the ranking, both taking effect on
// the very next /recommend. `postJSON` surfaces a 422 body's `detail` in the
// thrown Error message, so callers can show the backend's validation reason.
// --------------------------------------------------------------------------- //

/**
 * POST /feedback — record a helpful / unhelpful vote on a served recommendation (C17).
 * The vote references a real prior result: `recommendation_id` (from the last
 * RecommendResponse) and an `incident_id` that was one of that recommendation's
 * suggestions. The response echoes the post-update aggregate for that pair.
 * @param {object} body {recommendation_id, incident_id, helpful}
 * @returns {Promise<object>} FeedbackResponse — {recorded, query_pattern, incident_id, helpful_count, unhelpful_count} (201)
 */
export function postFeedback(body) {
  return postJSON("/feedback", "POST", body);
}

/**
 * GET /config — current effective runtime ranking config + its global version (C17).
 * @returns {Promise<object>} ConfigResponse — {version, config:{weight_semantic, weight_contextual, weight_feedback, epsilon_explore, diversity_threshold, recency_half_life_days, top_k, high_confidence_threshold, medium_confidence_threshold}}
 */
export function getConfig() {
  return getJSON("/config");
}

/**
 * PUT /config — partial runtime update, no restart. Send only the fields to change;
 * an out-of-range or unknown key is rejected with 422 (its `detail` is folded into
 * the thrown Error message). On success the version bumps and the new effective
 * config is returned — the next /recommend recomputes under it.
 * @param {object} updates {weight_semantic?, weight_contextual?, weight_feedback?, epsilon_explore?, diversity_threshold?, ...}
 * @returns {Promise<object>} the new effective ConfigResponse — {version, config}
 */
export function putConfig(updates) {
  return postJSON("/config", "PUT", updates);
}

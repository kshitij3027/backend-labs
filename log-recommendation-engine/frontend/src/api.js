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

// --------------------------------------------------------------------------- //
// Stubs wired to the UI in later commits. Defined now so C16/C17 need only build
// components, not touch the client. Each mirrors the real API contract.
// --------------------------------------------------------------------------- //

/**
 * POST /recommend — rank incident recommendations for a query (wired in C16).
 * @param {object} body {query, service?, severity?, top_k?, ...}
 * @returns {Promise<object>} RecommendResponse
 */
export function postRecommend(body) {
  return postJSON("/recommend", "POST", body);
}

/**
 * POST /feedback — record a helpful / unhelpful vote on a served recommendation (C17).
 * @param {object} body {recommendation_id, incident_id, helpful, ...}
 * @returns {Promise<object>} FeedbackResponse
 */
export function postFeedback(body) {
  return postJSON("/feedback", "POST", body);
}

/**
 * GET /config — current runtime ranking config (weights, thresholds, top_k) (C17).
 * @returns {Promise<object>} ConfigResponse
 */
export function getConfig() {
  return getJSON("/config");
}

/**
 * PUT /config — partial runtime update, no restart. Send only fields to change (C17).
 * @param {object} partial {weight_semantic?, weight_contextual?, weight_feedback?, top_k?, ...}
 * @returns {Promise<object>} the new effective ConfigResponse
 */
export function putConfig(partial) {
  return postJSON("/config", "PUT", partial);
}

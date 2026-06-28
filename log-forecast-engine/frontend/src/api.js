// Backend API client for the Predictive Log Analytics Engine dashboard.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to
//     the `api` service (the trailing slash strips the `/api` prefix, so
//     `/api/predictions` -> `http://api:8000/predictions`).
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy.

export const API_BASE = "/api";

// Fallback poll interval (ms) used until /config supplies the real cadence.
// The backend's requirements §7 dashboard_poll_interval is 30s.
export const DEFAULT_POLL_MS = 30_000;

/** Internal: GET `path` under the API base and return parsed JSON, or throw. */
async function getJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`GET ${path} failed: ${res.status}`);
  }
  return res.json();
}

/** Internal: send a JSON body with `method` to `path`, return parsed JSON. */
async function sendJSON(path, method, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body == null ? undefined : { "Content-Type": "application/json" },
    body: body == null ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    // Surface the backend's validation detail when present (e.g. 422 from PUT /config).
    let detail = "";
    try {
      const j = await res.json();
      detail = j && j.detail ? ` — ${typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail)}` : "";
    } catch {
      /* ignore non-JSON error bodies */
    }
    throw new Error(`${method} ${path} failed: ${res.status}${detail}`);
  }
  return res.json();
}

/**
 * GET /health — service status, deployed model count, db/redis booleans, perf.
 * @returns {Promise<object>} HealthResponse
 */
export function getHealth() {
  return getJSON("/health");
}

/**
 * GET /metrics — application metrics JSON (per-model accuracy, processing times,
 * resource usage, counts).
 * @returns {Promise<object>} AppMetricsResponse
 */
export function getAppMetrics() {
  return getJSON("/metrics");
}

/**
 * GET /metrics/{name} — recent ACTUAL observed points for a metric (oldest-first).
 * @param {string} name metric name
 * @param {number} [limit=100]
 * @returns {Promise<object>} MetricQueryResponse {metric_name, count, points:[{metric_name,timestamp,value}]}
 */
export function getMetricData(name, limit = 100) {
  return getJSON(`/metrics/${encodeURIComponent(name)}?limit=${encodeURIComponent(limit)}`);
}

/**
 * GET /predictions — latest ensemble forecast for a metric (cache -> DB).
 * @param {string} [metric] metric name (defaults server-side to first available)
 * @param {number} [horizon] horizon in MINUTES (optional)
 * @returns {Promise<object>} ForecastResponse
 */
export function getPrediction(metric, horizon) {
  const params = new URLSearchParams();
  if (metric) params.set("metric", metric);
  if (horizon != null) params.set("horizon", String(horizon));
  const qs = params.toString();
  return getJSON(`/predictions${qs ? `?${qs}` : ""}`);
}

/**
 * GET /forecast/{steps} — custom-horizon forecast, computed on demand (1..288).
 * @param {number} steps number of future steps
 * @param {string} [metric] metric name
 * @returns {Promise<object>} ForecastResponse
 */
export function getForecastSteps(steps, metric) {
  const qs = metric ? `?metric=${encodeURIComponent(metric)}` : "";
  return getJSON(`/forecast/${encodeURIComponent(steps)}${qs}`);
}

/**
 * GET /forecast/{metric}/history — past forecasts + recent per-model accuracy.
 * @param {string} metric metric name
 * @param {number} [limit=50]
 * @returns {Promise<object>} ForecastHistoryResponse {metric_name,count,items,recent_accuracy}
 */
export function getForecastHistory(metric, limit = 50) {
  return getJSON(
    `/forecast/${encodeURIComponent(metric)}/history?limit=${encodeURIComponent(limit)}`,
  );
}

/**
 * GET /models — ensemble roster with weights, accuracy and deploy flags.
 * @returns {Promise<object>} ModelsResponse {count, deployed_count, models:[ModelInfo]}
 */
export function getModels() {
  return getJSON("/models");
}

/**
 * POST /retrain — schedule an out-of-band retrain (202; never blocks).
 * @param {string} [metric] metric to retrain; omit to retrain all known metrics
 * @returns {Promise<object>} RetrainResponse {status, metric, mode, task_id?}
 */
export function postRetrain(metric) {
  const qs = metric ? `?metric=${encodeURIComponent(metric)}` : "";
  return sendJSON(`/retrain${qs}`, "POST", null);
}

/**
 * GET /config — current runtime config (weights, thresholds, alert + static context).
 * @returns {Promise<object>} ConfigResponse
 */
export function getConfig() {
  return getJSON("/config");
}

/**
 * PUT /config — partial runtime update (no restart). Send only the fields to change.
 * @param {object} partial {model_weights?, high_confidence_threshold?, medium_confidence_threshold?, alert_settings?}
 * @returns {Promise<object>} the new effective ConfigResponse
 */
export function putConfig(partial) {
  return sendJSON("/config", "PUT", partial);
}

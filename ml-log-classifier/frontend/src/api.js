// Backend API client.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to
//     the `app` service and upgrades `/ws/*` to its WebSocket.
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy.

export const API_BASE = "/api";

// Build the absolute WebSocket URL from the current page origin, picking wss://
// when the page itself is served over https. `location.host` already includes
// the port, so this is correct behind nginx (`:8080`) and in dev (`:5173`).
export const WS_URL =
  (window.location.protocol === "https:" ? "wss:" : "ws:") +
  "//" +
  window.location.host +
  "/ws/metrics";

/**
 * GET the latest metrics snapshot (the same shape the WS streams).
 * Used once on mount so cards paint before the first WS tick.
 * @returns {Promise<object>} the metrics snapshot
 */
export async function getMetrics() {
  const res = await fetch(`${API_BASE}/metrics`);
  if (!res.ok) {
    throw new Error(`GET /metrics failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET aggregate service stats ({ total_classified, model_status }).
 * @returns {Promise<object>}
 */
export async function getStats() {
  const res = await fetch(`${API_BASE}/stats`);
  if (!res.ok) {
    throw new Error(`GET /stats failed: ${res.status}`);
  }
  return res.json();
}

/**
 * POST a single raw log line for classification (base ensemble).
 * @param {string} raw_log
 * @returns {Promise<object>} the classification result
 */
export async function classify(raw_log) {
  const res = await fetch(`${API_BASE}/classify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ raw_log }),
  });
  if (!res.ok) {
    throw new Error(`POST /classify failed: ${res.status}`);
  }
  return res.json();
}

/**
 * POST a single raw log line to the HIERARCHICAL multi-service classifier.
 * Returns service + service-specific severity + category + anomaly_score, so the
 * demo surfaces service routing and the cross-service anomaly signal.
 * @param {string} raw_log
 * @returns {Promise<object>} the multi-service classification result
 */
export async function classifyService(raw_log) {
  const res = await fetch(`${API_BASE}/classify/service`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ raw_log }),
  });
  if (!res.ok) {
    throw new Error(`POST /classify/service failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET the registry/A-B model view ({ models, champion, a_version, b_version, split_b }).
 * @returns {Promise<object>}
 */
export async function getModels() {
  const res = await fetch(`${API_BASE}/models`);
  if (!res.ok) {
    throw new Error(`GET /models failed: ${res.status}`);
  }
  return res.json();
}

/**
 * POST a promote: make `version` the champion (registry current + A/B group A).
 * @param {string} version registry version id (e.g. "v2")
 * @returns {Promise<object>} the updated models view
 */
export async function promote(version) {
  const res = await fetch(`${API_BASE}/models/promote`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ version }),
  });
  if (!res.ok) {
    throw new Error(`POST /models/promote failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET the adaptive drift-monitor status ({ recent_accuracy, threshold,
 * retrains_triggered, is_training, ... }).
 * @returns {Promise<object>}
 */
export async function getAdaptiveStatus() {
  const res = await fetch(`${API_BASE}/adaptive/status`);
  if (!res.ok) {
    throw new Error(`GET /adaptive/status failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET the known services + per-service severity classes for the multi-service model.
 * @returns {Promise<object>} ({ services, status, per_service_severity_classes })
 */
export async function getServices() {
  const res = await fetch(`${API_BASE}/services`);
  if (!res.ok) {
    throw new Error(`GET /services failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET the top-N feature importances ({ features: [{name, importance}], model_version }).
 * @param {number} [top=15] number of features to request
 * @returns {Promise<object>}
 */
export async function getFeatureImportance(top = 15) {
  const res = await fetch(
    `${API_BASE}/feature-importance?top=${encodeURIComponent(top)}`,
  );
  if (!res.ok) {
    throw new Error(`GET /feature-importance failed: ${res.status}`);
  }
  return res.json();
}

/**
 * POST to kick off a background retrain (202). Optionally pass a corpus size.
 * @param {number} [count] number of synthetic logs to train on (omit for default)
 * @returns {Promise<object>} the training-status snapshot
 */
export async function train(count) {
  const body =
    count === undefined || count === null ? {} : { count: Number(count) };
  const res = await fetch(`${API_BASE}/train`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  // 202 Accepted is the success status here; treat any non-2xx as an error.
  if (!res.ok) {
    throw new Error(`POST /train failed: ${res.status}`);
  }
  return res.json();
}

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
  "/ws/stream";

/** Internal: GET `path` under the API base and return parsed JSON, or throw. */
async function getJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`GET ${path} failed: ${res.status}`);
  }
  return res.json();
}

/**
 * GET the current aggregate engine stats (the `StatsSnapshot` shape that also
 * rides inside each WS frame's `stats` field). Used once on mount so the cards
 * paint before the first WS tick.
 * @returns {Promise<object>} the stats snapshot
 */
export async function getStats() {
  return getJSON("/stats");
}

/**
 * GET per-cluster summaries for one algorithm (e.g. "dbscan", "hdbscan", "kmeans").
 * @param {string} algo algorithm name
 * @returns {Promise<Array>}
 */
export async function getClusters(algo) {
  return getJSON(`/clusters/${encodeURIComponent(algo)}`);
}

/**
 * GET every discovered pattern (count descending).
 * @returns {Promise<Array>}
 */
export async function getPatterns() {
  return getJSON("/patterns");
}

/**
 * GET the most recent anomaly alerts (newest first), capped at `limit`.
 * @param {number} [limit=50]
 * @returns {Promise<Array>}
 */
export async function getAnomalies(limit = 50) {
  return getJSON(`/anomalies?limit=${encodeURIComponent(limit)}`);
}

/**
 * GET recent buffered points projected to 2-D for one algorithm's scatter plot.
 * @param {string} algo algorithm name
 * @param {number} [limit=500]
 * @returns {Promise<Array>}
 */
export async function getScatter(algo, limit = 500) {
  return getJSON(
    `/scatter/${encodeURIComponent(algo)}?limit=${encodeURIComponent(limit)}`,
  );
}

/**
 * GET the runtime configuration view.
 * @returns {Promise<object>}
 */
export async function getConfig() {
  return getJSON("/config");
}

/**
 * POST a single log for clustering.
 * @param {object} log the log payload (shape per the backend `/cluster` contract)
 * @returns {Promise<object>} the clustering result
 */
export async function postCluster(log) {
  const res = await fetch(`${API_BASE}/cluster`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(log),
  });
  if (!res.ok) {
    throw new Error(`POST /cluster failed: ${res.status}`);
  }
  return res.json();
}

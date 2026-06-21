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
 * POST a single raw log line for classification.
 * (Wired up for the classify form in the next commit; included now so the API
 * surface is complete.)
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

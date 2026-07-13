// Backend REST client for the RCA Analysis Engine dashboard.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to the
//     `backend` service, forwarding the path VERBATIM (no prefix strip — our routes
//     already live under `/api`).
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy. (The live
// feed is a separate WebSocket to the relative `/ws` — see hooks/useWebSocket.js.)

// Abort a request that hangs so a wedged backend can't stall the dashboard; the caller
// treats an abort like any other failure (keeps prior data, flags the error).
const REQUEST_TIMEOUT_MS = 5000;

/**
 * GET a relative `/api/...` path as JSON, aborting after REQUEST_TIMEOUT_MS. Throws on
 * a non-2xx response or a timeout/network error.
 * @param {string} path relative API path (must start with `/api/`)
 * @returns {Promise<any>} the parsed JSON body
 */
async function getJson(path) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(path, { signal: controller.signal });
    if (!res.ok) {
      throw new Error(`GET ${path} failed: ${res.status}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

/**
 * GET /api/incidents — the bounded in-memory incident history, newest-first (the
 * backend already reverses it). Returns an array of IncidentReport objects.
 * @returns {Promise<Array<object>>}
 */
export async function getIncidents() {
  const data = await getJson("/api/incidents");
  return Array.isArray(data) ? data : [];
}

/**
 * GET /api/incidents/{id}/report — the exported post-mortem for one incident
 * (`{incident_id, markdown, recovery_points, classifications}`). Not used by the C11
 * shell; provided for the C12 panels.
 * @param {string} id incident id
 * @returns {Promise<object>}
 */
export async function getIncidentReport(id) {
  return getJson(`/api/incidents/${encodeURIComponent(id)}/report`);
}

/**
 * GET /api/health — the spec-verbatim liveness probe
 * (`{status:"healthy", analyzer_ready:true}`). Handy for a manual connectivity check.
 * @returns {Promise<object>}
 */
export async function getHealth() {
  return getJson("/api/health");
}

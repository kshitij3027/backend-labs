// Backend API client for the Correlation Analysis dashboard.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to the
//     `backend` service, forwarding the path VERBATIM (no prefix strip — our routes
//     already live under `/api/v1`).
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy.

// The single fat endpoint the dashboard polls: everything it needs in one GET.
const DASHBOARD_URL = "/api/v1/dashboard";

// Abort a poll that hangs so a wedged backend can't stall the polling loop; the
// caller treats an abort like any other failure (keeps prior data, flags stale).
const REQUEST_TIMEOUT_MS = 4000;

/**
 * GET /api/v1/dashboard — the whole dashboard payload in one poll: status,
 * stats, timeline, scatter, matrix, recent_correlations, recent_logs, alerts.
 * Aborts after REQUEST_TIMEOUT_MS. Throws on a non-2xx or a timeout/network error.
 * @returns {Promise<object>} the parsed DashboardResponse
 */
export async function getDashboard() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(DASHBOARD_URL, { signal: controller.signal });
    if (!res.ok) {
      throw new Error(`GET ${DASHBOARD_URL} failed: ${res.status}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

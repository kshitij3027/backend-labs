// Backend REST client for the NLP Log Processing Engine dashboard.
//
// All URLs are RELATIVE so the exact same build works in two environments:
//   * production: nginx (see frontend/nginx.conf) reverse-proxies `/api/*` to the
//     `backend` service, forwarding the path VERBATIM (no prefix strip — our routes
//     already live under `/api`).
//   * local dev:  Vite's dev-server proxy (see vite.config.js) does the same.
// Nothing here hardcodes a host, so there is nothing to rewrite per-deploy. (The live feed
// is a separate WebSocket to the relative `/ws` — see hooks/useWebSocket.js.)

// Abort a request that hangs so a wedged backend can't stall the dashboard; the caller
// treats an abort like any other failure (surfaces the error, keeps prior data).
const REQUEST_TIMEOUT_MS = 8000;

/** Try to read FastAPI's `{"detail": ...}` from an error body; "" if there is none. */
async function safeDetail(res) {
  try {
    const body = await res.json();
    if (body && typeof body.detail === "string" && body.detail) {
      return ` — ${body.detail}`;
    }
  } catch {
    /* non-JSON / empty body — nothing to add */
  }
  return "";
}

/**
 * Fetch a relative `/api/...` path as JSON, aborting after REQUEST_TIMEOUT_MS. Throws a
 * useful Error on a timeout, a network failure, or a non-2xx response.
 * @param {string} path relative API path (must start with `/api/`)
 * @param {RequestInit} [options] fetch options (method/body/headers)
 * @returns {Promise<any>} the parsed JSON body
 */
async function request(path, options = {}) {
  const method = options.method || "GET";
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(path, { ...options, signal: controller.signal });
  } catch (err) {
    if (err && err.name === "AbortError") {
      throw new Error(`${method} ${path} timed out after ${REQUEST_TIMEOUT_MS}ms`);
    }
    throw new Error(`${method} ${path} failed: ${err?.message || "network error"}`);
  } finally {
    clearTimeout(timer);
  }

  if (!res.ok) {
    throw new Error(`${method} ${path} failed: ${res.status}${await safeDetail(res)}`);
  }
  return res.json();
}

/** JSON POST helper — sets the content-type and serialises `body`. */
function postJson(path, body) {
  return request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

/**
 * POST /api/analyze — analyze ONE log line.
 * @param {string} message the log line to analyze
 * @returns {Promise<object>} AnalysisResponse:
 *   `{message, entities:[{text,label,start,end}], intent:{label,confidence},
 *     sentiment:{label,score}, keywords:[...]}`
 */
export async function postAnalyze(message) {
  return postJson("/api/analyze", { message });
}

/**
 * POST /api/analyze/batch — analyze MANY log lines in one request (order preserved).
 * @param {string[]} messages the log lines to analyze
 * @returns {Promise<object>} `{results: AnalysisResponse[], count: number}`
 */
export async function postAnalyzeBatch(messages) {
  return postJson("/api/analyze/batch", { messages });
}

/**
 * GET /api/stats — the rolling aggregate snapshot:
 *   `{total_analyzed, intent_distribution, sentiment_distribution,
 *     entity_type_distribution, trending_keywords, recent, throughput_per_sec}`.
 * @returns {Promise<object>}
 */
export async function getStats() {
  return request("/api/stats");
}

/**
 * GET /api/health — the frozen liveness contract `{status, analyzer_ready}`.
 * @returns {Promise<object>}
 */
export async function getHealth() {
  return request("/api/health");
}

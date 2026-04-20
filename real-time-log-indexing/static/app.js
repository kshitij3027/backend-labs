// Real-Time Log Indexing — dashboard client.
//
// Vanilla JS, no build step, no frameworks. Responsibilities:
//   - Poll /api/stats every 1s AND subscribe to /ws for the same
//     payload; both paths drop into applyStats so we never have two
//     formatting implementations to keep in sync.
//   - Poll /health every 1s to drive the status dot.
//   - Debounced search (300 ms) against /api/search with service /
//     level / limit filters. Renders highlighted_message from the
//     server, so <mark> tags flow through unescaped.
//   - Three "Generate N" buttons POST to /api/generate-sample and
//     then kick a stats refresh so the counters move immediately.
//   - Live-feed pane: every `new_document` WS event prepends a small
//     row to #live-feed, capped at 20 so memory stays bounded during
//     long demos.
//
// The WS client reconnects with exponential backoff (1s → 30s max) so
// a restart of the FastAPI process doesn't leave the dashboard stuck
// on a stale socket. We answer `ping` frames with `pong` to keep the
// server's stale-eviction timer from dropping us.

const $ = (sel) => document.querySelector(sel);

function formatBytes(n) {
  if (n === null || n === undefined) return "0 B";
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + " MB";
  return (n / 1024 / 1024 / 1024).toFixed(2) + " GB";
}

function pill(level) {
  const cls = {
    DEBUG: "pill-debug",
    INFO: "pill-info",
    WARN: "pill-warn",
    WARNING: "pill-warn",
    ERROR: "pill-error",
    FATAL: "pill-error",
    CRITICAL: "pill-error",
  }[level] || "pill-info";
  return `<span class="pill ${cls}">${level}</span>`;
}

// HTML-escape arbitrary strings. Used for live-feed messages since
// those come straight off the wire without any server-side sanitizer;
// a producer that XADDs `<script>` should not be able to execute it
// in the dashboard.
function escapeHtml(s) {
  if (s === null || s === undefined) return "";
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

// ---------------------------------------------------------------------
// Stats rendering — one helper, used by both the 1 Hz poll and the
// push-based WS stats_update event. Any change to the formatting
// lands in both paths automatically.
// ---------------------------------------------------------------------

function applyStats(s) {
  if (!s) return;
  $("#stats-docs-indexed").textContent = (s.docs_indexed || 0).toLocaleString();
  $("#stats-current-segment-docs").textContent = (s.current_segment_docs || 0).toLocaleString();
  $("#stats-flushed-memory-segments").textContent = (s.flushed_memory_segments || 0).toLocaleString();
  $("#stats-disk-segments").textContent = (s.disk_segments || 0).toLocaleString();
  $("#stats-memory-bytes").textContent = formatBytes(s.memory_bytes);
  $("#stats-throughput").textContent = (s.throughput_1m || 0).toFixed(1) + " /s";
  $("#stats-vocab-size").textContent = (s.vocab_size || 0).toLocaleString();
  $("#stats-query-p95").textContent = s.query_p95_ms
    ? s.query_p95_ms.toFixed(1) + " ms"
    : "\u2014";
}

async function fetchStats() {
  try {
    const s = await fetch("/api/stats").then((r) => r.json());
    applyStats(s);
  } catch (e) {
    // ignore — tick again in 1s
  }

  try {
    const h = await fetch("/health").then((r) => r.json());
    const dot = $("#status-dot");
    dot.classList.toggle("status-ok", h.status === "ok");
    dot.classList.toggle("status-degraded", h.status !== "ok");
  } catch (e) {
    const dot = $("#status-dot");
    if (dot) {
      dot.classList.remove("status-ok");
      dot.classList.add("status-degraded");
    }
  }
}

// ---------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------

async function runSearch() {
  const q = $("#search-input").value.trim();
  const svc = $("#search-service").value;
  const lvl = $("#search-level").value;
  const limit = $("#search-limit").value || 50;
  if (!q) {
    $("#search-results").innerHTML = "";
    $("#search-meta").textContent = "";
    return;
  }
  const url = new URL("/api/search", window.location.origin);
  url.searchParams.set("q", q);
  if (svc) url.searchParams.set("service", svc);
  if (lvl) url.searchParams.set("level", lvl);
  url.searchParams.set("limit", limit);

  try {
    const res = await fetch(url).then((r) => r.json());
    const total = res.total || 0;
    const took = typeof res.took_ms === "number" ? res.took_ms.toFixed(1) : "0.0";
    $("#search-meta").textContent = `${total} result${total === 1 ? "" : "s"} in ${took} ms`;
    $("#search-results").innerHTML = (res.results || [])
      .map(
        (r) => `
      <li class="result-row">
        <div class="result-meta">
          <span class="pill pill-svc">${r.service}</span>
          ${pill(r.level)}
          <span class="result-ts">${new Date(r.timestamp * 1000).toISOString()}</span>
        </div>
        <div class="result-message">${r.highlighted_message}</div>
      </li>
    `
      )
      .join("");
  } catch (e) {
    $("#search-meta").textContent = "search failed";
    $("#search-results").innerHTML = "";
  }
}

function debounce(fn, ms) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

async function generate(count) {
  const btn = document.activeElement;
  if (btn && btn.tagName === "BUTTON") btn.disabled = true;
  try {
    await fetch("/api/generate-sample", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count }),
    });
  } catch (e) {
    // swallow — stats tick will show whether anything landed
  } finally {
    if (btn && btn.tagName === "BUTTON") btn.disabled = false;
  }
  fetchStats();
}

// ---------------------------------------------------------------------
// Live-feed (new_document events over WS)
// ---------------------------------------------------------------------

const LIVE_FEED_CAP = 20;

function pushFeed(doc) {
  const feed = $("#live-feed");
  if (!feed || !doc) return;
  const li = document.createElement("li");
  li.className = "live-feed-item";
  li.innerHTML = `${pill(doc.level || "INFO")} <span class="pill pill-svc">${escapeHtml(doc.service || "unknown")}</span> <span class="feed-msg">${escapeHtml(doc.message || "")}</span>`;
  feed.insertBefore(li, feed.firstChild);
  while (feed.children.length > LIVE_FEED_CAP) {
    feed.removeChild(feed.lastChild);
  }
}

// ---------------------------------------------------------------------
// WebSocket client — reconnect with exponential backoff, reply to
// pings so the server doesn't evict us as stale.
// ---------------------------------------------------------------------

let ws = null;
let wsReconnectDelay = 1000;
const WS_RECONNECT_MAX_MS = 30000;

function setWsStatus(connected) {
  const el = $("#live-feed-status");
  if (!el) return;
  el.textContent = connected ? "live" : "disconnected";
  el.classList.toggle("ws-connected", !!connected);
}

function openWS() {
  let url;
  try {
    url = new URL("/ws", window.location.href);
    url.protocol = url.protocol.replace("http", "ws");
  } catch (e) {
    return;
  }
  try {
    ws = new WebSocket(url.toString());
  } catch (e) {
    // Browser refused to construct the socket (e.g. mixed-content).
    // Retry later so the rest of the dashboard keeps working.
    setTimeout(openWS, wsReconnectDelay);
    wsReconnectDelay = Math.min(wsReconnectDelay * 2, WS_RECONNECT_MAX_MS);
    return;
  }

  ws.onopen = () => {
    wsReconnectDelay = 1000;
    setWsStatus(true);
  };

  ws.onmessage = (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "new_document") {
      pushFeed(msg.document);
    } else if (msg.type === "stats_update") {
      applyStats(msg.data);
    } else if (msg.type === "ping") {
      try {
        ws.send(JSON.stringify({ type: "pong", t: Date.now() / 1000 }));
      } catch (e) {
        // If send fails, onclose will fire and the reconnect path
        // will pick it up — nothing to do here.
      }
    }
    // `connected` event is acknowledged by the open state itself; no
    // additional handling needed.
  };

  ws.onclose = () => {
    setWsStatus(false);
    setTimeout(openWS, wsReconnectDelay);
    wsReconnectDelay = Math.min(wsReconnectDelay * 2, WS_RECONNECT_MAX_MS);
  };

  ws.onerror = () => {
    // Let onclose handle the reconnect; avoids racing two timers.
  };
}

// ---------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------

function wire() {
  const debounced = debounce(runSearch, 300);
  $("#search-input").addEventListener("input", debounced);
  $("#search-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") runSearch();
  });
  $("#search-service").addEventListener("change", runSearch);
  $("#search-level").addEventListener("change", runSearch);
  $("#search-limit").addEventListener("change", runSearch);
  $("#search-button").addEventListener("click", runSearch);
  $("#generate-500").addEventListener("click", () => generate(500));
  $("#generate-5000").addEventListener("click", () => generate(5000));
  $("#generate-50000").addEventListener("click", () => generate(50000));
}

function start() {
  wire();
  fetchStats();
  // Keep polling as a belt-and-braces path — if WS is blocked by a
  // proxy or the server restarts, the dashboard still updates.
  setInterval(fetchStats, 1000);
  openWS();
}

document.addEventListener("DOMContentLoaded", start);

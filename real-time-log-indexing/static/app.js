// Real-Time Log Indexing — dashboard client.
//
// Vanilla JS, no build step, no frameworks. Responsibilities:
//   - Poll /api/stats every 1s; update the eight stat cards.
//   - Poll /health every 1s to drive the status dot.
//   - Debounced search (300 ms) against /api/search with service /
//     level / limit filters. Renders highlighted_message from the
//     server, so <mark> tags flow through unescaped.
//   - Three "Generate N" buttons POST to /api/generate-sample and
//     then kick a stats refresh so the counters move immediately.
//
// WebSocket live feed ships in Commit 11; for now stats refresh on
// a 1 Hz timer, which is responsive enough for the dashboard flow.

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

async function fetchStats() {
  try {
    const s = await fetch("/api/stats").then((r) => r.json());
    $("#stats-docs-indexed").textContent = s.docs_indexed.toLocaleString();
    $("#stats-current-segment-docs").textContent = s.current_segment_docs.toLocaleString();
    $("#stats-flushed-memory-segments").textContent = s.flushed_memory_segments.toLocaleString();
    $("#stats-disk-segments").textContent = s.disk_segments.toLocaleString();
    $("#stats-memory-bytes").textContent = formatBytes(s.memory_bytes);
    $("#stats-throughput").textContent = s.throughput_1m.toFixed(1) + " /s";
    $("#stats-vocab-size").textContent = s.vocab_size.toLocaleString();
    $("#stats-query-p95").textContent = s.query_p95_ms
      ? s.query_p95_ms.toFixed(1) + " ms"
      : "\u2014";
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
  setInterval(fetchStats, 1000);
}

document.addEventListener("DOMContentLoaded", start);

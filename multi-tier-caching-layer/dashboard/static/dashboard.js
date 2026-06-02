(function () {
  "use strict";

  // How many trailing hit-rate points to chart. The server caps the series at
  // DASHBOARD_POINTS already; this is a client-side ceiling for the line chart.
  const MAX_POINTS = 60;

  // Palette (kept in sync with dashboard.css for a coherent dark theme).
  const COLORS = {
    hitRate: "#34d399", // green
    l1: "#60a5fa",      // blue
    l2: "#a78bfa",      // violet
    l3: "#fbbf24",      // amber
    backend: "#f87171", // red
    cached: "#34d399",  // green
    uncached: "#f87171",// red
    grid: "#2a2f3a",
    tick: "#6b7280",
    legend: "#e8e8ea",
  };

  // ---------------------------------------------------------------- helpers

  /** Set a metric card's text, falling back to an em dash when value is null. */
  function setCard(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value == null ? "—" : value;
  }

  /** Round a number to `digits` decimals, null-safe. */
  function round(n, digits) {
    if (n == null || Number.isNaN(n)) return null;
    const p = Math.pow(10, digits);
    return Math.round(n * p) / p;
  }

  /** Keep only the trailing `n` items of an array (null-safe -> []). */
  function tail(arr, n) {
    if (!Array.isArray(arr)) return [];
    return arr.length > n ? arr.slice(arr.length - n) : arr.slice();
  }

  /** Safely read a nested property chain, returning undefined on any gap. */
  function dig(obj, path) {
    let cur = obj;
    for (let i = 0; i < path.length; i++) {
      if (cur == null) return undefined;
      cur = cur[path[i]];
    }
    return cur;
  }

  // ----------------------------------------------------------------- charts

  /** Build a Chart.js line chart (animation off, responsive fixed-height box). */
  function makeLineChart(canvasId, label, color, yScaleOpts) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label: label,
            data: [],
            borderColor: color,
            backgroundColor: "transparent",
            borderWidth: 2,
            tension: 0.2,
            pointRadius: 0,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true }, grid: { color: COLORS.grid } },
          y: Object.assign(
            { ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
            yScaleOpts || {}
          ),
        },
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });
  }

  /** Build a Chart.js bar chart (animation off, responsive fixed-height box). */
  function makeBarChart(canvasId, labels, colors) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    return new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            data: labels.map(function () { return 0; }),
            backgroundColor: colors,
            borderWidth: 0,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: { ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
          y: { beginAtZero: true, ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
        },
        plugins: { legend: { display: false } },
      },
    });
  }

  // hitRateChart: overall hit rate (%) over time from `series.hit_rate`.
  const hitRateChart = makeLineChart(
    "hitRateChart",
    "Hit rate %",
    COLORS.hitRate,
    { min: 0, max: 100, title: { display: true, text: "%", color: COLORS.tick } }
  );

  // tierChart: per-tier hit/miss counts (l1/l2/l3/backend).
  const tierChart = makeBarChart(
    "tierChart",
    ["l1", "l2", "l3", "backend"],
    [COLORS.l1, COLORS.l2, COLORS.l3, COLORS.backend]
  );

  // latencyChart: cached vs uncached p90 latency (ms).
  const latencyChart = makeBarChart(
    "latencyChart",
    ["cached p90", "uncached p90"],
    [COLORS.cached, COLORS.uncached]
  );

  /** Re-render the hit-rate line from the rolling `series.hit_rate` (0..1). */
  function renderHitRateChart(series) {
    const raw = tail(dig(series, ["hit_rate"]), MAX_POINTS);
    // Server stores fractions (0..1); chart shows percent.
    const pct = raw.map(function (v) { return round((v || 0) * 100, 1); });
    hitRateChart.data.labels = pct.map(function (_, i) { return String(i + 1); });
    hitRateChart.data.datasets[0].data = pct;
    hitRateChart.update("none");
  }

  /** Re-render the per-tier bar from `stats.tiers`. */
  function renderTierChart(stats) {
    const tiers = dig(stats, ["tiers"]) || {};
    const l1 = dig(tiers, ["l1", "hits"]) || 0;
    const l2 = dig(tiers, ["l2", "hits"]) || 0;
    const l3 = dig(tiers, ["l3", "hits"]) || 0;
    const backend = dig(tiers, ["backend", "misses"]) || 0;
    tierChart.data.datasets[0].data = [l1, l2, l3, backend];
    tierChart.update("none");
  }

  /** Re-render the cached-vs-uncached p90 bar from `stats.timing_ms`. */
  function renderLatencyChart(stats) {
    const timing = dig(stats, ["timing_ms"]) || {};
    const cached = round(timing.cached_p90 || 0, 1);
    const uncached = round(timing.uncached_p90 || 0, 1);
    latencyChart.data.datasets[0].data = [cached, uncached];
    latencyChart.update("none");
  }

  // ----------------------------------------------------------------- cards

  /** Patch the metric cards from a single tick payload. */
  function updateCards(stats, degraded) {
    const hitRate = dig(stats, ["performance", "overall_hit_rate"]);
    const totalReq = dig(stats, ["performance", "total_requests"]);
    const l1Mb = dig(stats, ["memory", "l1_mb"]);
    const cachedP90 = dig(stats, ["timing_ms", "cached_p90"]);
    const uncachedP90 = dig(stats, ["timing_ms", "uncached_p90"]);

    setCard("m-hit-rate", hitRate == null ? null : round(hitRate * 100, 1));
    setCard("m-total-requests", totalReq == null ? null : totalReq);
    setCard("m-l1-mb", l1Mb == null ? null : round(l1Mb, 3));
    setCard("m-cached-p90", cachedP90 == null ? null : round(cachedP90, 1));
    setCard("m-uncached-p90", uncachedP90 == null ? null : round(uncachedP90, 1));

    const degEl = document.getElementById("m-degraded");
    if (degEl) {
      degEl.textContent = degraded ? "YES" : "no";
      degEl.className = "value " + (degraded ? "bad" : "ok");
    }
  }

  /** Render the warming-recommendations list into #recommendations. */
  function renderRecommendations(recs) {
    const list = document.getElementById("recommendations");
    if (!list) return;
    list.innerHTML = "";
    if (!Array.isArray(recs) || recs.length === 0) {
      const li = document.createElement("li");
      li.className = "muted";
      li.textContent = "No recommendations yet.";
      list.appendChild(li);
      return;
    }
    recs.forEach(function (rec) {
      const li = document.createElement("li");
      const q = document.createElement("span");
      q.className = "rec-query";
      // Prefer a human-readable query label; fall back to the cache key.
      q.textContent = (rec && (rec.query || rec.key)) || "(unknown)";
      const score = document.createElement("span");
      score.className = "rec-score";
      const s = rec && rec.score != null ? round(rec.score, 3) : null;
      const c = rec && rec.count != null ? rec.count : null;
      score.textContent =
        (s != null ? "score " + s : "") +
        (c != null ? "  ·  ×" + c : "");
      li.appendChild(q);
      li.appendChild(score);
      list.appendChild(li);
    });
  }

  /** Show/hide the degradation banner from `degraded` or `stats.alert`. */
  function updateAlertBanner(degraded, stats) {
    const banner = document.getElementById("alert-banner");
    if (!banner) return;
    const alert = dig(stats, ["alert"]);
    const show = !!degraded || !!alert;
    banner.hidden = !show;
    if (show) {
      const detail = document.getElementById("alert-detail");
      if (detail && alert && alert.reason) {
        detail.textContent =
          "Reason: " + alert.reason +
          " (hit rate " + round((alert.hit_rate || 0) * 100, 1) + "%).";
      }
    }
  }

  // ----------------------------------------------------------------- badge

  /** Update the connection badge. `kind` ∈ {connecting, connected, reconnecting}. */
  function setConnBadge(kind) {
    const badge = document.getElementById("conn-status");
    if (!badge) return;
    if (kind === "connected") {
      badge.textContent = "Connected";
      badge.className = "pill conn-ok";
    } else if (kind === "reconnecting") {
      badge.textContent = "Reconnecting…";
      badge.className = "pill conn-bad";
    } else {
      badge.textContent = "Connecting…";
      badge.className = "pill conn-bad";
    }
  }

  /** Apply one decoded `{type, stats, series, recommendations, degraded}` tick. */
  function applyTick(msg) {
    if (!msg || typeof msg !== "object") return;
    const stats = msg.stats || null;
    const series = msg.series || null;
    const degraded = !!msg.degraded;

    updateCards(stats, degraded);
    renderHitRateChart(series);
    renderTierChart(stats);
    renderLatencyChart(stats);
    renderRecommendations(msg.recommendations);
    updateAlertBanner(degraded, stats);
  }

  // -------------------------------------------------- resilient WS client

  // Derive the WS URL from the page location (ws:// or wss:// to match scheme).
  const wsUrl = (location.protocol === "https:" ? "wss" : "ws") +
    "://" + location.host + "/ws/metrics";

  // Exponential backoff with full jitter: base 500ms, doubling, capped at 10s.
  const BACKOFF_BASE_MS = 500;
  const BACKOFF_CAP_MS = 10000;
  let backoffMs = BACKOFF_BASE_MS;
  let socket = null;
  let reconnectTimer = null;

  function scheduleReconnect() {
    if (reconnectTimer) return; // already pending
    setConnBadge("reconnecting");
    // Full jitter: wait a random duration in [0, backoffMs].
    const delay = Math.random() * backoffMs;
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connect();
    }, delay);
    // Grow the ceiling for the next failure, capped.
    backoffMs = Math.min(backoffMs * 2, BACKOFF_CAP_MS);
  }

  function connect() {
    setConnBadge(backoffMs > BACKOFF_BASE_MS ? "reconnecting" : "connecting");
    try {
      socket = new WebSocket(wsUrl);
    } catch (e) {
      console.error("ws construct failed", e);
      scheduleReconnect();
      return;
    }

    socket.onopen = function () {
      backoffMs = BACKOFF_BASE_MS; // reset backoff on a healthy connection
      setConnBadge("connected");
    };

    socket.onmessage = function (ev) {
      try {
        applyTick(JSON.parse(ev.data));
      } catch (e) {
        console.error("ws message parse failed", e);
      }
    };

    socket.onclose = function () {
      scheduleReconnect();
    };

    socket.onerror = function () {
      // onerror is typically followed by onclose; close defensively so the
      // close handler drives the single reconnect path.
      try { socket.close(); } catch (e) { /* ignore */ }
    };
  }

  // dashboard.js is loaded with `defer`, so the DOM is already parsed; connect
  // immediately. (Guard with DOMContentLoaded too in case of cached eager load.)
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();

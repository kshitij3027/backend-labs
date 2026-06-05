(function () {
  "use strict";

  // Client-side ceiling for the rolling line charts. The server already caps the
  // series at `metrics_history_points`; this trims to the trailing window.
  const MAX_POINTS = 60;

  // Palette (kept in sync with dashboard.css for a coherent dark theme).
  const COLORS = {
    row: "#60a5fa",       // blue
    columnar: "#a78bfa",  // violet
    hybrid: "#fbbf24",    // amber
    completed: "#34d399", // green
    failed: "#f87171",    // red
    storage: "#60a5fa",   // blue
    ratio: "#34d399",     // green
    index: "#a78bfa",     // violet
    hot: "#f87171",       // red
    warm: "#fbbf24",      // amber
    cold: "#60a5fa",      // blue
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

  /** Coerce to a finite number or return a fallback (default 0). */
  function num(v, fallback) {
    const n = Number(v);
    return Number.isFinite(n) ? n : (fallback === undefined ? 0 : fallback);
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

  // Shared option blocks so every chart gets the same dark, animation-free look.
  function axisScales(yOpts) {
    return {
      x: { ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true }, grid: { color: COLORS.grid } },
      y: Object.assign(
        { beginAtZero: true, ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
        yOpts || {}
      ),
    };
  }

  /**
   * Create a chart once and cache it on the canvas via Chart.js's own registry.
   * Re-invocation returns the existing instance (guards against double-init), so
   * `Chart.getChart(canvasId)` is always a stable handle for tests + updates.
   */
  function ensureChart(canvasId, config) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    const existing = Chart.getChart(canvas);
    if (existing) return existing;
    return new Chart(canvas.getContext("2d"), config);
  }

  function initCharts() {
    // formatChart: overall format distribution (row/columnar/hybrid) doughnut.
    ensureChart("formatChart", {
      type: "doughnut",
      data: {
        labels: ["row", "columnar", "hybrid"],
        datasets: [{
          data: [0, 0, 0],
          backgroundColor: [COLORS.row, COLORS.columnar, COLORS.hybrid],
          borderColor: "#1a1d24",
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });

    // tenantChart: stacked bar — one dataset per format across tenant labels.
    ensureChart("tenantChart", {
      type: "bar",
      data: {
        labels: [],
        datasets: [
          { label: "row", data: [], backgroundColor: COLORS.row, stack: "fmt" },
          { label: "columnar", data: [], backgroundColor: COLORS.columnar, stack: "fmt" },
          { label: "hybrid", data: [], backgroundColor: COLORS.hybrid, stack: "fmt" },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: { stacked: true, ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
          y: { stacked: true, beginAtZero: true, ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
        },
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });

    // migrationChart: completed vs failed migrations over time (line).
    ensureChart("migrationChart", {
      type: "line",
      data: {
        labels: [],
        datasets: [
          { label: "completed", data: [], borderColor: COLORS.completed, backgroundColor: "transparent", borderWidth: 2, tension: 0.2, pointRadius: 0 },
          { label: "failed", data: [], borderColor: COLORS.failed, backgroundColor: "transparent", borderWidth: 2, tension: 0.2, pointRadius: 0 },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        scales: axisScales(),
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });

    // latencyByFormatChart: grouped/single bar of per-format p90 latency (ms).
    ensureChart("latencyByFormatChart", {
      type: "bar",
      data: {
        labels: ["row", "columnar", "hybrid"],
        datasets: [{
          label: "p90 ms",
          data: [0, 0, 0],
          backgroundColor: [COLORS.row, COLORS.columnar, COLORS.hybrid],
          borderWidth: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: axisScales({ title: { display: true, text: "ms", color: COLORS.tick } }),
        plugins: { legend: { display: false } },
      },
    });

    // storageChart: compression ratio (left axis) + storage MB (right axis) over time.
    ensureChart("storageChart", {
      type: "line",
      data: {
        labels: [],
        datasets: [
          { label: "compression ×", data: [], borderColor: COLORS.ratio, backgroundColor: "transparent", borderWidth: 2, tension: 0.2, pointRadius: 0, yAxisID: "y" },
          { label: "storage MB", data: [], borderColor: COLORS.storage, backgroundColor: "transparent", borderWidth: 2, tension: 0.2, pointRadius: 0, yAxisID: "y1" },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true }, grid: { color: COLORS.grid } },
          y: { beginAtZero: true, position: "left", ticks: { color: COLORS.tick }, grid: { color: COLORS.grid }, title: { display: true, text: "×", color: COLORS.tick } },
          y1: { beginAtZero: true, position: "right", ticks: { color: COLORS.tick }, grid: { drawOnChartArea: false }, title: { display: true, text: "MB", color: COLORS.tick } },
        },
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });

    // indexChart: count of indexed columns across all partitions (bar).
    ensureChart("indexChart", {
      type: "bar",
      data: {
        labels: ["columns indexed"],
        datasets: [{
          label: "columns indexed",
          data: [0],
          backgroundColor: COLORS.index,
          borderWidth: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: axisScales(),
        plugins: { legend: { display: false } },
      },
    });

    // tierChart: partition counts per storage tier (hot/warm/cold) bar.
    ensureChart("tierChart", {
      type: "bar",
      data: {
        labels: ["hot", "warm", "cold"],
        datasets: [{
          data: [0, 0, 0],
          backgroundColor: [COLORS.hot, COLORS.warm, COLORS.cold],
          borderWidth: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: axisScales(),
        plugins: { legend: { display: false } },
      },
    });
  }

  // ----------------------------------------------------------------- cards

  /** Patch the metric cards from a single tick payload (defensive on gaps). */
  function updateCards(stats) {
    const totalBytes = dig(stats, ["storage", "total_bytes"]);
    const ratio = dig(stats, ["storage", "compression_ratio"]);
    const partitions = dig(stats, ["formats", "partitions_total"]);
    const eps = dig(stats, ["ingest", "entries_per_sec"]);
    const speedup = dig(stats, ["performance", "analytical_speedup_vs_row"]);
    const migCompleted = dig(stats, ["migrations", "completed"]);

    setCard("m-total-storage", totalBytes == null ? null : (num(totalBytes) / 1048576).toFixed(2));
    setCard("m-compression-ratio", ratio == null ? null : num(ratio).toFixed(2));
    setCard("m-active-partitions", partitions == null ? null : num(partitions));
    setCard("m-ingest-eps", eps == null ? null : num(eps).toFixed(1));
    setCard("m-analytical-speedup", speedup == null ? null : num(speedup).toFixed(2));
    setCard("m-migrations-completed", migCompleted == null ? null : num(migCompleted));
  }

  // ----------------------------------------------------------------- renders

  /** formatChart <- stats.formats.distribution {row, columnar, hybrid}. */
  function renderFormatChart(stats) {
    const chart = Chart.getChart("formatChart");
    if (!chart) return;
    const dist = dig(stats, ["formats", "distribution"]) || {};
    chart.data.datasets[0].data = [num(dist.row), num(dist.columnar), num(dist.hybrid)];
    chart.update("none");
  }

  /** tenantChart <- payload.tenants {tenant: {row, columnar, hybrid}} (stacked). */
  function renderTenantChart(tenants) {
    const chart = Chart.getChart("tenantChart");
    if (!chart) return;
    const map = tenants && typeof tenants === "object" ? tenants : {};
    const labels = Object.keys(map).sort();
    chart.data.labels = labels;
    chart.data.datasets[0].data = labels.map(function (t) { return num(dig(map, [t, "row"])); });
    chart.data.datasets[1].data = labels.map(function (t) { return num(dig(map, [t, "columnar"])); });
    chart.data.datasets[2].data = labels.map(function (t) { return num(dig(map, [t, "hybrid"])); });
    chart.update("none");
  }

  // Rolling history for the migration line chart — the tick only carries running
  // totals, so we accumulate points client-side (trimmed to MAX_POINTS).
  const migHistory = { labels: [], completed: [], failed: [] };
  let migTick = 0;

  /** migrationChart <- stats.migrations.completed/failed accumulated over ticks. */
  function renderMigrationChart(stats) {
    const chart = Chart.getChart("migrationChart");
    if (!chart) return;
    migTick += 1;
    migHistory.labels.push(String(migTick));
    migHistory.completed.push(num(dig(stats, ["migrations", "completed"])));
    migHistory.failed.push(num(dig(stats, ["migrations", "failed"])));
    migHistory.labels = tail(migHistory.labels, MAX_POINTS);
    migHistory.completed = tail(migHistory.completed, MAX_POINTS);
    migHistory.failed = tail(migHistory.failed, MAX_POINTS);
    chart.data.labels = migHistory.labels.slice();
    chart.data.datasets[0].data = migHistory.completed.slice();
    chart.data.datasets[1].data = migHistory.failed.slice();
    chart.update("none");
  }

  /** latencyByFormatChart <- stats.performance.by_format.{row,columnar,hybrid}.p90. */
  function renderLatencyByFormatChart(stats) {
    const chart = Chart.getChart("latencyByFormatChart");
    if (!chart) return;
    const bf = dig(stats, ["performance", "by_format"]) || {};
    chart.data.datasets[0].data = [
      num(dig(bf, ["row", "p90"])),
      num(dig(bf, ["columnar", "p90"])),
      num(dig(bf, ["hybrid", "p90"])),
    ];
    chart.update("none");
  }

  /** storageChart <- series.compression_ratio + storage MB derived per point. */
  function renderStorageChart(stats, series) {
    const chart = Chart.getChart("storageChart");
    if (!chart) return;
    const ratios = tail(dig(series, ["compression_ratio"]), MAX_POINTS).map(function (v) { return num(v); });
    chart.data.labels = ratios.map(function (_, i) { return String(i + 1); });
    chart.data.datasets[0].data = ratios;
    // Storage MB is a scalar (current total) — show it as a flat reference line
    // across the same window so the right axis reads the live footprint.
    const storageMb = num(dig(stats, ["storage", "total_bytes"])) / 1048576;
    chart.data.datasets[1].data = ratios.map(function () { return Math.round(storageMb * 100) / 100; });
    chart.update("none");
  }

  /** indexChart <- payload.indexes.columns_indexed. */
  function renderIndexChart(indexes) {
    const chart = Chart.getChart("indexChart");
    if (!chart) return;
    chart.data.datasets[0].data = [num(dig(indexes, ["columns_indexed"]))];
    chart.update("none");
  }

  /** tierChart <- payload.tiers {hot, warm, cold}. */
  function renderTierChart(tiers) {
    const chart = Chart.getChart("tierChart");
    if (!chart) return;
    const t = tiers || {};
    chart.data.datasets[0].data = [num(t.hot), num(t.warm), num(t.cold)];
    chart.update("none");
  }

  /** migration-log <- payload.migrations, most recent first. */
  function renderMigrationLog(migrations) {
    const list = document.getElementById("migration-log");
    if (!list) return;
    list.innerHTML = "";
    if (!Array.isArray(migrations) || migrations.length === 0) {
      const li = document.createElement("li");
      li.className = "muted";
      li.textContent = "No migrations yet.";
      list.appendChild(li);
      return;
    }
    // Render newest first; cap the visible rows so the panel stays bounded.
    migrations.slice().reverse().slice(0, 40).forEach(function (m) {
      const li = document.createElement("li");
      const route = document.createElement("span");
      route.className = "mig-route";
      const tenant = (m && m.tenant != null) ? m.tenant : "?";
      const partition = (m && m.partition != null) ? m.partition : "?";
      const from = (m && m.from != null) ? m.from : "?";
      const to = (m && m.to != null) ? m.to : "?";
      route.textContent = tenant + "/" + partition + ": " + from + "→" + to;
      const reason = document.createElement("span");
      reason.className = "mig-reason";
      reason.textContent = (m && m.reason != null) ? m.reason : "";
      li.appendChild(route);
      li.appendChild(reason);
      list.appendChild(li);
    });
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
      badge.textContent = "Disconnected";
      badge.className = "pill conn-bad";
    }
  }

  /** Apply one decoded tick payload to every card, chart, and the activity log. */
  function applyTick(msg) {
    if (!msg || typeof msg !== "object") return;
    const stats = msg.stats || null;
    const series = msg.series || null;

    updateCards(stats);
    renderFormatChart(stats);
    renderTenantChart(msg.tenants);
    renderMigrationChart(stats);
    renderLatencyByFormatChart(stats);
    renderStorageChart(stats, series);
    renderIndexChart(msg.indexes);
    renderTierChart(msg.tiers);
    renderMigrationLog(msg.migrations);
  }

  // -------------------------------------------------- resilient WS client

  // Derive the WS URL from the page location (ws:// or wss:// to match scheme).
  const wsUrl = (location.protocol === "https:" ? "wss" : "ws") +
    "://" + location.host + "/ws";

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
        const msg = JSON.parse(ev.data);
        if (msg && msg.type === "tick") applyTick(msg);
      } catch (e) {
        console.error("ws message parse failed", e);
      }
    };

    socket.onclose = function () {
      setConnBadge("reconnecting");
      scheduleReconnect();
    };

    socket.onerror = function () {
      // onerror is typically followed by onclose; close defensively so the
      // close handler drives the single reconnect path.
      try { socket.close(); } catch (e) { /* ignore */ }
    };
  }

  function boot() {
    initCharts();   // paint empty charts immediately so the page is readable
    connect();
  }

  // dashboard.js is loaded with `defer`, so the DOM is already parsed; boot
  // immediately. (Guard with DOMContentLoaded too in case of cached eager load.)
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

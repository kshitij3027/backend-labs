(function () {
  "use strict";

  // How many trailing points to chart. Injected by the template; defaults to 20.
  const DASHBOARD_POINTS = window.DASHBOARD_POINTS || 20;

  // Palette (kept in sync with dashboard.css for a coherent dark theme).
  const COLORS = {
    throughput: "#60a5fa", // blue
    batch: "#a78bfa",      // violet
    cpu: "#fbbf24",        // amber
    memory: "#34d399",     // green
    grid: "#2a2f3a",
    tick: "#6b7280",
    legend: "#e8e8ea",
  };

  // ---- Operating-state badge: text + CSS class per OptimizerState value. ----
  // Backend sends lowercase enum values (learning/optimizing/stable/emergency).
  const STATE_META = {
    learning: { label: "LEARNING", cls: "state-learning" },
    optimizing: { label: "OPTIMIZING", cls: "state-optimizing" },
    stable: { label: "STABLE", cls: "state-stable" },
    emergency: { label: "EMERGENCY", cls: "state-emergency" },
  };

  // ---------------------------------------------------------------- helpers

  /** Format an epoch-seconds timestamp to HH:MM:SS (local time). */
  function fmtTime(ts) {
    if (ts == null) return "";
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString([], { hour12: false });
  }

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

  /** Last element of an array, or null when empty/missing. */
  function last(arr) {
    return Array.isArray(arr) && arr.length ? arr[arr.length - 1] : null;
  }

  /** Keep only the trailing `n` items of an array (null-safe -> []). */
  function tail(arr, n) {
    if (!Array.isArray(arr)) return [];
    return arr.length > n ? arr.slice(arr.length - n) : arr.slice();
  }

  // ----------------------------------------------------------------- charts

  /**
   * Build a Chart.js line chart with animation disabled and a fixed-height,
   * responsive container. `datasets` describes label/color/axis bindings;
   * `yScales` configures one or two y-axes.
   */
  function makeChart(canvasId, datasets, yScales) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: datasets.map((d) => ({
          label: d.label,
          data: [],
          borderColor: d.color,
          backgroundColor: "transparent",
          borderWidth: 2,
          tension: 0.2,
          pointRadius: 0,
          yAxisID: d.yAxisID || "y",
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        scales: Object.assign(
          { x: { ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true }, grid: { color: COLORS.grid } } },
          yScales
        ),
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });
  }

  // throughputChart: throughput on the left axis, batch size on the right axis.
  const throughputChart = makeChart(
    "throughputChart",
    [
      { label: "Throughput (rec/s)", color: COLORS.throughput, yAxisID: "y" },
      { label: "Batch size", color: COLORS.batch, yAxisID: "y1" },
    ],
    {
      y: {
        position: "left",
        beginAtZero: true,
        ticks: { color: COLORS.tick },
        grid: { color: COLORS.grid },
        title: { display: true, text: "rec/s", color: COLORS.tick },
      },
      y1: {
        position: "right",
        beginAtZero: true,
        ticks: { color: COLORS.tick },
        grid: { drawOnChartArea: false },
        title: { display: true, text: "batch", color: COLORS.tick },
      },
    }
  );

  // resourceChart: CPU% and Memory% on a shared 0–100 axis.
  const resourceChart = makeChart(
    "resourceChart",
    [
      { label: "CPU %", color: COLORS.cpu, yAxisID: "y" },
      { label: "Memory %", color: COLORS.memory, yAxisID: "y" },
    ],
    {
      y: {
        min: 0,
        max: 100,
        ticks: { color: COLORS.tick },
        grid: { color: COLORS.grid },
        title: { display: true, text: "%", color: COLORS.tick },
      },
    }
  );

  /** Re-render both charts from the rolling `series` payload (null-safe). */
  function renderCharts(series) {
    const s = series || {};
    const labels = tail(s.timestamp, DASHBOARD_POINTS).map(fmtTime);

    throughputChart.data.labels = labels;
    throughputChart.data.datasets[0].data = tail(s.throughput, DASHBOARD_POINTS);
    throughputChart.data.datasets[1].data = tail(s.batch_size, DASHBOARD_POINTS);
    throughputChart.update("none");

    resourceChart.data.labels = labels;
    resourceChart.data.datasets[0].data = tail(s.cpu_percent, DASHBOARD_POINTS);
    resourceChart.data.datasets[1].data = tail(s.memory_percent, DASHBOARD_POINTS);
    resourceChart.update("none");
  }

  // ----------------------------------------------------------------- badges

  /** Update the operating-state badge text + color class. */
  function setStateBadge(state) {
    const badge = document.getElementById("state-badge");
    if (!badge) return;
    const meta = STATE_META[state] || { label: String(state || "—").toUpperCase(), cls: "state-learning" };
    badge.textContent = meta.label;
    badge.className = "pill " + meta.cls;
  }

  /** Update the constraint indicator from `constraint_active` + `reason`. */
  function setConstraintBadge(active, reason) {
    const badge = document.getElementById("constraint-badge");
    if (!badge) return;
    if (active) {
      badge.textContent = reason ? "Constraint: " + reason : "Constraint active";
      badge.className = "pill constraint-on";
    } else {
      badge.textContent = "No constraint";
      badge.className = "pill constraint-ok";
    }
  }

  /** Update the connection badge. `kind` ∈ {connecting, connected, reconnecting}. */
  function setConnBadge(kind) {
    const badge = document.getElementById("conn-badge");
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

  // ------------------------------------------------------------------ cards

  /**
   * Update the metric cards from a single tick. Prefers the explicit
   * `snapshot`; when it is null (before the first processed batch) falls back
   * to the last element of each chart series. Batch size / gradient come from
   * `status` (always present).
   */
  function updateCards(snapshot, status, series) {
    const s = series || {};
    const snap = snapshot || {};

    const throughput = snap.throughput != null ? snap.throughput : last(s.throughput);
    const latency = snap.latency_ms != null ? snap.latency_ms : last(s.latency_ms);
    const cpu = snap.cpu_percent != null ? snap.cpu_percent : last(s.cpu_percent);
    const memory = snap.memory_percent != null ? snap.memory_percent : last(s.memory_percent);
    const queue = snap.queue_depth != null ? snap.queue_depth : last(s.queue_depth);

    // Batch size: trust the optimizer status, fall back to snapshot/series.
    const batch =
      (status && status.batch_size != null) ? status.batch_size
      : (snap.batch_size != null ? snap.batch_size : last(s.batch_size));

    setCard("m-batch", batch);
    setCard("m-throughput", round(throughput, 1));
    setCard("m-latency", round(latency, 1));
    setCard("m-cpu", round(cpu, 1));
    setCard("m-memory", round(memory, 1));
    setCard("m-queue", queue);
    setCard("m-gradient", status ? round(status.last_gradient, 4) : null);
  }

  /** Apply one decoded `{type:"tick", snapshot, status, series}` envelope. */
  function applyTick(msg) {
    if (!msg || typeof msg !== "object") return;
    const status = msg.status || null;
    const series = msg.series || null;

    renderCharts(series);
    updateCards(msg.snapshot || null, status, series);

    if (status) {
      setStateBadge(status.state);
      setConstraintBadge(!!status.constraint_active, status.reason);
    }
  }

  // -------------------------------------------------- resilient WS client

  // Exponential backoff with jitter: base 500ms, doubling, capped at 10s.
  const BACKOFF_BASE_MS = 500;
  const BACKOFF_CAP_MS = 10000;
  let backoffMs = BACKOFF_BASE_MS;
  let socket = null;
  let reconnectTimer = null;

  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return proto + "://" + location.host + "/ws/metrics";
  }

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
      socket = new WebSocket(wsUrl());
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

  document.addEventListener("DOMContentLoaded", function () {
    const note = document.getElementById("points-note");
    if (note) note.textContent = String(DASHBOARD_POINTS);
    connect();
  });
})();

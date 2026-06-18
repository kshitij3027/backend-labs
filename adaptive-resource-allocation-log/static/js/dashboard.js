/*
 * Live dashboard client for the Adaptive Resource Allocation System.
 *
 * Opens a single Socket.IO connection to the SAME ORIGIN (`io()`) and renders the
 * two server-pushed events into the stat cards AND four live Chart.js line charts:
 *
 *   - "status_update"  -> updateStatus(): the full Orchestrator.snapshot() — worker
 *                          pool, current metrics, forecast, last scaling decision,
 *                          cooldown, anomaly, cost and the scaling-history log.
 *   - "metrics_update" -> updateCharts(): current_metrics plus the rolling per-field
 *                          `series` map and `workers_series`, pushed onto the charts.
 *
 * Both events are emitted immediately on connect (so the page paints at once) and
 * then on the server's cadence. There is NO polling — the view is entirely event
 * driven. Every field read is defensive: a missing/null field degrades to the "—"
 * placeholder (cards) or 0 (charts) rather than throwing, so one malformed payload
 * never breaks the page.
 *
 * The interactive controls (manual scale up/down, load injection) `fetch` the
 * SAME-ORIGIN REST endpoints (`/api/scaling`, `/api/load`) — the API and the
 * dashboard are one Flask process on one port, so the browser never needs CORS.
 *
 * Dependency-free vanilla JS apart from two globals from the vendored UMD bundles
 * loaded by index.html BEFORE this file: `Chart` (chart.umd.min.js) and `io`
 * (socket.io.min.js). No build step.
 */
(function () {
  "use strict";

  var DASH = "—"; // em dash placeholder for missing values

  // Rolling window for the live charts (~60 points; old points shift off the front).
  // Matches the server's _SERIES_POINTS so a fresh connect repaints a full window.
  var MAX_POINTS = 60;

  // Palette (kept in sync with style.css for a coherent dark theme).
  var COLORS = {
    util: "#4c8dff", // blue   — effective utilization
    forecast: "#a371f7", // violet — forecast marker line
    cpu: "#2ea043", // green  — cpu
    memory: "#d29922", // amber  — memory
    workers: "#f778ba", // pink   — worker count
    queue: "#3fb950", // green  — queue depth
    latency: "#f0883e", // orange — latency
    grid: "#222a35",
    tick: "#8b97a7",
    legend: "#e6edf3",
  };

  // --- tiny DOM helpers ----------------------------------------------------
  function el(id) {
    return document.getElementById(id);
  }

  function setText(id, value) {
    var node = el(id);
    if (node) {
      node.textContent = value;
    }
  }

  function setStatus(text, state) {
    var node = el("conn-status");
    if (!node) {
      return;
    }
    node.textContent = text;
    node.className = "pill pill--" + state;
  }

  // --- formatters (defensive: non-numbers fall back to the placeholder) ----
  function isNum(v) {
    return typeof v === "number" && isFinite(v);
  }

  /** Coerce to a finite number, else 0 (used for plotting — zeros are fine to chart). */
  function num(v) {
    var n = Number(v);
    return isNum(n) ? n : 0;
  }

  function pct(v) {
    return isNum(v) ? v.toFixed(1) + "%" : DASH;
  }

  function ms(v) {
    return isNum(v) ? v.toFixed(1) + " ms" : DASH;
  }

  function rps(v) {
    return isNum(v) ? v.toFixed(1) + " req/s" : DASH;
  }

  function intOr(v) {
    return isNum(v) ? String(Math.round(v)) : DASH;
  }

  function seconds(v) {
    return isNum(v) ? Math.round(v) + " s" : DASH;
  }

  function str(v) {
    return typeof v === "string" && v.length ? v : DASH;
  }

  /** Format an epoch-seconds (or ISO) timestamp as a HH:MM:SS clock label. */
  function clockLabel(ts) {
    var when;
    if (isNum(ts)) {
      // Epoch seconds (snapshot timestamp is time.time()).
      when = new Date(ts * 1000);
    } else if (typeof ts === "string" && ts) {
      var parsed = Date.parse(ts);
      when = isFinite(parsed) ? new Date(parsed) : new Date();
    } else {
      when = new Date();
    }
    function pad(n) {
      return String(n).padStart(2, "0");
    }
    return pad(when.getHours()) + ":" + pad(when.getMinutes()) + ":" + pad(when.getSeconds());
  }

  // --- safe nested access --------------------------------------------------
  function get(obj, path) {
    var cur = obj;
    for (var i = 0; i < path.length; i++) {
      if (cur == null) {
        return undefined;
      }
      cur = cur[path[i]];
    }
    return cur;
  }

  // --- charts --------------------------------------------------------------
  // Four line charts, created once on boot. Created lazily/guarded so a missing
  // `Chart` global (the vendored bundle failed to load) degrades to a cards-only
  // dashboard instead of throwing.
  var utilChart = null; // effective utilization + forecast marker line
  var cpuMemChart = null; // cpu + memory
  var workersChart = null; // worker count over time (stepped)
  var queueLatencyChart = null; // queue depth + latency

  function lineDataset(label, color, opts) {
    var ds = {
      label: label,
      data: [],
      borderColor: color,
      backgroundColor: "transparent",
      borderWidth: 2,
      tension: 0.2,
      pointRadius: 0,
    };
    if (opts && opts.stepped) {
      ds.stepped = true;
      ds.tension = 0;
    }
    if (opts && opts.dashed) {
      ds.borderDash = [6, 4];
    }
    if (opts && typeof opts.yAxisID === "string") {
      ds.yAxisID = opts.yAxisID;
    }
    return ds;
  }

  function baseLineOptions(yTickFormat) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: {
          ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
          grid: { color: COLORS.grid },
        },
        y: {
          beginAtZero: true,
          ticks: { color: COLORS.tick, callback: yTickFormat },
          grid: { color: COLORS.grid },
        },
      },
      plugins: { legend: { labels: { color: COLORS.legend, boxWidth: 12 } } },
    };
  }

  /** Dual-axis options: left axis `yLeft`, right axis `yRight` (no grid on right). */
  function dualAxisOptions(leftFmt, rightFmt) {
    var o = baseLineOptions(leftFmt);
    o.scales.yLeft = {
      type: "linear",
      position: "left",
      beginAtZero: true,
      ticks: { color: COLORS.tick, callback: leftFmt },
      grid: { color: COLORS.grid },
    };
    o.scales.yRight = {
      type: "linear",
      position: "right",
      beginAtZero: true,
      ticks: { color: COLORS.tick, callback: rightFmt },
      grid: { drawOnChartArea: false },
    };
    delete o.scales.y;
    return o;
  }

  function initCharts() {
    if (typeof Chart === "undefined") {
      // Vendored bundle missing — keep the cards working, skip charts entirely.
      return;
    }

    try {
      var utilCanvas = el("chart-utilization");
      if (utilCanvas) {
        utilChart = new Chart(utilCanvas.getContext("2d"), {
          type: "line",
          data: {
            labels: [],
            datasets: [
              lineDataset("effective utilization", COLORS.util),
              lineDataset("forecast", COLORS.forecast, { dashed: true }),
            ],
          },
          options: baseLineOptions(function (v) {
            return v + "%";
          }),
        });
      }

      var cpuMemCanvas = el("chart-cpu-mem");
      if (cpuMemCanvas) {
        cpuMemChart = new Chart(cpuMemCanvas.getContext("2d"), {
          type: "line",
          data: {
            labels: [],
            datasets: [
              lineDataset("cpu", COLORS.cpu),
              lineDataset("memory", COLORS.memory),
            ],
          },
          options: baseLineOptions(function (v) {
            return v + "%";
          }),
        });
      }

      var workersCanvas = el("chart-workers");
      if (workersCanvas) {
        workersChart = new Chart(workersCanvas.getContext("2d"), {
          type: "line",
          data: {
            labels: [],
            datasets: [lineDataset("workers", COLORS.workers, { stepped: true })],
          },
          options: baseLineOptions(function (v) {
            return v;
          }),
        });
      }

      var qlCanvas = el("chart-queue-latency");
      if (qlCanvas) {
        queueLatencyChart = new Chart(qlCanvas.getContext("2d"), {
          type: "line",
          data: {
            labels: [],
            datasets: [
              lineDataset("queue depth", COLORS.queue, { yAxisID: "yLeft" }),
              lineDataset("latency", COLORS.latency, { yAxisID: "yRight" }),
            ],
          },
          options: dualAxisOptions(
            function (v) {
              return v;
            },
            function (v) {
              return v + "ms";
            }
          ),
        });
      }
    } catch (e) {
      // A bad chart init must not break the cards/controls — log and move on.
      if (window.console) {
        console.error("chart init failed:", e);
      }
    }
  }

  /**
   * Re-seed a chart from a full server series (one redraw). Used by metrics_update,
   * whose payload already carries the trailing rolling window per field — so we
   * mirror it directly rather than appending point-by-point. `series` is an array of
   * arrays, one per dataset; `labels` is the matching label array.
   */
  function setSeries(chart, labels, series) {
    if (!chart) {
      return;
    }
    var n = Math.min(labels.length, MAX_POINTS);
    var trimmedLabels = labels.slice(labels.length - n);
    chart.data.labels = trimmedLabels;
    for (var i = 0; i < chart.data.datasets.length; i++) {
      var src = series[i] || [];
      var pts = src.slice(src.length - n).map(num);
      chart.data.datasets[i].data = pts;
    }
    chart.update("none"); // no animation jank on the tick cadence
  }

  // --- handle metrics_update: repaint the four charts from the series block --
  function updateCharts(m) {
    try {
      if (typeof Chart === "undefined" || !m) {
        return;
      }
      var series = m.series || {};
      var util = (series.effective_utilization || []).map(num);
      var cpu = (series.cpu_percent || []).map(num);
      var mem = (series.memory_percent || []).map(num);
      var queue = (series.queue_depth || []).map(num);
      var latency = (series.latency_ms || []).map(num);
      var workers = (m.workers_series || []).map(num);

      // Build a synthetic rolling label axis (the series carry no timestamps); use
      // the utilization length as the canonical point count, falling back to others.
      var count = Math.max(
        util.length,
        cpu.length,
        mem.length,
        queue.length,
        latency.length,
        workers.length
      );
      var labels = [];
      for (var i = 0; i < count; i++) {
        labels.push("t-" + (count - 1 - i));
      }

      // Forecast marker line: a flat line at the current predicted utilization across
      // the whole window, so the operator can eyeball "where it's heading" vs actual.
      var predicted = num(get(latestStatus, ["forecast", "predicted"]));
      var forecastLine = [];
      for (var j = 0; j < util.length; j++) {
        forecastLine.push(predicted);
      }

      setSeries(utilChart, labels, [util, forecastLine]);
      setSeries(cpuMemChart, labels, [cpu, mem]);
      setSeries(workersChart, labels, [workers]);
      setSeries(queueLatencyChart, labels, [queue, latency]);
    } catch (e) {
      if (window.console) {
        console.error("updateCharts failed:", e);
      }
    }
  }

  // --- render the scaling-history log --------------------------------------
  function renderHistory(history) {
    var list = el("scaling-history");
    if (!list) {
      return;
    }
    list.innerHTML = "";
    if (!Array.isArray(history) || history.length === 0) {
      var empty = document.createElement("li");
      empty.className = "history-empty";
      empty.textContent = "No scaling actions yet.";
      list.appendChild(empty);
      return;
    }
    // Newest first, capped so the DOM stays small.
    var rows = history.slice(-20).reverse();
    for (var i = 0; i < rows.length; i++) {
      var d = rows[i] || {};
      var li = document.createElement("li");
      li.className = "history-row";

      var action = str(d.action);
      var tag = document.createElement("span");
      tag.className = "history-action history-action--" + action.replace(/[^a-z_]/gi, "");
      tag.textContent = action;

      var detail = document.createElement("span");
      detail.className = "history-detail";
      var from = intOr(d.from_workers);
      var to = intOr(d.to_workers);
      var reason = str(d.reason);
      var trig = str(d.trigger_metric);
      detail.textContent =
        from + " → " + to + " workers · " + reason + " (" + trig + ")";

      li.appendChild(tag);
      li.appendChild(detail);
      list.appendChild(li);
    }
  }

  // --- handle status_update: paint every stat card -------------------------
  // The most recent snapshot is cached so updateCharts() can read the forecast for
  // the marker line even when only a metrics_update arrived.
  var latestStatus = {};

  function updateStatus(s) {
    try {
      if (!s || typeof s !== "object") {
        return;
      }
      latestStatus = s;

      var metrics = s.current_metrics || {};
      var forecast = s.forecast || {};
      var workers = s.workers || {};
      var decision = s.last_decision || {};
      var anomaly = s.anomaly || {};

      // Worker pool: current value + min/max/backend sub-text.
      setText("stat-workers", intOr(workers.current));
      setText(
        "stat-workers-sub",
        "min " + intOr(workers.min) + " · max " + intOr(workers.max) + " · " + str(workers.backend)
      );

      // Current metrics.
      setText("stat-utilization", pct(metrics.effective_utilization));
      setText("stat-cpu", pct(metrics.cpu_percent));
      setText("stat-memory", pct(metrics.memory_percent));
      setText("stat-queue", intOr(metrics.queue_depth));
      setText("stat-latency", ms(metrics.latency_ms));
      setText("stat-arrival", rps(metrics.arrival_rate));

      // Forecast: predicted % + confidence + trend (+ horizon sub-text).
      setText("stat-forecast", pct(forecast.predicted));
      var conf = forecast.confidence;
      var confText = isNum(conf) ? (conf * 100).toFixed(0) + "% conf" : DASH;
      setText(
        "stat-forecast-sub",
        str(forecast.trend) +
          " · " +
          confText +
          " · " +
          (isNum(forecast.horizon_minutes) ? forecast.horizon_minutes + "m horizon" : DASH)
      );

      // Last scaling decision: action + reason (+ worker delta sub-text).
      setText("stat-decision", str(decision.action));
      var fromW = decision.from_workers;
      var toW = decision.to_workers;
      var deltaText =
        isNum(fromW) && isNum(toW) ? intOr(fromW) + " → " + intOr(toW) + " workers" : "";
      setText("stat-decision-sub", str(decision.reason) + (deltaText ? " · " + deltaText : ""));

      // Cooldown remaining.
      setText("stat-cooldown", seconds(s.cooldown_remaining_s));

      // Anomaly: active flag + zscore.
      var active = anomaly.active === true;
      setText("stat-anomaly", active ? "ANOMALY" : "normal");
      setText(
        "stat-anomaly-sub",
        "z = " + (isNum(anomaly.zscore) ? anomaly.zscore.toFixed(2) : DASH)
      );
      var anomalyCard = el("card-anomaly");
      if (anomalyCard) {
        anomalyCard.classList.toggle("card--alert", active);
      }

      // Cost (placeholder block in early commits — show a friendly summary or —).
      var cost = s.cost || {};
      var costVal =
        cost && typeof cost === "object" && Object.keys(cost).length
          ? isNum(cost.hourly)
            ? "$" + cost.hourly.toFixed(2) + "/h"
            : JSON.stringify(cost)
          : DASH;
      setText("stat-cost", costVal);

      // Scaling-history log.
      renderHistory(s.scaling_history);
    } catch (e) {
      if (window.console) {
        console.error("updateStatus failed:", e);
      }
    }
  }

  // --- interactive controls (same-origin fetch, never throws) --------------

  /** Set a result element's text + tone class ("muted" | "ok" | "err"). */
  function showResult(id, tone, text) {
    var node = el(id);
    if (!node) {
      return;
    }
    node.className = "result " + tone;
    node.textContent = text;
  }

  /**
   * POST JSON to a same-origin endpoint and hand the parsed body to onOk. Any non-2xx
   * response or network failure is shown via showResult(resultId, "err", …) — this
   * never rejects/throws out of the handler.
   */
  function postApi(path, payload, resultId, onOk) {
    showResult(resultId, "muted", "…");
    fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (resp) {
        return resp
          .json()
          .catch(function () {
            return null;
          })
          .then(function (body) {
            return { ok: resp.ok, status: resp.status, body: body };
          });
      })
      .then(function (r) {
        if (!r.ok) {
          var detail =
            r.body && r.body.error
              ? typeof r.body.error === "string"
                ? r.body.error
                : JSON.stringify(r.body.error)
              : "request failed (HTTP " + r.status + ")";
          showResult(resultId, "err", detail);
          return;
        }
        try {
          onOk(r.body || {});
        } catch (e) {
          showResult(resultId, "err", "render error: " + e);
        }
      })
      .catch(function (err) {
        showResult(resultId, "err", "network error: " + err);
      });
  }

  /** Read a number input by id; returns null when blank or non-numeric. */
  function inputNum(id) {
    var node = el(id);
    if (!node || node.value === "" || node.value == null) {
      return null;
    }
    var n = Number(node.value);
    return isNum(n) ? n : null;
  }

  function wireControls() {
    var upBtn = el("btn-scale-up");
    if (upBtn) {
      upBtn.addEventListener("click", function () {
        postApi("/api/scaling", { direction: "up" }, "controls-result", function (body) {
          showResult(
            "controls-result",
            "ok",
            "scaled up: " + intOr(body.from_workers) + " → " + intOr(body.to_workers) + " workers"
          );
        });
      });
    }

    var downBtn = el("btn-scale-down");
    if (downBtn) {
      downBtn.addEventListener("click", function () {
        postApi("/api/scaling", { direction: "down" }, "controls-result", function (body) {
          showResult(
            "controls-result",
            "ok",
            "scaled down: " + intOr(body.from_workers) + " → " + intOr(body.to_workers) + " workers"
          );
        });
      });
    }

    var injectBtn = el("btn-inject-load");
    if (injectBtn) {
      injectBtn.addEventListener("click", function () {
        var rate = inputNum("load-rate");
        var secs = inputNum("load-seconds");
        if (rate == null || rate < 0) {
          showResult("controls-result", "err", "enter an arrival rate ≥ 0");
          return;
        }
        var payload = { arrival_rate: rate };
        if (secs != null) {
          payload.ramp_seconds = secs;
        }
        postApi("/api/load", payload, "controls-result", function (body) {
          showResult(
            "controls-result",
            "ok",
            "ramping to " +
              num(body.target_arrival_rate).toFixed(0) +
              " req/s over " +
              num(body.ramp_seconds).toFixed(0) +
              "s"
          );
        });
      });
    }
  }

  // --- Socket.IO lifecycle (same-origin; the client handles reconnect) -----
  function connect() {
    if (typeof io === "undefined") {
      setStatus("socket.io missing", "disconnected");
      if (window.console) {
        console.error("Socket.IO client not loaded — live updates disabled.");
      }
      return;
    }

    setStatus("connecting…", "connecting");
    var socket = io(); // same-origin; default transports (polling -> websocket)

    socket.on("connect", function () {
      setStatus("connected", "connected");
    });

    socket.on("disconnect", function () {
      setStatus("disconnected", "disconnected");
    });

    socket.on("connect_error", function () {
      setStatus("connection error", "disconnected");
    });

    socket.on("reconnect_attempt", function () {
      setStatus("reconnecting…", "connecting");
    });

    socket.on("status_update", updateStatus);
    socket.on("metrics_update", updateCharts);
  }

  // Boot once the DOM is parsed: create the charts, wire the controls, then open the
  // socket. The script tag uses `defer`, so the DOM is already parsed by the time this
  // runs, but the guard keeps it robust to eager/synchronous loads too.
  function boot() {
    initCharts();
    wireControls();
    connect();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

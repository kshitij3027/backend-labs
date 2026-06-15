/*
 * Live dashboard client for the delta-encoding log engine.
 *
 * Opens a single WebSocket to /ws and renders each "tick" the server pushes into the
 * stat cards AND two live Chart.js line charts. There is no polling for the live view:
 * the cards/charts are entirely tick-driven. On socket close it reconnects with a short
 * capped backoff. Every field read is defensive (the tick may carry stats=null on a
 * server-side error, or omit a key), so a malformed tick degrades to a placeholder
 * rather than throwing.
 *
 * The interactive controls (generate / compress / reconstruct / random-access / reset)
 * `fetch` the SAME-ORIGIN REST endpoints — the API and the dashboard are one process on
 * one port, so the browser never needs CORS. Each action shows its own JSON response in
 * a result element and lets the next WS tick refresh the cards + charts. A failed fetch
 * is caught and shown as text; it never throws.
 *
 * Dependency-free vanilla JS apart from the global `Chart` from the vendored UMD bundle
 * (loaded by index.html before this file). No build step.
 */
(function () {
  "use strict";

  var DASH = "—"; // em dash placeholder for missing values

  // Rolling window for the live charts (~30 points; old points shift off the front).
  var MAX_POINTS = 30;

  // Palette (kept in sync with dashboard.css for a coherent dark theme).
  var COLORS = {
    delta: "#4c8dff",        // blue   — delta-only reduction
    gzipRaw: "#d29922",      // amber  — gzip(raw) reduction
    deltaGzip: "#2ea043",    // green  — delta + gzip reduction
    latency: "#a371f7",      // violet — reconstruct p99
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

  function eps(v) {
    return isNum(v) ? v.toFixed(2) + " eps" : DASH;
  }

  function ms(v) {
    return isNum(v) ? v.toFixed(3) + " ms" : DASH;
  }

  function intOr(v) {
    return isNum(v) ? String(Math.round(v)) : DASH;
  }

  function seconds(v) {
    return isNum(v) ? v.toFixed(1) + " s" : DASH;
  }

  function rate(v) {
    // hit_rate is a 0..1 fraction; show as a percentage.
    return isNum(v) ? (v * 100).toFixed(1) + "%" : DASH;
  }

  /** Format an epoch-seconds tick as a HH:MM:SS clock label. */
  function clockLabel(ts) {
    var when = new Date(num(ts) * 1000);
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
  // Two line charts, created once on DOMContentLoaded. The reduction chart carries
  // three datasets (delta / gzip-raw / delta+gzip); the latency chart carries one
  // (reconstruct p99). Both share one rolling label axis. Created lazily/guarded so a
  // missing `Chart` global (e.g. the vendored bundle failed to load) degrades to the
  // cards-only dashboard instead of throwing.
  var reductionChart = null;
  var latencyChart = null;

  function lineDataset(label, color) {
    return {
      label: label,
      data: [],
      borderColor: color,
      backgroundColor: "transparent",
      borderWidth: 2,
      tension: 0.2,
      pointRadius: 0,
    };
  }

  function baseLineOptions(yTickFormat) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: {
          ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true },
          grid: { color: COLORS.grid },
        },
        y: {
          beginAtZero: true,
          ticks: { color: COLORS.tick, callback: yTickFormat },
          grid: { color: COLORS.grid },
        },
      },
      plugins: { legend: { labels: { color: COLORS.legend } } },
    };
  }

  function initCharts() {
    if (typeof Chart === "undefined") {
      // Vendored bundle missing — keep the cards working, skip charts entirely.
      return;
    }

    var reductionCanvas = el("chart-reduction");
    if (reductionCanvas) {
      reductionChart = new Chart(reductionCanvas.getContext("2d"), {
        type: "line",
        data: {
          labels: [],
          datasets: [
            lineDataset("delta", COLORS.delta),
            lineDataset("gzip(raw)", COLORS.gzipRaw),
            lineDataset("delta + gzip", COLORS.deltaGzip),
          ],
        },
        options: baseLineOptions(function (v) {
          return v + "%";
        }),
      });
    }

    var latencyCanvas = el("chart-latency");
    if (latencyCanvas) {
      latencyChart = new Chart(latencyCanvas.getContext("2d"), {
        type: "line",
        data: {
          labels: [],
          datasets: [lineDataset("reconstruct p99", COLORS.latency)],
        },
        options: baseLineOptions(function (v) {
          return v + " ms";
        }),
      });
    }
  }

  /** Push one point onto a chart, trimming the rolling window, then redraw. */
  function pushPoint(chart, label, values) {
    if (!chart) {
      return;
    }
    chart.data.labels.push(label);
    while (chart.data.labels.length > MAX_POINTS) {
      chart.data.labels.shift();
    }
    for (var i = 0; i < chart.data.datasets.length; i++) {
      chart.data.datasets[i].data.push(num(values[i]));
      while (chart.data.datasets[i].data.length > MAX_POINTS) {
        chart.data.datasets[i].data.shift();
      }
    }
    chart.update("none"); // no animation jank on the tick cadence
  }

  function updateCharts(storage, performance, ts) {
    var label = clockLabel(ts);
    // Before any compression the reductions are 0 — that is fine to plot.
    pushPoint(reductionChart, label, [
      num(storage.delta_reduction),
      num(storage.gzip_raw_reduction),
      num(storage.delta_plus_gzip_reduction),
    ]);
    pushPoint(latencyChart, label, [num(performance.reconstruct_p99_ms)]);
  }

  // --- render one tick into the cards + charts -----------------------------
  function render(tick) {
    if (!tick || tick.type !== "tick") {
      return;
    }

    // Server-side stats build failed: surface it on the status pill, leave cards as-is.
    if (tick.error) {
      setStatus("error: " + tick.error, "error");
      return;
    }

    setStatus("live", "live");

    var stats = tick.stats || {};
    var storage = stats.storage || {};
    var performance = stats.performance || {};
    var system = stats.system || {};
    var cache = performance.cache || {};

    // Storage section.
    setText("stat-delta-reduction", pct(storage.delta_reduction));
    setText("stat-gzip-raw-reduction", pct(storage.gzip_raw_reduction));
    setText("stat-delta-plus-gzip-reduction", pct(storage.delta_plus_gzip_reduction));
    setText("stat-count", intOr(storage.count));
    setText("stat-keyframes", intOr(storage.keyframe_count));

    // Performance section.
    setText("stat-throughput", eps(performance.compress_throughput_eps));
    setText("stat-reconstruct-p99", ms(performance.reconstruct_p99_ms));
    setText("stat-cache-hitrate", rate(cache.hit_rate));

    // System section. errors lives on both system and performance; prefer system.
    var errors = isNum(system.errors) ? system.errors : performance.errors;
    setText("stat-errors", intOr(errors));
    setText("stat-uptime", seconds(get(system, ["uptime_seconds"])));

    // Live charts: push this tick's reduction trio + reconstruct p99.
    updateCharts(storage, performance, tick.ts);
  }

  // --- interactive controls (same-origin fetch, never throws) --------------

  /** Set a result span's text + tone class ("muted" | "ok" | "err"). */
  function showResult(id, tone, text) {
    var node = el(id);
    if (!node) {
      return;
    }
    node.className = "result " + tone;
    node.textContent = text;
  }

  /**
   * fetch JSON from a same-origin endpoint and hand the parsed body to onOk.
   * Any non-2xx response or network failure is shown via showResult(resultId, "err", …)
   * — this never rejects/throws out of the handler.
   *
   *   method   : "GET" | "POST"
   *   path     : same-origin URL (e.g. "/api/compress")
   *   payload  : object to JSON-encode as the POST body, or null for GET / no body
   *   resultId : element id used for the "…" / error toast
   *   onOk     : function(body) called with the parsed JSON on a 2xx response
   */
  function callApi(method, path, payload, resultId, onOk) {
    showResult(resultId, "muted", "…");
    var opts = { method: method };
    if (payload != null) {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(payload);
    }
    fetch(path, opts)
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
            r.body && r.body.detail
              ? typeof r.body.detail === "string"
                ? r.body.detail
                : JSON.stringify(r.body.detail)
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
    var genBtn = el("btn-generate");
    if (genBtn) {
      genBtn.addEventListener("click", function () {
        var count = inputNum("gen-count");
        var seed = inputNum("gen-seed");
        var payload = { count: count != null ? count : 1000 };
        if (seed != null) {
          payload.seed = seed;
        }
        callApi("POST", "/api/generate", payload, "generate-result", function (body) {
          showResult(
            "generate-result",
            "ok",
            "generated " + num(body.count) + " entries"
          );
        });
      });
    }

    var compressBtn = el("btn-compress");
    if (compressBtn) {
      compressBtn.addEventListener("click", function () {
        callApi(
          "POST",
          "/api/compress",
          { use_generated: true },
          "compress-result",
          function (body) {
            showResult(
              "compress-result",
              "ok",
              "delta reduction " +
                num(body.delta_reduction).toFixed(2) +
                "% over " +
                num(body.count) +
                " entries"
            );
          }
        );
      });
    }

    var reconBtn = el("btn-reconstruct");
    if (reconBtn) {
      reconBtn.addEventListener("click", function () {
        callApi(
          "POST",
          "/api/reconstruct",
          { verify: true },
          "reconstruct-result",
          function (body) {
            var ok = body.fidelity_ok === true;
            showResult(
              "reconstruct-result",
              ok ? "ok" : "err",
              "fidelity " +
                (ok ? "OK" : "MISMATCH") +
                " over " +
                num(body.count) +
                " entries"
            );
          }
        );
      });
    }

    var logsBtn = el("btn-logs");
    if (logsBtn) {
      logsBtn.addEventListener("click", function () {
        var index = inputNum("logs-index");
        if (index == null) {
          showResult("logs-status", "err", "enter an index");
          return;
        }
        callApi(
          "GET",
          "/api/logs/" + encodeURIComponent(index),
          null,
          "logs-status",
          function (body) {
            showResult(
              "logs-status",
              "ok",
              "index " +
                num(body.index) +
                " · nearest keyframe " +
                num(body.nearest_keyframe_index)
            );
            var out = el("logs-result");
            if (out) {
              out.textContent = JSON.stringify(body.entry, null, 2);
            }
          }
        );
      });
    }

    var resetBtn = el("btn-reset");
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        callApi("POST", "/api/reset", {}, "reset-result", function (body) {
          showResult("reset-result", "ok", body.status || "reset");
          var out = el("logs-result");
          if (out) {
            out.textContent = "";
          }
        });
      });
    }
  }

  // --- WebSocket lifecycle with capped backoff -----------------------------
  var ws = null;
  var backoff = 500; // ms, doubles on each failed attempt up to the cap
  var BACKOFF_MAX = 8000;

  function connect() {
    var scheme = location.protocol === "https:" ? "wss" : "ws";
    var url = scheme + "://" + location.host + "/ws";
    try {
      ws = new WebSocket(url);
    } catch (e) {
      scheduleReconnect();
      return;
    }

    ws.onopen = function () {
      backoff = 500; // reset the backoff once a connection succeeds
      setStatus("live", "live");
    };

    ws.onmessage = function (event) {
      var tick;
      try {
        tick = JSON.parse(event.data);
      } catch (e) {
        return; // ignore an unparseable frame
      }
      render(tick);
    };

    ws.onerror = function () {
      // onclose fires next and drives the reconnect; just reflect the state here.
      setStatus("disconnected", "error");
    };

    ws.onclose = function () {
      setStatus("reconnecting…", "connecting");
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    var delay = backoff;
    backoff = Math.min(backoff * 2, BACKOFF_MAX);
    setTimeout(connect, delay);
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

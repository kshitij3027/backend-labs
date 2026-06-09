/* Bloom Filter Log Membership — dashboard client.
 *
 * Vanilla JS, no build step. Consumes the /ws tick contract produced by
 * src/dashboard.py:
 *
 *   { type: "tick", ts, refresh_ms,
 *     api:      <GET /stats>          | null,
 *     pipeline: <GET /pipeline/stats> | null,
 *     sessions: <GET /sessions/stats> | null,
 *     error:    null | "<reason everything above is null>" }
 *
 * Renders: 4 per-filter stat cards, an FP-rate-over-time line chart (rolling
 * 60-point window accumulated client-side), a memory-per-filter bar chart,
 * and the two-tier pipeline strip. The add / query / session forms POST to
 * this same origin's /proxy/* routes, so the browser never needs CORS.
 */
(function () {
  "use strict";

  // The four managed filters, in card/chart order. Must match the ids baked
  // into index.html (card-<name>-*) and the names in tick.api.filters.
  const FILTERS = ["error_logs", "access_logs", "security_logs", "sessions"];

  // Rolling window for the FP line chart (~5 minutes at the 5s default tick).
  const MAX_POINTS = 60;

  // Palette (kept in sync with dashboard.css for a coherent dark theme).
  const COLORS = {
    error_logs: "#f87171",    // red
    access_logs: "#60a5fa",   // blue
    security_logs: "#a78bfa", // violet
    sessions: "#34d399",      // green
    grid: "#2a2f3a",
    tick: "#6b7280",
    legend: "#e8e8ea",
  };

  // ---------------------------------------------------------------- helpers

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value == null ? "—" : value;
  }

  /** Coerce to a finite number, else 0. */
  function num(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  /** Format a 0..1 rate as a compact percentage (FP rates are tiny). */
  function fmtPct(v) {
    const n = num(v) * 100;
    if (n === 0) return "0%";
    if (n >= 1) return n.toFixed(2) + "%";
    return n.toPrecision(2) + "%";
  }

  /** Keep only the trailing MAX_POINTS items (in place). */
  function trim(arr) {
    while (arr.length > MAX_POINTS) arr.shift();
  }

  // ----------------------------------------------------------------- charts

  // Client-side rolling history for the FP chart: the tick carries only the
  // current estimate per filter, so points accumulate here between ticks.
  const fpHistory = { labels: [], series: {} };
  FILTERS.forEach(function (f) { fpHistory.series[f] = []; });

  function initCharts() {
    // fp-chart: estimated FP rate over time, one line per filter. Linear y
    // axis (the values are small fractions); ticks formatted as percentages.
    new Chart(document.getElementById("fp-chart").getContext("2d"), {
      type: "line",
      data: {
        labels: [],
        datasets: FILTERS.map(function (f) {
          return {
            label: f,
            data: [],
            borderColor: COLORS[f],
            backgroundColor: "transparent",
            borderWidth: 2,
            tension: 0.2,
            pointRadius: 0,
          };
        }),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { ticks: { color: COLORS.tick, maxRotation: 0, autoSkip: true }, grid: { color: COLORS.grid } },
          y: {
            beginAtZero: true,
            ticks: { color: COLORS.tick, callback: function (v) { return fmtPct(v); } },
            grid: { color: COLORS.grid },
          },
        },
        plugins: { legend: { labels: { color: COLORS.legend } } },
      },
    });

    // mem-chart: current memory footprint (MB) per filter, one bar each.
    new Chart(document.getElementById("mem-chart").getContext("2d"), {
      type: "bar",
      data: {
        labels: FILTERS.slice(),
        datasets: [{
          label: "memory MB",
          data: FILTERS.map(function () { return 0; }),
          backgroundColor: FILTERS.map(function (f) { return COLORS[f]; }),
          borderWidth: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: { ticks: { color: COLORS.tick }, grid: { color: COLORS.grid } },
          y: {
            beginAtZero: true,
            ticks: { color: COLORS.tick },
            grid: { color: COLORS.grid },
            title: { display: true, text: "MB", color: COLORS.tick },
          },
        },
        plugins: { legend: { display: false } },
      },
    });
  }

  function renderFpChart(filters, ts) {
    const chart = Chart.getChart("fp-chart");
    if (!chart) return;
    const when = new Date(num(ts) * 1000);
    fpHistory.labels.push(
      String(when.getHours()).padStart(2, "0") + ":" +
      String(when.getMinutes()).padStart(2, "0") + ":" +
      String(when.getSeconds()).padStart(2, "0")
    );
    trim(fpHistory.labels);
    FILTERS.forEach(function (f, i) {
      const block = filters[f] || {};
      fpHistory.series[f].push(num(block.estimated_fp_rate));
      trim(fpHistory.series[f]);
      chart.data.datasets[i].data = fpHistory.series[f].slice();
    });
    chart.data.labels = fpHistory.labels.slice();
    chart.update("none"); // no animation jank on a 5s cadence
  }

  function renderMemChart(filters) {
    const chart = Chart.getChart("mem-chart");
    if (!chart) return;
    chart.data.datasets[0].data = FILTERS.map(function (f) {
      return num((filters[f] || {}).memory_mb);
    });
    chart.update("none");
  }

  // ------------------------------------------------------------------ cards

  /** Patch one filter card's sub-fields from its /stats block. */
  function renderCard(name, block) {
    const b = block || {};
    setText("card-" + name + "-elements", num(b.elements_added).toLocaleString());
    setText("card-" + name + "-adds", num(b.adds_total).toLocaleString());
    setText("card-" + name + "-queries", num(b.queries_total).toLocaleString());
    setText("card-" + name + "-avgq", num(b.avg_query_ms).toFixed(3) + " ms");
    setText("card-" + name + "-mem", num(b.memory_mb).toFixed(2) + " MB");
    setText("card-" + name + "-fp", fmtPct(b.estimated_fp_rate) + " / " + fmtPct(b.target_fp_rate));
    setText("card-" + name + "-slices", num(b.slice_count));
    setText("card-" + name + "-rot", num(b.rotations));
  }

  // -------------------------------------------------------- pipeline strip

  /** One chip per filter: storage_skipped_pct + observed FP, plus a totals chip. */
  function renderPipelineStrip(pipeline) {
    const strip = document.getElementById("pipeline-strip");
    if (!strip || !pipeline || typeof pipeline !== "object") return;
    strip.innerHTML = "";

    function chip(label, block) {
      const span = document.createElement("span");
      span.className = "chip";
      const name = document.createElement("span");
      name.className = "name";
      name.textContent = label;
      span.appendChild(name);
      const skipped = num(block.storage_skipped_pct).toFixed(1);
      let text = " · storage skipped " + skipped + "% · obs FP " + fmtPct(block.observed_fp_rate);
      span.appendChild(document.createTextNode(text));
      if (block.fallback_active) {
        const warn = document.createElement("span");
        warn.className = "warn";
        warn.textContent = " · FALLBACK";
        span.appendChild(warn);
      }
      return span;
    }

    if (pipeline._totals) strip.appendChild(chip("all", pipeline._totals));
    FILTERS.forEach(function (f) {
      if (pipeline[f]) strip.appendChild(chip(f, pipeline[f]));
    });
  }

  // ------------------------------------------------------------------ badge

  /** kind ∈ {connecting, connected, reconnecting, api-error}; title = detail. */
  function setPill(kind, title) {
    const pill = document.getElementById("conn-pill");
    if (!pill) return;
    pill.title = title || "";
    if (kind === "connected") {
      pill.textContent = "Connected";
      pill.className = "pill conn-ok";
    } else if (kind === "api-error") {
      // The WS is up (we ARE receiving ticks) but the membership API is not.
      pill.textContent = "API unreachable";
      pill.className = "pill conn-warn";
    } else if (kind === "reconnecting") {
      pill.textContent = "Reconnecting…";
      pill.className = "pill conn-bad";
    } else {
      pill.textContent = "Connecting…";
      pill.className = "pill conn-bad";
    }
  }

  // ------------------------------------------------------------------- tick

  function applyTick(msg) {
    if (!msg || typeof msg !== "object") return;

    // Live cadence labels (header pill + footer sentence).
    if (msg.refresh_ms != null) {
      const seconds = Math.round(num(msg.refresh_ms) / 100) / 10;
      setText("refresh-label", "every " + seconds + "s");
      setText("footer-refresh", seconds);
    }

    // Error-shaped tick: API down. Surface the reason in the pill (text +
    // hover title) and keep the last-painted numbers on screen.
    if (!msg.api) {
      setPill("api-error", msg.error || "stats fetch failed");
      return;
    }
    setPill("connected", "");

    const filters = msg.api.filters || {};
    FILTERS.forEach(function (f) { renderCard(f, filters[f]); });
    renderFpChart(filters, msg.ts);
    renderMemChart(filters);
    renderPipelineStrip(msg.pipeline);
  }

  // ------------------------------------------------------------------ forms

  /** POST JSON to a /proxy/* route and hand the parsed body to render(). */
  function postProxy(path, payload, resultId, render) {
    const result = document.getElementById(resultId);
    if (result) { result.className = "result muted"; result.textContent = "…"; }
    fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (resp) {
        return resp.json().then(function (body) { return { ok: resp.ok, body: body }; });
      })
      .then(function (r) {
        if (!r.ok) {
          const detail = r.body && r.body.detail ? JSON.stringify(r.body.detail) : "request failed";
          showResult(resultId, "red", detail);
          return;
        }
        render(r.body);
      })
      .catch(function (err) {
        showResult(resultId, "red", "network error: " + err);
      });
  }

  function showResult(id, tone, text) {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = "result " + tone;
    el.textContent = text;
  }

  function wireForms() {
    document.getElementById("add-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      postProxy(
        "/proxy/add",
        {
          log_type: document.getElementById("add-type").value,
          log_key: document.getElementById("add-key").value,
        },
        "add-result",
        function (body) {
          showResult(
            "add-result", "green",
            "added in " + num(body.processing_time_ms).toFixed(3) + " ms"
          );
        }
      );
    });

    document.getElementById("query-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      postProxy(
        "/proxy/query",
        {
          log_type: document.getElementById("query-type").value,
          log_key: document.getElementById("query-key").value,
        },
        "query-result",
        function (body) {
          // Bloom asymmetry in the styling: a positive is only a "probably"
          // (amber), a negative is a guarantee (green).
          if (body.might_exist) {
            showResult(
              "query-result", "amber",
              "probably exists (" + body.confidence + ", " +
              num(body.processing_time_ms).toFixed(3) + " ms)"
            );
          } else {
            showResult(
              "query-result", "green",
              "definitely new (" + body.confidence + ", " +
              num(body.processing_time_ms).toFixed(3) + " ms)"
            );
          }
        }
      );
    });

    document.getElementById("session-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      postProxy(
        "/proxy/session-query",
        { session_id: document.getElementById("session-id").value },
        "session-result",
        function (body) {
          if (!body.might_exist) {
            showResult(
              "session-result", "green",
              "definitely new session — storage never touched (" +
              num(body.processing_time_ms).toFixed(3) + " ms)"
            );
          } else if (body.found) {
            showResult(
              "session-result", "amber",
              "probably exists — verified in storage (" +
              num(body.processing_time_ms).toFixed(3) + " ms)"
            );
          } else {
            showResult(
              "session-result", "amber",
              "bloom false positive — storage says absent (" +
              num(body.processing_time_ms).toFixed(3) + " ms)"
            );
          }
        }
      );
    });
  }

  // -------------------------------------------------- resilient WS client

  // ws:// or wss:// to match the page scheme; same host:port as the page.
  const wsUrl = (location.protocol === "https:" ? "wss" : "ws") +
    "://" + location.host + "/ws";

  // Exponential backoff: 1s → 2s → 4s → … capped at 15s; reset on connect.
  const BACKOFF_BASE_MS = 1000;
  const BACKOFF_CAP_MS = 15000;
  let backoffMs = BACKOFF_BASE_MS;
  let socket = null;
  let reconnectTimer = null;

  function scheduleReconnect() {
    if (reconnectTimer) return; // one pending attempt at a time
    setPill("reconnecting", "retrying in " + Math.round(backoffMs / 1000) + "s");
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connect();
    }, backoffMs);
    backoffMs = Math.min(backoffMs * 2, BACKOFF_CAP_MS);
  }

  function connect() {
    setPill(backoffMs > BACKOFF_BASE_MS ? "reconnecting" : "connecting", "");
    try {
      socket = new WebSocket(wsUrl);
    } catch (e) {
      scheduleReconnect();
      return;
    }

    socket.onopen = function () {
      backoffMs = BACKOFF_BASE_MS; // healthy again — reset the ladder
      setPill("connected", "");
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
      scheduleReconnect();
    };

    socket.onerror = function () {
      // onerror is normally followed by onclose; close defensively so the
      // close handler is the single reconnect path.
      try { socket.close(); } catch (e) { /* ignore */ }
    };
  }

  function boot() {
    initCharts(); // paint empty axes immediately so the page reads as alive
    wireForms();
    connect();
  }

  // Loaded with `defer`, so the DOM is parsed; guard anyway for eager loads.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

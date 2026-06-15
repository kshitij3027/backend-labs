/*
 * Live dashboard client for the delta-encoding log engine.
 *
 * Opens a single WebSocket to /ws and renders each "tick" the server pushes into the
 * stat cards. There is no polling: the page is entirely tick-driven. On socket close it
 * reconnects with a short capped backoff. Every field read is defensive (the tick may
 * carry stats=null on a server-side error, or omit a key), so a malformed tick degrades
 * to a placeholder rather than throwing.
 *
 * NEXT COMMIT: this file grows Chart.js series updates + form submit handlers. For now it
 * only writes the text cards and manages the connection-status pill.
 */
(function () {
  "use strict";

  var DASH = "—"; // em dash placeholder for missing values

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

  // --- render one tick into the cards -------------------------------------
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
  }

  // --- WebSocket lifecycle with capped backoff -----------------------------
  var ws = null;
  var backoff = 500; // ms, doubles on each failed attempt up to the cap
  var BACKOFF_MAX = 8000;

  function connect() {
    var url = "ws://" + location.host + "/ws";
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

  // Kick off once the DOM is parsed (the script tag is at the end of body, so the
  // elements already exist, but this is robust to defer/async too).
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();

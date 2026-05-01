// Vanilla-JS dashboard client.
//
// Connects to ws://<host>/ws and renders snapshots pushed by the
// dashboard server. Each snapshot has the shape:
//   {
//     nodes: [{ node_id, state, role, lock_holder, known_winner,
//               term, log_count, host, port, ... }, ...],
//     throughput_lps: <float>,
//     timestamp: <epoch_seconds>
//   }
//
// Falls back to 2s polling of GET /api/snapshot if the WebSocket fails
// to connect after a couple of attempts. Reconnect uses capped
// exponential backoff (max 10s).

(function () {
  "use strict";

  // ---------- DOM handles ----------
  var nodesGrid = document.getElementById("nodes-grid");
  var connectionStatus = document.getElementById("connection-status");
  var lastUpdate = document.getElementById("last-update");
  var logsPerSec = document.getElementById("logs-per-sec");
  var transportMode = document.getElementById("transport-mode");
  var failoverBtn = document.getElementById("failover-btn");
  var failoverResult = document.getElementById("failover-result");

  // ---------- Throughput chart (Chart.js) ----------
  // Rolling 60-data-point history. We append on every snapshot and trim
  // from the front so the chart shows roughly the last minute of data.
  var THROUGHPUT_CAP = 60;
  var throughputData = []; // array of { t: epoch_sec, v: lps }

  var chartCanvas = document.getElementById("throughput-chart");
  var chart = new Chart(chartCanvas.getContext("2d"), {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "logs/sec",
          data: [],
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59,130,246,0.12)",
          borderWidth: 2,
          fill: true,
          tension: 0.25,
          pointRadius: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: true, ticks: { maxTicksLimit: 6 } },
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });

  function pushThroughput(snapshotTs, lps) {
    throughputData.push({ t: snapshotTs, v: lps });
    while (throughputData.length > THROUGHPUT_CAP) {
      throughputData.shift();
    }
    var labels = throughputData.map(function (d) {
      var dt = new Date(d.t * 1000);
      return dt.toLocaleTimeString();
    });
    var values = throughputData.map(function (d) { return d.v; });
    chart.data.labels = labels;
    chart.data.datasets[0].data = values;
    chart.update("none");
  }

  // ---------- Render helpers ----------
  function setConnection(state) {
    connectionStatus.textContent = state;
    connectionStatus.classList.remove("connected", "disconnected", "connecting");
    if (state === "connected") {
      connectionStatus.classList.add("connected");
    } else if (state === "disconnected") {
      connectionStatus.classList.add("disconnected");
    } else {
      connectionStatus.classList.add("connecting");
    }
  }

  function fmtTs(ts) {
    if (!ts) return "—";
    var dt = new Date(ts * 1000);
    return dt.toLocaleTimeString();
  }

  function renderSnapshot(snap) {
    if (!snap) return;
    lastUpdate.textContent = fmtTs(snap.timestamp);
    logsPerSec.textContent = (snap.throughput_lps || 0).toFixed(2);

    var nodes = snap.nodes || [];
    // Build / update cards. We replace the grid wholesale on every frame —
    // it's only 3 cards and the perf cost is negligible.
    nodesGrid.innerHTML = "";
    nodes.forEach(function (n) {
      var card = document.createElement("div");
      var state = (n.state || "INACTIVE").toUpperCase();
      card.className = "node-card state-" + state;

      var header = document.createElement("div");
      header.className = "node-card-header";
      var idEl = document.createElement("span");
      idEl.className = "node-id";
      idEl.textContent = n.node_id || "(unknown)";
      var badge = document.createElement("span");
      badge.className = "state-badge";
      badge.textContent = state;
      header.appendChild(idEl);
      header.appendChild(badge);

      var fields = document.createElement("div");
      fields.className = "node-fields";
      [
        ["role", n.role != null ? n.role : "—"],
        ["lock_holder", n.lock_holder != null ? n.lock_holder : "—"],
        ["known_winner", n.known_winner != null ? n.known_winner : "—"],
        ["term", n.term != null ? n.term : 0],
        ["logs", n.log_count != null ? n.log_count : 0],
      ].forEach(function (kv) {
        var k = document.createElement("div");
        k.className = "key";
        k.textContent = kv[0];
        var v = document.createElement("div");
        v.className = "val";
        v.textContent = String(kv[1]);
        fields.appendChild(k);
        fields.appendChild(v);
      });

      card.appendChild(header);
      card.appendChild(fields);
      nodesGrid.appendChild(card);
    });

    pushThroughput(snap.timestamp || (Date.now() / 1000), snap.throughput_lps || 0);
  }

  // ---------- WebSocket transport ----------
  var ws = null;
  var reconnectDelay = 1000; // start at 1s, cap at 10s
  var pollTimer = null;
  var lastConnectAttempts = 0;

  function wsUrl() {
    var proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + window.location.host + "/ws";
  }

  function clearPolling() {
    if (pollTimer != null) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function startPolling() {
    if (pollTimer != null) return;
    transportMode.textContent = "polling (2s)";
    pollTimer = setInterval(function () {
      fetch("/api/snapshot")
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (snap) {
          if (snap) {
            setConnection("connected");
            renderSnapshot(snap);
          }
        })
        .catch(function () {
          setConnection("disconnected");
        });
    }, 2000);
  }

  function connectWs() {
    setConnection("connecting");
    transportMode.textContent = "websocket";
    try {
      ws = new WebSocket(wsUrl());
    } catch (e) {
      lastConnectAttempts += 1;
      scheduleReconnect();
      return;
    }

    ws.onopen = function () {
      setConnection("connected");
      reconnectDelay = 1000;
      lastConnectAttempts = 0;
      clearPolling(); // websocket wins; stop polling
    };

    ws.onmessage = function (ev) {
      try {
        var snap = JSON.parse(ev.data);
        renderSnapshot(snap);
      } catch (e) {
        // ignore malformed frames
      }
    };

    ws.onclose = function () {
      setConnection("disconnected");
      ws = null;
      lastConnectAttempts += 1;
      // After 2 failed attempts, fall back to polling so the user
      // still sees data even if the WebSocket path is broken.
      if (lastConnectAttempts >= 2) {
        startPolling();
      }
      scheduleReconnect();
    };

    ws.onerror = function () {
      // Trigger close + reconnect path. Don't double-handle here.
      try { ws.close(); } catch (e) {}
    };
  }

  function scheduleReconnect() {
    setTimeout(connectWs, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 10000);
  }

  // ---------- Manual failover button ----------
  failoverBtn.addEventListener("click", function () {
    failoverBtn.disabled = true;
    failoverResult.textContent = "triggering failover…";
    fetch("/proxy/admin/trigger-failover", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    })
      .then(function (r) {
        return r.json().then(function (body) {
          return { code: r.status, body: body };
        });
      })
      .then(function (res) {
        if (res.code >= 200 && res.code < 300) {
          failoverResult.textContent =
            "failover triggered on " + (res.body.holder || "primary") +
            " (status " + res.code + ")";
        } else {
          failoverResult.textContent =
            "failed: " + (res.body.reason || "http " + res.code);
        }
      })
      .catch(function (err) {
        failoverResult.textContent = "error: " + err;
      })
      .finally(function () {
        setTimeout(function () { failoverBtn.disabled = false; }, 1500);
      });
  });

  // ---------- Boot ----------
  connectWs();
})();

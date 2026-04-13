/* Anomaly Detection Engine — Dashboard JavaScript */

(function () {
  "use strict";

  // ---------------------------------------------------------------
  // DOM references
  // ---------------------------------------------------------------
  const statTotal    = document.getElementById("stat-total");
  const statAnomalies = document.getElementById("stat-anomalies");
  const statTPR      = document.getElementById("stat-tpr");
  const statFPR      = document.getElementById("stat-fpr");
  const statusDot    = document.getElementById("status-dot");
  const statusText   = document.getElementById("status-text");
  const anomalyTbody = document.getElementById("anomaly-tbody");

  // ---------------------------------------------------------------
  // Chart.js — Anomaly Timeline (line)
  // ---------------------------------------------------------------
  const MAX_POINTS = 50;

  const timelineCtx = document.getElementById("timelineChart").getContext("2d");
  const timelineChart = new Chart(timelineCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Confidence",
          data: [],
          borderColor: "#00e5ff",
          backgroundColor: "rgba(0,229,255,0.1)",
          borderWidth: 1.5,
          pointRadius: 2,
          tension: 0.3,
          fill: true,
        },
        {
          label: "Threshold",
          data: [],
          borderColor: "#ff1744",
          borderWidth: 1,
          borderDash: [6, 3],
          pointRadius: 0,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      scales: {
        x: { display: false },
        y: { min: 0, max: 1, ticks: { color: "#8892b0" }, grid: { color: "#1e2a4a" } },
      },
      plugins: {
        legend: { labels: { color: "#8892b0", boxWidth: 12 } },
      },
    },
  });

  // ---------------------------------------------------------------
  // Chart.js — Anomaly Type Distribution (doughnut)
  // ---------------------------------------------------------------
  const TYPE_COLORS = {
    slow_response: "#ff1744",
    unusual_payload: "#ff9100",
    suspicious_agent: "#ffd600",
    bad_status: "#d500f9",
  };

  const typeCounts = { slow_response: 0, unusual_payload: 0, suspicious_agent: 0, bad_status: 0 };

  const typeCtx = document.getElementById("typeChart").getContext("2d");
  const typeChart = new Chart(typeCtx, {
    type: "doughnut",
    data: {
      labels: Object.keys(TYPE_COLORS),
      datasets: [{
        data: Object.values(typeCounts),
        backgroundColor: Object.values(TYPE_COLORS),
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      plugins: {
        legend: { position: "right", labels: { color: "#8892b0", boxWidth: 12, padding: 8 } },
      },
    },
  });

  // ---------------------------------------------------------------
  // Chart.js — Per-Algorithm Scores (bar)
  // ---------------------------------------------------------------
  const algoCtx = document.getElementById("algoChart").getContext("2d");
  const algoChart = new Chart(algoCtx, {
    type: "bar",
    data: {
      labels: ["Z-Score", "Isolation Forest", "Temporal"],
      datasets: [{
        label: "Latest Score",
        data: [0, 0, 0],
        backgroundColor: ["#00e5ff", "#76ff03", "#ffab00"],
        borderWidth: 0,
        barPercentage: 0.6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      scales: {
        x: { ticks: { color: "#8892b0" }, grid: { display: false } },
        y: { min: 0, max: 1, ticks: { color: "#8892b0" }, grid: { color: "#1e2a4a" } },
      },
      plugins: {
        legend: { display: false },
      },
    },
  });

  // ---------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------

  function fmtPct(val) {
    return (val * 100).toFixed(1) + "%";
  }

  function fmtNum(val) {
    return val.toLocaleString();
  }

  function confClass(c) {
    if (c >= 0.85) return "high";
    if (c >= 0.7) return "med";
    return "low";
  }

  function scoreBar(cls, val) {
    var w = Math.max(4, Math.round(val * 60));
    return '<span class="score-bar ' + cls + '" style="width:' + w + 'px"></span>' +
           '<span>' + val.toFixed(2) + '</span>';
  }

  function guessAnomalyType(a) {
    // Heuristic: infer type from log characteristics since the API
    // does not expose _anomaly_type on the result.
    var rt = a.log_summary.response_time || 0;
    var sc = a.log_summary.status_code || 200;

    if (rt > 2500) return "slow_response";
    if (sc >= 500 || sc === 429) return "bad_status";

    // Use highest scoring detector as hint
    var scores = a.scores || {};
    var maxName = "";
    var maxVal = 0;
    for (var k in scores) {
      if (scores[k] > maxVal) { maxVal = scores[k]; maxName = k; }
    }
    if (maxName === "temporal") return "suspicious_agent";
    return "unusual_payload";
  }

  // Track already-seen timestamps to count types only once
  var seenTimestamps = {};

  // ---------------------------------------------------------------
  // Update functions
  // ---------------------------------------------------------------

  function updateStats(stats) {
    statTotal.textContent    = fmtNum(stats.total_processed);
    statAnomalies.textContent = fmtNum(stats.anomalies_detected);
    statTPR.textContent       = fmtPct(stats.true_positive_rate);
    statFPR.textContent       = fmtPct(stats.false_positive_rate);
  }

  function updateTimeline(anomalies) {
    var labels = timelineChart.data.labels;
    var confData = timelineChart.data.datasets[0].data;
    var threshData = timelineChart.data.datasets[1].data;

    for (var i = 0; i < anomalies.length; i++) {
      var a = anomalies[i];
      if (seenTimestamps[a.timestamp]) continue;

      var t = a.timestamp.split("T")[1];
      if (t) t = t.substring(0, 8);
      else t = a.timestamp;

      labels.push(t);
      confData.push(a.confidence);
      threshData.push(0.7);

      if (labels.length > MAX_POINTS) {
        labels.shift();
        confData.shift();
        threshData.shift();
      }
    }
    timelineChart.update();
  }

  function updateTypeChart(anomalies) {
    for (var i = 0; i < anomalies.length; i++) {
      var a = anomalies[i];
      if (seenTimestamps[a.timestamp]) continue;
      var atype = guessAnomalyType(a);
      if (typeCounts[atype] !== undefined) {
        typeCounts[atype]++;
      }
    }
    typeChart.data.datasets[0].data = [
      typeCounts.slow_response,
      typeCounts.unusual_payload,
      typeCounts.suspicious_agent,
      typeCounts.bad_status,
    ];
    typeChart.update();
  }

  function updateAlgoChart(anomalies) {
    if (anomalies.length === 0) return;
    var latest = anomalies[anomalies.length - 1];
    var scores = latest.scores || {};
    algoChart.data.datasets[0].data = [
      scores.zscore || 0,
      scores.isolation_forest || 0,
      scores.temporal || 0,
    ];
    algoChart.update();
  }

  function updateTable(anomalies) {
    // Show last 10, newest first
    var rows = "";
    var start = Math.max(0, anomalies.length - 10);
    for (var i = anomalies.length - 1; i >= start; i--) {
      var a = anomalies[i];
      var ls = a.log_summary;
      var sc = a.scores || {};
      var atype = guessAnomalyType(a);
      var cc = confClass(a.confidence);

      var ts = a.timestamp.split("T")[1];
      if (ts) ts = ts.substring(0, 12);
      else ts = a.timestamp;

      rows += '<tr class="type-' + atype + '">';
      rows += "<td>" + ts + "</td>";
      rows += "<td>" + ls.ip + "</td>";
      rows += "<td>" + ls.method + "</td>";
      rows += "<td>" + ls.path + "</td>";
      rows += "<td>" + ls.status_code + "</td>";
      rows += "<td>" + (ls.response_time || 0).toFixed(0) + "ms</td>";
      rows += '<td class="confidence ' + cc + '">' + a.confidence.toFixed(3) + "</td>";
      rows += "<td>" + scoreBar("zscore", sc.zscore || 0) + "</td>";
      rows += "<td>" + scoreBar("iforest", sc.isolation_forest || 0) + "</td>";
      rows += "<td>" + scoreBar("temporal", sc.temporal || 0) + "</td>";
      rows += "</tr>";
    }
    anomalyTbody.innerHTML = rows;
  }

  function markSeen(anomalies) {
    for (var i = 0; i < anomalies.length; i++) {
      seenTimestamps[anomalies[i].timestamp] = true;
    }
  }

  // ---------------------------------------------------------------
  // Socket.IO
  // ---------------------------------------------------------------
  var socket = io();

  socket.on("connect", function () {
    statusDot.className  = "dot connected";
    statusText.textContent = "Connected";
  });

  socket.on("disconnect", function () {
    statusDot.className  = "dot disconnected";
    statusText.textContent = "Disconnected";
  });

  socket.on("anomaly_update", function (data) {
    if (data.stats) updateStats(data.stats);

    var anomalies = data.recent_anomalies || [];
    if (anomalies.length > 0) {
      updateTimeline(anomalies);
      updateTypeChart(anomalies);
      updateAlgoChart(anomalies);
      updateTable(anomalies);
      markSeen(anomalies);
    }
  });
})();

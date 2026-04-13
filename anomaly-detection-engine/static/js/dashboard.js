/* ==========================================================================
   Anomaly Detection Engine — Dashboard JavaScript
   Clean Chart.js 4.x configuration, live anomaly table, smooth updates
   ========================================================================== */

(function () {
  "use strict";

  // -------------------------------------------------------------------
  // DOM References
  // -------------------------------------------------------------------
  var $statTotal     = document.getElementById("stat-total");
  var $statAnomalies = document.getElementById("stat-anomalies");
  var $statTPR       = document.getElementById("stat-tpr");
  var $statFPR       = document.getElementById("stat-fpr");
  var $statusDot     = document.getElementById("status-dot");
  var $statusText    = document.getElementById("status-text");
  var $anomalyTbody  = document.getElementById("anomaly-tbody");
  var $countBadge    = document.getElementById("anomaly-count-badge");

  // Memory panel
  var $memGaugeFill  = document.getElementById("mem-gauge-fill");
  var $memUsageText  = document.getElementById("mem-usage-text");
  var $memUniqueIPs  = document.getElementById("mem-unique-ips");
  var $memUniqueUAs  = document.getElementById("mem-unique-uas");
  var $memUniquePaths = document.getElementById("mem-unique-paths");
  var $memThreshold  = document.getElementById("mem-threshold");
  var $memLoadFactor = document.getElementById("mem-load-factor");

  // -------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------
  var MAX_TIMELINE_POINTS = 50;
  var MAX_TABLE_ROWS      = 15;
  var MEM_LIMIT_BYTES     = 200 * 1024 * 1024; // 200 MB

  // Anomaly type colors
  var TYPE_COLORS = {
    slow_response:    "#ef4444",
    unusual_payload:  "#f97316",
    suspicious_agent: "#eab308",
    bad_status:       "#a855f7",
    unknown:          "#64748b"
  };

  var TYPE_LABELS = {
    slow_response:    "Slow Response",
    unusual_payload:  "Unusual Payload",
    suspicious_agent: "Suspicious Agent",
    bad_status:       "Bad Status",
    unknown:          "Unknown"
  };

  // -------------------------------------------------------------------
  // Formatters
  // -------------------------------------------------------------------

  function fmtNum(n) {
    if (n == null) return "0";
    return n.toLocaleString("en-US");
  }

  function fmtPct(v) {
    return (v * 100).toFixed(1) + "%";
  }

  function fmtBytes(b) {
    if (b == null || b === 0) return "0 B";
    var units = ["B", "KB", "MB", "GB"];
    var i = 0;
    var val = b;
    while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
    return val.toFixed(i === 0 ? 0 : 1) + " " + units[i];
  }

  function fmtTime(ts) {
    if (!ts) return "--";
    var t = ts.split("T")[1];
    return t ? t.substring(0, 8) : ts;
  }

  // -------------------------------------------------------------------
  // Animated Number Counter
  // -------------------------------------------------------------------
  var _animRunning = {};

  function animateValue(el, newVal, duration) {
    var key = el.id;
    if (_animRunning[key]) cancelAnimationFrame(_animRunning[key]);

    var raw = el.textContent.replace(/[^0-9.\-]/g, "");
    var start = parseFloat(raw) || 0;
    var isPercent = el.textContent.indexOf("%") !== -1;
    var diff = newVal - start;
    if (Math.abs(diff) < 0.01) {
      el.textContent = isPercent ? fmtPct(newVal / 100) : fmtNum(Math.round(newVal));
      return;
    }

    var t0 = performance.now();
    var dur = duration || 400;

    function step(now) {
      var elapsed = now - t0;
      var progress = Math.min(elapsed / dur, 1);
      // ease-out cubic
      var ease = 1 - Math.pow(1 - progress, 3);
      var current = start + diff * ease;

      if (isPercent) {
        el.textContent = (current).toFixed(1) + "%";
      } else {
        el.textContent = fmtNum(Math.round(current));
      }

      if (progress < 1) {
        _animRunning[key] = requestAnimationFrame(step);
      } else {
        delete _animRunning[key];
      }
    }

    _animRunning[key] = requestAnimationFrame(step);
  }

  // -------------------------------------------------------------------
  // Anomaly Type Inference
  // -------------------------------------------------------------------

  function inferType(a) {
    var ls = a.log_summary || {};
    var rt = ls.response_time || 0;
    var sc = ls.status_code || 200;
    var bs = ls.bytes_sent || 0;

    if (rt > 2000) return "slow_response";
    if (bs > 40000) return "unusual_payload";
    if (sc >= 429) return "bad_status";

    // Use the user agent length heuristic if available
    // (not in log_summary, so fall back to detector hints)
    var scores = a.scores || {};
    var maxName = "";
    var maxVal = 0;
    for (var k in scores) {
      if (scores[k] > maxVal) { maxVal = scores[k]; maxName = k; }
    }
    if (maxName === "temporal") return "suspicious_agent";
    if (maxName === "zscore") return "slow_response";
    return "unusual_payload";
  }

  // -------------------------------------------------------------------
  // Chart.js — Shared Options
  // -------------------------------------------------------------------
  var CHART_FONT = { family: "'SF Mono', 'Consolas', monospace", size: 11 };
  var GRID_COLOR = "rgba(148,163,184,0.08)";
  var TICK_COLOR = "#64748b";

  // -------------------------------------------------------------------
  // Chart: Anomaly Timeline (line)
  // -------------------------------------------------------------------
  var timelineCtx = document.getElementById("timelineChart").getContext("2d");

  // Gradient fill
  var gradientFill = timelineCtx.createLinearGradient(0, 0, 0, 250);
  gradientFill.addColorStop(0, "rgba(34,211,238,0.25)");
  gradientFill.addColorStop(1, "rgba(34,211,238,0.0)");

  var timelineChart = new Chart(timelineCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Confidence",
          data: [],
          borderColor: "#22d3ee",
          backgroundColor: gradientFill,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          pointHoverBackgroundColor: "#22d3ee",
          tension: 0.4,
          fill: true
        },
        {
          label: "Threshold",
          data: [],
          borderColor: "rgba(239,68,68,0.6)",
          borderWidth: 1.5,
          borderDash: [6, 4],
          pointRadius: 0,
          fill: false
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      interaction: { mode: "index", intersect: false },
      scales: {
        x: {
          display: true,
          ticks: { color: TICK_COLOR, font: CHART_FONT, maxRotation: 0, maxTicksLimit: 8 },
          grid: { display: false }
        },
        y: {
          min: 0,
          max: 1,
          ticks: { color: TICK_COLOR, font: CHART_FONT, stepSize: 0.25 },
          grid: { color: GRID_COLOR }
        }
      },
      plugins: {
        legend: {
          labels: { color: "#94a3b8", boxWidth: 10, padding: 12, font: { size: 11 } }
        },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          borderColor: "rgba(148,163,184,0.2)",
          borderWidth: 1,
          titleFont: { size: 11 },
          bodyFont: { size: 11 },
          padding: 10,
          cornerRadius: 8
        }
      }
    }
  });

  // -------------------------------------------------------------------
  // Chart: Type Distribution (doughnut)
  // -------------------------------------------------------------------
  var typeCounts = { slow_response: 0, unusual_payload: 0, suspicious_agent: 0, bad_status: 0 };
  var typeKeys = Object.keys(typeCounts);

  var typeCtx = document.getElementById("typeChart").getContext("2d");
  var typeChart = new Chart(typeCtx, {
    type: "doughnut",
    data: {
      labels: typeKeys.map(function (k) { return TYPE_LABELS[k]; }),
      datasets: [{
        data: typeKeys.map(function () { return 0; }),
        backgroundColor: typeKeys.map(function (k) { return TYPE_COLORS[k]; }),
        borderWidth: 0,
        hoverOffset: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "62%",
      animation: { duration: 300 },
      plugins: {
        legend: {
          position: "right",
          labels: {
            color: "#94a3b8",
            boxWidth: 10,
            padding: 10,
            font: { size: 11 },
            usePointStyle: true,
            pointStyle: "rectRounded"
          }
        },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          borderColor: "rgba(148,163,184,0.2)",
          borderWidth: 1,
          padding: 10,
          cornerRadius: 8
        }
      }
    }
  });

  // -------------------------------------------------------------------
  // Chart: Per-Algorithm Scores (horizontal bar)
  // -------------------------------------------------------------------
  var algoCtx = document.getElementById("algoChart").getContext("2d");
  var algoChart = new Chart(algoCtx, {
    type: "bar",
    data: {
      labels: ["Z-Score", "Isolation Forest", "Temporal"],
      datasets: [{
        label: "Latest Score",
        data: [0, 0, 0],
        backgroundColor: ["#22d3ee", "#22c55e", "#f59e0b"],
        borderWidth: 0,
        borderRadius: 4,
        barPercentage: 0.55
      }]
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      scales: {
        x: {
          min: 0,
          max: 1,
          ticks: { color: TICK_COLOR, font: CHART_FONT, stepSize: 0.25 },
          grid: { color: GRID_COLOR }
        },
        y: {
          ticks: { color: "#94a3b8", font: { size: 12, weight: "500" } },
          grid: { display: false }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          borderColor: "rgba(148,163,184,0.2)",
          borderWidth: 1,
          padding: 10,
          cornerRadius: 8,
          callbacks: {
            label: function (ctx) { return " Score: " + ctx.parsed.x.toFixed(3); }
          }
        }
      }
    }
  });

  // -------------------------------------------------------------------
  // Track seen timestamps to avoid double counting
  // -------------------------------------------------------------------
  var seenTs = {};

  // -------------------------------------------------------------------
  // Update Functions
  // -------------------------------------------------------------------

  function updateStats(stats) {
    if (!stats) return;

    animateValue($statTotal, stats.total_processed || 0);
    animateValue($statAnomalies, stats.anomalies_detected || 0);

    var tpr = (stats.true_positive_rate || 0) * 100;
    var fpr = (stats.false_positive_rate || 0) * 100;
    animateValue($statTPR, tpr);
    animateValue($statFPR, fpr);

    // Memory panel
    var mem = stats.memory_efficient || {};
    var usageBytes = mem.memory_usage_bytes || 0;
    var pct = Math.min(100, (usageBytes / MEM_LIMIT_BYTES) * 100);
    $memGaugeFill.style.width = pct.toFixed(1) + "%";

    // Color the gauge based on usage
    if (pct > 80) {
      $memGaugeFill.style.background = "linear-gradient(90deg, #f59e0b, #ef4444)";
    } else if (pct > 50) {
      $memGaugeFill.style.background = "linear-gradient(90deg, #22d3ee, #f59e0b)";
    } else {
      $memGaugeFill.style.background = "linear-gradient(90deg, #22d3ee, #22c55e)";
    }

    $memUsageText.textContent = fmtBytes(usageBytes);
    $memUniqueIPs.textContent = fmtNum(mem.unique_ips || 0);
    $memUniqueUAs.textContent = fmtNum(mem.unique_user_agents || 0);
    $memUniquePaths.textContent = fmtNum(mem.unique_paths || 0);

    // Adaptive threshold
    var at = stats.adaptive_threshold || {};
    $memThreshold.textContent = (at.current_threshold || 0.7).toFixed(3);

    // Load factor from contextual
    var ctx = stats.contextual || {};
    $memLoadFactor.textContent = (ctx.load_factor || 0).toFixed(2);
  }

  function updateTimeline(anomalies) {
    var labels = timelineChart.data.labels;
    var confData = timelineChart.data.datasets[0].data;
    var threshData = timelineChart.data.datasets[1].data;
    var changed = false;

    for (var i = 0; i < anomalies.length; i++) {
      var a = anomalies[i];
      if (seenTs[a.timestamp]) continue;

      labels.push(fmtTime(a.timestamp));
      confData.push(a.confidence);
      threshData.push(0.7);
      changed = true;

      if (labels.length > MAX_TIMELINE_POINTS) {
        labels.shift();
        confData.shift();
        threshData.shift();
      }
    }

    if (changed) timelineChart.update();
  }

  function updateTypeChart(anomalies) {
    var changed = false;
    for (var i = 0; i < anomalies.length; i++) {
      var a = anomalies[i];
      if (seenTs[a.timestamp]) continue;
      var t = inferType(a);
      if (typeCounts[t] !== undefined) {
        typeCounts[t]++;
        changed = true;
      }
    }

    if (changed) {
      typeChart.data.datasets[0].data = typeKeys.map(function (k) { return typeCounts[k]; });
      typeChart.update();
    }
  }

  function updateAlgoChart(anomalies) {
    if (anomalies.length === 0) return;
    var latest = anomalies[anomalies.length - 1];
    var sc = latest.scores || {};
    algoChart.data.datasets[0].data = [
      sc.zscore || 0,
      sc.isolation_forest || 0,
      sc.temporal || 0
    ];
    algoChart.update();
  }

  // -------------------------------------------------------------------
  // Anomaly Table
  // -------------------------------------------------------------------

  function confClass(c) {
    if (c >= 0.8) return "conf--high";
    if (c >= 0.5) return "conf--mid";
    return "conf--low";
  }

  function scoreBarHTML(cls, val) {
    var w = Math.max(3, Math.round(val * 50));
    return '<span class="score-cell">' +
      '<span class="score-bar score-bar--' + cls + '" style="width:' + w + 'px"></span>' +
      '<span>' + val.toFixed(2) + '</span>' +
      '</span>';
  }

  function typeBadge(t) {
    var label = TYPE_LABELS[t] || t;
    return '<span class="type-badge type-badge--' + t + '">' + label + '</span>';
  }

  function updateTable(anomalies) {
    // Keep last MAX_TABLE_ROWS, newest first
    var source = anomalies.slice(-MAX_TABLE_ROWS).reverse();

    if (source.length === 0) {
      $anomalyTbody.innerHTML = '<tr><td colspan="11" class="empty-state">No anomalies detected yet. Waiting for data...</td></tr>';
      $countBadge.textContent = "0";
      return;
    }

    $countBadge.textContent = fmtNum(source.length);

    var html = "";
    for (var i = 0; i < source.length; i++) {
      var a = source[i];
      var ls = a.log_summary || {};
      var sc = a.scores || {};
      var atype = inferType(a);
      var cc = confClass(a.confidence);

      html += '<tr class="type-' + atype + '">';
      html += '<td>' + fmtTime(a.timestamp) + '</td>';
      html += '<td>' + (ls.ip || "--") + '</td>';
      html += '<td>' + (ls.method || "--") + '</td>';
      html += '<td>' + (ls.path || "--") + '</td>';
      html += '<td>' + (ls.status_code || "--") + '</td>';
      html += '<td>' + (ls.response_time || 0).toFixed(0) + ' ms</td>';
      html += '<td class="conf ' + cc + '">' + a.confidence.toFixed(3) + '</td>';
      html += '<td>' + scoreBarHTML("zscore", sc.zscore || 0) + '</td>';
      html += '<td>' + scoreBarHTML("iforest", sc.isolation_forest || 0) + '</td>';
      html += '<td>' + scoreBarHTML("temporal", sc.temporal || 0) + '</td>';
      html += '<td>' + typeBadge(atype) + '</td>';
      html += '</tr>';
    }

    $anomalyTbody.innerHTML = html;
  }

  function markSeen(anomalies) {
    for (var i = 0; i < anomalies.length; i++) {
      seenTs[anomalies[i].timestamp] = true;
    }
  }

  // -------------------------------------------------------------------
  // Socket.IO Connection
  // -------------------------------------------------------------------
  var socket = io();

  socket.on("connect", function () {
    $statusDot.className = "status-dot status-dot--on";
    $statusText.textContent = "Connected";
  });

  socket.on("disconnect", function () {
    $statusDot.className = "status-dot status-dot--off";
    $statusText.textContent = "Disconnected";
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

  // Initial empty-state message
  $anomalyTbody.innerHTML = '<tr><td colspan="11" class="empty-state">Connecting to server...</td></tr>';

})();

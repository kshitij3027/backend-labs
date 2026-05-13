(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const tbody = $("#experiments-table tbody");
  const createMessage = $("#create-message");
  const livePanel = $("#live-panel");
  const eventsLog = $("#events-log");
  const reportPre = $("#recovery-report");
  const activeRunIdEl = $("#active-run-id");
  const cbIndicator = $("#cb-indicator");
  const cbText = $("#cb-text");

  let chart = null;
  let currentWs = null;
  let currentRunId = null;

  // -- experiments list --------------------------------------------------
  async function fetchExperiments() {
    const r = await fetch("/experiments");
    if (!r.ok) throw new Error(`GET /experiments failed: ${r.status}`);
    return r.json();
  }

  function renderExperiments(items) {
    tbody.innerHTML = "";
    for (const e of items) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(e.name)}</td>
        <td>${escapeHtml(e.type)}</td>
        <td>${escapeHtml(e.target)}</td>
        <td>${e.duration}s</td>
        <td>${e.severity}</td>
        <td><button data-action="run" data-id="${e.id}" type="button">Run</button></td>
      `;
      tbody.appendChild(tr);
    }
  }

  async function refreshExperiments() {
    try {
      const items = await fetchExperiments();
      renderExperiments(items);
    } catch (err) {
      console.error(err);
    }
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
    );
  }

  // -- create experiment -------------------------------------------------
  $("#create-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    createMessage.textContent = "";
    createMessage.classList.remove("success", "error");
    const fd = new FormData(ev.target);
    const params = (fd.get("parameters") || "").trim();
    let parameters = {};
    try {
      if (params) parameters = JSON.parse(params);
    } catch (_err) {
      createMessage.textContent = "Parameters must be valid JSON.";
      createMessage.classList.add("error");
      return;
    }
    const payload = {
      name: fd.get("name"),
      type: fd.get("type"),
      target: fd.get("target"),
      duration: parseInt(fd.get("duration"), 10),
      severity: parseInt(fd.get("severity"), 10),
      parameters,
    };
    const r = await fetch("/experiments", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      const body = await r.text();
      createMessage.textContent = `Create failed: ${r.status} ${body}`;
      createMessage.classList.add("error");
      return;
    }
    createMessage.textContent = "Experiment created.";
    createMessage.classList.add("success");
    await refreshExperiments();
  });

  // -- run controls ------------------------------------------------------
  tbody.addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button[data-action='run']");
    if (!btn) return;
    btn.disabled = true;
    try {
      const r = await fetch(`/experiments/${btn.dataset.id}/run`, { method: "POST" });
      if (!r.ok) {
        alert(`Run failed: ${r.status}`);
        return;
      }
      const body = await r.json();
      openLivePanel(body.run_id);
    } finally {
      btn.disabled = false;
    }
  });

  function openLivePanel(runId) {
    if (currentWs) { try { currentWs.close(); } catch (_e) {} }
    currentRunId = runId;
    activeRunIdEl.textContent = runId;
    livePanel.hidden = false;
    eventsLog.innerHTML = "";
    reportPre.textContent = "(pending)";
    initChart();

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws/runs/${runId}`);
    currentWs = ws;

    ws.addEventListener("message", (ev) => {
      let frame;
      try { frame = JSON.parse(ev.data); } catch (_e) { return; }
      handleFrame(frame);
    });
    ws.addEventListener("close", () => {
      pushEvent("ws_closed", { run_id: runId });
    });
  }

  // -- chart -------------------------------------------------------------
  function initChart() {
    if (chart) { chart.destroy(); chart = null; }
    const ctx = document.getElementById("live-chart").getContext("2d");
    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: [
          { label: "cpu_pct", data: [], borderColor: "#4f6df5", tension: 0.2 },
          { label: "mem_pct", data: [], borderColor: "#22c55e", tension: 0.2 },
          { label: "network_latency_ms", data: [], borderColor: "#f59e0b", tension: 0.2 },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: { legend: { position: "bottom" } },
        scales: { y: { beginAtZero: true } },
      },
    });
  }

  function pushMetrics(metrics) {
    if (!chart || !metrics) return;
    const labels = chart.data.labels;
    const ts = (metrics.timestamp || new Date().toISOString()).replace("T", " ").slice(11, 19);
    labels.push(ts);
    chart.data.datasets[0].data.push(metrics.cpu_pct ?? 0);
    chart.data.datasets[1].data.push(metrics.mem_pct ?? 0);
    chart.data.datasets[2].data.push(metrics.network_latency_ms ?? 0);
    if (labels.length > 60) {
      labels.shift();
      for (const ds of chart.data.datasets) ds.data.shift();
    }
    chart.update("none");
  }

  function pushEvent(name, data) {
    const li = document.createElement("li");
    li.innerHTML = `<code>${escapeHtml(name)}</code> ${escapeHtml(JSON.stringify(data || {}))}`;
    eventsLog.prepend(li);
    while (eventsLog.children.length > 20) eventsLog.removeChild(eventsLog.lastChild);
  }

  function handleFrame(frame) {
    if (!frame || !frame.type) return;
    if (frame.type === "snapshot") {
      if (frame.data && frame.data.metrics) pushMetrics(frame.data.metrics);
      return;
    }
    if (frame.type === "metrics") {
      pushMetrics(frame.data);
      return;
    }
    if (frame.type === "event") {
      pushEvent(frame.data.event || "event", frame.data);
      if (frame.data.event === "run_completed") {
        fetchRecovery(currentRunId);
      }
      return;
    }
    // heartbeats: ignore
  }

  async function fetchRecovery(runId) {
    try {
      const r = await fetch(`/runs/${runId}`);
      if (!r.ok) return;
      const run = await r.json();
      reportPre.textContent = JSON.stringify(run, null, 2);
    } catch (_e) {}
  }

  // -- kill switch -------------------------------------------------------
  $("#kill-switch").addEventListener("click", async () => {
    if (!confirm("Abort ALL active experiments?")) return;
    const r = await fetch("/admin/abort", { method: "POST" });
    const body = r.ok ? await r.json() : { error: r.status };
    alert(`Kill switch: ${JSON.stringify(body)}`);
  });

  // -- circuit breaker poll ---------------------------------------------
  async function pollCircuitBreaker() {
    try {
      const r = await fetch("/admin/circuit-breaker-state");
      if (!r.ok) return;
      const body = await r.json();
      cbIndicator.dataset.tripped = body.tripped ? "true" : "false";
      cbText.textContent = `Circuit breaker: ${body.tripped ? "TRIPPED" : "ok"} (trips: ${body.total_trips})`;
    } catch (_e) {}
  }

  $("#refresh-list").addEventListener("click", refreshExperiments);

  // -- bootstrap ---------------------------------------------------------
  refreshExperiments();
  pollCircuitBreaker();
  setInterval(pollCircuitBreaker, 5000);
})();

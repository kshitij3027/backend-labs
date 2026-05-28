(function () {
  "use strict";
  const REFRESH_MS = window.REFRESH_MS || 2000;
  const STAGES = ["parse", "validate", "transform", "write"];
  const COLORS = {
    parse: "#60a5fa", validate: "#34d399",
    transform: "#fbbf24", write: "#f87171",
  };

  function makeChart(canvasId, ylabel) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: STAGES.map((s) => ({
          label: s, data: [], borderColor: COLORS[s],
          backgroundColor: "transparent", borderWidth: 2, tension: 0.2, pointRadius: 0,
        })),
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        scales: {
          x: { ticks: { color: "#6b7280" }, grid: { color: "#2a2f3a" } },
          y: {
            beginAtZero: true, ticks: { color: "#6b7280" },
            grid: { color: "#2a2f3a" },
            title: { text: ylabel, color: "#6b7280", display: true },
          },
        },
        plugins: { legend: { labels: { color: "#e8e8ea" } } },
      },
    });
  }

  function makeThroughputChart() {
    const ctx = document.getElementById("throughput-chart").getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: { labels: [], datasets: [{ label: "samples", data: [], borderColor: "#a78bfa", borderWidth: 2, tension: 0.2, pointRadius: 0 }] },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        scales: { x: { ticks: { color: "#6b7280" }, grid: { color: "#2a2f3a" } }, y: { beginAtZero: true, ticks: { color: "#6b7280" }, grid: { color: "#2a2f3a" } } },
        plugins: { legend: { labels: { color: "#e8e8ea" } } },
      },
    });
  }

  const charts = {
    cpu: makeChart("cpu-chart", "CPU %"),
    mem: makeChart("mem-chart", "MB"),
    queue: makeChart("queue-chart", "depth"),
    throughput: makeThroughputChart(),
  };

  function updateStagedChart(chart, samples, key) {
    const buckets = {};
    STAGES.forEach((s) => { buckets[s] = []; });
    samples.forEach((s) => {
      if (buckets[s.stage] !== undefined) buckets[s.stage].push(s[key]);
    });
    const maxLen = Math.max(...STAGES.map((s) => buckets[s].length), 1);
    chart.data.labels = Array.from({ length: maxLen }, (_, i) => i);
    chart.data.datasets.forEach((ds, idx) => {
      ds.data = buckets[STAGES[idx]];
    });
    chart.update("none");
  }

  function updateThroughputChart(chart, samples) {
    const buckets = {};
    samples.forEach((s) => {
      const second = Math.floor(s.ts);
      buckets[second] = (buckets[second] || 0) + 1;
    });
    const keys = Object.keys(buckets).sort();
    chart.data.labels = keys.map((k) => new Date(parseInt(k, 10) * 1000).toLocaleTimeString());
    chart.data.datasets[0].data = keys.map((k) => buckets[k]);
    chart.update("none");
  }

  async function loadOptimizations() {
    try {
      const r = await fetch("/api/optimizations");
      const items = await r.json();
      const sel = document.getElementById("opt-select");
      items.forEach((it) => {
        const opt = document.createElement("option");
        opt.value = it.name;
        opt.textContent = `${it.name} — ${it.description}`;
        sel.appendChild(opt);
      });
    } catch (e) { console.error("loadOptimizations", e); }
  }

  async function refresh() {
    try {
      const snap = await fetch("/api/metrics/snapshot?window_sec=60").then((r) => r.json());
      const samples = snap.samples || [];
      updateStagedChart(charts.cpu, samples, "cpu_pct");
      updateStagedChart(charts.mem, samples, "mem_mb");
      updateStagedChart(charts.queue, samples, "queue_depth");
      updateThroughputChart(charts.throughput, samples);

      const runs = await fetch("/api/runs?limit=10").then((r) => r.json());
      const recentEl = document.getElementById("recent-runs");
      if (runs.length === 0) {
        recentEl.innerHTML = '<li class="muted">no runs yet</li>';
      } else {
        recentEl.innerHTML = "";
        runs.forEach((r) => {
          const li = document.createElement("li");
          const a = document.createElement("a");
          a.href = `/api/runs/${r.run_id}`;
          a.textContent = `${r.run_id.slice(0,8)} · ${r.baseline_or_optimized}${r.optimization_name ? " ("+r.optimization_name+")" : ""} · ${r.throughput_lps.toFixed(1)} lps`;
          a.style.color = "#93c5fd";
          li.appendChild(a);
          recentEl.appendChild(li);
        });
      }

      // banner + recs from the most recent run
      if (runs.length > 0) {
        const latest = runs[0];
        const bn = await fetch(`/api/runs/${latest.run_id}/bottlenecks`).then((r) => r.json());
        const rec = await fetch(`/api/runs/${latest.run_id}/recommendations`).then((r) => r.json());
        const banner = document.getElementById("bottleneck-banner");
        if (bn && bn.length > 0) {
          banner.hidden = false;
          banner.querySelector("#bn-text").textContent =
            bn.map((b) => `${b.type} on ${b.stage} (${b.severity}, z=${b.z_score.toFixed(2)})`).join("; ");
        } else {
          banner.hidden = true;
        }
        const recsEl = document.getElementById("recs");
        if (rec && rec.length > 0) {
          recsEl.innerHTML = "";
          rec.forEach((r) => {
            const li = document.createElement("li");
            li.innerHTML = `<div class="suggestion">${r.suggestion}</div><div class="impact">${r.expected_impact}${r.optimization_name ? " · opt=" + r.optimization_name : ""}</div>`;
            recsEl.appendChild(li);
          });
        } else {
          recsEl.innerHTML = '<li class="muted">no recommendations yet</li>';
        }
      }
    } catch (e) { console.error("refresh", e); }
  }

  async function startBaseline() {
    const status = document.getElementById("run-status");
    status.textContent = "running baseline…";
    try {
      const r = await fetch("/api/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ log_count: 1000, concurrency: 4, seed: 42 }),
      });
      const data = await r.json();
      status.textContent = `started ${data.run_id.slice(0,8)} (${data.mode})`;
    } catch (e) { status.textContent = "error"; }
  }

  async function applyOptimization() {
    const sel = document.getElementById("opt-select");
    const name = sel.value;
    if (!name) return;
    const status = document.getElementById("run-status");
    status.textContent = `running compare with ${name}…`;
    try {
      const r = await fetch("/api/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ log_count: 1000, concurrency: 4, seed: 42, optimization_name: name }),
      });
      const data = await r.json();
      status.textContent = `started ${data.run_id.slice(0,8)} (${data.mode}) — open Compare view when complete`;
    } catch (e) { status.textContent = "error"; }
  }

  document.addEventListener("DOMContentLoaded", () => {
    loadOptimizations();
    document.getElementById("start-baseline").addEventListener("click", startBaseline);
    document.getElementById("apply-opt").addEventListener("click", applyOptimization);
    document.getElementById("opt-select").addEventListener("change", (e) => {
      document.getElementById("apply-opt").disabled = !e.target.value;
    });
    refresh();
    setInterval(refresh, REFRESH_MS);
  });
})();

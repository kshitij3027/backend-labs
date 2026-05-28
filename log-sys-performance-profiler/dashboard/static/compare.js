(function () {
  "use strict";
  const STAGES = ["parse", "validate", "transform", "write"];
  const COLORS = {
    parse: "#60a5fa", validate: "#34d399",
    transform: "#fbbf24", write: "#f87171",
  };

  function getParam(name) {
    return new URLSearchParams(window.location.search).get(name);
  }

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
          y: { beginAtZero: true, ticks: { color: "#6b7280" }, grid: { color: "#2a2f3a" },
               title: { text: ylabel, color: "#6b7280", display: true } },
        },
        plugins: { legend: { labels: { color: "#e8e8ea" } } },
      },
    });
  }

  function makeThroughputChart(canvasId) {
    const ctx = document.getElementById(canvasId).getContext("2d");
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

  function populateStagedChart(chart, samples, key) {
    const buckets = {};
    STAGES.forEach((s) => { buckets[s] = []; });
    samples.forEach((s) => {
      if (buckets[s.stage] !== undefined) buckets[s.stage].push(s[key]);
    });
    const maxLen = Math.max(...STAGES.map((s) => buckets[s].length), 1);
    chart.data.labels = Array.from({ length: maxLen }, (_, i) => i);
    chart.data.datasets.forEach((ds, idx) => { ds.data = buckets[STAGES[idx]]; });
    chart.update("none");
  }

  function populateThroughputChart(chart, samples) {
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

  function renderSummary(diff, baseline, optimized) {
    const verdictColor = {
      improved: "#34d399", regressed: "#f87171", neutral: "#9ca3af",
    }[diff.verdict] || "#9ca3af";
    const el = document.getElementById("diff-summary");
    const fmt = (x) => (x >= 0 ? "+" : "") + x.toFixed(1) + "%";
    el.innerHTML =
      `<div style="display:flex;gap:2rem;align-items:center;flex-wrap:wrap">` +
        `<div><strong style="color:${verdictColor}">${diff.verdict.toUpperCase()}</strong></div>` +
        `<div><span class="muted">opt:</span> ${diff.optimization_name}</div>` +
        `<div><span class="muted">throughput Δ:</span> <strong>${fmt(diff.throughput_delta_pct)}</strong></div>` +
        `<div><span class="muted">p95 Δ:</span> <strong>${fmt(diff.p95_delta_pct)}</strong></div>` +
        `<div><span class="muted">p99 Δ:</span> <strong>${fmt(diff.p99_delta_pct)}</strong></div>` +
        `<div><span class="muted">peak CPU Δ:</span> ${fmt(diff.peak_cpu_delta_pct)}</div>` +
        `<div><span class="muted">peak mem Δ:</span> ${fmt(diff.peak_mem_delta_pct)}</div>` +
      `</div>` +
      `<div style="margin-top:0.5rem;font-size:0.85rem" class="muted">` +
        `baseline ${baseline.run_id.slice(0,8)} · ${baseline.throughput_lps.toFixed(1)} lps · p95 ${baseline.p95_ms.toFixed(2)}ms` +
        ` &nbsp;|&nbsp; ` +
        `optimized ${optimized.run_id.slice(0,8)} · ${optimized.throughput_lps.toFixed(1)} lps · p95 ${optimized.p95_ms.toFixed(2)}ms` +
      `</div>`;
  }

  async function loadCompare() {
    const a = getParam("a");
    const b = getParam("b");
    if (!a || !b) {
      document.getElementById("diff-summary").innerHTML =
        '<div class="muted">missing ?a=&lt;baseline_run_id&gt;&b=&lt;optimized_run_id&gt; query parameters</div>';
      return;
    }
    let data;
    try {
      const r = await fetch(`/api/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
      if (!r.ok) {
        document.getElementById("diff-summary").innerHTML =
          `<div class="muted">/api/compare returned ${r.status}</div>`;
        return;
      }
      data = await r.json();
    } catch (e) {
      document.getElementById("diff-summary").innerHTML =
        `<div class="muted">error loading comparison: ${e.message}</div>`;
      return;
    }
    renderSummary(data.diff, data.baseline, data.optimized);

    const baselineCharts = {
      cpu: makeChart("baseline-cpu", "CPU %"),
      mem: makeChart("baseline-mem", "MB"),
      queue: makeChart("baseline-queue", "depth"),
      throughput: makeThroughputChart("baseline-throughput"),
    };
    const optimizedCharts = {
      cpu: makeChart("optimized-cpu", "CPU %"),
      mem: makeChart("optimized-mem", "MB"),
      queue: makeChart("optimized-queue", "depth"),
      throughput: makeThroughputChart("optimized-throughput"),
    };

    populateStagedChart(baselineCharts.cpu, data.baseline.samples, "cpu_pct");
    populateStagedChart(baselineCharts.mem, data.baseline.samples, "mem_mb");
    populateStagedChart(baselineCharts.queue, data.baseline.samples, "queue_depth");
    populateThroughputChart(baselineCharts.throughput, data.baseline.samples);

    populateStagedChart(optimizedCharts.cpu, data.optimized.samples, "cpu_pct");
    populateStagedChart(optimizedCharts.mem, data.optimized.samples, "mem_mb");
    populateStagedChart(optimizedCharts.queue, data.optimized.samples, "queue_depth");
    populateThroughputChart(optimizedCharts.throughput, data.optimized.samples);
  }

  document.addEventListener("DOMContentLoaded", loadCompare);
})();

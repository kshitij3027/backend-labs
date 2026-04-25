// Vanilla JS dashboard for the log search + reranker.
// Talks to /api/search, /api/search/suggestions, /api/search/stats, /api/logs/bulk.

const $ = (sel) => document.querySelector(sel);

const state = {
  suggestionTimer: null,
};

document.addEventListener("DOMContentLoaded", () => {
  $("#search-form").addEventListener("submit", (ev) => {
    ev.preventDefault();
    runSearch();
  });
  $("#search-input").addEventListener("input", scheduleSuggestions);
  $("#search-input").addEventListener("focus", scheduleSuggestions);
  $("#search-input").addEventListener("blur", () => {
    // Delay so a click on a suggestion can register first.
    setTimeout(() => $("#suggestions").hidden = true, 150);
  });
  $("#seed-btn").addEventListener("click", runSeed);
  refreshStats();
});

function showError(msg) {
  const banner = $("#error-banner");
  banner.textContent = msg;
  banner.hidden = false;
  setTimeout(() => banner.hidden = true, 8000);
}

function scheduleSuggestions() {
  const q = $("#search-input").value.trim();
  if (state.suggestionTimer) clearTimeout(state.suggestionTimer);
  if (!q) { $("#suggestions").hidden = true; return; }
  state.suggestionTimer = setTimeout(() => fetchSuggestions(q), 120);
}

async function fetchSuggestions(q) {
  try {
    const r = await fetch(`/api/search/suggestions?q=${encodeURIComponent(q)}&limit=8`);
    if (!r.ok) return;
    const data = await r.json();
    const ul = $("#suggestions");
    ul.innerHTML = "";
    if (!data.suggestions.length) { ul.hidden = true; return; }
    for (const s of data.suggestions) {
      const li = document.createElement("li");
      li.textContent = s;
      li.addEventListener("mousedown", () => {
        $("#search-input").value = s;
        ul.hidden = true;
      });
      ul.appendChild(li);
    }
    ul.hidden = false;
  } catch (e) {
    // Silent — suggestions are best-effort.
  }
}

async function runSearch() {
  const query = $("#search-input").value.trim();
  if (!query) return;
  const limit = parseInt($("#limit-input").value, 10) || 10;
  const mode = $("#mode-select").value;
  const body = { query, limit };
  if (mode) body.context = { mode };

  $("#results").innerHTML = '<p class="muted">Searching...</p>';
  try {
    const r = await fetch("/api/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.text().catch(() => r.statusText);
      throw new Error(`Search failed: ${r.status} ${err}`);
    }
    const data = await r.json();
    renderResults(data);
    refreshStats();
  } catch (e) {
    showError(e.message);
    $("#results").innerHTML = "";
  }
}

function renderResults(data) {
  const root = $("#results");
  root.innerHTML = "";
  const summary = document.createElement("p");
  summary.className = "muted";
  summary.innerHTML = `intent <strong>${data.intent}</strong>, expanded [${(data.expanded_terms || []).map(t => `<code>${t}</code>`).join(", ")}], ${data.ranked_hits}/${data.total_hits} ranked, took ${data.execution_time_ms}ms`;
  root.appendChild(summary);
  if (!data.results.length) {
    const empty = document.createElement("p");
    empty.textContent = "No matches.";
    root.appendChild(empty);
    return;
  }
  for (const hit of data.results) {
    root.appendChild(renderCard(hit));
  }
}

function renderCard(hit) {
  const card = document.createElement("article");
  card.className = "result-card";
  card.dataset.testid = "result-card";
  const level = (hit.level || "INFO").toUpperCase();
  const ts = new Date(hit.timestamp * 1000).toISOString();
  card.innerHTML = `
    <div class="result-head">
      <span class="score" data-testid="score">${Number(hit.score).toFixed(4)}</span>
      <span class="level-pill ${level}">${level}</span>
      <span class="service-tag">${escapeHtml(hit.service || "unknown")}</span>
      <span class="ts">${ts}</span>
      <button class="explain-toggle" type="button">Why this rank?</button>
    </div>
    <div class="message">${escapeHtml(hit.log_entry || "")}</div>
    <div class="explain">
      <div class="factor-grid">
        <span class="label">tfidf</span>     <span>${num(hit.ranking_explanation?.tfidf)}</span>
        <span class="label">temporal</span>  <span>${num(hit.ranking_explanation?.temporal)}</span>
        <span class="label">severity</span>  <span>${num(hit.ranking_explanation?.severity)}</span>
        <span class="label">service</span>   <span>${num(hit.ranking_explanation?.service)}</span>
        <span class="label">context</span>   <span>${num(hit.ranking_explanation?.context)}</span>
      </div>
      <div class="reasons"><strong>Reasons:</strong> ${(hit.ranking_explanation?.reasons || []).map(r => escapeHtml(r)).join(", ") || "—"}</div>
    </div>
  `;
  card.querySelector(".explain-toggle").addEventListener("click", () => {
    card.classList.toggle("expanded");
  });
  return card;
}

function num(v) { return v === undefined || v === null ? "—" : Number(v).toFixed(3); }
function escapeHtml(s) {
  return String(s).replace(/[&<>'"]/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
  }[c]));
}

async function runSeed() {
  const status = $("#seed-status");
  status.textContent = "Seeding...";
  try {
    const r = await fetch("/api/sample/seed?count=500", { method: "POST" });
    if (!r.ok) throw new Error(`Seed failed: ${r.status}`);
    const data = await r.json();
    status.textContent = `Seeded ${data.accepted} entries (versions: ${data.index_version}).`;
    refreshStats();
  } catch (e) {
    status.textContent = "";
    showError(e.message);
  }
}

async function refreshStats() {
  try {
    const r = await fetch("/api/search/stats");
    if (!r.ok) return;
    const data = await r.json();
    const root = $("#stats");
    root.innerHTML = "";
    const cards = [
      ["Total docs",   data.total_docs],
      ["Unique tokens", data.unique_tokens],
      ["Index version", data.index_version],
      ["IDF version",   data.idf_version],
      ["Cache hit %",   ((data.cache_hit_ratio || 0) * 100).toFixed(1) + "%"],
      ["p95 latency",   (data.p95_latency_ms || 0).toFixed(2) + "ms"],
    ];
    for (const [label, value] of cards) {
      const div = document.createElement("div");
      div.className = "stat-card";
      div.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
      root.appendChild(div);
    }
  } catch (e) {
    // Silent — stats are best-effort.
  }
}

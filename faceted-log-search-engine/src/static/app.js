// Faceted Log Search dashboard - vanilla ES2022, no build step.
//
// State lives in a single object so it's easy to trace.
// All fetches go through `fetchSearch`, which aborts stale in-flight
// requests via AbortController. The facet panel uses event delegation
// to avoid re-wiring listeners after every re-render.

const API = "/api";
const MAX_FACET_DISPLAY = 8;

const state = {
  query: "",
  filters: {
    service: [],
    level: [],
    region: [],
    latency_bucket: [],
    hour_bucket: [],
  },
  cursor: null,
  expandedFacets: new Set(),
  limit: 20,
};

let inflight = null; // AbortController of the active fetch

// --------------------------- helpers ---------------------------------

function debounce(fn, ms) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function escapeHtml(s) {
  if (s == null) return "";
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function highlight(text, query) {
  const escaped = escapeHtml(text);
  if (!query || !query.trim()) return escaped;
  const re = new RegExp(escapeRegex(query.trim()), "gi");
  return escaped.replace(re, (m) => `<mark>${m}</mark>`);
}

function formatTs(tsEpoch) {
  if (tsEpoch == null) return "";
  return new Date(tsEpoch * 1000).toISOString().replace("T", " ").slice(0, 19);
}

function activeFilterCount() {
  return Object.values(state.filters).reduce((n, arr) => n + arr.length, 0);
}

// --------------------------- networking ------------------------------

async function fetchSearch(opts = {}) {
  if (inflight) inflight.abort();
  inflight = new AbortController();

  const body = {
    query: state.query || null,
    filters: state.filters,
    cursor: opts.append ? state.cursor : null,
    limit: state.limit,
  };

  try {
    const res = await fetch(`${API}/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: inflight.signal,
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const data = await res.json();
    render(data, opts);
    hideError();
  } catch (err) {
    if (err.name === "AbortError") return; // stale request, ignore
    console.error("search failed", err);
    showError(`Search failed: ${err.message}`);
  }
}

async function generateLogs(count = 1000) {
  const btn = document.getElementById("generate-btn");
  btn.disabled = true;
  const origText = btn.textContent;
  btn.textContent = `Generating ${count}...`;
  try {
    const res = await fetch(`${API}/logs/generate?count=${count}`, {
      method: "POST",
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.cursor = null;
    await fetchSearch({ append: false });
    hideError();
  } catch (err) {
    console.error("generate failed", err);
    showError(`Generate failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = origText;
  }
}

function clearFilters() {
  state.query = "";
  state.filters = {
    service: [],
    level: [],
    region: [],
    latency_bucket: [],
    hour_bucket: [],
  };
  state.cursor = null;
  state.expandedFacets.clear();
  const input = document.getElementById("query-input");
  if (input) input.value = "";
  fetchSearch({ append: false });
}

// --------------------------- render ----------------------------------

function render(response, opts) {
  renderFacets(response.facets || []);
  renderResults(response.logs || [], Boolean(opts.append));
  updateStats(response);
  state.cursor = response.next_cursor ?? null;
  const loadBtn = document.getElementById("load-more-btn");
  if (loadBtn) loadBtn.hidden = !response.has_more;
}

function renderFacets(facets) {
  const panel = document.getElementById("facet-panel");
  if (!facets.length) {
    panel.innerHTML = '<div class="facet-empty-state">No facets yet.</div>';
    return;
  }
  const parts = [];
  for (const f of facets) {
    const expanded = state.expandedFacets.has(f.name);
    const values = f.values || [];
    parts.push(`<fieldset data-facet="${escapeHtml(f.name)}">`);
    parts.push(`<legend>${escapeHtml(f.display_name)}</legend>`);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      const isZero = v.count === 0 && v.selected;
      const hidden = i >= MAX_FACET_DISPLAY && !expanded ? " facet-hidden" : "";
      const emptyClass = isZero ? " facet-empty" : "";
      const id = `facet-${f.name}-${String(v.value).replaceAll(/[^a-zA-Z0-9_-]/g, "_")}`;
      parts.push(
        `<div class="facet-item${hidden}${emptyClass}">` +
          `<input type="checkbox" id="${id}" ` +
          `data-facet="${escapeHtml(f.name)}" ` +
          `data-value="${escapeHtml(String(v.value))}" ` +
          (v.selected ? "checked " : "") +
          `/>` +
          `<label for="${id}">` +
          `${escapeHtml(String(v.value))} ` +
          `<span class="facet-count">(${v.count})</span>` +
          `</label>` +
          `</div>`
      );
    }
    if (values.length > MAX_FACET_DISPLAY) {
      const hiddenCount = values.length - MAX_FACET_DISPLAY;
      const label = expanded ? "Show less" : `Show more (${hiddenCount} more)`;
      parts.push(
        `<button type="button" class="show-more" data-facet="${escapeHtml(f.name)}">` +
          `${label}</button>`
      );
    } else if (f.has_more_values) {
      parts.push(
        `<span class="facet-count">+${escapeHtml(String(f.has_more_values))} more on server</span>`
      );
    }
    parts.push(`</fieldset>`);
  }
  panel.innerHTML = parts.join("");
}

function renderResults(logs, append) {
  const container = document.getElementById("results");
  if (!append) container.innerHTML = "";
  if (!logs.length && !append) {
    container.innerHTML = '<div class="results-empty-state">No logs match the current filters.</div>';
    return;
  }
  const parts = [];
  for (const log of logs) {
    const level = log.level || "INFO";
    const badgeClass = `level-${escapeHtml(level)}`;
    parts.push(
      `<article class="log-row">` +
        `<div class="log-header">` +
        `<span class="level-badge ${badgeClass}">${escapeHtml(level)}</span>` +
        `<span class="log-ts">${escapeHtml(formatTs(log.ts))}</span>` +
        `<span class="log-service">${escapeHtml(log.service || "")}</span>` +
        `<span class="log-region">${escapeHtml(log.region || "")}</span>` +
        `<span class="log-latency">${escapeHtml(String(log.response_time_ms ?? "0"))}ms</span>` +
        `</div>` +
        `<div class="log-message">${highlight(log.message || "", state.query)}</div>` +
        `</article>`
    );
  }
  container.insertAdjacentHTML("beforeend", parts.join(""));
}

function updateStats(response) {
  const logs = response.logs || [];
  const total = response.total_count != null ? response.total_count : logs.length;
  const hasMore = response.has_more ? "+" : "";
  document.getElementById("stat-total").textContent = `${total}${hasMore}`;
  const qt = Number(response.query_time_ms ?? 0).toFixed(1);
  document.getElementById("stat-time").textContent = `${qt}ms`;
  document.getElementById("stat-filters").textContent = activeFilterCount();
  document.getElementById("stat-cache").textContent = response.cached ? "hit" : "miss";
}

// --------------------------- errors ----------------------------------

function showError(msg) {
  const bar = document.getElementById("error-bar");
  if (!bar) return;
  bar.textContent = msg;
  bar.hidden = false;
}
function hideError() {
  const bar = document.getElementById("error-bar");
  if (bar) bar.hidden = true;
}

// --------------------------- events ----------------------------------

function onFacetChange(e) {
  const target = e.target;
  if (target.matches('input[type="checkbox"][data-facet]')) {
    const facet = target.dataset.facet;
    const value = target.dataset.value;
    if (!facet || !(facet in state.filters)) return;
    const list = state.filters[facet];
    // hour_bucket needs to be an int on the wire.
    const coerced = facet === "hour_bucket" ? Number(value) : value;
    if (target.checked) {
      if (!list.some((v) => String(v) === String(coerced))) list.push(coerced);
    } else {
      state.filters[facet] = list.filter((v) => String(v) !== String(coerced));
    }
    state.cursor = null;
    fetchSearch({ append: false });
    return;
  }
  if (target.matches("button.show-more[data-facet]")) {
    const facet = target.dataset.facet;
    if (state.expandedFacets.has(facet)) {
      state.expandedFacets.delete(facet);
    } else {
      state.expandedFacets.add(facet);
    }
    // Only re-toggle visibility classes locally rather than re-fetch.
    const fs = document.querySelector(`fieldset[data-facet="${facet}"]`);
    if (fs) {
      const items = fs.querySelectorAll(".facet-item");
      const expanded = state.expandedFacets.has(facet);
      items.forEach((el, i) => {
        if (i >= MAX_FACET_DISPLAY) el.classList.toggle("facet-hidden", !expanded);
      });
      const hiddenCount = items.length - MAX_FACET_DISPLAY;
      target.textContent = expanded ? "Show less" : `Show more (${hiddenCount} more)`;
    }
  }
}

function setupEventListeners() {
  const input = document.getElementById("query-input");
  const onQuery = debounce((val) => {
    state.query = val;
    state.cursor = null;
    fetchSearch({ append: false });
  }, 200);
  input.addEventListener("input", (e) => onQuery(e.target.value));

  document.getElementById("facet-panel").addEventListener("click", onFacetChange);
  document.getElementById("facet-panel").addEventListener("change", onFacetChange);

  document.getElementById("generate-btn").addEventListener("click", () => generateLogs(1000));
  document.getElementById("clear-btn").addEventListener("click", clearFilters);
  document.getElementById("load-more-btn").addEventListener("click", () => {
    if (state.cursor != null) fetchSearch({ append: true });
  });
}

// --------------------------- boot ------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  setupEventListeners();
  fetchSearch();
});

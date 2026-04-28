// Vanilla ES2020 dashboard for the Log Search API.
//
// Reads the API prefix from <body data-api-prefix="...">, manages a JWT in
// localStorage, drives the search form, renders results + aggregations, and
// surfaces 4xx/5xx envelopes as toasts.

(function () {
  "use strict";

  const TOKEN_KEY = "lsa_token";
  const USERNAME_KEY = "lsa_username";

  const apiPrefix = (document.body.dataset.apiPrefix || "/api/v1").replace(/\/$/, "");

  // ---- DOM refs ----------------------------------------------------------
  const loginForm = document.getElementById("login-form");
  const usernameEl = document.getElementById("username");
  const passwordEl = document.getElementById("password");
  const loginBtn = document.getElementById("login-button");
  const logoutBtn = document.getElementById("logout-button");
  const authStatus = document.getElementById("auth-status");

  const searchForm = document.getElementById("search-form");
  const searchBtn = document.getElementById("search-button");
  const qEl = document.getElementById("q");
  const levelsEl = document.getElementById("levels");
  const servicesEl = document.getElementById("services");
  const startTimeEl = document.getElementById("start_time");
  const endTimeEl = document.getElementById("end_time");
  const sortByEl = document.getElementById("sort_by");
  const sortOrderEl = document.getElementById("sort_order");
  const limitEl = document.getElementById("limit");
  const offsetEl = document.getElementById("offset");

  const resultMeta = document.getElementById("result-meta");
  const badgeTotal = document.getElementById("badge-total");
  const badgeExec = document.getElementById("badge-exec");
  const badgeCache = document.getElementById("badge-cache");
  const resultsBody = document.getElementById("results-body");
  const prevBtn = document.getElementById("prev-button");
  const nextBtn = document.getElementById("next-button");
  const pageInfo = document.getElementById("page-info");

  const aggLevels = document.getElementById("agg-levels");
  const aggServices = document.getElementById("agg-services");
  const aggTimeline = document.getElementById("agg-timeline");

  const toastContainer = document.getElementById("toast-container");

  // ---- helpers -----------------------------------------------------------
  function getToken() {
    return localStorage.getItem(TOKEN_KEY);
  }
  function setToken(token, username) {
    localStorage.setItem(TOKEN_KEY, token);
    if (username) {
      localStorage.setItem(USERNAME_KEY, username);
    }
  }
  function clearToken() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USERNAME_KEY);
  }

  function refreshAuthUI() {
    const token = getToken();
    const username = localStorage.getItem(USERNAME_KEY) || "user";
    if (token) {
      authStatus.textContent = "logged in as " + username;
      authStatus.classList.add("ok");
      loginBtn.hidden = true;
      logoutBtn.hidden = false;
      usernameEl.disabled = true;
      passwordEl.disabled = true;
      searchBtn.disabled = false;
    } else {
      authStatus.textContent = "not signed in";
      authStatus.classList.remove("ok");
      loginBtn.hidden = false;
      logoutBtn.hidden = true;
      usernameEl.disabled = false;
      passwordEl.disabled = false;
      searchBtn.disabled = true;
    }
  }

  function showToast(message, kind, requestId) {
    const node = document.createElement("div");
    node.className = "toast" + (kind ? " toast-" + kind : "");
    const text = document.createElement("div");
    text.className = "toast-message";
    text.textContent = message;
    node.appendChild(text);
    if (requestId) {
      const meta = document.createElement("div");
      meta.className = "toast-meta";
      meta.textContent = "request_id: " + requestId;
      node.appendChild(meta);
    }
    toastContainer.appendChild(node);
    setTimeout(function () {
      node.classList.add("dismiss");
      setTimeout(function () { node.remove(); }, 300);
    }, 5000);
  }

  function fmtTimestamp(iso) {
    if (!iso) return "";
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return iso;
      return d.toISOString().replace("T", " ").replace("Z", " UTC");
    } catch (e) {
      return iso;
    }
  }

  function isoOrNull(localValue) {
    if (!localValue) return null;
    // datetime-local has no zone; treat as local time and convert to UTC ISO.
    const d = new Date(localValue);
    if (isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  function selectedValues(selectEl) {
    return Array.from(selectEl.selectedOptions).map(function (o) { return o.value; }).filter(Boolean);
  }

  function buildSearchPayload() {
    const payload = {};
    const q = qEl.value.trim();
    if (q) payload.q = q;

    const levels = selectedValues(levelsEl);
    if (levels.length > 0) payload.levels = levels;

    const services = selectedValues(servicesEl);
    if (services.length > 0) payload.services = services;

    const startIso = isoOrNull(startTimeEl.value);
    if (startIso) payload.start_time = startIso;

    const endIso = isoOrNull(endTimeEl.value);
    if (endIso) payload.end_time = endIso;

    const limit = parseInt(limitEl.value, 10);
    if (!isNaN(limit)) payload.limit = limit;
    const offset = parseInt(offsetEl.value, 10);
    if (!isNaN(offset)) payload.offset = offset;

    payload.sort_by = sortByEl.value || "relevance";
    payload.sort_order = sortOrderEl.value || "desc";

    return payload;
  }

  // ---- API calls ---------------------------------------------------------
  async function postJson(path, body, useAuth) {
    const headers = { "Content-Type": "application/json", "Accept": "application/json" };
    if (useAuth) {
      const t = getToken();
      if (!t) throw new AuthError("not authenticated");
      headers["Authorization"] = "Bearer " + t;
    }
    const resp = await fetch(apiPrefix + path, {
      method: "POST",
      headers: headers,
      body: JSON.stringify(body || {}),
    });
    return await handleResponse(resp);
  }

  async function postForm(path, params) {
    const body = new URLSearchParams();
    Object.keys(params).forEach(function (k) { body.append(k, params[k]); });
    const resp = await fetch(apiPrefix + path, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
      },
      body: body.toString(),
    });
    return await handleResponse(resp);
  }

  function AuthError(message) { this.name = "AuthError"; this.message = message; }
  AuthError.prototype = Object.create(Error.prototype);

  async function handleResponse(resp) {
    let bodyText;
    try { bodyText = await resp.text(); } catch (e) { bodyText = ""; }
    let body = null;
    if (bodyText) {
      try { body = JSON.parse(bodyText); } catch (e) { body = null; }
    }
    if (resp.status === 401) {
      clearToken();
      refreshAuthUI();
      const msg = (body && body.error && body.error.message) || "session expired — please sign in again";
      showToast(msg, "error", body && body.request_id);
      throw new AuthError(msg);
    }
    if (resp.status === 429) {
      const retry = resp.headers.get("Retry-After") || "?";
      const reset = resp.headers.get("X-RateLimit-Reset");
      const msg = "rate limited — retry in " + retry + "s" + (reset ? " (reset=" + reset + ")" : "");
      showToast(msg, "warn", body && body.request_id);
      const err = new Error(msg);
      err.status = 429;
      throw err;
    }
    if (!resp.ok) {
      const msg = (body && body.error && body.error.message) || ("HTTP " + resp.status);
      showToast(msg, "error", body && body.request_id);
      const err = new Error(msg);
      err.status = resp.status;
      throw err;
    }
    return body;
  }

  // ---- handlers ----------------------------------------------------------
  loginForm.addEventListener("submit", async function (ev) {
    ev.preventDefault();
    const username = usernameEl.value.trim();
    const password = passwordEl.value;
    if (!username || !password) return;
    try {
      const body = await postForm("/auth/token", { username: username, password: password });
      if (body && body.access_token) {
        setToken(body.access_token, username);
        refreshAuthUI();
        showToast("logged in", "ok");
      }
    } catch (e) {
      // toast already shown
    }
  });

  logoutBtn.addEventListener("click", function () {
    clearToken();
    refreshAuthUI();
    showToast("signed out", "ok");
  });

  searchForm.addEventListener("submit", async function (ev) {
    ev.preventDefault();
    if (!getToken()) {
      showToast("login required before search", "warn");
      return;
    }
    await runSearch();
  });

  prevBtn.addEventListener("click", function () {
    const limit = Math.max(1, parseInt(limitEl.value, 10) || 25);
    const offset = Math.max(0, (parseInt(offsetEl.value, 10) || 0) - limit);
    offsetEl.value = String(offset);
    runSearch();
  });

  nextBtn.addEventListener("click", function () {
    const limit = Math.max(1, parseInt(limitEl.value, 10) || 25);
    const offset = (parseInt(offsetEl.value, 10) || 0) + limit;
    offsetEl.value = String(offset);
    runSearch();
  });

  // ---- search execution + render ----------------------------------------
  async function runSearch() {
    searchBtn.disabled = true;
    try {
      const payload = buildSearchPayload();
      const body = await postJson("/logs/search", payload, true);
      renderResults(body);
      renderAggregations(body.aggregations);
      hydrateServiceOptions(body.aggregations);
    } catch (e) {
      // handled in handleResponse via toast
    } finally {
      if (getToken()) searchBtn.disabled = false;
    }
  }

  function renderResults(body) {
    resultMeta.hidden = false;
    badgeTotal.textContent = "total_hits: " + (body.total_hits || 0);
    badgeExec.textContent = "exec_ms: " + (body.execution_time_ms || 0).toFixed(2);
    badgeCache.textContent = "cache_hit: " + (body.cache_hit ? "yes" : "no");
    badgeCache.classList.toggle("ok", !!body.cache_hit);

    while (resultsBody.firstChild) resultsBody.removeChild(resultsBody.firstChild);

    const rows = body.results || [];
    if (rows.length === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.className = "empty";
      td.textContent = "No matching logs.";
      tr.appendChild(td);
      resultsBody.appendChild(tr);
    } else {
      rows.forEach(function (row) {
        const tr = document.createElement("tr");
        appendCell(tr, fmtTimestamp(row.timestamp));
        appendCell(tr, row.level || "", "level level-" + String(row.level || "").toLowerCase());
        appendCell(tr, row.service_name || "");
        appendCell(tr, row.message || "");
        appendCell(tr, row.score != null ? row.score.toFixed(3) : "");
        resultsBody.appendChild(tr);
      });
    }

    const pag = body.pagination || { offset: 0, limit: 25, has_more: false };
    pageInfo.textContent = "offset " + pag.offset + " · limit " + pag.limit;
    prevBtn.disabled = (pag.offset || 0) <= 0;
    nextBtn.disabled = !pag.has_more;
  }

  function appendCell(tr, text, className) {
    const td = document.createElement("td");
    td.textContent = text;
    if (className) td.className = className;
    tr.appendChild(td);
  }

  function renderAggregations(aggs) {
    if (!aggs) return;
    fillBars(aggLevels, aggs.levels || [], "key", "doc_count");
    fillBars(aggServices, aggs.services || [], "key", "doc_count");
    fillBars(aggTimeline, aggs.timeline || [], "key_as_string", "doc_count");
  }

  function fillBars(target, items, labelKey, countKey) {
    while (target.firstChild) target.removeChild(target.firstChild);
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "empty";
      li.textContent = "—";
      target.appendChild(li);
      return;
    }
    const max = items.reduce(function (m, b) { return Math.max(m, b[countKey] || 0); }, 1);
    items.forEach(function (b) {
      const li = document.createElement("li");
      const label = document.createElement("span");
      label.className = "bar-label";
      label.textContent = b[labelKey];
      const bar = document.createElement("span");
      bar.className = "bar-track";
      const fill = document.createElement("span");
      fill.className = "bar-fill";
      fill.style.width = Math.round(((b[countKey] || 0) / max) * 100) + "%";
      bar.appendChild(fill);
      const count = document.createElement("span");
      count.className = "bar-count";
      count.textContent = String(b[countKey] || 0);
      li.appendChild(label);
      li.appendChild(bar);
      li.appendChild(count);
      target.appendChild(li);
    });
  }

  function hydrateServiceOptions(aggs) {
    if (!aggs || !aggs.services) return;
    const existing = new Set(Array.from(servicesEl.options).map(function (o) { return o.value; }));
    let firstReal = null;
    aggs.services.forEach(function (b) {
      if (!existing.has(b.key)) {
        const opt = document.createElement("option");
        opt.value = b.key;
        opt.textContent = b.key + " (" + b.doc_count + ")";
        servicesEl.appendChild(opt);
        if (firstReal === null) firstReal = opt;
      }
    });
    // Drop the disabled placeholder once we have real options.
    if (firstReal && servicesEl.options.length > 0 && servicesEl.options[0].disabled) {
      servicesEl.removeChild(servicesEl.options[0]);
    }
  }

  // ---- bootstrap ---------------------------------------------------------
  refreshAuthUI();
})();

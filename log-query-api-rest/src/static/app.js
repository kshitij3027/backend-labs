// Vanilla ES2020 dashboard for the Log Query API. No framework, no bundler, no build step.
//
// The page is served by the API process itself (see `src/api/dashboard.py`), so every request
// below is SAME-ORIGIN and there is no CORS in this path at all — the `expose_headers` list in
// `src/main.py` exists for third-party clients, not for this file. That is also why the API
// prefix is a hardcoded constant rather than something templated in: there is no second origin
// it could ever point at.
//
// Four things this file is really demonstrating, in rough order of how much they matter:
//
//   1. Cursor pagination that RE-SENDS ITS FILTERS. The server binds a cursor to a fingerprint
//      of the query that minted it; a bare `?cursor=` is a 400, by design.
//   2. An SSE tail authenticated by query parameter, because `EventSource` cannot set headers.
//   3. The rate limiter made visible — every response carries `X-RateLimit-Remaining`, and this
//      page displays it instead of discovering the ceiling by hitting it.
//   4. 401 and 403 handled as the different things they are.

(function () {
  "use strict";

  var API = "/api/v1";
  var TOKEN_KEY = "lqa_token";
  var CLAIMS_KEY = "lqa_claims";

  var PAGE_SIZE = 50;
  var STATS_POLL_MS = 5000;
  // The tail is unbounded in time, so it must be bounded in DOM. The server drops a subscriber
  // that falls more than SSE_QUEUE_SIZE behind; a page that grew a row per frame forever would
  // be the same failure with extra steps.
  var TAIL_MAX_ROWS = 200;

  var LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
  var LEVEL_COLORS = {
    DEBUG: "#6c7893",
    INFO: "#4cc38a",
    WARN: "#f5a623",
    ERROR: "#ff6a6a",
    FATAL: "#ff3b3b"
  };

  // ---- tiny DOM helpers ---------------------------------------------------
  function $(testid, root) { return (root || document).querySelector('[data-testid="' + testid + '"]'); }
  function show(el, on) { if (el) el.hidden = !on; }
  function text(el, value) { if (el) el.textContent = value; }

  // ---- element refs -------------------------------------------------------
  var elLoginPanel = $("login-panel");
  var elLoginForm = $("login-form");
  var elUser = $("login-username");
  var elPass = $("login-password");
  var elLoginSubmit = $("login-submit");
  var elLoginError = $("login-error");
  var elDemo = $("demo-accounts");

  var elSignedIn = $("signed-in");
  var elChipRole = $("chip-role");
  var elChipTier = $("chip-tier");
  var elChipSubject = $("chip-subject");
  var elLogout = $("logout-button");

  var elRlBadge = $("ratelimit-badge");
  var elRlBanner = $("ratelimit-banner");
  var elErrBanner = $("error-banner");

  var elFilterForm = $("filter-form");
  var elFLevel = $("filter-level");
  var elFService = $("filter-service");
  var elFHost = $("filter-host");
  var elFQ = $("filter-q");
  var elFReset = $("filter-reset");

  var elTbody = $("log-tbody");
  var elTotal = $("log-total");
  var elShown = $("log-shown");
  var elLoadMore = $("load-more");
  var elLogStatus = $("log-status");

  var elStatTotal = $("stat-total");
  var elStatIngest = $("stat-ingest");
  var elStatResident = $("stat-resident");
  var elStatEvicted = $("stat-evicted");
  var elPollState = $("stats-poll-state");
  var elChartFallback = $("chart-fallback");
  var elCanvasLevels = $("chart-levels");
  var elCanvasBuckets = $("chart-buckets");

  var elTailToggle = $("tail-toggle");
  var elTailClear = $("tail-clear");
  var elTailRows = $("tail-rows");
  var elTailStatus = $("tail-status");
  var elTailState = $("tail-state");
  var elTailCount = $("tail-count");

  // ---- state --------------------------------------------------------------
  //
  // `pageFilters` is a SNAPSHOT of the filter bar taken when a page-1 request went out. Every
  // "load more" replays that snapshot alongside the cursor rather than re-reading the form,
  // because the server rejects a cursor whose filters no longer match the ones it was minted
  // under. Editing the form mid-scroll and then paging would otherwise be a 400 the user did
  // not do anything to deserve.
  var pageFilters = null;
  var nextCursor = null;
  var shownCount = 0;
  var statsTimer = null;
  var source = null;       // the EventSource, when connected
  var tailFrames = 0;
  var charts = { levels: null, buckets: null };

  // ---- token ---------------------------------------------------------------
  //
  // sessionStorage, not localStorage. This is a short-lived demo credential for a service whose
  // entire corpus is synthetic; it has no business outliving the tab that asked for it, and
  // sessionStorage is per-tab and cleared on close. Nothing here is a substitute for the
  // httpOnly cookie a real deployment would use — a token readable by JS is readable by any
  // script that gets onto the page.
  function getToken() { try { return sessionStorage.getItem(TOKEN_KEY); } catch (e) { return null; } }
  function setSession(token, claims) {
    try {
      sessionStorage.setItem(TOKEN_KEY, token);
      sessionStorage.setItem(CLAIMS_KEY, JSON.stringify(claims || {}));
    } catch (e) { /* private mode: the page still works for this pageview */ }
  }
  function getClaims() {
    try { return JSON.parse(sessionStorage.getItem(CLAIMS_KEY) || "{}"); } catch (e) { return {}; }
  }
  function clearSession() {
    try { sessionStorage.removeItem(TOKEN_KEY); sessionStorage.removeItem(CLAIMS_KEY); } catch (e) { /* ignore */ }
  }

  // ---- banners -------------------------------------------------------------
  function showError(msg) {
    text(elErrBanner, msg);
    show(elErrBanner, true);
  }
  function clearError() { show(elErrBanner, false); text(elErrBanner, ""); }

  function showRateLimited(retryAfter) {
    var secs = retryAfter ? Number(retryAfter) : null;
    text(elRlBanner,
      "429 Too Many Requests — the token bucket for this tier is empty." +
      (secs ? " Retry-After: " + secs + "s." : "") +
      " Rate limits are per-principal and sized by tier; see /docs.");
    show(elRlBanner, true);
    // Self-clearing, at the interval the server itself named. A banner that outlives the
    // condition it describes trains people to ignore banners.
    window.setTimeout(function () { show(elRlBanner, false); }, Math.max(1500, (secs || 2) * 1000));
  }

  // Read off EVERY response, success or failure — that is the whole point of the API stamping
  // the triple in middleware rather than only on rejection.
  function readRateLimit(res) {
    var remaining = res.headers.get("X-RateLimit-Remaining");
    var limit = res.headers.get("X-RateLimit-Limit");
    if (remaining === null) return;
    var n = Number(remaining);
    text(elRlBadge, "rate limit: " + remaining + (limit ? " / " + limit : "") + " left");
    elRlBadge.dataset.level = n <= 0 ? "none" : (n <= 5 ? "low" : "ok");
  }

  // ---- HTTP ----------------------------------------------------------------
  //
  // One choke point so the rate-limit badge, the 401 logout and the 403 message cannot be
  // forgotten at a call site. Rejects with an Error carrying `.status`.
  function api(path, options) {
    var opts = options || {};
    var headers = opts.headers || {};
    var token = getToken();
    if (token) headers["Authorization"] = "Bearer " + token;

    return fetch(path, {
      method: opts.method || "GET",
      headers: headers,
      body: opts.body
    }).then(function (res) {
      readRateLimit(res);

      if (res.ok) return res.json();

      return res.json().catch(function () { return {}; }).then(function (body) {
        var detail = (body && body.detail) || res.statusText || ("HTTP " + res.status);
        var err = new Error(detail);
        err.status = res.status;
        err.requestId = (body && body.request_id) || res.headers.get("X-Request-ID");

        if (res.status === 429) {
          showRateLimited(res.headers.get("Retry-After"));
        } else if (res.status === 401) {
          // "I don't know who you are" — the credential is gone or expired, so the only useful
          // thing is to ask for a new one.
          logout("Session expired or token rejected (401). Sign in again.");
        } else if (res.status === 403) {
          // "I know who you are, and no." A 403 is NOT an authentication failure and must not
          // log anyone out: the token is perfectly valid, the role simply does not reach. A
          // viewer clicking Connect on the live tail lands here, and bouncing them to the login
          // form would teach exactly the wrong lesson about the ladder.
          showError(
            "403 — not permitted for your role (" + (getClaims().role || "?") + "). " + detail +
            (err.requestId ? "  [request " + err.requestId + "]" : "")
          );
        } else {
          showError(res.status + " — " + detail + (err.requestId ? "  [request " + err.requestId + "]" : ""));
        }
        throw err;
      });
    });
  }

  // ---- auth ----------------------------------------------------------------
  function decodeJwtPayload(token) {
    // Display only. The signature is checked by the server on every call; nothing on this page
    // trusts these claims for access decisions, it only renders them.
    try {
      var part = token.split(".")[1];
      var b64 = part.replace(/-/g, "+").replace(/_/g, "/");
      while (b64.length % 4) b64 += "=";
      return JSON.parse(atob(b64));
    } catch (e) {
      return {};
    }
  }

  function login(username, password) {
    show(elLoginError, false);
    elLoginSubmit.disabled = true;

    // Form-encoded, per RFC 6749 §4.3 — the token route takes an OAuth2 password grant, not JSON.
    var body = new URLSearchParams();
    body.set("username", username);
    body.set("password", password);

    fetch(API + "/auth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString()
    }).then(function (res) {
      readRateLimit(res);
      return res.json().catch(function () { return {}; }).then(function (data) {
        if (!res.ok) {
          var msg = res.status === 401
            ? "401 — unknown username or wrong password. (The API deliberately does not say which.)"
            : (data.detail || ("HTTP " + res.status));
          throw new Error(msg);
        }
        return data;
      });
    }).then(function (data) {
      var claims = decodeJwtPayload(data.access_token);
      setSession(data.access_token, {
        role: data.role || claims.role,
        tier: data.tier || claims.tier,
        subject: claims.sub || username,
        expires_at: data.expires_at
      });
      onSignedIn();
    }).catch(function (err) {
      text(elLoginError, err.message);
      show(elLoginError, true);
    }).finally(function () {
      elLoginSubmit.disabled = false;
    });
  }

  // Put the page back into the state a fresh load would have produced.
  //
  // Signing out is a STATE reset, not merely a token deletion, and the first version of this
  // code only did the token half: the previous principal's tail rows, frame counter, stat
  // cards, charts and rate-limit badge all survived until someone reloaded. None of it is
  // privileged — a viewer can fetch the same entries an analyst just streamed — so this is not
  // a disclosure bug. It is worse in a quieter way: it makes "sign out" mean something weaker
  // than it says, and it is one route away from being a real leak the day an endpoint starts
  // returning something genuinely role-scoped.
  //
  // A reload is the reference behaviour, so the rule for anything added to this page from here
  // on is simply: **if a reload would rebuild it, this function has to tear it down.**
  function resetToSignedOut() {
    // -- module state ---------------------------------------------------------------------
    // `pageFilters` especially: leaving the last principal's snapshot in place would scope the
    // next principal's very first page to a query they never typed.
    pageFilters = null;
    nextCursor = null;
    shownCount = 0;
    tailFrames = 0;

    // -- panels and identity chips ---------------------------------------------------------
    show(elSignedIn, false);
    show(elLoginPanel, true);
    show(elLogout, false);
    show(elChipRole, false);
    show(elChipTier, false);
    show(elChipSubject, false);
    // Blanked as well as hidden. A hidden element still answers `textContent`, and a chip that
    // reads "role: analyst" while nobody is signed in is a lie waiting to be read by something.
    text(elChipRole, "role: —");
    text(elChipTier, "tier: —");
    text(elChipSubject, "—");

    // -- the login form itself -------------------------------------------------------------
    elUser.value = "";
    elPass.value = "";
    show(elLoginError, false);
    text(elLoginError, "");

    // -- filters -----------------------------------------------------------------------------
    Array.prototype.forEach.call(elFLevel.options, function (o) { o.selected = false; });
    elFService.value = "";
    elFHost.value = "";
    elFQ.value = "";

    // -- log table ---------------------------------------------------------------------------
    // This one was only ever *overwritten* by the next session's `loadPage(null)`, which looks
    // identical right up until the next sign-in fails or is slow.
    elTbody.innerHTML = "";
    text(elTotal, "—");
    text(elShown, "0");
    text(elLogStatus, "—");
    show(elLoadMore, false);

    // -- stats ---------------------------------------------------------------------------------
    text(elStatTotal, "—");
    text(elStatIngest, "—");
    text(elStatResident, "—");
    text(elStatEvicted, "—");
    LEVELS.forEach(function (l) { text($("stat-level-" + l), "0"); });
    text(elPollState, "polling every 5s");
    show(elChartFallback, false);
    // Destroyed and nulled rather than zeroed, because `ensureCharts` reads a non-null handle
    // as "already built" — nulling the refs is what lets the next session construct clean ones,
    // and it is also exactly what a reload does. Guarded: this function is called from the 401
    // path inside a fetch handler, and a throw here would replace the real error with this one.
    ["levels", "buckets"].forEach(function (key) {
      if (charts[key]) {
        try { charts[key].destroy(); } catch (e) { /* a half-built chart is not worth a throw */ }
        charts[key] = null;
      }
    });

    // -- live tail -----------------------------------------------------------------------------
    elTailRows.innerHTML = "";
    text(elTailCount, "0 frames");
    text(elTailStatus, "disconnected");
    text(elTailToggle, "Connect");
    elTailState.dataset.state = "idle";

    // -- banners and the rate-limit badge --------------------------------------------------------
    // The badge reports a PER-PRINCIPAL allowance. Carrying "0 / 20 left" across a sign-out
    // would describe a bucket the next principal does not own.
    text(elRlBadge, "rate limit: —");
    elRlBadge.removeAttribute("data-level");
    show(elRlBanner, false);
    clearError();
  }

  function logout(message) {
    // Order matters: close the EventSource and stop the poller BEFORE clearing the DOM, so an
    // in-flight frame cannot append a row to a container that has just been emptied.
    stopTail();
    stopStatsPolling();
    clearSession();
    resetToSignedOut();
    // Applied after the reset, which clears this element. A 401 hands us the one message worth
    // carrying across the transition — it is the reason the user is looking at a login form.
    if (message) { text(elLoginError, message); show(elLoginError, true); }
  }

  function onSignedIn() {
    var c = getClaims();
    text(elChipRole, "role: " + (c.role || "?"));
    text(elChipTier, "tier: " + (c.tier || "?"));
    text(elChipSubject, c.subject || "?");
    show(elChipRole, true);
    show(elChipTier, true);
    show(elChipSubject, true);
    show(elLogout, true);
    show(elLoginPanel, false);
    show(elSignedIn, true);
    clearError();
    reload();
    startStatsPolling();
  }

  // ---- filters -------------------------------------------------------------
  //
  // Returns a plain array of [key, value] pairs rather than an object, because `level` is
  // repeatable (`?level=ERROR&level=FATAL`) and an object cannot hold a duplicate key.
  function currentFilters() {
    var pairs = [];
    Array.prototype.forEach.call(elFLevel.selectedOptions, function (o) { pairs.push(["level", o.value]); });
    if (elFService.value.trim()) pairs.push(["service", elFService.value.trim()]);
    if (elFHost.value.trim()) pairs.push(["host", elFHost.value.trim()]);
    if (elFQ.value.trim()) pairs.push(["q", elFQ.value.trim()]);
    return pairs;
  }

  function qs(pairs, extra) {
    var p = new URLSearchParams();
    pairs.forEach(function (kv) { p.append(kv[0], kv[1]); });
    Object.keys(extra || {}).forEach(function (k) {
      if (extra[k] !== null && extra[k] !== undefined && extra[k] !== "") p.set(k, extra[k]);
    });
    return p.toString();
  }

  // ---- log table -----------------------------------------------------------
  function fmtTs(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
  }

  function appendRows(items) {
    var frag = document.createDocumentFragment();
    items.forEach(function (e) {
      var tr = document.createElement("tr");
      tr.setAttribute("data-testid", "log-row");
      tr.dataset.level = e.level;

      var td1 = document.createElement("td");
      td1.className = "col-ts";
      td1.textContent = fmtTs(e.ts);

      var td2 = document.createElement("td");
      var span = document.createElement("span");
      span.className = "lvl lvl-tag-" + e.level;
      span.textContent = e.level;
      td2.appendChild(span);

      var td3 = document.createElement("td");
      td3.textContent = e.service || "";

      var td4 = document.createElement("td");
      td4.textContent = e.host || "";

      var td5 = document.createElement("td");
      td5.className = "col-msg";
      // textContent throughout, never innerHTML: log messages are attacker-influenced strings
      // in any real deployment, and this is the one place a dashboard usually gets XSS'd.
      td5.textContent = e.message || "";

      tr.appendChild(td1); tr.appendChild(td2); tr.appendChild(td3); tr.appendChild(td4); tr.appendChild(td5);
      frag.appendChild(tr);
    });
    elTbody.appendChild(frag);
    shownCount += items.length;
    text(elShown, String(shownCount));
  }

  function loadPage(cursor) {
    // Page 1 snapshots the form; subsequent pages replay the snapshot verbatim.
    if (!cursor) {
      pageFilters = currentFilters();
      elTbody.innerHTML = "";
      shownCount = 0;
      nextCursor = null;
    }
    text(elLogStatus, "loading…");
    show(elLoadMore, false);

    // THE cursor rule: the filters go out again WITH the cursor. The server fingerprints the
    // query that minted a cursor and refuses one presented under a different filter or sort
    // order, so `?cursor=…` on its own is a 400 rather than "page 2 of everything".
    var url = API + "/logs?" + qs(pageFilters, { limit: PAGE_SIZE, order: "desc", cursor: cursor || null });

    api(url).then(function (page) {
      appendRows(page.items || []);
      var info = page.page || {};
      text(elTotal, info.total === undefined ? "—" : String(info.total));
      nextCursor = info.next_cursor || null;
      show(elLoadMore, Boolean(info.has_more && nextCursor));
      text(elLogStatus, info.has_more ? "more available" : "end of results");
      if (shownCount === 0) text(elLogStatus, "no entries match these filters");
    }).catch(function (err) {
      text(elLogStatus, "failed (" + (err.status || "network") + ")");
    });
  }

  function reload() {
    loadPage(null);
    fetchStats();
    // A filter change invalidates the tail's server-side filter too, so a connected tail is
    // reconnected under the new one rather than left showing stale criteria.
    if (source) { stopTail(); startTail(); }
  }

  // ---- stats + charts ------------------------------------------------------
  function haveChartJs() { return typeof window.Chart !== "undefined"; }

  function ensureCharts() {
    if (!haveChartJs()) { show(elChartFallback, true); return false; }
    if (charts.levels) return true;

    var common = {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#98a1b2", font: { size: 10 } }, grid: { color: "#1e222c" } },
        y: { beginAtZero: true, ticks: { color: "#98a1b2", font: { size: 10 } }, grid: { color: "#1e222c" } }
      }
    };

    charts.levels = new window.Chart(elCanvasLevels.getContext("2d"), {
      type: "bar",
      data: {
        labels: LEVELS,
        datasets: [{ data: LEVELS.map(function () { return 0; }), backgroundColor: LEVELS.map(function (l) { return LEVEL_COLORS[l]; }) }]
      },
      options: common
    });

    charts.buckets = new window.Chart(elCanvasBuckets.getContext("2d"), {
      type: "line",
      data: { labels: [], datasets: [{ data: [], borderColor: "#6aa9ff", backgroundColor: "rgba(106,169,255,0.18)", fill: true, tension: 0.25, pointRadius: 0, borderWidth: 2 }] },
      options: common
    });
    return true;
  }

  function fetchStats() {
    // The SNAPSHOT, not the live form — the same one the table is paging under. A poll that
    // read the form directly would silently re-scope the numbers while someone was still typing
    // a filter they had not applied yet, and the cards would stop describing the rows below.
    var url = API + "/stats?" + qs(pageFilters || currentFilters(), {});
    api(url).then(function (s) {
      text(elStatTotal, String(s.total));
      var ingest = (s.ingest || {});
      text(elStatIngest, (ingest.per_sec === undefined ? "—" : ingest.per_sec.toFixed(2)));
      text(elStatResident, String(ingest.resident === undefined ? "—" : ingest.resident));
      text(elStatEvicted, String(ingest.evicted === undefined ? "—" : ingest.evicted));

      var byLevel = s.by_level || {};
      LEVELS.forEach(function (l) { text($("stat-level-" + l), String(byLevel[l] || 0)); });

      if (!ensureCharts()) return;
      charts.levels.data.datasets[0].data = LEVELS.map(function (l) { return byLevel[l] || 0; });
      charts.levels.update();

      var buckets = s.buckets || [];
      charts.buckets.data.labels = buckets.map(function (b) { return fmtTs(b.bucket_start).slice(11, 19); });
      charts.buckets.data.datasets[0].data = buckets.map(function (b) { return b.count; });
      charts.buckets.update();
    }).catch(function () { /* the shared handler already surfaced it */ });
  }

  function startStatsPolling() {
    stopStatsPolling();
    statsTimer = window.setInterval(function () {
      // A background tab polling every 5s forever is precisely the behaviour the token bucket
      // exists to punish, and it would be this page's fault. `visibilitychange` resumes it (and
      // fires one immediate refresh) when the tab comes back.
      if (document.hidden) return;
      fetchStats();
    }, STATS_POLL_MS);
    text(elPollState, "polling every 5s");
  }
  function stopStatsPolling() {
    if (statsTimer) { window.clearInterval(statsTimer); statsTimer = null; }
  }

  document.addEventListener("visibilitychange", function () {
    if (!getToken()) return;
    if (document.hidden) {
      text(elPollState, "paused (tab hidden)");
    } else {
      text(elPollState, "polling every 5s");
      fetchStats();
    }
  });

  // ---- live tail -----------------------------------------------------------
  function tailNote(msg, cls) {
    var d = document.createElement("div");
    d.className = "tail-note" + (cls ? " " + cls : "");
    d.textContent = msg;
    elTailRows.appendChild(d);
    elTailRows.scrollTop = elTailRows.scrollHeight;
  }

  function startTail() {
    if (source) return;
    var token = getToken();
    if (!token) return;

    // The token rides in the QUERY STRING here and nowhere else in this file. The browser's
    // native EventSource API cannot set an Authorization header — there is no options bag for
    // it — so `GET /logs/stream` (and only that route) accepts `?access_token=`. The server
    // sets `Referrer-Policy: no-referrer` on the response for the same reason.
    var url = API + "/logs/stream?" + qs(pageFilters || currentFilters(), { access_token: token });

    source = new EventSource(url);
    elTailState.dataset.state = "retry";
    text(elTailStatus, "connecting…");
    text(elTailToggle, "Disconnect");

    // The server's FIRST frame. It reports what was replayed and from where, which is how a
    // reconnect proves itself: `resumed_from` is non-null when the browser sent Last-Event-ID.
    source.addEventListener("ready", function (ev) {
      elTailState.dataset.state = "open";
      var info = {};
      try { info = JSON.parse(ev.data); } catch (e) { /* ignore */ }
      text(elTailStatus, "connected" + (info.resumed_from ? " (resumed from seq " + info.resumed_from + ")" : ""));
      tailNote("— stream ready" + (info.replayed ? ", replayed " + info.replayed + " entr" + (info.replayed === 1 ? "y" : "ies") : "") + " —");
    });

    source.addEventListener("log", function (ev) {
      var e;
      try { e = JSON.parse(ev.data); } catch (err) { return; }
      var row = document.createElement("div");
      row.className = "tail-row";
      row.setAttribute("data-testid", "tail-row");
      row.dataset.level = e.level;

      var ts = document.createElement("span"); ts.className = "t-ts"; ts.textContent = fmtTs(e.ts).slice(11);
      var lv = document.createElement("span"); lv.className = "lvl lvl-tag-" + e.level; lv.textContent = e.level;
      var sv = document.createElement("span"); sv.className = "t-svc"; sv.textContent = e.service || "";
      var ms = document.createElement("span"); ms.className = "t-msg"; ms.textContent = e.message || "";
      row.appendChild(ts); row.appendChild(lv); row.appendChild(sv); row.appendChild(ms);
      elTailRows.appendChild(row);

      // Bounded DOM. Trim from the front so the newest frame is always the one on screen.
      while (elTailRows.childElementCount > TAIL_MAX_ROWS) {
        elTailRows.removeChild(elTailRows.firstElementChild);
      }
      elTailRows.scrollTop = elTailRows.scrollHeight;

      tailFrames += 1;
      text(elTailCount, tailFrames + " frame" + (tailFrames === 1 ? "" : "s"));
    });

    // Sent when the server cuts a consumer that fell too far behind. Worth showing: it is the
    // difference between "you saw everything" and "you have a hole".
    source.addEventListener("dropped", function (ev) {
      var info = {};
      try { info = JSON.parse(ev.data); } catch (e) { /* ignore */ }
      tailNote("— dropped: " + (info.detail || info.reason || "slow consumer") + " —");
    });

    source.onerror = function () {
      // EventSource retries on its own, and because the server honours `Last-Event-ID` (the
      // browser echoes the last `id:` it saw, and every `log` frame's id is the entry's seq),
      // the reconnect RESUMES rather than restarting — no gap, no duplicates. Demonstrating
      // that is half the reason this feature exists, so the UI says "reconnecting" rather than
      // pretending nothing happened. `readyState === CLOSED` means it gave up for good, which
      // on this API is what a 403 (wrong role) or a 429 (stream cap) looks like from here:
      // EventSource surfaces neither the status code nor the body to script.
      if (!source) return;
      if (source.readyState === EventSource.CLOSED) {
        elTailState.dataset.state = "error";
        text(elTailStatus, "closed by server — a non-200 here is usually 403 (needs analyst) or 429 (stream cap)");
        showError("Live tail refused. The stream requires the analyst role; the free tier also caps concurrent streams. Current role: " + (getClaims().role || "?") + ".");
        stopTail();
      } else {
        elTailState.dataset.state = "retry";
        text(elTailStatus, "reconnecting (will resume from Last-Event-ID)…");
      }
    };
  }

  function stopTail() {
    if (source) { source.close(); source = null; }
    elTailState.dataset.state = "idle";
    text(elTailToggle, "Connect");
    if (elTailStatus.textContent.indexOf("closed by server") === -1) text(elTailStatus, "disconnected");
  }

  // ---- wiring --------------------------------------------------------------
  elLoginForm.addEventListener("submit", function (ev) {
    ev.preventDefault();
    login(elUser.value.trim(), elPass.value);
  });

  elDemo.addEventListener("click", function (ev) {
    var btn = ev.target.closest("button[data-user]");
    if (!btn) return;
    elUser.value = btn.dataset.user;
    elPass.value = btn.dataset.pass;
    elUser.focus();
  });

  elLogout.addEventListener("click", function () { logout(null); });

  elFilterForm.addEventListener("submit", function (ev) { ev.preventDefault(); clearError(); reload(); });
  elFReset.addEventListener("click", function () {
    Array.prototype.forEach.call(elFLevel.options, function (o) { o.selected = false; });
    elFService.value = ""; elFHost.value = ""; elFQ.value = "";
    clearError();
    reload();
  });

  elLoadMore.addEventListener("click", function () { if (nextCursor) loadPage(nextCursor); });

  elTailToggle.addEventListener("click", function () {
    clearError();
    if (source) { stopTail(); } else { startTail(); }
  });
  elTailClear.addEventListener("click", function () {
    elTailRows.innerHTML = "";
    tailFrames = 0;
    text(elTailCount, "0 frames");
  });

  // A live EventSource holds a server-side subscription and counts against this principal's
  // concurrent-stream cap. Closing it on unload returns the slot immediately instead of waiting
  // for the server to notice the socket is gone.
  window.addEventListener("pagehide", function () { if (source) source.close(); });

  // A token surviving a reload (same tab) skips the login panel.
  if (getToken()) { onSignedIn(); } else { show(elSignedIn, false); }
})();

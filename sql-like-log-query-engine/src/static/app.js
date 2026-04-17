/* Distributed SQL Log Query Engine — UI controller.
 *
 * Wires the DOM elements declared in `templates/index.html` to the
 * coordinator's REST + WebSocket endpoints.
 *
 * Flow:
 *   1. User types a query in `#sql` (or clicks a sample-query button).
 *   2. Run -> POST /api/query/stream -> {query_id}
 *      -> open WebSocket /ws/query/{id}
 *      -> on each message append to progress list
 *      -> on `done` message render plan + results from the payload.
 *   3. Explain -> POST /api/explain -> render plan_text in <pre>.
 *   4. A 5s setInterval polls /api/health and colours the health pill.
 */
(function () {
  "use strict";

  // --------------------------------------------------------------- helpers

  function $(id) {
    return document.getElementById(id);
  }

  function clearChildren(el) {
    while (el && el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function setStatus(msg, kind) {
    var line = $("status-line");
    if (!line) return;
    line.textContent = msg || "";
    line.className = "status-line" + (kind ? " status-line--" + kind : "");
  }

  function wsUrlFor(queryId) {
    var proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + window.location.host + "/ws/query/" + queryId;
  }

  // --------------------------------------------------------- progress feed

  function appendProgress(event) {
    var list = $("progress-list");
    if (!list) return;
    var li = document.createElement("li");
    li.className = "progress-list__item";

    var stage = document.createElement("span");
    stage.className = "progress-list__stage";
    stage.textContent = event.stage;
    li.appendChild(stage);

    if (event.payload && Object.keys(event.payload).length > 0) {
      var payload = document.createElement("span");
      payload.className = "progress-list__payload";
      try {
        payload.textContent = JSON.stringify(event.payload);
      } catch (e) {
        payload.textContent = String(event.payload);
      }
      li.appendChild(payload);
    }

    list.appendChild(li);
    list.scrollTop = list.scrollHeight;
  }

  // ------------------------------------------------------------- rendering

  function renderPlanFromQueryResponse(queryResponse) {
    var pre = $("plan-text");
    if (!pre) return;
    var lines = [];
    if (queryResponse.plan) {
      var plan = queryResponse.plan;
      lines.push(
        "Execution plan: " +
          (plan.steps ? plan.steps.length : 0) +
          " steps, parallelism level " +
          (plan.parallelism || 1)
      );
      if (plan.optimization_notes && plan.optimization_notes.length) {
        plan.optimization_notes.forEach(function (note) {
          lines.push("  - " + note);
        });
      }
      if (plan.steps && plan.steps.length) {
        lines.push("");
        lines.push("Steps:");
        plan.steps.forEach(function (step, idx) {
          lines.push(
            "  " +
              (idx + 1) +
              ". " +
              step.op +
              (step.partition_id ? " (" + step.partition_id + ")" : "")
          );
        });
      }
    }
    lines.push("");
    lines.push(
      "Execution time: " +
        (queryResponse.execution_time_ms || 0).toFixed(2) +
        " ms  |  records processed: " +
        (queryResponse.records_processed || 0)
    );
    if (queryResponse.partial_results) {
      lines.push(
        "Partial results: failed partitions = " +
          (queryResponse.failed_partitions || []).join(", ")
      );
    }
    pre.textContent = lines.join("\n");
  }

  function renderPlanText(planText) {
    var pre = $("plan-text");
    if (!pre) return;
    pre.textContent = planText || "—";
  }

  function renderResults(queryResponse) {
    var head = $("results-head");
    var body = $("results-body");
    var meta = $("results-meta");
    if (!head || !body) return;

    clearChildren(head);
    clearChildren(body);

    var rows = queryResponse.results || [];
    if (meta) {
      meta.textContent =
        "(" + rows.length + " row" + (rows.length === 1 ? "" : "s") + ")";
    }

    if (rows.length === 0) {
      var emptyRow = document.createElement("tr");
      var emptyCell = document.createElement("td");
      emptyCell.className = "results-table__empty";
      emptyCell.textContent = "No rows";
      emptyRow.appendChild(emptyCell);
      body.appendChild(emptyRow);
      return;
    }

    // Column set = union of keys in the first row (stable ordering).
    var columns = Object.keys(rows[0]);

    var headerRow = document.createElement("tr");
    columns.forEach(function (col) {
      var th = document.createElement("th");
      th.textContent = col;
      headerRow.appendChild(th);
    });
    head.appendChild(headerRow);

    rows.forEach(function (row) {
      var tr = document.createElement("tr");
      columns.forEach(function (col) {
        var td = document.createElement("td");
        var val = row[col];
        td.textContent = val === null || val === undefined ? "" : String(val);
        if (col === "level" && typeof val === "string") {
          td.classList.add("level-" + val.toUpperCase());
        }
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
  }

  // ------------------------------------------------------------- actions

  var currentSocket = null;

  function runQuery() {
    var sql = ($("sql") && $("sql").value || "").trim();
    if (!sql) {
      setStatus("Enter a SQL query first.", "warn");
      return;
    }

    clearChildren($("progress-list"));
    setStatus("Submitting query...", "info");

    if (currentSocket) {
      try {
        currentSocket.close();
      } catch (e) {
        /* noop */
      }
      currentSocket = null;
    }

    fetch("/api/query/stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: sql }),
    })
      .then(function (res) {
        if (!res.ok) {
          return res.text().then(function (txt) {
            throw new Error(
              "HTTP " + res.status + ": " + (txt || "request failed")
            );
          });
        }
        return res.json();
      })
      .then(function (body) {
        if (!body || !body.query_id) {
          throw new Error("server did not return a query_id");
        }
        setStatus("Streaming query " + body.query_id + "...", "info");
        openStream(body.query_id);
      })
      .catch(function (err) {
        setStatus("Error: " + err.message, "error");
      });
  }

  function openStream(queryId) {
    var ws = new WebSocket(wsUrlFor(queryId));
    currentSocket = ws;

    ws.onmessage = function (ev) {
      var msg;
      try {
        msg = JSON.parse(ev.data);
      } catch (e) {
        return;
      }

      appendProgress(msg);

      if (msg.stage === "done" && msg.payload) {
        // The final `done` event's payload is the full QueryResponse JSON.
        renderPlanFromQueryResponse(msg.payload);
        renderResults(msg.payload);
        setStatus(
          "Done in " + (msg.payload.execution_time_ms || 0).toFixed(2) + " ms",
          "success"
        );
      } else if (msg.stage === "error") {
        setStatus(
          "Query failed: " +
            (msg.payload && msg.payload.error ? msg.payload.error : "unknown"),
          "error"
        );
      }
    };

    ws.onerror = function () {
      setStatus("WebSocket error", "error");
    };

    ws.onclose = function () {
      if (currentSocket === ws) currentSocket = null;
    };
  }

  function explainQuery() {
    var sql = ($("sql") && $("sql").value || "").trim();
    if (!sql) {
      setStatus("Enter a SQL query first.", "warn");
      return;
    }
    setStatus("Fetching plan...", "info");

    fetch("/api/explain", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: sql }),
    })
      .then(function (res) {
        if (!res.ok) {
          return res.text().then(function (txt) {
            throw new Error(
              "HTTP " + res.status + ": " + (txt || "request failed")
            );
          });
        }
        return res.json();
      })
      .then(function (body) {
        renderPlanText(body.plan_text);
        setStatus("Plan rendered", "success");
      })
      .catch(function (err) {
        setStatus("Error: " + err.message, "error");
      });
  }

  // --------------------------------------------------------------- health

  function refreshHealth() {
    var pill = $("health-pill");
    if (!pill) return;

    fetch("/api/health")
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then(function (body) {
        var partitions = body.partitions || [];
        var total = partitions.length;
        var healthy = partitions.filter(function (p) {
          return p.healthy;
        }).length;

        var cls = "health-pill--red";
        var label = "down";
        if (total > 0 && healthy === total) {
          cls = "health-pill--green";
          label = "all " + healthy + "/" + total + " healthy";
        } else if (healthy > 0) {
          cls = "health-pill--yellow";
          label = "partial " + healthy + "/" + total;
        } else {
          label = "0/" + total + " healthy";
        }

        pill.className = "health-pill " + cls;
        pill.textContent = "health: " + label;
      })
      .catch(function () {
        pill.className = "health-pill health-pill--red";
        pill.textContent = "health: unreachable";
      });
  }

  // ------------------------------------------------------------- bootstrap

  function installSamples() {
    var buttons = document.querySelectorAll(".sample-btn");
    var sql = $("sql");
    if (!sql) return;
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var q = btn.getAttribute("data-sample");
        if (q) {
          sql.value = q;
          sql.focus();
        }
      });
    });
  }

  function main() {
    var run = $("run-btn");
    var explain = $("explain-btn");
    if (run) run.addEventListener("click", runQuery);
    if (explain) explain.addEventListener("click", explainQuery);

    installSamples();

    refreshHealth();
    window.setInterval(refreshHealth, 5000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();

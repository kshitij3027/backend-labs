# RCA Analysis Engine

A causal **root-cause analysis** service for distributed-system incidents. It ingests a batch of log events (`timestamp`, `service`, `level`, `message`), corrects for clock skew, reconstructs the incident **timeline**, builds a directed **causal graph** (`networkx.DiGraph`) between events, ranks the most likely **root causes** with a **calibrated confidence**, tracks **multiple concurrent hypotheses**, and quantifies the **blast radius** — then exports a post-mortem and streams every result to a live **React + Plotly** dashboard over WebSocket. It runs as two long-lived Docker services (a FastAPI backend, an nginx-served SPA), keeps **all state in-memory** (no DB, no Redis, no queue), and is verified end-to-end by a black-box harness and a hard-gated load test.

---

## What It Does

When an incident spans many services, the raw logs tell you *what* failed but not *why* — you need the events *related to each other* causally, and the true trigger separated from its downstream noise. This engine does exactly that, as a nine-stage pipeline behind one `POST /api/analyze-incident` call.

It follows the dominant real-world RCA pattern — **(1) score anomalies → (2) build a weighted causal DiGraph → (3) random-walk / centrality to rank sources** — in the MonitorRank → CloudRanger → MicroRCA research lineage, shipping the spec's hand-crafted formulas as a faithful "cheap but correct" core and layering the research-backed fidelity upgrades on top (base-rate correction, personalized PageRank, probability calibration, clock-skew handling, incremental re-ranking).

Every result is served from an in-memory incident history and pushed live to connected dashboards, so an operator watches incidents appear, inspects the causal network, reads the ranked causes and their calibrated confidence, and exports a markdown post-mortem — all without polling.

---

## Architecture

One FastAPI process runs the whole engine. `RCAAnalyzer.analyze()` composes nine small, pure, individually-tested stages; each reads its knobs off `Settings`, so the scoring is entirely config-tunable. There is no locking anywhere — analysis is synchronous and in-memory, and the optional background live-stream loop runs on the same event loop.

```
POST /api/analyze-incident   (JSON array of LogEvent)
        │
        ▼
  RCAAnalyzer.analyze(events)
        │
        ├─ 1. ClockSkewCorrector     ε-tolerance band + dependency happens-before reorder   [E]
        ├─ 2. TimelineReconstructor  chronological sort · T+M:SS offsets · event_id · context
        ├─ 3. AnomalyAmplifier       base-rate correction: surprise · z-score · Ochiai lift  [D]
        ├─ 4. CausalGraphBuilder     networkx.DiGraph · temporal-sweep edges · strength formula
        ├─ 5. RootCauseIdentifier +  severity + temporal position + out-degree centrality
        │     ConfidenceScorer
        ├─ 6. MultiHypothesisTracker top-k · anomaly-seeded reversed-graph PageRank          [A]
        ├─ 7. ImpactAnalyzer         nx.descendants blast radius · weighted reachability
        ├─ 8. ConfidenceCalibrator   isotonic / Platt vs. history · Brier (display value)    [C]
        └─ 9. PostMortemReporter     recovery points · event classification · markdown       [F]
        │
        ▼
   IncidentReport ──► bounded in-memory history (MAX_INCIDENT_HISTORY)
        │
        └─► ConnectionManager.broadcast({"type":"incident_update","data": report}) ──► /ws clients

Background LiveStream loop (LIVE_STREAM_ENABLED, off by default):
   generator ─► IncrementalAnalyzer (rolling window · warm-started re-rank) [B] ─► broadcast

React + Vite + Plotly dashboard (nginx :3000, /api + /ws reverse-proxied):
   incidents list · timeline · Plotly causal-graph network plot · ranked causes · impact · live via WS
```

Bracketed tags `[A]`–`[F]` map to the six extended feature areas: **A** multi-hypothesis tracking, **B** incremental analysis, **C** confidence calibration, **D** anomaly amplification, **E** clock-skew handling, **F** incident reporting.

**Module layout** (`src/`; `models.py` is the single source of truth, `analysis/` is the strategy subpackage with `RCAAnalyzer` as its orchestrator):

```
src/
├── config.py                  # pydantic-settings Settings + get_settings() (all §7 tunables)
├── models.py                  # LogEvent, TimelineEntry, RootCause, Hypothesis, ImpactAnalysis, IncidentReport + enums
├── api.py                     # create_app(runtime) factory: REST routes + /ws WebSocket + CORS
├── main.py                    # Runtime dataclass + lifespan + background live-stream loop + `app`
├── ws.py                      # ConnectionManager (connect/disconnect/broadcast, dead-socket pruning)
├── service_map.py             # ServiceDependencyMap (externalized upstream→downstream topology)
├── generators.py              # deterministic seedable incident/event generators (tests, E2E, load)
├── config/service_dependency_map.json
└── analysis/
    ├── __init__.py            # RCAAnalyzer — orchestrates the 9 stages, owns the history
    ├── clock.py               # ClockSkewCorrector          [E]
    ├── timeline.py            # TimelineReconstructor
    ├── anomaly.py             # AnomalyAmplifier             [D]
    ├── causal_graph.py        # CausalGraphBuilder
    ├── root_cause.py          # RootCauseIdentifier + ConfidenceScorer
    ├── hypotheses.py          # MultiHypothesisTracker       [A]
    ├── impact.py              # ImpactAnalyzer
    ├── calibration.py         # ConfidenceCalibrator         [C]
    ├── report.py              # PostMortemReporter           [F]
    └── incremental.py         # IncrementalAnalyzer          [B]
```

**Services.**

| Service  | Port   | Role                                                                              |
|----------|--------|-----------------------------------------------------------------------------------|
| backend  | `8000` | uvicorn / FastAPI — the RCA engine + REST API + `/ws` WebSocket (all state in-memory) |
| frontend | `3000` | nginx serving the React SPA, reverse-proxying `/api` → `backend:8000` and upgrading `/ws` |

---

## Tech Stack

- **Language / runtime:** Python 3.11
- **API:** FastAPI + `uvicorn[standard]`, with a real WebSocket (`/ws`) via `websockets`
- **Causal graph:** NetworkX (`DiGraph`, `descendants`, single-source Dijkstra)
- **Numerics / ML:** numpy + scipy (sparse vectorized PageRank power iteration), scikit-learn (isotonic / Platt calibration + Brier score)
- **Models / config:** pydantic v2 + pydantic-settings
- **Timestamps:** python-dateutil (tolerant ISO-8601 parsing)
- **Frontend:** React 18 + Vite 5 + Plotly.js (loaded from CDN, not bundled), served by nginx 1.27
- **Infra:** Docker + Docker Compose. No database, no Redis, no message queue — state is in-memory.

---

## Scoring & Algorithms

All formulas are hand-crafted, fully config-tunable (every weight lives on `Settings`), and clamped to their stated ranges. Each collaborator is pure and deterministic given its inputs.

### Causal edge `u → v` (CausalGraphBuilder)

An edge is drawn only when **admissible on all three axes**:

- **temporal** — `0 ≤ t(v) − t(u) ≤ TEMPORAL_WINDOW` (v is after u, inside the window);
- **service dependency** — `service(u)` is upstream-of-or-equal `service(v)`: either a declared **one-hop** dependency in the service map, or the **same** service (self-propagation);
- **severity** — **both** endpoints are at least `WARNING` (an `INFO` endpoint never participates).

Its **strength** is the additive form, clamped to `[CAUSAL_STRENGTH_MIN, CAUSAL_STRENGTH_MAX]`:

```
strength = BASE_CAUSAL_STRENGTH
         + SERVICE_DEPENDENCY_BONUS   (if u→v is a declared cross-service dependency)
         + ERROR_PROPAGATION_BONUS    (if both endpoints are ERROR)
         − TEMPORAL_GAP_PENALTY       (if t(v) − t(u) > TEMPORAL_GAP_THRESHOLD)
```

Edges are built with a **sorted temporal sweep** (two-pointer sliding window): events are sorted once by time, a left pointer that only ever advances tracks the earliest predecessor still inside the window, so each event considers just its small active window — **O(n·k)**, not the O(n²) of a double loop. This is what lets the engine sustain 1000+ events/sec. Each edge's value is stored as both `weight` (feeds the impact Dijkstra) and `strength` (feeds the dashboard).

### Confidence(event e) (ConfidenceScorer)

```
confidence = clamp(
    severity_score
    + TEMPORAL_SCORE_WEIGHT   · temporal_pos
    + CENTRALITY_SCORE_WEIGHT · centrality,
    0.0, 1.0)
```

- `severity_score` = `{CRITICAL: SCORE_CRITICAL, ERROR: SCORE_ERROR, WARNING: SCORE_WARNING, INFO: 0}`;
- `temporal_pos = 1 − (t_e − t_start) / (t_end − t_start)` — **earlier events score higher** (a cause precedes its effects); defined as `1.0` for a zero-span incident;
- `centrality = out_degree(e) / max_out_degree` — **unweighted** out-degree centrality, normalized against the busiest source (`0` when the graph has no edges).

**Candidates** are the union of *causal sources* (out-degree > 0) and *intrinsically severe* events (ERROR / CRITICAL), ranked by confidence descending with deterministic tie-breaks (earlier timestamp, then event id). For a seeded cascading incident the injected root — the earliest event, the sole `CRITICAL`, and the highest-out-degree source — scores `0.6 + 0.3 + 0.2 = 1.1` (clamped to `1.0`) and lands at rank #1.

### Multi-hypothesis tracking (MultiHypothesisTracker) — feature A

Rather than commit to one cause, the engine keeps the top-`MAX_HYPOTHESES` concurrent hypotheses with **independent** confidences and a `tentative → confirmed → pruned` lifecycle, ranked by **personalized PageRank / random-walk-with-restart on the *reversed* causal graph**. Causal edges run `cause → effect`, but anomalies light up on the *symptoms*; reversing the adjacency (`Aᵀ`) makes a walker step from a symptom back toward its causes, so mass accumulates on the upstream sources that explain many anomalous downstream events. The restart / personalization vector is the L1-normalized **anomaly-score** vector, and the walk is a vectorized `scipy.sparse` power iteration:

```
π_next = α · (Pᵀ π) + (α · dangling_mass + (1 − α)) · v
```

iterated until the L1 change < `PAGERANK_TOL` (or `PAGERANK_MAX_ITER`). The restart term makes this a contraction with factor `α = PAGERANK_ALPHA`, so it converges geometrically. Each hypothesis's independent confidence is its **relative** PageRank mass `π_i / max(π)` (a max-normalization, deliberately *not* sum-to-1) modulated by its own anomaly score — so several rival hypotheses can each be highly confident at once.

### Anomaly amplification (AnomalyAmplifier) — feature D

Base-rate correction so *common, benign* events aren't amplified into false-positive causes. An event's type key is `(service, level)`; a running baseline over past incidents feeds a blend of three research-backed signals plus a light severity prior, producing `anomaly_score ∈ [0, 1]` (which seeds the PageRank restart vector above):

- **surprise** `= −log₂ p_type` with Laplace-smoothed `p_type` (rarer ⇒ higher);
- **z-score** of the type's in-incident count vs. its baseline per-incident mean/std (a sudden *burst* of a normally-quiet type);
- **Ochiai spectrum lift** `ef / √((ef + nf)(ef + ep))`, treating the current incident as the single *failing* window; with `ef = 1, nf = 0` it collapses to `1 / √(1 + ep)`, so a type that fired in *every* historical incident is suppressed toward 0 while a never-seen type scores 1.0.

Scoring runs **before** the incident is folded into the baseline, so an incident is always graded against prior history only and can never trivially explain itself. With no history yet, it degrades to a within-incident-rarity + severity fallback.

### Impact / blast radius (ImpactAnalyzer)

From the top root cause's downstream cone in the graph:

- **blast radius** = `len(networkx.descendants(graph, root))`;
- **affected services** = the distinct services across that cone plus the root's own;
- **weighted reachability** = `Σ_d severity_weight(level_d) · best_path_product(root → d)`, where the strongest (highest-product) path is found *without enumerating paths* — minimizing the additive `−log(weight)` transform via a single `single_source_dijkstra_path_length` maximizes the multiplicative product, so `product = exp(−dist)`.

### Confidence calibration (ConfidenceCalibrator) — feature C

A raw score of `0.8` doesn't inherently mean "80% of such events are the real cause". The calibrator learns a monotonic mapping from raw confidence to *empirical* root-cause probability from **resolved** incidents (fed back via the feedback endpoint): each resolved incident contributes one `(raw_confidence, was_root_cause)` sample per ranked candidate. It fits **isotonic regression** (default) or **Platt scaling** (`CALIBRATION_METHOD`) only once there are ≥ `CALIBRATION_MIN_SAMPLES` samples across *both* outcome classes; until then `transform` is the **identity**, so a fresh engine behaves exactly as before and `analyze()` never breaks. Quality is reported as the **Brier score** (raw vs. calibrated) and a 10-bin **reliability diagram**. Because both fitted transforms and the identity are monotonic, calibration is applied as a *display* value that never reorders the ranking.

### Clock-skew correction (ClockSkewCorrector) — feature E

Runs **first**, before the timeline, so the whole pipeline keys off one causally-consistent order. Three bounded ideas from the distributed-clocks literature (Lamport / HLC):

- **ε-tolerance band** — events within `|Δt| < CLOCK_SKEW_EPSILON` are treated as *concurrent* (clock noise, not signal), greedily bucketed into ε-clusters; across clusters the gap is real and raw order is trusted;
- **dependency happens-before** — a declared upstream dependency is a hard ordering edge, so within a cluster members are topologically ordered (Kahn's algorithm, stable on input order) — pulling an upstream cause ahead of a skew-early effect;
- **correlation-id happens-before** (best-effort) — a shared request/trace id parsed from two messages corroborates the dependency direction.

The corrected effective time is written back onto each event as a canonical timestamp (cluster anchor + a microscopic per-rank step), so the ordering survives the independent re-sorts the timeline and graph builder each perform.

### Incremental streaming (IncrementalAnalyzer) — feature B

Backs the optional live-stream loop. It maintains a **rolling window** (bounded by both `TEMPORAL_WINDOW` seconds and `INCREMENTAL_MAX_EVENTS`), reuses the batch stages verbatim, and **warm-starts** each re-rank's PageRank power iteration from the previous tick's `π` instead of a cold start — the classic dynamic-PageRank optimization. When the graph changed only slightly the walk begins near its new stationary point and converges in far fewer iterations, landing on the identical fixed point (so the ranking is unchanged).

### Post-mortem reporting (PostMortemReporter) — feature F

- **Recovery points** — interior nodes on the top cause's propagation path where an intervention would truncate the largest downstream subtree (`gated_subtree_size = len(descendants(node))`, leaves dropped, ranked descending);
- **Event classification** — every event placed into exactly one class from its graph position: **primary_trigger** (source: in-degree 0, out-degree > 0), **propagation_path** (interior: both degrees > 0), **contributing_factor** (leaf / isolated / absent);
- **Markdown export** — a well-formed post-mortem (header, summary, ranked causes with raw + calibrated confidence, impact, recovery points, classification table, alternative hypotheses).

### Service dependency map

Externalized to `src/config/service_dependency_map.json` (with a hard-coded fallback so a missing file never blocks startup). Upstream → direct-downstream only; it gates causal-edge *direction*.

```
api-gateway → { auth, user, payment }
auth        → { database, redis }
user        → { database, file-storage }
payment     → { database, external-payment-api }
database, redis, file-storage, external-payment-api → (leaves, no downstream)
```

---

## How to Run

Everything runs in Docker — no local Python or Node needed, only Docker with Compose v2.

```bash
# Full stack incl. the dashboard (backend + frontend), detached
make ui                 # Dashboard: http://localhost:3000 · API: http://localhost:8000

# equivalent helper scripts (build, wait for /api/health, print URLs)
./start.sh
./stop.sh

# backend only (no dashboard)
make up                 # API: http://localhost:8000  (GET /api/health)

# or drive compose directly
docker compose up --build -d backend
```

**Overriding ports.** The two host ports are compose-level and overridable via env vars on any target — e.g. if `8000 / 3000` are taken:

```bash
BACKEND_PORT=8010 FRONTEND_PORT=3001 make ui
```

Quick smoke test:

```bash
curl -s http://localhost:8000/api/health
# {"status":"healthy","analyzer_ready":true}
```

### Make Targets

| Target       | What it does                                                                     |
|--------------|----------------------------------------------------------------------------------|
| `build`      | Build all images (backend + test)                                                |
| `up`         | Run the backend detached, print the API URL                                      |
| `down`       | Stop and remove the stack                                                        |
| `logs`       | Tail the backend logs                                                            |
| `ui`         | Run backend + React dashboard detached, print the URLs                           |
| `test`       | Full pytest suite in Docker (unit + integration; rebuilds the tester image first)|
| `test-unit`  | Unit tests only, in Docker                                                        |
| `test-int`   | Integration tests only, in Docker                                                |
| `e2e`        | Black-box E2E verifier vs. the live backend — 11 ordered checks                  |
| `load`       | Perf/load gates vs. the live backend (throughput, p95 latency, memory)           |
| `clean`      | `down` + remove volumes and orphans                                              |

`make e2e` and `make load` are **hard-gated** — the first failed check / breached gate exits non-zero. Every gate is host-overridable, so e.g. `MIN_EVENTS_PER_SEC=100000 make load` proves the throughput gate bites.

---

## REST API

Every handler reads shared state off `app.state.runtime` and **degrades gracefully** when a piece is missing — reads fall back to empty / `404`, writes to `503` — so a missing runtime never becomes a `500`.

| Method | Path                                  | Purpose                                                                |
|--------|---------------------------------------|------------------------------------------------------------------------|
| `GET`  | `/api/health`                         | Liveness — dependency-free, always `200` while alive                   |
| `POST` | `/api/analyze-incident`               | Analyze a posted JSON event array into an `IncidentReport` (+ broadcast)|
| `GET`  | `/api/incidents?limit=N`              | Bounded in-memory incident history, **newest first** (`limit` clamped) |
| `GET`  | `/api/incidents/{id}`                 | One stored report by id, or `404`                                      |
| `GET`  | `/api/incidents/{id}/report`          | Export the post-mortem: `{markdown, recovery_points, classifications}` |
| `GET`  | `/api/calibration`                    | Confidence-calibrator stats (method, Brier, reliability bins)          |
| `POST` | `/api/incidents/{id}/feedback`        | Record a resolved incident's true root cause; refit + return calibration |
| `GET`  | `/api/debug/memory`                   | Backend RSS in MB (`{"memory_mb": …}`) — load-test probe               |
| `GET`  | `/api/debug/ground-truth`             | Latest live-stream incident's injected ground truth — E2E aid only     |
| `WS`   | `/ws`                                 | Real-time incident feed (see below)                                    |

**`GET /api/health`** — the spec-verbatim contract (the two keys, nothing more; asserted verbatim by tests and the E2E verifier):

```json
{ "status": "healthy", "analyzer_ready": true }
```

**`POST /api/analyze-incident`** — the request body is a top-level JSON **array** of log events. A malformed element → `422`; an unparseable timestamp → `422`.

```bash
curl -s -X POST http://localhost:8000/api/analyze-incident \
  -H 'Content-Type: application/json' \
  -d '[
    {"timestamp":"2026-01-01T00:00:00Z","service":"api-gateway","level":"CRITICAL","message":"circuit breaker opened"},
    {"timestamp":"2026-01-01T00:00:05Z","service":"auth","level":"ERROR","message":"connection refused"},
    {"timestamp":"2026-01-01T00:00:08Z","service":"user","level":"ERROR","message":"request timed out"},
    {"timestamp":"2026-01-01T00:00:15Z","service":"database","level":"ERROR","message":"query execution failed"}
  ]'
```

returns a full `IncidentReport` (abbreviated below; every collection defaults empty so partial reports still validate):

```json
{
  "incident_id": "inc-a1b2c3d4e5f6",
  "timestamp": "2026-01-01T00:00:00+00:00",
  "events": [
    {"timestamp":"2026-01-01T00:00:00+00:00","service":"api-gateway","level":"CRITICAL","message":"circuit breaker opened","event_id":"evt-9f2c1a4b7e30"}
  ],
  "timeline": [
    {
      "sequence_id": 1,
      "timestamp": "2026-01-01T00:00:00+00:00",
      "relative_time": "T+0:00",
      "service": "api-gateway",
      "level": "CRITICAL",
      "message": "circuit breaker opened",
      "event_id": "evt-9f2c1a4b7e30",
      "context": {
        "preceding_event_id": null,
        "following_event_id": "evt-2b7d...",
        "prior_same_service_event_id": null,
        "position": 1,
        "total": 4
      }
    }
  ],
  "root_causes": [
    {
      "event_id": "evt-9f2c1a4b7e30",
      "service": "api-gateway",
      "level": "CRITICAL",
      "message": "circuit breaker opened",
      "confidence": 1.0,
      "raw_confidence": 1.0,
      "timestamp": "2026-01-01T00:00:00+00:00"
    }
  ],
  "impact_analysis": {
    "blast_radius": 3,
    "affected_services": ["api-gateway", "auth", "database", "user"],
    "total_events": 4,
    "details": {
      "primary_root_cause_event_id": "evt-9f2c1a4b7e30",
      "weighted_impact": 1.87,
      "reachable_event_ids": ["evt-2b7d...", "evt-4c8e...", "evt-6a1f..."],
      "per_root_cause": [ { "event_id": "evt-9f2c1a4b7e30", "blast_radius": 3, "affected_services": ["..."], "weighted_impact": 1.87 } ],
      "affected_service_count": 4
    }
  },
  "hypotheses": [
    { "hypothesis_id": "hyp-9f2c1a4b", "root_cause_event_id": "evt-9f2c1a4b7e30", "confidence": 0.95, "state": "confirmed" },
    { "hypothesis_id": "hyp-2b7d0e11", "root_cause_event_id": "evt-2b7d...",      "confidence": 0.41, "state": "tentative" }
  ],
  "anomaly_scores": { "evt-9f2c1a4b7e30": 0.88, "evt-2b7d...": 0.52 },
  "causal_graph": {
    "nodes": [
      { "id": "evt-9f2c1a4b7e30", "service": "api-gateway", "level": "CRITICAL", "message": "circuit breaker opened", "timestamp": "2026-01-01T00:00:00+00:00" }
    ],
    "edges": [
      { "source": "evt-9f2c1a4b7e30", "target": "evt-2b7d...", "strength": 0.8 }
    ]
  },
  "recovery_points": [
    { "event_id": "evt-2b7d...", "service": "auth", "gated_subtree_size": 1, "rationale": "Intervening at auth (evt-2b7d...) truncates a downstream subtree of 1 event(s) on the propagation path, preventing further escalation." }
  ],
  "event_classifications": {
    "evt-9f2c1a4b7e30": "primary_trigger",
    "evt-2b7d...": "propagation_path",
    "evt-6a1f...": "contributing_factor"
  }
}
```

*(`confidence` equals `raw_confidence` until the calibrator has been fitted from feedback; `event_id`s are derived by SHA-1 when the client omits them.)*

**`GET /api/calibration`** — unfitted until enough resolved incidents have been fed back:

```json
{ "method": "isotonic", "n_samples": 0, "fitted": false, "brier_raw": null, "brier_calibrated": null, "reliability_bins": [] }
```

**`POST /api/incidents/{id}/feedback`** — body `{"true_root_cause_event_id": "evt-…"}`; records one calibration sample per ranked candidate of that incident, refits, and returns the fresh calibration stats. Unknown incident → `404`; malformed body → `422`.

### WebSocket `/ws`

- **Connect** to `ws://localhost:3000/ws` (through nginx) or `:8000/ws` (direct). The server accepts the handshake and registers the client.
- **Keepalive:** client sends the text frame `"ping"` → server replies `"pong"`. Any other inbound text is ignored (the client is a listener).
- **Push:** on every completed analysis (a `POST`) and every live-stream tick, the server broadcasts a JSON frame to all connected clients:

  ```json
  { "type": "incident_update", "data": { /* a full IncidentReport, as above */ } }
  ```

Broadcasting is best-effort and dead sockets are pruned, so one broken client never breaks the fan-out or the `POST` that triggered it.

---

## Dashboard

A React 18 + Vite SPA served by nginx, using **Plotly.js from CDN** (kept out of the npm bundle). It loads the incident history over nginx's `/api` reverse proxy on mount and then live-updates over the `/ws` WebSocket — relative URLs only, so no CORS and no hard-coded backend host. The layout is a left incidents rail + a responsive right detail grid (collapses to one column ≤ 900px):

- **Incidents list** — recent incidents, newest first; selecting one drives every panel.
- **Plotly causal-graph panel** — an interactive network plot: nodes laid out from the serialized graph with **size/color keyed to severity**, edges weighted by causal strength; hovering shows event detail and clicking a node highlights its downstream blast radius.
- **Root causes panel** — the ranked list with confidence bars and hypothesis state chips (a shared `focusNodeId` keeps it in sync with the graph).
- **Impact panel** — blast radius + affected services.
- **Timeline panel** — the reconstructed `T+M:SS` timeline.

**Graceful degradation** is built into the WebSocket hook: on a dropped feed it keeps the last-good incidents on screen and shows an error banner (reconnecting with backoff) rather than blanking out. nginx re-resolves the `backend` service name per request via Docker DNS, so the proxy stays correct if the backend container is recreated.

---

## Configuration

Backend settings (`src/config.py`) are read from **field defaults → optional `.env` → environment variables**. Each env var name is the **upper-cased field name** (e.g. `TEMPORAL_WINDOW` ← `temporal_window`). See [`.env.example`](.env.example) for the full committed template.

| Setting                        | Default                                   | Meaning                                                              |
|--------------------------------|-------------------------------------------|---------------------------------------------------------------------|
| `temporal_window`              | `300`                                     | Max seconds between two events for a causal edge to be admissible    |
| `base_causal_strength`         | `0.5`                                     | Base edge strength before bonuses / penalties                       |
| `service_dependency_bonus`     | `0.3`                                     | + when the map declares the upstream→downstream dependency          |
| `error_propagation_bonus`      | `0.2`                                     | + for an ERROR → ERROR edge                                         |
| `temporal_gap_threshold`       | `60`                                      | Seconds beyond which the temporal-gap penalty applies               |
| `temporal_gap_penalty`         | `0.1`                                     | − when the inter-event gap exceeds the threshold                    |
| `causal_strength_min` / `_max` | `0.1` / `1.0`                             | Edge-strength clamp range                                           |
| `score_critical` / `_error` / `_warning` | `0.6` / `0.4` / `0.2`           | Severity component of the confidence score (INFO ⇒ 0)              |
| `temporal_score_weight`        | `0.3`                                     | Weight on temporal position (earlier ⇒ higher)                     |
| `centrality_score_weight`      | `0.2`                                     | Weight on normalized out-degree centrality                          |
| `max_hypotheses`               | `5`                                       | Max concurrent root-cause hypotheses (top-k by PageRank)            |
| `hypothesis_confirm_threshold` | `0.6`                                     | Confidence ≥ this ⇒ CONFIRMED                                      |
| `hypothesis_prune_threshold`   | `0.1`                                     | Confidence < this ⇒ PRUNED (dropped from the report)              |
| `pagerank_alpha`               | `0.85`                                    | Personalized-PageRank damping (restart prob = 1 − α)               |
| `pagerank_max_iter`            | `100`                                     | Max power-iteration steps                                           |
| `pagerank_tol`                 | `1e-6`                                    | L1 convergence tolerance for the power iteration                    |
| `clock_skew_epsilon`           | `2.0`                                     | Seconds; events within ±ε are "concurrent" (sub-ε order not forced) |
| `calibration_min_samples`      | `10`                                      | Min samples before the calibrator fits (below ⇒ identity)          |
| `calibration_method`           | `isotonic`                                | `isotonic` \| `platt` (anything else ⇒ isotonic)                  |
| `max_incident_history`         | `1000`                                    | Max incidents retained in the in-memory history                     |
| `live_stream_enabled`          | `false`                                   | Master switch for the background live-stream loop                   |
| `live_stream_interval`         | `5.0`                                     | Seconds between synthetic incidents in the live loop                |
| `live_stream_seed`             | `0`                                       | Base RNG seed for the live loop (tick uses seed + counter)          |
| `incremental_max_events`       | `500`                                     | Max events in the IncrementalAnalyzer rolling window                |
| `server_port`                  | `8000`                                    | uvicorn bind port inside the container                              |
| `cors_origins`                 | `*`                                       | Comma-separated allowed origins, or `*` for any                     |
| `log_level`                    | `INFO`                                    | Log level                                                          |
| `service_dependency_map_path`  | `src/config/service_dependency_map.json`  | Path to the externalized upstream→downstream map                    |

**Host ports** (`BACKEND_PORT` 8000, `FRONTEND_PORT` 3000) are compose-level host mappings, not backend settings.

---

## Testing & Performance

Everything is verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a load harness, all profile-gated compose services.

```bash
make test        # 173 unit + integration tests
make e2e         # 11-check black-box verifier vs. the live backend
make load        # hard-gated perf gates vs. the live backend
```

- **Unit tests** cover every module — timeline, causal graph, root-cause + confidence, impact, anomaly, hypotheses, clock, incremental, calibration, report, ws, config, generators.
- **Integration tests** exercise the analyze/incidents API, the WebSocket, the multi-hypothesis flow, the calibration + report endpoints, and the live-stream loop against the injected-runtime app.
- **E2E** (`scripts/verify_e2e.py`) walks 11 ordered black-box checks over HTTP/WS: `/api/health` verbatim → analyze returns a valid report → timeline ordered → **causal accuracy > 85%** vs. generator ground truth → **actual root cause in top-3** → impact/blast-radius present → WS receives `incident_update` → history grows → post-mortem exports → multi-hypothesis ≥ 2 → **analysis latency < 30 s** → `/calibration` present.
- **Load** (`scripts/load_test.py`) fires 1000 synthetic events and gates throughput, p95 analyze latency, and server-reported memory.

### Measured Performance

From the final Docker verification run, framed against the spec's success criteria:

| Metric                                  | Result                                        | Spec bar                        |
|-----------------------------------------|-----------------------------------------------|---------------------------------|
| Unit + integration tests                | **173 passing**                               | all green                       |
| E2E assertions                          | **11 / 11 passed**                            | all pass                        |
| Throughput                              | **6,125 events/s** (1000 events / 50 incidents in 0.16 s, 0 errors) | ≥ 1,000 events/s |
| Analyze latency                         | **p50 1.3 ms · p95 3.6 ms · p99 5.2 ms**      | —                               |
| Single incident (300 events)            | **0.03 s**                                    | < 30 s                          |
| Root-cause accuracy (20 seeded scenarios) | **top-1 100% · top-3 100%**                 | root cause in top-3; causal > 85% |
| Confidence calibration (isotonic)       | **Brier 0.405 → 0.000** after feedback        | improves vs. raw                |
| Backend memory                          | **102.8 MB**                                  | < 500 MB (target < 200 MB)      |

---

## What I Learned

- **A causal `DiGraph` is the right backbone for RCA.** Modeling events as nodes and "u plausibly caused v" as directed, weighted edges turns "find the root cause" into standard graph questions — sources are triggers, out-degree centrality is influence, `descendants` is blast radius, and the strongest propagation path is a single Dijkstra over `−log(weight)`. Almost every stage is a small read off one shared graph.

- **The temporal-sweep edge build is what makes 1000+/s realistic.** Sorting events once and sliding a two-pointer window over admissible predecessors is **O(n·k)** instead of the O(n²) of comparing every pair — the measured throughput (6,125 events/s) rides on it. Getting the monotonic left-pointer invariant right (it only ever advances, so window bookkeeping is amortized O(n)) was the whole trick.

- **Ranking causes means walking the graph *backwards*.** Anomalies surface on symptoms (effects), but you want to rank *causes*. Personalized PageRank on the **reversed** adjacency, with the restart vector seeded by anomaly scores, flows probability mass from symptoms back onto the upstream sources that explain them — the industry workhorse (MicroRCA / CloudRanger), and far more robust than picking the single highest-confidence node.

- **Base-rate correction stops routine events from becoming false root causes.** A `(database, INFO)` that fires in every incident should score *low* no matter how loud it is. Combining Laplace-smoothed **surprise**, a burst **z-score**, and **Ochiai** spectrum lift (which drives ubiquitous types toward zero) — and scoring an incident against *prior* history only, before observing it — was the difference between "the noisiest event wins" and "the anomalous one does".

- **Calibration is a fidelity upgrade you must make impossible to break.** A raw `0.8` isn't a probability. Isotonic / Platt regression against resolved outcomes maps it to an empirical one, measured by Brier score + reliability bins (0.405 → 0.000 after feedback). The design rule that made it safe: stay the **identity** until there's enough signal, keep both transforms monotonic, and apply the calibrated number as a *display* value over the preserved raw ranking — so a fresh engine is unaffected and the ranking can never invert.

- **Clock skew is a causality bug, not a formatting one.** Naive timestamp sorting inverts cause and effect when a downstream host's clock runs fast. An ε-tolerance band (treat sub-ε gaps as concurrent) plus a dependency-driven happens-before topological sort *within* each band restores the causal order — and baking the corrected effective time back into the event's timestamp was what let the fix survive the independent re-sorts the timeline and graph builder each perform.

- **Warm-starting incremental PageRank is a big, free win.** For the live stream, reusing the previous tick's `π` as the power-iteration's initial vector (instead of a cold restart) converges in far fewer iterations when the windowed graph barely changed — the same fixed point, a fraction of the work — while a rolling window bounded by both time and count keeps each re-rank cheap no matter how long the stream runs.

- **Serving Plotly behind an nginx `/ws` upgrade proxy.** Loading Plotly from CDN keeps the bundle lean, and a single nginx config both reverse-proxies `/api` and upgrades `/ws` (HTTP/1.1 + `Upgrade`/`Connection` headers + long read timeout) so the browser only ever talks to nginx on one origin — no CORS, relative URLs, and the SPA keeps its last-good data on a dropped socket instead of blanking out.

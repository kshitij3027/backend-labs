# Log Service Circuit Breaker Engine

A three-state circuit breaker system that wraps downstream service calls (database, message queue, external API) to prevent cascading failures by failing fast when services are unhealthy and providing graceful fallbacks.

## Tech Stack

- **Language:** Python 3.11+
- **Framework:** FastAPI (REST + WebSocket)
- **Server:** Uvicorn (ASGI)
- **Concurrency:** asyncio
- **Frontend:** Vanilla HTML/JS dashboard (real-time via WebSocket)
- **Testing:** pytest, pytest-asyncio, httpx
- **Containerization:** Docker + docker-compose

## What It Does

Implements the classic **three-state circuit breaker** pattern around downstream dependencies:

| State        | Behavior                                                                 |
|--------------|--------------------------------------------------------------------------|
| `CLOSED`     | All calls pass through. Failures are counted against a sliding window.   |
| `OPEN`       | Calls fail fast (no downstream contact). Fallback is returned instead.   |
| `HALF_OPEN`  | A limited number of probe calls are allowed to test recovery.            |

Transitions:
- `CLOSED → OPEN` when failure rate / consecutive failures cross a threshold.
- `OPEN → HALF_OPEN` after a cooldown timer elapses.
- `HALF_OPEN → CLOSED` after N consecutive successful probes.
- `HALF_OPEN → OPEN` if any probe fails.

The engine wraps three simulated downstream services:
1. **Database** — log persistence
2. **Message Queue** — log fan-out
3. **External API** — enrichment / forwarding

Each wrapped call exposes hooks for: timeouts, error classification, fallback handlers, and metrics emission.

## How It Runs

Two modes:

### 1. Long-lived FastAPI server (primary)
- REST endpoints to **submit logs**, **simulate failures**, **inspect breaker state**, and **trigger forced transitions**.
- WebSocket channel that streams real-time state changes, request outcomes, and metrics to the browser dashboard.
- Browser dashboard at `/` renders per-breaker state, failure rates, latency histograms, and a live event log.

### 2. One-shot CLI demo
- Single command runs a scripted scenario (healthy → degraded → outage → recovery) against the in-process engine and prints transitions to stdout. Useful for quick verification without spinning up the full server stack.

## How to Run

> _Filled in once implementation begins._

## API (planned)

| Method | Path                              | Purpose                                     |
|--------|-----------------------------------|---------------------------------------------|
| POST   | `/logs`                           | Submit a log; routed through breakers       |
| GET    | `/breakers`                       | List all breakers and their current state   |
| GET    | `/breakers/{name}`                | Detailed metrics for one breaker            |
| POST   | `/breakers/{name}/trip`           | Force trip (testing)                        |
| POST   | `/breakers/{name}/reset`          | Force reset to CLOSED                       |
| POST   | `/simulate/{service}/fail`        | Inject failures into a downstream           |
| POST   | `/simulate/{service}/recover`     | Restore healthy behavior                    |
| WS     | `/ws/events`                      | Live stream of state changes & metrics      |
| GET    | `/`                               | Monitoring dashboard (HTML)                 |

## Configuration (planned)

Per-breaker tunables:
- `failure_threshold` — trip after N failures (or % rate)
- `rolling_window_seconds` — sliding window for failure counting
- `open_cooldown_seconds` — time in OPEN before HALF_OPEN
- `half_open_max_calls` — concurrent probes allowed
- `half_open_success_threshold` — successes needed to close
- `request_timeout_seconds` — per-call deadline
- `fallback` — function returning a degraded response

## What I Learned

> _Filled in as the project evolves._

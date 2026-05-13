# Chaos Testing for Log Processing

A safety-controlled chaos engineering tool that injects failures (network, resource, component) into a target distributed log processing system, monitors its behavior in real time, and validates that it recovers correctly.

## Overview

This framework lets you design and run controlled chaos experiments against a running log processing pipeline (e.g., producers, brokers, consumers, storage). It exposes an HTTP API and a live WebSocket dashboard so you can:

- Create, run, pause, and abort chaos experiments
- Inject network faults (latency, packet loss, partition), resource pressure (CPU, memory, disk, I/O), and component failures (kill, pause, restart)
- Stream real-time metrics and event timelines while an experiment runs
- Enforce safety controls (blast-radius limits, automatic rollback, kill switch)
- Validate steady-state and recovery hypotheses after each run

## How It Runs

- A **long-lived FastAPI server** exposes the experiment API and serves the dashboard.
- A **WebSocket channel** pushes live experiment state, target metrics, and fault timelines.
- Target services run as **Docker containers**; the framework manipulates them via the **Docker socket** (start/stop/pause/kill, exec `tc`/`stress-ng`-style fault tooling inside the target, attach/detach networks).
- An **experiment engine** orchestrates the steady-state check → fault injection → observation → recovery check → rollback lifecycle.
- A **safety supervisor** runs alongside every experiment and aborts on guardrail breach (e.g., error rate spike, dropped logs above threshold, blast radius exceeded).

## Tech Stack

- **Language:** Python 3.11+
- **Backend:** FastAPI + Uvicorn
- **Real-time transport:** WebSockets (FastAPI native)
- **Container control:** Docker SDK for Python (`docker`) via the Docker socket
- **Async runtime:** `asyncio`
- **Validation / models:** Pydantic v2
- **Metrics / observability:** Prometheus client + structured logging
- **Storage (experiment history):** SQLite (via SQLAlchemy) — pluggable
- **Dashboard:** Served as static assets from FastAPI (HTML + JS; framework-agnostic, no build step required initially)
- **Testing:** Pytest, pytest-asyncio, httpx (API), `testcontainers` (E2E against ephemeral target stacks)

## How to Run

```bash
docker compose up -d         # framework + redis + producer + consumer
curl http://localhost:8000/health
open http://localhost:8000/dashboard
```

Tear down:

```bash
docker compose down --remove-orphans
```

## Dashboard

The dashboard is served as static HTML+JS at `http://localhost:8000/dashboard` (no separate Node build). It lets you:

- Create new experiments via a form (POST `/experiments`).
- Run any experiment (POST `/experiments/{id}/run`) and watch a live chart of CPU/memory/latency over a WebSocket (`/ws/runs/{run_id}`).
- Toggle the global kill switch (POST `/admin/abort`).
- See the circuit-breaker status (polled every 5s from `/admin/circuit-breaker-state`).

### Screenshot

_Placeholder — a captured screenshot of an in-flight 200ms latency injection (live chart, run status, and recovery report panel) will land at `docs/screenshots/dashboard.png` after the Chrome MCP verification pass._

## API

| Method | Path                                | Purpose                                                 |
| ------ | ----------------------------------- | ------------------------------------------------------- |
| GET    | `/health`                           | Liveness + readiness for engine / monitor / docker      |
| GET    | `/metrics`                          | Prometheus scrape endpoint                              |
| POST   | `/experiments`                      | Create a new chaos experiment definition                |
| GET    | `/experiments`                      | List experiments                                        |
| GET    | `/experiments/{id}`                 | Get a single experiment + last result                   |
| POST   | `/experiments/{id}/run`             | Start an experiment run                                 |
| POST   | `/experiments/{id}/abort`           | Abort the active run (triggers rollback)                |
| GET    | `/runs/{run_id}`                    | Fetch run status, timeline, verdict                     |
| GET    | `/targets`                          | List discoverable Docker targets (allowlist-filtered)   |
| POST   | `/admin/abort`                      | Global kill switch — aborts all active runs             |
| POST   | `/admin/dry-run`                    | Toggle dry-run mode (plan only, no injection)           |
| GET    | `/admin/circuit-breaker-state`      | Current SafetySupervisor state and counters             |
| WS     | `/ws/runs/{run_id}`                 | Live stream of run events + metrics (`*` = all runs)    |

Full OpenAPI docs are available at `http://localhost:8000/docs` once the stack is up.

## Safety Model (Planned)

- **Allowlist of targets** — only containers tagged for chaos may be touched.
- **Blast-radius caps** — max % of a service's replicas that can be impaired simultaneously.
- **Automatic rollback** on guardrail breach or supervisor heartbeat loss.
- **Dry-run mode** — render the plan without executing any fault.
- **Kill switch** — single endpoint that aborts all active runs and restores state.

## What I Learned

- **Docker-socket security model.** Mounting `/var/run/docker.sock` into the framework grants root-equivalent host access; we mitigate by filtering containers via a `chaos.target=true` label *and* a config-driven allowlist before any `exec`/`pause`/`disconnect`. The injector refuses to act on anything outside that intersection.
- **`tc netem` requires `NET_ADMIN` on the target, not the framework.** Latency/loss injection runs *inside* the target container via `docker exec`, so the target image needs `iproute2` baked in and the compose service needs `cap_add: [NET_ADMIN]`. The framework container itself stays unprivileged.
- **Asyncio queue decoupling is the cleanest WebSocket fan-out.** A bounded `asyncio.Queue` between the metric collector (1 Hz) and the broadcaster (4 Hz throttled) means a slow client never back-pressures the collector, and the broadcaster can prune dead sockets on a failed send without taking down the loop.
- **Principles of Chaos: steady-state → hypothesis → blast → recover.** The engine's lifecycle (probe → inject → observe → rollback → validate) directly mirrors the canonical chaos loop. Every `RecoveryReport` validates the hypothesis explicitly — the run is only `completed` if the post-fault probes match the pre-fault steady-state within the recovery budget.
- **`pydantic-settings` precedence is env > yaml > defaults.** The `Settings` class loads `config/safety_config.yaml` first, then overrides with env vars (e.g., `DOCKER_SOCKET_PATH`, `CPU_EMERGENCY_THRESHOLD_PCT`), so a container override never requires editing the YAML on disk.
- **`SafetySupervisor` needs debounce, not just a threshold.** A single CPU spike (a single GC pause, a burst of `docker stats` accounting) should not trip the kill switch. The supervisor requires the breach to persist over N consecutive samples before firing emergency stop, which eliminated all observed false trips during E2E.

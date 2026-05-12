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

## Project Status

Scaffold only. Implementation has not started.

## How to Run

_To be filled in once implementation begins._

## What I Learned

_To be filled in as the project evolves._

## API (Planned)

| Method | Path                                | Purpose                                  |
| ------ | ----------------------------------- | ---------------------------------------- |
| POST   | `/experiments`                      | Create a new chaos experiment definition |
| GET    | `/experiments`                      | List experiments                         |
| GET    | `/experiments/{id}`                 | Get a single experiment + last result    |
| POST   | `/experiments/{id}/run`             | Start an experiment run                  |
| POST   | `/experiments/{id}/abort`           | Abort the active run (triggers rollback) |
| GET    | `/runs/{run_id}`                    | Fetch run status, timeline, verdict      |
| GET    | `/targets`                          | List discoverable Docker targets         |
| WS     | `/ws/runs/{run_id}`                 | Live stream of run events + metrics      |

_Endpoints are illustrative and may change during design._

## Safety Model (Planned)

- **Allowlist of targets** — only containers tagged for chaos may be touched.
- **Blast-radius caps** — max % of a service's replicas that can be impaired simultaneously.
- **Automatic rollback** on guardrail breach or supervisor heartbeat loss.
- **Dry-run mode** — render the plan without executing any fault.
- **Kill switch** — single endpoint that aborts all active runs and restores state.

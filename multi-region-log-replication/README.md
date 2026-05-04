# Multi-Region Log Replication Engine

A simulated multi-region distributed log store that replicates writes from a primary region to secondary regions using vector clocks for causal ordering and deterministic conflict resolution.

## Tech Stack

- Python 3.11
- FastAPI + Uvicorn
- Pydantic v2
- Vue 3 + Tailwind (CDN, raw HTML)
- Docker + Docker Compose

## Run

```bash
make build   # build the app image
make test    # run unit tests inside Docker
make run     # docker compose up the full stack (app + redis)
make e2e     # boot stack + run the replication + failover E2E driver
```

## Failover semantics

The `HealthMonitor` background task ticks every `HEALTH_CHECK_INTERVAL_SEC`
seconds (default 1s) and inspects the elected primary's `is_healthy` flag.

- **Detection threshold**: 2 consecutive unhealthy ticks. A single missed
  tick may be a transient blip, so we wait one more before triggering
  failover. With the default 1s cadence the primary must look unhealthy
  for ~2s before re-election fires.
- **Recovery time budget**: <5s end-to-end. Detection (≤2s) plus the
  in-process election step (microseconds) means a real outage is
  re-routed to the next preferred region well inside the 5s SLO.
- **One-way failover**: healing the original primary via `POST
  /api/regions/{id}/heal` does **not** auto-promote it back. The current
  primary (set by the most recent `elect_primary`) keeps the role until
  it itself becomes unhealthy. Re-promoting requires manual reset
  (e.g. restarting the app, or extending the controller with an explicit
  `promote` endpoint).
- **Failover history**: the most recent ten failover events are kept in
  a bounded `collections.deque` and surfaced on every `HealthSnapshot`
  via `recent_failovers: list[dict]` (each entry has `at`, `old_primary`,
  `new_primary`, `elapsed_ms`).

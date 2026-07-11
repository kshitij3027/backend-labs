# rca-analysis-engine

A causal analysis service that ingests distributed log events, reconstructs incident timelines, builds a directed causal graph between events, and ranks the most likely root causes with confidence scores.

## Tech Stack
- Language: Python
- Framework: FastAPI (REST + WebSocket)
- Graph analysis: NetworkX
- Server: Uvicorn (ASGI)
- Deployment: Docker Compose

## How It Runs
A long-lived FastAPI process exposes:
- **REST endpoints**
  - `POST /api/analyze-incident` — submit log events for causal analysis
  - `GET /api/incidents` — list/query reconstructed incidents
  - `GET /api/health` — health check
- **WebSocket channel**
  - `/ws` — real-time incident push
- **Browser dashboard** served alongside the API for visualizing incident timelines and causal graphs.

## How to Run
<!-- Fill in as development progresses -->

## What I Learned
<!-- Fill in as the project evolves -->

## API Docs
<!-- Fill in as endpoints are implemented -->

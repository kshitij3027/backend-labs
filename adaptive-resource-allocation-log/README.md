# Adaptive Resource Allocation (Log Processing)

A system that monitors real-time cluster metrics, predicts near-future load, and
automatically scales processing resources (containers/workers) up or down to
maintain optimal performance. It runs as a long-lived server process exposing an
HTTP API and a live, browser-based dashboard.

## What It Does

- **Collects metrics** — a background loop continuously samples cluster/worker
  resource metrics (CPU, memory, queue depth, throughput) on a fixed interval.
- **Predicts near-future load** — a lightweight forecasting model projects load a
  few intervals ahead from the recent metric history.
- **Auto-scales** — an orchestration loop compares predicted load against target
  utilization thresholds and scales the pool of processing workers up or down to
  keep performance within the desired band (with cooldowns to avoid flapping).
- **Streams live state** — current metrics, predictions, and scaling decisions are
  pushed to a browser dashboard in real time over WebSockets.

## How It Runs

- A single long-lived server process: **Flask + Flask-SocketIO on port `8080`**.
- Two background loops alongside the web server:
  - a **metric-collection loop** that samples and stores recent metrics, and
  - an **orchestration loop** that predicts load and issues scaling actions.
- Interacted with via **HTTP API endpoints** and a **browser-based dashboard**.

## Tech Stack

- **Language:** Python
- **Web / real-time:** Flask, Flask-SocketIO (eventlet server)
- **Metrics:** psutil
- **Prediction:** NumPy
- **Testing:** pytest

## How to Run

<!-- Filled in once the implementation lands. Will cover:
     - installing requirements into a virtualenv
     - starting the server on port 8080
     - opening the dashboard at http://localhost:8080 -->

## API & Dashboard

<!-- Endpoint reference and dashboard usage to be documented during implementation. -->

## What I Learned

<!-- Key takeaways captured as the project evolves. -->

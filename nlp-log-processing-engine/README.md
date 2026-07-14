# nlp-log-processing-engine

Extracts semantic meaning from free-text log messages — **entity recognition**, **intent classification**, **sentiment analysis**, and **keyword extraction** — exposed through a REST API and a real-time web dashboard.

---

## What it does

Raw log lines are noisy, unstructured free text. This engine turns each log message into structured semantic signal:

| Capability | What it produces |
|---|---|
| **Entity recognition (NER)** | Named entities in the message — services, hosts, IPs, user IDs, error codes, file paths, URLs. |
| **Intent classification** | The *purpose* of the log line — e.g. `authentication`, `deployment`, `error_report`, `health_check`, `resource_warning`. |
| **Sentiment / severity analysis** | Emotional/operational tone — from `positive`/`neutral` up through `negative`/`critical` — to surface distress signals in text that isn't formally tagged as an error. |
| **Keyword extraction** | The salient terms/phrases that summarize the message, for search, tagging, and trend detection. |

Results are returned per-message over the API and streamed to a dashboard that visualizes entities, intent/sentiment distributions, and trending keywords in real time.

---

## Tech stack

- **Language:** Python 3.11+
- **API framework:** framework-agnostic in our implementation (Flask is the reference design; we may use Flask or FastAPI)
- **NLP:** spaCy (entities, tokenization), scikit-learn (intent classification), plus lightweight sentiment + keyword extraction (e.g. VADER / RAKE / TF‑IDF)
- **Dashboard:** web UI served by the app, with live updates (WebSocket / SSE polling)
- **Serving model:** long-lived server process exposing a REST API on a local port

> Note: exact library choices are finalized during implementation. This document describes intent, not a frozen dependency contract.

---

## How it runs

A long-lived server hosts both the REST API and the web dashboard. A startup script launches the process; once up, it's reachable on a local port (default configurable via env var).

```
# high-level shape (implementation pending — not yet built)
./start.sh            # or: python -m app
# → server listens on http://localhost:<PORT>
# → dashboard at    http://localhost:<PORT>/
# → REST API under  http://localhost:<PORT>/api/...
```

---

## API (planned)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/analyze` | Analyze a single log message; returns entities, intent, sentiment, keywords. |
| `POST` | `/api/analyze/batch` | Analyze multiple messages in one request. |
| `GET`  | `/api/health` | Liveness/readiness check. |
| `GET`  | `/api/stats` | Aggregate stats powering the dashboard (intent/sentiment distributions, trending keywords). |

Example (planned) response for `POST /api/analyze`:

```json
{
  "message": "auth service on host web-03 rejected login for user 4821: invalid token",
  "entities": [
    {"text": "auth service", "label": "SERVICE"},
    {"text": "web-03", "label": "HOST"},
    {"text": "4821", "label": "USER_ID"}
  ],
  "intent": {"label": "authentication", "confidence": 0.94},
  "sentiment": {"label": "negative", "score": -0.6},
  "keywords": ["rejected login", "invalid token", "auth service"]
}
```

---

## What I learned

_To be filled in as the project is built — notes on NLP model selection, serving unstructured text at low latency, and dashboard streaming patterns._

---

## Status

📋 **Scaffolding only.** This folder currently contains just the README, `requirements.txt`, and `.gitignore`. No application code or Docker configuration has been written yet.

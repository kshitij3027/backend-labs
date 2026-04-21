# log-fulltext-search-rerank

A log search system that uses TF-IDF and multi-factor scoring to return relevance-ranked log entries in response to natural language queries.

## Overview

Logs are indexed in real-time as they arrive. Queries are served on-demand via HTTP endpoints. A long-lived FastAPI server accepts natural language search queries, scores candidate log entries using TF-IDF, re-ranks them with a multi-factor scoring function (term frequency, recency, severity, field boosts, etc.), and returns the top-k results.

## Tech Stack

- **Language:** Python 3.11+
- **Framework:** FastAPI (REST API) + Uvicorn (ASGI server)
- **Search / Scoring:** TF-IDF via scikit-learn, custom multi-factor reranker
- **Text processing:** NLTK (tokenization, stopwords, stemming)
- **Storage:** In-memory inverted index (with optional disk persistence)
- **Testing:** pytest, pytest-asyncio, httpx

## How It Runs

- Long-lived REST API server (FastAPI + Uvicorn).
- Logs are ingested via `POST /logs` (single or batch) and indexed in real-time.
- Queries are served via `GET /search?q=...` and return ranked results on-demand.
- Optional web frontend for interactive querying.

## Planned API (subject to change)

| Method | Path           | Purpose                                      |
|--------|----------------|----------------------------------------------|
| POST   | `/logs`        | Ingest one or more log entries               |
| GET    | `/search`      | Search logs with natural language query      |
| GET    | `/stats`       | Index stats (doc count, unique terms, etc.)  |
| GET    | `/health`      | Liveness probe                               |
| DELETE | `/index`       | Reset the index (dev/testing only)           |

## Scoring Factors (planned)

1. **TF-IDF** — base lexical relevance score.
2. **Recency** — newer logs weighted higher (exponential decay).
3. **Severity** — ERROR/WARN boosted over INFO/DEBUG.
4. **Field boosts** — matches in `message` > `service` > `host`.
5. **Exact-phrase bonus** — bump for phrase matches.
6. **Length normalization** — avoid bias toward very long entries.

## How to Run

_To be filled in as development progresses._

## What I Learned

_To be filled in as the project evolves._

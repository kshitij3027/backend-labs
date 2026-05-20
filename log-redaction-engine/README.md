# Log Redaction Engine

A real-time log processing service that detects and redacts sensitive data (PII, PHI, payment info) from log entries using configurable strategies. Exposes a REST API for programmatic use and a live web dashboard for inspection.

The goal: give engineers a stateless HTTP boundary they can point their log shippers at, so structured log payloads come out the other side with credit-card numbers, SSNs, email addresses, phone numbers, medical record IDs, and other sensitive fields replaced — under a redaction strategy chosen per-field or per-pattern.

Built as a learning exercise in the `backend-labs` mono-repo. The focus is on **detection rules layered correctly** (field-name match → value regex → checksum validation), **configurable redaction strategies** (mask / hash / tokenize / drop / partial), and **operating the whole thing as a long-lived HTTP service with live observability** rather than a one-shot CLI filter.

## How it runs

| Service        | URL                              | What it is                                                                 |
|----------------|----------------------------------|----------------------------------------------------------------------------|
| App (HTTP API) | http://localhost:8000            | FastAPI + uvicorn. Detection + redaction endpoints + dashboard + metrics.  |
| Dashboard      | http://localhost:8000/           | Server-rendered page. Paste a log, see what was detected and how it was redacted. |
| Prometheus     | http://localhost:8000/metrics    | HTTP histograms + custom counters (`redactions_total`, `detections_total`, per-strategy). |

Stateless processing service — no database, no queue, no external dependencies required to run the core path. The redaction rules live in YAML config loaded at startup; the audit trail of what got redacted (counts only, never plaintext) lives in an in-process ring buffer.

## Planned scope

- **Detection layers**
  - Field-name match (e.g., a field literally named `password` or `ssn` is sensitive regardless of value).
  - Value-regex match for emails, phone numbers, IPv4/IPv6, JWTs, AWS keys, credit cards (with Luhn validation), SSNs (with area-number validation), and similar.
  - Configurable categories: **PII** (name, email, phone, address), **PHI** (medical record IDs, diagnosis codes), **payment** (PAN, CVV, expiry, IBAN).
- **Redaction strategies** (chosen per-pattern or per-field in config)
  - `mask` — replace with a fixed token, e.g. `[REDACTED]` or `***`.
  - `partial` — keep last 4 digits of a card, first character of an email local-part, etc.
  - `hash` — deterministic SHA-256 (with a per-deployment salt) so the redacted token still joins across logs.
  - `tokenize` — random opaque token, no reversibility (no token vault here — that's a separate project).
  - `drop` — remove the field entirely from the output document.
- **REST API surface (planned)**
  - `POST /v1/redact` — single log entry in, redacted log out.
  - `POST /v1/redact/batch` — many in one call.
  - `POST /v1/detect` — dry-run: return detections without applying redaction (lets a caller preview what would be touched).
  - `GET /v1/rules` — list active patterns and the strategy bound to each.
  - `GET /api/health`, `GET /api/stats`, `GET /metrics`.
- **Dashboard**
  - Live counters (logs processed, fields redacted, by category, by strategy).
  - Paste-a-log textarea that returns the redacted version side-by-side with the detections that fired.

## Tech Stack

- **Language**: Python 3.11+
- **Framework**: FastAPI + uvicorn (ASGI)
- **Validation**: pydantic v2 + pydantic-settings
- **Templating**: Jinja2 (server-rendered dashboard, no JS build step)
- **Config**: YAML for patterns + redaction rules, `.env` for runtime settings
- **Observability**: prometheus-client + prometheus_fastapi_instrumentator
- **Testing**: pytest + httpx + asgi-lifespan
- **Runtime**: Docker + docker-compose (to be added)

## How to Run

<!-- To be filled in once the service is implemented. Planned shape: -->

```bash
# Planned — not yet implemented
make demo      # builds image, starts the stack, prints the dashboard URL
make test      # full pytest suite inside Docker
make e2e       # curl-driven end-to-end against the live container
make down      # stops the stack
```

## What I Learned

<!-- Filled in as the project is built. -->

## Status

Scaffold only. Code, Dockerfile, and tests have not been written yet.

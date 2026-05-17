# Field-Level Log Encryption Service

A middleware service that detects PII (Personally Identifiable Information) in structured log entries and selectively encrypts only the sensitive fields using AES-256-GCM, while leaving operational data (timestamps, log levels, service names, request IDs, etc.) readable for debugging and observability.

The goal: let engineers keep their logs useful for triage without ever exposing user data in plaintext to log storage, downstream pipelines, or SIEM systems.

---

## What It Does

- **Accepts structured log entries** (JSON) via an HTTP/REST API.
- **Scans each field** to detect PII using a combination of:
  - Field-name heuristics (e.g., `email`, `ssn`, `phone`, `password`, `credit_card`).
  - Value-level regex/pattern detectors (emails, phone numbers, IPs, SSN-like patterns, JWTs, etc.).
  - Optional allow-list / deny-list overrides per tenant or service.
- **Encrypts only the sensitive fields** in place using **AES-256-GCM** with a per-record nonce.
- **Preserves the log structure** — operational fields (`timestamp`, `level`, `service`, `trace_id`, etc.) remain plaintext.
- **Returns or forwards** the transformed log entry with sensitive fields replaced by a structured ciphertext blob plus metadata:
  ```json
  {
    "ciphertext": "<base64>",
    "nonce": "<base64>",
    "key_id": "key-v1",
    "alg": "AES-256-GCM",
    "detector": "regex:email"
  }
  ```
- **Provides a decrypt endpoint** for authorized callers (e.g., on-call engineers) to selectively reveal a single field for a single record — every decrypt is audit-logged.
- **Exposes a web dashboard** for live monitoring of throughput, detection counts by PII type, encryption latency, and an interactive "try it" panel for testing detection rules against sample log lines.

---

## How It Runs

- **Long-lived server process** exposing an HTTP/REST API built with **FastAPI**.
- Logs are submitted via API, processed through the encryption pipeline (detect → classify → encrypt → assemble), and returned (or forwarded to a configured sink) with sensitive fields replaced by encrypted blobs + metadata.
- A **web dashboard** (served by the same FastAPI app) provides monitoring metrics and an interactive testing console.
- Key material is loaded from environment variables / a key file at startup; the service supports key rotation by `key_id` so old ciphertext remains decryptable after rotation.

---

## Tech Stack

- **Language:** Python 3.11+
- **Framework:** FastAPI (HTTP API + dashboard)
- **ASGI server:** Uvicorn
- **Crypto:** `cryptography` (AES-256-GCM via the `AESGCM` primitive)
- **Validation:** Pydantic v2
- **Templating (dashboard):** Jinja2 + static assets
- **Metrics:** Prometheus client (`prometheus-client`)
- **Testing:** pytest + httpx
- **Config:** `python-dotenv` for local dev

---

## Planned API Surface

| Method | Endpoint               | Purpose                                                              |
|--------|------------------------|----------------------------------------------------------------------|
| POST   | `/v1/logs/encrypt`     | Submit one or more log entries; receive encrypted-field versions.    |
| POST   | `/v1/logs/decrypt`     | Authorized selective decryption of a single field. Audit-logged.     |
| POST   | `/v1/detect`           | Dry-run: return which fields would be classified as PII, no encrypt. |
| GET    | `/v1/keys`             | List active key IDs and their status (active / retired).             |
| GET    | `/metrics`             | Prometheus-format metrics.                                           |
| GET    | `/healthz`             | Liveness probe.                                                      |
| GET    | `/`                    | Web dashboard (monitoring + interactive tester).                     |

(Exact paths and request/response schemas will be finalized during implementation.)

---

## Planned Project Layout

```
field-level-log-encryption-service/
├── README.md
├── requirements.txt
├── .gitignore
└── (source code, tests, docker files — added later, on approval)
```

---

## How to Run

> _To be filled in once the implementation lands. Will be Docker-based, following the repo convention._

---

## What I Learned

> _To be filled in as the project evolves. Topics expected to come up:_
> - AES-256-GCM nonce hygiene and the cost of nonce reuse.
> - Trade-offs between regex-based and ML-based PII detection.
> - Designing for key rotation without breaking historical ciphertext.
> - Keeping encryption latency low enough to sit in the hot logging path.

---

## Status

**Scaffold only.** Implementation has not started — awaiting approval before any code is written.

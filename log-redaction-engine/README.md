# Log Redaction Engine

A real-time log processing service that detects sensitive data (PII, PHI, payment info) in structured log entries and redacts it under a configurable per-pattern strategy — mask, partial, hash, or tokenize — before the log ever leaves the boundary.

## What it does

Point your log shippers at the HTTP endpoint; structured payloads come back with credit-card numbers, SSNs, email addresses, phone numbers, medical record IDs, person names, and organization names rewritten according to the active redaction preset. The engine ships three presets out of the box (`general`, `healthcare`, `financial`), supports atomic hot-reload of the policy without restarting the service, and exposes per-regime compliance reports (GDPR / HIPAA / PCI_DSS) drawn from a sealed-schema audit trail that never stores plaintext.

Detection layers regex (with regex-evaluation timeouts to bound catastrophic backtracking), Luhn validation for card numbers, and spaCy NER for person/org names — short fragments skip NER via a length gate so high-throughput regex-only paths stay hot. The whole thing is a single FastAPI process with a Jinja2 + HTMX dashboard, Prometheus metrics, and an optional Redis backend for cross-process token-mirror consistency.

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│  Browser → http://localhost:8000/    (Jinja2 + HTMX dashboard, 5s poll)│
│      │                                                                  │
│  Client → POST /api/redact         (batch redaction)                    │
│           POST /v1/detect          (dry-run, never returns plaintext)   │
│           GET  /api/config         POST /api/config  (hot-reload)       │
│           GET  /api/stats          /api/compliance/{GDPR|HIPAA|PCI_DSS} │
│           GET  /api/health         /metrics                             │
│      │                                                                  │
│      ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  FastAPI on :8000                                                │  │
│  │                                                                  │  │
│  │   RedactionProcessor (pipeline)                                  │  │
│  │     ┌──────────┐    ┌───────────────┐   ┌────────────────────┐  │  │
│  │     │ Detector │ →  │ Configuration │ → │ StrategyRegistry   │  │  │
│  │     │  regex   │    │   Manager     │   │  mask | partial    │  │  │
│  │     │  + Luhn  │    │  (atomic      │   │  | hash | tokenize │  │  │
│  │     │  + NER   │    │   hot-reload) │   │                    │  │  │
│  │     └──────────┘    └───────────────┘   └─────────┬──────────┘  │  │
│  │                                                   │              │  │
│  │                                              TokenStore          │  │
│  │                                       (in-memory + Redis-backed │  │
│  │                                        cross-process mirror)    │  │
│  │                                                   │              │  │
│  │   AuditLogger (RingBuffer) ← every redaction / detect / reload  │  │
│  │   StatsCounters (atomic)     event; sealed schema, no plaintext │  │
│  │   PrometheusInstrumentator → /metrics                            │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

Stateless processing service — Redis is optional (the service falls back to an in-memory backend on Redis-unreachable). The policy lives in JSON presets under `config/presets/`; the audit ring buffer is bounded, the salt for `hash` strategy is loaded at startup from `REDACTION_HASH_SALT`.

## Tech Stack

| Layer                | Library / Tool                                            |
|----------------------|-----------------------------------------------------------|
| Language             | Python 3.11                                               |
| HTTP framework       | FastAPI 0.115 + uvicorn 0.30 (ASGI)                       |
| Validation / config  | pydantic 2.9 + pydantic-settings 2.5                      |
| Detection — regex    | `re` with per-pattern evaluation timeouts                 |
| Detection — names    | spaCy 3.7 + `en_core_web_sm` (baked into the image)       |
| Templating           | Jinja2 3.1 + HTMX 1.9 (server-rendered, no JS build step) |
| Metrics              | prometheus-client 0.20 + prometheus-fastapi-instrumentator 7 |
| Cache                | redis-py 5.0 (with in-memory fallback)                    |
| Testing              | pytest 8.3 + httpx 0.27 + asgi-lifespan 2.1               |
| Runtime              | Docker + docker-compose                                   |

## How to Run

Every workflow is Docker-first — never run pytest or the app on the host directly.

```bash
# One-time setup: generate a fresh salt and write it to .env.
cp .env.example .env
python3 -c 'import secrets; print("REDACTION_HASH_SALT=" + secrets.token_hex(32))' >> .env

# Build images + start the stack + print the dashboard URL.
make demo

# Open the dashboard.
open http://localhost:8000/      # macOS; xdg-open on Linux

# When done:
make down
```

For testing inside Docker:

```bash
make build                 # build app + tester images
make test                  # full pytest suite (249 tests, coverage report)
make e2e                   # curl-driven end-to-end against the live app
make throughput            # 1000-log batch — asserts >=1000 logs/sec
make logs                  # tail app logs
make clean                 # down + remove locally-built images
```

`make e2e` and `make throughput` both assume `make up` already ran; the targets stay focused on the probe itself so an operator can re-run them without paying restart cost.

## API Surface

| Method | Endpoint                            | Purpose                                                                |
|--------|-------------------------------------|------------------------------------------------------------------------|
| POST   | `/api/redact`                       | Batch redaction — `{"log_entries":[...]}` in, `{"processed_entries":[...]}` out. |
| POST   | `/v1/detect`                        | Dry-run detection — returns detection metadata only, NEVER plaintext.   |
| GET    | `/api/config`                       | Current active redaction policy as JSON.                                |
| POST   | `/api/config`                       | Atomic hot-reload of the policy; old config stays active on 422.        |
| GET    | `/api/stats`                        | Throughput / latency / pattern-hit counters.                            |
| GET    | `/api/compliance/{GDPR\|HIPAA\|PCI_DSS}` | Per-regime redaction summary drawn from the audit ring buffer.    |
| GET    | `/api/health`                       | Liveness probe: `{"status":"healthy", "service":"log-redaction-engine"}`. |
| GET    | `/metrics`                          | Prometheus exposition (default HTTP + custom counters).                 |
| GET    | `/`                                 | Live dashboard (HTML + HTMX poll).                                      |
| GET    | `/api/stats/html`                   | HTMX partial used by the dashboard's stats card.                        |
| GET    | `/api/pattern_hits/html`            | HTMX partial used by the dashboard's pattern-hits table.                |

## Demo Flow

A round trip against the live container under the default `general` preset.

**Request:**

```bash
curl -fsS -X POST http://localhost:8000/api/redact \
  -H "Content-Type: application/json" \
  -d '{
    "log_entries": [
      {
        "message": "User alice@example.com called (415) 555-1234",
        "timestamp": "2026-05-21T10:00:00Z",
        "level": "INFO"
      }
    ]
  }'
```

**Response (excerpt — counters and exact ordering elided):**

```json
{
  "processed_entries": [
    {
      "message": "User a***@example.com called (***) ***-1234",
      "timestamp": "2026-05-21T10:00:00Z",
      "level": "INFO",
      "redactions": [
        {"pattern": "email",    "strategy": "partial", "start": 5,  "end": 22},
        {"pattern": "us_phone", "strategy": "partial", "start": 30, "end": 44}
      ]
    }
  ]
}
```

The email's local-part collapses to `a***` (one char preserved); the phone keeps only the last 4 digits. SSNs and MRNs fall through to `mask` under the same preset; switch to `healthcare` and they become `partial` (`***-**-6789`, `MRN-***456`).

Try it visually: open http://localhost:8000/, paste the JSON above into the textarea, click **Redact** — the side-by-side panes show input and redacted output.

## Configuration & Presets

Three presets ship under `config/presets/`:

| Preset       | Active regime(s) | Notable bindings                                                      |
|--------------|------------------|-----------------------------------------------------------------------|
| `general`    | GDPR             | email→partial, us_phone→partial, ssn→mask, credit_card→mask, mrn→mask |
| `healthcare` | HIPAA            | mrn→partial, ssn→partial, email→mask, us_phone→mask, person→mask      |
| `financial`  | PCI_DSS + GDPR   | credit_card→mask, ssn→hash, email→tokenize                            |

The active preset at boot is the value of `REDACTION_PRESET` in `.env`. Atomic hot-reload happens via `POST /api/config` — the new policy is validated outside the lock, swapped in under the lock on success, and rolled back to the old policy on 422. There is no restart, no warm-up, no dropped traffic.

Hash strategy needs a salt: `REDACTION_HASH_SALT` must be set in `.env` (the service fails fast on startup if it's missing). Tokenize strategy needs the in-process `TokenStore`; cross-process consistency lands on Redis when `REDIS_ENABLED=true`, with graceful in-memory fallback on Redis-unreachable.

## Test Suite

- **249 tests** total, all run inside Docker (`make test`):
  - **207 unit tests** — detection (41 — regex, Luhn, length-gated NER, timeout guard), strategies (42 — mask, partial per-pattern, hash, tokenize), token store (15 — admin-gated reverse map, eviction), configuration manager (18 — atomic reload, JSON round-trip), audit + ring buffer (18 — sealed schema, append-only), stats counters (12 — atomic increments), compliance reports (8 — per-regime aggregation, 100k-event budget), cache backends (16 — Redis + in-memory), processor (29), NER (6), smoke (2).
  - **42 integration tests** — full API surface via `asgi-lifespan` + `httpx` (redact 11, detect 7, config 8, compliance 4, dashboard 5), plus Redis backend integration (7) gated on a live container.
- **Coverage**: ~92% on `src/*` (`pytest --cov=src --cov-fail-under=90` runs on every `make test`).
- Every test runs in the `tester` profile of `docker-compose.yml`, so test results match production layout exactly. The tester image bakes `en_core_web_sm` so NER tests don't re-download the model on every run.

## Throughput

The C11 baseline is **≥1000 logs/sec** sustained on a single host, measured via `scripts/throughput.py`:

```bash
make up
make throughput     # 1000-log batch, asserts >=1000 logs/sec; warns <10000
make down
```

The probe sends a 10-entry warm-up (pays the spaCy lazy-load + regex JIT cost), then a timed 1000-entry batch. Each entry stays under the 40-char `NER_MIN_LENGTH` gate so the measurement reflects the regex-only hot path — which is what most production log shippers actually hit. A stretch target of 10000 logs/sec produces a soft warning (no fail) when missed.

## Compliance Reports

```bash
curl -fsS http://localhost:8000/api/compliance/HIPAA | jq
```

```jsonc
{
  "rule_set": "HIPAA",
  "generated_at": "2026-05-21T17:34:00.123456+00:00",
  "report_window_start": "2026-05-21T17:32:00.000000+00:00",
  "report_window_end":   "2026-05-21T17:33:55.000000+00:00",
  "total_redactions": 12,
  "breakdown":   {"mrn": 7, "ssn": 5},
  "strategies_used": {"mask": 12},
  "report_generation_time_ms": 2.41
}
```

The aggregation walks the audit ring buffer once (O(n)), filtered by `compliance_tags`. The spec budget is **30 s for 100k events**; the test suite asserts the actual cost stays comfortably under that on laptop-class CPUs.

## Audit & no-plaintext invariant

The `AuditEvent` model has slots for `pattern_name`, `strategy`, `field_name`, `entry_id`, `outcome`, `compliance_tags` — but **no slot for the matched plaintext**. Pydantic's `extra="forbid"` policy means a future regression that tries to attach a plaintext value fails schema validation rather than silently leaking; the `_value_preview` masker on the detect endpoint enforces the same boundary on the wire side (first 2 chars + `***` + last 2 chars for values ≥5 chars long; full asterisk-mask otherwise). The ring buffer is bounded — once `AUDIT_BUFFER_SIZE` is reached the oldest event is silently evicted, which means the system gracefully degrades the audit trail under sustained traffic rather than backpressuring the redact path.

## What I Learned

- **Layered detection beats a single regex monster.** Field-name match (a field literally named `password` is sensitive regardless of value), then value regex with Luhn validation, then NER for free-text names — each layer cheap and skippable. The length gate on NER is the single biggest throughput knob.
- **Atomic hot-reload is a "validate then swap" problem.** Validate the new policy OUTSIDE the lock, then rebind a single attribute INSIDE the lock — no rolling restart, no warm-up, no dropped traffic. The 422-rollback test is the load-bearing one.
- **A sealed-schema audit event is the cheapest "no plaintext" guarantee.** Pydantic v2's `extra="forbid"` plus a slot-free model for plaintext means the only way to violate the invariant is to edit the model — which a code review catches.
- **Prometheus + HTMX gives you a live dashboard for free.** Two HTMX partials polling at 5 s and 10 s, three counters surfaced through the same view layer the JSON API uses — no JS framework, no SPA hydration.
- **The `tester` compose profile is what makes Docker-first tests bearable.** Bind-mount `src/` and `tests/` so iteration doesn't rebuild; bake `en_core_web_sm` into the image so the model isn't a per-run download; gate Redis-backed tests on a healthy compose dependency.

## Project Layout

```
log-redaction-engine/
├── README.md
├── plan.md                            (commit-by-commit plan — frozen)
├── project_requirements.md
├── requirements.txt / pyproject.toml / pytest.ini
├── Dockerfile / Dockerfile.test
├── docker-compose.yml / Makefile / .env.example
├── config/
│   ├── default.json
│   └── presets/
│       ├── general.json
│       ├── healthcare.json
│       └── financial.json
├── src/
│   ├── main.py                        (FastAPI app + lifespan handler)
│   ├── settings.py                    (pydantic-settings)
│   ├── api/
│   │   ├── routes.py                  (every business endpoint)
│   │   ├── models.py                  (request/response schemas)
│   │   └── metrics.py                 (custom Prometheus counters)
│   ├── audit/
│   │   ├── audit_logger.py            (sealed-schema event emitter)
│   │   ├── events.py                  (AuditEvent — no plaintext slot)
│   │   └── ring_buffer.py             (bounded thread-safe deque)
│   ├── cache/
│   │   ├── backend.py                 (Backend ABC)
│   │   ├── in_memory.py               (dict + RLock fallback)
│   │   └── redis_backend.py           (redis-py client)
│   ├── compliance/
│   │   └── reports.py                 (per-regime aggregator)
│   ├── config/
│   │   ├── loader.py                  (preset loader)
│   │   ├── manager.py                 (ConfigurationManager — atomic reload)
│   │   └── models.py                  (RedactionConfig pydantic model)
│   ├── detection/
│   │   ├── detector.py                (regex orchestrator)
│   │   ├── ner.py                     (spaCy en_core_web_sm wrapper)
│   │   └── patterns.py                (regex + Luhn)
│   ├── processor/
│   │   └── redaction_processor.py     (detect → strategy → assemble pipeline)
│   ├── redaction/
│   │   ├── strategies.py              (MaskStrategy / PartialStrategy / ...)
│   │   ├── salt.py                    (REDACTION_HASH_SALT loader)
│   │   └── token_store.py             (reversible tokenization)
│   └── stats/
│       ├── counters.py                (PatternCounters)
│       ├── latency.py                 (LatencyHistogram — p50/p95/p99)
│       └── throughput.py              (sliding-window ops/sec)
├── templates/
│   ├── dashboard.html
│   ├── _stats_card.html               (HTMX partial)
│   └── _pattern_hits.html             (HTMX partial)
├── static/
│   ├── dashboard.css
│   └── htmx.min.js                    (HTMX 1.9, vendored)
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   │   ├── log_pii.json
│   │   ├── log_phi.json
│   │   ├── log_pci.json
│   │   └── log_mixed_batch.json
│   ├── unit/                          (207 tests across 11 files)
│   └── integration/                   (42 tests — API + Redis)
└── scripts/
    ├── e2e.sh                         (curl matrix vs the live app; PASS/FAIL summary)
    └── throughput.py                  (1000-log batch; asserts >=1000 logs/sec)
```

## Status

Complete. Code, Docker images, full test suite, dashboard, Prometheus instrumentation, compliance reporting, and the `make e2e` runner all in place.

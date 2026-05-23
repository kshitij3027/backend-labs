# Immutable Audit Trail System

A cryptographically secure audit logging system that intercepts every log-access request and records it in a tamper-evident hash chain for compliance and forensic verification.

The idea: any time a downstream service reads, searches, exports, or redacts log data, an `@audit_access` decorator wraps that call and emits a sealed audit record. Each record links to the previous one by a SHA-256 hash of its predecessor's contents, so a single forged or deleted record breaks the chain and is detected on the next verification pass. The chain plus per-record HMAC/Ed25519 signatures give you a forensic trail that survives both insider tampering and silent storage corruption.

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize **append-only hash chains**, **decorator-based interception**, and **compliance-grade verification reports** as they appear in real audit subsystems (GDPR Art. 30, HIPAA 164.312, SOC 2 CC7.2, PCI DSS 10).

---

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** FastAPI + uvicorn (REST API + Jinja2 dashboard)
- **Data validation:** pydantic v2 + pydantic-settings
- **Crypto:** `cryptography` (Ed25519 signatures) + `hashlib` (SHA-256 hash chain) + `hmac` (constant-time compare)
- **Persistence:** SQLite via SQLAlchemy 2 + aiosqlite — append-only `audit_records` table, write-once semantics enforced in the ORM layer
- **Dashboard:** Jinja2 templates + HTMX (server-rendered, no JS build step)
- **Observability:** prometheus-client + prometheus_fastapi_instrumentator
- **Testing:** pytest, pytest-asyncio, asgi-lifespan, httpx
- **Containerization (later):** Docker + docker-compose

---

## How It Runs

A long-lived FastAPI service exposes:

1. A **decorator** (`@audit_access`) that any caller can wrap around an existing log-access function. On every invocation the decorator captures `(actor, action, resource, timestamp, args_digest, result_digest)` and appends a sealed record to the chain.
2. A **REST API** for querying records, verifying the chain, exporting compliance reports, and (optionally) ingesting audit events from external services that can't import the decorator directly.
3. A **web dashboard** for live monitoring: latest records, integrity status, per-actor activity, and one-click report generation.

```
┌──────────────────────────────────────────────────────────────────────┐
│  Browser → http://localhost:8000/      (Jinja2 dashboard + HTMX)     │
│                                                                       │
│  Wrapped service ─── @audit_access ────►  POST /v1/audit/append      │
│  (e.g. log search,                          ▲                         │
│   redaction API,                            │ (in-process or HTTP)   │
│   export endpoint)                          │                         │
│                                             ▼                         │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  FastAPI on :8000                                               │ │
│  │                                                                  │ │
│  │   ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐   │ │
│  │   │ Interceptor  │ → │  ChainAppender   │ → │  SQLite      │   │ │
│  │   │ (@decorator) │   │ - prev_hash      │   │  (WAL mode,  │   │ │
│  │   │              │   │ - SHA-256 link   │   │   append-    │   │ │
│  │   │              │   │ - Ed25519 sign   │   │   only)      │   │ │
│  │   └──────────────┘   └──────────────────┘   └──────┬───────┘   │ │
│  │                                                      │           │ │
│  │   ┌──────────────────────┐   ┌──────────────────────▼────────┐  │ │
│  │   │  Verifier             │ ← │  Query / Report APIs          │  │ │
│  │   │  - replay chain       │   │  /v1/records  /v1/verify      │  │ │
│  │   │  - check signatures   │   │  /v1/reports/{framework}      │  │ │
│  │   │  - emit gap diagnosis │   │                                │  │ │
│  │   └──────────────────────┘   └────────────────────────────────┘  │ │
│  │                                                                   │ │
│  │   Prometheus /metrics  •  Atomic stats counters                  │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Core Concepts

### Hash Chain (tamper evidence)

Every audit record has shape:

```
{
  "seq":         <monotonic int, starts at 1>,
  "timestamp":   <UTC ISO-8601, server-assigned>,
  "actor":       <subject identifier, e.g. user/service>,
  "action":      <"read" | "search" | "export" | "redact" | ...>,
  "resource":    <opaque resource id — log file, query, record set>,
  "args_digest": <SHA-256 of canonicalised call args>,
  "result_digest":<SHA-256 of canonicalised result or row-count summary>,
  "prev_hash":   <SHA-256 of the previous record's canonical bytes>,
  "self_hash":   <SHA-256 of all fields above, computed on append>,
  "signature":   <Ed25519 over self_hash, base64>
}
```

The chain head is bootstrapped with a deterministic genesis record (`seq=0`, `prev_hash=000…`). Modifying or deleting any record causes the `prev_hash` of the next record to mismatch, and the `self_hash`/signature of the modified record to mismatch — verification reports the exact `seq` of the break.

### Decorator-based Interception

```python
from immutable_audit_trail import audit_access

@audit_access(action="search", resource_from="query.target")
async def search_logs(query: LogQuery) -> list[LogRecord]:
    ...
```

The decorator inspects the bound arguments + return value, redacts anything matching the sensitive-field policy, computes the two SHA-256 digests, and POSTs (or in-process calls) `ChainAppender.append(...)`. No application code changes beyond the decorator.

### Compliance Reports

`/v1/reports/{framework}` emits a structured report scoped to a time range and actor/resource filters:

- **GDPR Art. 30** — record of processing activities, lawful basis tags, retention.
- **HIPAA 164.312** — audit controls (PHI access log + integrity verification result).
- **SOC 2 CC7.2** — system monitoring and anomaly indicators.
- **PCI DSS 10.2** — daily audit log review with integrity attestation.

Each report bundles: filtered records, the verification result for the spanning range, and an Ed25519 signature over the bundle.

---

## API Endpoints (planned)

| Method | Path | Purpose |
|--------|------|---------|
| POST   | `/v1/audit/append`              | Append a sealed audit record (called by the decorator or external services). |
| GET    | `/v1/records`                   | Paginated query: filter by `actor`, `action`, `resource`, time range. |
| GET    | `/v1/records/{seq}`             | Fetch a single record by sequence number. |
| GET    | `/v1/verify`                    | Verify the full chain. Returns `{ok, head_seq, first_break_seq?, signature_failures[]}`. |
| GET    | `/v1/verify?from=X&to=Y`        | Verify only the `[X, Y]` range — fast incremental verification. |
| GET    | `/v1/reports/{framework}`       | Generate a compliance report (`gdpr`, `hipaa`, `soc2`, `pci_dss`). |
| GET    | `/api/health`                   | Liveness probe. |
| GET    | `/api/stats`                    | Atomic counter snapshot (`records_appended`, `verifications_run`, `integrity_breaks_detected`). |
| GET    | `/metrics`                      | Prometheus exposition. |
| GET    | `/`                             | Live dashboard. |

---

## Configuration (planned)

| Variable | Default | Purpose |
|----------|---------|---------|
| `PORT` | `8000` | HTTP port. |
| `LOG_LEVEL` | `INFO` | Application log level. |
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/audit.db` | SQLAlchemy URL — defaults to a local SQLite file. |
| `SIGNING_KEY_B64` | (required) | Base64 of a 32-byte Ed25519 seed. Service fails fast at startup if absent. |
| `VERIFY_KEY_B64` | (derived) | Base64 Ed25519 public key — emitted to the dashboard for external verifiers. |
| `CHAIN_GENESIS_NOTE` | `immutable-audit-trail` | Free-form string baked into the genesis record for deployment identification. |
| `REPORT_DEFAULT_RANGE_DAYS` | `30` | Default span for compliance reports when no range is given. |

---

## How to Run

All operations run in Docker. There are no host-side dependencies beyond Docker
Desktop.

### Quick start

```bash
# 1. Generate a signing key into .env (one-time, on a fresh clone)
cp .env.example .env
python -c "import os, base64; print('SIGNING_KEY_B64=' + base64.b64encode(os.urandom(32)).decode())" >> .env

# 2. Bring up the stack
make demo
# -> http://localhost:8000

# 3. Seed some demo data
docker compose --profile test run --rm tester python scripts/seed_demo.py

# 4. Open the dashboard
open http://localhost:8000
```

### Make targets

| Target | What it does |
|--------|-------------|
| `make build` | Build the app + tester images. |
| `make up` | Start the `app` container in the background. |
| `make demo` | `build` + `up`; print dashboard URL. |
| `make down` | Stop and remove the stack. |
| `make logs` | Tail app logs. |
| `make test` | Run the full pytest suite in the tester container. |
| `make test-unit` | Run only unit tests. |
| `make test-int` | Run only integration tests. |
| `make e2e` | Run scripts/e2e.sh against the live `app` (requires `make up` first). |
| `make throughput` | Run the throughput harness; asserts >=100 RPS at <=100ms p50. |
| `make clean` | Tear down + remove locally-built images. |

### Hash Chain Verification Walkthrough

A full integrity check is just one HTTP call:

```bash
curl -s http://localhost:8000/v1/verify | jq
```

A clean chain returns:

```json
{
  "ok": true,
  "integrity_status": "VALID",
  "head_seq": 30,
  "total_records": 31,
  "verified_records": 31,
  "failed_records": 0,
  "first_break_seq": null,
  "signature_failures": [],
  "seq_gaps": []
}
```

To **demonstrate tamper detection**: open the SQLite file (mounted at
`./data/audit.db`), drop the immutability triggers temporarily, mutate a row,
then re-verify. The next `/v1/verify` reports the exact `first_break_seq` and
reason (`hash_mismatch` / `signature_invalid` / `seq_gap`). The decorator-side
counters and dashboard will also surface an `integrity_break` alert on the
next poll.

> Production code never drops the triggers — this is only for the
> tamper-evidence demo. In normal operation, the triggers are engine-enforced
> and any UPDATE or DELETE on `audit_records` returns
> `audit_records is append-only` from SQLite directly.

### Compliance reports

Four framework-specific report shapes are exposed at `/v1/reports/{framework}`:

| Framework | Endpoint | Filters |
|-----------|----------|---------|
| GDPR Art. 30 | `/v1/reports/gdpr` | optional actor / resource |
| HIPAA §164.312(b) | `/v1/reports/hipaa` | resource startswith `PATIENT_`, action in {read,search,export} |
| SOC 2 CC7.2 | `/v1/reports/soc2` | none; reports anomaly indicators |
| PCI DSS 10.2 | `/v1/reports/pci_dss` | resource startswith `CARDHOLDER_` |

Each report includes an Ed25519 attestation signature over the canonical
bundle bytes so downstream auditors can re-verify out-of-band.

### Observability

- `/api/stats` — JSON snapshot of process-local counters.
- `/metrics` — Prometheus exposition (auto-scrape ready).
- Dashboard at `/` — live cards refreshed via HTMX every 10s.

---

## What I Want to Learn

- **Append-only hash chains.** Why per-record `prev_hash` plus `self_hash` is enough for tamper evidence, and where you still need signatures (insider with DB write access vs. external attacker with read-only access).
- **Decorator interception without leaking the wrapped function's semantics.** Capturing `(actor, action, resource, args, result)` from `inspect.signature` and `functools.wraps` without breaking async/sync polymorphism or coroutine return types.
- **SQLite as an append-only store.** WAL mode + an `AFTER UPDATE`/`AFTER DELETE` trigger that raises, so the engine itself enforces immutability — defence-in-depth on top of the application contract.
- **Forensic verification under partial corruption.** Reporting *which* record broke the chain and *why* (hash mismatch vs. signature failure vs. missing seq) instead of a single boolean — and doing it without re-reading the whole table on every check.
- **Compliance framework mapping.** Translating one underlying record schema into four different report shapes (GDPR/HIPAA/SOC 2/PCI DSS) without forking the storage layer.
- **Ed25519 in Python via `cryptography`.** Key generation, seed-based reconstruction, deterministic signatures, and constant-time verification — the bits you'd otherwise hand to a KMS.

---

## Project Layout (planned)

```
immutable-audit-trail-sys/
├── README.md
├── requirements.txt
├── .gitignore
├── (Dockerfile, docker-compose.yml, Makefile — added later)
├── src/
│   ├── main.py                   # FastAPI app + lifespan handler
│   ├── settings.py               # pydantic-settings
│   ├── api/
│   │   ├── routes.py             # All HTTP endpoints
│   │   └── models.py             # pydantic request/response schemas
│   ├── chain/
│   │   ├── appender.py           # ChainAppender — prev_hash + sign + persist
│   │   ├── verifier.py           # Full + range verification, gap diagnosis
│   │   └── schema.py             # AuditRecord pydantic model + canonicalisation
│   ├── crypto/
│   │   ├── hasher.py             # SHA-256 canonical-bytes helper
│   │   └── signer.py             # Ed25519 sign/verify wrappers
│   ├── interceptor/
│   │   └── decorator.py          # @audit_access (sync + async)
│   ├── persistence/
│   │   ├── db.py                 # SQLAlchemy engine + session factory
│   │   └── models.py             # ORM models — append-only constraints
│   ├── reports/
│   │   ├── base.py               # Common bundle + signature
│   │   ├── gdpr.py
│   │   ├── hipaa.py
│   │   ├── soc2.py
│   │   └── pci_dss.py
│   └── stats/
│       └── counters.py           # Atomic counters for /api/stats + /metrics
├── templates/
│   ├── dashboard.html
│   └── _records_card.html        # HTMX partial
├── static/
│   ├── dashboard.css
│   └── htmx.min.js
└── tests/
    ├── unit/                     # chain, crypto, interceptor, reports
    └── integration/              # API end-to-end via httpx + asgi-lifespan
```

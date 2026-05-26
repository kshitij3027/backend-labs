# Multi-Framework Compliance Reporting Engine

Multi-framework synthetic-log compliance reporting engine — generates signed PDF / CSV / JSON / XML reports for SOX, HIPAA, PCI-DSS, GDPR, and a bonus FinHealth dual-signed framework.

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize how real audit-grade reporting pipelines stitch together log aggregation, per-framework evidence rules, multi-format export, cryptographic signing, and at-rest encryption — without needing a real upstream log source or a separate frontend toolchain.

## Tech stack

- **Language:** Python 3.11
- **API framework:** FastAPI 0.115 + uvicorn
- **Database:** PostgreSQL 16 (via SQLAlchemy 2.0 async + asyncpg)
- **Scheduler:** APScheduler 3.10 (`AsyncIOScheduler`, lifespan-managed)
- **Crypto:** `cryptography` — HMAC-SHA256 for signing, Fernet for at-rest file encryption
- **Aggregation:** pandas (CSV flattening)
- **PDF rendering:** ReportLab (Platypus pipeline)
- **Templating:** Jinja2 + vendored HTMX (polling partials)
- **Synthetic data:** Faker (deterministic seeder)
- **Containers:** Docker + Docker Compose
- **Testing:** pytest + pytest-asyncio + httpx (unit + integration)

## Architecture

```
                       ┌────────────────────────────────────────────────────┐
   Browser ◀──HTMX────▶│  FastAPI app  (uvicorn on :8000)                   │
                       │   ├─ Dashboard router  (GET /, /partials/*)        │
                       │   ├─ REST routers      (/reports, /frameworks,     │
                       │   │                     /dashboard/stats, /health) │
                       │   └─ APScheduler       (per-framework cron jobs)   │
                       └──────────────┬─────────────────────────────────────┘
                                      │  BackgroundTasks.add_task
                                      ▼
                       ┌────────────────────────────────────────────────────┐
                       │  ReportCoordinator  (Semaphore(MAX_CONCURRENT=5))  │
                       │                                                    │
                       │   PENDING → AGGREGATING → EXPORTING → SIGNING →    │
                       │                                       COMPLETED    │
                       │                                                    │
                       │   ┌──────────┐ ┌──────────┐ ┌────────┐ ┌─────────┐ │
                       │   │ Framework│ │Aggregator│ │Exporter│ │ HMAC +  │ │
                       │   │ rules    │ │          │ │PDF/CSV │ │ Fernet  │ │
                       │   │ (5 frwks)│ │          │ │JSON/XML│ │ at rest │ │
                       │   └────┬─────┘ └────┬─────┘ └───┬────┘ └────┬────┘ │
                       └────────┼────────────┼───────────┼───────────┼──────┘
                                │            │           │           │
                                ▼            ▼           ▼           ▼
                       ┌────────────────────────────────────────────────────┐
                       │  PostgreSQL 16  (log_events, reports, report_files)│
                       │  + on-disk storage  (STORAGE_PATH, Fernet-encrypted)│
                       └────────────────────────────────────────────────────┘
```

## Frameworks and evidence categories

| Framework | Scope | Evidence categories |
|---|---|---|
| **SOX** | Financial controls | `admin_access`, `financial_transactions`, `system_changes`, `approval_workflows`, `sod_violations` |
| **HIPAA** | Protected Health Information | `phi_access`, `auth_failures`, `phi_modifications`, `breach_events`, `user_audit` |
| **PCI-DSS** | Payment card data | `cardholder_access`, `payment_processing`, `key_rotation`, `failed_auth`, `config_changes` |
| **GDPR** | Personal data of EU residents | `personal_data_processing`, `consent_records`, `dsr_requests`, `breach_notifications`, `cross_border_transfers` |
| **FinHealth** (bonus) | Composite SOX + HIPAA | `financial_transactions`, `admin_access`, `phi_access`, `phi_modifications`, `composite_risk` |

FinHealth is a custom framework that draws its evidence from the SOX-financial-controls and HIPAA-PHI subsets and **dual-signs** the resulting report (HMAC-SHA256 with the primary signing key + a second HMAC-SHA256 with a distinct HIPAA-scope key).

## Quick start

```bash
# from compliance-reporting-engine/
make build
make up
curl http://localhost:8000/health
open http://localhost:8000/
```

## Demo

Populate the dashboard with a meaningful spread of data:

```bash
make seed-logs        # ~5000 synthetic log events tagged across frameworks
make seed-reports     # one report per framework x format (20 reports total)
open http://localhost:8000/
```

The dashboard polls every few seconds via HTMX, so the cards (stats, recent, breakdown, in-flight, FinHealth) update as the coordinator works through the queue.

## Endpoint reference

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/health` | Liveness probe — `{"status":"ok"}` |
| `GET`  | `/` | HTMX dashboard HTML |
| `GET`  | `/partials/stats` | Dashboard partial — totals, success rate, in-flight |
| `GET`  | `/partials/recent` | Dashboard partial — last 10 reports |
| `GET`  | `/partials/breakdown` | Dashboard partial — count per framework |
| `GET`  | `/partials/inflight` | Dashboard partial — reports in PENDING/AGGREGATING/EXPORTING/SIGNING |
| `GET`  | `/partials/finhealth` | Dashboard partial — last 5 FinHealth reports |
| `GET`  | `/frameworks` | Registered frameworks + their evidence categories |
| `GET`  | `/dashboard/stats` | JSON form of the dashboard aggregates |
| `POST` | `/reports/generate` | Kick off a report; returns 202 + `report_id` |
| `GET`  | `/reports/{id}` | Status row for one report |
| `GET`  | `/reports/{id}/download` | Decrypted artefact (`Content-Disposition: attachment`) |
| `GET`  | `/reports/{id}/verify` | Recompute the signature(s) and report `verified` |
| `GET`  | `/docs` | Auto-generated OpenAPI / Swagger |

Generate-report body:

```json
{
  "framework": "SOX",
  "period_start": "2026-04-26T00:00:00Z",
  "period_end":   "2026-05-26T00:00:00Z",
  "export_format": "JSON",
  "title": "May SOX review"
}
```

## Verifying a signature

After generating a report, hit `/verify` to recompute the HMAC-SHA256 digest over the same payload the coordinator signed and constant-time-compare it against the stored hex:

```bash
REPORT_ID=...   # from POST /reports/generate
curl -s http://localhost:8000/reports/$REPORT_ID/verify | jq
# {
#   "report_id": "...",
#   "verified": true,
#   "signature_hex": "ab12...",
#   "signature_secondary_hex": null,
#   "secondary_verified": null
# }
```

For a FinHealth report the response also carries `signature_secondary_hex` + `secondary_verified=true`. Mutating one byte of either signature in the database flips the corresponding field to `false`.

## FinHealth bonus

The FinHealth framework is a composite: it pulls in the SOX financial-controls evidence (financial transactions + admin access) **and** the HIPAA PHI evidence (PHI access + PHI modifications), and emits an extra `composite_risk` finding whenever the same actor appears on both sides — a heuristic for cross-scope insider risk.

Every FinHealth report is **dual-signed**: the coordinator signs the canonical payload once with the primary HMAC key and once with the secondary HMAC key (`HMAC_SIGNING_KEY_SECONDARY`). `/reports/{id}/verify` checks both and returns `secondary_verified` alongside the primary `verified` flag. This satisfies the "dual signature" stretch goal without introducing asymmetric key tooling.

## Testing

The full suite runs inside the `tester` profile — no host Python required.

```bash
make test          # full pytest suite (unit + integration)
make test-unit     # only tests/unit
make test-int      # only tests/integration (requires app container up)
make e2e           # bash probe: seed -> generate per framework -> verify -> download
make load          # 5-concurrent generations sanity test (asserts <120s + 100% success)
```

Notable tests:

- `tests/unit/test_hmac_signer.py` — positive verify, tamper detection, deterministic canonical JSON.
- `tests/unit/test_fernet_store.py` — at-rest encrypt round-trip + blank-key auto-generation.
- `tests/unit/test_framework_*.py` — each framework's classifier, summary, and findings.
- `tests/integration/test_reports_api.py` — full generate-poll-download-verify for SOX/JSON.
- `tests/integration/test_finhealth_dual_signature.py` — dual-signature happy path + mutation detection.
- `tests/integration/test_concurrent_generation.py` — 5 simultaneous generate requests all reach COMPLETED.

## Configuration

All settings are read from environment variables. Defaults live in `src/settings.py`.

| Env var | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://compliance:compliance@postgres:5432/compliance` | Async Postgres DSN |
| `API_HOST` | `0.0.0.0` | uvicorn bind host |
| `API_PORT` | `8000` | uvicorn bind port |
| `STORAGE_PATH` | `/app/exports` | Directory where encrypted reports land |
| `SUPPORTED_FRAMEWORKS` | `SOX,HIPAA,PCI_DSS,GDPR` | CSV of frameworks the seeder targets (FinHealth always registered) |
| `MAX_CONCURRENT_REPORTS` | `5` | Coordinator semaphore size |
| `REPORT_TIMEOUT_SECONDS` | `300` | Per-report wall-clock budget |
| `SCHEDULER_ENABLED` | `false` | If true, APScheduler installs per-framework cron jobs in the lifespan |
| `HMAC_SIGNING_KEY` | _(required, ≥32 bytes)_ | Primary HMAC-SHA256 signing key |
| `HMAC_SIGNING_KEY_SECONDARY` | _(optional, ≥32 bytes)_ | Secondary signing key for FinHealth dual-sign |
| `FERNET_ENCRYPTION_KEY` | _(empty → auto-generated with WARNING)_ | Fernet key for at-rest file encryption |
| `DEFAULT_EXPORT_FORMAT` | `PDF` | Default when not specified on the request |
| `LOG_LEVEL` | `INFO` | structlog level |
| `DASHBOARD_REFRESH_MS` | `5000` | HTMX poll interval (ms) |

## What I learned

- **HMAC-SHA256 is the right default for report integrity.** Stripe / Shopify / GitHub / AWS webhooks all use exactly this pattern (sign canonical JSON + constant-time compare) and it sidesteps the key-management burden of Ed25519 / RSA-PSS. The trade-off is that anyone with the key can both sign and verify — fine here because the verifier is the same service that produced the report. For multi-party scenarios asymmetric crypto wins; for "did this file survive intact?" HMAC wins on every axis.
- **Fernet at rest is one line.** A single `Fernet(key).encrypt(plaintext)` covers AES-CBC + HMAC + a version byte + a timestamp + a random IV. The hardest part is operational: an auto-generated key dies with the container, so the lifespan logs a WARNING when `FERNET_ENCRYPTION_KEY` is blank to make the "you must persist this" story visible.
- **APScheduler inside a FastAPI lifespan is the lightweight scheduler answer.** No Celery beat, no separate worker process — just `AsyncIOScheduler()`, install jobs on startup, `shutdown(wait=False)` on teardown. The `SCHEDULER_ENABLED` flag keeps it off in tests where deterministic timing matters.
- **HTMX-over-the-wire is the right call for small server-rendered dashboards.** Five polling partials + Jinja2 templates beat hauling in React + a build step for ~120 lines of presentation logic. One language end-to-end, one Docker stack, no Node toolchain.
- **`asyncio.Semaphore(N)` is enough concurrency control for an in-process coordinator.** No need to reach for arq / Celery / Redis-queue when the bound is small and the workload is in-process. The semaphore lives on `app.state` and the `BackgroundTasks` adapter drops new generate requests directly onto it.

## Layout

```
compliance-reporting-engine/
├── Dockerfile               # multi-stage builder + runtime
├── Dockerfile.test          # test runner image (with jq + curl)
├── docker-compose.yml       # postgres + app + tester (profile: test)
├── Makefile                 # build, up, down, logs, test, seed-*, e2e, load, clean
├── requirements.txt
├── pytest.ini               # asyncio_mode = auto
├── .env.example             # every settings key
├── README.md
├── src/
│   ├── main.py              # FastAPI app + lifespan + router/static mounts
│   ├── settings.py          # Pydantic BaseSettings
│   ├── logging_config.py    # structlog JSON renderer
│   ├── persistence/         # async engine, models (LogEvent, Report, ReportFile)
│   ├── logs/                # synthetic seeder + framework-scoped repository
│   ├── frameworks/          # base + SOX, HIPAA, PCI_DSS, GDPR, FinHealth
│   ├── signing/             # HMAC signer + Fernet at-rest store
│   ├── reporting/           # state machine, aggregator, coordinator, exporters/
│   ├── scheduling/          # APScheduler wrapper + cron jobs
│   ├── services/            # stats_service
│   └── api/                 # routers: reports, frameworks, stats, dashboard
├── templates/               # dashboard.html + 5 polling partials
├── static/                  # dashboard.css + vendored htmx.min.js
├── scripts/
│   ├── seed_logs.py         # ~5000 synthetic log events
│   ├── seed_reports.py      # POST one report per framework x format
│   ├── e2e.sh               # end-to-end probe (5 frameworks)
│   └── load_test.py         # 5-concurrent sanity load test
└── tests/
    ├── conftest.py          # sqlite-in-memory engine + session_factory
    ├── unit/                # framework rules, signer, fernet, exporters, routes, coordinator
    └── integration/         # reports API, dashboard, finhealth dual-sign, concurrent generation
```

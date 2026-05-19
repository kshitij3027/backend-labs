# Field-Level Log Encryption Service

A FastAPI middleware that detects PII in structured log entries and selectively encrypts only the sensitive fields using **AES-256-GCM**, leaving operational fields (timestamps, request IDs, amounts, log levels) human-readable for triage and observability.

The goal: let engineers keep their logs useful for triage without ever exposing user data in plaintext to log storage, downstream pipelines, or SIEM systems.

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize **envelope encryption**, **DEK rotation with retired-readable state**, and **AAD-bound nonces** as they appear in real log/observability pipelines.

## How it runs

| Service        | URL                              | What it is                                                     |
|----------------|----------------------------------|----------------------------------------------------------------|
| App (HTTP API) | http://localhost:8000            | FastAPI + uvicorn. Detect/encrypt/decrypt + dashboard + metrics. |
| Dashboard      | http://localhost:8000/           | Jinja2 + HTMX server-rendered page; auto-refreshes every 5 s.   |
| Prometheus     | http://localhost:8000/metrics    | Default HTTP metrics + custom `encryptions_total` / `decryptions_total` / `pii_detections_total`. |
| Redis cache    | localhost:6379                   | Per-`key_id` usage counters (encrypts/decrypts). Falls back to in-memory if unreachable. |
| Tester         | (no port)                        | One-shot container that runs the pytest suite (`docker compose run --rm tester`). |

The whole stack is **single-host and Docker-Compose orchestrated** — no database, no external KMS, no message queue. The KEK is loaded from `MASTER_KEY_B64` at startup, DEKs are generated in-process, the audit log lives in a 1000-entry ring buffer, and stats counters are atomic Python ints.

## Quick start

```bash
# One-time setup: generate a fresh 32-byte KEK and write it to .env.
cp .env.example .env
python3 -c 'import os,base64; print("MASTER_KEY_B64=" + base64.b64encode(os.urandom(32)).decode())' >> .env

# Bring up the stack (builds images, starts app + redis, prints the URL).
make demo

# Then open http://localhost:8000/ in a browser.
# When done:
make down
```

For a one-shot test inside Docker:

```bash
make build && make test       # full pytest suite (211 tests)
make e2e                      # curl-driven end-to-end against the live stack
make throughput               # 100-log batch, asserts >=50 logs/sec
```

The first build is ~2-3 minutes (pip install of cryptography + redis + httpx). Subsequent builds reuse the layer cache.

## Tech stack

- **Python 3.11**.
- **FastAPI 0.115** + **uvicorn 0.30** — ASGI app.
- **pydantic 2.9** + **pydantic-settings 2.5** — request/response models, env-var config.
- **cryptography 43** — AES-256-GCM primitives (the `AESGCM` class).
- **redis 5.0** — per-key-id usage counters, with graceful in-memory fallback when Redis is unreachable.
- **Jinja2 3.1** + **HTMX 1.9** — server-rendered dashboard, no JS build step.
- **prometheus-client 0.20** + **prometheus_fastapi_instrumentator 7** — `/metrics`.
- **pytest 8.3** + **httpx 0.27** + **asgi-lifespan 2.1** — unit + integration tests.
- **docker** + **docker-compose** — runtime.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Browser → http://localhost:8000/    (Jinja2 dashboard + HTMX poll)  │
│      │                                                                │
│  Client → POST /v1/logs/encrypt[/batch]                               │
│           POST /v1/logs/decrypt                                       │
│           POST /v1/detect      (dry-run)                              │
│           GET  /v1/keys        /api/health  /api/stats  /metrics      │
│      │                                                                │
│      ▼                                                                │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  FastAPI on :8000                                              │ │
│  │                                                                 │ │
│  │   LogProcessor (pipeline)                                       │ │
│  │     ┌─────────┐    ┌──────────────┐   ┌─────────────────────┐  │ │
│  │     │Detector │ →  │  Parallel    │ → │   AESGCMEncryptor   │  │ │
│  │     │ (regex  │    │  Encryptor   │   │  (12B nonce, AAD =  │  │ │
│  │     │ + name) │    │  (>=4 fields │   │   key|rec|path,     │  │ │
│  │     │ + Luhn) │    │   AND >=4KB) │   │   ct||tag, 16B tag) │  │ │
│  │     └─────────┘    └──────────────┘   └──────────┬──────────┘  │ │
│  │                                                  │              │ │
│  │                                              KeyStore           │ │
│  │                                       (active / retired /       │ │
│  │                                        destroyed lifecycle,     │ │
│  │                                        per-key usage counters)  │ │
│  │                                                  │              │ │
│  │                                            KeyProvider          │ │
│  │                                       (KEK wraps DEKs via       │ │
│  │                                        envelope encryption)     │ │
│  │                                                  │              │ │
│  │   AuditLogger (RingBuffer) ← every encrypt / decrypt /          │ │
│  │   StatsCounters (atomic)     rotate / destroy event             │ │
│  │   CacheProvider ──────────→  Redis (or in-memory fallback)      │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

**Crypto design highlights**

- Algorithm: **AES-256-GCM**, 12-byte random nonce per field (`os.urandom(12)`), 16-byte tag.
- AAD: `f"{key_id}|{record_id}|{field_path}".encode()` — prevents cross-record / cross-field ciphertext swaps.
- Envelope: a 32-byte KEK (from `MASTER_KEY_B64`) wraps each 32-byte DEK. DEKs are rotated every `KEY_ROTATION_DAYS` days; retired DEKs stay decryptable until explicitly destroyed.
- Crypto-shredding: `destroy_key(key_id)` zeros the DEK bytes and flips status to `destroyed` — irrecoverable.

## API surface

| Method | Endpoint                     | Purpose                                                       |
|--------|------------------------------|---------------------------------------------------------------|
| POST   | `/v1/logs/encrypt`           | Encrypt one log (PII fields replaced by `EncryptedField`).    |
| POST   | `/v1/logs/encrypt/batch`     | Encrypt many logs in one call (`{"logs": [...]}`).            |
| POST   | `/v1/logs/decrypt`           | Reverse — recovers the original log, strips `_processing`.    |
| POST   | `/v1/detect`                 | Dry-run — return detections without encryption.                |
| GET    | `/v1/keys`                   | List active + retired DEKs with `{encrypts, decrypts}` usage. |
| GET    | `/api/health`                | Liveness probe: `{"status":"healthy", "service":"..."}`.       |
| GET    | `/api/stats`                 | Atomic counter snapshot (`logs_processed`, `fields_encrypted`, ...). |
| GET    | `/api/stats/html`            | HTMX partial used by the dashboard's stats card.              |
| GET    | `/metrics`                   | Prometheus exposition (custom + instrumentator default).      |
| GET    | `/`                          | Live dashboard.                                               |
| POST   | `/api/dashboard/encrypt`     | Form-encoded encrypt for the dashboard textarea.              |
| POST   | `/api/dashboard/decrypt`     | Form-encoded decrypt for the dashboard textarea.              |

Error mapping (decrypt path):

| Exception                                     | Status | Meaning                                           |
|-----------------------------------------------|--------|---------------------------------------------------|
| `KeyNotFoundError` / `KeyDestroyedError`      | 404    | DEK was rotated-out and destroyed, or never existed. |
| `InvalidTag` (GCM auth fail)                  | 422    | Ciphertext was tampered or AAD doesn't match.      |
| `ProcessorError` / `pydantic.ValidationError` | 422    | Malformed `EncryptedField` record in the input.    |
| anything else                                 | 500    | Bumps `stats.errors`, emits a failure audit event. |

## Make targets

```
make build       # build app + tester images
make up          # start app + redis (detached)
make down        # stop the stack, remove volumes
make logs        # tail app logs
make test        # full pytest suite in Docker (211 tests, coverage report)
make test-unit   # unit tests only (excludes the `integration` marker)
make e2e         # bring stack up, run scripts/e2e.sh, tear down
make throughput  # 100-log batch encrypt — asserts >=50 logs/sec
make demo        # build + start; prints the dashboard URL
make clean       # down + remove locally-built images
```

`make e2e` and `make throughput` always run `make down` on their way out, even if the script inside fails — so a flaky run never leaves a stale stack holding port 8000.

## Configuration

All settings come from environment variables loaded by `pydantic-settings`. The `.env` file is the operator-side store; `.env.example` documents every variable with safe placeholder values.

| Variable                          | Default      | Purpose                                                          |
|-----------------------------------|--------------|------------------------------------------------------------------|
| `PORT`                            | `8000`       | HTTP port (uvicorn binds inside the container).                  |
| `LOG_LEVEL`                       | `INFO`       | Application log level (DEBUG/INFO/WARNING/ERROR/CRITICAL).       |
| `MASTER_KEY_B64`                  | (required)   | Base64 of a 32-byte KEK. Service fails fast at startup if absent. |
| `KEY_ROTATION_DAYS`               | `30`         | Days between automatic DEK rotations.                            |
| `BATCH_PARALLEL_THRESHOLD_FIELDS` | `4`          | Min sensitive fields/log for parallel encryption (otherwise serial). |
| `BATCH_PARALLEL_THRESHOLD_BYTES`  | `4096`       | Min total plaintext bytes for parallel encryption.                |
| `THREAD_POOL_SIZE`                | `4`          | Worker count for the parallel encryption pool.                   |
| `REDIS_HOST`                      | `redis`      | Cache host (compose service name, or external host).             |
| `REDIS_PORT`                      | `6379`       | Cache port.                                                      |

Generate a fresh KEK:

```bash
python3 -c 'import os,base64; print(base64.b64encode(os.urandom(32)).decode())'
```

## Demo flow

After `make demo`, open http://localhost:8000/:

1. The dashboard renders three cards: **Live stats**, **Encrypt log**, **Decrypt log**.
2. **Live stats** is HTMX-polled every 5 s (`hx-get="/api/stats/html"`); the counters tick visibly as you fire requests.
3. **Encrypt log** is pre-filled with a sample e-commerce log (`customer_email`, `phone`, `order_id`, `amount`, `timestamp`). Click **Encrypt** — the result `<pre>` populates with the transformed log: `customer_email` and `phone` become `EncryptedField` JSON dicts (`encrypted_value`, `iv`, `algorithm`, `key_id`, ...), while `order_id`, `amount`, and `timestamp` stay plaintext.
4. Copy the encrypt output into the **Decrypt log** textarea, click **Decrypt** — the original log is recovered (the `_processing` envelope is stripped).
5. The dashboard footer shows the active `key_id`. Rotate via the API (or wait for `KEY_ROTATION_DAYS`) and refresh — a new active key appears, with the previous one moving to `retired` (still readable).
6. `make e2e` exercises every endpoint from the command line and reports PASS/FAIL.
7. `make throughput` hammers the batch endpoint with 100 logs and asserts the service sustains ≥50 logs/sec.

## Test suite

- **211 tests** total, all run inside Docker (`make test`).
  - **Unit tests** (~171): detection (40 — pattern regex + field-name + Luhn + nested-dict walking), crypto (28 — AES-GCM round-trip, nonce uniqueness, tamper detection, AAD binding, KEK wrap/unwrap), keystore (22 — lifecycle, rotation, crypto-shredding), log processor (34 — fixture round-trips, parallel-vs-serial threshold, envelope shape), audit (21 — append-only, ring-buffer overflow, no plaintext in logs), stats (8 — thread-safe increments), cache (17 — provider ABC, in-memory get/set/incr/ttl), smoke (1).
  - **Integration tests** (~40): HTTP API (30 — all endpoints via `asgi-lifespan` + `httpx`), Redis cache (11 — live Redis backend, gated on `REDIS_HOST`).
- Coverage: ~94% on `src/*`.
- Every test runs in the `tester` profile of `docker-compose.yml` so test results match production layout exactly.

## What I learned

- **AES-256-GCM nonce hygiene is the whole game.** Random 96-bit nonces are safe for ~2^32 messages per key before the birthday-bound matters; bind `key_id|record_id|field_path` into the AAD and ciphertext-swap attacks become impossible without re-encrypting under the right tuple.
- **Envelope encryption is genuinely simple.** A 32-byte KEK wraps a 32-byte DEK with one `AESGCM.encrypt` call. The DEK is the only thing the data plane needs; the KEK never leaves the trusted boundary. Rotating DEKs daily becomes cheap because the KEK is untouched.
- **Retired-but-readable + crypto-shredding is the real-world key lifecycle.** Old ciphertext must keep decrypting under retired DEKs until explicitly destroyed; `destroy_key()` zeros the DEK bytes and flips status to `destroyed` — irrecoverable, and that's the point.
- **Field-name-first PII detection beats value regex.** A field literally named `password` should fire even if its value is `"hello"`; field-name hits use `confidence=0.95` and short-circuit value scanning. This is what most SIEM products get backwards.
- **HTMX + Jinja2 partials give a live dashboard with zero JS build step.** Three HTML files (one page, one partial, one CSS) + the vendored 48 KB `htmx.min.js` and the stats card auto-refreshes via `hx-get="/api/stats/html" hx-trigger="every 5s"`. No Vite, no npm, no SPA hydration.
- **`httpx.AsyncClient` + `asgi-lifespan.LifespanManager` is the right way to test FastAPI apps on httpx 0.27.** The lifespan handler builds all the singletons; without `LifespanManager` the test app's `app.state.processor` is never set and every route 500s.
- **Coarse-grained locks on the keystore are fine.** Every op is nanosecond-scale and the lock is never held while waiting on I/O. The throughput test still hits >50 logs/sec with a single `threading.Lock` around the entire `KeyStore._records` dict.
- **Pydantic v2's `extra="forbid"` keeps plaintext out of the audit log.** `AuditEvent` has slots for `key_id`, `field_path`, `byte_count` — but no slot for the plaintext or the ciphertext. A future regression that tries to add `event.plaintext = ...` fails validation rather than silently leaking.
- **`prometheus_fastapi_instrumentator` gives you the default HTTP histograms for free.** You still get to define custom counters (`encryptions_total{result, key_id}`) on the side; the instrumentator just wires the route latencies + status-code labels.

## Project layout

```
field-level-log-encryption-service/
├── README.md
├── project_requirements.md          (spec — frozen)
├── requirements.txt
├── Dockerfile / Dockerfile.test
├── docker-compose.yml / Makefile
├── pytest.ini / pyproject.toml / .env.example
├── config/
│   ├── patterns.yaml                (PII regex: email, phone, ssn, jwt, ip*, cc+Luhn)
│   └── field_names.yaml             (sensitive field-name substrings)
├── src/
│   ├── main.py                      (FastAPI app + lifespan handler)
│   ├── settings.py                  (pydantic-settings)
│   ├── api/
│   │   ├── routes.py                (every HTTP endpoint)
│   │   ├── models.py                (pydantic request/response schemas)
│   │   └── metrics.py               (custom Prometheus counters/histograms)
│   ├── audit/
│   │   ├── audit_logger.py          (sealed-schema AuditEvent emitter)
│   │   └── ring_buffer.py           (bounded thread-safe deque)
│   ├── cache/
│   │   ├── provider.py              (CacheProvider ABC)
│   │   ├── in_memory.py             (dict + TTL fallback)
│   │   ├── redis_cache.py           (redis-py client)
│   │   └── factory.py               (Redis-with-fallback selector)
│   ├── crypto/
│   │   ├── aesgcm.py                (AESGCMEncryptor)
│   │   ├── key_provider.py          (KeyProvider ABC + EnvKeyProvider)
│   │   └── schema.py                (EncryptedField pydantic model)
│   ├── detection/
│   │   ├── patterns.py              (regex + Luhn)
│   │   ├── field_names.py           (case-insensitive substring matcher)
│   │   └── detector.py              (recursive walker)
│   ├── keystore/
│   │   ├── store.py                 (KeyStore lifecycle)
│   │   └── rotator.py               (RotationManager)
│   ├── processor/
│   │   ├── log_processor.py         (detect → encrypt → assemble pipeline)
│   │   └── parallel.py              (ThreadPool wrapper + threshold)
│   └── stats/
│       └── counters.py              (atomic counters)
├── templates/
│   ├── dashboard.html               (Jinja2: page shell + 3 cards)
│   └── _stats_card.html             (HTMX partial)
├── static/
│   ├── dashboard.css                (minimal grid layout, monospace <pre>)
│   └── htmx.min.js                  (HTMX 1.9, vendored)
├── tests/
│   ├── conftest.py                  (autouse MASTER_KEY_B64 fixture)
│   ├── unit/                        (171 tests across 8 files)
│   ├── integration/                 (40 tests — API + Redis)
│   └── fixtures/
│       ├── ecommerce_log.json
│       └── support_ticket_log.json
└── scripts/
    ├── e2e.sh                       (curl matrix vs the live stack; PASS/FAIL summary)
    └── throughput.py                (100-log batch; asserts >=50 logs/sec)
```

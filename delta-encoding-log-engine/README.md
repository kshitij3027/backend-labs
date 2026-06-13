# Delta Encoding Log Engine

A compression engine for structured logs that stores only the **differences (deltas)
between consecutive entries** instead of full entries. Because adjacent log lines in a
stream overwhelmingly share the same fields (`service`, `host`, `level`, `trace_id`, …)
and only a few values change per line, encoding just the changes targets a **60–80%
storage reduction** for structured logs — before any general-purpose compression is even
applied. It runs as a long-lived **FastAPI + Uvicorn** service that exposes a **web
dashboard on port `8080`** plus REST endpoints for **log generation, compression,
reconstruction, and stats**.

> **Status — scaffold stage.** This commit contains only the project scaffold:
> `README.md`, `requirements.txt`, and `.gitignore`. No application code, tests, or
> Docker files exist yet. The sections below describe the **intended design and API**;
> commands and endpoints marked _(planned)_ become real as implementation lands. Nothing
> here has been built or benchmarked yet — the 60–80% figure is the **design target**,
> not a measured result.

**Tech stack:** Python 3.12, FastAPI + Uvicorn, Pydantic / pydantic-settings, Jinja2 +
vanilla JS dashboard, Docker Compose, pytest + httpx.

---

## The Problem

A structured log stream is extraordinarily repetitive. Consecutive lines from the same
service look almost identical — same `service`, same `host`, same `level`, often the same
`trace_id` — with only a timestamp, a latency, or a status code moving between them:

```json
{"ts":"2026-06-15T10:00:00.000Z","level":"INFO","service":"auth-api","host":"node-7","trace_id":"a1b2c3","status":200,"latency_ms":12,"msg":"request completed"}
{"ts":"2026-06-15T10:00:00.140Z","level":"INFO","service":"auth-api","host":"node-7","trace_id":"a1b2c3","status":200,"latency_ms":9, "msg":"request completed"}
```

Storing the second line in full re-pays for seven fields that did not change just to
record that two did. Across millions of lines, that redundancy *is* the storage bill.
General-purpose compressors (gzip/zstd) exploit some of it, but they work on opaque byte
windows and discard the one thing we know for free: these are **records with fields**,
and **the diff between neighbours is tiny**. Delta encoding captures that structure
directly — and still composes with byte compression on top.

---

## How Delta Encoding Works

The model is borrowed from video codecs: an occasional full **keyframe** (an I-frame),
followed by a run of small **deltas** (P-frames) that describe only what changed.

### Keyframes and deltas

- **Keyframe** — a full log entry, stored verbatim. One is emitted at the start of every
  *segment* (every `KEYFRAME_INTERVAL` entries). Keyframes are the resync points that
  make reconstruction bounded and random access possible.
- **Delta** — for every non-keyframe entry, store only the field-level diff against the
  previous entry:
  - `~` — fields that were **changed or added** (key → new value)
  - `-` — keys that were **removed**
  - unchanged fields are **omitted entirely** — that omission is the whole saving.

Worked example, encoding the two lines above plus an error line:

```text
entry 0  (keyframe — stored in full)
  {"ts":"…00.000Z","level":"INFO","service":"auth-api","host":"node-7",
   "trace_id":"a1b2c3","status":200,"latency_ms":12,"msg":"request completed"}

entry 1  (delta vs entry 0 — only ts and latency moved; status 200→200 is omitted)
  {"~":{"ts":"…00.140Z","latency_ms":9}}

entry 2  (delta vs entry 1 — level/status/latency/msg change, "error" is added)
  {"~":{"ts":"…00.205Z","level":"ERROR","status":500,"latency_ms":230,
        "msg":"upstream timeout","error":"ETIMEDOUT"}}
```

Entry 1 shrinks from eight fields to two. That ratio — a handful of changed fields out of
a wide record — is exactly what drives the 60–80% target on real structured logs.

### Reconstruction

Reconstruction replays a segment from its keyframe: start with the keyframe object, then
apply each delta in order — set every key in `~`, delete every key in `-` — until the
target index is reached. Because every segment begins with a self-contained keyframe,
**reconstructing entry _i_ never costs more than `KEYFRAME_INTERVAL` delta applications**,
regardless of how many millions of entries precede it. That bound is the entire reason
keyframes exist: without them, random access to the last line would mean replaying the
whole history from line zero.

### The keyframe-interval trade-off

`KEYFRAME_INTERVAL` is the dial between storage and access cost:

- **Larger interval** → fewer full keyframes → **better compression**, but more deltas to
  replay per random read.
- **Smaller interval** → more keyframes → **faster random access and more resync points**,
  but weaker compression.

Deltas are diffed against the **previous entry** by default (smallest diffs); diffing
against the **segment keyframe** instead trades a little ratio for single-hop
reconstruction. Both modes are selectable via configuration.

---

## Architecture (planned)

A single long-lived FastAPI process owns the encoder, an in-memory segment store, and the
dashboard. Everything is one service so a generate → compress → reconstruct round trip
stays in-process and easy to reason about.

```text
                         ┌──────────────────────────────────────────────┐
   browser  ──GET / ───► │            FastAPI service  :8080            │
   (dashboard,           │                                              │
    fetch/JSON)          │   /api/generate ─► Generator (synthetic     │
            ◄── JSON ──── │                    structured log entries)   │
                         │   /api/compress ─► DeltaEncoder ─► Segment   │
   curl / API ──────────►│                    store (keyframe + deltas) │
            ◄── JSON ──── │   /api/reconstruct ◄─ DeltaDecoder (replay) │
                         │   /api/stats ─► ratio, bytes saved, counts   │
                         └──────────────────────────────────────────────┘
```

- **Generator** — produces synthetic structured log entries with a configurable schema
  and field-churn rate, so the compression behaviour can be exercised without external
  data.
- **DeltaEncoder / DeltaDecoder** — the core: diff consecutive entries into keyframe +
  delta segments, and replay them back to the originals (a reconstruction must be
  byte-for-byte equal to the input — that round-trip equality is the correctness contract).
- **Segment store** — holds keyframes and deltas in memory for the current session
  (durable storage is a later milestone).
- **Dashboard** — a single page that drives the endpoints and visualises the live
  compression ratio.

---

## API Reference (planned)

Base URL `http://localhost:8080`.

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/` | Web dashboard (single page) |
| `GET`  | `/health` | Liveness probe |
| `POST` | `/api/generate` | Generate _N_ synthetic structured log entries (configurable schema + churn) |
| `POST` | `/api/compress` | Delta-encode a batch of entries → keyframe + delta segments; returns the encoded form and the achieved ratio |
| `POST` | `/api/reconstruct` | Rebuild original entries from encoded segments — all, a range, or a single index |
| `GET`  | `/api/logs` | Page through reconstructed entries from the current store |
| `GET`  | `/api/logs/{index}` | Random-access reconstruct one entry (replays from the nearest keyframe) |
| `GET`  | `/api/stats` | Compression stats: raw vs encoded bytes, reduction %, entry/keyframe/delta counts, timings |
| `POST` | `/api/reset` | Clear the in-memory segment store |

Exact request/response schemas are defined as the endpoints are implemented.

---

## Web Dashboard

Served at `http://localhost:8080/`, the dashboard is the human-facing view of the engine:
generate a batch, compress it, watch the **storage-reduction ratio** update live, inspect
a keyframe alongside the compact deltas that follow it, and reconstruct any entry to
confirm it matches the original. It is the quickest way to *see* why structured logs
collapse so well under delta encoding.

---

## How to Run

> _(planned — the Docker Compose / Makefile workflow is added during implementation.)_

Docker-first, exposing the dashboard and API on port `8080`:

```bash
cd delta-encoding-log-engine

docker compose up --build        # dashboard + API on http://localhost:8080

# generate, compress, and inspect via the API
curl -X POST http://localhost:8080/api/generate  -H 'Content-Type: application/json' \
     -d '{"count": 1000}'
curl -X POST http://localhost:8080/api/compress  -H 'Content-Type: application/json' \
     -d '{"use_generated": true}'
curl http://localhost:8080/api/stats             # → reduction %, bytes saved, counts
```

A local (non-Docker) path will also be documented:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080   # exact module path TBD in implementation
```

---

## Configuration (planned)

All settings are environment variables (parsed by pydantic-settings; defaults shown):

| Variable | Default | Purpose |
|----------|---------|---------|
| `API_HOST` / `API_PORT` | `0.0.0.0` / `8080` | Service bind address and port |
| `KEYFRAME_INTERVAL` | `100` | Emit a full keyframe every _N_ entries (storage ↔ random-access dial) |
| `DELTA_BASELINE` | `previous` | Diff each entry against the `previous` entry or the segment `keyframe` |
| `GZIP_DELTAS` | `false` | Also gzip the delta stream (delta encoding composes with byte compression) |
| `GENERATOR_FIELD_CHURN` | `0.2` | Fraction of fields that change between generated entries |
| `LOG_LEVEL` | `INFO` | Stdlib logging level |

---

## Project Status / Roadmap

- [x] **Scaffold** — README, `requirements.txt`, `.gitignore` _(this commit)_
- [ ] Core `DeltaEncoder` / `DeltaDecoder` with round-trip equality tests
- [ ] Synthetic structured-log generator
- [ ] FastAPI app + REST endpoints (`/api/generate|compress|reconstruct|stats`)
- [ ] Web dashboard on port 8080
- [ ] Dockerfile + `docker-compose.yml`
- [ ] Unit + end-to-end tests in Docker; validate the 60–80% reduction target on
      generated structured logs

---

## What I Learned

<!-- Filled in as the engine is built — e.g. measured reduction vs the 60–80% target,
     the real cost of the keyframe-interval trade-off, and how delta encoding stacks
     with gzip on top. -->

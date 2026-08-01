# API Rate Limit & Quota System

An **enforcement layer that sits in front of an HTTP API**, applying **per-user token-bucket rate limits** (short-term burst control) and **cumulative usage quotas** (long-term consumption caps) by user tier. All counter state lives in **Redis**, so the limits hold across **≥2 API replicas** rather than being enforced once per process. A live dashboard at `/dashboard/` reads the same counters the middleware writes.

> Scaffold stage — this README describes the intended design. Sections marked *TBD* fill in as the implementation lands.

---

## What It Does

Rate limiting and quota enforcement are two different questions that get conflated constantly, and answering only one of them leaves a real hole:

| Concern | Question it answers | Mechanism | Reset behaviour |
|---|---|---|---|
| **Rate limit** | "Are you calling me *too fast right now*?" | Token bucket per principal, sized by tier (`rate` refill/sec + `burst` capacity) | Continuous refill — recovers in seconds |
| **Quota** | "Have you used up *your allowance* for the period?" | Monotonic counter per principal per window (e.g. daily / monthly) | Fixed window — resets at a period boundary |

A caller can be inside its rate limit and out of quota (`429` vs `402`/`429` with a different reason), or the reverse. Both gates run on every request, both report their state in response headers, and neither is allowed to be enforced per-process.

The third concern is **distribution**. A token bucket held in Python memory is not a rate limit when there are two replicas behind a load balancer — it is two rate limits, and the caller gets double. Bucket refill and quota decrement therefore happen **in Redis, in a Lua script**, so the read-modify-write is atomic and every replica sees one truth.

---

## Planned HTTP Surface

| Route | Purpose |
|---|---|
| `GET /health` | Liveness + Redis reachability; unauthenticated, never rate limited |
| `ANY /api/...` | The protected API surface — every request passes through the limiter middleware |
| `GET /dashboard/` | Static HTML page: live per-tier usage, quota burn-down, `429` rate |
| `GET /dashboard/api/stats` | JSON feed backing the dashboard (aggregate counters, top consumers, recent rejections) |

**Authentication** accepts either an **API key** (`Authorization: ApiKey <key>` / `X-API-Key`) or a **JWT** (`Authorization: Bearer <token>`). Both resolve to the same thing the limiter needs: a *principal id* and a *tier*.

**Response headers** on success *and* on rejection:

```
X-RateLimit-Limit      burst capacity for the caller's tier
X-RateLimit-Remaining  tokens left in the bucket
X-RateLimit-Reset      seconds until the bucket is full again
X-Quota-Limit          period allowance for the tier
X-Quota-Remaining      requests left in the current period
X-Quota-Reset          unix seconds at which the period rolls over
Retry-After            on 429 only
```

Headers on the *rejection* path are the point, not a nicety — a `429` that doesn't say when to come back is barely better than a dropped connection.

---

## Tech Stack

- **Language:** Python 3.11
- **Framework:** FastAPI + uvicorn (long-lived ASGI process; enforcement as ASGI/HTTP middleware)
- **Shared state:** Redis (async `redis-py`, atomic bucket + quota updates via Lua)
- **Auth:** PyJWT (JWT) + bcrypt (API-key hashing)
- **Dashboard:** static HTML/JS served by the same process — no build step
- **Deployment:** Docker Compose (or k3d) with **≥2 API replicas** behind a load balancer, to prove enforcement is distributed rather than per-process

---

## How to Run

*TBD — filled in once the compose stack exists.* Intended shape:

```bash
make up      # Redis + N API replicas + load balancer
make test    # unit + integration, in Docker
make e2e     # black-box verifier against the live stack
make load    # perf/load gates (including a two-replica double-spend check)
```

---

## Configuration

*TBD — the table below is the intended surface; every key host-overridable via `.env` / compose.*

| Setting | Intended default | Meaning |
|---|---|---|
| `API_PORT` | *TBD* | Host port for the load balancer |
| `API_REPLICAS` | `2` | Number of API replicas — the whole point of the shared store |
| `REDIS_URL` | `redis://redis:6379/0` | Shared state connection |
| `JWT_SECRET` | *(none)* | HS256 signing key — required, no usable default |
| `TIER_LIMITS` | `free:5:10, pro:50:100, enterprise:500:1000` | `tier:rate_per_sec:burst` |
| `TIER_QUOTAS` | `free:1000, pro:50000, enterprise:1000000` | Requests per quota period, per tier |
| `QUOTA_WINDOW` | `daily` | Quota period (`hourly` \| `daily` \| `monthly`) |
| `FAIL_MODE` | *TBD* | Behaviour when Redis is unreachable — fail-open (serve, unmetered) vs fail-closed (reject). A deliberate decision, documented rather than defaulted by accident |

---

## Testing & Measured Performance

*TBD.* The verification that matters most here is the **distributed** one: fire a burst at the load balancer with replicas round-robining, and assert the *total* allowed count matches a single tier's burst — not `N × burst`. A single-replica test can't catch the bug this project is about.

---

## What I Learned

<!-- Fill in as the project evolves -->

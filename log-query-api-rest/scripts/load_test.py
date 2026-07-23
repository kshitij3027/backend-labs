"""Performance + load gates for the Log Query API (REST) (C12).

Runs **inside Docker** (the profile-gated ``loadtest`` compose service) against the LIVE ``api``
service over the compose network. Where :mod:`scripts.verify_e2e` answers "are the answers
CORRECT?", this script answers "does the service stay fast, lean and honest under concurrency?"
— four fail-fast phases, the first breached gate exiting non-zero so ``make load`` propagates
it.

**Everything is driven as ``admin``, whose tier is ``enterprise`` (refill 1000/s, burst 2000).**
That is deliberate: the free tier's bucket empties after 20 requests, so a harness running as
``viewer`` would measure the rate limiter's ceiling and call it the server's throughput. The
limiter is verified by ``verify_e2e.py`` check 14, where provoking it is the point; here it is
noise, and the enterprise ceiling is what keeps it out of the measurement.

The four phases:

**A — concurrent paginated reads.** ``LOAD_REQUESTS`` ``GET /api/v1/logs`` calls fired through
an ``httpx.AsyncClient`` bounded by an ``asyncio.Semaphore(LOAD_CONCURRENCY)``, each recording
its own wall latency and status. Gates: throughput ``>= LOAD_MIN_RPS``, error rate
``<= LOAD_MAX_ERROR_RATE``, p95 ``<= LOAD_MAX_P95_MS``.

**B — mixed reads.** ``LOAD_REQUESTS // 2`` calls over a fixed mix — 45% ``GET /logs``, 25%
``POST /logs/search`` (a compound ``all``/``in``/``not`` tree), 20% ``GET /logs/{id}``, 10%
``GET /stats`` — at the same concurrency and the same three gates, with per-endpoint p50/p95
reported so a regression can be attributed rather than merely noticed.

.. rubric:: Why the latency gate is ``LOAD_MAX_P95_MS`` and not the E2E's ``MAX_P95_MS``

They measure different quantities. ``verify_e2e.py`` check 15 times **sequential** reads — one
request in flight — so its 250 ms ceiling bounds service time. This harness runs at
``LOAD_CONCURRENCY`` (50) against a **single** uvicorn process (the ``Dockerfile`` starts one
worker), where Little's Law fixes mean latency at ``concurrency / throughput`` no matter how
efficient the handler is: at the measured ~370 req/s that is ~135 ms of pure queueing, with the
p95 tail landing ~2.8x higher. Measured phase-A p95 across three clean runs was 380.4 / 386.0 /
370.2 ms at 372.5 / 361.5 / 387.5 req/s with **zero errors in 6000 requests** — a server
comfortably over its own 200 rps bar, which the sequential ceiling would nonetheless have
failed. The default 800 ms is ~2.1x the worst of those three, leaving room for host and
container-scheduling noise while still catching a real tail regression; a throughput collapse
big enough to drag the tail past it would be caught by ``LOAD_MIN_RPS`` first, which is the
division of labour the two gates are supposed to have.

**C — SSE fan-out with a slow consumer.** ``LOAD_SSE_CLIENTS`` streams spread round-robin
across the demo principals, held for ``LOAD_SSE_SECONDS`` while a writer appends, each
well-behaved client needing ``>= LOAD_MIN_SSE_EVENTS`` frames. Alongside them runs one
deliberate **slow consumer** that never reads its socket: the contract is that a client which
cannot keep up is *dropped*, never buffered at the server's expense. Kernel socket buffers
absorb a fair amount before the server's own per-subscriber queue can overflow, so this phase
asserts **bounded memory and eventual release** — server RSS inside its ceiling throughout, the
subscriber count never above what was actually opened, and back to baseline after teardown —
rather than an instant drop. Whether a ``dropped`` frame was actually emitted is *reported*,
not gated, for exactly that reason.

**D — memory.** ``GET /api/v1/debug/memory`` ``memory_mb`` ``<= MAX_BACKEND_MEM_MB``. This is
the **server's** reported RSS and never this client's: a load generator can measure its own
memory or its container's, and neither is the number that matters.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``                 API base URL (default ``http://api:8000``)
* ``LOAD_READY_TIMEOUT``         seconds to wait for ``/health`` (default 90)
* ``LOAD_REQUESTS``              phase-A request count; phase B fires half (default 2000)
* ``LOAD_CONCURRENCY``           max in-flight requests (default 50)
* ``LOAD_MIN_RPS``               throughput gate, requests/second (default 200)
* ``LOAD_MAX_ERROR_RATE``        tolerated non-2xx fraction (default 0.0 — zero)
* ``LOAD_MAX_P95_MS``            **under-load** p95 latency gate, ms (default 800)
* ``LOAD_SSE_CLIENTS``           well-behaved concurrent streams (default 8)
* ``LOAD_SSE_SECONDS``           how long phase C holds them (default 10)
* ``LOAD_MIN_SSE_EVENTS``        frames each well-behaved client must receive (default 5)
* ``MAX_BACKEND_MEM_MB``         server RSS gate, MB (default 400)
* ``MAX_STREAMS_PER_PRINCIPAL``  the server's SSE cap, mirrored here (default 3)

``MAX_P95_MS`` is **not** read here — that is the E2E's sequential ceiling, and this harness
deliberately does not share it (see the rubric above).

Every gate is host-overridable, which is how we prove they are real rather than decorative:
``LOAD_MIN_RPS=1000000 make load`` **MUST** exit non-zero, and so must
``MAX_BACKEND_MEM_MB=1 make load`` and ``LOAD_MAX_P95_MS=1 make load``. The run ends with
machine-readable ``RESULT key=value``
lines (one per measured number, so C14 transcribes real figures into the README rather than
plausible ones) followed by ``LOAD PASSED``, or ``FAIL: <reason>`` + ``LOAD FAILED (<gate>)``
on stderr with a non-zero exit.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time
import uuid
from typing import Any

import httpx
from httpx_sse import aconnect_sse

from src.auth import DEV_PASSWORDS

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
LOAD_REQUESTS = max(1, int(os.environ.get("LOAD_REQUESTS", "2000")))
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "50")))
MIN_RPS = float(os.environ.get("LOAD_MIN_RPS", "200"))
MAX_ERROR_RATE = float(os.environ.get("LOAD_MAX_ERROR_RATE", "0.0"))
SSE_CLIENTS = max(1, int(os.environ.get("LOAD_SSE_CLIENTS", "8")))
SSE_SECONDS = float(os.environ.get("LOAD_SSE_SECONDS", "10"))
MIN_SSE_EVENTS = int(os.environ.get("LOAD_MIN_SSE_EVENTS", "5"))
# `LOAD_MAX_P95_MS`, NOT the E2E's `MAX_P95_MS`. See the module docstring: a p95 measured at
# concurrency 50 in front of one uvicorn process is a different quantity from a p95 measured
# one-request-at-a-time, and the two cannot share a ceiling without failing a healthy server.
LOAD_MAX_P95_MS = float(os.environ.get("LOAD_MAX_P95_MS", "800"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "400"))
MAX_STREAMS = max(1, int(os.environ.get("MAX_STREAMS_PER_PRINCIPAL", "3")))

API = "/api/v1"
_REQUEST_TIMEOUT = 60.0

#: Phase B fires half of phase A's volume. Phase A is sized to the enterprise burst (2000);
#: running the mix at full volume as well would put the *combined* run past what the bucket
#: refills between phases, and a 429 in a throughput measurement is a measurement of the
#: limiter, not of the API.
MIXED_REQUESTS = max(1, LOAD_REQUESTS // 2)

#: The fixed phase-B mix, as (label, weight). Weights are counts out of 100.
MIXED_MIX: tuple[tuple[str, int], ...] = (
    ("GET /logs", 45),
    ("POST /logs/search", 25),
    ("GET /logs/{id}", 20),
    ("GET /stats", 10),
)

#: Principals that may hold an SSE stream. ``GET /logs/stream`` is **analyst**-gated, so the
#: `viewer` account cannot open one at all — it would take a 403, not a stream. Spreading over
#: the three eligible principals is what keeps each one under `MAX_STREAMS_PER_PRINCIPAL`,
#: since the cap is per-principal and a single account could not hold the whole fan-out.
STREAM_PRINCIPALS: tuple[str, ...] = ("analyst", "writer", "admin")

#: Service tag on every entry phase C appends, so the fan-out streams see a tail that is
#: entirely ours and the well-behaved event counts mean something.
SSE_LOAD_SERVICE = "load-sse-probe"

#: Appends/second during phase C. The `writer` account is on `pro` (refill 100/s, burst 200),
#: so anything above ~100/s would start collecting 429s and the phase would be measuring the
#: limiter instead of the fan-out. Note the consequence for the slow consumer: at this rate a
#: 10-second window produces well under SSE_QUEUE_SIZE (1000) frames, which is precisely why
#: the drop is reported rather than gated.
SSE_APPEND_RPS = 90.0

#: A compound filter tree for phase B's search calls — `not` has no query-param spelling, so
#: this exercises the path `GET /logs` genuinely cannot reach.
SEARCH_TREE: dict[str, Any] = {
    "all": [
        {"field": "level", "op": "in", "value": ["ERROR", "FATAL"]},
        {"not": {"field": "service", "op": "eq", "value": "search-svc"}},
    ]
}


class CheckError(AssertionError):
    """Raised to fail a load gate with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live rather than at exit)."""
    print(f"[load] {msg}", flush=True)


def result(key: str, value: object) -> None:
    """Print one machine-readable summary line (scraped for the README's measured numbers)."""
    print(f"RESULT {key}={value}", flush=True)


def percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); 0.0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


# --------------------------------------------------------------------------- #
# Setup: readiness, credentials, warm-up
# --------------------------------------------------------------------------- #
def wait_ready(client: httpx.Client, timeout: float = READY_TIMEOUT) -> None:
    """Poll ``GET /health`` until it answers 200, within the timeout."""
    info(f"waiting for {BASE_URL}/health (up to {timeout:.0f}s)...")
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = client.get("/health", timeout=5.0)
            if resp.status_code == 200:
                info("api is ready")
                return
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        else:
            last = f"HTTP {resp.status_code}"
        time.sleep(2.0)
    raise CheckError(f"/health not ready after {timeout:.0f}s (last: {last})")


def login_all(client: httpx.Client) -> dict[str, str]:
    """Exchange every demo credential for a bearer token.

    Credentials come from :data:`src.auth.DEV_PASSWORDS` — the same declaration the server
    authenticates against — rather than a second hard-coded copy that a rename would break.
    These POSTs are **never** timed: bcrypt at 12 rounds costs ~250 ms on purpose, and it is the
    one deliberately-slow, unmetered route in the API.
    """
    tokens: dict[str, str] = {}
    for username, password in DEV_PASSWORDS.items():
        resp = client.post(
            f"{API}/auth/token",
            data={"username": username, "password": password},
            timeout=_REQUEST_TIMEOUT,
        )
        check(resp.status_code == 200, f"login {username} -> HTTP {resp.status_code}")
        tokens[username] = resp.json()["access_token"]
    info(f"authenticated {len(tokens)} demo principals (admin drives phases A/B/D)")
    return tokens


def bearer(token: str) -> dict[str, str]:
    """The ``Authorization`` header for a token."""
    return {"Authorization": f"Bearer {token}"}


def wait_for_budget(client: httpx.Client, token: str, needed: int, timeout: float = 20.0) -> int:
    """Wait until the principal's bucket holds ``needed`` tokens; return what it holds.

    The limiter reports its own state on every response, so this polls a real signal rather
    than sleeping a guessed interval. It exists because ``LOAD_MAX_ERROR_RATE`` defaults to
    zero: starting a phase on a bucket that a previous phase left half-drained would score the
    limiter's 429s as server errors, which is a measurement bug, not a finding.
    """
    def header_int(resp: httpx.Response, name: str) -> int:
        try:
            return int(float(resp.headers.get(name, "0")))
        except ValueError:
            return 0

    deadline = time.monotonic() + timeout
    while True:
        resp = client.get(
            f"{API}/logs", params={"limit": 1}, headers=bearer(token), timeout=_REQUEST_TIMEOUT
        )
        limit = header_int(resp, "X-RateLimit-Limit")
        remaining = header_int(resp, "X-RateLimit-Remaining")
        want = min(needed, limit) if limit else needed
        # `remaining + 1`: this very poll consumed a token, so a full bucket can never report
        # `want` back. Without the +1 the wait could never be satisfied when needed == burst.
        if remaining + 1 >= want or time.monotonic() >= deadline:
            return remaining
        time.sleep(0.25)


def sample_ids(client: httpx.Client, token: str, count: int) -> list[str]:
    """Pull real entry ids for phase B's single-fetch slice (a 404 would not be a read)."""
    resp = client.get(
        f"{API}/logs",
        params={"limit": min(count, 500)},
        headers=bearer(token),
        timeout=_REQUEST_TIMEOUT,
    )
    check(resp.status_code == 200, f"id sampling -> HTTP {resp.status_code}")
    ids = [item["id"] for item in resp.json()["items"]]
    check(bool(ids), "the store returned no entries; there is nothing to load-test against")
    return ids


def backend_memory(client: httpx.Client, token: str) -> dict[str, Any]:
    """Read the SERVER's own ``/debug/memory`` snapshot (never this client's RSS)."""
    resp = client.get(f"{API}/debug/memory", headers=bearer(token), timeout=_REQUEST_TIMEOUT)
    check(resp.status_code == 200, f"GET /debug/memory -> HTTP {resp.status_code}")
    body = resp.json()
    memory_mb = body.get("memory_mb")
    check(
        isinstance(memory_mb, (int, float)) and not isinstance(memory_mb, bool),
        f"/debug/memory memory_mb is {memory_mb!r} (want a number; psutil unavailable?)",
    )
    return body


# --------------------------------------------------------------------------- #
# Phase A — concurrent paginated reads
# --------------------------------------------------------------------------- #
async def _fire(
    requests: list[tuple[str, str, str, dict[str, Any] | None, dict[str, Any] | None]],
    token: str,
) -> tuple[float, list[tuple[str, float, int]]]:
    """Run every ``(label, method, path, params, json)`` through the semaphore.

    Returns ``(wall_seconds, [(label, latency_ms, status), ...])``. A request that raises is
    recorded with status ``0`` so the error gate counts it as the hard failure it is. Wall time
    spans the first dispatch to the last completion, so ``rps = successes / wall`` is the real
    end-to-end rate at ``CONCURRENCY``.
    """
    sem = asyncio.Semaphore(CONCURRENCY)
    samples: list[tuple[str, float, int]] = []
    limits = httpx.Limits(max_connections=CONCURRENCY * 2, max_keepalive_connections=CONCURRENCY)

    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=_REQUEST_TIMEOUT, limits=limits, headers=bearer(token)
    ) as client:

        async def one(
            label: str,
            method: str,
            path: str,
            params: dict[str, Any] | None,
            payload: dict[str, Any] | None,
        ) -> None:
            async with sem:
                started = time.perf_counter()
                try:
                    resp = await client.request(method, path, params=params, json=payload)
                    samples.append(
                        (label, (time.perf_counter() - started) * 1000.0, resp.status_code)
                    )
                except Exception:  # noqa: BLE001 — any transport failure is a hard error
                    samples.append((label, (time.perf_counter() - started) * 1000.0, 0))

        wall0 = time.perf_counter()
        await asyncio.gather(*(one(*spec) for spec in requests))
        wall = time.perf_counter() - wall0

    return wall, samples


def _gate_phase(name: str, wall: float, samples: list[tuple[str, float, int]]) -> dict[str, float]:
    """Apply the three shared gates (rps, error rate, p95) to one phase's samples."""
    latencies = [latency for _label, latency, _status in samples]
    ok = sum(1 for _label, _latency, status in samples if 200 <= status < 300)
    errors = len(samples) - ok
    error_rate = errors / len(samples) if samples else 1.0
    rps = (ok / wall) if wall > 0 else 0.0
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    info(f"{name}: {ok}/{len(samples)} ok in {wall:.2f}s -> {rps:.1f} req/s (errors={errors})")
    info(f"{name}: p50 {p50:.1f}ms / p95 {p95:.1f}ms / p99 {p99:.1f}ms")

    if errors:
        failing = sorted({status for _l, _lat, status in samples if not 200 <= status < 300})
        info(f"{name}: non-2xx statuses seen: {failing}")
    check(
        error_rate <= MAX_ERROR_RATE,
        f"{name} error rate {error_rate:.4f} ({errors}/{len(samples)}) above gate "
        f"{MAX_ERROR_RATE:.4f}",
    )
    check(rps >= MIN_RPS, f"{name} throughput {rps:.1f} req/s below gate {MIN_RPS:.0f} req/s")
    check(
        p95 <= LOAD_MAX_P95_MS,
        f"{name} p95 {p95:.1f}ms above gate LOAD_MAX_P95_MS={LOAD_MAX_P95_MS:.0f}ms "
        f"(under-load ceiling at concurrency {CONCURRENCY}; not the E2E's sequential MAX_P95_MS)",
    )
    return {"rps": rps, "errors": float(errors), "p50": p50, "p95": p95, "p99": p99, "wall": wall}


def phase_a(token: str) -> dict[str, float]:
    """A. ``LOAD_REQUESTS`` concurrent paginated reads at ``LOAD_CONCURRENCY``."""
    info(f"phase A: {LOAD_REQUESTS} GET /logs at concurrency {CONCURRENCY}...")
    # `offset` walks a different slice per request so the server cannot serve one hot page
    # over and over; it is also the paging mode that does real index work per call.
    specs = [
        ("GET /logs", "GET", f"{API}/logs", {"limit": 50, "offset": (i * 50) % 5000}, None)
        for i in range(LOAD_REQUESTS)
    ]
    wall, samples = asyncio.run(_fire(specs, token))
    return _gate_phase("phase A", wall, samples)


# --------------------------------------------------------------------------- #
# Phase B — the mixed read profile
# --------------------------------------------------------------------------- #
def phase_b(token: str, ids: list[str]) -> dict[str, Any]:
    """B. A fixed mix of list / search / single-fetch / stats under the same gates."""
    info(f"phase B: {MIXED_REQUESTS} mixed reads at concurrency {CONCURRENCY}...")
    specs: list[tuple[str, str, str, dict[str, Any] | None, dict[str, Any] | None]] = []
    for i in range(MIXED_REQUESTS):
        slot = i % 100
        cursor = 0
        label = MIXED_MIX[-1][0]
        for candidate, weight in MIXED_MIX:
            cursor += weight
            if slot < cursor:
                label = candidate
                break
        if label == "GET /logs":
            params = {"limit": 50, "offset": (i * 7) % 5000}
            specs.append((label, "GET", f"{API}/logs", params, None))
        elif label == "POST /logs/search":
            payload = {"filter": SEARCH_TREE, "limit": 50}
            specs.append((label, "POST", f"{API}/logs/search", None, payload))
        elif label == "GET /logs/{id}":
            specs.append((label, "GET", f"{API}/logs/{ids[i % len(ids)]}", None, None))
        else:
            specs.append((label, "GET", f"{API}/stats", {"bucket_sec": 60}, None))

    wall, samples = asyncio.run(_fire(specs, token))
    summary = _gate_phase("phase B", wall, samples)

    per_endpoint: dict[str, dict[str, float]] = {}
    for label, _weight in MIXED_MIX:
        latencies = [lat for lbl, lat, _status in samples if lbl == label]
        if not latencies:
            continue
        per_endpoint[label] = {
            "n": float(len(latencies)),
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
        }
        info(
            f"phase B: {label:<18} n={len(latencies):<5} "
            f"p50 {per_endpoint[label]['p50']:.1f}ms / p95 {per_endpoint[label]['p95']:.1f}ms"
        )
    return {"summary": summary, "per_endpoint": per_endpoint}


# --------------------------------------------------------------------------- #
# Phase C — SSE fan-out with a deliberate slow consumer
# --------------------------------------------------------------------------- #
def _assign_principals(count: int) -> list[str]:
    """Round-robin ``count`` streams over the streaming-capable principals."""
    return [STREAM_PRINCIPALS[i % len(STREAM_PRINCIPALS)] for i in range(count)]


async def _well_behaved_client(
    token: str, deadline: float, counter: list[int], index: int
) -> None:
    """One stream that reads every frame promptly until the phase's deadline."""
    timeout = httpx.Timeout(_REQUEST_TIMEOUT, read=None)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        async with aconnect_sse(
            client,
            "GET",
            f"{API}/logs/stream",
            params={"service": SSE_LOAD_SERVICE},
            headers=bearer(token),
        ) as source:
            if source.response.status_code != 200:
                await source.response.aread()
                raise CheckError(
                    f"SSE client #{index} -> HTTP {source.response.status_code}: "
                    f"{source.response.text[:200]}"
                )
            frames = source.aiter_sse()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                try:
                    # Two-arg `anext` so exhaustion arrives as a sentinel rather than a
                    # StopAsyncIteration thrown across the wait_for task boundary.
                    frame = await asyncio.wait_for(anext(frames, None), timeout=remaining)
                except TimeoutError:
                    return
                if frame is None:
                    return
                if frame.event == "log":
                    counter[index] += 1


async def _slow_consumer(token: str, deadline: float, observed: dict[str, Any]) -> None:
    """One stream that opens, then never reads — the client the server must not carry.

    It holds the connection for the whole phase without pulling a single frame, so the server's
    per-subscriber queue is the only thing standing between a stalled reader and unbounded
    memory growth. At the end it drains whatever the socket happened to hold, purely to
    *report* whether a ``dropped`` frame was emitted. That is reporting, not gating: kernel
    socket buffers absorb a large slice of the backlog before the server-side queue can
    overflow, so within a short window the honest observable is "memory stayed bounded and the
    subscription was released", not "the drop happened by now".
    """
    timeout = httpx.Timeout(_REQUEST_TIMEOUT, read=None)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        async with client.stream(
            "GET",
            f"{API}/logs/stream",
            params={"service": SSE_LOAD_SERVICE},
            headers=bearer(token),
        ) as resp:
            if resp.status_code != 200:
                await resp.aread()
                raise CheckError(
                    f"slow consumer -> HTTP {resp.status_code}: {resp.text[:200]}"
                )
            observed["connected"] = True
            # The whole point: park without reading for the entire window.
            await asyncio.sleep(max(0.0, deadline - time.monotonic()))
            # Drain what the socket buffered, bounded, to see whether we were told we were cut.
            chunks: list[bytes] = []
            try:
                async with asyncio.timeout(2.0):
                    async for chunk in resp.aiter_bytes():
                        chunks.append(chunk)
                        if sum(len(c) for c in chunks) > 1_000_000:
                            break
            except (TimeoutError, httpx.ReadError):
                pass
            text = b"".join(chunks).decode("utf-8", "replace")
            observed["buffered_bytes"] = len(text)
            observed["dropped"] = "event: dropped" in text


async def _appender(token: str, deadline: float, counter: list[int]) -> None:
    """Append matching entries at a paced rate for the whole phase, as the writer."""
    interval = 1.0 / SSE_APPEND_RPS
    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=_REQUEST_TIMEOUT, headers=bearer(token)
    ) as client:
        index = 0
        while time.monotonic() < deadline:
            payload = {
                "level": "INFO",
                "service": SSE_LOAD_SERVICE,
                "host": "load-runner",
                "message": f"sse fan-out probe {index} {uuid.uuid4().hex[:8]}",
            }
            try:
                resp = await client.post(f"{API}/logs", json=payload)
                if resp.status_code == 201:
                    counter[0] += 1
                else:
                    counter[1] += 1
            except Exception:  # noqa: BLE001 — a failed append is counted, never fatal here
                counter[1] += 1
            index += 1
            await asyncio.sleep(interval)


async def _sampler(token: str, deadline: float, peaks: dict[str, float]) -> None:
    """Poll the server's own /debug/memory through the phase, keeping the peaks.

    Sampled continuously rather than only at the end because a slow consumer that *was* being
    buffered would show up as a transient RSS bulge that a single after-the-fact reading would
    miss entirely — the failure mode this phase exists to catch.
    """
    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=_REQUEST_TIMEOUT, headers=bearer(token)
    ) as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(f"{API}/debug/memory")
                if resp.status_code == 200:
                    body = resp.json()
                    peaks["memory_mb"] = max(peaks["memory_mb"], float(body["memory_mb"]))
                    peaks["subscribers"] = max(peaks["subscribers"], float(body["subscribers"]))
            except Exception:  # noqa: BLE001 — a sampling miss is not a phase failure
                pass
            await asyncio.sleep(0.5)


async def _run_phase_c(tokens: dict[str, str]) -> dict[str, Any]:
    """Hold the fan-out, the slow consumer, the appender and the sampler concurrently."""
    assignments = _assign_principals(SSE_CLIENTS)
    # Put the slow consumer on whichever eligible principal is carrying the fewest streams, so
    # the per-principal cap is respected by construction rather than by luck.
    load = {name: assignments.count(name) for name in STREAM_PRINCIPALS}
    slow_principal = min(STREAM_PRINCIPALS, key=lambda name: load[name])
    load[slow_principal] += 1
    over = {name: n for name, n in load.items() if n > MAX_STREAMS}
    check(
        not over,
        f"the requested fan-out needs {over} streams per principal but the server caps it at "
        f"{MAX_STREAMS}; lower LOAD_SSE_CLIENTS (max "
        f"{len(STREAM_PRINCIPALS) * MAX_STREAMS - 1}) or raise MAX_STREAMS_PER_PRINCIPAL",
    )

    counters = [0] * SSE_CLIENTS
    appends = [0, 0]
    observed: dict[str, Any] = {"connected": False, "dropped": False, "buffered_bytes": 0}
    peaks = {"memory_mb": 0.0, "subscribers": 0.0}
    deadline = time.monotonic() + SSE_SECONDS

    info(
        f"phase C: {SSE_CLIENTS} well-behaved streams over {sorted(set(assignments))} "
        f"(+1 slow consumer on {slow_principal!r}) for {SSE_SECONDS:.0f}s while the writer "
        f"appends at ~{SSE_APPEND_RPS:.0f}/s"
    )
    tasks = [
        asyncio.create_task(_well_behaved_client(tokens[name], deadline, counters, i))
        for i, name in enumerate(assignments)
    ]
    tasks.append(asyncio.create_task(_slow_consumer(tokens[slow_principal], deadline, observed)))
    tasks.append(asyncio.create_task(_appender(tokens["writer"], deadline, appends)))
    tasks.append(asyncio.create_task(_sampler(tokens["admin"], deadline, peaks)))

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, CheckError):
            raise outcome
        if isinstance(outcome, BaseException):
            raise CheckError(f"phase C task failed: {type(outcome).__name__}: {outcome}")

    return {
        "events": counters,
        "appends_ok": appends[0],
        "appends_rejected": appends[1],
        "slow": observed,
        "peaks": peaks,
        "expected_subscribers": SSE_CLIENTS + 1,
    }


def phase_c(client: httpx.Client, tokens: dict[str, str]) -> dict[str, Any]:
    """C. SSE fan-out: every well-behaved client keeps up, and the slow one costs nothing."""
    baseline = int(backend_memory(client, tokens["admin"])["subscribers"])
    check(baseline == 0, f"{baseline} SSE subscriber(s) already registered before phase C")

    outcome = asyncio.run(_run_phase_c(tokens))
    counters: list[int] = outcome["events"]
    peaks = outcome["peaks"]
    slow = outcome["slow"]

    info(
        f"phase C: appends ok={outcome['appends_ok']} rejected={outcome['appends_rejected']}; "
        f"events per client={counters}"
    )
    info(
        f"phase C: peak subscribers={int(peaks['subscribers'])} "
        f"(expected {outcome['expected_subscribers']}), peak server RSS "
        f"{peaks['memory_mb']:.1f} MB; slow consumer connected={slow['connected']} "
        f"dropped={slow['dropped']} buffered={slow['buffered_bytes']}B"
    )

    check(slow["connected"], "the slow consumer never connected; the phase proved nothing")
    worst = min(counters) if counters else 0
    check(
        worst >= MIN_SSE_EVENTS,
        f"the slowest well-behaved SSE client received {worst} log frames, gate is "
        f"{MIN_SSE_EVENTS} (per-client counts: {counters})",
    )
    # No leak: the server never registered more subscriptions than were actually opened.
    check(
        peaks["subscribers"] <= outcome["expected_subscribers"],
        f"server reported {int(peaks['subscribers'])} subscribers but only "
        f"{outcome['expected_subscribers']} were opened",
    )
    # Bounded memory under a stalled reader — the gate the slow consumer exists to test.
    check(
        peaks["memory_mb"] <= MAX_BACKEND_MEM_MB,
        f"peak server RSS during the fan-out {peaks['memory_mb']:.1f} MB above gate "
        f"{MAX_BACKEND_MEM_MB:.0f} MB (a slow consumer was buffered rather than dropped?)",
    )

    # Eventual release: every subscription — the slow one included — returns the slot.
    after = _wait_for_subscribers(client, tokens["admin"], baseline, timeout=15.0)
    check(
        after <= baseline,
        f"{after} SSE subscriber(s) still registered 15s after teardown (baseline {baseline}); "
        "a disconnected client is leaking a subscription",
    )
    outcome["subscribers_after"] = after
    return outcome


def _wait_for_subscribers(
    client: httpx.Client, token: str, target: int, timeout: float
) -> int:
    """Poll the server's subscriber count until it is back at ``target``; return the reading."""
    deadline = time.monotonic() + timeout
    count = int(backend_memory(client, token)["subscribers"])
    while count > target and time.monotonic() < deadline:
        time.sleep(0.5)
        count = int(backend_memory(client, token)["subscribers"])
    return count


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== load harness against {BASE_URL} ==")
    info(
        f"gates: rps >= {MIN_RPS:.0f}; error rate <= {MAX_ERROR_RATE:.4f}; "
        f"p95 <= {LOAD_MAX_P95_MS:.0f}ms (under load, c={CONCURRENCY}); "
        f"SSE >= {MIN_SSE_EVENTS} frames/client; "
        f"server RSS <= {MAX_BACKEND_MEM_MB:.0f} MB"
    )
    with httpx.Client(base_url=BASE_URL, timeout=_REQUEST_TIMEOUT) as client:
        wait_ready(client)
        tokens = login_all(client)
        admin = tokens["admin"]
        ids = sample_ids(client, admin, 200)

        budget = wait_for_budget(client, admin, LOAD_REQUESTS)
        info(f"phase A start: admin bucket holds {budget} tokens")
        a = phase_a(admin)

        budget = wait_for_budget(client, admin, MIXED_REQUESTS)
        info(f"phase B start: admin bucket holds {budget} tokens")
        b = phase_b(admin, ids)

        c = phase_c(client, tokens)

        memory = backend_memory(client, admin)
        memory_mb = float(memory["memory_mb"])
        info(
            f"phase D: server RSS {memory_mb:.1f} MB, entries={memory['entries']}, "
            f"evicted={memory['evicted']}, rate_buckets={memory['rate_buckets']}"
        )

    # Machine-readable summary lines (C14 transcribes these into the README verbatim).
    result("requests", LOAD_REQUESTS)
    result("concurrency", CONCURRENCY)
    # The gates themselves, emitted alongside the measurements so C14's
    # `| Metric | Result | Gate |` table is transcribed from one source rather than two.
    result("gate_min_rps", f"{MIN_RPS:.0f}")
    result("gate_max_p95_ms", f"{LOAD_MAX_P95_MS:.0f}")
    result("gate_max_error_rate", f"{MAX_ERROR_RATE:.4f}")
    result("gate_max_memory_mb", f"{MAX_BACKEND_MEM_MB:.0f}")
    result("phase_a_wall_s", f"{a['wall']:.2f}")
    result("phase_a_rps", f"{a['rps']:.1f}")
    result("phase_a_errors", int(a["errors"]))
    result("phase_a_p50_ms", f"{a['p50']:.1f}")
    result("phase_a_p95_ms", f"{a['p95']:.1f}")
    result("phase_a_p99_ms", f"{a['p99']:.1f}")
    result("phase_b_requests", MIXED_REQUESTS)
    result("phase_b_rps", f"{b['summary']['rps']:.1f}")
    result("phase_b_errors", int(b["summary"]["errors"]))
    result("phase_b_p50_ms", f"{b['summary']['p50']:.1f}")
    result("phase_b_p95_ms", f"{b['summary']['p95']:.1f}")
    for label, stats in b["per_endpoint"].items():
        slug = label.replace(" ", "_").replace("/", "_").replace("{", "").replace("}", "").lower()
        result(f"phase_b_{slug}_n", int(stats["n"]))
        result(f"phase_b_{slug}_p50_ms", f"{stats['p50']:.1f}")
        result(f"phase_b_{slug}_p95_ms", f"{stats['p95']:.1f}")
    result("sse_clients", SSE_CLIENTS)
    result("sse_seconds", f"{SSE_SECONDS:.0f}")
    result("sse_events_total", sum(c["events"]))
    result("sse_events_min_per_client", min(c["events"]) if c["events"] else 0)
    result("sse_appends_ok", c["appends_ok"])
    result("sse_appends_rejected", c["appends_rejected"])
    result("sse_peak_subscribers", int(c["peaks"]["subscribers"]))
    result("sse_subscribers_after", c["subscribers_after"])
    result("sse_peak_memory_mb", f"{c['peaks']['memory_mb']:.1f}")
    result("slow_consumer_dropped", str(bool(c["slow"]["dropped"])).lower())
    result("slow_consumer_buffered_bytes", c["slow"]["buffered_bytes"])
    result("memory_mb", f"{memory_mb:.1f}")

    # Phase D — the last gate, on the SERVER's reported RSS and nothing else.
    check(
        memory_mb <= MAX_BACKEND_MEM_MB,
        f"server RSS {memory_mb:.1f} MB above gate {MAX_BACKEND_MEM_MB:.0f} MB",
    )

    print("", flush=True)
    info(
        f"RESULT (headline) phase_a_rps={a['rps']:.1f} phase_a_p95_ms={a['p95']:.1f} "
        f"phase_b_rps={b['summary']['rps']:.1f} phase_b_p95_ms={b['summary']['p95']:.1f} "
        f"memory_mb={memory_mb:.1f}"
    )
    print("LOAD PASSED", flush=True)


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print(f"LOAD FAILED ({exc})", file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 — any unexpected error is a hard failure
        print("", flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED (unexpected error)", file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

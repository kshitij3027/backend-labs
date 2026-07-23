"""Black-box end-to-end verifier for the Log Query API (REST) (C12).

Runs **inside Docker** (the profile-gated ``e2e`` compose service) against the LIVE ``api``
service over HTTP + SSE only, reached by service name (``TARGET_URL=http://api:8000``) rather
than a host port. It never imports the app, the router or the store — the only thing it is
allowed to reach into is :mod:`src.auth` (for the demo credentials the server itself
authenticates against) and :mod:`src.generators` (the deterministic corpus that is the
**ground truth** every count is graded against). The tester image ships ``src/`` precisely so
those two imports resolve; hard-coding a second copy of either would let a rename drift the
harness away from the thing it is supposed to be checking.

The first failing check prints a loud ``FAIL`` line and exits 1 immediately, so ``make e2e``
propagates it.

The 15 checks, in order:

 1. ``GET /health`` returns ``{status, version, uptime_sec}`` with ``status == "healthy"``, and
    ``X-Request-ID`` is both **minted** when absent and **echoed** verbatim when supplied.
 2. ``POST /api/v1/auth/token`` issues a bearer token for all four demo users, and each
    response's ``role``/``tier`` match :data:`src.auth.DEV_ACCOUNTS`.
 3. No token -> ``401`` **with** ``WWW-Authenticate: Bearer``; a garbage bearer -> ``401``. A
    ``401`` carries **no** ``X-RateLimit-*`` headers (there is no principal to meter).
 4. A viewer's ``GET /api/v1/logs`` returns the ``{items, page}`` envelope with all five
    ``page`` keys (``limit, returned, next_cursor, has_more, total``) and the full
    ``X-RateLimit-Limit/-Remaining/-Reset`` triple.
 5. **A full cursor walk covers the corpus exactly once.** Page through at ``E2E_PAGE_LIMIT``,
    re-sending the identical filter params alongside every cursor (a cursor is bound to its
    filter's fingerprint), counting ids: zero duplicates, and the id count equals the first
    page's ``page.total``.
 6. ``limit=100000`` is **clamped** to ``max_page_size`` and carries ``X-Page-Limit-Clamped`` —
    it is never a ``422``.
 7. ``GET /api/v1/logs/{id}`` round-trips an id taken from the list; a bogus id -> ``404`` with
    the ``{detail, code, request_id}`` error envelope.
 8. ``POST /api/v1/logs/search`` -> ``403`` for the viewer, ``200`` for the analyst, and its
    ``page.total`` agrees BOTH with the equivalent ``GET /api/v1/logs`` filter AND with
    :func:`src.generators.expected_counts` over the regenerated corpus (level, service, and a
    compound ``all``/``in``/``not`` tree that ``GET /logs`` cannot express).
 9. SSE delivers a **post-connect** append: connect as the analyst on a filter no seeded entry
    matches, wait for the ``ready`` frame (the deterministic go-signal — never a sleep), append
    a uniquely marked entry as the writer, and assert the marker frame arrives within
    ``E2E_SSE_TIMEOUT`` and that its JSON is byte-identical to ``GET /logs/{id}``.
10. SSE concurrency cap: ``MAX_STREAMS_PER_PRINCIPAL + 1`` simultaneous connects for one
    principal -> ``429`` whose ``detail`` is the **stream-specific** one, asserted to be
    distinct from the rate-limiter's.
11. ``Last-Event-ID`` resume delivers the gap **without duplication**: stream A, disconnect,
    append B and C, reconnect from A's id -> ``ready.replayed == 2`` and exactly ``[B, C]``.
12. ``GET /api/v1/stats`` ``total`` equals ``GET /api/v1/logs`` ``page.total`` for the same
    filter (three different filters), and ``by_level`` sums to ``total``.
13. RBAC: the writer on ``GET /api/v1/debug/memory`` -> ``403`` (which **does** carry the
    ``X-RateLimit-*`` triple — the bucket is peeked, not consumed); the admin -> ``200`` with
    all six fields.
14. Rate limiting is reachable: ``E2E_RATE_LIMIT_PROBE`` requests as the **free-tier** viewer
    produce at least one ``429`` carrying ``Retry-After >= 1`` and ``X-RateLimit-Remaining: 0``.
15. p95 over ``E2E_LATENCY_SAMPLES`` sequential reads <= ``MAX_P95_MS`` (p50 + p95 printed),
    the backend's own ``memory_mb`` <= ``MAX_BACKEND_MEM_MB``, and ``/openapi.json``, ``/docs``
    and ``/redoc`` all answer ``200``.

**``POST /auth/token`` is excluded from every latency measurement.** bcrypt at 12 rounds costs
~250 ms per verification by design — that cost *is* the brake on credential stuffing, and it is
the one deliberately-slow, unmetered route. Timing it would measure the hash, not the API.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``                 API base URL (default ``http://api:8000``)
* ``E2E_READY_TIMEOUT``          seconds to wait for ``/health`` (default 90)
* ``E2E_PAGE_LIMIT``             page size for the check-5 cursor walk (default 100)
* ``E2E_SSE_TIMEOUT``            seconds to wait for an SSE frame (default 15)
* ``E2E_RATE_LIMIT_PROBE``       requests fired at the free-tier viewer (default 60)
* ``E2E_LATENCY_SAMPLES``        sequential reads timed for p95 (default 50)
* ``MAX_P95_MS``                 **sequential** read p95 ceiling, ms (default 250)
* ``MAX_BACKEND_MEM_MB``         backend RSS ceiling, MB (default 400)
* ``MAX_STREAMS_PER_PRINCIPAL``  the server's own SSE cap, mirrored here (default 3)

``MAX_P95_MS`` bounds check 15 only, where the reads are issued **one at a time**, so it
measures service time. It is deliberately *not* the load harness's ceiling: ``load_test.py``
runs at concurrency 50, where queueing alone dominates the number, and it reads its own
``LOAD_MAX_P95_MS``. Giving the two measurements one variable fails a healthy server on
arithmetic, which is exactly what it did before they were split.

Every gate is host-overridable, which is how we prove the gates are real rather than
decorative: ``MAX_BACKEND_MEM_MB=1 make e2e`` **MUST** exit non-zero, and so must
``MAX_P95_MS=0 make e2e``. Exit code 0 with ``E2E PASSED (15/15)`` only when all 15 hold.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx
from httpx_sse import aconnect_sse

from src.auth import DEV_ACCOUNTS, DEV_PASSWORDS
from src.generators import DEFAULT_SEED, expected_counts, generate_entries

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))
PAGE_LIMIT = int(os.environ.get("E2E_PAGE_LIMIT", "100"))
SSE_TIMEOUT = float(os.environ.get("E2E_SSE_TIMEOUT", "15"))
RATE_LIMIT_PROBE = int(os.environ.get("E2E_RATE_LIMIT_PROBE", "60"))
LATENCY_SAMPLES = int(os.environ.get("E2E_LATENCY_SAMPLES", "50"))
MAX_P95_MS = float(os.environ.get("MAX_P95_MS", "250"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "400"))
# Mirrors the server's SSE cap. Not declared on the `e2e` compose service (only the `api` one
# is), so it is read here with the same default the api service uses — an operator who exports
# a different value for the stack gets it applied on both sides.
MAX_STREAMS = int(os.environ.get("MAX_STREAMS_PER_PRINCIPAL", "3"))

TOTAL_CHECKS = 15

API = "/api/v1"

#: The page-envelope contract from C2. All five keys, or the envelope is not the documented one.
_PAGE_KEYS = frozenset({"limit", "returned", "next_cursor", "has_more", "total"})

#: The `/debug/memory` contract from C11 — all six fields (check 13).
_MEMORY_KEYS = frozenset(
    {"memory_mb", "entries", "capacity", "evicted", "subscribers", "rate_buckets"}
)

#: The three headers the middleware stamps on every *authenticated* response (checks 4 + 13).
_RATELIMIT_HEADERS = ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset")

#: The two 429 details, spelled once here so check 10 can assert it got the one about streams
#: and *not* the one about requests. They meter different resources; a harness that accepted
#: either would pass even if the concurrency cap had silently stopped existing.
_STREAM_429_DETAIL = "too many concurrent SSE streams"
_RATE_429_DETAIL = "rate limit exceeded for tier"

#: Service names used by the SSE probes. Deliberately absent from the generated corpus, so a
#: stream filtered on one of them sees *only* what this verifier appends — no replay, no
#: background traffic, and therefore no ambiguity about which frame is ours.
_SSE_PROBE_SERVICE = "e2e-sse-probe"
_SSE_RESUME_SERVICE = "e2e-resume-probe"

#: Cross-check state. Facts a later check needs from an earlier one (the corpus size measured
#: *before* any append, the ground-truth tally derived from it, a real entry id).
STATE: dict[str, Any] = {}


class CheckFailure(AssertionError):
    """Raised inside a check to fail it with a single clear detail line."""


# --------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------- #
CLIENT = httpx.Client(base_url=BASE_URL, timeout=60.0)

#: `username -> bearer token`, populated by check 2 and consumed by every check after it.
TOKENS: dict[str, str] = {}


def auth(username: str) -> dict[str, str]:
    """The ``Authorization`` header for a demo principal (requires check 2 to have run)."""
    token = TOKENS.get(username)
    if not token:
        raise CheckFailure(f"no token for {username!r} (check 2 did not run or did not issue it)")
    return {"Authorization": f"Bearer {token}"}


def _percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); 0.0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


def request(
    method: str,
    path: str,
    *,
    username: str | None = None,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """Issue one request, turning a transport failure into a check failure rather than a crash."""
    merged = dict(headers or {})
    if username is not None:
        merged.update(auth(username))
    try:
        return CLIENT.request(method, path, headers=merged, **kwargs)
    except Exception as exc:  # noqa: BLE001 — a network failure is a check failure
        raise CheckFailure(f"{method} {path} raised {type(exc).__name__}: {exc}") from exc


def expect(
    method: str, path: str, status: int, *, username: str | None = None, **kwargs: Any
) -> httpx.Response:
    """Issue one request and fail the check unless it returned exactly ``status``."""
    resp = request(method, path, username=username, **kwargs)
    if resp.status_code != status:
        raise CheckFailure(
            f"{method} {path} -> HTTP {resp.status_code} (expected {status}): {resp.text[:200]}"
        )
    return resp


def body_of(resp: httpx.Response) -> Any:
    """Decode a JSON body, failing the check when the response is not JSON at all."""
    try:
        return resp.json()
    except ValueError as exc:
        raise CheckFailure(
            f"{resp.request.method} {resp.request.url.path} returned non-JSON: {resp.text[:200]}"
        ) from exc


def list_logs(params: dict[str, Any], *, username: str = "admin") -> dict[str, Any]:
    """``GET /api/v1/logs`` expecting 200; return the decoded ``{items, page}`` envelope."""
    body = body_of(expect("GET", f"{API}/logs", 200, username=username, params=params))
    if not isinstance(body, dict) or "items" not in body or "page" not in body:
        raise CheckFailure(f"GET /logs body is not a {{items, page}} envelope: {body!r}"[:300])
    return body


def search_logs(payload: dict[str, Any], *, username: str = "analyst") -> dict[str, Any]:
    """``POST /api/v1/logs/search`` expecting 200; return the decoded envelope."""
    return body_of(expect("POST", f"{API}/logs/search", 200, username=username, json=payload))


def append_entry(message: str, *, service: str, level: str = "INFO") -> dict[str, Any]:
    """Append one entry as the **writer** and return the created ``LogEntry`` (asserts 201)."""
    payload = {"level": level, "service": service, "host": "e2e-runner", "message": message}
    return body_of(expect("POST", f"{API}/logs", 201, username="writer", json=payload))


# --------------------------------------------------------------------------- #
# The check harness
# --------------------------------------------------------------------------- #
_counter = 0


def check(name: str, fn: Callable[[], str]) -> None:
    """Run one check; print PASS with evidence, or FAIL + exit 1 immediately."""
    global _counter
    _counter += 1
    prefix = f"[{_counter:2d}/{TOTAL_CHECKS}]"
    try:
        evidence = fn()
    except CheckFailure as exc:
        print(f"{prefix} FAIL {name}: {exc}", flush=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — an unexpected error is still a failure
        print(f"{prefix} FAIL {name}: unexpected {type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)
    print(f"{prefix} PASS {name} ({evidence})", flush=True)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live rather than at exit)."""
    print(f"[e2e] {msg}", flush=True)


def _require_int(value: object, name: str) -> int:
    """Fail unless ``value`` is a real (non-bool) integer; return it."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CheckFailure(f"{name} is {value!r} (want an int)")
    return value


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll ``GET /health`` until it answers 200, or exit 1 at the timeout.

    Compose already gates the ``e2e`` service on the api healthcheck, so this normally returns
    on the first poll. It stays because ``TARGET_URL`` can point at something compose does not
    manage, and a verifier whose very first check races a still-seeding store is a flake
    generator rather than a gate.
    """
    info(f"waiting for {BASE_URL}/health (up to {timeout:.0f}s)...")
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = CLIENT.get("/health", timeout=5.0)
            if resp.status_code == 200:
                info("api is ready")
                return
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        else:
            last = f"HTTP {resp.status_code}"
        time.sleep(2.0)
    print(f"FAIL bootstrap: /health not ready after {timeout:.0f}s (last: {last})", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# 1 — health + request-id
# --------------------------------------------------------------------------- #
def check_health() -> str:
    """1. Health shape, plus X-Request-ID minted when absent and echoed when supplied."""
    resp = expect("GET", "/health", 200)
    body = body_of(resp)
    missing = {"status", "version", "uptime_sec"} - set(body)
    if missing:
        raise CheckFailure(f"/health is missing {sorted(missing)}: {body!r}")
    if body["status"] != "healthy":
        raise CheckFailure(f"/health status is {body['status']!r} (want 'healthy')")

    minted = resp.headers.get("X-Request-ID")
    if not minted:
        raise CheckFailure("/health response carries no X-Request-ID (middleware did not mint one)")

    supplied = f"e2e-{uuid.uuid4().hex[:12]}"
    echoed = expect("GET", "/health", 200, headers={"X-Request-ID": supplied})
    if echoed.headers.get("X-Request-ID") != supplied:
        raise CheckFailure(
            f"X-Request-ID not echoed: sent {supplied!r}, got "
            f"{echoed.headers.get('X-Request-ID')!r}"
        )
    return f"version={body['version']}, uptime={body['uptime_sec']}s, request-id minted + echoed"


# --------------------------------------------------------------------------- #
# 2 — token issue for all four demo users
# --------------------------------------------------------------------------- #
def check_token_issue() -> str:
    """2. Every demo account authenticates, and the claims match the server's own directory."""
    for username, (_password, role, tier) in DEV_ACCOUNTS.items():
        password = DEV_PASSWORDS[username]
        # Form-encoded, not JSON: the route consumes an OAuth2PasswordRequestForm so the Swagger
        # "Authorize" button and the README's literal `curl -d 'username=...'` both work.
        resp = expect(
            "POST",
            f"{API}/auth/token",
            200,
            data={"username": username, "password": password},
        )
        body = body_of(resp)
        token = body.get("access_token")
        if not isinstance(token, str) or not token:
            raise CheckFailure(f"{username}: no access_token in {body!r}")
        if body.get("token_type") != "bearer":
            raise CheckFailure(f"{username}: token_type is {body.get('token_type')!r}")
        if body.get("role") != role.value:
            raise CheckFailure(
                f"{username}: role is {body.get('role')!r}, DEV_ACCOUNTS says {role.value!r}"
            )
        if body.get("tier") != tier.value:
            raise CheckFailure(
                f"{username}: tier is {body.get('tier')!r}, DEV_ACCOUNTS says {tier.value!r}"
            )
        if _require_int(body.get("expires_in"), f"{username}.expires_in") <= 0:
            raise CheckFailure(f"{username}: expires_in is not positive ({body.get('expires_in')})")
        if not body.get("expires_at"):
            raise CheckFailure(f"{username}: no expires_at in {body!r}")
        TOKENS[username] = token
    pairs = ", ".join(
        f"{name}={role.value}/{tier.value}" for name, (_pw, role, tier) in DEV_ACCOUNTS.items()
    )
    return f"4 tokens issued, claims match DEV_ACCOUNTS ({pairs})"


# --------------------------------------------------------------------------- #
# 3 — 401 surface
# --------------------------------------------------------------------------- #
def check_unauthenticated() -> str:
    """3. Missing and malformed credentials both 401, and the challenge header is present."""
    anon = expect("GET", f"{API}/logs", 401)
    challenge = anon.headers.get("WWW-Authenticate", "")
    if "Bearer" not in challenge:
        raise CheckFailure(f"401 without a token carries WWW-Authenticate={challenge!r}")
    # A 401 has no principal, so there is no bucket to report on. Headers here would be a lie
    # about a limit that was never consulted.
    leaked = [h for h in _RATELIMIT_HEADERS if h in anon.headers]
    if leaked:
        raise CheckFailure(f"401 leaked rate-limit headers {leaked} (there is no principal)")

    garbage = expect(
        "GET",
        f"{API}/logs",
        401,
        headers={"Authorization": "Bearer not.a.real.token"},
    )
    if "Bearer" not in garbage.headers.get("WWW-Authenticate", ""):
        raise CheckFailure("401 for a garbage bearer carries no WWW-Authenticate: Bearer")
    return "no-token and garbage-bearer both 401 + WWW-Authenticate: Bearer, no X-RateLimit-*"


# --------------------------------------------------------------------------- #
# 4 — the list envelope
# --------------------------------------------------------------------------- #
def check_list_envelope() -> str:
    """4. A viewer's list returns the documented envelope and the rate-limit triple."""
    resp = expect("GET", f"{API}/logs", 200, username="viewer", params={"limit": 5})
    body = body_of(resp)
    if not isinstance(body, dict) or set(body) - {"items", "page"} or "items" not in body:
        raise CheckFailure(f"envelope keys are {sorted(body)} (want exactly items + page)")
    page = body.get("page")
    if not isinstance(page, dict):
        raise CheckFailure(f"page is {type(page).__name__} (want an object)")
    missing = _PAGE_KEYS - set(page)
    if missing:
        raise CheckFailure(f"page is missing {sorted(missing)} (has {sorted(page)})")
    if not isinstance(body["items"], list):
        raise CheckFailure(f"items is {type(body['items']).__name__} (want a list)")
    if page["returned"] != len(body["items"]):
        raise CheckFailure(f"page.returned {page['returned']} != len(items) {len(body['items'])}")
    absent = [h for h in _RATELIMIT_HEADERS if h not in resp.headers]
    if absent:
        raise CheckFailure(f"authenticated 200 is missing {absent}")
    return (
        f"items={page['returned']}, page.total={page['total']}, "
        f"X-RateLimit-Remaining={resp.headers['X-RateLimit-Remaining']}"
    )


# --------------------------------------------------------------------------- #
# 5 — the full cursor walk
# --------------------------------------------------------------------------- #
def check_cursor_walk() -> str:
    """5. Paging to exhaustion visits every entry exactly once.

    This is the correctness core of the whole API: the pagination contract is that a walk
    neither skips nor duplicates. Run as the **admin** (enterprise tier, burst 2000) because the
    walk is ~100 requests and the free-tier viewer's bucket would 429 partway through — the
    thing under test here is the cursor, not the limiter.

    Every page re-sends the identical filter params alongside the cursor. That is not
    boilerplate: the cursor carries a fingerprint of the filter it was minted under, and
    replaying it against a different filter is a deliberate 400 rather than a plausible-looking
    wrong page.
    """
    base_params = {"limit": PAGE_LIMIT, "order": "desc"}
    first = list_logs(dict(base_params))
    total = _require_int(first["page"]["total"], "page.total")
    if total <= 0:
        raise CheckFailure(f"corpus is empty (page.total={total}); nothing to walk")

    seen: set[str] = set()
    duplicates: list[str] = []
    pages = 0
    cursor = None
    # A walk over a ring that is not being appended to terminates in ceil(total/limit) pages;
    # the slack bounds a malformed `has_more` into a failure instead of an infinite loop.
    max_pages = math.ceil(total / max(1, PAGE_LIMIT)) + 5
    body = first
    while True:
        pages += 1
        for item in body["items"]:
            entry_id = item.get("id")
            if entry_id in seen:
                duplicates.append(entry_id)
            seen.add(entry_id)
        page = body["page"]
        cursor = page.get("next_cursor")
        if not page.get("has_more"):
            break
        if not cursor:
            raise CheckFailure(f"page {pages} says has_more but carries no next_cursor")
        if pages >= max_pages:
            raise CheckFailure(f"walk did not terminate within {max_pages} pages (total={total})")
        body = list_logs({**base_params, "cursor": cursor})

    if duplicates:
        raise CheckFailure(
            f"cursor walk returned {len(duplicates)} duplicate id(s), e.g. {duplicates[:3]}"
        )
    if len(seen) != total:
        raise CheckFailure(
            f"cursor walk covered {len(seen)} ids but page.total said {total} "
            f"({total - len(seen)} skipped) over {pages} pages"
        )

    # Captured BEFORE any append: checks 8's ground truth is only valid against the corpus as
    # seeded, and checks 9/11 add entries of their own.
    STATE["corpus_total"] = total
    STATE["sample_id"] = next(iter(seen))
    return f"{len(seen)} unique ids over {pages} pages of {PAGE_LIMIT}, 0 duplicates, 0 skips"


# --------------------------------------------------------------------------- #
# 6 — limit clamping
# --------------------------------------------------------------------------- #
def check_limit_clamped() -> str:
    """6. An absurd `limit` is clamped with a header, never rejected with a 422."""
    resp = expect("GET", f"{API}/logs", 200, username="admin", params={"limit": 100000})
    page = body_of(resp)["page"]
    clamped_header = resp.headers.get("X-Page-Limit-Clamped")
    if not clamped_header:
        raise CheckFailure("limit=100000 was accepted without an X-Page-Limit-Clamped header")
    effective = _require_int(page["limit"], "page.limit")
    if effective >= 100000:
        raise CheckFailure(f"limit was not clamped: page.limit={effective}")
    if page["returned"] > effective:
        raise CheckFailure(f"returned {page['returned']} exceeds the clamped limit {effective}")
    return (
        f"limit=100000 -> page.limit={effective}, X-Page-Limit-Clamped={clamped_header!r}, "
        f"HTTP 200 (not 422)"
    )


# --------------------------------------------------------------------------- #
# 7 — single fetch + 404
# --------------------------------------------------------------------------- #
def check_single_fetch() -> str:
    """7. A known id round-trips; an unknown one is a 404 with a detail and a correlation id.

    ``detail`` is the *only* body field asserted here, and that is the contract rather than a
    concession: ``ErrorBody.code`` and ``ErrorBody.request_id`` are both optional, handlers
    raise a plain ``HTTPException(detail=...)``, and the correlation id reaches the client
    through the ``X-Request-ID`` header that ``RequestContextMiddleware`` stamps on **every**
    response. Demanding it in the body too would be demanding the id be produced in two places,
    which is exactly the duplication ``src/api/v1.py`` documents itself as avoiding. So the
    correlation half of this check is asserted where the id actually lives — on the header.
    """
    sample_id = STATE.get("sample_id")
    if not sample_id:
        raise CheckFailure("no sample id captured (check 5 did not run)")
    entry = body_of(expect("GET", f"{API}/logs/{sample_id}", 200, username="admin"))
    if entry.get("id") != sample_id:
        raise CheckFailure(f"GET /logs/{sample_id} returned id {entry.get('id')!r}")
    for field in ("ts", "level", "service", "host", "message"):
        if field not in entry:
            raise CheckFailure(f"entry is missing {field!r}: {entry!r}"[:300])

    bogus = f"no-such-entry-{uuid.uuid4().hex}"
    correlation = f"e2e-404-{uuid.uuid4().hex[:12]}"
    missing = expect(
        "GET",
        f"{API}/logs/{bogus}",
        404,
        username="admin",
        headers={"X-Request-ID": correlation},
    )
    err = body_of(missing)
    detail = err.get("detail")
    if not isinstance(detail, str) or not detail:
        raise CheckFailure(f"404 body has no usable detail: {err!r}")
    if missing.headers.get("X-Request-ID") != correlation:
        raise CheckFailure(
            f"the 404 did not echo X-Request-ID: sent {correlation!r}, got "
            f"{missing.headers.get('X-Request-ID')!r} — the error path lost the correlation id"
        )
    return (
        f"id {sample_id} round-tripped; unknown id -> 404 detail={detail[:60]!r} "
        f"with X-Request-ID echoed"
    )


# --------------------------------------------------------------------------- #
# 8 — advanced search vs GET /logs vs generated ground truth
# --------------------------------------------------------------------------- #
def check_search_agrees_with_ground_truth() -> str:
    """8. Search is analyst-gated, and its counts agree with both the REST filter and the oracle.

    The oracle is :func:`src.generators.expected_counts` over a **regenerated** corpus of the
    size measured in check 5. Regeneration is legitimate because the generator is a pure
    function of ``(seed, index)``: the same seed produces the same ids, levels and services the
    api seeded itself with at startup. Grading against the API's own answer would only prove
    it is self-consistent.
    """
    forbidden = expect(
        "POST", f"{API}/logs/search", 403, username="viewer", json={"filter": None, "limit": 1}
    )
    detail = body_of(forbidden).get("detail", "")
    # A 403 is metered-but-not-consumed, so unlike a 401 it *does* carry the triple.
    absent = [h for h in _RATELIMIT_HEADERS if h not in forbidden.headers]
    if absent:
        raise CheckFailure(f"403 is missing {absent} (a 403 has a principal, so it must report)")

    # Precondition for the oracle, asserted rather than assumed. `generate_entries(n)` is the
    # first `n` draws of the seeded sequence, so it only describes the store while the store
    # still holds that *prefix*. One eviction and the resident corpus becomes a suffix, the
    # regenerated tally silently stops matching, and the failure would read as a search bug.
    ingest = body_of(expect("GET", f"{API}/stats", 200, username="admin"))["ingest"]
    total = STATE["corpus_total"]
    if ingest["evicted"] != 0 or ingest["entries_total"] != total:
        raise CheckFailure(
            f"the oracle needs an un-evicted, freshly-seeded store: entries_total="
            f"{ingest['entries_total']}, evicted={ingest['evicted']}, resident total={total}. "
            "Raise STORE_CAPACITY above SEED_ENTRIES, or run this against a fresh stack."
        )
    entries = generate_entries(total, seed=DEFAULT_SEED)
    truth = expected_counts(entries)

    def search_total(tree: dict[str, Any] | None) -> int:
        body = search_logs({"filter": tree, "limit": 1}, username="analyst")
        return _require_int(body["page"]["total"], "search page.total")

    # (a) single-level filter — expressible three ways, so all three must agree.
    error_search = search_total({"field": "level", "op": "eq", "value": "ERROR"})
    error_rest = _require_int(list_logs({"level": "ERROR", "limit": 1})["page"]["total"], "total")
    error_truth = truth.by_level.get("ERROR", 0)
    if not error_search == error_rest == error_truth:
        raise CheckFailure(
            f"level=ERROR disagrees: search={error_search}, GET /logs={error_rest}, "
            f"generator ground truth={error_truth}"
        )

    # (b) single-service filter — same three-way agreement on a different dimension.
    svc = "auth-svc"
    svc_search = search_total({"field": "service", "op": "eq", "value": svc})
    svc_rest = _require_int(list_logs({"service": svc, "limit": 1})["page"]["total"], "total")
    svc_truth = truth.by_service.get(svc, 0)
    if not svc_search == svc_rest == svc_truth:
        raise CheckFailure(
            f"service={svc} disagrees: search={svc_search}, GET /logs={svc_rest}, "
            f"generator ground truth={svc_truth}"
        )

    # (c) a compound tree GET /logs cannot express at all (`not` has no query-param spelling).
    #     Graded straight off the generated corpus, which is the whole reason the oracle exists.
    compound = {
        "all": [
            {"field": "level", "op": "in", "value": ["ERROR", "FATAL"]},
            {"not": {"field": "service", "op": "eq", "value": "search-svc"}},
        ]
    }
    compound_search = search_total(compound)
    compound_truth = sum(
        1
        for entry in entries
        if entry.level.value in {"ERROR", "FATAL"} and entry.service != "search-svc"
    )
    if compound_search != compound_truth:
        raise CheckFailure(
            f"compound all/in/not tree: search={compound_search}, "
            f"generator ground truth={compound_truth}"
        )

    if not detail:
        raise CheckFailure("the viewer's 403 carried no detail")
    return (
        f"viewer 403, analyst 200; ERROR={error_truth}, {svc}={svc_truth}, "
        f"compound={compound_truth} — search == GET /logs == generator oracle over n={total}"
    )


# --------------------------------------------------------------------------- #
# 9 — SSE delivers a post-connect append
# --------------------------------------------------------------------------- #
async def _next_frame(frames: Any, timeout: float, what: str) -> Any:
    """Await the next SSE frame, or fail the check on timeout / early close.

    ``anext(frames, None)`` rather than the one-argument form on purpose: exhaustion then
    surfaces as a sentinel instead of a ``StopAsyncIteration`` thrown across the
    :func:`asyncio.wait_for` task boundary, where an in-flight iteration protocol exception is
    exactly the sort of thing that turns into a confusing ``RuntimeError`` instead of a clear
    "the stream closed early".
    """
    try:
        frame = await asyncio.wait_for(anext(frames, None), timeout=timeout)
    except TimeoutError as exc:
        raise CheckFailure(f"{what}: no frame within {timeout:.1f}s") from exc
    if frame is None:
        raise CheckFailure(f"{what}: the stream closed before the frame arrived")
    return frame


async def _sse_marker_probe(marker: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Open a filtered stream, append a marked entry once it is READY, return (frame, ready).

    The ``ready`` frame is the go-signal. Appending before it arrives would race the
    subscription registration and lose the entry through no fault of the server, so this never
    sleeps — the protocol already provides the deterministic edge to synchronise on.
    """
    params = {"service": _SSE_PROBE_SERVICE, "q": marker, "max_events": 1}
    # read=None: the stream is long-lived by design, so the only deadline that may fire is the
    # explicit asyncio one below. An httpx read timeout here would be an arbitrary second gate.
    timeout = httpx.Timeout(SSE_TIMEOUT + 30.0, read=None)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        async with aconnect_sse(
            client, "GET", f"{API}/logs/stream", params=params, headers=auth("analyst")
        ) as source:
            if source.response.status_code != 200:
                await source.response.aread()
                raise CheckFailure(
                    f"GET /logs/stream -> HTTP {source.response.status_code}: "
                    f"{source.response.text[:200]}"
                )
            frames = source.aiter_sse()
            first = await _next_frame(frames, SSE_TIMEOUT, "ready frame")
            if first.event != "ready":
                raise CheckFailure(f"first frame is {first.event!r} (want 'ready')")
            ready = json.loads(first.data)

            created = append_entry(f"{marker} e2e sse probe", service=_SSE_PROBE_SERVICE)

            deadline = time.monotonic() + SSE_TIMEOUT
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise CheckFailure(f"marker {marker!r} did not arrive in {SSE_TIMEOUT:.0f}s")
                frame = await _next_frame(frames, remaining, f"marker {marker!r}")
                if frame.event != "log":
                    continue
                payload = json.loads(frame.data)
                if payload.get("id") == created["id"]:
                    return payload, ready


def check_sse_post_connect_append() -> str:
    """9. A stream opened before an append receives it, and the frame is the canonical entry."""
    marker = f"e2e-marker-{uuid.uuid4().hex[:12]}"
    frame, ready = asyncio.run(_sse_marker_probe(marker))
    if ready.get("replayed") != 0 or ready.get("resumed_from") is not None:
        raise CheckFailure(f"a fresh (non-resuming) connect replayed something: {ready!r}")

    # The stream and the REST route must serve the same entry JSON — one parser has to serve
    # both. Worth knowing why this could plausibly drift: the SSE frame is produced by
    # `entry.model_dump_json()` (pydantic-core) while the REST body goes out through
    # `ORJSONResponse` (orjson). They agree only because `LogEntry` serialises `ts` in BOTH dump
    # modes rather than relying on the encoder — so this assertion is the regression test for
    # that decision, and a failure here names the field that broke it.
    canonical = body_of(expect("GET", f"{API}/logs/{frame['id']}", 200, username="admin"))
    if frame != canonical:
        differing = sorted(
            set(frame) | set(canonical),
            key=str,
        )
        diffs = [
            f"{key}: sse={frame.get(key)!r} rest={canonical.get(key)!r}"
            for key in differing
            if frame.get(key) != canonical.get(key)
        ]
        raise CheckFailure(
            f"SSE frame != GET /logs/{frame['id']} on {len(diffs)} field(s): " + "; ".join(diffs)
        )
    return f"marker {marker} arrived on the tail; frame == GET /logs/{frame['id']}"


# --------------------------------------------------------------------------- #
# 10 — the concurrent-stream cap
# --------------------------------------------------------------------------- #
def _subscribers() -> int:
    """The server's live SSE subscription count, straight off the admin probe."""
    body = body_of(expect("GET", f"{API}/debug/memory", 200, username="admin"))
    return _require_int(body["subscribers"], "subscribers")


def _wait_for_subscribers(target: int, timeout: float = 10.0) -> int:
    """Poll ``/debug/memory`` until ``subscribers <= target``; return the last reading.

    A disconnect is observed by the server when its side of the socket unwinds, which is not
    instantaneous. Polling the server's own counter is still a *signal* rather than a sleep: it
    returns the moment the slot is actually released, and its timeout is a failure, not a guess.
    """
    deadline = time.monotonic() + timeout
    count = _subscribers()
    while count > target and time.monotonic() < deadline:
        time.sleep(0.2)
        count = _subscribers()
    return count


async def _open_streams(count: int, username: str) -> tuple[int, str]:
    """Hold ``count`` streams open for ``username`` and return the (status, body) of one more."""
    timeout = httpx.Timeout(SSE_TIMEOUT + 30.0, read=None)
    params = {"service": f"{_SSE_PROBE_SERVICE}-cap"}
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        held: list[Any] = []
        try:
            for index in range(count):
                ctx = client.stream(
                    "GET", f"{API}/logs/stream", params=params, headers=auth(username)
                )
                resp = await ctx.__aenter__()
                held.append((ctx, resp))
                if resp.status_code != 200:
                    await resp.aread()
                    raise CheckFailure(
                        f"stream #{index + 1} of {count} (under the cap) -> HTTP "
                        f"{resp.status_code}: {resp.text[:200]}"
                    )
            # The one over the cap.
            async with client.stream(
                "GET", f"{API}/logs/stream", params=params, headers=auth(username)
            ) as over:
                await over.aread()
                return over.status_code, over.text
        finally:
            for ctx, _resp in reversed(held):
                await ctx.__aexit__(None, None, None)


def check_stream_cap() -> str:
    """10. One connect past the cap is a stream-specific 429, not the rate limiter's."""
    before = _wait_for_subscribers(0)
    if before != 0:
        raise CheckFailure(
            f"{before} subscriber(s) still registered before the cap probe; "
            "an earlier check leaked a stream and the cap test would be measuring that"
        )
    status, text = asyncio.run(_open_streams(MAX_STREAMS, "analyst"))
    if status != 429:
        raise CheckFailure(
            f"connect #{MAX_STREAMS + 1} -> HTTP {status} (want 429): {text[:200]}"
        )
    try:
        detail = json.loads(text).get("detail", "")
    except ValueError:
        detail = text
    if _STREAM_429_DETAIL not in detail:
        raise CheckFailure(f"429 detail is {detail!r}, want the stream-cap wording")
    if _RATE_429_DETAIL in detail:
        raise CheckFailure(
            f"the stream cap answered with the RATE-LIMIT detail ({detail!r}); the two 429s "
            "meter different resources and must stay distinguishable"
        )
    _wait_for_subscribers(0)
    return f"{MAX_STREAMS} held, #{MAX_STREAMS + 1} -> 429 {detail!r}"


# --------------------------------------------------------------------------- #
# 11 — Last-Event-ID resume
# --------------------------------------------------------------------------- #
async def _resume_probe() -> tuple[dict[str, Any], list[str], list[str]]:
    """Stream one entry, disconnect, append two more, resume; return (ready, got_ids, want_ids)."""
    timeout = httpx.Timeout(SSE_TIMEOUT + 30.0, read=None)
    params = {"service": _SSE_RESUME_SERVICE}
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        # Leg 1: connect, append A, take A's frame (and its `id:`, which is the store seq).
        async with aconnect_sse(
            client,
            "GET",
            f"{API}/logs/stream",
            params={**params, "max_events": 1},
            headers=auth("analyst"),
        ) as source:
            frames = source.aiter_sse()
            first = await _next_frame(frames, SSE_TIMEOUT, "ready frame (leg 1)")
            if first.event != "ready":
                raise CheckFailure(f"first frame is {first.event!r} (want 'ready')")
            entry_a = append_entry("resume anchor A", service=_SSE_RESUME_SERVICE)
            frame_a = await _next_frame(frames, SSE_TIMEOUT, "anchor A's log frame")
            if frame_a.event != "log" or json.loads(frame_a.data)["id"] != entry_a["id"]:
                raise CheckFailure(f"expected A's log frame, got {frame_a.event!r}")
            anchor = frame_a.id
            if not anchor:
                raise CheckFailure("the log frame carried no `id:` field to resume from")

        # The gap: appended while nothing is listening, so only a resume can deliver it.
        entry_b = append_entry("resume gap B", service=_SSE_RESUME_SERVICE)
        entry_c = append_entry("resume gap C", service=_SSE_RESUME_SERVICE)

        # Leg 2: reconnect from A's id. B and C are already stored, so the replay is computed at
        # connect time — `ready.replayed` states the answer before a single frame is read.
        async with aconnect_sse(
            client,
            "GET",
            f"{API}/logs/stream",
            params={**params, "max_events": 2},
            headers={**auth("analyst"), "Last-Event-ID": anchor},
        ) as source:
            frames = source.aiter_sse()
            first = await _next_frame(frames, SSE_TIMEOUT, "ready frame (resume)")
            if first.event != "ready":
                raise CheckFailure(f"first frame on resume is {first.event!r} (want 'ready')")
            ready = json.loads(first.data)
            got: list[str] = []
            for index in range(2):
                frame = await _next_frame(frames, SSE_TIMEOUT, f"replay frame {index + 1}/2")
                if frame.event != "log":
                    raise CheckFailure(f"replay frame is {frame.event!r} (want 'log')")
                got.append(json.loads(frame.data)["id"])
            return ready, got, [entry_b["id"], entry_c["id"]]


def check_resume() -> str:
    """11. Reconnecting with Last-Event-ID delivers exactly the gap — no repeats, no losses."""
    ready, got, want = asyncio.run(_resume_probe())
    if ready.get("resumed_from") is None:
        raise CheckFailure(f"resume connect reported resumed_from=None: {ready!r}")
    if ready.get("replayed") != 2:
        raise CheckFailure(f"ready says replayed={ready.get('replayed')} (want 2): {ready!r}")
    if ready.get("truncated"):
        raise CheckFailure(f"a 2-entry gap was reported truncated: {ready!r}")
    if got != want:
        raise CheckFailure(f"replay delivered {got} (want exactly {want}, in order)")
    _wait_for_subscribers(0)
    return f"resumed_from={ready['resumed_from']}, replayed 2, ids match, anchor not repeated"


# --------------------------------------------------------------------------- #
# 12 — stats agree with the list route
# --------------------------------------------------------------------------- #
def check_stats_agree() -> str:
    """12. For the same filter, /stats total == /logs page.total, and by_level sums to total."""
    filters: list[dict[str, Any]] = [
        {},
        {"level": "ERROR"},
        {"service": "auth-svc"},
        {"level": ["ERROR", "FATAL"]},
    ]
    checked = []
    for flt in filters:
        stats = body_of(
            expect("GET", f"{API}/stats", 200, username="admin", params={**flt, "bucket_sec": 60})
        )
        listed = list_logs({**flt, "limit": 1})["page"]["total"]
        stats_total = _require_int(stats["total"], "stats.total")
        if stats_total != listed:
            raise CheckFailure(
                f"filter {flt or '(none)'}: /stats total={stats_total} but "
                f"/logs page.total={listed}"
            )
        by_level_sum = sum(stats["by_level"].values())
        if by_level_sum != stats_total:
            raise CheckFailure(
                f"filter {flt or '(none)'}: by_level sums to {by_level_sum}, total={stats_total}"
            )
        for key in ("by_service", "buckets", "top_errors", "window", "ingest"):
            if key not in stats:
                raise CheckFailure(f"/stats is missing {key!r}: {sorted(stats)}")
        checked.append(f"{flt or 'unfiltered'}={stats_total}")
    return f"{len(filters)} filters agree with /logs and sum by level: " + ", ".join(checked)


# --------------------------------------------------------------------------- #
# 13 — RBAC on the operational probe
# --------------------------------------------------------------------------- #
def check_debug_rbac() -> str:
    """13. /debug/memory is admin-only: the writer gets a 403 that still reports its bucket."""
    forbidden = expect("GET", f"{API}/debug/memory", 403, username="writer")
    absent = [h for h in _RATELIMIT_HEADERS if h not in forbidden.headers]
    if absent:
        raise CheckFailure(f"the writer's 403 is missing {absent}")
    detail = body_of(forbidden).get("detail", "")
    if not detail:
        raise CheckFailure("the writer's 403 carried no detail")

    body = body_of(expect("GET", f"{API}/debug/memory", 200, username="admin"))
    missing = _MEMORY_KEYS - set(body)
    if missing:
        raise CheckFailure(f"/debug/memory is missing {sorted(missing)} (has {sorted(body)})")
    return (
        f"writer 403 (with X-RateLimit-*), admin 200 with all six fields "
        f"(memory_mb={body['memory_mb']}, entries={body['entries']})"
    )


# --------------------------------------------------------------------------- #
# 14 — the rate limiter is reachable
# --------------------------------------------------------------------------- #
def check_rate_limit() -> str:
    """14. The free-tier viewer can provably exhaust its bucket, and the 429 says how to recover.

    The viewer is on ``free`` (burst 20, refill 10/s) precisely so this is a handful of requests
    rather than a load test. A 429 that carried no ``Retry-After`` would be telling a client to
    back off without saying how long, which is the same as telling it to spin.
    """
    statuses: list[int] = []
    first_429: httpx.Response | None = None
    for _ in range(RATE_LIMIT_PROBE):
        resp = request("GET", f"{API}/logs", username="viewer", params={"limit": 1})
        statuses.append(resp.status_code)
        if resp.status_code == 429 and first_429 is None:
            first_429 = resp
    limited = statuses.count(429)
    if first_429 is None:
        raise CheckFailure(
            f"{RATE_LIMIT_PROBE} viewer requests produced no 429 "
            f"(statuses seen: {sorted(set(statuses))}) — is RATE_LIMIT_ENABLED off?"
        )
    retry_after = first_429.headers.get("Retry-After")
    if retry_after is None:
        raise CheckFailure("the 429 carries no Retry-After")
    try:
        delay = float(retry_after)
    except ValueError as exc:
        raise CheckFailure(f"Retry-After is {retry_after!r} (want delay-seconds)") from exc
    if delay < 1:
        raise CheckFailure(f"Retry-After is {delay} (< 1s is an invitation to spin)")
    remaining = first_429.headers.get("X-RateLimit-Remaining")
    if remaining != "0":
        raise CheckFailure(f"the 429 reports X-RateLimit-Remaining={remaining!r} (want '0')")
    detail = body_of(first_429).get("detail", "")
    if _RATE_429_DETAIL not in detail:
        raise CheckFailure(f"the 429 detail is {detail!r}, want the rate-limit wording")
    return (
        f"{limited}/{RATE_LIMIT_PROBE} viewer requests were 429; "
        f"Retry-After={delay:.0f}s, X-RateLimit-Remaining=0"
    )


# --------------------------------------------------------------------------- #
# 15 — latency, memory, and the documentation surface
# --------------------------------------------------------------------------- #
def check_perf_and_docs() -> str:
    """15. Sequential read p95, backend RSS and the OpenAPI surface all inside their gates.

    Timed as the **admin** so the samples measure the API rather than the free tier's bucket,
    and over ``GET /logs`` only: ``POST /auth/token`` is excluded from every latency number in
    this file because bcrypt at 12 rounds costs ~250 ms *on purpose*.
    """
    samples_ms: list[float] = []
    for _ in range(LATENCY_SAMPLES):
        started = time.perf_counter()
        expect("GET", f"{API}/logs", 200, username="admin", params={"limit": 50})
        samples_ms.append((time.perf_counter() - started) * 1000.0)
    p50 = _percentile(samples_ms, 50)
    p95 = _percentile(samples_ms, 95)
    if p95 > MAX_P95_MS:
        raise CheckFailure(
            f"GET /logs p95 {p95:.1f}ms > gate {MAX_P95_MS:.0f}ms over n={LATENCY_SAMPLES}"
        )

    memory = body_of(expect("GET", f"{API}/debug/memory", 200, username="admin"))
    memory_mb = memory["memory_mb"]
    if not isinstance(memory_mb, (int, float)) or isinstance(memory_mb, bool):
        raise CheckFailure(f"memory_mb is {memory_mb!r} (want a number)")
    if float(memory_mb) > MAX_BACKEND_MEM_MB:
        raise CheckFailure(
            f"backend RSS {float(memory_mb):.1f} MB > gate {MAX_BACKEND_MEM_MB:.0f} MB"
        )

    for path in ("/openapi.json", "/docs", "/redoc"):
        resp = request("GET", path)
        if resp.status_code != 200:
            raise CheckFailure(f"GET {path} -> HTTP {resp.status_code}")

    return (
        f"p50 {p50:.1f}ms / p95 {p95:.1f}ms <= {MAX_P95_MS:.0f}ms over n={LATENCY_SAMPLES}; "
        f"RSS {float(memory_mb):.1f} MB <= {MAX_BACKEND_MEM_MB:.0f} MB; "
        f"openapi/docs/redoc all 200"
    )


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def main() -> None:
    info(f"== Log Query API (REST) black-box verifier vs {BASE_URL} ==")
    info(
        f"gates: cursor walk at limit {PAGE_LIMIT}; SSE within {SSE_TIMEOUT:.0f}s; "
        f"{RATE_LIMIT_PROBE} rate-limit probes; p95 <= {MAX_P95_MS:.0f}ms over "
        f"{LATENCY_SAMPLES} samples; RSS <= {MAX_BACKEND_MEM_MB:.0f} MB; "
        f"SSE cap = {MAX_STREAMS}"
    )
    wait_ready()

    check("health shape + X-Request-ID mint/echo", check_health)
    check("token issue for all four demo users", check_token_issue)
    check("401 surface + WWW-Authenticate", check_unauthenticated)
    check("list envelope + rate-limit headers", check_list_envelope)
    check("cursor walk covers the corpus exactly once", check_cursor_walk)
    check("over-large limit is clamped, not rejected", check_limit_clamped)
    check("single fetch round-trip + 404 envelope", check_single_fetch)
    check("search RBAC + agreement with ground truth", check_search_agrees_with_ground_truth)
    check("SSE delivers a post-connect append", check_sse_post_connect_append)
    check("SSE concurrent-stream cap is its own 429", check_stream_cap)
    check("Last-Event-ID resume delivers the gap once", check_resume)
    check("stats agree with the list route", check_stats_agree)
    check("debug/memory is admin-only", check_debug_rbac)
    check("free-tier rate limit is reachable", check_rate_limit)
    check("read p95 + backend RSS + docs surface", check_perf_and_docs)

    print(f"E2E PASSED ({TOTAL_CHECKS}/{TOTAL_CHECKS})", flush=True)


if __name__ == "__main__":
    main()

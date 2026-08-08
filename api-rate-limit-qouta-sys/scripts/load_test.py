"""Performance + load gates for the API Rate Limiter & Quota Manager (C14).

Runs **inside Docker** (the profile-gated ``loadtest`` compose service) against the LIVE stack over
HTTP only, reached by SERVICE NAME over the compose network rather than by a published host port:

* ``TARGET_URL=http://lb:80``            — through nginx, i.e. across **both** replicas.
* ``DIRECT_URL=http://api1:8000``        — ONE replica, for the single-replica leg of phase D and
  for the metered leg of phase C.
* ``BASELINE_URL=http://api-baseline:8000`` — the SAME image and the SAME environment as ``api1``
  with ``RATE_LIMIT_ENABLED=false``, and nothing else changed. Phase C's baseline.
* ``LOAD_REPLICA_URLS=http://api1:8000,http://api2:8000`` — every real replica, addressed
  individually, because phase E's question is "did **a** replica grow?" and an average hides the
  one that did.

Where :mod:`scripts.verify_e2e` answers "are the answers CORRECT?", this answers "what does the
enforcement layer COST, and does it still cost that under load?" — five fail-fast phases, the first
breached gate exiting non-zero so ``make load`` propagates it. Every measured number and every gate
value is printed as a machine-readable ``RESULT key=value`` line **before** the gate that judges it,
so a failing run still shows what it measured and C16's README table is transcribed from real output
rather than retyped from memory.

Three things are established **before** any phase runs, because each of them can make every number
below wrong while leaving the run looking perfectly healthy: that the LB is proxying to the two
replicas and **not** to the unmetered baseline (:func:`assert_lb_fans_out` — this failed for real,
see its docstring), that the baseline replica really is running with enforcement off while ``api1``
really is running with it on (:func:`_assert_baseline_is_unmetered`), and that every target is warm
(:func:`warm_up`).

.. rubric:: The phases

**A — the spec's literal test.** ``LOAD_SPEC_REQUESTS`` (100) requests fired in parallel as one
free-tier principal, printing ``Successful: <n>, Rate Limited: <n>`` — the project brief names that
output format literally, so it is matched literally, bare, with no ``[load]`` prefix. Gates: every
reply is a 200 or a 429 (**no third status**), no 5xx, and at least one 429. That last one is not
decoration: 100 requests against a 60-token bucket that produced zero refusals did not exercise the
limiter at all, and printing ``Successful: 100, Rate Limited: 0`` as a PASS would certify the
opposite of what was tested.

**B — throughput.** ``LOAD_REQUESTS`` (20000) over ``LOAD_CONCURRENCY`` (32) keep-alive
connections through the LB, spread across a pool of fresh **enterprise** principals so the limiter
admits every one of them. Gate ``rps >= LOAD_MIN_RPS``. See the throughput rubric below for what the
number is and how the gate was chosen — the honest version, not the flattering one.

**C — limiter overhead, against a real ``RATE_LIMIT_ENABLED=false`` baseline.** Paired, interleaved,
sequential samples of ``GET /api/v1/whoami`` on ``api1`` (enforcement on) and on ``api-baseline``
(enforcement off): the same route, the same image, the same handler, the same one network hop,
differing in exactly one environment variable. Gate p95 delta ``<= LOAD_MAX_OVERHEAD_MS``.

**D — scaling.** Phase-B-style load against ``DIRECT_URL`` (one replica) and then through the LB
(two), back to back. Gate ``rps_2 / rps_1 >= LOAD_MIN_SCALING``. See the scaling rubric — a healthy
result here is ~1.5x, **not** 2.0x, and the reason is architectural rather than a shortfall.

**E — memory.** RSS from ``GET /api/v1/admin/debug/memory`` on **each** replica, before any load and
after all of it. Gates: absolute ``MAX_BACKEND_MEM_MB`` and growth ``MAX_MEM_GROWTH_MB``, both taken
over the worst replica.

.. rubric:: Phase C is the gap C13 left open, and this is what closing it required

``verify_e2e.py`` check 14 measures the limiter's cost as *metered p95 minus the p95 of an exempt
path* (``GET /docs``). Its own docstring lists why that is a lower bound rather than a measurement:
the two requests run different handlers, so the residual includes whatever those two handlers differ
by. The clean experiment needs the **same route** with enforcement switched off, which needs a second
process — so there is one, ``api-baseline``, profile-gated behind ``loadtest`` and never started by
``docker compose up``.

That makes this a true ``RATE_LIMIT_ENABLED=false`` baseline. It is worth being precise about what
the switch removes, because that IS the quantity being reported (see
:class:`src.middleware.RateLimitMiddleware`): with it off the middleware classifies the endpoint and
returns, so no cost is looked up, **no principal is resolved**, no decision script runs, no analytics
record is written and no headers are emitted. So the delta is the whole enforcement layer —
identity + decision + analytics — and not the Lua round trip alone. That is the number the spec's
"<5ms latency overhead for a rate limit check" is asking about, since it is what a caller actually
pays.

Two residuals stated rather than buried:

1. ``api1`` and ``api-baseline`` are two **processes**. They share a host, a Redis and an image, and
   the samples are interleaved (and the order within each pair alternated) so both see the same host
   conditions — but they are not the same process, and a per-process scheduling artefact is not
   subtracted out.
2. The gate is on a **difference of p95s**. The percentile of the *paired* differences is a
   different statistic; it is computed and printed too (``overhead_paired_p*``), it is simply not
   what the gate reads, because at millisecond scale it is dominated by which of the two containers
   the scheduler happened to favour on that iteration and would flap.

.. rubric:: Throughput: what was measured, versus the 1,000 rps the spec asks for

The brief asks for "1,000+ requests/second throughput". **Measured on the calibration host — a
6-core laptop running the whole stack and this generator inside Docker Desktop's VM — the LB leg
serves ~5,500-7,400 req/s and one replica ~3,600-4,700 req/s**, so the requirement is met roughly
six times over. Read the ``RESULT`` lines rather than this paragraph; they are printed from the run
in front of you and this sentence is from the one in front of the author.

``LOAD_MIN_RPS`` nonetheless defaults to **3000**, not 6000 and not the spec's 1000, and the
distinction is worth stating because all three numbers are defensible and they mean different
things:

* **1000** is the *requirement*. Printed as ``RESULT spec_target_rps`` and answered by
  ``RESULT throughput_meets_spec_target=<true|false>``, never gated on — a gate that restates the
  requirement tells you nothing the requirement did not.
* **~6000** is the *measurement*, and gating near it would fail this build every time another
  process on the machine wanted a core.
* **3000** is the *regression bar*: roughly half the observed floor, so a change that cost this
  service half its throughput fails the run, and a busy laptop does not.

Tuning the gate up to look impressive would fail clean runs; tuning it down without saying so would
hide a shortfall. It is printed (``RESULT gate_min_rps``) beside the other two so nobody has to
guess which number is which.

.. rubric:: The load generator must not be the thing being measured, and at first it was

Calibration run 1 of this file reported ``phase_b_rps=569.7`` with ``client_cpu_util=1.00``. Run 2,
sharded across three processes, reported ``1144.1`` at ``2.87`` — and a phase D **scaling factor of
0.94x**, because a generator that is already saturated cannot push harder at two replicas than at
one. Both numbers were measurements of ``httpx``, which costs **2.41 ms of client CPU per request**
at concurrency 100. The service was never the constraint.

So the throughput phases (B and D) use :func:`_raw_fire`, a minimal keep-alive HTTP/1.1 client that
speaks only the narrow dialect this one route answers in: **0.05 ms of client CPU per request**,
~60x cheaper, generator idling at ~0.2 of a core while the SERVICE sets the number. Phase A keeps
httpx, because the spec names httpx and phase A is the spec's test. Phase C keeps httpx too, since
it samples sequentially and subtracts one httpx call from another.

``client_cpu_util`` is then **gated**, not merely printed (``LOAD_MAX_CLIENT_CPU``, default 0.8 of a
core): it must be impossible for this harness to publish its own ceiling as the service's again.
And because a hand-rolled HTTP client is only as trustworthy as its framing, phase B reads one
principal's charged usage back off the admin API and requires it to equal the number of requests the
generator believes it sent — the server's own accounting, from the other side of the network,
grading the client.

.. rubric:: Scaling: 1.5x is what "it scales, and the shared store is the ceiling" looks like

**Two replicas over one Redis cannot reach 2.0x, and a run that reported 2.0x would be evidence the
limiter had stopped working.** Every metered request runs one Lua script on a Redis that executes
scripts on a single thread, one at a time, globally. That serialised section is Amdahl's fraction:
the replicas parallelise the HTTP parsing, the routing, the pydantic serialisation and the response
write, and they queue for the decision. Adding the second replica also adds the nginx hop to the
two-replica leg, which the single-replica leg (addressed directly) does not pay — the comparison is
deliberately of the two *deployments* rather than of two identically-proxied configurations, because
the deployment is what a user gets.

Measured, across twenty-two clean legs spanning four concurrency settings: **1.40 - 1.72x**. At the
shipped ``LOAD_CONCURRENCY=32`` the two arrangements separate, and the separation is itself a
finding: **1.40-1.54x** with the legs run one after the other, **1.56x and 1.61x** once they were
interleaved (:data:`SCALING_CHUNKS`). Sequential legs are not merely noisier — they are *biased*,
because the single-replica leg always ran first, in the quiet window right after phase C, which
flatters the denominator. The interleaved figure is what this harness now reports, and it is the
shape the paragraph above predicts, arrived at from the other direction.

``LOAD_MIN_SCALING`` therefore defaults to **1.30** rather than the 1.5 the plan pencilled in before
anything had been measured. 1.5 sits *inside* the observed spread — a sequential run measured 1.404
against it — and would make this build flake on a busy machine, which is the worse failure: a gate
nobody trusts gets raised until it passes, and then it is decoration. 1.30 is below every one of
those twenty-two observations, far above the ~1.0
that "the LB is not fanning out" or "the generator was the bottleneck" produce, and far below the
2.0 that would mean the decision had stopped serialising. A result near 1.5x is the healthy one, and
the measured value is always printed as ``RESULT scaling_factor`` — the gate is a floor under it,
not a substitute for reading it. The gate is on the ratio rather than on absolute rps, so it stays
meaningful on a machine slower or faster than the one it was calibrated on.

.. rubric:: What this file mutates

Only tier ASSIGNMENTS, and only for principals it invented: every phase that needs headroom mints
fresh ``uuid4`` ids and ``PUT``s them onto ``enterprise``. It never re-sizes a tier, so unlike
``verify_e2e.py`` it has nothing to restore and a failed run cannot leave the stack mis-configured.
Fresh ids per phase also mean no phase inherits another's drained bucket — the reason phase D runs
its own two legs instead of reusing phase B's number.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``            base URL through the LOAD BALANCER (default ``http://lb:80``)
* ``DIRECT_URL``            base URL of ONE replica (default ``http://api1:8000``)
* ``BASELINE_URL``          base URL of the ``RATE_LIMIT_ENABLED=false`` replica
* ``LOAD_REPLICA_URLS``     comma-separated base URLs of every real replica (phase E)
* ``LOAD_READY_TIMEOUT``    seconds to wait for ``/health`` on every base URL (default 90)
* ``LOAD_WARMUP_REQUESTS``  unmeasured requests per target before phase A (default 500)
* ``LOAD_SPEC_REQUESTS``    phase A's parallel burst (default 100 — the spec's number)
* ``LOAD_REQUESTS``         phase B request count (default 20000)
* ``LOAD_CONCURRENCY``      keep-alive connections in flight during B and D (default 32)
* ``LOAD_MAX_CLIENT_CPU``   cores the generator may consume before its number is void (default 0.8)
* ``LOAD_MIN_RPS``          throughput gate, requests/second (default 3000)
* ``LOAD_OVERHEAD_SAMPLES`` paired (metered, baseline) samples behind phase C (default 200)
* ``LOAD_MAX_OVERHEAD_MS``  ceiling on the measured p95 overhead, ms (default 5)
* ``LOAD_SCALING_REQUESTS`` requests per leg in phase D (default: ``LOAD_REQUESTS``)
* ``LOAD_MIN_SCALING``      ``rps_2 / rps_1`` gate (default 1.30)
* ``MAX_BACKEND_MEM_MB``    absolute RSS ceiling per replica, MiB (default 150)
* ``MAX_MEM_GROWTH_MB``     RSS growth ceiling per replica across the run, MiB (default 25)

Every gate is host-overridable, which is how we prove they bite rather than decorate::

    LOAD_MIN_RPS=1000000 make load       MUST exit non-zero
    LOAD_MAX_OVERHEAD_MS=0 make load     MUST exit non-zero
    MAX_BACKEND_MEM_MB=1 make load       MUST exit non-zero

Exit 0 with ``LOAD PASSED`` only when all five phases hold; 1 for a breached gate; 2 for anything
unexpected.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Final

import httpx

from src.config import Settings, get_settings
from src.identity import issue_token

# --------------------------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://lb:80").rstrip("/")
DIRECT_URL = os.environ.get("DIRECT_URL", "http://api1:8000").rstrip("/")
BASELINE_URL = os.environ.get("BASELINE_URL", "http://api-baseline:8000").rstrip("/")
REPLICA_URLS: Final[tuple[str, ...]] = tuple(
    url.strip().rstrip("/")
    for url in os.environ.get(
        "LOAD_REPLICA_URLS", "http://api1:8000,http://api2:8000"
    ).split(",")
    if url.strip()
)

READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
WARMUP_REQUESTS = max(0, int(os.environ.get("LOAD_WARMUP_REQUESTS", "500")))
SPEC_REQUESTS = max(1, int(os.environ.get("LOAD_SPEC_REQUESTS", "100")))
# 20000, not the 5000 the plan named. That figure was written when the harness was assumed to run
# at ~1000 req/s, where it is a five-second sample; measured, this stack serves ~6000 req/s through
# the LB, which turns 5000 requests into a 0.8-second window dominated by connection ramp-up. Three
# repeats at 5000 spread 1.51-1.72x on phase D's ratio; the same repeats at 20000 spread 1.47-1.53x
# at the same concurrency. The number went up because the measurement was noise-bound, and 20000 is
# still only ~3 seconds of load through the LB.
LOAD_REQUESTS = max(1, int(os.environ.get("LOAD_REQUESTS", "20000")))
# 32 concurrent connections, not the plan's 100, and the number is not a taste — it is
# REDIS_MAX_CONNECTIONS. Phase D's single-replica leg sends every one of these connections at ONE
# replica, and that replica answers each request out of a pool of 32 with a 50 ms wait budget, so
# offering more than 32 means some request is always waiting for a store connection and the
# measurement starts including that queue. Measured, on the single-replica leg:
#
#   c=100 -> p95 45 ms and 15 x 503 per 8000 requests   (the wait budget breached outright)
#   c=64  -> p95 22 ms and  1 x 503 per 20000 requests  (a coin flip per run — run 5 died here)
#   c=40  -> p95 10 ms, clean across 3 x 20000
#   c=32  -> p95  9 ms, clean across 3 x 20000, and the replica is ALREADY saturated (it serves
#            ~3.7-4.3k req/s at 24, 32, 40 and 64 alike — past ~24 the extra concurrency buys
#            queueing, not throughput)
#
# Those 503s are the limiter refusing rather than admitting unmetered, which is correct behaviour
# and precisely why they must not appear in a throughput number: they are the overload regime.
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "32")))
# The guard that makes a client-bound number impossible to publish. See `gate_clean_measurement`.
MAX_CLIENT_CPU = float(os.environ.get("LOAD_MAX_CLIENT_CPU", "0.8"))
MIN_RPS = float(os.environ.get("LOAD_MIN_RPS", "3000"))
OVERHEAD_SAMPLES = max(1, int(os.environ.get("LOAD_OVERHEAD_SAMPLES", "200")))
MAX_OVERHEAD_MS = float(os.environ.get("LOAD_MAX_OVERHEAD_MS", "5"))
# `or str(LOAD_REQUESTS)` and not a plain default, so that an UNSET *or empty* value follows
# LOAD_REQUESTS. The compose service declares this as `${LOAD_SCALING_REQUESTS:-}` precisely so the
# two counts cannot drift: someone lowering LOAD_REQUESTS to shorten a run would otherwise leave
# phase D firing the original 5000 and wonder why the "quick" run took just as long.
SCALING_REQUESTS = max(1, int(os.environ.get("LOAD_SCALING_REQUESTS") or str(LOAD_REQUESTS)))
MIN_SCALING = float(os.environ.get("LOAD_MIN_SCALING", "1.30"))
#: Phase D fires each leg in this many chunks, alternating legs between them, instead of running one
#: leg to completion and then the other. Both legs then span the SAME window of machine time, so a
#: slow patch (another container, a host process, thermal throttling) lands on both rather than on
#: whichever leg happened to be running. Measured effect: 1.40-1.54x sequentially versus 1.56x and
#: 1.61x interleaved, on the same stack. That gap is not noise — it is the bias, because the
#: single-replica leg always ran first, in the quietest window, which flatters the denominator.
#: Four chunks is enough to average the drift out without paying connection setup per request.
SCALING_CHUNKS: Final = 4
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "150"))
MAX_MEM_GROWTH_MB = float(os.environ.get("MAX_MEM_GROWTH_MB", "25"))

#: The spec's throughput figure. NOT a gate and deliberately not one — see the throughput rubric in
#: the module docstring. It is printed beside the gate so a reader of the ``RESULT`` lines can see
#: what was asked for next to what this hardware delivered, without either number standing in for
#: the other.
SPEC_TARGET_RPS: Final = 1000.0

API: Final = "/api/v1"
ADMIN: Final = f"{API}/admin"

#: The measured route for every phase: cost 1, touches no store, allocates nothing, does no
#: downstream work. A burst against it measures the limiter and the framework, not a handler. Spelled
#: without a trailing slash — a metered path with one is charged TWICE (the 307 plus the redirected
#: request), which would silently halve every throughput number in this file.
PATH_WHOAMI: Final = f"{API}/whoami"

#: The tier phases B, C and D drive as. Enterprise is 1000 rpm / 1000 burst by default, and the pool
#: below is sized off the LIVE table rather than off that assumption.
LOAD_TIER: Final = "enterprise"

#: Fraction of a principal's cold-bucket capacity any one principal is allowed to spend in a phase.
#: At 0.5 a 20000-request phase over a 1000-token tier needs 40 principals and each one finishes with
#: half its bucket untouched, so a slow phase (which refills) and a fast one (which does not) both
#: stay comfortably inside the ceiling. The point is that **no request in phase B or D may be
#: refused**: a 429 in a throughput measurement is a measurement of the limiter's ceiling, not of the
#: server's rate.
PRINCIPAL_BUDGET_FRACTION: Final = 0.5

#: Header the middleware stamps when a request was served by the DEGRADED fallback path (Redis
#: unreachable, or this process could not get a connection out of its own pool within the wait
#: budget). Counted on every measured phase, because a degraded 200 is an **unmetered** 200: it never
#: ran the decision script, so a run carrying them would report the limiter as cheaper and the
#: service as faster than either is. Gated at zero.
HEADER_DEGRADED: Final = "X-RateLimit-Degraded"

_REQUEST_TIMEOUT: Final = 60.0

#: Cross-phase state: the live tier table, the default tier name, the phase-E baseline snapshot.
STATE: dict[str, Any] = {}


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
    """Print one machine-readable summary line.

    Emitted **before** the gate that judges the number, never after, so a breached gate still leaves
    the measurement on stdout. C16 transcribes the README's performance table from these lines.
    """
    print(f"RESULT {key}={value}", flush=True)


def percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); 0.0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


def _load_settings() -> Settings:
    """Build :class:`~src.config.Settings`, or fail the bootstrap loudly.

    Two values are needed out of it and both must be **the same value the replicas were started
    with**: ``JWT_SECRET`` (this harness signs its own throwaway principals with the server's own
    :func:`src.identity.issue_token`) and ``ADMIN_TOKEN`` (it drives tier assignment and reads the
    memory probe). ``Settings`` refuses to construct without ``API_KEY_PEPPER`` as well, so the
    compose service declares all three from the same ``${VAR:-default}`` expressions the ``x-api``
    anchor uses. A mis-wire surfaces here as one readable line instead of as a wall of 401s.
    """
    try:
        return get_settings()
    except Exception as exc:  # noqa: BLE001 — a config failure is a bootstrap failure
        print(
            "FAIL bootstrap: could not build Settings from the environment "
            f"({type(exc).__name__}: {exc}). JWT_SECRET, API_KEY_PEPPER and ADMIN_TOKEN must be "
            "declared on the loadtest compose service with the SAME values the api replicas got.",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(2) from exc


SETTINGS: Final[Settings] = _load_settings()

#: The control-plane client. Through the LB like everything else that is not a deliberate
#: single-replica measurement.
CLIENT: Final = httpx.Client(base_url=BASE_URL, timeout=_REQUEST_TIMEOUT)


# --------------------------------------------------------------------------------------------- #
# Principals
# --------------------------------------------------------------------------------------------- #
def token_for(user_id: str) -> str:
    """Sign a bearer token for ``user_id`` with the server's own issuer."""
    return issue_token(user_id, settings=SETTINGS)


def new_principal(label: str) -> str:
    """A user id nothing has ever seen: empty bucket, zeroed quota, no tier record."""
    return f"load-{label}-{uuid.uuid4().hex[:16]}"


def bearer(user_id: str) -> dict[str, str]:
    """The ``Authorization`` header for a freshly signed token."""
    return {"Authorization": f"Bearer {token_for(user_id)}"}


def admin_json(method: str, path: str, status: int = 200, **kwargs: Any) -> Any:
    """Call the control plane through the LB with the operator token, asserting the status."""
    headers = dict(kwargs.pop("headers", None) or {})
    headers["X-Admin-Token"] = SETTINGS.admin_token
    try:
        resp = CLIENT.request(method, path, headers=headers, **kwargs)
    except Exception as exc:  # noqa: BLE001 — a transport failure here is a bootstrap failure
        raise CheckError(f"{method} {path} raised {type(exc).__name__}: {exc}") from exc
    if resp.status_code != status:
        raise CheckError(
            f"{method} {path} -> HTTP {resp.status_code} (expected {status}): {resp.text[:200]}"
        )
    try:
        return resp.json()
    except ValueError as exc:
        raise CheckError(f"{method} {path} returned non-JSON: {resp.text[:200]}") from exc


def ceiling_of(tier: str) -> int:
    """Cost-1 requests ``tier`` admits from cold, before any refill: ``min(burst, rpm)``.

    Both rate gates are independent and either can bind — the token bucket's capacity is ``burst``,
    the account-wide sliding window's ceiling over 60 seconds is ``rate_limit_per_min`` — so the
    smaller is what a principal can actually spend. Read from the LIVE table so an operator running
    a re-sized ``TIER_LIMITS`` gets a pool sized to *their* numbers rather than a phase full of 429s.
    """
    config = STATE["tiers"].get(tier)
    if config is None:
        raise CheckError(
            f"tier {tier!r} is not in the live table {sorted(STATE['tiers'])}; this harness drives "
            "its measured phases as that tier so the limiter is not what is being measured"
        )
    return min(int(config["burst"]), int(config["rate_limit_per_min"]))


def principal_pool(label: str, total: int, concurrency: int = CONCURRENCY) -> list[str]:
    """Mint and tier-assign enough fresh principals to absorb ``total`` requests without a 429.

    Sized from the LIVE ceiling and :data:`PRINCIPAL_BUDGET_FRACTION`, so the pool grows if someone
    raises ``LOAD_REQUESTS`` and shrinks if someone raises the enterprise tier. The arithmetic is
    plain ``ceil(total / budget)`` only because :func:`_raw_fire` rotates principals **per request**
    rather than per connection — bound per connection, the busiest principal would spend
    ``total / concurrency`` however many were minted, and the pool could not fix it. ``concurrency``
    is taken here so the log line can say how the spend is spread.

    Every id is fresh, so the phase starts against full buckets it can account for entirely; the
    assignment takes effect on the very next request on **every** replica (who is on which tier is
    read inside the decision script, not from a cached snapshot), which is why there is no
    convergence wait here.
    """
    budget = max(1, int(ceiling_of(LOAD_TIER) * PRINCIPAL_BUDGET_FRACTION))
    count = max(1, math.ceil(total / budget))
    principals = [new_principal(label) for _ in range(count)]
    for user_id in principals:
        body = admin_json("PUT", f"{ADMIN}/users/{user_id}/tier", json={"tier": LOAD_TIER})
        if body.get("tier") != LOAD_TIER:
            raise CheckError(f"assigning {user_id!r} to {LOAD_TIER!r} reported {body.get('tier')!r}")
    info(
        f"{label}: {count} fresh {LOAD_TIER} principals for {total} requests over "
        f"{min(concurrency, total)} connections (<= {math.ceil(total / count)} each of "
        f"{ceiling_of(LOAD_TIER)}, budget {budget})"
    )
    return principals


# --------------------------------------------------------------------------------------------- #
# The load engine
#
# Phases A, B and D all need "fire N requests at concurrency C, rotating over a set of principals,
# and tell me the status, the latency and whether the limiter was degraded". It returns a projection
# rather than the responses: holding 5000 `httpx.Response` objects alive to read three fields off
# each is memory a load generator has no business spending while it is also the clock.
# --------------------------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Outcome:
    """What one phase's fire-and-measure run produced."""

    wall_s: float
    #: Generator CPU seconds over the same window. Divided by wall time it reads as CORES CONSUMED
    #: BY THE CLIENT, and near 1.0 it means the load generator — not the service — set the number.
    #: Gated, not merely printed: see `gate_clean_measurement`.
    cpu_s: float
    latencies_ms: list[float]
    statuses: list[int]
    degraded: int
    #: Requests sent per principal, by index into the phase's pool. Empty for the httpx engine.
    #: Phase B grades the server's own charged usage against this, which is what turns "the client
    #: believes it sent 20000 requests" into "the limiter charged for 20000 requests".
    per_principal: tuple[int, ...] | list[int] = ()

    @property
    def total(self) -> int:
        return len(self.statuses)

    @property
    def ok(self) -> int:
        return sum(1 for status in self.statuses if status == 200)

    @property
    def limited(self) -> int:
        return sum(1 for status in self.statuses if status == 429)

    @property
    def other(self) -> list[int]:
        return sorted({s for s in self.statuses if s not in (200, 429)})

    @property
    def other_label(self) -> str:
        """The third-status set as one whitespace-free token, for a ``RESULT`` value.

        ``0`` in here means a transport failure rather than an HTTP status — a request that never
        got an answer. It is reported in the same field because it belongs to the same question
        ("what came back that should not have?"), and it is spelled out in the gate messages.
        """
        return ",".join(str(status) for status in self.other) or "none"

    @property
    def server_errors(self) -> int:
        return sum(1 for status in self.statuses if status >= 500 or status == 0)

    @property
    def rps(self) -> float:
        return (self.ok / self.wall_s) if self.wall_s > 0 else 0.0

    @property
    def cpu_util(self) -> float:
        return (self.cpu_s / self.wall_s) if self.wall_s > 0 else 0.0


async def _fire(
    base_url: str, headers: list[dict[str, str]], total: int, concurrency: int
) -> tuple[list[float], list[int], int]:
    """Fire ``total`` GETs at ``base_url + PATH_WHOAMI`` through httpx, ``concurrency`` in flight.

    Phase A's engine, and only phase A's: the spec's acceptance test says *"100 parallel requests via
    httpx"*, so the test it names is run with the library it names. :func:`_raw_fire` is what the
    throughput phases use, and the rubric there explains why the two cannot be the same function.

    A request that raises is recorded with status ``0`` — every gate treats that as the hard failure
    it is rather than dropping the sample and quietly measuring a smaller, healthier run.
    """
    gate = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    statuses: list[int] = []
    degraded = 0
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(base_url=base_url, timeout=_REQUEST_TIMEOUT, limits=limits) as cli:

        async def one(index: int) -> None:
            nonlocal degraded
            async with gate:
                started = time.perf_counter()
                try:
                    resp = await cli.get(PATH_WHOAMI, headers=headers[index % len(headers)])
                except Exception:  # noqa: BLE001 — any transport failure is a hard error
                    latencies.append((time.perf_counter() - started) * 1000.0)
                    statuses.append(0)
                    return
                latencies.append((time.perf_counter() - started) * 1000.0)
                statuses.append(resp.status_code)
                if HEADER_DEGRADED in resp.headers:
                    degraded += 1

        await asyncio.gather(*(one(i) for i in range(total)))

    return latencies, statuses, degraded


# --------------------------------------------------------------------------------------------- #
# The throughput engine: a minimal keep-alive HTTP/1.1 client
#
# .. rubric:: Why the throughput phases do NOT use httpx, measured rather than asserted
#
# Because httpx costs more CPU per request than the server it is timing. The numbers, from this
# stack:
#
#   * httpx, at concurrency 100: **2.41 ms of client CPU per request**. One process saturates one
#     core at ~570 req/s; three processes saturate three at ~1150 req/s. The first two calibration
#     runs of this harness reported exactly those figures as "throughput" — they were measurements
#     of httpx, and phase D's scaling factor came out at **0.94x** because the generator was the
#     bottleneck in both legs and adding a replica could not move a number the client was setting.
#   * this client, same load: **0.04-0.07 ms of client CPU per request**, ~60x cheaper. The
#     generator sits at 0.1-0.4 of one core and the SERVER becomes the constraint, which is the
#     only condition under which a throughput or scaling number means anything. Same stack, same
#     route, same requests: **~4000 req/s on one replica and ~6300 req/s through the LB**.
#
# That is the whole justification. A load generator has one job — to not be the bottleneck — and
# the cost of the honest measurement here is ~60 lines of HTTP/1.1 that only has to speak the
# narrow dialect this one route answers in. It is deliberately strict rather than tolerant: an
# unexpected framing (a chunked body, a missing Content-Length) raises instead of guessing, because
# a generator that silently mis-parses is a generator that reports whatever number you hoped for.
# --------------------------------------------------------------------------------------------- #
async def _connection(
    host: str,
    port: int,
    requests: list[bytes],
    offset: int,
    count: int,
    out: list[tuple[int, bool, float]],
) -> None:
    """Fire ``count`` sequential requests down ONE keep-alive connection.

    Sequential *per connection* and never pipelined: the next request is written only after the
    previous response has been read to its last body byte, so ``concurrency`` connections mean
    exactly ``concurrency`` requests in flight — the property every latency number here depends on.

    ``requests`` holds one pre-built byte string per principal (the whole request, Authorization
    header included, encoded once before the clock started) and this connection walks them from
    ``offset``, wrapping. Rotating **per request** rather than per connection is what keeps the
    spend even: bound per connection instead, the busiest principal spends ``total / concurrency``
    regardless of how large the pool is, so a 20000-request phase over 32 connections would need a
    principal able to absorb 625 requests no matter how many principals were minted.

    A connection that fails records status ``0`` for every request it did not get to, so the totals
    stay exact and the gates see a transport failure rather than a short run that looks healthy.
    """
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None
    done = 0
    width = len(requests)
    try:
        reader, writer = await asyncio.open_connection(host, port)
        while done < count:
            started = time.perf_counter()
            writer.write(requests[(offset + done) % width])
            await writer.drain()

            status_line = await reader.readline()
            if not status_line:
                raise ConnectionError("the server closed the connection mid-run")
            parts = status_line.split(b" ", 2)
            if len(parts) < 2 or not parts[0].startswith(b"HTTP/1."):
                raise ConnectionError(f"unparseable status line {status_line[:60]!r}")
            status = int(parts[1])

            length = 0
            degraded = False
            close_after = False
            framed = False
            while True:
                header = await reader.readline()
                if header in (b"\r\n", b"\n", b""):
                    break
                lowered = header.lower()
                if lowered.startswith(b"content-length:"):
                    length = int(header.split(b":", 1)[1])
                    framed = True
                elif lowered.startswith(b"transfer-encoding:"):
                    # Never emitted for these JSON responses. Refused rather than guessed: a
                    # generator that mis-frames a body reads the next response's status line out of
                    # this one's payload and reports nonsense with total confidence.
                    raise ConnectionError(
                        "response used Transfer-Encoding; this generator only speaks "
                        "Content-Length framing and will not guess at the rest"
                    )
                elif lowered.startswith(b"x-ratelimit-degraded:"):
                    degraded = True
                elif lowered.startswith(b"connection:") and b"close" in lowered:
                    close_after = True
            if not framed:
                raise ConnectionError("response carried no Content-Length; cannot frame the body")
            if length:
                await reader.readexactly(length)

            out.append((status, degraded, (time.perf_counter() - started) * 1000.0))
            done += 1

            if close_after and done < count:
                writer.close()
                reader, writer = await asyncio.open_connection(host, port)
    except Exception:  # noqa: BLE001 — a broken connection is a measurement failure, not a crash
        for _ in range(count - done):
            out.append((0, False, 0.0))
    finally:
        if writer is not None:
            writer.close()


async def _raw_fire(
    base_url: str, auth: list[str], total: int, concurrency: int
) -> tuple[list[tuple[int, bool, float]], list[int]]:
    """Spread ``total`` requests over ``concurrency`` keep-alive connections.

    Which principal sends which request is fixed before anything is dispatched — connection *i*
    starts at principal *i* and rotates — so the per-principal spend is **deterministic and even**,
    which is what lets phase B cross-check the server's own charged usage against the number of
    requests this generator believes it sent. Returns the per-request outcomes and the exact
    per-principal counts.
    """
    host, _, port = base_url.split("//", 1)[1].partition(":")
    out: list[tuple[int, bool, float]] = []
    shares = _shard(total, concurrency)
    width = len(auth)
    per_principal = [0] * width
    requests = [
        (
            f"GET {PATH_WHOAMI} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Authorization: Bearer {token}\r\n"
            "Accept: application/json\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode()
        for token in auth
    ]
    tasks = []
    for index, share in enumerate(shares):
        if not share:
            continue
        offset = index % width
        for step in range(share):
            per_principal[(offset + step) % width] += 1
        tasks.append(_connection(host, int(port or "80"), requests, offset, share, out))
    await asyncio.gather(*tasks)
    return out, per_principal


def _shard(total: int, buckets: int) -> list[int]:
    """Split ``total`` into ``buckets`` shares that differ by at most one."""
    base, extra = divmod(total, buckets)
    return [base + (1 if index < extra else 0) for index in range(buckets)]


def fire(base_url: str, principals: list[str], total: int, concurrency: int) -> Outcome:
    """Phase A's engine: ``total`` requests through httpx at ``concurrency`` in flight."""
    headers = [bearer(user_id) for user_id in principals]
    cpu0 = time.process_time()
    wall0 = time.perf_counter()
    latencies, statuses, degraded = asyncio.run(_fire(base_url, headers, total, concurrency))
    wall = time.perf_counter() - wall0
    return Outcome(
        wall_s=wall,
        cpu_s=time.process_time() - cpu0,
        latencies_ms=latencies,
        statuses=statuses,
        degraded=degraded,
    )


def drive(base_url: str, principals: list[str], total: int, concurrency: int) -> Outcome:
    """The throughput engine: ``total`` requests over ``concurrency`` keep-alive connections.

    Tokens are signed **before** the clock starts. Wall time spans the first write to the last body
    byte, so ``rps = ok / wall`` is the real end-to-end rate at ``concurrency``.
    """
    auth = [token_for(user_id) for user_id in principals]
    cpu0 = time.process_time()
    wall0 = time.perf_counter()
    out, per_principal = asyncio.run(_raw_fire(base_url, auth, total, concurrency))
    wall = time.perf_counter() - wall0
    return Outcome(
        wall_s=wall,
        cpu_s=time.process_time() - cpu0,
        latencies_ms=[latency for _s, _d, latency in out],
        statuses=[status for status, _d, _lat in out],
        degraded=sum(1 for _s, degraded, _lat in out if degraded),
        per_principal=per_principal,
    )


def _merge(parts: list[Outcome]) -> Outcome:
    """Fold several measured chunks of the SAME leg into one :class:`Outcome`.

    Wall times and CPU times are summed rather than averaged, so ``rps`` stays
    ``requests / seconds-spent-generating`` — the time between chunks (the other leg's turn) is not
    in either total and must not be, or a leg would be charged for the wait while its counterpart
    ran.
    """
    return Outcome(
        wall_s=sum(part.wall_s for part in parts),
        cpu_s=sum(part.cpu_s for part in parts),
        latencies_ms=[value for part in parts for value in part.latencies_ms],
        statuses=[value for part in parts for value in part.statuses],
        degraded=sum(part.degraded for part in parts),
    )


def gate_clean_measurement(name: str, outcome: Outcome) -> None:
    """Refuse to grade a throughput number that was not measuring what it claims to.

    Four ways a phase can produce a fast, wrong number, all of them fatal here:

    * a **non-200** means either a refusal (the limiter's ceiling, not the server's rate) or an
      error, and both make ``rps`` a statistic about something else;
    * a **degraded** response never ran the decision script, so it is an unmetered request wearing a
      200, and a run of them measures the service with its limiter switched off;
    * a **transport failure** (status 0) is a dropped request, which flatters the rate by shrinking
      the numerator's denominator in exactly the wrong direction;
    * a **saturated generator** means the number is this container's ceiling rather than the
      service's. That one is not hypothetical — it is what the first three calibration runs of this
      file actually measured, and it is why the last gate below exists. Without it a harness reports
      its own throughput with a straight face, and reports a scaling factor of 1.0 for a stack that
      scales perfectly well, because a client that is already saturated cannot push harder at a
      second replica than at a first.
    """
    if outcome.other or outcome.limited:
        raise CheckError(
            f"{name}: {outcome.limited} x 429 and statuses {outcome.other_label} in "
            f"{outcome.total} requests. This phase drives {LOAD_TIER} principals precisely so the "
            "limiter admits every request — a refusal here means the pool was under-sized and the "
            "number measured is the limiter's ceiling, not the server's throughput. A 503 instead "
            f"means the replica's Redis pool (REDIS_MAX_CONNECTIONS) could not keep up with "
            f"{CONCURRENCY} concurrent requests, which is the overload regime rather than the "
            "throughput one — lower LOAD_CONCURRENCY."
        )
    if outcome.degraded:
        raise CheckError(
            f"{name}: {outcome.degraded} of {outcome.total} responses carried {HEADER_DEGRADED} — "
            "the store was unreachable or the connection pool was exhausted, so those requests were "
            "served UNMETERED by the fallback path. A throughput number that includes them is a "
            "measurement of this service with its limiter switched off."
        )
    if outcome.cpu_util > MAX_CLIENT_CPU:
        raise CheckError(
            f"{name}: the load generator consumed {outcome.cpu_util:.2f} of a core (gate "
            f"LOAD_MAX_CLIENT_CPU={MAX_CLIENT_CPU:.2f}), so {outcome.rps:.0f} req/s is the "
            "GENERATOR's ceiling and not the service's. Measured for reference: this client costs "
            "~0.05 ms of CPU per request and normally sits near 0.2 of a core at ~6000 req/s. Do "
            "not raise this gate to make the run pass — the number it would then publish is a "
            "number about this container."
        )


# --------------------------------------------------------------------------------------------- #
# Bootstrap: readiness, the live tier table, warm-up
# --------------------------------------------------------------------------------------------- #
def health_of(base_url: str) -> dict[str, Any]:
    """``GET /health`` on one base URL, decoded. Unauthenticated, unmetered, dependency-proof."""
    try:
        with httpx.Client(base_url=base_url, timeout=10.0) as probe:
            resp = probe.get("/health")
    except Exception as exc:  # noqa: BLE001 — a transport failure here is a bootstrap failure
        raise CheckError(f"{base_url}/health raised {type(exc).__name__}: {exc}") from exc
    check(resp.status_code == 200, f"{base_url}/health -> HTTP {resp.status_code}")
    return resp.json()


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll ``/health`` on every base URL this run will touch, or fail the bootstrap.

    Compose already gates this service on ``lb``, both replicas and ``api-baseline`` being *healthy*,
    so this normally returns on the first poll of each. It stays because ``TARGET_URL`` can be
    pointed at something compose does not manage, and because discovering at phase C that the
    baseline replica was never reachable would waste two minutes of load.

    Each URL's answering hostname is recorded in :data:`STATE` — :func:`assert_lb_fans_out` grades
    the proxy against it.
    """
    targets = [("lb", BASE_URL), ("direct", DIRECT_URL), ("baseline", BASELINE_URL)]
    targets += [(f"replica{i + 1}", url) for i, url in enumerate(REPLICA_URLS)]
    hosts: dict[str, str] = {}
    for label, base in targets:
        if base in hosts:
            continue
        deadline = time.time() + timeout
        last = "no response"
        while time.time() < deadline:
            try:
                with httpx.Client(base_url=base, timeout=5.0) as probe:
                    resp = probe.get("/health")
                if resp.status_code == 200:
                    hosts[base] = str(resp.json().get("served_by"))
                    info(f"{label} ready at {base} (served_by={hosts[base]})")
                    break
            except Exception as exc:  # noqa: BLE001 — the service may still be starting
                last = type(exc).__name__
            else:
                last = f"HTTP {resp.status_code}"
            time.sleep(2.0)
        else:
            raise CheckError(f"{label} {base}/health not ready after {timeout:.0f}s (last: {last})")
    STATE["hosts"] = hosts


def assert_lb_fans_out() -> None:
    """Fail the bootstrap unless the LB proxies to exactly the replicas — and never the baseline.

    .. rubric:: This is not paranoia; it is the failure that happened

    nginx resolves the names in its ``upstream`` block **once, at startup**, and never again. So a
    replica that is recreated after the proxy started leaves the proxy holding an address Docker is
    free to hand to some other container — and the container most likely to take it is the one this
    profile adds, ``api-baseline``, because it is created at exactly that moment. Observed, in the
    first run of this harness: every ``/health`` through the LB was answered by the baseline's
    hostname, phase A printed ``Successful: 100, Rate Limited: 0``, and the entire run would have
    been a measurement of this service **with no rate limiter in it** — the fastest, cleanest,
    most completely worthless numbers this file could produce.

    Nothing downstream can detect that on its own: an unmetered replica answers 200 to everything,
    which is what a healthy stack looks like from every angle except the one that counts. So it is
    established here, before any measurement, from three facts the probes already carry — the LB's
    answering hosts, each replica's own hostname, and the baseline's.

    The ``make load`` target is written to prevent the condition (build first, create the baseline
    in the same wave as the replicas, never rebuild under ``run``). This is the assertion that says
    so out loud rather than trusting the Makefile's comment.
    """
    replicas = {STATE["hosts"][url] for url in REPLICA_URLS}
    baseline_host = STATE["hosts"][BASELINE_URL]
    probes = max(4, len(REPLICA_URLS) * 4)
    seen: dict[str, int] = {}
    for _ in range(probes):
        host = str(health_of(BASE_URL).get("served_by"))
        seen[host] = seen.get(host, 0) + 1

    check(
        baseline_host not in seen,
        f"the load balancer is proxying to {baseline_host}, which is the RATE_LIMIT_ENABLED=false "
        f"baseline replica at {BASELINE_URL}. nginx resolves its upstream names once at startup, so "
        "a replica recreated after the proxy started leaves a stale address that this container can "
        "inherit. Every request through the LB would be served UNMETERED and every number in this "
        "run would describe a service with no rate limiter. Recreate the stack with the baseline "
        "brought up alongside the replicas (`make load` does this) rather than after the proxy.",
    )
    unknown = sorted(set(seen) - replicas)
    check(
        not unknown,
        f"the load balancer answered from {unknown}, which is not among the replicas named by "
        f"LOAD_REPLICA_URLS ({sorted(replicas)}). Phase D divides the LB's throughput by one "
        "replica's, so the two legs must be measuring the same processes.",
    )
    check(
        len(seen) == len(replicas),
        f"{probes} probes through the LB were answered by {sorted(seen)} — {len(replicas)} distinct "
        "replicas were expected. Phase D's scaling factor compares one replica against all of them, "
        "and a proxy that is really serving one process would report ~1.0x for a healthy stack.",
    )
    info(
        f"LB fans out over {sorted(seen)} ({probes} probes: "
        + ", ".join(f"{host}x{count}" for host, count in sorted(seen.items()))
        + f"); baseline {baseline_host} is NOT in the pool"
    )


def load_tier_table() -> None:
    """Read the live tier table into :data:`STATE`. Bootstrap.

    Nothing in this file hard-codes a limit: the enterprise ceiling that sizes every principal pool
    and the default tier that phase A relies on both come from ``GET /api/v1/admin/tiers``, which is
    the only source that reflects a tier somebody changed at runtime. The verifier owns the
    live-versus-configured cross-check (:func:`scripts.verify_e2e._assert_live_table_matches_config`)
    and this harness deliberately does not duplicate it — nothing here grades a ceiling, it only
    needs to stay under one.
    """
    table = admin_json("GET", f"{ADMIN}/tiers")
    STATE["tiers"] = table["tiers"]
    STATE["default_tier"] = table["default_tier"]
    shipped = ", ".join(
        f"{name}={cfg['rate_limit_per_min']}/{cfg['burst']}"
        for name, cfg in sorted(STATE["tiers"].items())
    )
    info(f"live tiers (rpm/burst): {shipped}; default={STATE['default_tier']}")


def _assert_baseline_is_unmetered() -> None:
    """Fail the bootstrap unless ``BASELINE_URL`` really is running with enforcement OFF.

    Phase C subtracts this replica's latency from ``api1``'s and calls the difference the limiter's
    cost. If the baseline were metered too, the difference would be ~0 and the phase would report a
    free rate limiter — a passing gate certifying the opposite of the truth, which is the single
    worst failure mode available to this file. So it is checked, twice over, against two independent
    signals the middleware emits:

    * ``/dashboard/api/stats`` reports ``rate_limit_enabled`` — the switch, read from the process
      that is serving;
    * a metered path answers **without any** ``X-RateLimit-*`` header and with ``metered: false`` in
      the body, because with the switch off no decision exists to describe. (``whoami`` reads that
      decision defensively precisely so it can serve this configuration rather than 500 on it.)

    The mirror image is checked on ``DIRECT_URL`` in the same breath: the metered leg must actually
    be metered, or the delta is two baselines subtracted from each other.
    """
    with httpx.Client(base_url=BASELINE_URL, timeout=_REQUEST_TIMEOUT) as probe:
        stats = probe.get("/dashboard/api/stats", params={"minutes": 1, "hours": 1})
        check(
            stats.status_code == 200,
            f"baseline {BASELINE_URL}/dashboard/api/stats -> HTTP {stats.status_code}",
        )
        enabled = stats.json().get("rate_limit_enabled")
        check(
            enabled is False,
            f"the baseline replica reports rate_limit_enabled={enabled!r}. Phase C subtracts its "
            "latency from the metered replica's, so a baseline that is ALSO metered would report "
            "the enforcement layer as free.",
        )
        sample = probe.get(PATH_WHOAMI, headers=bearer(new_principal("baseline-probe")))
        check(
            sample.status_code == 200,
            f"baseline {PATH_WHOAMI} -> HTTP {sample.status_code}: {sample.text[:160]}",
        )
        leaked = sorted(h for h in sample.headers if h.lower().startswith("x-ratelimit-"))
        check(
            not leaked,
            f"the baseline replica emitted {leaked} on a metered path; a header describing a limit "
            "that was never evaluated means enforcement is not actually off there",
        )
        check(
            sample.json().get("metered") is False,
            "the baseline replica reports metered=true on /whoami; enforcement is not off there",
        )

    with httpx.Client(base_url=DIRECT_URL, timeout=_REQUEST_TIMEOUT) as probe:
        sample = probe.get(PATH_WHOAMI, headers=bearer(new_principal("metered-probe")))
        check(
            sample.status_code == 200,
            f"metered {DIRECT_URL}{PATH_WHOAMI} -> HTTP {sample.status_code}: {sample.text[:160]}",
        )
        check(
            sample.json().get("metered") is True,
            "the metered replica reports metered=false on /whoami — RATE_LIMIT_ENABLED is off "
            "there too, so phase C would be subtracting a baseline from a baseline",
        )
    info(
        f"phase C control established: {DIRECT_URL} is metered, {BASELINE_URL} is "
        "RATE_LIMIT_ENABLED=false (same image, same env, one variable apart)"
    )


def warm_up() -> None:
    """Fire unmeasured traffic at every target so no phase pays for a cold path.

    First-request costs that would otherwise land inside a measurement: uvicorn's per-connection
    setup, the Redis pool's lazy connect, the script cache's first ``EVALSHA`` miss (which costs a
    full ``EVAL`` plus a load), the tier snapshot's first fetch, and Python's own import-on-first-use
    inside the JSON and pydantic paths. None of that is what any gate here is about.
    """
    if not WARMUP_REQUESTS:
        return
    connections = min(CONCURRENCY, 32)
    for label, base in (("lb", BASE_URL), ("direct", DIRECT_URL), ("baseline", BASELINE_URL)):
        # A pool PER TARGET rather than one shared across all three: a shared pool would have to be
        # sized for the sum, and the busiest-principal arithmetic in `_pool_size` is per drive.
        pool = principal_pool(f"warmup-{label}", WARMUP_REQUESTS, connections)
        outcome = drive(base, pool, WARMUP_REQUESTS, connections)
        info(
            f"warm-up {label}: {outcome.ok}/{outcome.total} ok in {outcome.wall_s:.2f}s "
            f"(p50 {percentile(outcome.latencies_ms, 50):.2f}ms, discarded)"
        )


# --------------------------------------------------------------------------------------------- #
# Phase A — the spec's literal test
# --------------------------------------------------------------------------------------------- #
def phase_a() -> Outcome:
    """A. ``LOAD_SPEC_REQUESTS`` parallel requests as ONE free-tier principal.

    The brief's own acceptance line: *"Concurrent load test: 100 parallel requests via httpx,
    counting 200s vs 429s"*, with the output spelled ``Successful: <n>, Rate Limited: <n>``. Both are
    reproduced exactly — the count, the client library, and the output format, which is printed bare
    so it is greppable rather than wearing this file's ``[load]`` prefix.

    A **fresh** principal on the default tier rather than the seeded ``demo-free-key``, because the
    split has to be attributable: a shared demo key carries whatever the last run, the dashboard or a
    curl in the README left in its bucket, and ``Successful: 43`` would then be a fact about history
    rather than about capacity. From cold the split is the tier's ceiling and the remainder.

    All ``LOAD_SPEC_REQUESTS`` are genuinely in flight at once (the semaphore is the request count),
    so this is also the one phase where nginx has to fan a single caller's burst across both replicas
    — the configuration in which a per-process bucket would show up as ~2x the admissions. Grading
    that exactly is check 7 of the E2E verifier's job; here the gates are the spec's: two statuses
    only, no 5xx, and at least one refusal.
    """
    principal = new_principal("spec")
    info(
        f"phase A: {SPEC_REQUESTS} parallel requests as one fresh {STATE['default_tier']}-tier "
        f"principal through the LB..."
    )
    outcome = fire(BASE_URL, [principal], SPEC_REQUESTS, SPEC_REQUESTS)

    # The spec's literal output line. Bare, exactly this shape, nothing else on it.
    print(f"Successful: {outcome.ok}, Rate Limited: {outcome.limited}", flush=True)

    result("phase_a_requests", outcome.total)
    result("phase_a_successful", outcome.ok)
    result("phase_a_rate_limited", outcome.limited)
    result("phase_a_other_status", outcome.other_label)
    result("phase_a_server_errors", outcome.server_errors)
    result("phase_a_degraded", outcome.degraded)
    result("phase_a_wall_s", f"{outcome.wall_s:.3f}")
    result("phase_a_p50_ms", f"{percentile(outcome.latencies_ms, 50):.2f}")
    result("phase_a_p95_ms", f"{percentile(outcome.latencies_ms, 95):.2f}")

    check(
        not outcome.other,
        f"phase A saw statuses {outcome.other} besides 200/429 in {outcome.total} requests; the "
        "spec's test counts exactly two outcomes and a third means something other than the limiter "
        "answered",
    )
    check(
        outcome.ok + outcome.limited == outcome.total,
        f"phase A: {outcome.ok} + {outcome.limited} != {outcome.total}",
    )
    check(
        outcome.server_errors == 0,
        f"phase A saw {outcome.server_errors} server error(s)/transport failure(s); a limiter that "
        "5xxes under a burst has failed closed by accident rather than refused by design",
    )
    check(
        outcome.limited > 0,
        f"phase A admitted all {outcome.total} requests with zero 429s. The burst never reached the "
        f"tier's ceiling (currently {ceiling_of(STATE['default_tier'])}), so it did not exercise the "
        "limiter at all — 'Successful: N, Rate Limited: 0' would be a PASS certifying the opposite "
        "of what the spec's test is for. Raise LOAD_SPEC_REQUESTS above the ceiling.",
    )
    info(
        f"phase A: {outcome.ok} admitted / {outcome.limited} refused in {outcome.wall_s:.2f}s "
        f"(free ceiling {ceiling_of(STATE['default_tier'])}, degraded={outcome.degraded})"
    )
    return outcome


# --------------------------------------------------------------------------------------------- #
# Phase B — throughput
# --------------------------------------------------------------------------------------------- #
def _report_throughput(prefix: str, outcome: Outcome) -> None:
    """Emit the ``RESULT`` lines every throughput-shaped phase shares."""
    result(f"{prefix}_requests", outcome.total)
    result(f"{prefix}_wall_s", f"{outcome.wall_s:.3f}")
    result(f"{prefix}_rps", f"{outcome.rps:.1f}")
    result(f"{prefix}_ok", outcome.ok)
    result(f"{prefix}_rate_limited", outcome.limited)
    result(f"{prefix}_other_status", outcome.other_label)
    result(f"{prefix}_degraded", outcome.degraded)
    result(f"{prefix}_p50_ms", f"{percentile(outcome.latencies_ms, 50):.2f}")
    result(f"{prefix}_p95_ms", f"{percentile(outcome.latencies_ms, 95):.2f}")
    result(f"{prefix}_p99_ms", f"{percentile(outcome.latencies_ms, 99):.2f}")
    result(f"{prefix}_client_cpu_util", f"{outcome.cpu_util:.2f}")
    result(f"{prefix}_concurrency", CONCURRENCY)


def _assert_server_charged_every_request(pool: list[str], outcome: Outcome) -> str:
    """Cross-check the generator's own count against the SERVER's charged usage for one principal.

    This is the assertion that makes the hand-rolled HTTP client trustworthy. Everything else in a
    throughput phase is the generator grading its own homework: it counts the responses it believes
    it parsed and divides by a clock it also owns. A framing bug that dropped every second response
    would show up as a *faster* run, not a broken one.

    So one principal's charge is read back from ``GET /api/v1/admin/users/{id}/usage`` — the daily
    quota counter, incremented inside the same Lua script that made the decision, on the other side
    of the network from anything this file controls. ``/whoami`` costs 1, and phase B admits every
    request (``gate_clean_measurement`` has already established there were no refusals), so the
    counter must equal the number of requests this generator sent to that principal, exactly.

    One principal rather than all of them: it is one admin call against a deterministic number, and
    a framing bug is not selective about which connection it corrupts.
    """
    if not outcome.per_principal or not pool:
        return "skipped (no per-principal accounting)"
    expected = outcome.per_principal[0]
    usage = admin_json("GET", f"{ADMIN}/users/{pool[0]}/usage")
    charged = int(usage["daily"]["used"])
    check(
        charged == expected,
        f"the generator sent {expected} requests as {pool[0]!r} and the server charged {charged}. "
        "Every one of them was a cost-1 /whoami and none was refused, so these must be equal. A "
        "shortfall means this client mis-framed responses and counted requests the service never "
        "saw; an excess means the limiter charged twice for one call.",
    )
    return f"server charged {charged} for {expected} sent (1 principal, exact)"


def phase_b(pool: list[str]) -> Outcome:
    """B. ``LOAD_REQUESTS`` requests at ``LOAD_CONCURRENCY`` through the LB, as enterprise.

    Enterprise, and across a pool sized from the live ceiling, for one reason: **the limiter must
    admit every request**. A free-tier principal empties after 60, so a harness that ran this phase
    as one would measure how fast this service can say no. That is a real number and it is not
    throughput.

    Gated on ``rps >= LOAD_MIN_RPS`` and, before that, on the measurement being clean (see
    :func:`gate_clean_measurement`, which includes the generator-saturation guard) and on the
    server's own charged usage agreeing with what this client believes it sent
    (:func:`_assert_server_charged_every_request`). ``spec_target_rps`` and
    ``throughput_meets_spec_target`` are printed alongside so the spec's 1,000 rps and this
    hardware's gate are never read as the same claim — see the throughput rubric in the module
    docstring.
    """
    info(
        f"phase B: {LOAD_REQUESTS} requests over {CONCURRENCY} keep-alive connections "
        f"through the LB..."
    )
    outcome = drive(BASE_URL, pool, LOAD_REQUESTS, CONCURRENCY)

    _report_throughput("phase_b", outcome)
    result("spec_target_rps", f"{SPEC_TARGET_RPS:.0f}")
    result("throughput_meets_spec_target", str(outcome.rps >= SPEC_TARGET_RPS).lower())

    info(
        f"phase B: {outcome.ok}/{outcome.total} ok in {outcome.wall_s:.2f}s -> "
        f"{outcome.rps:.1f} req/s (generator used {outcome.cpu_util:.2f} of a core, p50 "
        f"{percentile(outcome.latencies_ms, 50):.2f}ms / p95 "
        f"{percentile(outcome.latencies_ms, 95):.2f}ms at c={CONCURRENCY})"
    )
    gate_clean_measurement("phase B", outcome)
    info(f"phase B: {_assert_server_charged_every_request(pool, outcome)}")

    if outcome.rps < SPEC_TARGET_RPS:
        info(
            f"phase B: MEASURED {outcome.rps:.1f} req/s, BELOW the spec's {SPEC_TARGET_RPS:.0f} "
            f"req/s target. The gate is {MIN_RPS:.0f} — calibrated to this stack rather than to the "
            "spec's figure. Reported, not hidden: see RESULT throughput_meets_spec_target."
        )
    else:
        info(
            f"phase B: {outcome.rps:.1f} req/s CLEARS the spec's {SPEC_TARGET_RPS:.0f} req/s "
            f"target ({outcome.rps / SPEC_TARGET_RPS:.1f}x). The gate stays at {MIN_RPS:.0f} — a "
            "regression bar with margin for a laptop, not a restatement of the requirement."
        )
    check(
        outcome.rps >= MIN_RPS,
        f"phase B throughput {outcome.rps:.1f} req/s below gate LOAD_MIN_RPS={MIN_RPS:.0f} req/s "
        f"(generator at {outcome.cpu_util:.2f} of a core, so the service was the constraint)",
    )
    return outcome


# --------------------------------------------------------------------------------------------- #
# Phase C — limiter overhead against a RATE_LIMIT_ENABLED=false baseline
# --------------------------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Overhead:
    """Phase C's paired-sample result."""

    metered_ms: list[float]
    baseline_ms: list[float]
    paired_ms: list[float]


def _sample(client: httpx.Client, headers: dict[str, str]) -> float:
    """One sequential, timed ``GET /api/v1/whoami``. Returns the wall latency in ms."""
    started = time.perf_counter()
    resp = client.get(PATH_WHOAMI, headers=headers)
    elapsed = (time.perf_counter() - started) * 1000.0
    if resp.status_code != 200:
        raise CheckError(
            f"overhead sample against {resp.request.url} -> HTTP {resp.status_code}: "
            f"{resp.text[:160]}"
        )
    return elapsed


def phase_c() -> Overhead:
    """C. The limiter's ADDED latency, baselined against the same route with enforcement OFF.

    This is C13's acknowledged gap, closed. ``verify_e2e.py`` check 14 measures metered p95 minus the
    p95 of an **exempt** path (``/docs``), which compares two different handlers and can only be a
    lower bound. Here both legs are ``GET /api/v1/whoami`` — same route, same image, same handler,
    same single network hop, no proxy in either — and the only difference between the two processes
    is ``RATE_LIMIT_ENABLED``. :func:`_assert_baseline_is_unmetered` has already proved that is
    actually true of the running containers rather than merely written in the compose file.

    **Sequential, one request in flight.** The question is what the enforcement layer adds to a
    request, and under concurrency Little's Law puts queueing delay into both legs — a real cost, but
    a different one, and the one phases B and D already report as p95-under-load.

    **Interleaved, with the order alternating.** Pair *i* fires both legs back to back so they see
    the same host conditions; the leg that goes first alternates so that whatever the first request
    of a pair pays (a scheduler wake-up, a keep-alive probe) is charged to both legs equally across
    the run.

    **The analytics write is inside the number, and that is deliberate.** The middleware issues it
    after the response body is on the wire, so it is not in the response the client just timed — but
    each leg holds ONE keep-alive connection, and uvicorn does not read the next request on a
    connection until the previous ASGI call has returned. So sample *i*'s measured latency carries
    sample *i-1*'s analytics write, which over 200 samples means the metered leg pays for 200 of
    them and the baseline leg (which writes none) pays for none. Counted rather than argued away: it
    is real cost the enforcement layer imposes on the caller.

    Both statistics are computed, and the gate reads only one:

    * ``overhead_p50/p95/p99`` — the difference of the corresponding percentiles. **This is what the
      gate reads.** It is the stable statistic: each percentile is estimated from all
      ``LOAD_OVERHEAD_SAMPLES`` samples of its own leg.
    * ``overhead_paired_p50/p95/p99`` — percentiles of the per-pair differences. Reported because it
      is the statistic a reader may assume "p95 of the delta" means, and NOT gated because at
      millisecond scale a single pair's difference is dominated by which container the scheduler
      favoured on that iteration; its tail measures host jitter, not the limiter.
    """
    # Concurrency 1: phase C samples sequentially, so one principal carries every sample.
    pool = principal_pool("overhead", OVERHEAD_SAMPLES, 1)
    headers = [bearer(user_id) for user_id in pool]
    info(
        f"phase C: {OVERHEAD_SAMPLES} paired sequential samples, {DIRECT_URL} (metered) vs "
        f"{BASELINE_URL} (RATE_LIMIT_ENABLED=false)..."
    )

    metered_ms: list[float] = []
    baseline_ms: list[float] = []
    paired_ms: list[float] = []
    with (
        httpx.Client(base_url=DIRECT_URL, timeout=_REQUEST_TIMEOUT) as metered_client,
        httpx.Client(base_url=BASELINE_URL, timeout=_REQUEST_TIMEOUT) as baseline_client,
    ):
        for index in range(OVERHEAD_SAMPLES):
            header = headers[index % len(headers)]
            if index % 2 == 0:
                baseline = _sample(baseline_client, header)
                metered = _sample(metered_client, header)
            else:
                metered = _sample(metered_client, header)
                baseline = _sample(baseline_client, header)
            metered_ms.append(metered)
            baseline_ms.append(baseline)
            paired_ms.append(metered - baseline)

    overhead = Overhead(metered_ms=metered_ms, baseline_ms=baseline_ms, paired_ms=paired_ms)
    deltas = {
        pct: percentile(metered_ms, pct) - percentile(baseline_ms, pct) for pct in (50, 95, 99)
    }

    result("phase_c_samples", OVERHEAD_SAMPLES)
    result("phase_c_baseline_url_metered", "false")
    for pct in (50, 95, 99):
        result(f"phase_c_metered_p{pct}_ms", f"{percentile(metered_ms, pct):.3f}")
        result(f"phase_c_baseline_p{pct}_ms", f"{percentile(baseline_ms, pct):.3f}")
        result(f"overhead_p{pct}_ms", f"{deltas[pct]:.3f}")
        result(f"overhead_paired_p{pct}_ms", f"{percentile(paired_ms, pct):.3f}")

    info(
        f"phase C: metered p50/p95/p99 {percentile(metered_ms, 50):.2f}/"
        f"{percentile(metered_ms, 95):.2f}/{percentile(metered_ms, 99):.2f}ms vs baseline "
        f"{percentile(baseline_ms, 50):.2f}/{percentile(baseline_ms, 95):.2f}/"
        f"{percentile(baseline_ms, 99):.2f}ms -> added p50 {deltas[50]:.2f}ms, p95 "
        f"{deltas[95]:.2f}ms, p99 {deltas[99]:.2f}ms"
    )
    check(
        deltas[95] <= MAX_OVERHEAD_MS,
        f"limiter overhead p95 {deltas[95]:.3f}ms above gate LOAD_MAX_OVERHEAD_MS="
        f"{MAX_OVERHEAD_MS:.3f}ms (metered p95 {percentile(metered_ms, 95):.3f}ms - "
        f"RATE_LIMIT_ENABLED=false p95 {percentile(baseline_ms, 95):.3f}ms over "
        f"n={OVERHEAD_SAMPLES} interleaved pairs)",
    )
    return overhead


# --------------------------------------------------------------------------------------------- #
# Phase D — scaling
# --------------------------------------------------------------------------------------------- #
def phase_d() -> dict[str, Any]:
    """D. One replica versus two, back to back, reported as ``rps_2 / rps_1``.

    .. rubric:: ~1.5x is the healthy answer here, and 2.0x would be the alarming one

    Both replicas share ONE Redis, and every metered request runs one Lua script there. Redis
    executes scripts on a **single thread**, one at a time, globally — so that section of the request
    is serialised across the whole cluster no matter how many API processes are added. What the
    second replica parallelises is everything else: the HTTP parse, the routing, the identity
    resolution, the pydantic serialisation, the response write. That is Amdahl's law with a small but
    irreducible serial fraction, and it puts the ceiling below 2.0x by construction.

    A run that DID report ~2.0x would mean the decision had stopped serialising — which is what a
    per-process bucket looks like, i.e. the exact defect this project exists to prevent, and which
    ``verify_e2e.py`` check 7 grades directly. So the gate is a floor (``>= LOAD_MIN_SCALING``) and
    deliberately not a band: this phase's job is to catch "adding a replica bought nothing", and
    over-performance is check 7's business, on a burst designed to detect it.

    Measured on the calibration host: **1.56x and 1.61x** in the shipped (interleaved) arrangement,
    inside a 1.40-1.72x band across twenty-two legs at four concurrency settings.
    ``LOAD_MIN_SCALING`` defaults to 1.30 — below every one of those — rather than to the 1.5 the
    plan pencilled in before anything had been measured, because 1.5 sits inside the spread (a
    sequential run measured 1.404) and would flake on a busy machine.

    Three further honest notes:

    * the two-replica leg pays an **nginx hop** the single-replica leg does not, because the legs
      compare the two *deployments* — one replica addressed directly is how you would run one, and
      through the LB is how you run two. The proxy's cost is part of what the second replica has to
      earn back.
    * both legs are sized identically, use fresh principal pools, and are **interleaved** in
      :data:`SCALING_CHUNKS` alternating chunks rather than run one-after-the-other, so neither
      inherits the other's drained buckets and neither owns a different window of machine time.
    * the offered load has to **saturate one replica**, or the comparison measures nothing. It
      does: one replica serves ~3.7-4.3k req/s at 24, 32, 40 and 64 connections alike, so past ~24
      the extra concurrency buys queueing rather than throughput. The default sits at 32 for the
      separate reason given at :data:`CONCURRENCY` — it is the replica's Redis pool size, and
      offering more than that puts the pool's 50 ms wait budget inside the measurement.
    """
    single_pool = principal_pool("scale-1", SCALING_REQUESTS)
    pair_pool = principal_pool("scale-2", SCALING_REQUESTS)
    chunks = _shard(SCALING_REQUESTS, SCALING_CHUNKS)
    info(
        f"phase D: {SCALING_REQUESTS} requests per leg over {CONCURRENCY} connections, "
        f"interleaved in {SCALING_CHUNKS} alternating chunks..."
    )

    single_parts: list[Outcome] = []
    pair_parts: list[Outcome] = []
    for index, chunk in enumerate(chunks):
        if not chunk:
            continue
        # Alternate which leg goes first so that whatever a chunk's first run pays — a cold proxy
        # connection, the scheduler noticing this container again — is charged to both legs equally.
        if index % 2 == 0:
            single_parts.append(drive(DIRECT_URL, single_pool, chunk, CONCURRENCY))
            pair_parts.append(drive(BASE_URL, pair_pool, chunk, CONCURRENCY))
        else:
            pair_parts.append(drive(BASE_URL, pair_pool, chunk, CONCURRENCY))
            single_parts.append(drive(DIRECT_URL, single_pool, chunk, CONCURRENCY))

    single = _merge(single_parts)
    pair = _merge(pair_parts)
    _report_throughput("phase_d_single", single)
    gate_clean_measurement("phase D leg 1 (one replica)", single)
    _report_throughput("phase_d_lb", pair)
    gate_clean_measurement("phase D leg 2 (two replicas)", pair)

    scaling = (pair.rps / single.rps) if single.rps > 0 else 0.0
    result("scaling_factor", f"{scaling:.3f}")
    info(
        f"phase D: 1 replica {single.rps:.1f} req/s -> 2 replicas {pair.rps:.1f} req/s = "
        f"{scaling:.2f}x (gate >= {MIN_SCALING:.2f}x; 2.0x is unreachable over one Redis and would "
        "mean the decision had stopped serialising)"
    )
    check(
        scaling >= MIN_SCALING,
        f"scaling factor {scaling:.3f}x below gate LOAD_MIN_SCALING={MIN_SCALING:.2f}x "
        f"({single.rps:.1f} req/s on one replica -> {pair.rps:.1f} req/s on two). Adding a replica "
        "bought less than the gate demands: either the LB is not fanning out, or the shared store "
        "is saturated and is now the whole cost of a request.",
    )
    return {"single": single, "pair": pair, "scaling": scaling}


# --------------------------------------------------------------------------------------------- #
# Phase E — memory
# --------------------------------------------------------------------------------------------- #
def memory_snapshot(label: str) -> dict[str, dict[str, Any]]:
    """RSS and vitals from **every** replica, keyed by REPLICA URL.

    Each replica is addressed **directly** rather than through the LB, and that is the whole design
    of this probe: ``GET /api/v1/admin/debug/memory`` reports the process that answered it, so
    through a round-robin proxy "RSS before" and "RSS after" would routinely come from different
    containers and the growth figure would be the difference between two unrelated processes.

    Keyed by URL and not by ``served_by`` because the ``RESULT`` lines are derived from these keys
    and a container hostname is a fresh hex string on every ``docker compose up`` — C16 transcribes
    a table from this output, and ``memory_a1b2c3d4e5f6_growth_mb`` is not a row anyone can compare
    against last week's run. The hostname is still reported, as a value.
    """
    snapshot: dict[str, dict[str, Any]] = {}
    for url in REPLICA_URLS:
        try:
            with httpx.Client(base_url=url, timeout=_REQUEST_TIMEOUT) as probe:
                resp = probe.get(
                    f"{ADMIN}/debug/memory", headers={"X-Admin-Token": SETTINGS.admin_token}
                )
        except Exception as exc:  # noqa: BLE001 — an unreachable replica is a check failure
            raise CheckError(f"{url}{ADMIN}/debug/memory raised {type(exc).__name__}: {exc}") from exc
        check(
            resp.status_code == 200,
            f"{url}{ADMIN}/debug/memory -> HTTP {resp.status_code}: {resp.text[:160]}",
        )
        body = resp.json()
        rss = body.get("rss_mb")
        check(
            isinstance(rss, (int, float)) and not isinstance(rss, bool),
            f"{url} reported rss_mb={rss!r} (want a number; psutil unavailable?)",
        )
        snapshot[url] = body
    hosts = {body["served_by"] for body in snapshot.values()}
    check(
        len(hosts) == len(REPLICA_URLS),
        f"{len(REPLICA_URLS)} replica URLs reported only {len(hosts)} distinct served_by values "
        f"({sorted(hosts)}); two URLs resolving to one process would make phase E's growth figure "
        "a comparison of a process with itself",
    )
    info(
        f"memory {label}: "
        + ", ".join(
            f"{body['served_by']}={body['rss_mb']:.1f}MiB (uptime {body['uptime_sec']:.0f}s)"
            for _url, body in sorted(snapshot.items())
        )
    )
    return snapshot


def phase_e(before: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """E. RSS per replica, before any load and after all of it: absolute and growth gates.

    Two gates rather than one, because they fail differently and both matter:

    * **absolute** (``MAX_BACKEND_MEM_MB``) is the number an orchestrator OOM-kills on. It catches a
      process that was already large before this run touched it.
    * **growth** (``MAX_MEM_GROWTH_MB``) is what "stable memory usage under sustained load" in the
      brief actually asks for. A limiter accumulating per-principal state in process memory would
      pass the absolute gate for a long time and fail this one immediately — and this run mints
      several hundred brand-new principals precisely so that mistake has something to accumulate.

    Both are taken over the **worst** replica, never an average: with two processes behind a proxy an
    average halves the growth of the one that is actually leaking.

    ``rss_mb`` is MiB (the probe divides by 1024 * 1024), so the gates are MiB too; the variables are
    spelled ``_MB`` because that is what the brief and every orchestrator call it.
    """
    after = memory_snapshot("after")

    worst_rss = 0.0
    worst_rss_host = ""
    worst_growth = 0.0
    worst_growth_host = ""
    for index, url in enumerate(REPLICA_URLS, start=1):
        host = str(after[url]["served_by"])
        check(
            host == before[url]["served_by"],
            f"{url} was {before[url]['served_by']} before the run and is {host} now — that replica "
            "restarted mid-run, so its RSS was reset and its growth figure would be a fiction "
            "(check the container logs: a crash under load is a finding, not a measurement)",
        )
        rss_after = float(after[url]["rss_mb"])
        rss_before = float(before[url]["rss_mb"])
        growth = rss_after - rss_before
        result(f"memory_replica{index}_host", host)
        result(f"memory_replica{index}_before_mb", f"{rss_before:.1f}")
        result(f"memory_replica{index}_after_mb", f"{rss_after:.1f}")
        result(f"memory_replica{index}_growth_mb", f"{growth:.1f}")
        if rss_after > worst_rss:
            worst_rss, worst_rss_host = rss_after, host
        if growth > worst_growth:
            worst_growth, worst_growth_host = growth, host

    result("memory_max_rss_mb", f"{worst_rss:.1f}")
    result("memory_max_growth_mb", f"{worst_growth:.1f}")
    info(
        f"phase E: worst RSS {worst_rss:.1f} MiB ({worst_rss_host}), worst growth "
        f"{worst_growth:.1f} MiB ({worst_growth_host or 'none'}) across "
        f"{len(REPLICA_URLS)} replicas"
    )
    check(
        worst_rss <= MAX_BACKEND_MEM_MB,
        f"replica {worst_rss_host} RSS {worst_rss:.1f} MiB above gate MAX_BACKEND_MEM_MB="
        f"{MAX_BACKEND_MEM_MB:.0f} MiB",
    )
    check(
        worst_growth <= MAX_MEM_GROWTH_MB,
        f"replica {worst_growth_host} grew {worst_growth:.1f} MiB across the run, above gate "
        f"MAX_MEM_GROWTH_MB={MAX_MEM_GROWTH_MB:.0f} MiB. Sustained load must not leave per-request "
        "state behind — every bucket, counter and window this service keeps lives in Redis.",
    )
    return {"before": before, "after": after, "max_rss": worst_rss, "max_growth": worst_growth}


# --------------------------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------------------------- #
def run() -> None:
    info(f"== load harness vs {BASE_URL} (one replica: {DIRECT_URL}, baseline: {BASELINE_URL}) ==")
    info(
        f"gates: rps >= {MIN_RPS:.0f} (spec asks {SPEC_TARGET_RPS:.0f}); overhead p95 <= "
        f"{MAX_OVERHEAD_MS:.0f}ms vs RATE_LIMIT_ENABLED=false; scaling >= {MIN_SCALING:.2f}x; "
        f"RSS <= {MAX_BACKEND_MEM_MB:.0f} MiB and growth <= {MAX_MEM_GROWTH_MB:.0f} MiB"
    )

    # The gates first, before anything is measured. They are inputs, not findings, and printing them
    # up front means a run that dies in phase B still tells C16's README what it was grading against.
    result("gate_min_rps", f"{MIN_RPS:.0f}")
    result("gate_max_overhead_ms", f"{MAX_OVERHEAD_MS:.1f}")
    result("gate_min_scaling", f"{MIN_SCALING:.2f}")
    result("gate_max_backend_mem_mb", f"{MAX_BACKEND_MEM_MB:.0f}")
    result("gate_max_mem_growth_mb", f"{MAX_MEM_GROWTH_MB:.0f}")
    result("config_spec_requests", SPEC_REQUESTS)
    result("config_requests", LOAD_REQUESTS)
    result("config_concurrency", CONCURRENCY)
    result("config_scaling_requests", SCALING_REQUESTS)
    result("config_overhead_samples", OVERHEAD_SAMPLES)
    result("config_replicas", len(REPLICA_URLS))
    result("config_client_cores", os.cpu_count() or 0)
    result("gate_max_client_cpu", f"{MAX_CLIENT_CPU:.2f}")

    wait_ready()
    assert_lb_fans_out()
    load_tier_table()
    _assert_baseline_is_unmetered()
    warm_up()

    # Phase E's "before" is taken here, after warm-up: the pools, the script cache and the tier
    # snapshot are one-off allocations every process makes on its first request, and counting them
    # as growth-under-load would report a fixed cost as a leak.
    before = memory_snapshot("before")

    a = phase_a()
    # The pool is built OUTSIDE the phase so its admin traffic is not inside the timed window and
    # so the phase can grade the server's charged usage against the ids it actually drove.
    b = phase_b(principal_pool("throughput", LOAD_REQUESTS))
    c = phase_c()
    d = phase_d()
    e = phase_e(before)

    print("", flush=True)
    info(
        f"headline: phase_a={a.ok}/{a.limited} (200/429) · throughput {b.rps:.0f} req/s "
        f"(spec target {SPEC_TARGET_RPS:.0f}) · overhead p95 "
        f"{percentile(c.metered_ms, 95) - percentile(c.baseline_ms, 95):.2f}ms · scaling "
        f"{d['scaling']:.2f}x · RSS {e['max_rss']:.0f} MiB (+{e['max_growth']:.1f})"
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
    finally:
        CLIENT.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

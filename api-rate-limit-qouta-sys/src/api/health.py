"""Unversioned liveness probe — ``GET /health``.

**Why this route is deliberately NOT under ``/api/v1``.** Every metered route in this project is
hard-prefixed ``/api/v1`` precisely so a breaking change can arrive as a new namespace instead of
a changelog entry. ``/health`` is the opposite kind of thing: it is a *liveness contract* consumed
by the container HEALTHCHECK, compose's ``condition: service_healthy``, nginx's upstream check
(C12) and any orchestrator probe — none of which should have to be redeployed the day
``/api/v2`` ships.

**Why it is never metered.** The limiter middleware exempts this path, and that is not a
convenience: a liveness probe that can return 429 is not a liveness probe. The container's own
HEALTHCHECK polls every 10 s from the same source address, so metering it would eventually mark a
perfectly healthy replica unhealthy and have compose restart it — the limiter would have taken the
service down by working correctly. Same reasoning for authentication: this route is unauthenticated,
and everything is read defensively off ``request.app.state.runtime`` via ``getattr``, so a missing
or half-wired runtime degrades to zeroed vitals rather than raising.

**It is not dependency-free — it is dependency-*proof*.** As of C2 the route performs a live Redis
``PING`` on every call, so the probe genuinely touches a dependency (bounded by the gateway's 250 ms
socket timeout, and free once the breaker is open). What is guaranteed is the stronger and more
useful property: **no dependency can make this endpoint fail.** A Redis outage is reported as
``redis: "unreachable"`` with a **200**, never as a 5xx and never as a timeout, because everything
that consumes this route treats a non-200 as "restart this replica" — and a replica that is
degraded but still correctly serving must not be restarted. See the rubric at the bottom.

.. rubric:: The response body

``status`` and ``rate_limiter`` are the spec's two keys, **verbatim** — same names, same values,
top level, not nested. Everything else is additive:

* ``version`` — the running API version, so a rollout can be observed.
* ``uptime_sec`` — how long this process has been up.
* ``served_by`` — this container's hostname. C12 runs two replicas behind a load balancer, and
  this field is how the E2E verifier proves that *both* of them answered: without it, the
  distributed double-spend check would pass trivially against a single replica and the bug the
  whole project exists to catch would go undetected.
* ``redis`` — ``"ok"`` or ``"unreachable"``, from a real ``PING`` through the gateway (so the probe
  also feeds the circuit breaker rather than being a second, unobserved path to the server).
* ``config_version`` — the ``config:version`` this replica's tier snapshot was built from. This is
  how C10 watches a runtime tier change propagate: after a ``PUT /admin/tiers/{tier}`` the number
  climbs on the replica that served the write immediately and on every other replica within
  ``TIER_CACHE_TTL_SEC``. Without it, "did the change reach replica 2 yet?" is only answerable by
  firing traffic at it and inferring the answer from a 429. ``0`` means no snapshot has ever been
  read from Redis — a legitimate degraded state (the configured defaults are being enforced), not
  an error.

``rate_limiter`` is the constant ``"active"`` for now; C8 makes it report ``"degraded"`` (still
with a 200) when Redis is unreachable and the fallback bucket is carrying the load. A silent
fail-open is indistinguishable from having no rate limiter at all, which is why the degraded state
has to be visible on the one endpoint everything already polls.

.. rubric:: What is deliberately NOT reported here: the C5 identity-cache counters

:meth:`~src.identity.IdentityResolver.cache_stats` is a pure counter read — no I/O, no lock — so
adding ``identity_cache`` to this body would be cheap in CPU. It is not added, for three reasons
that are not about cost:

* **This body is a pinned contract, not a dashboard.** ``tests/unit/test_health.py`` asserts the
  exact key set and the C13 verifier asserts the spec's two keys character-for-character across a
  container boundary. The value of that pinning comes from the payload being small and stable;
  every field added "because it is cheap" is one more thing a probe consumer can start depending on
  and one more reason a later commit cannot change it.
* **There is already a surface for counters.** C11's ``GET /dashboard/api/stats`` is where the
  limiter's, the registry's and the gateway's counters are published together, which is also the
  only place they can be *compared* — an identity hit rate is meaningless next to a Redis ping and
  informative next to the request rate that produced it.
* **``/health`` is unauthenticated and public.** Cache size and hit rate are a (weak, but free)
  side channel about credential traffic: a jump in the negative-hit share is exactly the signature
  of a key-guessing flood, and that is operator information rather than caller information.

So the counters exist, are exposed on the resolver, and C11 publishes them. If a future operator
genuinely needs them on the liveness probe, adding the field is a one-line change *and* a
deliberate edit to the key-set test — which is the right amount of friction for changing a probe's
contract.

.. rubric:: A Redis outage does NOT make this endpoint red

``status`` stays ``"healthy"`` and the HTTP status stays 200 when ``redis`` is ``"unreachable"``.
That is a decision, not an oversight, and it is worth stating because the instinct is the opposite.

``/health`` is consumed by the container ``HEALTHCHECK``, by compose's ``condition:
service_healthy`` and (C12) by nginx's upstream check. Every one of those treats a non-200 as
"take this replica out and restart it". But a replica that cannot reach Redis is still alive, still
serving, and — per C8's ``FAIL_MODE=open`` — still correctly handling every request through the
bounded local fallback bucket. Turning the probe red would restart a process that is doing exactly
what it was designed to do, and it would do so on *all* replicas simultaneously, because they all
share the one Redis that is down. The outcome is a total outage triggered by a dependency failure
the system was explicitly built to survive: a liveness probe that reports a *dependency's* health
converts partial degradation into a full restart loop.

So liveness and dependency health are reported as two separate fields. ``status`` answers "is this
process alive?" (a liveness question). ``redis`` answers "can it reach the shared store?" (a
readiness/observability question) and is what the dashboard, the E2E verifier and an operator read.
``rate_limiter`` likewise stays ``"active"`` here; C8 is what turns it ``"degraded"``, driven by the
limiter's own fallback state rather than by this ping.
"""

from __future__ import annotations

import socket

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from src.redis_client import BackingStoreUnavailable

router = APIRouter(tags=["health"])

#: The spec's two literal values. Named constants because the E2E verifier asserts them
#: character-for-character, and a "harmless" rewording here would fail a check on the other side
#: of a container boundary.
STATUS_HEALTHY = "healthy"
RATE_LIMITER_ACTIVE = "active"

#: The two values of the additive ``redis`` field. Deliberately not ``true``/``false``: a boolean
#: named ``redis`` reads as "is Redis configured?" in a payload, and this is a live reachability
#: answer with a 250 ms timeout behind it.
REDIS_OK = "ok"
REDIS_UNREACHABLE = "unreachable"


def _hostname() -> str:
    """This container's hostname, or ``"unknown"`` if the OS declines to say.

    Resolved once at import rather than per request: the hostname cannot change under a running
    process, and ``/health`` is polled every 10 s by the container healthcheck plus once per
    dashboard refresh, so there is no reason to pay a syscall for a constant.
    """
    try:
        return socket.gethostname() or "unknown"
    except OSError:  # pragma: no cover - gethostname does not fail on a supported platform
        return "unknown"


#: Under compose this is the container id, which is exactly what makes two replicas
#: distinguishable to the E2E verifier without any coordination between them.
SERVED_BY: str = _hostname()


class HealthResponse(BaseModel):
    """The ``GET /health`` body. Small, stable, and safe to expose unauthenticated."""

    status: str = Field(description="Always 'healthy' while the process is serving.")
    rate_limiter: str = Field(
        description="'active' while enforcement is backed by Redis; 'degraded' on the C8 "
        "fail-open fallback path. Never absent — a missing field would read as 'no limiter'."
    )
    version: str = Field(description="API version reported by the FastAPI app.")
    uptime_sec: float = Field(description="Seconds since the runtime was constructed.")
    served_by: str = Field(
        description="Hostname of the replica that answered. Proves the load balancer is "
        "fanning out across replicas rather than pinning to one."
    )
    redis: str = Field(
        description="'ok' or 'unreachable', from a live PING. Reported separately from `status` "
        "on purpose: this process stays healthy (and keeps serving, fail-open) when the shared "
        "store is down."
    )
    config_version: int = Field(
        description="`config:version` behind this replica's tier snapshot. C10 watches this "
        "number climb to prove a runtime tier change propagated; 0 means no snapshot has been "
        "read from Redis yet and the configured defaults are in force."
    )


def _config_version(runtime: object) -> int:
    """Read the tier snapshot's version off the runtime, degrading to ``0`` rather than raising.

    The same ``getattr`` discipline as :func:`_probe_redis`, applied one level deeper: a runtime
    with no registry (a half-wired process, or a test that injected a bare object) must produce a
    200 with an honest ``0``, not an ``AttributeError`` on the one endpoint an orchestrator uses to
    decide whether to restart this replica.

    :meth:`~src.tiers.TierRegistry.snapshot` is synchronous and touches no I/O, so calling it here
    costs an attribute read — and, as a side effect worth having, the container's 10-second
    ``/health`` poll keeps the 5-second snapshot warm on a replica that is otherwise idle.
    """
    snapshot = getattr(getattr(runtime, "tiers", None), "snapshot", None)
    if snapshot is None:
        return 0
    return int(getattr(snapshot(), "version", 0) or 0)


async def _probe_redis(runtime: object) -> str:
    """Return ``"ok"`` or ``"unreachable"`` for the runtime's Redis gateway.

    Read through ``getattr`` for the same reason everything else in this module is: a half-wired or
    pre-startup runtime must produce a 200 with an honest field, not an ``AttributeError`` and a
    500. A Runtime built but never started (the ``create_app(runtime=...)`` test seam) has a gateway
    that is constructed and not connected, and :meth:`~src.redis_client.RedisGateway.run` reports
    that as :class:`~src.redis_client.BackingStoreUnavailable` — which is exactly "unreachable".

    Only ``BackingStoreUnavailable`` is caught, and only because the gateway guarantees it is the
    *single* classified failure type. Catching ``Exception`` here would hide a real bug in the probe
    behind a plausible-looking ``"unreachable"``, on the one endpoint an operator trusts to tell
    them where the problem is.

    No extra timeout wrapper: the gateway's client is built with ``socket_timeout`` =
    ``REDIS_TIMEOUT_MS`` (250 ms), so the ping is already bounded, and once the breaker is open it
    costs nothing at all.
    """
    gateway = getattr(runtime, "redis", None)
    if gateway is None:
        return REDIS_UNREACHABLE
    try:
        answered = await gateway.ping()
    except BackingStoreUnavailable:
        return REDIS_UNREACHABLE
    return REDIS_OK if answered else REDIS_UNREACHABLE


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description=(
        "Liveness check. Public, unversioned, unauthenticated and never rate limited. It does "
        "touch a dependency — a live Redis PING, bounded by a 250 ms timeout — but it can never "
        "*fail* on one: while the process is alive this always returns 200, and an unreachable "
        "store is reported as `redis: \"unreachable\"` in the body rather than as a bad status "
        "code. Consumers of this probe restart the replica on a non-200, and a replica that "
        "cannot reach Redis is still serving correctly through the fail-open fallback."
    ),
)
async def health(request: Request) -> HealthResponse:
    """Report liveness, the limiter's state, Redis reachability, and two cheap vital signs.

    ``version`` is read off ``request.app.version`` rather than imported from ``src.main``:
    ``src.main`` imports this router, so importing back would be a cycle, and the app object is
    the authoritative source of its own version anyway.

    ``async`` because of the ping. The route still holds no locks and does no work beyond one
    bounded round trip, so it stays the cheap probe the container HEALTHCHECK polls every 10 s.
    """
    runtime = getattr(request.app.state, "runtime", None)
    uptime_sec = float(getattr(runtime, "uptime_sec", 0.0) or 0.0)
    return HealthResponse(
        status=STATUS_HEALTHY,
        rate_limiter=RATE_LIMITER_ACTIVE,
        version=str(getattr(request.app, "version", "")),
        uptime_sec=round(uptime_sec, 3),
        served_by=SERVED_BY,
        redis=await _probe_redis(runtime),
        config_version=_config_version(runtime),
    )

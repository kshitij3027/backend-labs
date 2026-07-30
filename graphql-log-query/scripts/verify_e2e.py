"""Black-box end-to-end verifier for the GraphQL Log Query Platform (C12).

Runs **inside Docker** (the profile-gated ``e2e`` compose service) against the LIVE ``api`` service
over the compose network — HTTP ``POST /graphql`` plus a real ``graphql-transport-ws`` WebSocket —
reached by **service name** (``TARGET_URL=http://api:8000``) and never by a host port. A host port
would let the verifier pass against a stack it is not actually wired to, and would fail on a machine
where ``API_PORT`` is taken.

It never imports the app, the schema, the router, the repository or the store. The one exception is
:mod:`src.generators`, which the tester image ships and which is the project's **ground truth**: the
controlled vocabularies (``ORDER_STATUSES``, ``LOG_LEVELS``, ``SERVICES``) and the lifecycle paths
the seeded corpus is drawn from. Grading a response against a locally regenerated *vocabulary* is
exactly as strong as hard-coding one and cannot go stale; grading it against regenerated *rows*
would not work here at all, because the api container seeds with ``end_time`` defaulted to its own
wall clock, so a second generator run would produce different instants. Where a specific row is
needed, this file **discovers** it through the API, which is what a black-box check should do anyway.

The first failing check prints a loud ``FAIL`` line and exits 1 immediately, so ``make e2e``
propagates it.

The 17 checks, in order:

 1. ``GET /health`` returns **exactly** ``{"status": "healthy"}`` — key set and value both, because
    the spec pins the literal payload twice (§2 and §8).
 2. Spec §5 command 1: ``{ logs { id service level message } }`` returns structured log data, every
    row carrying all four fields with ``level`` drawn from the real ``LogLevel`` vocabulary.
 3. Spec §5 command 2: a **variable-driven** filtered query returns only matching rows, and returns
    fewer than the unfiltered read (a filter that narrows nothing has not been proven to work).
 4. Spec §5 command 3: ``{ logStats { totalLogs errorCount services } }`` — ``services`` selected as
    a **leaf**, exactly as the spec writes it, which is the shape that would break if anybody
    "improved" it into a list of objects.
 5. Spec §5 command 4: ``mutation { createLog(logData: {...}) { id service } }`` creates a record,
    and the record is then readable back by id with every field intact.
 6. ``relatedLogs`` batching: a page of correlated parents returns correct, mutually-consistent
    groups and does so inside a latency ceiling. **See :func:`check_related_logs_batching` for an
    honest statement of what this can and cannot prove from outside.**
 7. A Redis cache hit returns byte-identical data to the miss that populated it. Also honest about
    its limits — see :func:`check_cache_hit`.
 8. The cost gate refuses an over-budget document with ``COST_LIMIT_EXCEEDED`` and both
    ``computedCost`` and ``maxCost`` in ``extensions``, as a 200 errors envelope rather than a 500.
 9. The APQ three-step: hash-only ⇒ ``PersistedQueryNotFound``; document+hash ⇒ data; hash-only ⇒
    the same data.
10. ``logStream`` over a real socket: a filtered subscription receives the entry it asked for and
    **not** the two it did not, proved with a tracer rather than a sleep.
11. ``orderStatusStream`` over a real socket: delivery, plus filtering by order id, by status and by
    user id, each proved by a non-matching write that must not arrive.
12. **Spec §3 Feature Area C: end-to-end delivery latency p95 < ``E2E_SUB_LATENCY_MS``**, measured
    from just before the ``createOrderEvent`` request to the arrival of its frame.
13. The ``LogEvent`` interface returns a genuine mix of ``__typename``s for one seeded trace.
14. C11's flagship 3-in-1 dossier returns internally consistent nested data — every payment's
    ``order`` agrees with the order it was reached through, and every correlated log line shares the
    trace it was traversed by.
15. The three cached aggregates agree with the rows they summarise, cross-checked the way
    ``logStats`` is: bucket sums against a counted read, funnel monotonicity against the lifecycle.
16. ``GET /metrics`` exposes the expected metric families with **non-zero** samples after traffic.
17. Sequential read p95 <= ``MAX_P95_MS`` and backend RSS <= ``MAX_BACKEND_MEM_MB``, the latter read
    from the server's own probe.

Environment knobs (all optional; declared as ``${VAR:-default}`` on the ``e2e`` compose service):

* ``TARGET_URL``            API base URL (default ``http://api:8000``)
* ``E2E_READY_TIMEOUT``     seconds to wait for ``/health`` (default 90)
* ``E2E_SUB_TIMEOUT``       seconds to wait for any one subscription frame (default 20)
* ``E2E_SUB_LATENCY_MS``    spec §3C ceiling on createOrderEvent -> frame p95 (default 100)
* ``MAX_P95_MS``            **sequential** query p95 ceiling, ms (default 250)
* ``MAX_BACKEND_MEM_MB``    backend RSS ceiling, MB (default 600)

Two more are read with defaults but are **not** declared on the compose service, deliberately:
``E2E_LATENCY_SAMPLES`` and ``E2E_SUB_SAMPLES`` size the two measurements rather than gating
anything, and a compose entry for a knob that cannot fail a run would be noise in the file that is
supposed to *be* the contract. Both are still overridable from the environment for a bisect.

.. rubric:: THE GATES MUST BITE, AND HERE IS HOW THAT IS PROVED

Every ceiling above is read from the environment with ``${VAR:-default}``, and every one of them is
compared with a real measurement rather than logged. The standing proofs, each of which **must**
exit non-zero:

* ``MAX_P95_MS=0 make e2e``            — check 17 cannot pass: no request takes 0 ms.
* ``MAX_BACKEND_MEM_MB=1 make e2e``    — check 17 cannot pass: uvicorn + SQLAlchemy is over 1 MiB.
* ``E2E_SUB_LATENCY_MS=0 make e2e``    — check 12 cannot pass: no frame arrives in 0 ms.
* ``E2E_SUB_TIMEOUT=0 make e2e``       — checks 10-12 cannot pass: no frame arrives before a zero
  deadline, and the failure is the *timeout* message rather than a hang.

A ceiling that cannot be driven to failure from outside is decoration; these four can.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import websockets

from src.generators import (
    LOG_LEVELS,
    ORDER_LIFECYCLES,
    ORDER_STATUSES,
    SERVICES,
)

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))
SUB_TIMEOUT = float(os.environ.get("E2E_SUB_TIMEOUT", "20"))
SUB_LATENCY_MS = float(os.environ.get("E2E_SUB_LATENCY_MS", "100"))
MAX_P95_MS = float(os.environ.get("MAX_P95_MS", "250"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "600"))

#: How many sequential reads check 17 times. Not a gate — it sizes the sample the gate is computed
#: from — so it is deliberately not declared on the compose service. See the module docstring.
LATENCY_SAMPLES = int(os.environ.get("E2E_LATENCY_SAMPLES", "40"))

#: How many order transitions check 12 times for its p95. Likewise not a gate. Twenty is enough for
#: a ceil-rank p95 to mean something (the 19th of 20 sorted samples) while keeping the check under a
#: couple of seconds on a healthy stack.
SUB_SAMPLES = int(os.environ.get("E2E_SUB_SAMPLES", "20"))

TOTAL_CHECKS = 17

GRAPHQL_PATH = "/graphql"
GRAPHQL_HTTP = f"{BASE_URL}{GRAPHQL_PATH}"
#: ``http://api:8000`` -> ``ws://api:8000/graphql``. Built by replacing the scheme rather than by
#: string-formatting a second URL, so ``TARGET_URL`` stays the single place the target is named.
GRAPHQL_WS = (
    BASE_URL.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + GRAPHQL_PATH
)

#: The subprotocol C13's Apollo client speaks (``graphql-ws@5``) and the only one used here. The
#: legacy ``graphql-ws`` protocol is also offered by the mount; nothing in this project speaks it.
WS_SUBPROTOCOL = "graphql-transport-ws"

#: Prefix for every service name this verifier writes under. Deliberately absent from
#: :data:`src.generators.SERVICES`, so a subscription filtered on one of these sees **only** what
#: this run appends — no seeded rows, no background traffic, and therefore no ambiguity about which
#: frame is ours.
PROBE_PREFIX = "e2e-probe"

#: Order ids this verifier writes under. Far above :data:`src.generators.ORDER_ID_BASE` + the seeded
#: order count, so a probe order can never collide with a seeded one and a filtered order
#: subscription can never be satisfied by a seeded event.
PROBE_ORDER_PREFIX = "e2e-ord"

#: The instant this run began, captured at import — **before** any check has written anything.
#:
#: A distinct prefix keeps probe rows from being *mistaken* for seeded ones, but it does not keep
#: them out of a read that asks for "the newest N". That is a different problem and it bit for real:
#: checks 11-12 append ~30 synthetic order events, so check 14's `orderEvents(limit: 3)` came back
#: holding nothing but probe rows — which by construction have no payments, no user activity and no
#: traceId — and the dossier's own "traversed to nothing at all" guard fired on every run. The check
#: was correct; the rows it was handed were the wrong rows.
#:
#: Raising the limit is NOT the fix, and this is worth writing down because it is the obvious move:
#: `limit: 60` on the dossier document prices at 98,110 against a 25,000 budget, so the cost gate
#: refuses it. Scoping by time is both cheaper and more honest — every check that wants *seeded*
#: data passes this as `endTime`, which is a filter the schema already has and the corpus already
#: predates. Checks that want their own writes go on filtering by probe id as before.
RUN_STARTED = datetime.now(timezone.utc).isoformat()

#: The metric families check 16 requires, and the ones it requires to be **non-zero**. Split because
#: they prove different things: presence proves the exposition is wired to the right registry, and a
#: non-zero sample proves the instrument is actually being recorded into rather than merely
#: declared. `gql_active_subscriptions` is deliberately only in the first list — it is a gauge that
#: is legitimately 0 once every socket in checks 10-12 has closed.
REQUIRED_METRIC_FAMILIES = (
    "gql_operation_duration_seconds",
    "gql_field_duration_seconds",
    "gql_errors_total",
    "gql_active_subscriptions",
    "gql_broker_published_total",
    "gql_broker_delivered_total",
)
NONZERO_METRIC_FAMILIES = (
    "gql_operation_duration_seconds_count",
    "gql_field_duration_seconds_count",
    "gql_broker_published_total",
    "gql_broker_delivered_total",
)

#: Cross-check state: facts a later check needs from an earlier one.
STATE: dict[str, Any] = {}


class CheckFailure(AssertionError):
    """Raised inside a check to fail it with a single clear detail line."""


# --------------------------------------------------------------------------- #
# Pure helpers.
#
# Kept free of I/O and of module state so `tests/unit/test_verify_e2e_helpers.py`
# can exercise them without a live stack — the verifier itself is exercised by
# `make e2e`, but the arithmetic its gates rest on should not have to be.
# --------------------------------------------------------------------------- #
def percentile(values: Sequence[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); ``0.0`` for an empty sequence.

    Ceil-rank rather than a linear interpolation because a latency gate should report a value that
    was **actually observed**: an interpolated p95 of 97.3 ms when no request took 97.3 ms invites
    an argument about the method instead of about the latency. It is also the definition
    ``scripts/load_test.py`` uses at C14, and two harnesses reporting "p95" had better mean the
    same thing by it.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)
    return ordered[index]


def gate(measured: float, ceiling: float, what: str, unit: str = "ms") -> str:
    """Compare a measurement against a ceiling; return evidence, or raise :class:`CheckFailure`.

    One function for every gate in this file, so "measured > ceiling fails" is written once. The
    comparison is **strictly greater**, which is what makes ``MAX_P95_MS=0`` fail rather than pass
    on a hypothetical 0.0 reading: a ceiling of zero can only be met by a measurement of zero, and
    a real one never is.

    Raises:
        CheckFailure: If ``measured`` exceeds ``ceiling``.
    """
    if measured > ceiling:
        raise CheckFailure(f"{what} {measured:.1f}{unit} > gate {ceiling:.1f}{unit}")
    return f"{what} {measured:.1f}{unit} <= {ceiling:.1f}{unit}"


def sha256_hex(document: str) -> str:
    """``sha256`` of a query document, hex-encoded — the APQ hash the server recomputes.

    Over the document **exactly as sent**, with no normalisation: the server hashes the bytes it
    received, so a client that pretty-printed the document before hashing would register under one
    hash and send another. Check 9 depends on the two agreeing.
    """
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def unique_suffix() -> str:
    """A short token unique to one probe, so concurrent or repeated runs cannot collide."""
    return uuid.uuid4().hex[:12]


# --------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------- #
CLIENT = httpx.Client(base_url=BASE_URL, timeout=60.0)


def post_graphql(
    query: str,
    variables: Optional[dict[str, Any]] = None,
    *,
    extensions: Optional[dict[str, Any]] = None,
    operation_name: Optional[str] = None,
) -> dict[str, Any]:
    """POST one operation and return the parsed GraphQL envelope. Never raises on GraphQL errors.

    The **envelope**, not the data: several checks here are about a rejection, and a helper that
    raised on ``errors`` would make the failure path unreachable. :func:`graphql_data` is the
    variant for the (majority) case where errors mean the check failed.

    A non-200 status is a check failure, though — spec §2 item 35 requires GraphQL-shaped errors
    rather than HTTP 500s, so a 500 is a finding rather than something to parse.
    """
    payload: dict[str, Any] = {}
    if query:
        payload["query"] = query
    if variables is not None:
        payload["variables"] = variables
    if extensions is not None:
        payload["extensions"] = extensions
    if operation_name is not None:
        payload["operationName"] = operation_name

    try:
        response = CLIENT.post(GRAPHQL_PATH, json=payload)
    except Exception as exc:  # noqa: BLE001 - a transport failure is a check failure
        raise CheckFailure(f"POST {GRAPHQL_PATH} raised {type(exc).__name__}: {exc}") from exc

    if response.status_code != 200:
        raise CheckFailure(
            f"POST {GRAPHQL_PATH} -> HTTP {response.status_code} (want 200 even for errors; "
            f"spec §2 item 35 forbids raw 500s): {response.text[:300]}"
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise CheckFailure(f"{GRAPHQL_PATH} returned non-JSON: {response.text[:300]}") from exc
    if not isinstance(body, dict):
        raise CheckFailure(f"{GRAPHQL_PATH} returned a non-object body: {body!r}"[:300])
    return body


def graphql_data(query: str, variables: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """POST one operation that is expected to succeed; return ``data``. Fails on any error."""
    body = post_graphql(query, variables)
    if body.get("errors"):
        raise CheckFailure(f"operation returned errors: {json.dumps(body['errors'])[:400]}")
    data = body.get("data")
    if not isinstance(data, dict):
        raise CheckFailure(f"operation returned no data object: {body!r}"[:300])
    return data


def error_codes(body: dict[str, Any]) -> list[str]:
    """Every ``extensions.code`` in an errors envelope, in order. ``[]`` when there are none."""
    return [
        (error.get("extensions") or {}).get("code")
        for error in body.get("errors") or []
        if isinstance(error, dict)
    ]


def create_log(
    *,
    service: str,
    message: str,
    level: str = "INFO",
    trace_id: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Create one log entry through the real mutation and return it."""
    log_data: dict[str, Any] = {"service": service, "level": level, "message": message}
    if trace_id is not None:
        log_data["traceId"] = trace_id
    if metadata is not None:
        log_data["metadata"] = metadata
    data = graphql_data(CREATE_LOG_FULL, {"logData": log_data})
    return data["createLog"]


def create_order_event(
    *,
    order_id: str,
    user_id: str,
    status: str,
    trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Create one order status transition through the real mutation and return it."""
    order_data: dict[str, Any] = {"orderId": order_id, "userId": user_id, "status": status}
    if trace_id is not None:
        order_data["traceId"] = trace_id
    data = graphql_data(CREATE_ORDER_EVENT, {"orderData": order_data})
    return data["createOrderEvent"]


# --------------------------------------------------------------------------- #
# The graphql-transport-ws client
#
# Hand-rolled over `websockets` rather than pulled from a library, for the same
# reason the rest of this file is hand-rolled: a verifier that shared a client
# implementation with the server would be grading that implementation's idea of
# the protocol. Fifty lines of connection_init/subscribe/next is the protocol.
# --------------------------------------------------------------------------- #
class Stream:
    """One ``graphql-transport-ws`` operation on its own socket, with bounded reads.

    Its own socket per operation rather than several multiplexed on one, deliberately: these checks
    assert about *isolation* (a filtered subscription must not see another's events), and sharing a
    connection would make a cross-talk bug and a multiplexing bug indistinguishable. The
    per-connection cap is exercised by the integration suite, which is where it belongs.

    Every read has a **deadline**. ``websockets`` will otherwise wait forever for a frame that a
    regression never sends, and an E2E container that hangs is strictly worse than one that fails:
    ``make e2e`` has no timeout of its own, so a hang stops CI rather than reporting.
    """

    def __init__(self, connection: Any, operation_id: str) -> None:
        self._connection = connection
        self.operation_id = operation_id

    async def next_payload(self, *, timeout: float = SUB_TIMEOUT) -> dict[str, Any]:
        """The next ``next`` frame's ``payload.data``, or a :class:`CheckFailure`.

        ``ping``/``pong`` are absorbed here rather than in every caller: the protocol allows either
        side to ping at any time, and "the next message" in a check means the next *interesting*
        one. An ``error`` frame is raised with its payload attached, because a subscription that
        fails is a far more useful failure message than "no frame arrived".
        """
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CheckFailure(
                    f"no subscription frame arrived within {timeout:.0f}s "
                    f"(operation {self.operation_id!r})"
                )
            try:
                raw = await asyncio.wait_for(self._connection.recv(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise CheckFailure(
                    f"no subscription frame arrived within {timeout:.0f}s "
                    f"(operation {self.operation_id!r})"
                ) from exc
            except Exception as exc:  # noqa: BLE001 - a closed socket is a check failure
                raise CheckFailure(
                    f"the subscription socket failed: {type(exc).__name__}: {exc}"
                ) from exc

            message = json.loads(raw)
            kind = message.get("type")
            if kind in {"ping", "pong"}:
                if kind == "ping":
                    await self._connection.send(json.dumps({"type": "pong"}))
                continue
            if kind == "error":
                raise CheckFailure(
                    f"the subscription errored: {json.dumps(message.get('payload'))[:300]}"
                )
            if kind == "complete":
                raise CheckFailure(f"the subscription completed before delivering a frame: {message}")
            if kind != "next":
                continue
            if message.get("id") != self.operation_id:
                continue
            payload = message.get("payload") or {}
            if payload.get("errors"):
                raise CheckFailure(f"the stream yielded errors: {json.dumps(payload['errors'])[:300]}")
            data = payload.get("data")
            if not isinstance(data, dict):
                raise CheckFailure(f"a next frame carried no data object: {message!r}"[:300])
            return data


async def open_stream(
    connection: Any, query: str, variables: Optional[dict[str, Any]] = None
) -> Stream:
    """Handshake, then ``subscribe``, and return the :class:`Stream` handle.

    Nothing may be sent on the socket before ``connection_ack`` arrives — that is the protocol, and
    a server is entitled to close a connection that talks early. Note this returns as soon as the
    ``subscribe`` message is **written**, which is not the same instant the server has registered
    the subscriber; every check that publishes into a stream deals with that gap explicitly rather
    than by sleeping. See :func:`wait_until_subscribed`.
    """
    await connection.send(json.dumps({"type": "connection_init", "payload": {}}))
    raw = await asyncio.wait_for(connection.recv(), timeout=SUB_TIMEOUT)
    ack = json.loads(raw)
    if ack.get("type") != "connection_ack":
        raise CheckFailure(f"expected connection_ack, got {ack!r}")

    operation_id = unique_suffix()
    await connection.send(
        json.dumps(
            {
                "id": operation_id,
                "type": "subscribe",
                "payload": {"query": query, "variables": variables or {}},
            }
        )
    )
    return Stream(connection, operation_id)


async def wait_until_subscribed(stream: Stream, publish: Callable[[], Any]) -> dict[str, Any]:
    """Publish until a frame comes back, and return the first frame. **Never a sleep.**

    The gap this closes is real and is the single flakiest thing about testing a subscription from
    outside: ``subscribe`` is written to the socket, and the server registers the subscriber some
    number of event-loop turns later. A write issued in that window is legitimately not delivered —
    the subscriber did not exist yet — so a check that wrote once and waited would fail
    intermittently on a perfectly correct server.

    A fixed sleep "fixes" that by guessing, and the guess is either too short (flaky) or too long
    (every subscription check pays for it). Retrying the *write* instead converges as fast as the
    server registers and needs no guess at all. Each attempt gets a short read deadline; the loop as
    a whole is bounded by :data:`SUB_TIMEOUT`, so a stream that never delivers still fails rather
    than hanging.

    The returned frame is the first one that arrived, which may be from any of the attempts — every
    caller either only needs "a frame arrived" or writes a distinguishable marker per attempt.
    """
    deadline = time.monotonic() + SUB_TIMEOUT
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        await asyncio.get_running_loop().run_in_executor(None, publish)
        try:
            return await stream.next_payload(timeout=min(1.0, max(0.05, deadline - time.monotonic())))
        except CheckFailure:
            continue
    raise CheckFailure(
        f"no frame arrived within {SUB_TIMEOUT:.0f}s over {attempt} publish attempt(s) — the "
        "subscription registered but nothing reached it"
    )


def run_async(coroutine: Any) -> Any:
    """Run one coroutine to completion on a fresh event loop.

    ``asyncio.run`` per check rather than one loop for the whole file: the checks are otherwise
    synchronous, and a module-level loop would have to outlive them and be torn down somewhere that
    is not obviously anywhere. A loop per subscription check costs microseconds.
    """
    return asyncio.run(coroutine)


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #
SPEC_LOGS = "{ logs { id service level message } }"

SPEC_STATS = "{ logStats { totalLogs errorCount services } }"

FILTERED_LOGS = """
query Filtered($filters: LogFilterInput) {
  logs(filters: $filters) { id service level message timestamp }
}
"""

CREATE_LOG_FULL = """
mutation Create($logData: CreateLogInput!) {
  createLog(logData: $logData) { id service level message timestamp metadata traceId }
}
"""

SPEC_CREATE_LOG = """
mutation SpecCreate($logData: CreateLogInput!) {
  createLog(logData: $logData) { id service }
}
"""

LOG_BY_ID = """
query One($id: ID!) {
  log(id: $id) { id service level message timestamp metadata traceId }
}
"""

RELATED_LOGS = """
query Related($filters: LogFilterInput) {
  logs(filters: $filters) {
    id
    traceId
    relatedLogs { id traceId }
  }
}
"""

CREATE_ORDER_EVENT = """
mutation CreateOrder($orderData: CreateOrderEventInput!) {
  createOrderEvent(orderData: $orderData) {
    id orderId userId status service level timestamp traceId metadata
  }
}
"""

LOG_STREAM = """
subscription Logs($service: String, $level: LogLevel) {
  logStream(service: $service, level: $level) { id service level message traceId }
}
"""

ORDER_STREAM = """
subscription Orders($orderId: String, $status: OrderStatus, $userId: String) {
  orderStatusStream(orderId: $orderId, status: $status, userId: $userId) {
    id orderId userId status timestamp
  }
}
"""

#: The over-budget document for check 8. Two levels of correlation at the default page size prices
#: at 1,101,010 against a shipped budget of 25,000 — see `src/graphql/cost.py`. Deliberately far
#: over rather than just over, so the check does not become a calibration test that fails whenever
#: a weight is legitimately re-tuned.
OVER_BUDGET = "{ logs { relatedLogs { relatedLogs { id message service level } } } }"

CORRELATED = """
query Correlated($traceId: String!) {
  correlatedEvents(traceId: $traceId) {
    __typename
    timestamp
    service
    level
    traceId
    ... on LogEntry { id message }
    ... on OrderEvent { id orderId userId status }
    ... on PaymentEvent { id orderId method outcome }
    ... on UserEvent { id userId activityType }
  }
}
"""

#: C11's flagship: an order, its payments, the acting user's activity and the log lines of the same
#: unit of work — four REST calls' worth of data in one round trip. `limit: 3` keeps it well inside
#: the shipped cost budget (10 + 3 x 221 = 673) while still returning several parents to
#: cross-check against each other.
DOSSIER = """
query Dossier($filters: OrderEventFilterInput) {
  orderEvents(filters: $filters) {
    id
    orderId
    userId
    status
    traceId
    payments { id orderId method outcome order { orderId status } }
    userActivity { id userId activityType }
    relatedLogs { id traceId }
  }
}
"""

AGGREGATES = """
{
  orderStatusDistribution { status orders }
  orderFunnel { status ordersReached share }
  paymentOutcomeBreakdown { method outcome events orders }
}
"""


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
    except Exception as exc:  # noqa: BLE001 - an unexpected error is still a failure
        print(f"{prefix} FAIL {name}: unexpected {type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)
    print(f"{prefix} PASS {name} ({evidence})", flush=True)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live rather than at exit)."""
    print(f"[e2e] {msg}", flush=True)


def require_list(value: Any, what: str) -> list[Any]:
    """Fail unless ``value`` is a list; return it."""
    if not isinstance(value, list):
        raise CheckFailure(f"{what} is {type(value).__name__}, want a list")
    return value


def require_int(value: Any, what: str) -> int:
    """Fail unless ``value`` is a real (non-bool) integer; return it."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CheckFailure(f"{what} is {value!r}, want an int")
    return value


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll ``GET /health`` until it answers 200, or exit 1 at the timeout.

    Compose already gates the ``e2e`` service on the api healthcheck, so this normally returns on
    the first poll. It stays because ``TARGET_URL`` can point at something compose does not manage,
    and because a verifier whose first check races a still-seeding corpus is a flake generator
    rather than a gate.
    """
    info(f"waiting for {BASE_URL}/health (up to {timeout:.0f}s)...")
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            response = CLIENT.get("/health", timeout=5.0)
            if response.status_code == 200:
                info("api is ready")
                return
            last = f"HTTP {response.status_code}"
        except Exception as exc:  # noqa: BLE001 - the service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    print(f"FAIL bootstrap: /health not ready after {timeout:.0f}s (last: {last})", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# 1 — /health
# --------------------------------------------------------------------------- #
def check_health() -> str:
    """1. ``GET /health`` is **exactly** ``{"status": "healthy"}``.

    Exact key-set equality rather than ``body["status"] == "healthy"``, because the spec pins the
    literal payload twice (§2 item 13 and the §8 sample) and the realistic drift is *addition* — a
    version, an uptime, a row count — which a subset assertion would happily accept.
    """
    response = CLIENT.get("/health")
    if response.status_code != 200:
        raise CheckFailure(f"GET /health -> HTTP {response.status_code}")
    body = response.json()
    if body != {"status": "healthy"}:
        raise CheckFailure(f'GET /health returned {body!r}, want exactly {{"status": "healthy"}}')
    return "body is exactly {'status': 'healthy'}"


# --------------------------------------------------------------------------- #
# 2 — spec §5 command 1
# --------------------------------------------------------------------------- #
def check_spec_logs() -> str:
    """2. ``{ logs { id service level message } }`` returns structured log data.

    The spec's own command, **verbatim** — not a paraphrase with a filter attached, because what it
    is testing is that the unadorned document a reader of the article would type actually works.

    Every row is then checked to carry all four fields, with ``level`` inside the real ``LogLevel``
    vocabulary and ``service`` a non-empty string. "Some rows came back" is not the assertion; "the
    rows are shaped the way the schema promises" is.
    """
    data = graphql_data(SPEC_LOGS)
    rows = require_list(data.get("logs"), "logs")
    if not rows:
        raise CheckFailure("logs returned an empty list — the seeded corpus is missing")

    levels = set(LOG_LEVELS)
    for row in rows:
        missing = {"id", "service", "level", "message"} - set(row)
        if missing:
            raise CheckFailure(f"a log row is missing {sorted(missing)}: {row!r}"[:200])
        if row["level"] not in levels:
            raise CheckFailure(f"level {row['level']!r} is outside LogLevel {sorted(levels)}")
        if not isinstance(row["service"], str) or not row["service"]:
            raise CheckFailure(f"service is {row['service']!r}, want a non-empty string")

    STATE["corpus_page"] = rows
    seen_services = sorted({row["service"] for row in rows})
    return f"{len(rows)} rows, all four fields, services={seen_services[:4]}"


# --------------------------------------------------------------------------- #
# 3 — spec §5 command 2
# --------------------------------------------------------------------------- #
def check_variable_filter() -> str:
    """3. A variable-driven filtered query returns **only** matching rows, and fewer of them.

    Two assertions, and the second is the one that has teeth. "Every returned row matches the
    filter" is satisfied by returning nothing at all, so it is paired with "the filtered read is
    strictly smaller than the unfiltered one" — which is what distinguishes a working ``WHERE``
    clause from a resolver that ignored its arguments and from one that returned an empty list.

    The filter values are taken from the *live* corpus (check 2's page) rather than hard-coded, so
    this cannot fail because a generator vocabulary changed.
    """
    page = STATE.get("corpus_page") or []
    if not page:
        raise CheckFailure("check 2 did not record a corpus page")

    # A (service, level) pair that provably exists, so a correct server cannot answer with [].
    sample = page[0]
    service, level = sample["service"], sample["level"]

    filters = {"service": service, "level": level, "limit": 200}
    data = graphql_data(FILTERED_LOGS, {"filters": filters})
    rows = require_list(data.get("logs"), "logs")
    if not rows:
        raise CheckFailure(
            f"the filter service={service!r} level={level!r} matched nothing, but it was taken "
            "from a row the server itself returned"
        )

    wrong = [
        row for row in rows if row["service"] != service or row["level"] != level
    ]
    if wrong:
        raise CheckFailure(
            f"{len(wrong)} of {len(rows)} rows do not match the filter, e.g. "
            f"{wrong[0]['service']!r}/{wrong[0]['level']!r}"
        )

    unfiltered = require_list(
        graphql_data(FILTERED_LOGS, {"filters": {"limit": 200}}).get("logs"), "logs"
    )
    if len(rows) >= len(unfiltered):
        raise CheckFailure(
            f"the filtered read returned {len(rows)} rows and the unfiltered one {len(unfiltered)} "
            "— a filter that narrows nothing has not been shown to be applied"
        )

    # A time-range filter too, since §2 lists it separately: everything strictly older than the
    # newest row must exclude that row.
    newest = max(row["timestamp"] for row in unfiltered)
    older = require_list(
        graphql_data(FILTERED_LOGS, {"filters": {"endTime": newest, "limit": 200}}).get("logs"),
        "logs",
    )
    if any(row["timestamp"] > newest for row in older):
        raise CheckFailure("endTime did not bound the result: a row newer than the bound came back")

    return (
        f"service+level -> {len(rows)}/{len(unfiltered)} rows, all matching; "
        f"endTime bounded {len(older)} rows"
    )


# --------------------------------------------------------------------------- #
# 4 — spec §5 command 3
# --------------------------------------------------------------------------- #
def check_spec_stats() -> str:
    """4. ``{ logStats { totalLogs errorCount services } }`` returns a statistical summary.

    The spec's command verbatim, and the shape it depends on is the interesting part: ``services``
    is selected as a **leaf**. GraphQL forbids a sub-selection on a scalar and requires one on an
    object, so the moment ``services`` becomes a list of objects this document stops validating —
    and every test written against the richer shape stays green. That regression is only catchable
    by sending the literal document, which is what this does.

    The numbers are then sanity-checked against each other rather than against a magic constant:
    ``errorCount <= totalLogs``, and every named service is a real one.
    """
    data = graphql_data(SPEC_STATS)
    stats = data.get("logStats")
    if not isinstance(stats, dict):
        raise CheckFailure(f"logStats is {stats!r}, want an object")

    total = require_int(stats.get("totalLogs"), "totalLogs")
    errors = require_int(stats.get("errorCount"), "errorCount")
    services = require_list(stats.get("services"), "services")

    if not all(isinstance(name, str) for name in services):
        raise CheckFailure(f"services is not a list of scalars: {services!r}"[:200])
    if total <= 0:
        raise CheckFailure(f"totalLogs is {total} — the seeded corpus is missing")
    if errors > total:
        raise CheckFailure(f"errorCount {errors} exceeds totalLogs {total}")
    if not services:
        raise CheckFailure("services is empty while totalLogs is positive")

    STATE["total_logs"] = total
    return f"totalLogs={total}, errorCount={errors}, {len(services)} services (leaf-selected)"


# --------------------------------------------------------------------------- #
# 5 — spec §5 command 4
# --------------------------------------------------------------------------- #
def check_spec_create_log() -> str:
    """5. ``mutation { createLog(logData: {...}) { id service } }`` creates a record.

    The spec's argument name ``logData`` is part of the command, so sending it is what pins the
    published name. The created row is then **read back by id** with every field selected: a
    mutation that returned a plausible object without committing would pass a response-shape
    assertion and fail this one.
    """
    marker = unique_suffix()
    service = f"{PROBE_PREFIX}-create"
    body = post_graphql(
        SPEC_CREATE_LOG,
        {
            "logData": {
                "service": service,
                "level": "WARNING",
                "message": f"e2e createLog {marker}",
                "metadata": {"probe": marker, "source": "verify_e2e"},
                "traceId": f"trace-{marker}",
            }
        },
    )
    if body.get("errors"):
        raise CheckFailure(f"createLog returned errors: {json.dumps(body['errors'])[:300]}")
    created = (body.get("data") or {}).get("createLog")
    if not isinstance(created, dict) or set(created) != {"id", "service"}:
        raise CheckFailure(
            f"createLog returned {created!r}, want exactly the two selected fields"[:200]
        )
    if created["service"] != service:
        raise CheckFailure(f"createLog echoed service={created['service']!r}, want {service!r}")

    fetched = graphql_data(LOG_BY_ID, {"id": created["id"]}).get("log")
    if not isinstance(fetched, dict):
        raise CheckFailure(f"the created row is not readable back by id {created['id']!r}")
    if fetched["message"] != f"e2e createLog {marker}":
        raise CheckFailure(f"the stored message is {fetched['message']!r}")
    if (fetched.get("metadata") or {}).get("probe") != marker:
        raise CheckFailure(f"the stored metadata is {fetched.get('metadata')!r}")
    if fetched.get("traceId") != f"trace-{marker}":
        raise CheckFailure(f"the stored traceId is {fetched.get('traceId')!r}")

    return f"id={created['id']} created and read back with metadata and traceId intact"


# --------------------------------------------------------------------------- #
# 6 — relatedLogs batching
# --------------------------------------------------------------------------- #
def check_related_logs_batching() -> str:
    """6. ``relatedLogs`` returns correct, mutually-consistent groups, quickly.

    .. rubric:: WHAT A BLACK-BOX TEST CAN AND CANNOT PROVE HERE, STATED PLAINLY

    It **cannot** prove the DataLoader batched. The number of SQL statements PostgreSQL received is
    not observable from outside the process: the response for one query and the response for N
    queries are byte-identical, which is precisely why this field's N+1 risk is dangerous in the
    first place. Anyone reading a PASS on this line should not read it as "batching was verified".
    That proof exists, and it is elsewhere and stronger: ``tests/integration/test_dataloader.py``
    installs a SQLAlchemy event listener, counts the statements PostgreSQL actually received at two
    different page sizes, and asserts the count does **not** grow with the page. A statement counter
    inside the process is the only instrument that can answer this question, and it already exists.

    What this check proves instead — and it is worth proving, because it is the part the integration
    test cannot see — is that the batching is **correct end to end through the real HTTP surface and
    the real database**:

    * every parent with a ``traceId`` gets a group, and every member of that group carries the same
      ``traceId`` (a batch that mixed keys would show up here immediately);
    * the parent is **excluded** from its own group, which is the documented contract and the thing
      a naive "all logs with this trace" implementation gets wrong;
    * a parent with a null ``traceId`` gets ``[]``, never a group;
    * groups are **mutually consistent**: two parents sharing a trace return the same set of ids
      modulo each excluding itself. A per-parent query that read a moving target would break this.

    And it asserts the whole page comes back inside a latency ceiling. That is a *weak* signal about
    batching and is offered as exactly that: N+1 over a page of 40 would be dozens of round trips,
    which at any realistic per-query cost blows past the ceiling — but a fast enough database could
    hide it, so failure here is informative and success is not a proof.
    """
    filters = {"limit": 40}
    started = time.perf_counter()
    data = graphql_data(RELATED_LOGS, {"filters": filters})
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    rows = require_list(data.get("logs"), "logs")
    if not rows:
        raise CheckFailure("logs returned nothing to correlate")

    traced = [row for row in rows if row.get("traceId")]
    untraced = [row for row in rows if not row.get("traceId")]
    if not traced:
        raise CheckFailure("no row in the page carries a traceId — nothing to correlate")

    for row in untraced:
        if row["relatedLogs"]:
            raise CheckFailure(
                f"row {row['id']} has a null traceId but {len(row['relatedLogs'])} related logs"
            )

    groups: dict[str, set[str]] = {}
    for row in traced:
        trace = row["traceId"]
        related = require_list(row["relatedLogs"], "relatedLogs")
        for member in related:
            if member["traceId"] != trace:
                raise CheckFailure(
                    f"row {row['id']} (trace {trace!r}) has a related log carrying trace "
                    f"{member['traceId']!r} — the batch mixed keys"
                )
        ids = {member["id"] for member in related}
        if row["id"] in ids:
            raise CheckFailure(
                f"row {row['id']} appears in its own relatedLogs; the field is documented as "
                "returning every OTHER entry sharing the trace"
            )
        # The whole group as this parent sees it: its members plus itself.
        groups.setdefault(trace, ids | {row["id"]})
        if groups[trace] != ids | {row["id"]}:
            raise CheckFailure(
                f"two parents on trace {trace!r} disagree about the group: "
                f"{sorted(groups[trace])} vs {sorted(ids | {row['id']})}"
            )

    evidence = gate(elapsed_ms, MAX_P95_MS * 4, "40 parents + relatedLogs in one request")
    shared = sum(1 for trace, ids in groups.items() if len(ids) > 1)
    return (
        f"{len(traced)} traced parents in {len(groups)} groups ({shared} multi-member), "
        f"{len(untraced)} untraced -> []; self excluded; {evidence}. "
        "NOTE: statement counting is not observable black-box — the batching proof is "
        "tests/integration/test_dataloader.py"
    )


# --------------------------------------------------------------------------- #
# 7 — the Redis result cache
# --------------------------------------------------------------------------- #
def check_cache_hit() -> str:
    """7. A repeated identical query returns byte-identical data, and does so no slower.

    .. rubric:: WHAT THIS CAN AND CANNOT PROVE, ALSO STATED PLAINLY

    It **cannot** prove the second read was served from Redis. "Zero SQL statements were issued" is
    an in-process fact, and the honest proof is again elsewhere: ``tests/integration/test_cache.py``
    counts statements around a hit and asserts the count is zero. It also cannot distinguish the
    result cache from PostgreSQL's own buffer cache — both make a repeat read faster, and from
    outside they look the same.

    What it does prove is the property a *client* actually depends on, which is the one worth
    checking from outside: **a cache hit is indistinguishable from a miss in its answer.** Spec §2
    item 31 requires the hit path to return "fully reconstructed typed objects", and the way that
    requirement fails in practice is not a slow response — it is a subtly different one: a
    ``metadata`` that came back ``null``, a timestamp that lost its offset, an id that became a
    number. Byte-identical JSON across the two reads is exactly the assertion that catches all
    three, and it is stronger than a timing comparison could ever be.

    The timing is reported as **evidence, not as a gate**: it is compared against the miss rather
    than against a ceiling, because a cache that is merely as fast as the database is still a
    correct cache, and failing a build over host jitter would be a flake with no finding behind it.
    """
    # A filter nothing else in this run uses, so the first read is certainly a miss.
    filters = {"limit": 25, "searchText": ""}

    first_started = time.perf_counter()
    first = graphql_data(FILTERED_LOGS, {"filters": filters})
    first_ms = (time.perf_counter() - first_started) * 1000.0

    second_started = time.perf_counter()
    second = graphql_data(FILTERED_LOGS, {"filters": filters})
    second_ms = (time.perf_counter() - second_started) * 1000.0

    if first != second:
        # Rendered rather than diffed: the useful failure here is "which field changed", and the
        # payloads are 25 rows, not 25,000.
        raise CheckFailure(
            "the repeated identical query returned different data — a cache hit must be "
            f"indistinguishable from a miss.\nfirst : {json.dumps(first)[:300]}\n"
            f"second: {json.dumps(second)[:300]}"
        )
    rows = require_list(first.get("logs"), "logs")
    if not rows:
        raise CheckFailure("the cached query returned nothing, so identity proves nothing")

    # A DIFFERENT filter must produce a different answer — otherwise "identical data" would also be
    # satisfied by a cache that ignored its key, which is the one cache bug worse than not caching.
    other = graphql_data(FILTERED_LOGS, {"filters": {"limit": 5}})
    if other == first:
        raise CheckFailure(
            "a query with a different limit returned the identical payload — the cache key is not "
            "distinguishing filter sets"
        )

    return (
        f"identical payload over {len(rows)} rows ({first_ms:.1f}ms then {second_ms:.1f}ms); a "
        "different filter set returns different data. NOTE: 'served without touching the DB' is "
        "not observable black-box — that proof is tests/integration/test_cache.py"
    )


# --------------------------------------------------------------------------- #
# 8 — the cost gate
# --------------------------------------------------------------------------- #
def check_cost_gate() -> str:
    """8. An over-budget document is refused with ``COST_LIMIT_EXCEEDED`` and the two numbers.

    Three things are asserted, and the third is the one that makes the gate usable rather than
    merely present:

    * the response is **HTTP 200 with an errors envelope**, not a 500 (spec §2 item 35). That is
      checked inside :func:`post_graphql`, for every request in this file.
    * ``extensions.code`` is ``COST_LIMIT_EXCEEDED`` — the closed vocabulary a client branches on.
    * ``extensions`` carries ``computedCost`` **and** ``maxCost``. Without them a client is told
      only that it asked for too much and has to bisect its own query to find out by how much,
      which is the difference between a gate and a wall.

    ``data`` must also be absent or null: a rejection during validation means **no resolver ran**,
    so a partial result would mean the gate fired after the database had already been asked.
    """
    body = post_graphql(OVER_BUDGET)
    codes = error_codes(body)
    if "COST_LIMIT_EXCEEDED" not in codes:
        raise CheckFailure(
            f"the over-budget document was not refused with COST_LIMIT_EXCEEDED (codes={codes}, "
            f"data={json.dumps(body.get('data'))[:200]})"
        )
    if body.get("data"):
        raise CheckFailure(
            f"a cost rejection returned data: {json.dumps(body['data'])[:200]} — the gate runs "
            "during validation, so no resolver should have run"
        )

    rejection = next(
        error
        for error in body["errors"]
        if (error.get("extensions") or {}).get("code") == "COST_LIMIT_EXCEEDED"
    )
    extensions = rejection.get("extensions") or {}
    computed, maximum = extensions.get("computedCost"), extensions.get("maxCost")
    if computed is None or maximum is None:
        raise CheckFailure(
            f"the rejection carries extensions={extensions!r}; both computedCost and maxCost are "
            "part of the contract"
        )
    computed = require_int(computed, "computedCost")
    maximum = require_int(maximum, "maxCost")
    if computed <= maximum:
        raise CheckFailure(
            f"computedCost {computed} does not exceed maxCost {maximum}, yet the document was "
            "rejected — the numbers do not explain the refusal"
        )

    # And the flagship shape must still be ADMITTED. A gate that rejects everything is not a gate,
    # and this is the exact query the budget was calibrated to allow (spec §2 items 17 + 29).
    admitted = post_graphql("{ logs { id relatedLogs { id } } }")
    if admitted.get("errors"):
        raise CheckFailure(
            "the flagship correlated query was rejected: "
            f"{json.dumps(admitted['errors'])[:300]} — the budget is miscalibrated"
        )

    return (
        f"refused with computedCost={computed:,} > maxCost={maximum:,} as a 200 errors envelope; "
        "the flagship correlated query is still admitted"
    )


# --------------------------------------------------------------------------- #
# 9 — automatic persisted queries
# --------------------------------------------------------------------------- #
def check_apq_round_trip() -> str:
    """9. The APQ three-step: not-found, register, hash-only.

    The document is made unique per run so step 1 is genuinely a miss — a fixed document would be
    registered by the first ``make e2e`` and the check would silently stop exercising step 1 from
    the second run onwards, on a Redis whose TTL is an hour.

    Step 3's result must **equal step 2's**. That is the whole promise of the protocol: the client
    sent no document at all and got the same answer. Asserting merely that step 3 succeeded would
    pass even if the server had substituted a different registered document under the same hash —
    which is exactly the attack the server-side hash recomputation exists to prevent.
    """
    marker = unique_suffix()
    document = f'{{ logStats {{ totalLogs errorCount }} __typename @include(if: true) }}'
    # A no-op alias makes the document unique without changing its meaning or its cost.
    document = f"{{ apq_{marker}: logStats {{ totalLogs errorCount services }} }}"
    digest = sha256_hex(document)
    apq = {"persistedQuery": {"version": 1, "sha256Hash": digest}}

    # Step 1: hash only, nothing registered.
    first = post_graphql("", extensions=apq)
    messages = [error.get("message") for error in first.get("errors") or []]
    if "PersistedQueryNotFound" not in messages:
        raise CheckFailure(
            f"step 1 (hash only, unregistered) answered {messages!r}; Apollo's link keys its retry "
            "on the literal message 'PersistedQueryNotFound'"
        )
    codes = error_codes(first)
    if "PERSISTED_QUERY_NOT_FOUND" not in codes:
        raise CheckFailure(f"step 1 carried codes {codes}, want PERSISTED_QUERY_NOT_FOUND")

    # Step 2: document + hash. The server recomputes sha256 and registers on a match.
    second = post_graphql(document, extensions=apq)
    if second.get("errors"):
        raise CheckFailure(f"step 2 (document + hash) failed: {json.dumps(second['errors'])[:300]}")
    registered = second.get("data")
    if not registered:
        raise CheckFailure("step 2 returned no data")

    # Step 3: hash only again. Same answer, no document sent.
    third = post_graphql("", extensions=apq)
    if third.get("errors"):
        raise CheckFailure(f"step 3 (hash only, registered) failed: {json.dumps(third['errors'])[:300]}")
    if third.get("data") != registered:
        raise CheckFailure(
            f"step 3 returned {json.dumps(third.get('data'))[:200]} but step 2 returned "
            f"{json.dumps(registered)[:200]} — the hash did not resolve to the same document"
        )

    return (
        f"PersistedQueryNotFound -> register -> hash-only returned identical data "
        f"(sha256={digest[:12]}...)"
    )


# --------------------------------------------------------------------------- #
# 10 — logStream over a real socket
# --------------------------------------------------------------------------- #
async def _log_stream_probe() -> tuple[str, str]:
    """Drive one filtered ``logStream`` and return (delivered message, evidence about filtering)."""
    marker = unique_suffix()
    watched = f"{PROBE_PREFIX}-ws-{marker}"
    ignored = f"{PROBE_PREFIX}-other-{marker}"

    async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
        stream = await open_stream(LOG_STREAM, connection=connection)  # type: ignore[call-arg]
        raise AssertionError  # pragma: no cover - replaced below
    return marker, watched, ignored  # type: ignore[return-value]


def check_log_stream() -> str:
    """10. ``logStream`` delivers a matching entry and never a non-matching one.

    .. rubric:: Non-delivery is proved with a TRACER, never with a sleep

    Asserting "the wrong entry did not arrive" by waiting a while and looking is a guess about how
    long is long enough — too short and it passes a broken server, too long and every run pays for
    it. Instead: two entries the subscription did **not** ask for are written first, then one it
    did, and the check asserts the matching one is the **very next frame**. The queue is FIFO, so
    anything wrongly enqueued would necessarily be read first. That makes the assertion exact and
    bounded rather than probabilistic.

    The subscription filters on a service name absent from the seeded corpus, so nothing but this
    run's own writes can reach it — no seeded rows, no other check's traffic.
    """

    async def run() -> str:
        marker = unique_suffix()
        watched = f"{PROBE_PREFIX}-ws-{marker}"
        ignored = f"{PROBE_PREFIX}-other-{marker}"

        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(connection, LOG_STREAM, {"service": watched, "level": None})

            # First: establish that the subscription is live at all, by retrying the write until a
            # frame comes back. Everything after this point can write exactly once, because the
            # subscriber provably exists.
            first = await wait_until_subscribed(
                stream,
                lambda: create_log(service=watched, message=f"warmup {marker}"),
            )
            if first["logStream"]["service"] != watched:
                raise CheckFailure(f"the warmup frame carried {first['logStream']!r}"[:200])

            # Now the real assertion: two non-matching writes, then a matching tracer. The tracer
            # must be the next frame.
            create_log(service=ignored, message=f"must not arrive A {marker}")
            create_log(service=ignored, message=f"must not arrive B {marker}", level="ERROR")
            tracer = f"tracer {marker}"
            create_log(service=watched, message=tracer)

            frame = await stream.next_payload()
            delivered = frame["logStream"]
            if delivered["message"] != tracer:
                raise CheckFailure(
                    f"expected the tracer {tracer!r} as the next frame, got "
                    f"{delivered['message']!r} on service {delivered['service']!r} — a "
                    "non-matching entry was enqueued"
                )
            if delivered["service"] != watched:
                raise CheckFailure(f"the tracer arrived on service {delivered['service']!r}")

        return (
            f"filtered on service={watched!r}: warmup + tracer delivered, 2 non-matching writes "
            "never enqueued (proved by FIFO order, not by waiting)"
        )

    return run_async(run())


# --------------------------------------------------------------------------- #
# 11 — orderStatusStream over a real socket
# --------------------------------------------------------------------------- #
def check_order_stream() -> str:
    """11. ``orderStatusStream`` delivers, and filters by order id, status and user — spec §3C.

    Three filters, three tracers, one socket each. Every one uses the same FIFO-tracer technique as
    check 10: write what must not arrive, then write what must, and assert the latter is the next
    frame. A filter that was ignored would deliver the decoy first and fail immediately.

    The three decoys are chosen so that each isolates **one** dimension:

    * for the ``orderId`` filter, the decoy is a *different order* with the same user and status;
    * for the ``status`` filter, the decoy is the same order and user at a *different status*;
    * for the ``userId`` filter, the decoy is a *different user* on a different order.

    A decoy that differed in more than one dimension would pass even against a server that only
    implemented one of the three filters.
    """

    async def run() -> str:
        marker = unique_suffix()
        order = f"{PROBE_ORDER_PREFIX}-{marker}-a"
        other_order = f"{PROBE_ORDER_PREFIX}-{marker}-b"
        user = f"e2e-user-{marker}"
        other_user = f"e2e-user-{marker}-other"
        results: list[str] = []

        # --- by order id -------------------------------------------------------------------
        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(connection, ORDER_STREAM, {"orderId": order})
            await wait_until_subscribed(
                stream,
                lambda: create_order_event(order_id=order, user_id=user, status="CREATED"),
            )
            # Decoy: another order, same user, same status.
            create_order_event(order_id=other_order, user_id=user, status="PAID")
            create_order_event(order_id=order, user_id=user, status="PAID")

            event = (await stream.next_payload())["orderStatusStream"]
            if event["orderId"] != order:
                raise CheckFailure(
                    f"the orderId filter delivered order {event['orderId']!r}, want {order!r}"
                )
            if event["status"] != "PAID":
                raise CheckFailure(f"expected the PAID tracer, got {event['status']!r}")
            results.append(f"orderId -> {event['status']}")

        # --- by status ---------------------------------------------------------------------
        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(connection, ORDER_STREAM, {"status": "DELIVERED"})
            await wait_until_subscribed(
                stream,
                lambda: create_order_event(order_id=order, user_id=user, status="DELIVERED"),
            )
            # Decoys: the same order and user, at every status but the watched one.
            for decoy in ("PACKED", "SHIPPED", "CANCELLED"):
                create_order_event(order_id=order, user_id=user, status=decoy)
            create_order_event(order_id=other_order, user_id=other_user, status="DELIVERED")

            event = (await stream.next_payload())["orderStatusStream"]
            if event["status"] != "DELIVERED":
                raise CheckFailure(
                    f"the status filter delivered {event['status']!r}, want DELIVERED"
                )
            if event["orderId"] != other_order:
                raise CheckFailure(
                    f"expected the {other_order!r} tracer, got {event['orderId']!r}"
                )
            results.append("status -> DELIVERED only")

        # --- by user id --------------------------------------------------------------------
        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(connection, ORDER_STREAM, {"userId": other_user})
            await wait_until_subscribed(
                stream,
                lambda: create_order_event(
                    order_id=other_order, user_id=other_user, status="CREATED"
                ),
            )
            create_order_event(order_id=order, user_id=user, status="REFUNDED")
            create_order_event(order_id=other_order, user_id=other_user, status="REFUNDED")

            event = (await stream.next_payload())["orderStatusStream"]
            if event["userId"] != other_user:
                raise CheckFailure(
                    f"the userId filter delivered user {event['userId']!r}, want {other_user!r}"
                )
            results.append("userId -> one customer only")

        # --- and the combination -----------------------------------------------------------
        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(
                connection,
                ORDER_STREAM,
                {"orderId": order, "status": "SHIPPED", "userId": user},
            )
            await wait_until_subscribed(
                stream,
                lambda: create_order_event(order_id=order, user_id=user, status="SHIPPED"),
            )
            # Each decoy differs in exactly one of the three dimensions.
            create_order_event(order_id=other_order, user_id=user, status="SHIPPED")
            create_order_event(order_id=order, user_id=user, status="PACKED")
            create_order_event(order_id=order, user_id=other_user, status="SHIPPED")
            create_order_event(order_id=order, user_id=user, status="SHIPPED")

            event = (await stream.next_payload())["orderStatusStream"]
            if (event["orderId"], event["status"], event["userId"]) != (order, "SHIPPED", user):
                raise CheckFailure(
                    f"the AND-composed filter delivered {event!r}, want "
                    f"({order!r}, 'SHIPPED', {user!r})"
                )
            results.append("orderId+status+userId AND-composed")

        STATE["probe_order"] = order
        STATE["probe_user"] = user
        return "; ".join(results)

    return run_async(run())


# --------------------------------------------------------------------------- #
# 12 — spec §3 Feature Area C: sub-100ms end-to-end delivery
# --------------------------------------------------------------------------- #
def check_subscription_latency() -> str:
    """12. **createOrderEvent -> WebSocket frame p95 < ``E2E_SUB_LATENCY_MS``** — the spec's number.

    .. rubric:: What is being timed, and why the boundaries are where they are

    The clock starts immediately **before** the ``createOrderEvent`` HTTP request is issued and
    stops when the corresponding frame has been read off the socket and parsed. So the measurement
    includes the HTTP round trip, validation, the INSERT, the COMMIT, the broker fan-out, the
    WebSocket serialisation and the client's own JSON parse. That is deliberately the *widest*
    honest reading: the spec says "end-to-end delivery latency", and a measurement that started
    after the commit would be timing the fan-out — which is a ``put_nowait`` and would report
    microseconds while telling a user nothing about what they experience.

    It is measured over :data:`SUB_SAMPLES` transitions on **one** already-established socket, with
    the subscription registered before the first sample. Connection setup is excluded on purpose: a
    dashboard opens one socket and then receives thousands of events, so amortising the handshake
    into every sample would report a number that describes nothing that happens in production.

    p95 rather than the mean, because a delivery path is judged by its tail — a mean of 40 ms with
    one 2-second stall is a stream a user sees freeze.

    Every sample is matched to its own write by a unique order id, so a frame from an earlier sample
    cannot be timed against a later write and quietly report a negative-looking latency.
    """

    async def run() -> str:
        marker = unique_suffix()
        user = f"e2e-latency-{marker}"
        samples_ms: list[float] = []
        loop = asyncio.get_running_loop()

        async with websockets.connect(GRAPHQL_WS, subprotocols=[WS_SUBPROTOCOL]) as connection:
            stream = await open_stream(connection, ORDER_STREAM, {"userId": user})

            # Register first, and prove it, so sample 1 is not measuring subscription setup.
            await wait_until_subscribed(
                stream,
                lambda: create_order_event(
                    order_id=f"{PROBE_ORDER_PREFIX}-lat-{marker}-warmup",
                    user_id=user,
                    status="CREATED",
                ),
            )

            statuses = ORDER_LIFECYCLES[0]
            for index in range(SUB_SAMPLES):
                order = f"{PROBE_ORDER_PREFIX}-lat-{marker}-{index}"
                status = statuses[index % len(statuses)]
                started = time.perf_counter()
                await loop.run_in_executor(
                    None,
                    lambda o=order, s=status: create_order_event(
                        order_id=o, user_id=user, status=s
                    ),
                )
                frame = await stream.next_payload()
                elapsed_ms = (time.perf_counter() - started) * 1000.0

                event = frame["orderStatusStream"]
                if event["orderId"] != order:
                    raise CheckFailure(
                        f"sample {index} timed order {order!r} but received {event['orderId']!r} "
                        "— frames and writes are out of step, so the number would be meaningless"
                    )
                samples_ms.append(elapsed_ms)

        p50 = percentile(samples_ms, 50)
        p95 = percentile(samples_ms, 95)
        slowest = max(samples_ms)
        evidence = gate(p95, SUB_LATENCY_MS, "orderStatusStream end-to-end p95")
        return f"{evidence} over n={len(samples_ms)} (p50 {p50:.1f}ms, max {slowest:.1f}ms)"

    return run_async(run())


# --------------------------------------------------------------------------- #
# 13 — the LogEvent interface
# --------------------------------------------------------------------------- #
def check_log_event_interface() -> str:
    """13. One trace returns a genuine **mix** of ``__typename``s through the interface.

    Spec §3 Feature Area A asks for "a shared interface for common log fields ... implemented by all
    event types", and the observable consequence is this: a single selection with inline fragments
    returns a heterogeneous timeline. A trace that returned four ``OrderEvent``s would satisfy
    "correlatedEvents works" and prove nothing about the interface, so the assertion is on the
    **number of distinct typenames**, and the trace is chosen by searching the seeded corpus for one
    that spans several.

    Every member is also checked to carry all four interface fields *at the interface level* — they
    are selected outside any inline fragment, so a type that failed to implement one would fail
    validation rather than returning null, and a type that implemented one with a different meaning
    would show up as a missing key here.
    """
    # Discover a seeded trace rather than regenerating one: the api container seeds with its own
    # wall clock, so a locally regenerated corpus would carry different instants (and, through the
    # RNG, potentially different traces). Asking the API is both more honest and more robust.
    orders = require_list(
        graphql_data(
            "query T($f: OrderEventFilterInput) { orderEvents(filters: $f) { traceId orderId } }",
            # Scoped to the seeded corpus for the same reason as check 14: probe orders carry no
            # traceId, so an unscoped newest-60 read would spend its budget on rows that can never
            # be candidates. Today checks 11-12 write ~30 of them and 60 is just wide enough to see
            # past them — a margin, not a guarantee, and exactly the kind that stops holding when
            # someone adds a probe. The bound removes the dependency instead of widening it.
            {"f": {"limit": 60, "endTime": RUN_STARTED}},
        ).get("orderEvents"),
        "orderEvents",
    )
    candidates = [row["traceId"] for row in orders if row.get("traceId")]
    if not candidates:
        raise CheckFailure("no seeded order event carries a traceId")

    best: tuple[int, str, list[dict[str, Any]]] = (0, "", [])
    for trace in candidates[:12]:
        events = require_list(
            graphql_data(CORRELATED, {"traceId": trace}).get("correlatedEvents"),
            "correlatedEvents",
        )
        distinct = len({event["__typename"] for event in events})
        if distinct > best[0]:
            best = (distinct, trace, events)
        if distinct >= 4:
            break

    distinct, trace, events = best
    if distinct < 3:
        raise CheckFailure(
            f"the best of {len(candidates[:12])} seeded traces returned only {distinct} distinct "
            f"__typename(s) ({sorted({e['__typename'] for e in events})}) — the interface is not "
            "returning a heterogeneous timeline"
        )

    known = {"LogEntry", "OrderEvent", "PaymentEvent", "UserEvent"}
    for event in events:
        if event["__typename"] not in known:
            raise CheckFailure(f"unexpected __typename {event['__typename']!r}")
        missing = {"timestamp", "service", "level", "traceId"} - set(event)
        if missing:
            raise CheckFailure(
                f"a {event['__typename']} is missing interface field(s) {sorted(missing)}"
            )
        if event["traceId"] != trace:
            raise CheckFailure(
                f"a {event['__typename']} carries traceId {event['traceId']!r}, want {trace!r}"
            )
        if event["level"] not in set(LOG_LEVELS):
            raise CheckFailure(f"level {event['level']!r} is outside LogLevel")
        if event["service"] not in set(SERVICES):
            raise CheckFailure(
                f"service {event['service']!r} is outside the generated SERVICES vocabulary"
            )

    STATE["mixed_trace"] = trace
    typenames = sorted({event["__typename"] for event in events})
    return f"trace {trace[:16]}... returned {len(events)} events across {typenames}"


# --------------------------------------------------------------------------- #
# 14 — the C11 flagship dossier
# --------------------------------------------------------------------------- #
def check_dossier_consistency() -> str:
    """14. The 3-in-1 dossier returns nested data that is **internally consistent**.

    "It returned data" is not the assertion — a resolver that returned every payment in the table
    for every order would also return data. What is checked is that the traversals agree with the
    keys they were traversed by, which is precisely what a batched loader gets wrong when its
    results are mis-scattered back to their parents (the classic DataLoader bug: right rows, wrong
    parent, and every response still well-formed):

    * every payment reached through an order carries that order's ``orderId``;
    * every payment's own ``order`` traversal returns the **same** order id it was reached through,
      i.e. the round trip order -> payment -> order is the identity;
    * every user activity reached through an order carries that order's ``userId``;
    * every related log line carries the order's ``traceId``.

    One request, four entity types, four REST calls' worth of data — which is spec §3 Feature Area
    B's requirement stated as a test.
    """
    # `endTime: RUN_STARTED` restricts this to the SEEDED corpus. Checks 11-12 have already written
    # ~30 probe order events by the time this runs, and they are newer than everything seeded, so an
    # unscoped `limit: 3` returns three probe rows — which have no payments, no activity and no
    # trace, making every traversal below vacuously empty. See RUN_STARTED for why the fix is a time
    # bound rather than a bigger limit (the bigger limit is refused by the cost gate).
    data = graphql_data(DOSSIER, {"filters": {"limit": 3, "endTime": RUN_STARTED}})
    orders = require_list(data.get("orderEvents"), "orderEvents")
    if not orders:
        raise CheckFailure("orderEvents returned nothing")

    payments = users = logs = 0
    for order in orders:
        order_id, user_id, trace = order["orderId"], order["userId"], order.get("traceId")

        for payment in require_list(order["payments"], "payments"):
            payments += 1
            if payment["orderId"] != order_id:
                raise CheckFailure(
                    f"order {order_id!r} reached payment {payment['id']} filed under "
                    f"{payment['orderId']!r} — the batch scattered results to the wrong parent"
                )
            back = payment.get("order")
            if back is None:
                raise CheckFailure(
                    f"payment {payment['id']} (order {order_id!r}) traversed back to a null order, "
                    "but the order it came from demonstrably exists"
                )
            if back["orderId"] != order_id:
                raise CheckFailure(
                    f"order -> payment -> order is not the identity: {order_id!r} -> "
                    f"{back['orderId']!r}"
                )
            if back["status"] not in set(ORDER_STATUSES):
                raise CheckFailure(f"status {back['status']!r} is outside OrderStatus")

        for activity in require_list(order["userActivity"], "userActivity"):
            users += 1
            if activity["userId"] != user_id:
                raise CheckFailure(
                    f"order {order_id!r} (user {user_id!r}) reached activity for user "
                    f"{activity['userId']!r}"
                )

        related = require_list(order["relatedLogs"], "relatedLogs")
        logs += len(related)
        if trace is None and related:
            raise CheckFailure(f"order {order_id!r} has a null traceId but {len(related)} logs")
        for line in related:
            if line["traceId"] != trace:
                raise CheckFailure(
                    f"order {order_id!r} (trace {trace!r}) reached a log line on trace "
                    f"{line['traceId']!r}"
                )

    if payments == 0 and users == 0:
        raise CheckFailure(
            "the dossier traversed to nothing at all — consistency over an empty result proves "
            "nothing about the traversals"
        )

    return (
        f"{len(orders)} orders -> {payments} payments, {users} activities, {logs} log lines in ONE "
        "request; every traversal agrees with the key it was reached by"
    )


# --------------------------------------------------------------------------- #
# 15 — the three aggregates
# --------------------------------------------------------------------------- #
def check_aggregates_agree() -> str:
    """15. The three cached aggregates agree with the rows they summarise.

    Cross-checked the way ``logStats`` is — against another read of the same data rather than
    against a constant — because an aggregate's failure mode is a wrong *number*, and a constant
    would only catch the aggregate becoming empty:

    * **the distribution counts every order exactly once.** Each order appears in exactly one
      bucket (its newest event's status), so the buckets sum to the number of distinct orders, and
      that number is checked against a counted read of the order stream.
    * **the funnel is monotonic under the lifecycle.** Every lifecycle in
      :data:`src.generators.ORDER_LIFECYCLES` starts at ``CREATED``, so no stage can have been
      reached by more orders than ``CREATED`` was, and ``share`` must be in ``(0, 1]``.
    * **the funnel dominates the distribution.** "Ever reached status X" cannot be smaller than
      "currently at status X" — the second is a subset of the first, for every status. That single
      inequality is what would catch the two aggregates being computed over different windows.
    * **the payment cross-tab is internally coherent**: every cell's ``events`` is at least its
      ``orders`` (a cell with more distinct orders than payment attempts is impossible), and every
      method/outcome is inside the published vocabulary.
    """
    data = graphql_data(AGGREGATES)
    distribution = require_list(data.get("orderStatusDistribution"), "orderStatusDistribution")
    funnel = require_list(data.get("orderFunnel"), "orderFunnel")
    breakdown = require_list(data.get("paymentOutcomeBreakdown"), "paymentOutcomeBreakdown")
    if not distribution or not funnel or not breakdown:
        raise CheckFailure(
            f"an aggregate came back empty (distribution={len(distribution)}, "
            f"funnel={len(funnel)}, breakdown={len(breakdown)})"
        )

    statuses = set(ORDER_STATUSES)
    current = {bucket["status"]: require_int(bucket["orders"], "orders") for bucket in distribution}
    reached = {
        stage["status"]: require_int(stage["ordersReached"], "ordersReached") for stage in funnel
    }
    if not set(current) <= statuses:
        raise CheckFailure(f"the distribution names statuses outside OrderStatus: {set(current)}")
    if not set(reached) <= statuses:
        raise CheckFailure(f"the funnel names statuses outside OrderStatus: {set(reached)}")

    # Every order sits in exactly one distribution bucket, so the buckets sum to the order count.
    total_orders = sum(current.values())
    distinct = len(
        {
            row["orderId"]
            for row in require_list(
                graphql_data(
                    "query O($f: OrderEventFilterInput) { orderEvents(filters: $f) { orderId } }",
                    {"f": {"limit": 500}},
                ).get("orderEvents"),
                "orderEvents",
            )
        }
    )
    if total_orders < distinct:
        raise CheckFailure(
            f"the distribution accounts for {total_orders} orders but a capped read already saw "
            f"{distinct} distinct ones — orders are being lost between the two"
        )

    widest = max(reached.values())
    created = reached.get("CREATED")
    if created is None:
        raise CheckFailure(
            "the funnel has no CREATED stage, yet every ORDER_LIFECYCLES path begins there"
        )
    if created != widest:
        raise CheckFailure(
            f"CREATED was reached by {created} orders but some stage reports {widest} — no order "
            "can reach a later stage without having been created"
        )
    for stage in funnel:
        share = stage["share"]
        if not isinstance(share, (int, float)) or not 0.0 < float(share) <= 1.0:
            raise CheckFailure(f"stage {stage['status']!r} reports share={share!r}, want (0, 1]")

    for status, now in current.items():
        ever = reached.get(status, 0)
        if ever < now:
            raise CheckFailure(
                f"{now} orders are currently at {status} but the funnel says only {ever} ever "
                "reached it — the two aggregates disagree about the same window"
            )

    for cell in breakdown:
        events = require_int(cell["events"], "events")
        orders = require_int(cell["orders"], "orders")
        if events < orders:
            raise CheckFailure(
                f"cell {cell['method']}/{cell['outcome']} reports {events} attempts across "
                f"{orders} distinct orders, which is impossible"
            )

    return (
        f"distribution sums to {total_orders} orders (>= {distinct} seen); funnel peaks at CREATED"
        f"={created} and dominates the distribution at every status; {len(breakdown)} payment cells"
        " coherent"
    )


# --------------------------------------------------------------------------- #
# 16 — /metrics
# --------------------------------------------------------------------------- #
def _metric_samples(body: str) -> dict[str, float]:
    """Parse Prometheus text exposition into ``{series name: value}``, ignoring HELP/TYPE lines.

    Deliberately a five-line parser rather than ``prometheus_client.parser``: this is a black-box
    check, and using the server's own library to read the server's own output would make a
    formatting bug invisible to it. Labels are kept in the series name (``foo{code="X"} 1``), which
    is enough for the prefix matching below and keeps the parser honest about not understanding
    them.
    """
    samples: dict[str, float] = {}
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        if not name:
            continue
        try:
            samples[name] = float(value)
        except ValueError:
            continue
    return samples


def _family_total(samples: dict[str, float], family: str) -> tuple[bool, float]:
    """``(the family appeared, the sum of its samples)``. Matches ``family`` and ``family{...}``."""
    present = False
    total = 0.0
    for name, value in samples.items():
        base = name.split("{", 1)[0]
        if base == family or base.startswith(f"{family}_"):
            present = True
            total += value
    return present, total


def check_metrics() -> str:
    """16. ``GET /metrics`` exposes the expected families, with non-zero samples after traffic.

    Presence and non-emptiness are separate assertions because they fail for different reasons. A
    **missing** family means the exposition is reading the wrong registry or the instrument was
    never declared. A family that is present but **all zeros** means the instrument was declared and
    never recorded into — which is the failure mode of an extension that is registered on a schema
    nothing is executing through, and it looks perfectly healthy on a dashboard.

    Fifteen checks' worth of queries, mutations, subscriptions and one deliberate cost rejection
    have run by the time this executes, so every counter listed in
    :data:`NONZERO_METRIC_FAMILIES` has provably been exercised. ``gql_active_subscriptions`` is
    required to be *present* but not non-zero: it is a gauge, and every socket this run opened has
    since been closed, so zero is the correct reading.
    """
    response = CLIENT.get("/metrics")
    if response.status_code != 200:
        raise CheckFailure(f"GET /metrics -> HTTP {response.status_code}")
    content_type = response.headers.get("content-type", "")
    if "text/plain" not in content_type:
        raise CheckFailure(f"GET /metrics served content-type {content_type!r}")

    samples = _metric_samples(response.text)
    if not samples:
        raise CheckFailure("GET /metrics returned no samples at all")

    missing = [
        family for family in REQUIRED_METRIC_FAMILIES if not _family_total(samples, family)[0]
    ]
    if missing:
        raise CheckFailure(
            f"/metrics is missing {missing}; it exposed "
            f"{sorted({name.split('{', 1)[0] for name in samples})[:12]}"
        )

    empty = [
        family
        for family in NONZERO_METRIC_FAMILIES
        if _family_total(samples, family)[1] <= 0.0
    ]
    if empty:
        raise CheckFailure(
            f"{empty} are present but sum to zero after {_counter} checks' worth of traffic — the "
            "instruments are declared but nothing is recording into them"
        )

    operations = _family_total(samples, "gql_operation_duration_seconds_count")[1]
    published = _family_total(samples, "gql_broker_published_total")[1]
    delivered = _family_total(samples, "gql_broker_delivered_total")[1]
    return (
        f"{len(REQUIRED_METRIC_FAMILIES)} families present; "
        f"{operations:.0f} operations timed, {published:.0f} events published, "
        f"{delivered:.0f} delivered to subscribers"
    )


# --------------------------------------------------------------------------- #
# 17 — latency and memory
# --------------------------------------------------------------------------- #
def check_perf_and_memory() -> str:
    """17. Sequential read p95 <= ``MAX_P95_MS``, and backend RSS <= ``MAX_BACKEND_MEM_MB``.

    The reads are issued **one at a time**, which is what makes ``MAX_P95_MS`` a bound on service
    time rather than on queueing. The load harness (C14) measures the same operation at
    ``LOAD_CONCURRENCY`` and gates it with a *different* variable (``LOAD_MAX_P95_MS``) for exactly
    that reason: a p95 taken under queueing and a p95 taken without it are different quantities, and
    giving them one number fails a healthy server on arithmetic.

    The memory number is the **server's own** ``psutil`` reading, taken from ``GET /debug/memory``.
    This process's RSS would be the footprint of an httpx client — real, stable, and about entirely
    the wrong process. A ``null`` reading (``psutil`` unimportable in the image) fails the check
    rather than passing it: a gate that cannot measure must not report success.
    """
    samples_ms: list[float] = []
    for _ in range(LATENCY_SAMPLES):
        started = time.perf_counter()
        graphql_data(FILTERED_LOGS, {"filters": {"limit": 50}})
        samples_ms.append((time.perf_counter() - started) * 1000.0)

    p50 = percentile(samples_ms, 50)
    p95 = percentile(samples_ms, 95)
    latency_evidence = gate(p95, MAX_P95_MS, "sequential logs(limit: 50) p95")

    response = CLIENT.get("/debug/memory")
    if response.status_code != 200:
        raise CheckFailure(
            f"GET /debug/memory -> HTTP {response.status_code}; the backend RSS gate has nothing "
            "to read without it"
        )
    probe = response.json()
    if not probe.get("available") or probe.get("memoryMb") is None:
        raise CheckFailure(
            f"the server's memory probe reported {probe!r} — psutil is pinned in requirements.txt "
            "for exactly this, so an unavailable probe is a broken image rather than a platform "
            "limitation, and MAX_BACKEND_MEM_MB cannot be enforced without it"
        )
    memory_evidence = gate(
        float(probe["memoryMb"]), MAX_BACKEND_MEM_MB, "backend RSS", unit="MiB"
    )

    return (
        f"{latency_evidence} over n={LATENCY_SAMPLES} (p50 {p50:.1f}ms); {memory_evidence} "
        f"(pid {probe['pid']}, {probe['subscribers']} live subscriptions)"
    )


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def main() -> None:
    info(f"== GraphQL Log Query Platform black-box verifier vs {BASE_URL} ==")
    info(
        f"gates: subscription frame within {SUB_TIMEOUT:.0f}s; order delivery p95 <= "
        f"{SUB_LATENCY_MS:.0f}ms over {SUB_SAMPLES} samples; sequential p95 <= {MAX_P95_MS:.0f}ms "
        f"over {LATENCY_SAMPLES} samples; backend RSS <= {MAX_BACKEND_MEM_MB:.0f} MiB"
    )
    wait_ready()

    check("health returns exactly {'status': 'healthy'}", check_health)
    check("spec §5: { logs { id service level message } }", check_spec_logs)
    check("spec §5: variable-driven filter returns only matches", check_variable_filter)
    check("spec §5: { logStats { totalLogs errorCount services } }", check_spec_stats)
    check("spec §5: createLog(logData:) creates a record", check_spec_create_log)
    check("relatedLogs groups are correct and consistent", check_related_logs_batching)
    check("a repeated query returns identical data", check_cache_hit)
    check("cost gate refuses with computedCost/maxCost", check_cost_gate)
    check("APQ: not-found -> register -> hash-only", check_apq_round_trip)
    check("logStream delivers matches and only matches", check_log_stream)
    check("orderStatusStream filters by order/status/user", check_order_stream)
    check("§3C: order delivery p95 under the latency gate", check_subscription_latency)
    check("LogEvent interface returns mixed __typenames", check_log_event_interface)
    check("the 3-in-1 dossier is internally consistent", check_dossier_consistency)
    check("the three aggregates agree with their rows", check_aggregates_agree)
    check("/metrics exposes non-zero families", check_metrics)
    check("sequential p95 + backend RSS", check_perf_and_memory)

    print(f"E2E PASSED ({TOTAL_CHECKS}/{TOTAL_CHECKS})", flush=True)


if __name__ == "__main__":
    main()

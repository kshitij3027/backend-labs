"""The metered stub downstream — the four routes :data:`src.keys.ROUTE_TABLE` already prices.

Everything C1-C6 built meters *something*. This is that something, and it is deliberately the
least interesting file in the project: a seeded in-memory corpus, a bounded list, four handlers.
The subject of this repository is the enforcement layer in front of these routes, not the log
store behind them, so the store is a stub and stays one. A second log-query engine here would be
several hundred lines that make the limiter no more or less correct — the sibling project
``log-query-api-rest`` is where that engine lives, and duplicating it would only give the C13
verifier two things to be flaky about instead of one.

.. rubric:: The paths are a CONTRACT with the classifier, and disagreeing with it is a bypass

:data:`src.keys.ROUTE_TABLE` prices these four ``(method, path)`` pairs and nothing else:

===========================================  =============  =====
Route                                        Category       Cost
===========================================  =============  =====
``GET  /api/v1/logs/query``                  ``logs_query``     5
``POST /api/v1/logs/ingest``                 ``logs_ingest``    2
``GET  /api/v1/whoami``                      ``default``        1
``GET  /api/v1/logs/{id}``                   ``default``        1
===========================================  =============  =====

The middleware runs **above** the router, so what a request *costs* is decided by
:func:`src.keys.classify` from the raw path while what a request *does* is decided by Starlette
from the mounted routes. Those two answers are produced by different code reading the same string,
and :func:`src.keys.classify` carries two rubrics on what happens when they disagree: the caller
is served endpoint X and charged for endpoint Y. Renaming a route in this file without editing the
table would silently reprice it to ``other``/1 — the project's most expensive endpoint served at a
fifth of its price, by a one-line edit that no test about *routing* would notice.

So the correspondence is declared as data (:data:`ROUTE_CONTRACT`) and checked, not commented:
:func:`verify_route_pricing` walks the app's real routes, runs each one's mounted path back
through the real classifier, and raises unless every mounted path prices to the row it is supposed
to. :func:`src.main.create_app` calls it at construction time, so a mismatch is a startup failure
rather than a billing discrepancy nobody reads.

.. rubric:: NOT ONE of these routes declares an auth dependency, and that is the feature

The spec's word for it is "transparent to route handlers". Precisely: authentication and metering
happen in :class:`~src.middleware.RateLimitMiddleware`, above the router, so by the time a handler
below runs the caller is already resolved and already charged. A ``Depends(...)`` here that
re-checked the credential would be a **second, divergent authentication path** — one that can
accept a caller the middleware rejected, or reject one it accepted, and that has to be kept in
sync by hand forever. There is exactly one gate, and it is not in this file.

The corollary is that every handler here must work when the gate is *off*. With
``RATE_LIMIT_ENABLED=false`` no principal is resolved and no decision is stashed, and
:func:`whoami` — the only handler that reads the decision at all — reports ``metered: false``
instead of raising. A handler that 500s when the limiter is disabled is a handler *coupled* to the
limiter, which is the exact opposite of transparent; see :func:`whoami` for how the read is made
defensive.

.. rubric:: The store is per-app, lazily created, and never persisted

There is no Redis here, no volume and no module-level singleton. The store hangs off
``app.state`` (see :func:`store_for`), which matters for one specific test: C12 builds **two**
applications in one process and drives both against one Redis to prove the *limiter's* state is
shared. A module-level store would make the *stub's* state shared too, which would quietly turn a
test about Redis into a test about a Python global — and, more mundanely, would leak one test's
ingests into the next test's counts.
"""

from __future__ import annotations

import logging
import random
import re
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Final

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from starlette.applications import Starlette
from starlette.routing import Match

from src.identity import parse_credential
from src.keys import ROUTE_TABLE, classify
from src.middleware import SCOPE_DECISION_KEY, SCOPE_ENDPOINT_KEY, is_exempt
from src.models import CredentialKind, LimitDecision

logger = logging.getLogger(__name__)

__all__ = [
    "ANCHOR_TS",
    "API_V1_PREFIX",
    "CORPUS_SEED",
    "CORPUS_SIZE",
    "DEFAULT_PAGE_SIZE",
    "IngestAccepted",
    "LimitSnapshot",
    "LogEntry",
    "LogIngest",
    "LogLevel",
    "LogPage",
    "LogStore",
    "MAX_PAGE_SIZE",
    "MIN_PAGE_SIZE",
    "PageInfo",
    "ROUTE_CONTRACT",
    "RouteContract",
    "SAMPLE_PATH_PARAM",
    "STORE_CAPACITY",
    "WhoAmI",
    "clamp_limit",
    "entry_id",
    "generate_corpus",
    "mounted_v1_routes",
    "resolve_route",
    "router",
    "sample_path",
    "store_for",
    "verify_route_pricing",
]

#: The versioned prefix, spelled here rather than imported from :data:`src.main.API_V1_PREFIX`:
#: ``src.main`` imports *this* module, so importing back would be a startup cycle.
#: ``tests/integration/test_protected_api.py`` asserts the two strings are equal, so the
#: duplication is pinned rather than merely hoped for.
API_V1_PREFIX: Final = "/api/v1"

#: What a route path must start with to be *inside* the priced surface. The trailing slash is
#: load-bearing: without it a future ``/api/v1x`` route would be dragged into the cross-check by a
#: naming coincidence, which is the same bug :func:`src.middleware.is_exempt` documents at length.
_V1_ROUTE_PREFIX: Final = API_V1_PREFIX + "/"


# ---------------------------------------------------------------------------------------------
# The deterministic corpus
#
# Seeded, anchored, and reproducible in any process. Not decoration: C13's verifier and the tests
# below assert on *exact* counts ("level=ERROR returns 14 rows"), and an assertion whose expected
# value drifts between the process that serves the data and the process that grades it is an
# assertion that can only pass by comparing the server to itself.
# ---------------------------------------------------------------------------------------------


class LogLevel(StrEnum):
    """Severity vocabulary of the stub corpus.

    ``StrEnum`` so a member *is* its wire string — the query parameter binds straight to it, an
    unknown value is a 422 from pydantic rather than a hand-written check, and the value
    serialises as the plain name.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


#: RNG seed. A module constant rather than a setting: the corpus is an *oracle* that tests and
#: (from C13) an out-of-process verifier assert exact counts against, so making it operator-tunable
#: would let a stray env var silently invalidate every one of those assertions.
CORPUS_SEED: Final = 20260804

#: How many entries the store starts life with. Small on purpose — a bigger corpus would slow the
#: suite and prove nothing extra about a limiter.
CORPUS_SIZE: Final = 200

#: Hard cap on the store. Ingest appends and evicts the oldest at this size, so a caller that
#: POSTs in a loop cannot grow the API's heap without bound — the same reasoning that bounds
#: :func:`src.keys.classify`'s memo table, applied to the one other caller-driven collection in
#: the process. (The limiter would stop them long before 500, which is precisely why the bound
#: must not *depend* on the limiter: `RATE_LIMIT_ENABLED=false` is a supported configuration.)
STORE_CAPACITY: Final = 500

#: The newest corpus timestamp is derived from this **fixed instant**, never ``datetime.now``.
#: A corpus that depended on container start time would produce different timestamps in the API
#: process and in the verifier process, and every timestamp-sensitive assertion would become a
#: flake with a plausible-looking cause.
ANCHOR_TS: Final = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)

#: Gap between two consecutive corpus entries. Ascending — oldest first — so the store's insertion
#: order and its time order agree, which is what lets ``offset``/``limit`` paginate a stable list.
CORPUS_STEP: Final = timedelta(seconds=60)

#: Emitting services. A short tuple so every name recurs many times across the corpus: a
#: ``?service=`` filter is only a useful probe if the value is guaranteed to match several rows.
SERVICES: Final[tuple[str, ...]] = (
    "auth-svc",
    "api-gateway",
    "payments-svc",
    "search-svc",
)

#: Severity mix of a healthy-ish service. Weighted rather than uniform so ``?level=ERROR`` returns
#: a small, assertable slice instead of a fifth of the corpus.
LEVELS: Final[tuple[LogLevel, ...]] = (
    LogLevel.DEBUG,
    LogLevel.INFO,
    LogLevel.WARN,
    LogLevel.ERROR,
    LogLevel.FATAL,
)
LEVEL_WEIGHTS: Final[tuple[int, ...]] = (20, 60, 12, 7, 1)

#: Message pool. Deliberately tiny and fully literal, so the same text repeats many times — the
#: stub is a fixture, not a log generator, and a corpus of 200 unique strings would be 200 strings
#: nobody can assert anything about.
MESSAGES: Final[tuple[str, ...]] = (
    "request completed",
    "cache miss",
    "upstream timeout",
    "token refreshed",
    "connection reset by peer",
    "batch flushed",
)

#: How much of a caller-supplied id is echoed back in a 404. Bounded so the error body cannot be
#: used as an arbitrary-length reflector; long enough that a real id is always shown in full.
_ECHO_ID_CHARS: Final = 64


class LogEntry(BaseModel):
    """One stored log line — the same model on the wire and in the store.

    Frozen, because an entry is a record of something that already happened and no handler may
    edit one on its way to a response. One model rather than a stored dataclass plus a response
    schema: the store is a stub, so a second shape would be ceremony with no second meaning.
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(description="Stable identifier, e.g. `log-00042`. Unique for the process.")
    ts: datetime = Field(description="Emission timestamp, UTC.")
    level: LogLevel = Field(description="Severity.")
    service: str = Field(description="Emitting service name.")
    message: str = Field(description="Log text.")


def entry_id(sequence: int) -> str:
    """Render the id for the ``sequence``-th entry ever created by a store — ``log-00042``.

    Zero-padded to five digits so ids sort lexicographically in the same order they were created,
    which is the property that makes a paged response readable in a terminal.
    """
    return f"log-{sequence:05d}"


def generate_corpus(size: int = CORPUS_SIZE, seed: int = CORPUS_SEED) -> list[LogEntry]:
    """Build the seeded corpus, **oldest first**. Same arguments in, byte-identical corpus out.

    Every draw comes from a private :class:`random.Random` instance and never from the global
    :mod:`random` module. That is not style: the global RNG is process-wide state that any other
    import is free to reseed, so a corpus built from it would be reproducible right up until an
    unrelated dependency called ``random.seed()`` — at which point the tests asserting exact counts
    would start failing for a reason with no connection to the code under test.

    Timestamps ascend from :data:`ANCHOR_TS` in :data:`CORPUS_STEP` increments, so insertion order
    and time order agree and pagination over the untouched corpus is stable.
    """
    rng = random.Random(seed)
    return [
        LogEntry(
            id=entry_id(index + 1),
            ts=ANCHOR_TS + CORPUS_STEP * index,
            # Drawn in a fixed order per entry — level, then service, then message. The *sequence*
            # of calls into the RNG is part of what "deterministic" means here, so reordering these
            # three lines changes the corpus even though it changes no logic.
            level=rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0],
            service=rng.choice(SERVICES),
            message=rng.choice(MESSAGES),
        )
        for index in range(size)
    ]


def _matches(entry: LogEntry, level: LogLevel | None, service: str | None) -> bool:
    """Whether ``entry`` satisfies the two optional filters. ``None`` means "no constraint"."""
    if level is not None and entry.level != level:
        return False
    if service is not None and entry.service != service:
        return False
    return True


class LogStore:
    """A bounded, in-memory, append-only log store. Nothing here is persisted.

    A ``deque`` with an explicit capacity rather than ``deque(maxlen=...)``, because eviction has
    to be *observed* to keep the id index exact: ``maxlen`` drops the oldest entry silently, which
    would leave a stale id in :attr:`_index` pointing at an object no longer in the store — a
    ``GET /logs/{id}`` returning a row the list endpoint no longer shows. Popping explicitly costs
    one line and removes the whole class of disagreement.

    The id counter is monotonic and **never reset by eviction**, so an id is unique for the life of
    the process. Reusing an evicted id would make a client's cached row silently refer to a
    different entry.
    """

    def __init__(
        self,
        *,
        seed: int = CORPUS_SEED,
        size: int = CORPUS_SIZE,
        capacity: int = STORE_CAPACITY,
    ) -> None:
        self._capacity = max(1, capacity)
        self._entries: deque[LogEntry] = deque(generate_corpus(size, seed))
        self._index: dict[str, LogEntry] = {entry.id: entry for entry in self._entries}
        self._next_sequence = len(self._entries) + 1

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def capacity(self) -> int:
        """Maximum entries retained; the oldest is evicted on the append that would exceed it."""
        return self._capacity

    def append(self, *, level: LogLevel, service: str, message: str) -> LogEntry:
        """Append one entry, evicting the oldest if the store is at capacity. Returns the entry.

        ``ts`` is the wall clock rather than a derived instant: the corpus is the deterministic
        part (it is the oracle the tests grade against), while an ingested entry is by definition
        new, and stamping it with a fabricated time would make the store's own ordering a lie.
        """
        entry = LogEntry(
            id=entry_id(self._next_sequence),
            ts=datetime.now(timezone.utc),
            level=level,
            service=service,
            message=message,
        )
        self._next_sequence += 1
        self._entries.append(entry)
        self._index[entry.id] = entry
        if len(self._entries) > self._capacity:
            evicted = self._entries.popleft()
            del self._index[evicted.id]
        return entry

    def get(self, log_id: str) -> LogEntry | None:
        """One entry by id, or ``None``. A dict lookup — the index is kept exact by :meth:`append`."""
        return self._index.get(log_id)

    def query(
        self,
        *,
        level: LogLevel | None,
        service: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[LogEntry], int]:
        """Return ``(page, total_matching)`` for the filters, oldest first.

        ``total`` is the size of the **filtered** match set rather than of the whole store, so a
        client can tell "you are on the last page" from "your filter matched nothing" without a
        second request. ``limit`` and ``offset`` are expected to be pre-clamped by the caller —
        see :func:`clamp_limit`, which is where the never-a-422 rule lives.
        """
        matched = [entry for entry in self._entries if _matches(entry, level, service)]
        return matched[offset : offset + limit], len(matched)


def store_for(request: Request) -> LogStore:
    """Return this application's store, building it on first use.

    Hung off ``app.state`` rather than held in a module global — see the module docstring for the
    C12 test that distinction protects.

    Lazily created rather than built in :func:`src.main.create_app` so that including this router
    stays a one-line change with no second wiring step a future router can forget. The
    check-then-set is safe under concurrency without a lock because nothing between the read and
    the write awaits: an ``asyncio`` task cannot be preempted mid-way through synchronous code, so
    two concurrent requests cannot both observe ``None``.
    """
    store: LogStore | None = getattr(request.app.state, "log_store", None)
    if store is None:
        store = LogStore()
        request.app.state.log_store = store
        logger.debug("log store created (%d seeded entries)", len(store))
    return store


# ---------------------------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------------------------

#: Page size when the caller expresses no preference.
DEFAULT_PAGE_SIZE: Final = 20

#: Ceiling on the page size. An over-large request is **clamped**, never rejected — see
#: :func:`clamp_limit`.
MAX_PAGE_SIZE: Final = 100

#: Floor. ``limit=0`` and ``limit=-5`` both become 1: there is no reading of "give me zero rows"
#: that is more useful than "give me one".
MIN_PAGE_SIZE: Final = 1


def clamp_limit(requested: int) -> int:
    """Resolve a caller's ``limit`` into an effective page size. **Never raises, never 422s.**

    Clamping rather than rejecting is the house pattern, and the reason is that the server already
    knows the right answer. A ``422`` for ``?limit=10000`` makes a naive client's first request
    fail and teaches them nothing they could not have been told by simply being handed the
    ceiling; the effective value comes back in ``page.limit``, so the client can see exactly what
    it got. The floor is clamped the same way purely for consistency.

    Declared as a bare ``int`` query parameter rather than ``Query(le=MAX_PAGE_SIZE)`` on purpose:
    the ``le=`` form is precisely the 422 this function exists to avoid, and it would put the rule
    in the signature where the reasoning cannot live with it.
    """
    if requested > MAX_PAGE_SIZE:
        return MAX_PAGE_SIZE
    if requested < MIN_PAGE_SIZE:
        return MIN_PAGE_SIZE
    return requested


class PageInfo(BaseModel):
    """Pagination metadata — the ``page`` half of the list response.

    ``limit`` is the **effective** value after clamping, not what the caller asked for, which is
    what makes an over-large request self-explanatory instead of merely tolerated.
    """

    limit: int = Field(description="Effective page size after clamping to MAX_PAGE_SIZE.")
    offset: int = Field(description="Effective offset after flooring at 0.")
    returned: int = Field(description="Number of entries actually in this page.")
    total: int = Field(description="Entries matching the filter across the whole store.")
    has_more: bool = Field(description="True when entries remain beyond this page.")


class LogPage(BaseModel):
    """The paginated envelope returned by ``GET /api/v1/logs/query``.

    An envelope and never a bare JSON array: a top-level array is a compatibility dead end, since
    no sibling field — pagination metadata, a warning, a deprecation notice — can be added without
    changing the response's *type* and breaking every client that already parses it.
    """

    items: list[LogEntry] = Field(description="The matching entries, oldest first.")
    page: PageInfo = Field(description="Pagination metadata for this page.")


class LogIngest(BaseModel):
    """Body of ``POST /api/v1/logs/ingest``.

    ``extra="forbid"`` so a misspelled field is a 422 rather than a silently-ignored key — an
    ingest that accepts ``{"levle": "ERROR"}`` and stores an INFO line is the worst kind of
    success. The bounds are ordinary input hygiene: this endpoint appends to a bounded in-process
    deque, so an unbounded ``message`` would be the one field a caller could use to grow the API's
    heap independently of the entry cap.
    """

    model_config = ConfigDict(extra="forbid")

    level: LogLevel = Field(description="Severity; one of DEBUG, INFO, WARN, ERROR, FATAL.")
    service: str = Field(min_length=1, max_length=64, description="Emitting service name.")
    message: str = Field(min_length=1, max_length=512, description="Log text.")


class IngestAccepted(BaseModel):
    """Body of a successful ``POST /api/v1/logs/ingest``: the new id and the resulting store size.

    ``size`` is reported because it is the only externally visible evidence of the capacity bound
    — a caller ingesting past :data:`STORE_CAPACITY` watches this number stop climbing, which is
    the eviction being honest rather than silent.
    """

    id: str = Field(description="Identifier assigned to the new entry.")
    size: int = Field(description="Entries held by the store after the append.")
    entry: LogEntry = Field(description="The stored entry, exactly as `GET /logs/{id}` returns it.")


class LimitSnapshot(BaseModel):
    """The live :class:`~src.models.LimitDecision`'s numbers, echoed back by ``/whoami``.

    Raw per-gate fields plus the one documented derived quantity
    (:attr:`~src.models.LimitDecision.effective_remaining`). Deliberately **not** a re-derivation
    of the header values: :meth:`~src.models.LimitDecision.headers` owns what
    ``X-RateLimit-Reset`` means, and a handler that computed its own version of it would be a
    second definition of the same number that can drift from the one on the wire.
    """

    window_limit: int = Field(description="The tier's sustained per-minute ceiling.")
    window_used: int = Field(description="Weighted account-wide cost consumed in this window.")
    window_reset_sec: int = Field(description="Delay-seconds until the window recovers.")
    bucket_limit: int = Field(description="Token-bucket capacity for THIS endpoint.")
    bucket_remaining: int = Field(description="Tokens left in this endpoint's bucket.")
    bucket_reset_sec: int = Field(description="Delay-seconds until the bucket is full again.")
    effective_remaining: int = Field(
        description="The binding allowance across both rate gates — the smaller of the two."
    )
    daily_limit: int = Field(description="Requests allowed per UTC day; <= 0 means unenforced.")
    daily_remaining: int = Field(description="Requests left today; -1 when unenforced.")
    daily_reset_at: int = Field(description="Unix seconds at which the daily counter expires.")
    monthly_limit: int = Field(description="Requests allowed per UTC month.")
    monthly_remaining: int = Field(description="Requests left this month; -1 when unenforced.")
    monthly_reset_at: int = Field(description="Unix seconds at which the monthly counter expires.")
    degraded: bool = Field(
        description="True when the decision came from the C8 local fallback rather than Redis."
    )

    @classmethod
    def from_decision(cls, decision: LimitDecision) -> LimitSnapshot:
        """Project a decision onto the wire shape. Every value is read, none is recomputed."""
        return cls(
            window_limit=decision.window_limit,
            window_used=decision.window_used,
            window_reset_sec=decision.window_reset_sec,
            bucket_limit=decision.bucket_limit,
            bucket_remaining=decision.bucket_remaining,
            bucket_reset_sec=decision.bucket_reset_sec,
            effective_remaining=decision.effective_remaining,
            daily_limit=decision.daily_limit,
            daily_remaining=decision.daily_remaining,
            daily_reset_at=decision.daily_reset_at,
            monthly_limit=decision.monthly_limit,
            monthly_remaining=decision.monthly_remaining,
            monthly_reset_at=decision.monthly_reset_at,
            degraded=decision.degraded,
        )


class WhoAmI(BaseModel):
    """Body of ``GET /api/v1/whoami`` — who the limiter decided you are, and what you have left.

    Every field is nullable, and that is the transparency property rather than defensive
    vagueness: with ``RATE_LIMIT_ENABLED=false`` nothing resolved a principal and nothing decided
    a limit, so there is no honest value to report. A ``null`` is a state a client can detect; a
    fabricated ``user_id`` or a zeroed allowance is one it cannot, and it would happily pace itself
    off the fiction. ``metered`` says which of the two worlds the answer came from.
    """

    user_id: str | None = Field(description="Principal the request was metered against.")
    credential: CredentialKind | None = Field(
        description="Which scheme carried the credential — `api_key` or `jwt`."
    )
    tier: str | None = Field(description="Tier read from `user:{id}` inside the decision script.")
    endpoint: str | None = Field(
        description="Classified endpoint label — the component that went into the bucket key."
    )
    cost: int | None = Field(description="Weighted units this request consumed.")
    metered: bool = Field(
        description="False when RATE_LIMIT_ENABLED is off and no decision was made at all."
    )
    limits: LimitSnapshot | None = Field(
        default=None, description="The live decision's numbers; null when unmetered."
    )


# ---------------------------------------------------------------------------------------------
# The router
#
# ROUTE DECLARATION ORDER IS LOAD-BEARING — READ THIS BEFORE ADDING A ROUTE.
#
# Starlette matches in declaration order, first full match wins, and `/logs/{log_id}` is a
# wildcard: it matches `/logs/query` and `/logs/ingest` perfectly well ("query" is a fine path
# segment). Declared above them it would swallow both — a query request would land in
# `read_log` with `log_id="query"`, miss the store, and come back as a 404 that reads like a
# routing bug and is actually an ordering bug.
#
# `src.keys.ROUTE_TABLE` has the identical rule, written out in its own comment, and for the same
# reason: the exact rows must be tried before the parameterised one. The two orderings can break
# INDEPENDENTLY, and `verify_route_pricing` below checks both directions:
#
#   * reorder the TABLE and `/api/v1/logs/query` prices as `{id}`/1 while the router still serves
#     it from the expensive handler — the 5x discount, caught by check 2 (pricing);
#   * reorder THIS FILE and the price stays 5 while `read_log` serves the request — a 404 that
#     costs what the real endpoint costs, caught by check 3 (dispatch), which resolves each
#     contract row through the real router rather than comparing path templates. Templates are
#     order-independent, so nothing short of asking the router can see this.
#
# `/logs/{log_id}` is deliberately LAST and must stay last.
# ---------------------------------------------------------------------------------------------

router = APIRouter(prefix=API_V1_PREFIX, tags=["protected"])

#: Documented on every route in this file. The middleware emits it *above* the router, so no
#: handler here raises it and no handler here can suppress it — publishing it is how a client
#: discovers the limit from the contract instead of by tripping it.
_RATE_LIMITED_RESPONSE = {
    "description": (
        "Rate limit or quota exceeded. Emitted by the limiter middleware before this handler "
        "runs, so the request cost zero downstream work. Carries `Retry-After` (>= 1 second)."
    )
}

_UNAUTHENTICATED_RESPONSE = {
    "description": (
        "No usable credential. Emitted by the limiter middleware with a `WWW-Authenticate` "
        "challenge and, deliberately, **no** `X-RateLimit-*` headers — with no principal there "
        "is no bucket, so every number they could carry would be fabricated."
    )
}


@router.get(
    "/whoami",
    response_model=WhoAmI,
    summary="Echo the resolved principal and the live limit decision",
    description=(
        "The clean probe: it touches no store, allocates nothing, and does no downstream work, "
        "so a burst against it measures the limiter and not the handler. Costs 1 token."
    ),
    responses={
        status.HTTP_401_UNAUTHORIZED: _UNAUTHENTICATED_RESPONSE,
        status.HTTP_429_TOO_MANY_REQUESTS: _RATE_LIMITED_RESPONSE,
    },
)
async def whoami(request: Request) -> WhoAmI:
    """Report who the middleware decided is calling, and what allowance they have left.

    .. rubric:: The decision is read DEFENSIVELY, and that is a design constraint not caution

    ``request.state.rlq_decision`` is the obvious spelling and it is the wrong one here:
    Starlette's ``Request.state`` raises ``AttributeError`` for a key nobody set, and nobody sets
    this one when ``RATE_LIMIT_ENABLED=false`` — the middleware returns at step 3, above the point
    where the decision exists. So the obvious spelling produces a **500 on every request to this
    route whenever the limiter is switched off**, which would make this handler coupled to the
    limiter in exactly the way "transparent to route handlers" forbids. It would also break the
    one measurement the switch exists for: C14 baselines limiter overhead by running the *same*
    application with enforcement off, and a route that 500s in that configuration cannot be part
    of the baseline.

    ``scope.get("state")`` twice over is therefore the load-bearing spelling. The same applies to
    the ``state`` dict itself: it is optional in the ASGI HTTP scope, and while
    :func:`src.middleware._scope_state` creates it on every metered request, a future middleware
    order (or a direct ASGI call in a test) is not something this handler gets to assume.

    The credential *kind* is re-read from the raw headers rather than carried on the decision,
    which needs saying because it looks like a second auth path and is not one: it verifies
    nothing, grants nothing and refuses nothing. :func:`src.identity.parse_credential` is a pure
    header parse — the same function the resolver itself dispatches on — and only its ``kind`` half
    is used. **The presented secret is never echoed**: it is the caller's live API key or bearer
    token, and putting it in a response body would write it into every proxy log between here and
    them.
    """
    state: Mapping[str, Any] = request.scope.get("state") or {}
    decision: LimitDecision | None = state.get(SCOPE_DECISION_KEY)

    presented = parse_credential(request.scope["headers"])
    credential = presented[0] if presented is not None else None

    if decision is None:
        # Unmetered: the switch is off, so nothing resolved a principal and nothing decided a
        # limit. Nulls rather than zeros — see `WhoAmI`. The endpoint label survives, because the
        # middleware classifies ABOVE the switch precisely so this stays observable.
        return WhoAmI(
            user_id=None,
            credential=credential,
            tier=None,
            endpoint=state.get(SCOPE_ENDPOINT_KEY),
            cost=None,
            metered=False,
            limits=None,
        )

    return WhoAmI(
        user_id=decision.user_id,
        credential=credential,
        tier=decision.tier,
        # The decision's own label, not `scope[SCOPE_ENDPOINT_KEY]`: they are the same string by
        # construction, and reading it off the decision is what makes this handler's answer and
        # the bucket the request was actually charged to provably one value.
        endpoint=decision.endpoint,
        cost=decision.cost,
        metered=True,
        limits=LimitSnapshot.from_decision(decision),
    )


@router.get(
    "/logs/query",
    response_model=LogPage,
    summary="Filter and paginate the stub log corpus",
    description=(
        "The weighted-cost demonstrator: priced at 5 tokens because a read that fans out across "
        "a log store is not the same unit of work as a whoami, and charging both one token "
        "prices the expensive call as though it were free. An over-large `limit` is **clamped**, "
        "never rejected."
    ),
    responses={
        status.HTTP_401_UNAUTHORIZED: _UNAUTHENTICATED_RESPONSE,
        status.HTTP_429_TOO_MANY_REQUESTS: _RATE_LIMITED_RESPONSE,
    },
)
async def query_logs(
    request: Request,
    level: LogLevel | None = Query(
        default=None, description="Return only entries at this severity."
    ),
    service: str | None = Query(
        default=None, description="Return only entries emitted by this service."
    ),
    limit: int = Query(
        default=DEFAULT_PAGE_SIZE,
        description=(
            f"Page size. Clamped to [{MIN_PAGE_SIZE}, {MAX_PAGE_SIZE}] — an over-large value is "
            "answered with the ceiling and reported back in `page.limit`, never with a 422."
        ),
    ),
    offset: int = Query(default=0, description="Entries to skip. Floored at 0."),
) -> LogPage:
    """Return one page of the corpus. Filters are ANDed; ``page.total`` counts the filtered set.

    No ``Depends`` and no guard — see the module docstring. The caller reaching this line has
    already been authenticated and charged 5 tokens by the middleware, and this handler could not
    tell you whether that happened.
    """
    store = store_for(request)
    effective_limit = clamp_limit(limit)
    # `max` rather than a branch: there is exactly one wrong answer for a negative offset and it
    # is not worth an `if`.
    effective_offset = max(0, offset)

    items, total = store.query(
        level=level, service=service, limit=effective_limit, offset=effective_offset
    )
    return LogPage(
        items=items,
        page=PageInfo(
            limit=effective_limit,
            offset=effective_offset,
            returned=len(items),
            total=total,
            has_more=effective_offset + len(items) < total,
        ),
    )


@router.post(
    "/logs/ingest",
    response_model=IngestAccepted,
    status_code=status.HTTP_201_CREATED,
    summary="Append one entry to the stub log store",
    description=(
        "Priced at 2 tokens — a write is more expensive than a whoami and cheaper than a fan-out "
        "read. A malformed body is a 422 from pydantic, and that 422 is still **metered**: the "
        "limiter ran above the router, so the cost was consumed before this handler was reached."
    ),
    responses={
        status.HTTP_401_UNAUTHORIZED: _UNAUTHENTICATED_RESPONSE,
        status.HTTP_429_TOO_MANY_REQUESTS: _RATE_LIMITED_RESPONSE,
    },
)
async def ingest_log(request: Request, body: LogIngest) -> IngestAccepted:
    """Append ``body`` to this application's store and report the new id and the store size.

    ``201``, not ``200``: a resource was created and it has an identifier the caller can fetch
    from ``GET /api/v1/logs/{id}``, which is exactly what the status code is for.
    """
    store = store_for(request)
    entry = store.append(level=body.level, service=body.service, message=body.message)
    return IngestAccepted(id=entry.id, size=len(store), entry=entry)


@router.get(
    "/logs/{log_id}",
    response_model=LogEntry,
    summary="Fetch one log entry by id",
    description="Costs 1 token. Every distinct id collapses onto ONE bucket key — see below.",
    responses={
        status.HTTP_401_UNAUTHORIZED: _UNAUTHENTICATED_RESPONSE,
        status.HTTP_404_NOT_FOUND: {"description": "No entry with that id (or it was evicted)."},
        status.HTTP_429_TOO_MANY_REQUESTS: _RATE_LIMITED_RESPONSE,
    },
)
async def read_log(request: Request, log_id: str) -> LogEntry:
    """Return one entry, or 404.

    **This is the route the parameterised row in** :data:`src.keys.ROUTE_TABLE` **exists for.**
    ``/logs/1`` and ``/logs/2`` are one endpoint label, ``GET:/api/v1/logs/{id}``, and therefore
    one Redis bucket key. Priced on the raw path instead, the set of bucket keys would be the set
    of ids a caller can invent — unbounded, chosen by the attacker, and each fresh key arriving
    with a *full* allowance.

    The id is echoed back truncated. A 404 body that reflects arbitrary caller input at arbitrary
    length is a free amplifier; 64 characters shows any real id in full.
    """
    entry = store_for(request).get(log_id)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no log entry with id {log_id[:_ECHO_ID_CHARS]!r}",
        )
    return entry


# ---------------------------------------------------------------------------------------------
# The classifier/router cross-check
#
# The one piece of this module that is not a stub. See the second rubric in the module docstring:
# the router and `src.keys.classify` read the same path and answer different questions, and any
# input they disagree about is a pricing bypass. This turns "they agree" from a property somebody
# has to re-verify by reading two files into a startup assertion.
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RouteContract:
    """One mounted route, the handler it must dispatch to, and the ``ROUTE_TABLE`` row it prices to.

    ``label`` and ``category`` are written out rather than looked up from the table, deliberately.
    The check has to have an independent statement of intent to compare *both* sides against —
    derive the expectation from the table and the assertion becomes "the table equals itself",
    which passes just as happily when someone edits the table by mistake.

    ``endpoint`` is the **handler function**, and it is what makes the declaration-order rule
    above checkable rather than merely commented. A contract of paths alone cannot notice that
    ``/logs/{log_id}`` was moved above ``/logs/query``: the paths, the labels and the prices are
    all still correct, and the request is simply served by the wrong function. Naming the function
    is the only statement of intent that catches it.
    """

    method: str
    path: str
    label: str
    category: str
    endpoint: Callable[..., Any]


#: The four routes above, as data. Adding a route to this file means adding a row here; forgetting
#: to is a startup failure rather than a silent repricing.
#:
#: Order here is documentation, not behaviour — :func:`verify_route_pricing` resolves each row
#: through the real router, so it is the router's declaration order that is under test.
ROUTE_CONTRACT: Final[tuple[RouteContract, ...]] = (
    RouteContract(
        method="GET",
        path="/api/v1/logs/query",
        label="GET:/api/v1/logs/query",
        category="logs_query",
        endpoint=query_logs,
    ),
    RouteContract(
        method="POST",
        path="/api/v1/logs/ingest",
        label="POST:/api/v1/logs/ingest",
        category="logs_ingest",
        endpoint=ingest_log,
    ),
    RouteContract(
        method="GET",
        path="/api/v1/whoami",
        label="GET:/api/v1/whoami",
        category="default",
        endpoint=whoami,
    ),
    RouteContract(
        method="GET",
        path="/api/v1/logs/{log_id}",
        label="GET:/api/v1/logs/{id}",
        category="default",
        endpoint=read_log,
    ),
)

#: Matches a Starlette path parameter — ``{log_id}`` — in a mounted route's path template.
_PATH_PARAM_RE: Final = re.compile(r"\{[^{}]+\}")

#: What a path parameter is replaced with to build a probe path. Any value that is a legal single
#: path segment works; a literal digit string is what a real log id looks like.
SAMPLE_PATH_PARAM: Final = "42"


def sample_path(path: str) -> str:
    """Turn a mounted path template into a concrete path the classifier can be asked about.

    ``/api/v1/logs/{log_id}`` -> ``/api/v1/logs/42``. The classifier matches *concrete* paths (it
    never sees a template), so a cross-check that compared template strings would be comparing two
    things neither side actually uses.
    """
    return _PATH_PARAM_RE.sub(SAMPLE_PATH_PARAM, path)


def mounted_v1_routes(app: Starlette) -> set[tuple[str, str]]:
    """Every metered ``(METHOD, path)`` the app mounts under ``/api/v1``.

    Scoped to the versioned prefix on purpose. ``/health`` and the docs paths are exempt from
    metering entirely, and C15's ``GET /`` is outside the priced namespace — requiring *those* to
    appear in :data:`src.keys.ROUTE_TABLE` would be demanding a price for something nobody
    charges. Exempt paths under the prefix (C10's ``/api/v1/admin/*``) are filtered out by
    :func:`src.middleware.is_exempt` for the same reason, from the one definition of exemption
    rather than a second list here.

    A ``Mount`` has no ``methods`` and therefore contributes nothing: it cannot be priced
    per-method, and the only mount this project plans sits under an exempt prefix.
    """
    found: set[tuple[str, str]] = set()
    for route in app.routes:
        path: str = getattr(route, "path", "")
        if not path.startswith(_V1_ROUTE_PREFIX) or is_exempt(path):
            continue
        for method in getattr(route, "methods", None) or ():
            found.add((method.upper(), path))
    return found


def resolve_route(app: Starlette, method: str, path: str) -> Any:
    """Return the route Starlette would actually dispatch ``(method, path)`` to, or ``None``.

    A faithful miniature of ``starlette.routing.Router.app``'s matching loop: the **first FULL
    match wins**, and failing that the first PARTIAL match (path matched, method did not) is the
    one that produces the 405. Reimplemented here rather than driven through the app because the
    question is "which handler would this reach?", and answering it by sending a request would
    mean building a receive/send pair, running the handler, and inferring the answer from a
    response body — a great deal of apparatus to learn something the router will simply say.

    ``root_path`` is set to ``""`` explicitly: ``Route.matches`` calls
    :func:`starlette.routing.get_route_path`, so an absent key would work by default rather than
    by statement, and the whole subject of this module is not relying on a default that a
    deployment can change.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "method": method.upper(),
        "path": path,
        "root_path": "",
        "headers": [],
        "query_string": b"",
    }
    partial: Any = None
    for route in app.routes:
        match, _ = route.matches(scope)
        if match is Match.FULL:
            return route
        if match is Match.PARTIAL and partial is None:
            partial = route
    return partial


def verify_route_pricing(app: Starlette) -> list[str]:
    """Assert the router, this file's contract and :data:`src.keys.ROUTE_TABLE` all agree.

    Called by :func:`src.main.create_app` immediately after ``include_router``, so a disagreement
    is a **startup failure with a message naming both sides** rather than a caller quietly getting
    the 5-token endpoint for 1 token.

    Four checks, in the order that produces the most specific message first:

    1. **Coverage.** The set of labels this router serves must equal the set the table prices. A
       table row nothing serves is dead pricing; a metered route no row prices is charged
       ``other``/1.
    2. **Pricing.** Each mounted path, made concrete, must classify to the row it is declared
       against. This is the check that fires when someone renames ``/logs/query`` and forgets the
       table — the rename would otherwise reprice the project's most expensive endpoint by 80%.
    3. **Dispatch.** Each of those paths must also be *routed* to the handler it is declared
       against. Checks 1 and 2 compare path **templates**, and a template says nothing about
       declaration order: move ``/logs/{log_id}`` above ``/logs/query`` and all the templates,
       labels and prices stay correct while every query request is served by ``read_log`` — a 404
       that still costs 5 tokens. This is the only check that can see that, because it is the only
       one that asks the router rather than the declaration.
    4. **Completeness.** The routes actually mounted must be exactly the ones declared, so a route
       added without a contract row cannot slip past the first three by not being looked at.

    Returns:
        One human-readable line per route, in contract order — the cross-check as an artifact, so
        it can be printed by a build step or a verifier rather than only asserted.

    Raises:
        RuntimeError: on any disagreement, naming what is on each side.
    """
    priced = {row.label for row in ROUTE_TABLE}
    declared = {contract.label for contract in ROUTE_CONTRACT}
    if declared != priced:
        raise RuntimeError(
            "route pricing mismatch: src/keys.py ROUTE_TABLE and src/api/protected.py "
            f"ROUTE_CONTRACT describe different endpoints. Priced but unserved: "
            f"{sorted(priced - declared)}. Served but unpriced: {sorted(declared - priced)}. "
            "An unpriced metered route is charged 'other'/1 regardless of what it costs to serve."
        )

    report: list[str] = []
    for contract in ROUTE_CONTRACT:
        probe = sample_path(contract.path)
        actual = classify(contract.method, probe)
        expected = (contract.label, contract.category)
        if actual != expected:
            raise RuntimeError(
                f"route pricing mismatch: {contract.method} {contract.path} is mounted, but "
                f"src.keys.classify prices {probe!r} as {actual} rather than {expected}. The "
                "router and the classifier read the same path and would answer different "
                "questions about it — the caller is served one endpoint and charged for another."
            )

        matched = resolve_route(app, contract.method, probe)
        dispatched = getattr(matched, "endpoint", None)
        if dispatched is not contract.endpoint:
            raise RuntimeError(
                f"route dispatch mismatch: {contract.method} {probe} is priced as "
                f"{contract.label!r} ({contract.category}) but Starlette dispatches it to "
                f"{getattr(dispatched, '__qualname__', dispatched)!r}, not "
                f"{contract.endpoint.__qualname__!r}. Routes match in DECLARATION ORDER, first "
                "full match wins, so a parameterised route declared above an exact one swallows "
                "it — the request is served by the wrong handler at the right price, which is a "
                "404 that still costs what the real endpoint costs."
            )

        report.append(
            f"{contract.method:<4} {contract.path:<24} -> "
            f"{contract.label:<26} [{contract.category}] "
            f"dispatch={contract.endpoint.__qualname__}"
        )

    mounted = mounted_v1_routes(app)
    expected_pairs = {(contract.method, contract.path) for contract in ROUTE_CONTRACT}
    if mounted != expected_pairs:
        raise RuntimeError(
            "route pricing mismatch: the application's mounted routes under "
            f"{API_V1_PREFIX} are not the ones ROUTE_CONTRACT declares. Mounted but undeclared: "
            f"{sorted(mounted - expected_pairs)}. Declared but unmounted: "
            f"{sorted(expected_pairs - mounted)}. Every metered route under the versioned prefix "
            "must be declared so that its price is checked against ROUTE_TABLE."
        )

    return report

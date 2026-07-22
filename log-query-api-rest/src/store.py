"""The in-memory log store: an append-only ring, secondary indexes, and a cursor scan.

This module is the correctness core of the project. Every route in ``src/api/v1.py`` — the
paginated list, the single fetch, the structured search, the SSE tail and the stats snapshot —
is a thin translation layer over :class:`LogStore`. If the invariants below hold, those routes
cannot skip or duplicate an entry; if any of them breaks, no amount of care in the HTTP layer
can put it back.

The five invariants
-------------------

1. **``seq`` is the spine.** Every appended entry is stamped with a process-monotonic ``seq``
   that starts at 0, only ever increases, and is **never reused — not even after eviction**. It
   is simultaneously the sort key, the cursor anchor, the SSE event id, and the residency test.
   Everything else in this module follows from that one fact.

2. **The ring holds a contiguous ``seq`` range.** Appends push right, eviction drops the head,
   so at any instant the deque holds exactly ``[oldest_seq .. next_seq - 1]`` with no gaps. That
   is what makes ``seq -> record`` an index computation (``entries[seq - oldest]``) instead of a
   search, and what makes "was this anchor evicted?" the single comparison
   ``anchor < oldest_seq``.

3. **Newest-first pagination cannot skip or duplicate.** A ``DESC`` page anchored at ``A``
   returns records with ``seq < A``. Because ``seq`` only increases, anything appended during
   the walk lands strictly *above* ``A`` and is therefore invisible to the remainder of that
   walk — no duplicate — while everything below ``A`` keeps its position — no skip. Offset
   pagination has no such property, which is why it drifts (see :meth:`LogStore.scan`'s ``skip``
   parameter, and ``tests/unit/test_cursor.py`` where the difference is asserted rather than
   merely documented).

4. **Indexes are hints, never authorities.** ``level`` / ``service`` / ``host`` each map a value
   to an *ascending* list of seqs — ascending for free, because appends only ever add a larger
   seq to the tail. The smallest candidate list drives the scan, but
   :meth:`Filter.matches` still runs on every candidate, so a stale or over-broad index can
   only cost time, never correctness. Time needs no index at all: append order *is* time order
   for the generated corpus, and the predicate handles the rest.

5. **Eviction prunes everything it touches.** ``_by_id`` and the three secondary indexes are all
   pruned when a record leaves the ring. A ring that bounds the deque but lets ``_by_id`` grow
   forever is not a bounded store — it is a memory leak with a bounded scan.

What deliberately lives here and not on the wire
------------------------------------------------

``seq``, ``ts_epoch`` and ``message_lower`` live on :class:`StoredEntry`, the store's *internal*
record, rather than on :class:`~src.models.LogEntry`. ``LogEntry`` is the published wire model:
every field on it appears in the OpenAPI document and can never be removed. ``seq`` is a
storage detail (it would be meaningless to a client that reconnects to a restarted process), and
the other two are caches. Keeping them one level in means the store can change its
representation without a schema migration.

Concurrency
-----------

FastAPI handlers that touch the store are ``async def`` and run on a single event loop, so
:meth:`LogStore.append` and :meth:`LogStore.scan` can never interleave mid-operation — that,
not the lock, is the primary argument for this module's thread-safety. The
:class:`threading.Lock` around the mutating paths is cheap defense-in-depth against the two ways
that argument could quietly stop holding: running uvicorn with ``--workers`` inside one process
model, or someone later declaring a handler ``def`` instead of ``async def`` (FastAPI then runs
it in a threadpool). Read paths are deliberately lock-free: holding a lock across a 100k-record
scan would serialise every reader behind the slowest one, which is a worse failure than the race
it would prevent.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import heapq
import json
import logging
import operator
import re
import threading
import time
from bisect import bisect_left, bisect_right
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from itertools import islice
from types import MappingProxyType
from typing import Any

from src.models import (
    LEVEL_ORDER,
    MAX_FILTER_DEPTH,
    ORDER_OPS,
    FilterAll,
    FilterAny,
    FilterField,
    FilterLeaf,
    FilterNode,
    FilterNot,
    FilterOp,
    LogEntry,
    LogQuery,
    SortOrder,
    coerce_filter_value,
)

# ---------------------------------------------------------------------------------------------
# Tunables that are implementation detail, not configuration
#
# None of these are env-tunable on purpose: they describe internal bookkeeping, and an operator
# who could set them wrong would be able to trade correctness for nothing.
# ---------------------------------------------------------------------------------------------

#: Head-garbage floor before a secondary-index list is compacted. Eviction marks a list's head
#: as garbage in O(1); the actual ``del seqs[:garbage]`` is deferred until the garbage is both
#: at least this large AND at least half the list. That bounds every list at
#: ``2 * resident + INDEX_COMPACT_MIN`` entries while keeping the amortised cost of an eviction
#: at O(1) — each compaction of a length-L list pays for at least L/2 evictions.
#: ``tests/unit/test_store.py`` asserts that exact bound, so this constant is part of the
#: contract rather than a private whim.
INDEX_COMPACT_MIN = 32

#: How much smaller than the ring a candidate set must be before the index hint is worth taking.
#: The hint is only *usually* a win, which is the whole reason plan decision 3 calls indexes
#: "hints, never authorities": walking one costs a random access per candidate, while a linear
#: pass reads the ring sequentially through the deque's own C-level iterator. Measured at
#: ``STORE_CAPACITY=100000``: a full linear pass is ~12 ms (~0.12 us/record) and a hinted walk is
#: ~2.3 us/candidate, putting the break-even near 5% selectivity. 8 (12.5%) sits just past it, so
#: a *paged* request — which consumes only as many candidates as it returns and is what this
#: store actually serves — keeps the hint over the widest useful range while a full-set walk
#: (``count``/``iter_matching``) never degenerates into something worse than one linear pass.
INDEX_HINT_MIN_SELECTIVITY = 8

#: Sliding window for :meth:`LogStore.ingest_rate`, in seconds.
INGEST_WINDOW_SEC = 60.0

#: Hard cap on the number of append timestamps retained for the ingest-rate estimate. Bounds the
#: memory of the estimator itself: at a sustained 10k appends/sec a 60-second window would hold
#: 600k floats, so the rate is instead computed over the most recent ``INGEST_SAMPLE_CAP``
#: appends (still a *recent* window, just a shorter one at high rates).
INGEST_SAMPLE_CAP = 4096

#: The prefix every cursor carries. It is in the README's example (``"next_cursor": "b64:…"``)
#: and it is load-bearing: it makes a cursor self-identifying, so a client that pastes a raw
#: base64 blob or a leftover offset gets a clean 400 instead of a plausible-looking wrong page.
CURSOR_PREFIX = "b64:"

#: The urlsafe-base64 alphabet, minus padding (which :func:`encode_cursor` strips). Checked
#: explicitly because ``base64.urlsafe_b64decode`` **silently discards** characters outside the
#: alphabet rather than raising — decoding ``"!!!!"`` returns ``b""`` instead of failing. Garbage
#: in must be an error out, so the charset is validated before decoding.
_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")

#: The three indexed dimensions. Named here once so :meth:`LogStore.index_for`,
#: :meth:`LogStore._index_append` and :meth:`LogStore._prune_indexes` cannot disagree about the
#: set of indexes that exists.
INDEXED_DIMENSIONS: tuple[str, ...] = ("level", "service", "host")

#: Fallback per-subscriber queue depth, used only when a caller does not pass one. The real
#: value is ``SSE_QUEUE_SIZE`` from :class:`~src.config.Settings`, threaded in by the route — the
#: store deliberately does not import ``Settings`` (it is the correctness core and has no
#: configuration of its own), so the default exists purely so unit tests and ad-hoc callers can
#: subscribe without inventing a number.
DEFAULT_SUBSCRIBER_QUEUE_SIZE = 1000

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StoredEntry:
    """The store's internal record: a wire entry plus the three things scanning needs.

    ``frozen`` because the same instance is handed to a paginated scan, to every SSE subscriber
    and to the stats pass simultaneously; a mutable shared record is a data race waiting for a
    second worker. ``slots`` because at ``STORE_CAPACITY=100000`` the per-record ``__dict__``
    would cost tens of megabytes for nothing.

    Attributes:
        seq: Process-monotonic sequence number. Sort key, cursor anchor, SSE event id, and
            residency test — see the module docstring's invariant 1.
        entry: The public :class:`~src.models.LogEntry`, shared by reference and never copied.
        ts_epoch: ``entry.ts.timestamp()``, precomputed. A ``since``/``until`` scan compares this
            against two floats instead of re-deriving a POSIX timestamp from an aware datetime
            once per record per request — at 100k records that difference is the whole scan.
        message_lower: ``entry.message.lower()``, precomputed. The ``q`` filter is
            case-insensitive, so without this every scan would lower-case the entire corpus
            again, allocating a fresh string per record.
    """

    seq: int
    entry: LogEntry
    ts_epoch: float
    message_lower: str


@dataclass(frozen=True, slots=True)
class ScanResult:
    """One page of a scan, plus everything the caller needs to describe or continue it.

    Attributes:
        items: The matching records, in the requested order.
        next_seq: Anchor for the next page (the last item's ``seq``), or ``None`` when the walk
            is exhausted. ``None`` is exactly the condition under which C5 emits
            ``next_cursor: null``.
        has_more: Whether a further page exists. Determined by looking **one record past** the
            page, never by ``len(items) == limit`` — the latter is wrong on an exact-boundary
            page (20 matches read with ``limit=20`` would claim a 21st exists).
        truncated: The requested cursor anchor had already been evicted from the ring. C5 turns
            this into the ``X-Cursor-Truncated`` header. Returning fewer rows *silently* is the
            one behaviour that must never happen.
    """

    items: list[StoredEntry]
    next_seq: int | None
    has_more: bool
    truncated: bool


@dataclass(frozen=True, slots=True)
class CursorState:
    """The decoded contents of a cursor: a position, and the identity of the walk it belongs to.

    ``fingerprint`` and ``order`` are not decoration — they are what makes replaying a cursor
    against a *different* query an error rather than a plausible-looking wrong page. See
    :func:`decode_cursor`.
    """

    seq: int
    order: SortOrder
    fingerprint: str
    total: int


class InvalidCursor(ValueError):
    """A cursor was malformed, or belonged to a different filter or sort order.

    Subclasses :class:`ValueError` so a caller that has not yet learned about cursors still
    handles it sanely. C5 maps it to ``400`` with ``code="invalid_cursor"``.
    """


@dataclass(frozen=True, slots=True)
class Filter:
    """The flat, ANDed predicate bundle that ``GET /logs``, the SSE tail and ``/stats`` share.

    Every field is ``None`` for "unconstrained"; a **present** collection means "the value must
    be one of these". The distinction matters at the edge: an *empty* ``frozenset`` is a real
    constraint that matches nothing, which is not the same as ``None``. That is why every check
    below is ``is not None`` and never a truthiness test.

    Scalars are ANDed across fields, values are ORed within a field — the only thing a flat query
    string can honestly express. Anything more expressive is C9's business.

    .. rubric:: The extension seam — used by C9, and still the only one

    Three members are all that :meth:`LogStore.scan`, :meth:`LogStore.count` and
    :meth:`LogStore.iter_matching` ever call on a filter: :meth:`matches`, :meth:`index_hint`
    and :attr:`is_empty` (plus :meth:`fingerprint`, which only the cursor codec needs). C9's
    :func:`compile_filter` produces a :class:`CompiledFilter` exposing exactly those four members
    and nothing else, so ``POST /logs/search`` reaches the store through the identical door
    ``GET /logs`` uses and **none of the three signatures changed**. Do not add filtering logic
    to the store: it belongs behind ``matches``, or the "one evaluator, three entry points"
    guarantee stops holding.

    Attributes:
        levels: Allowed ``level`` values, as plain strings (``"ERROR"``), not enum members.
        services: Allowed ``service`` values.
        hosts: Allowed ``host`` values.
        since_epoch: **Inclusive** lower bound on ``ts``, as a POSIX timestamp.
        until_epoch: **Inclusive** upper bound on ``ts``, as a POSIX timestamp.
        q_lower: Already-lower-cased substring to look for in ``message``. Lower-casing happens
            once here rather than once per record per request.
    """

    levels: frozenset[str] | None = None
    services: frozenset[str] | None = None
    hosts: frozenset[str] | None = None
    since_epoch: float | None = None
    until_epoch: float | None = None
    q_lower: str | None = None

    @property
    def is_empty(self) -> bool:
        """True when nothing is constrained — every record matches.

        Lets :meth:`LogStore.count` answer with :meth:`LogStore.size` instead of walking the
        corpus, which is the difference between an O(1) and an O(n) ``page.total`` on the most
        common request there is (an unfiltered first page).
        """
        return (
            self.levels is None
            and self.services is None
            and self.hosts is None
            and self.since_epoch is None
            and self.until_epoch is None
            and self.q_lower is None
        )

    def matches(self, rec: StoredEntry) -> bool:
        """Evaluate the predicate against one stored record.

        Cheapest and most selective tests first: the three set memberships are single hash
        lookups, the two range comparisons are float compares, and the substring search — the
        only test whose cost scales with the data — runs last, so it is skipped entirely for
        every record another predicate already rejected.
        """
        if self.levels is not None and rec.entry.level.value not in self.levels:
            return False
        if self.services is not None and rec.entry.service not in self.services:
            return False
        if self.hosts is not None and rec.entry.host not in self.hosts:
            return False
        if self.since_epoch is not None and rec.ts_epoch < self.since_epoch:
            return False
        if self.until_epoch is not None and rec.ts_epoch > self.until_epoch:
            return False
        if self.q_lower is not None and self.q_lower not in rec.message_lower:
            return False
        return True

    def fingerprint(self) -> str:
        """A short, **stable** hash of this filter's identity, embedded in every cursor.

        Stability is the whole point, so this cannot use Python's builtin ``hash()``: string
        hashing is salted per process (``PYTHONHASHSEED``), so a cursor minted before a restart
        would stop matching itself afterwards. ``blake2b`` over a canonical JSON rendering is
        deterministic across processes and machines.

        It is a *fingerprint*, not a signature. It carries no secret and is not authenticated —
        see :func:`encode_cursor` for why that is the right call.
        """
        canonical = json.dumps(
            {
                # Sorted so that {"ERROR", "FATAL"} and {"FATAL", "ERROR"} — which are the same
                # filter — cannot fingerprint differently just because a set iterated in a
                # different order.
                "levels": None if self.levels is None else sorted(self.levels),
                "services": None if self.services is None else sorted(self.services),
                "hosts": None if self.hosts is None else sorted(self.hosts),
                "since": self.since_epoch,
                "until": self.until_epoch,
                "q": self.q_lower,
            },
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        return hashlib.blake2b(canonical.encode("utf-8"), digest_size=8).hexdigest()

    def index_hint(self, store: LogStore) -> list[list[int]] | None:
        """Pick the cheapest secondary index that bounds this filter's match set.

        Returns the ascending per-value seq lists of the **most selective** constrained
        dimension, or ``None`` when no indexed dimension is constrained (the caller then falls
        back to a linear pass). An empty list means "this filter constrains an indexed dimension
        and nothing in the store has any of those values" — a genuine zero-match answer, which
        is why the caller must distinguish ``[]`` from ``None``.

        Selectivity is measured as the total number of seqs the dimension would contribute.
        The count includes not-yet-compacted head garbage, which makes it a slight overestimate
        — acceptable, because this is a hint: :meth:`matches` runs on every candidate regardless,
        so a bad choice costs time and never correctness.
        """
        best: list[list[int]] | None = None
        best_size = -1
        for dimension, values in (
            ("level", self.levels),
            ("service", self.services),
            ("host", self.hosts),
        ):
            if values is None:
                continue
            index = store.index_for(dimension)
            lists = [index[value] for value in values if value in index]
            size = sum(len(seqs) for seqs in lists)
            if best is None or size < best_size:
                best, best_size = lists, size
        return best

    @classmethod
    def from_query(cls, query: LogQuery) -> Filter:
        """Build a :class:`Filter` from the shared :class:`~src.models.LogQuery` bundle.

        This is the *only* place the wire vocabulary is translated into the store's vocabulary,
        so ``GET /logs``, ``GET /logs/stream`` and ``GET /stats`` cannot end up describing
        different sets for the same query string.

        Two normalisations happen here and nowhere else:

        * ``level`` members become plain strings. :class:`~src.models.LogLevel` is a ``StrEnum``
          so a member would hash identically today, but pinning the index keys and the filter
          values to ``str`` means the indexes never depend on that.
        * An **empty** ``q`` collapses to ``None``. ``"" in anything`` is always true, so an
          empty substring filter is not a filter — treating it as one would fingerprint two
          identical walks differently and invalidate a perfectly good cursor.
        """
        return cls(
            levels=None if query.level is None else frozenset(lv.value for lv in query.level),
            services=None if query.service is None else frozenset(query.service),
            hosts=None if query.host is None else frozenset(query.host),
            since_epoch=None if query.since is None else query.since.timestamp(),
            until_epoch=None if query.until is None else query.until.timestamp(),
            q_lower=query.q.lower() if query.q else None,
        )


# =============================================================================================
#  The compiled boolean filter — the predicate behind ``POST /logs/search`` (C9)
# ---------------------------------------------------------------------------------------------
#  :class:`Filter` above is everything a query string can express. This is the other shape: the
#  nested boolean tree from :class:`~src.models.SearchRequest`, **compiled once** into a single
#  closure and then handed to exactly the same :meth:`LogStore.scan` / :meth:`LogStore.count` /
#  :meth:`LogStore.iter_matching`. The store never learns which of the two it is holding, and
#  that is the guarantee: one scanner, one pager, so the list route and the search route cannot
#  drift into describing different sets — or different envelopes — for the same predicate.
#
#  "Compiled once" is a performance contract, not a turn of phrase. A request may sweep the whole
#  100k-entry ring (twice, in fact: once for ``page.total`` and once for the page), so walking
#  pydantic models per record — attribute lookups through ``BaseModel.__getattr__``, an
#  ``isinstance`` ladder per node, re-coercing ``value`` per comparison — would put the tree's
#  entire parse cost inside the hot loop. Everything that can be resolved from the tree alone is
#  resolved here, before the first record is touched: the operand, the accessor, the comparison
#  function, and the shape of the boolean combination.
# =============================================================================================


#: One record's verdict. Every node of a filter tree compiles into one of these, and the tree
#: collapses into exactly one before any record is read.
Predicate = Callable[[StoredEntry], bool]

#: The six identity/order comparisons, resolved from the wire operator **once** at compile time.
#: By the time one of these runs, both operands are already plain comparable values — a float
#: epoch, a :data:`~src.models.LEVEL_ORDER` ordinal, or a string — so the comparison is arithmetic
#: rather than enum juggling. ``in``/``nin``/``contains`` are not here: membership and substring
#: search invert the operand order (``needle in haystack``), so they are spelled out in
#: :func:`_compile_leaf` instead of being forced into this table.
_COMPARISONS: Mapping[FilterOp, Callable[[Any, Any], bool]] = MappingProxyType(
    {
        FilterOp.EQ: operator.eq,
        FilterOp.NE: operator.ne,
        FilterOp.GT: operator.gt,
        FilterOp.GTE: operator.ge,
        FilterOp.LT: operator.lt,
        FilterOp.LTE: operator.le,
    }
)

#: Leaf field -> the secondary index that can bound it. Exactly the three dimensions in
#: :data:`INDEXED_DIMENSIONS`; ``message`` and ``ts`` have no index and never appear here.
#: ``ts`` deliberately gets none even though the ring is time-ordered — the linear pass already
#: walks it in seq (= time) order, so a "time index" would be a second spelling of the deque.
_HINTABLE_FIELDS: Mapping[FilterField, str] = MappingProxyType(
    {
        FilterField.LEVEL: "level",
        FilterField.SERVICE: "service",
        FilterField.HOST: "host",
    }
)

#: The only two operators an index hint may be derived from. Both name a **finite set of values**
#: the record must have, which is exactly what a value-keyed index can enumerate. Everything else
#: — ``ne``/``nin`` (a complement), ``contains`` (a substring), the order comparisons (a range over
#: an unindexed dimension) — would need the index to enumerate what a record is *not*, and the
#: index does not hold that.
_HINTABLE_OPS: frozenset[FilterOp] = frozenset({FilterOp.EQ, FilterOp.IN})


# -- field accessors: entry attribute -> the value the compiled comparison sees ----------------
#
# Module-level functions rather than lambdas so a profile or a traceback names them, and so each
# one is created once for the process instead of once per compiled leaf.


def _read_ts(rec: StoredEntry) -> float:
    """``ts`` as a POSIX epoch — the precomputed one, never re-derived from the datetime."""
    return rec.ts_epoch


def _read_level_ordinal(rec: StoredEntry) -> int:
    """``level`` as a severity ordinal, for ``gt``/``gte``/``lt``/``lte``.

    This is the whole reason :data:`~src.models.LEVEL_ORDER` exists: ``level >= "WARN"`` must mean
    *at least as severe as WARN* (WARN, ERROR, FATAL), not the lexicographic reading, under which
    ``"WARN" > "ERROR" > "DEBUG"`` — almost exactly backwards.
    """
    return LEVEL_ORDER[rec.entry.level]


def _read_level(rec: StoredEntry) -> str:
    """``level`` as its wire string, for the identity operators."""
    return rec.entry.level.value


def _read_service(rec: StoredEntry) -> str:
    return rec.entry.service


def _read_host(rec: StoredEntry) -> str:
    return rec.entry.host


def _read_message(rec: StoredEntry) -> str:
    return rec.entry.message


def _read_message_lower(rec: StoredEntry) -> str:
    """``message`` lower-cased — read from the store's precomputed cache, not recomputed.

    ``contains`` is case-insensitive, and :class:`StoredEntry` already carries
    ``message_lower`` for exactly this. Lower-casing here instead would allocate a fresh string
    per record per request, which at 100k records is the scan.
    """
    return rec.message_lower


def _read_service_lower(rec: StoredEntry) -> str:
    """``service`` lower-cased, computed per record — there is no cache for it, deliberately.

    ``service`` and ``host`` are short, low-cardinality identifiers and ``contains`` over them is
    a rare query; caching a lower-cased copy of each would cost two extra strings on every one of
    the 100k resident records to speed up a filter almost nobody writes. ``message`` is the
    opposite trade — long, always searched — which is why it, and only it, is precomputed.
    """
    return rec.entry.service.lower()


def _read_host_lower(rec: StoredEntry) -> str:
    """``host`` lower-cased per record. Same trade as :func:`_read_service_lower`."""
    return rec.entry.host.lower()


def _accessor(field: FilterField, op: FilterOp) -> Callable[[StoredEntry], Any]:
    """Pick the reader whose output matches the operand :func:`coerce_filter_value` produced.

    The two must be chosen by the **same** ``(field, op)`` pair or the comparison is nonsense: the
    coercion turns ``{"field": "level", "op": "gte", "value": "WARN"}`` into the ordinal ``2``, so
    the reader has to yield an ordinal too; it turns a ``contains`` needle into lower case, so the
    reader has to yield the lower-cased attribute. Every asymmetry here mirrors one there.
    """
    if field is FilterField.TS:
        return _read_ts
    if field is FilterField.LEVEL:
        return _read_level_ordinal if op in ORDER_OPS else _read_level
    if field is FilterField.MESSAGE:
        return _read_message_lower if op is FilterOp.CONTAINS else _read_message
    if field is FilterField.SERVICE:
        return _read_service_lower if op is FilterOp.CONTAINS else _read_service
    return _read_host_lower if op is FilterOp.CONTAINS else _read_host


def _compile_leaf(leaf: FilterLeaf) -> Predicate:
    """Compile one leaf predicate into a closure over its already-resolved operand.

    :func:`~src.models.coerce_filter_value` is called here — the *second* of its two calls, the
    first having happened inside :class:`~src.models.FilterLeaf`'s validator so a bad value is a
    ``422`` at the edge. Calling the same function again (rather than caching its result on the
    model) is what guarantees the value that validated and the operand that evaluates are produced
    by one definition: a compiler that re-derived the operand its own way is how a filter ends up
    passing validation and then meaning something else.
    """
    operand = coerce_filter_value(leaf.field, leaf.op, leaf.value)
    read = _accessor(leaf.field, leaf.op)
    op = leaf.op

    if op is FilterOp.IN:
        # `operand` is a frozenset, so this is a hash lookup rather than a list scan — which is
        # why `coerce_filter_value` returns one for the list operators.
        def match_in(rec: StoredEntry) -> bool:
            return read(rec) in operand

        return match_in

    if op is FilterOp.NIN:

        def match_nin(rec: StoredEntry) -> bool:
            return read(rec) not in operand

        return match_nin

    if op is FilterOp.CONTAINS:
        # Note the reversed operands: `needle in haystack`. This is the one operator whose
        # arguments do not read left-to-right like the wire form does, which is exactly why it is
        # written out here instead of being squeezed into `_COMPARISONS`.
        def match_contains(rec: StoredEntry) -> bool:
            return operand in read(rec)

        return match_contains

    compare = _COMPARISONS[op]

    def match_compare(rec: StoredEntry) -> bool:
        return compare(read(rec), operand)

    return match_compare


# -- boolean combinators ----------------------------------------------------------------------
#
# Each takes an already-compiled tuple of children and returns one closure. Children are kept in
# the order the client wrote them: the operators are pure, so evaluation order cannot change the
# answer, and reordering them by a guessed cost would be a query planner making decisions on
# statistics this store does not collect. Short-circuiting still applies within the given order.


def _conjunction(children: tuple[Predicate, ...]) -> Predicate:
    """AND over two or more children, short-circuiting on the first refusal."""

    def match(rec: StoredEntry) -> bool:
        for child in children:
            if not child(rec):
                return False
        return True

    return match


def _disjunction(children: tuple[Predicate, ...]) -> Predicate:
    """OR over two or more children, short-circuiting on the first acceptance."""

    def match(rec: StoredEntry) -> bool:
        for child in children:
            if child(rec):
                return True
        return False

    return match


def _negation(child: Predicate) -> Predicate:
    """NOT of one child."""

    def match(rec: StoredEntry) -> bool:
        return not child(rec)

    return match


def _match_everything(rec: StoredEntry) -> bool:  # noqa: ARG001 - signature is the contract
    """The constant-true predicate: an omitted filter, or one that folded to vacuous truth."""
    return True


def _match_nothing(rec: StoredEntry) -> bool:  # noqa: ARG001 - signature is the contract
    """The constant-false predicate, e.g. ``{"any": []}`` or a self-contradictory conjunction."""
    return False


def _compile_node(node: FilterNode, depth: int) -> Predicate | bool:
    """Compile one node, returning a closure — or the **constant** ``True``/``False``.

    Folding constants out is not an optimisation for its own sake; it is how the two empty
    collections get their documented meanings without either of them becoming a special case at
    evaluation time. ``{"all": []}`` is vacuously true and ``{"any": []}`` is vacuously false (see
    :class:`~src.models.FilterAll` and :class:`~src.models.FilterAny`), and once those are
    constants they propagate correctly through every enclosing node for free — ``{"not": {"any":
    []}}`` becomes the constant ``True``, ``{"all": [A, {"any": []}]}`` becomes the constant
    ``False``, and a tree that reduces to "match everything" can be recognised as such by
    :attr:`CompiledFilter.is_empty`.

    Every test against a folded child is an **identity** check (``is True`` / ``is False``), never
    a truthiness test. A compiled child is a function, and every function is truthy — ``if child:``
    would silently treat the constant-false node as a live predicate.

    Args:
        node: The node to compile.
        depth: 1-based nesting depth of this node.

    Raises:
        ValueError: When the tree is nested deeper than :data:`~src.models.MAX_FILTER_DEPTH`.
            :func:`~src.models.check_filter_shape` already enforces this on the HTTP path, before
            pydantic recurses; the check is repeated here because a tree constructed **in Python**
            (a test, C10's stream filter, a future internal caller) never passes through that
            validator, and the depth bound exists to stop a ``RecursionError`` — which is an
            availability bug, not a validation nicety. The check runs *before* recursing, so this
            function can never itself exhaust the stack proving that the input would have.
    """
    if depth > MAX_FILTER_DEPTH:
        raise ValueError(
            f"filter tree is nested deeper than {MAX_FILTER_DEPTH} levels; flatten it or "
            "split the query"
        )

    if isinstance(node, FilterLeaf):
        return _compile_leaf(node)

    if isinstance(node, FilterAll):
        compiled = [_compile_node(child, depth + 1) for child in node.all]
        if any(child is False for child in compiled):
            return False  # one impossible conjunct makes the whole conjunction impossible
        live = tuple(child for child in compiled if child is not True)
        if not live:
            return True  # empty (or all-vacuous) conjunction: vacuously true
        if len(live) == 1:
            return live[0]  # no combinator frame for a one-child AND
        return _conjunction(live)  # type: ignore[arg-type]  - bools filtered out above

    if isinstance(node, FilterAny):
        compiled = [_compile_node(child, depth + 1) for child in node.any]
        if any(child is True for child in compiled):
            return True  # one certain disjunct satisfies the whole disjunction
        live = tuple(child for child in compiled if child is not False)
        if not live:
            return False  # empty (or all-impossible) disjunction: vacuously false
        if len(live) == 1:
            return live[0]
        return _disjunction(live)  # type: ignore[arg-type]  - bools filtered out above

    if isinstance(node, FilterNot):
        child = _compile_node(node.not_, depth + 1)
        if child is True:
            return False
        if child is False:
            return True
        return _negation(child)

    # Unreachable through the union, which admits exactly the four shapes above. Loud rather than
    # silent: a node type added to `FilterNode` without a branch here must fail immediately, not
    # quietly stop filtering.
    raise TypeError(f"not a filter node: {type(node).__name__}")


def _hint_constraints(node: FilterNode | None) -> dict[str, frozenset[str]]:
    """Collect the ``dimension -> allowed values`` facts an index hint may **soundly** rest on.

    .. rubric:: This is the most dangerous function in the file — the soundness argument

    A hint is a *candidate* seq list. :meth:`LogStore._walk_hinted` runs the predicate over every
    candidate, so a hint that is too **wide** only costs time. A hint that is too **narrow** drops
    matching records from the answer, and the client cannot tell: the page looks well-formed and
    is simply missing rows. Every rule below exists to make "too narrow" unreachable, and the
    invariant they enforce is:

        the returned candidate set must be a **superset** of the true match set.

    So a fact is only harvested when *every* record that matches the whole tree necessarily has
    one of the collected values:

    * **A leaf at the root.** The tree is that one predicate, so ``level eq ERROR`` means every
      match is in the ``ERROR`` index list. Sound.
    * **A leaf directly inside a root ``all``.** A conjunction requires every child, so every
      match satisfies that leaf too. Sound.
    * **Nothing under an ``any``.** A disjunct is an *alternative*: a record matching a different
      branch is still a match, and it need not carry the value this branch names. Harvesting from
      it would silently drop every row the other branches contributed.
    * **Nothing under a ``not``.** A negated leaf is a statement about the records to **exclude**,
      so its index list is close to the exact complement of what the caller wants — the most
      precisely wrong candidate set available.
    * **Only ``eq`` and ``in``** (:data:`_HINTABLE_OPS`), and only on an indexed field
      (:data:`_HINTABLE_FIELDS`). Everything else names a complement, a substring or a range,
      none of which a value-keyed index can enumerate.

    A conjunction nested *inside* the root conjunction (``{"all": [{"all": [leaf]}]}``) would in
    fact be sound to mine as well. It is deliberately **not** mined: every additional rule here is
    another chance to be subtly wrong in the one direction that returns a silently short answer,
    and the fallback — no hint, one linear pass — is always correct and never worse than the scan
    the store would run anyway. When in doubt, do not hint.

    Returns:
        ``{}`` when nothing can be harvested, which the caller turns into "no hint, scan
        linearly". Otherwise a dimension -> value-set map; **two constraints on the same
        dimension intersect**, because inside an ``all`` a record must satisfy both. An empty
        intersection is retained rather than dropped: it means no record can match, and an empty
        candidate list is the correct — and cheapest — expression of that.
    """
    if node is None:
        return {}

    if isinstance(node, FilterLeaf):
        children: tuple[FilterNode, ...] = (node,)
    elif isinstance(node, FilterAll):
        children = tuple(node.all)
    else:
        # `any` or `not` at the root. Neither can contribute anything sound; see above.
        return {}

    constraints: dict[str, frozenset[str]] = {}
    for child in children:
        if not isinstance(child, FilterLeaf):
            continue
        dimension = _HINTABLE_FIELDS.get(child.field)
        if dimension is None or child.op not in _HINTABLE_OPS:
            continue
        operand = coerce_filter_value(child.field, child.op, child.value)
        values: frozenset[str] = (
            frozenset(operand) if child.op is FilterOp.IN else frozenset({operand})
        )
        existing = constraints.get(dimension)
        constraints[dimension] = values if existing is None else existing & values
    return constraints


def _canonical(node: FilterNode | None) -> Any:
    """Render a tree as a plain JSON-able structure that depends only on its **meaning**.

    Feeds :meth:`CompiledFilter.fingerprint`, so what is normalised here is exactly what two
    filters may differ in while still sharing a cursor:

    * ``None`` renders as ``{"all": []}`` — an omitted filter and an empty conjunction both mean
      "match everything", so a cursor minted by one is legitimately usable by the other.
    * Leaf values render **post-coercion**: ``"2026-07-27T10:00:00Z"`` and its epoch spelling
      become the same float, ``{"op": "gte", "value": "WARN"}`` becomes the ordinal, and a
      ``contains`` needle is already lower-cased. Two spellings of one predicate are one
      predicate.
    * ``in``/``nin`` value sets are **sorted**, so ``["ERROR","FATAL"]`` and ``["FATAL","ERROR"]``
      — and any duplicate within either — agree. Sorting is safe because a leaf's list is
      homogeneous by construction: every element went through the same ``(field, op)`` coercion.
    * ``all``/``any`` children are **sorted by their own canonical rendering**, because AND and OR
      are commutative: reordering the boxes a UI ticked is not a different search.

    Deliberately *not* normalised: duplicate children, double negation, and the associativity of
    nested ``all``/``any``. Each would be a further true simplification, and each is another rule
    that must stay correct forever to avoid two genuinely different filters colliding on one
    fingerprint. The cost of not doing them is only a cursor that has to be re-minted.
    """
    if node is None:
        return {"all": []}
    if isinstance(node, FilterLeaf):
        operand = coerce_filter_value(node.field, node.op, node.value)
        value = sorted(operand) if isinstance(operand, frozenset) else operand
        return {"f": node.field.value, "o": node.op.value, "v": value}
    if isinstance(node, FilterAll):
        return {"all": sorted((_canonical(child) for child in node.all), key=_canonical_key)}
    if isinstance(node, FilterAny):
        return {"any": sorted((_canonical(child) for child in node.any), key=_canonical_key)}
    if isinstance(node, FilterNot):
        return {"not": _canonical(node.not_)}
    raise TypeError(f"not a filter node: {type(node).__name__}")  # pragma: no cover


def _canonical_key(rendered: Any) -> str:
    """Sort key for canonical child nodes: their own deterministic JSON text.

    Dicts are not orderable, so the children cannot be sorted directly. Serialising each one is
    both a total order and exactly the order that "identical meaning sorts identically" requires.
    """
    return json.dumps(rendered, separators=(",", ":"), sort_keys=True, allow_nan=False)


class CompiledFilter:
    """A boolean filter tree, compiled — the store's *other* filter, and its only other one.

    Exposes precisely the four members :class:`LogStore` calls on a filter — :attr:`matches`,
    :meth:`fingerprint`, :meth:`index_hint` and :attr:`is_empty` — and nothing else, so
    ``POST /logs/search`` reaches :meth:`LogStore.scan` through the identical door ``GET /logs``
    uses and not one of the store's three signatures changed to admit it. See :class:`Filter`'s
    "extension seam" note.

    Built only by :func:`compile_filter`; the constructor takes the already-resolved parts.
    """

    __slots__ = ("_constraints", "_fingerprint", "_matches_everything", "matches")

    #: The compiled predicate, bound as an **attribute rather than a forwarding method**.
    #:
    #: :meth:`LogStore._walk_linear` hoists ``matches = flt.matches`` out of its loop and calls it
    #: once per record. A method that forwarded to ``self._predicate`` would add a Python frame to
    #: every one of those calls — on the order of 6 ms across a 100k-record pass, roughly half the
    #: cost of the pass itself — to say nothing new. Binding the closure directly removes the
    #: layer while leaving every call site spelled exactly as it is for :class:`Filter`.
    matches: Predicate

    def __init__(
        self,
        *,
        predicate: Predicate,
        fingerprint: str,
        constraints: Mapping[str, frozenset[str]],
        matches_everything: bool,
    ) -> None:
        self.matches = predicate
        self._fingerprint = fingerprint
        self._constraints = dict(constraints)
        self._matches_everything = matches_everything

    @property
    def is_empty(self) -> bool:
        """True only when **every** record matches — ``None``, ``{"all": []}``, or equivalent.

        :meth:`LogStore.count` reads this to answer ``page.total`` with :meth:`LogStore.size`
        instead of walking the corpus, so it means "unconstrained" and never merely "trivial".
        ``{"any": []}`` matches *nothing* and is emphatically not empty in this sense — reporting
        it as such would put the whole corpus's size on a page with zero items.
        """
        return self._matches_everything

    def fingerprint(self) -> str:
        """The stable identity of this search, as embedded in every cursor it mints.

        Precomputed at compile time (the tree is at most
        :data:`~src.models.MAX_FILTER_NODES` nodes, so it costs nothing) and returned verbatim.
        Same construction as :meth:`Filter.fingerprint` — ``blake2b`` over a canonical JSON
        rendering, 8-byte digest, 16 hex characters — so a search cursor and a list cursor are
        indistinguishable in form, which is what keeps the cursor genuinely opaque rather than a
        label telling a client which route minted it.

        The rendering is namespaced (``{"tree": …}``) so it cannot collide with the flat filter's,
        and it carries the sort order, because the walk's identity is the filter *and* its
        direction. See :func:`_canonical` for exactly which differences are normalised away.
        """
        return self._fingerprint

    def index_hint(self, store: LogStore) -> list[list[int]] | None:
        """The cheapest sound candidate set, or ``None`` to scan linearly.

        Structurally identical to :meth:`Filter.index_hint` — the most selective constrained
        dimension wins, measured by total seqs contributed — over the constraints
        :func:`_hint_constraints` proved safe to harvest. ``None`` means "no indexed dimension is
        soundly constrained"; ``[]`` means "constrained, and the store holds nothing with any of
        those values", which is a real zero-match answer and why the caller must tell the two
        apart.

        Everything about **why** a given constraint is or is not present lives in
        :func:`_hint_constraints`. This method only chooses between the ones already collected.
        """
        if not self._constraints:
            return None

        best: list[list[int]] | None = None
        best_size = -1
        for dimension, values in self._constraints.items():
            index = store.index_for(dimension)
            lists = [index[value] for value in values if value in index]
            size = sum(len(seqs) for seqs in lists)
            if best is None or size < best_size:
                best, best_size = lists, size
        return best


def compile_filter(node: FilterNode | None, order: SortOrder) -> CompiledFilter:
    """Compile a :class:`~src.models.SearchRequest` filter tree into a store-ready predicate.

    The single translation from C9's wire vocabulary into the store's, exactly as
    :meth:`Filter.from_query` is the single translation from C5's. Everything expensive happens
    here, once per request: the operands are coerced, the accessors and comparisons are resolved,
    the constants are folded, the fingerprint is computed and the sound index constraints are
    harvested. What comes back is a closure and three cached facts.

    Args:
        node: The tree, or ``None`` for "match everything" (:attr:`CompiledFilter.is_empty` is
            then true, and ``page.total`` costs O(1) instead of a full sweep).
        order: The walk's sort direction. It is folded into the fingerprint rather than merely
            passed alongside it, because a cursor is a position in a *walk* and a walk is a filter
            plus a direction — that is the only reason this function needs to know the order at
            all; it does not affect the predicate. (:func:`decode_cursor` checks the order
            separately and first, so a reversed-direction replay still gets the specific "other
            sort order" message rather than a generic mismatch.)

    Raises:
        ValueError: If the tree is nested deeper than :data:`~src.models.MAX_FILTER_DEPTH`.
        TypeError: If ``node`` is not a filter node.
    """
    compiled = True if node is None else _compile_node(node, 1)

    # The two constants become real callables here so `matches` is uniformly callable, and
    # `is_empty` records which of them (if either) this is. Only the always-true case is "empty":
    # it is the one the store may answer with `size()` instead of a walk.
    if compiled is True:
        predicate: Predicate = _match_everything
    elif compiled is False:
        predicate = _match_nothing
    else:
        predicate = compiled

    canonical = json.dumps(
        {"tree": _canonical(node), "order": order.value},
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return CompiledFilter(
        predicate=predicate,
        # Same digest size and rendering as `Filter.fingerprint` — see that method for why a
        # builtin `hash()` cannot be used here (it is salted per process, so a cursor would stop
        # validating against its own filter after a restart).
        fingerprint=hashlib.blake2b(canonical.encode("utf-8"), digest_size=8).hexdigest(),
        constraints=_hint_constraints(node),
        matches_everything=compiled is True,
    )


# ---------------------------------------------------------------------------------------------
# Cursor codec
# ---------------------------------------------------------------------------------------------


def encode_cursor(*, seq: int, order: SortOrder, fingerprint: str, total: int) -> str:
    """Encode a walk position into the opaque ``b64:…`` cursor the client echoes back.

    The payload keys are one character each (``s``/``o``/``f``/``t``) purely to keep the cursor
    short enough to sit comfortably in a query string next to everything else.

    **The cursor is deliberately neither signed nor encrypted, and that is a decision, not an
    omission.** It carries no secret: a position, a sort direction, a filter fingerprint and a
    row count. Forging one cannot reach data the caller's role does not already permit — the
    RBAC dependency has already run by the time a cursor is decoded, and the store applies the
    *request's* filter, not the cursor's. Signing would add a key to rotate and a class of 500s
    for no privilege boundary. What the fingerprint does buy is **coherence**: it makes replaying
    a cursor against a different filter a loud 400 instead of a quiet wrong answer.

    Args:
        seq: The anchor — the ``seq`` of the last entry on the page just served.
        order: The sort direction this walk is using.
        fingerprint: :meth:`Filter.fingerprint` of the filter this walk belongs to.
        total: Match count as of walk start, carried so every page of a walk reports the same
            ``page.total`` even while the corpus grows underneath it.
    """
    payload = {"s": int(seq), "o": order.value, "f": fingerprint, "t": int(total)}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    # Padding is stripped so the cursor never contains '=' — harmless in a query string, but it
    # invites double-encoding bugs in clients that build URLs by hand. decode_cursor re-adds it.
    return CURSOR_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(
    raw: str, *, expected_fingerprint: str, expected_order: SortOrder
) -> CursorState:
    """Decode and **validate** a cursor, or raise :class:`InvalidCursor`.

    Every failure mode raises: a missing ``b64:`` prefix, a non-base64 body, bytes that are not
    UTF-8 JSON, JSON that is not an object, a missing key, a wrongly-typed value, a fingerprint
    that belongs to a different filter, or an order that belongs to a different walk.

    The last two are the important ones. A cursor is a position *within a specific walk*; a
    position from a different walk is still a perfectly well-formed integer, so without the
    fingerprint check the store would happily serve a page that is internally consistent and
    completely wrong — the client would silently skip or repeat an arbitrary slice of the
    corpus. **Never return a wrong answer where an error is available.** C5 maps this to a 400.

    Args:
        raw: The cursor string exactly as the client supplied it.
        expected_fingerprint: :meth:`Filter.fingerprint` of the *current request's* filter.
        expected_order: The *current request's* sort order.
    """
    if not isinstance(raw, str) or not raw.startswith(CURSOR_PREFIX):
        raise InvalidCursor("cursor must be an opaque 'b64:' value from a previous page")

    body = raw[len(CURSOR_PREFIX) :]
    if not body or not _B64URL_RE.match(body):
        # Checked explicitly: urlsafe_b64decode discards out-of-alphabet characters instead of
        # raising, so "b64:!!!!" would otherwise decode to b"" and fail later with a confusing
        # JSON error rather than an honest "this is not a cursor".
        raise InvalidCursor("cursor payload is not urlsafe base64")

    try:
        decoded = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
    except (binascii.Error, ValueError) as exc:  # pragma: no cover - charset check precedes it
        raise InvalidCursor("cursor payload is not urlsafe base64") from exc

    try:
        payload = json.loads(decoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidCursor("cursor payload is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise InvalidCursor("cursor payload must be a JSON object")

    missing = {"s", "o", "f", "t"} - payload.keys()
    if missing:
        raise InvalidCursor(f"cursor payload is missing {sorted(missing)}")

    seq, order_raw, fingerprint, total = payload["s"], payload["o"], payload["f"], payload["t"]

    # `isinstance(True, int)` is True in Python, so booleans are excluded explicitly — otherwise
    # {"s": true} would decode to the anchor 1.
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        raise InvalidCursor("cursor anchor must be a non-negative integer")
    if not isinstance(total, int) or isinstance(total, bool) or total < 0:
        raise InvalidCursor("cursor total must be a non-negative integer")
    if not isinstance(fingerprint, str):
        raise InvalidCursor("cursor fingerprint must be a string")
    if not isinstance(order_raw, str):
        raise InvalidCursor("cursor order must be a string")
    try:
        order = SortOrder(order_raw)
    except ValueError as exc:
        raise InvalidCursor(f"cursor carries an unknown sort order {order_raw!r}") from exc

    if order is not expected_order:
        raise InvalidCursor(
            "cursor belongs to a walk in the other sort order — start a new walk"
        )
    if fingerprint != expected_fingerprint:
        raise InvalidCursor("cursor belongs to a different filter — start a new walk")

    return CursorState(seq=seq, order=order, fingerprint=fingerprint, total=total)


# ---------------------------------------------------------------------------------------------
# SSE fan-out
#
# The tail half of the read surface. `GET /logs` walks the history under a filter; a subscriber
# holds the *same* filter against the future, and `append` is the single place the two meet.
#
# Three rules govern everything below, and every one of them exists because the alternative is a
# way for a client to hurt the server:
#
#   1. **A subscriber is a bounded queue, never an unbounded one.** `asyncio.Queue(maxsize=N)` —
#      so a reader that stops reading costs at most N record *references* (the records
#      themselves are already resident in the ring and shared by reference; a queued entry adds
#      a pointer, not a copy).
#   2. **Overflow drops the subscriber; it never blocks the writer and never grows the buffer.**
#      `append` publishes with `put_nowait` and treats `QueueFull` as "this consumer is gone".
#      Blocking would let one stalled reader stall ingest for everybody; growing would hand a
#      stalled reader the process's memory.
#   3. **Fan-out can never break ingest.** Every per-subscriber step runs inside a `try`, and any
#      failure removes *that* subscriber. `POST /logs` returns 201 even if every subscriber in
#      the process is broken, because the entry did land in the ring — which is what 201 claims.
# ---------------------------------------------------------------------------------------------


class StreamLimitExceeded(RuntimeError):
    """A principal already holds :data:`max_streams` concurrent subscriptions.

    Raised by :meth:`LogStore.subscribe` and mapped by ``GET /logs/stream`` to a ``429`` whose
    detail is deliberately *unlike* the rate limiter's — one long-lived connection and one
    thousand quick requests are different resources, and an operator reading a log line (or a
    test reading a body) must be able to tell which ceiling was hit.
    """

    def __init__(self, subject: str, limit: int) -> None:
        self.subject = subject
        self.limit = limit
        super().__init__(
            f"principal {subject!r} already holds {limit} concurrent stream(s)"
        )


class Subscription:
    """One live SSE subscriber: a bounded queue, the filter it is watching, and its lifecycle.

    Not a ``dataclass``: :attr:`released` and :attr:`dropped` mutate, the object is used as a
    dict key (so it needs identity hashing, which a non-frozen dataclass would keep but an
    ``eq=True`` one would break), and ``__slots__`` keeps it cheap enough that the per-principal
    cap is the only thing bounding how many exist.

    Attributes:
        subject: The owning principal's ``sub`` claim. The key the per-principal stream counter
            is kept under, captured here so :meth:`LogStore.unsubscribe` can decrement without
            being told who to decrement — a caller that had to pass the subject again would be a
            caller that could pass the wrong one.
        queue: Bounded ``asyncio.Queue``. Holds :class:`StoredEntry` records, plus at most one
            ``None`` **terminal sentinel** meaning "this subscription is over, stop reading".
        flt: The compiled filter. Evaluated once per subscriber per append, in the writer's
            thread — which is why a filter that raises must cost only this subscriber.
        released: Set exactly once, by :meth:`LogStore.unsubscribe`. **This flag is the whole
            idempotency mechanism**: the route has six exit paths that all unsubscribe, and the
            per-principal counter must decrement exactly once no matter how many of them run.
        dropped: True when the subscription ended because the consumer could not keep up, as
            opposed to disconnecting or being closed at shutdown. The route turns it into a
            terminal ``event: dropped`` frame, so a client learns it was cut off rather than
            silently believing it saw everything.
    """

    __slots__ = ("dropped", "flt", "queue", "released", "subject")

    def __init__(
        self,
        *,
        subject: str,
        flt: Filter | CompiledFilter,
        queue_size: int = DEFAULT_SUBSCRIBER_QUEUE_SIZE,
    ) -> None:
        self.subject = subject
        self.flt = flt
        #: `maxsize` is the hard memory bound. It is passed positionally-by-keyword here rather
        #: than defaulted to 0 (unbounded) anywhere, because an unbounded queue is precisely the
        #: bug this class exists to make unwritable.
        self.queue: asyncio.Queue[StoredEntry | None] = asyncio.Queue(maxsize=queue_size)
        self.released = False
        self.dropped = False

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"Subscription(subject={self.subject!r}, queued={self.queue.qsize()}, "
            f"released={self.released}, dropped={self.dropped})"
        )


# ---------------------------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------------------------


class LogStore:
    """A fixed-capacity, append-only ring of log entries with seq-anchored cursor scanning.

    See the module docstring for the invariants. The public surface is small on purpose: append
    (one or many), fetch by id, describe the ring, and scan/count/iterate under a
    :class:`Filter`.
    """

    __slots__ = (
        "_append_times",
        "_by_host",
        "_by_id",
        "_by_level",
        "_by_service",
        "_capacity",
        "_entries",
        "_heads",
        "_indexes",
        "_lock",
        "_next_seq",
        "_stream_counts",
        "_sub_lock",
        "_subscribers",
        "_time_func",
    )

    def __init__(self, capacity: int, *, time_func: Callable[[], float] = time.time) -> None:
        """Build an empty store.

        Args:
            capacity: Maximum entries retained. Appending past it evicts the oldest.
            time_func: Clock used *only* for the ingest-rate estimate. Injectable so the rate
                maths is testable without a single ``sleep()`` in the suite — a test drives a
                fake clock forward deterministically and gets an exact expected rate.

        Raises:
            ValueError: If ``capacity`` is less than 1. A zero-capacity ring would accept every
                append and retain nothing, which is not a degraded store but a silent data sink;
                failing at construction makes a mis-set ``STORE_CAPACITY`` a startup error.
        """
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")

        self._capacity = capacity
        #: Ascending by seq, always contiguous. ``maxlen`` gives us head eviction for free.
        self._entries: deque[StoredEntry] = deque(maxlen=capacity)
        #: O(1) ``GET /logs/{id}``. Pruned on eviction — see :meth:`_append_locked`.
        self._by_id: dict[str, StoredEntry] = {}

        # value -> ascending list of seqs. Ascending without ever sorting, because an append can
        # only ever add a seq larger than every seq already present. Nothing is ever inserted in
        # the middle of these lists; if it were, the bisect-based scan below would be wrong.
        self._by_level: dict[str, list[int]] = {}
        self._by_service: dict[str, list[int]] = {}
        self._by_host: dict[str, list[int]] = {}

        # Dimension -> the dict above (shared by reference, so `_indexes["level"] is _by_level`)
        # and dimension -> {value: count of evicted seqs still sitting at the head of its list}.
        self._indexes: dict[str, dict[str, list[int]]] = {
            "level": self._by_level,
            "service": self._by_service,
            "host": self._by_host,
        }
        self._heads: dict[str, dict[str, int]] = {dim: {} for dim in INDEXED_DIMENSIONS}

        #: Never reset, never reused — invariant 1. Also the count of appends ever made.
        self._next_seq = 0
        self._time_func = time_func
        self._append_times: deque[float] = deque(maxlen=INGEST_SAMPLE_CAP)
        self._lock = threading.Lock()

        # -- SSE fan-out state ---------------------------------------------------------------
        #
        #: Live subscribers, as an **ordered set** (a dict with ignored values). Order makes
        #: fan-out deterministic, which is what lets a multi-subscriber test assert on more than
        #: "somebody got it"; dict membership makes removal O(1) rather than a list scan, which
        #: matters because removal happens on every disconnect.
        self._subscribers: dict[Subscription, None] = {}
        #: subject -> live subscription count. Keys are deleted at zero rather than left at 0,
        #: so a process that churns through a million principals does not accumulate a million
        #: dict entries — the same bounded-bookkeeping argument the rate limiter's `sweep` makes.
        self._stream_counts: dict[str, int] = {}
        #: A **second** lock, guarding only the two structures above.
        #:
        #: It is separate from `_lock` on purpose and the reason is a deadlock, not tidiness.
        #: Publication happens inside `_append_locked`, i.e. with `_lock` already held; the drop
        #: path there calls `unsubscribe`, which must take a lock of its own. If that were
        #: `_lock` — a plain, non-reentrant `threading.Lock` — a single slow consumer would
        #: deadlock the writer permanently. Lock ordering is therefore fixed and one-directional:
        #: `_lock` may be held while taking `_sub_lock`, never the reverse. No method below takes
        #: `_lock`, so the reverse edge does not exist and no cycle can form.
        self._sub_lock = threading.Lock()

    # -- writes ---------------------------------------------------------------------------

    def append(self, entry: LogEntry) -> StoredEntry:
        """Append one entry, evicting the oldest if the ring is full. Returns the stored record.

        The returned :class:`StoredEntry` carries the assigned ``seq``, which C10 uses as the
        SSE event id and C7 uses to confirm the write landed.
        """
        with self._lock:
            return self._append_locked(entry)

    def append_many(self, entries: Iterable[LogEntry]) -> int:
        """Append many entries and return how many were appended — the bulk seed path.

        Takes the lock **once** for the whole batch rather than once per entry. Seeding 10,000
        entries at startup through :meth:`append` would be 10,000 uncontended lock round-trips
        for no benefit; and while the batch runs there is no meaningful intermediate state for
        another thread to observe anyway.
        """
        count = 0
        with self._lock:
            for entry in entries:
                self._append_locked(entry)
                count += 1
        return count

    def _append_locked(self, entry: LogEntry) -> StoredEntry:
        """Assign a seq, evict if full, and keep every index consistent. Caller holds the lock."""
        seq = self._next_seq
        self._next_seq = seq + 1
        record = StoredEntry(
            seq=seq,
            entry=entry,
            ts_epoch=entry.ts.timestamp(),
            message_lower=entry.message.lower(),
        )

        entries = self._entries
        # The head must be captured BEFORE the append: `deque(maxlen=…).append` drops it
        # silently, and a dropped record we never saw is a record whose index entries and
        # `_by_id` mapping we can never prune. `_by_id` in particular is the single easiest
        # memory leak to write in this file — it is a dict keyed by an unbounded id space
        # sitting behind a bounded ring.
        evicted = entries[0] if len(entries) == self._capacity else None
        entries.append(record)

        if evicted is not None:
            # Only drop the id mapping if it still points at the record leaving the ring: a
            # later append that reused the same id already owns the key, and deleting it here
            # would make a *resident* entry unfetchable.
            if self._by_id.get(evicted.entry.id) is evicted:
                del self._by_id[evicted.entry.id]
            self._prune_indexes(evicted)

        # Set after the eviction prune, so an entry that reuses the evicted entry's id wins.
        self._by_id[entry.id] = record
        self._index_append(record)
        self._append_times.append(self._time_func())

        # Publication is the LAST thing an append does, and it is deliberately inside the
        # critical section rather than after it. Two reasons:
        #
        #   * The record must be resident before any subscriber can see it. A frame carrying
        #     seq N whose record is not yet in the ring would make `Last-Event-ID: N` resume from
        #     an anchor the store does not have.
        #   * `append_many` funnels through here too, so a stream that is open during a bulk
        #     append sees the batch instead of silently missing it.
        #
        # Being inside the section is only safe because `_publish` takes `_sub_lock` and never
        # `_lock` (see `__init__`), and because it cannot raise — see its own docstring.
        self._publish(record)
        return record

    def _index_append(self, record: StoredEntry) -> None:
        """Append this record's seq to each of the three secondary index lists."""
        entry = record.entry
        self._by_level.setdefault(entry.level.value, []).append(record.seq)
        self._by_service.setdefault(entry.service, []).append(record.seq)
        self._by_host.setdefault(entry.host, []).append(record.seq)

    def _prune_indexes(self, evicted: StoredEntry) -> None:
        """Remove an evicted record's seq from the three secondary indexes.

        This is a **left-side trim, never a scan.** Two facts make that possible: the index lists
        are ascending (appends only add larger seqs), and eviction is always from the head of the
        ring (always the smallest resident seq). Therefore the evicted seq is necessarily the
        first live element of each list it appears in — so removing it is "advance the head by
        one", not "find it and delete it". A ``list.remove()`` or ``lst.pop(0)`` here would each
        be O(len) per eviction and would turn a full ring under sustained ingest into a quadratic
        hot path.

        Cost: the head advance is O(1). The physical ``del seqs[:garbage]`` is deferred until the
        garbage is both at least :data:`INDEX_COMPACT_MIN` entries and at least half the list, so
        each compaction of a length-L list is O(L) but pays for at least L/2 evictions — O(1)
        amortised. In exchange, a list may hold up to ``2 * resident + INDEX_COMPACT_MIN``
        entries. The stale head entries are harmless to a scan: it resolves each candidate seq
        through :meth:`_record_at`, which returns ``None`` for anything no longer resident.

        A key whose list becomes entirely garbage is deleted outright, so a store fed by a
        high-cardinality dimension (thousands of short-lived hostnames) does not accumulate an
        empty list per value it has ever seen.
        """
        entry = evicted.entry
        for dimension, key in (
            ("level", entry.level.value),
            ("service", entry.service),
            ("host", entry.host),
        ):
            index = self._indexes[dimension]
            heads = self._heads[dimension]
            seqs = index.get(key)
            if seqs is None:  # pragma: no cover - only reachable if an append skipped indexing
                continue
            garbage = heads.get(key, 0) + 1
            if garbage >= len(seqs) or (
                garbage >= INDEX_COMPACT_MIN and garbage * 2 >= len(seqs)
            ):
                del seqs[:garbage]
                if seqs:
                    heads[key] = 0
                else:
                    del index[key]
                    heads.pop(key, None)
            else:
                heads[key] = garbage

    # -- reads ----------------------------------------------------------------------------

    def get(self, entry_id: str) -> LogEntry | None:
        """Fetch one entry by id in O(1), or ``None`` if it is unknown or has been evicted."""
        record = self._by_id.get(entry_id)
        return None if record is None else record.entry

    def size(self) -> int:
        """Number of entries currently resident in the ring."""
        return len(self._entries)

    def __len__(self) -> int:
        """Same value as :meth:`size` — ``len(store)`` is what callers actually reach for.

        This is not sugar. ``src/api/health.py`` probes the store with ``len(store)`` behind a
        defensive ``except (TypeError, ValueError): return 0``, which is the correct shape for a
        liveness route that must never fail — and is exactly why a missing ``__len__`` is
        dangerous here. Without it the probe does not error, it quietly reports
        ``store_entries: 0`` forever, including after C5 seeds ten thousand entries. A defensive
        ``except`` around a container protocol turns "this type does not implement ``len``" from
        a loud ``TypeError`` into a plausible-looking number, so the protocol has to be
        implemented rather than assumed.

        ``size()`` stays as the explicit spelling used throughout the store's own code and tests.
        The two can never disagree: this returns ``size()``.

        .. warning::

           Defining ``__len__`` makes an **empty store falsy**. Every defensive read of the
           runtime must therefore test ``store is None``, never ``if not store`` — the latter
           now treats a perfectly healthy empty ring as a missing collaborator and silently
           takes the degraded path. ``src/api/health.py`` already uses the identity form; C5-C11
           must keep doing so.
        """
        return self.size()

    def capacity(self) -> int:
        """The ring's fixed capacity."""
        return self._capacity

    def total_appended(self) -> int:
        """Every append ever made, including entries since evicted. Monotone, never resets."""
        return self._next_seq

    def evicted(self) -> int:
        """Entries dropped from the ring to make room. ``total_appended() - size()``."""
        return self._next_seq - len(self._entries)

    def oldest_seq(self) -> int | None:
        """Smallest resident seq, or ``None`` when the store is empty.

        The residency test for a cursor anchor: ``anchor < oldest_seq()`` means the anchor has
        been evicted, which is exactly the condition :meth:`scan` reports as ``truncated``.
        """
        return self._entries[0].seq if self._entries else None

    def newest_seq(self) -> int | None:
        """Largest resident seq, or ``None`` when the store is empty."""
        return self._entries[-1].seq if self._entries else None

    def next_seq(self) -> int:
        """The seq the next append will receive. Also the count of appends ever made."""
        return self._next_seq

    def index_for(self, dimension: str) -> dict[str, list[int]]:
        """Return the secondary index for ``level``, ``service`` or ``host``.

        Exposed (rather than reached into) so :meth:`Filter.index_hint` — and C9's compiled
        filter after it — can pick a candidate set without knowing the store's attribute names.

        Raises:
            KeyError: On an unknown dimension. Loud, because a typo'd dimension that silently
                returned an empty index would look exactly like "nothing matches".
        """
        return self._indexes[dimension]

    def ingest_rate(self) -> float:
        """Appends per second over a bounded recent window. ``0.0`` before two appends exist.

        Computed as ``n / (now - oldest_sample)`` over the samples inside
        :data:`INGEST_WINDOW_SEC`, capped at :data:`INGEST_SAMPLE_CAP` samples so the estimator's
        own memory is bounded no matter how fast the ingest is. It is a store-level facet, not a
        query-level one: C11 reports it unfiltered, because "how fast are logs arriving" is a
        property of the store rather than of whatever the caller happened to search for.
        """
        now = self._time_func()
        cutoff = now - INGEST_WINDOW_SEC
        with self._lock:
            samples = self._append_times
            while samples and samples[0] < cutoff:
                samples.popleft()
            count = len(samples)
            oldest = samples[0] if samples else None
        if count < 2 or oldest is None:
            # One sample gives no interval to divide by, and inventing one (say, the full window)
            # would report a confident number derived from nothing.
            return 0.0
        span = now - oldest
        return count / span if span > 0 else 0.0

    # -- SSE fan-out ----------------------------------------------------------------------

    def subscribe(
        self,
        flt: Filter | CompiledFilter,
        *,
        subject: str,
        queue_size: int = DEFAULT_SUBSCRIBER_QUEUE_SIZE,
        max_streams: int | None = None,
    ) -> Subscription:
        """Register a live subscriber and return its handle.

        Args:
            flt: The filter the subscriber watches. The *same* vocabulary the paginated route
                uses, so "the tail of this search" and "the history of this search" are the same
                predicate applied to two ends of the ring rather than two implementations that
                have to be kept in agreement.
            subject: The owning principal, for the concurrent-stream cap.
            queue_size: Per-subscriber buffer depth. The route passes ``SSE_QUEUE_SIZE``.
            max_streams: Cap on this principal's concurrent subscriptions, or ``None`` for no
                cap. ``None`` is the unit-test default rather than the production one — the
                route always passes a number.

        Raises:
            StreamLimitExceeded: The principal is already at ``max_streams``. The check and the
                registration happen under one acquisition of ``_sub_lock``, so two connections
                arriving together cannot both read "one slot left" and both take it.
            ValueError: ``queue_size`` is not positive. A zero-maxsize ``asyncio.Queue`` is
                *unbounded*, which would silently turn the one guarantee this whole subsystem
                makes into its opposite — so it fails loudly at subscribe time instead.
        """
        if queue_size < 1:
            raise ValueError(
                f"queue_size must be >= 1 (0 means UNBOUNDED to asyncio.Queue), got {queue_size}"
            )

        sub = Subscription(subject=subject, flt=flt, queue_size=queue_size)
        with self._sub_lock:
            held = self._stream_counts.get(subject, 0)
            if max_streams is not None and held >= max_streams:
                raise StreamLimitExceeded(subject, max_streams)
            self._stream_counts[subject] = held + 1
            self._subscribers[sub] = None
        return sub

    def unsubscribe(self, sub: Subscription) -> bool:
        """Release a subscription. **Idempotent** — returns True only for the call that released.

        Idempotence is not a nicety here, it is the correctness property this method exists for.
        ``GET /logs/stream`` has six ways to end — the generator's ``finally``, the response's
        ``BackgroundTask``, the handler's ``except``, the slow-consumer drop below, lifespan
        shutdown, and an ordinary return once ``max_events`` is reached — and several of them
        routinely run for the *same* connection (a client that disconnects mid-frame trips both
        the ``finally`` and the background task). Each one calls this method. If the
        per-principal counter decremented per call rather than per subscription, a principal's
        slot count would drift downward until the cap stopped meaning anything, or upward until
        they could never connect again; the first is a security hole and the second is a support
        ticket.

        The guard is :attr:`Subscription.released`, read and written under ``_sub_lock`` so two
        exit paths racing on different threads still produce exactly one release.

        Returns:
            True if this call released the subscription, False if it was already released.
        """
        with self._sub_lock:
            if sub.released:
                return False
            sub.released = True
            self._subscribers.pop(sub, None)
            remaining = self._stream_counts.get(sub.subject, 1) - 1
            if remaining > 0:
                self._stream_counts[sub.subject] = remaining
            else:
                # Deleted rather than left at zero — see `_stream_counts` in `__init__`.
                self._stream_counts.pop(sub.subject, None)
        return True

    def subscriber_count(self) -> int:
        """How many subscriptions are live process-wide. C11's ``/debug/memory`` reports it."""
        with self._sub_lock:
            return len(self._subscribers)

    def stream_count(self, subject: str) -> int:
        """How many subscriptions ``subject`` currently holds. The cap is measured against this."""
        with self._sub_lock:
            return self._stream_counts.get(subject, 0)

    def close_all_subscribers(self) -> int:
        """Terminate every live subscription and return how many were closed. Shutdown path.

        Called from the lifespan teardown. Without it a shutdown leaves every generator parked
        on ``await queue.get()`` on a queue nothing will ever write to again, and the process
        waits on them instead of exiting.

        Unlike the drop path this does **not** drain first: whatever a client has already been
        sent is legitimately theirs, and the sentinel is appended behind it so the generator
        finishes delivering before it returns. Only if the queue is completely full does one
        record make way for the sentinel — a terminal frame that arrives is worth more than the
        last record of a stream that is ending anyway.
        """
        with self._sub_lock:
            current = list(self._subscribers)
        for sub in current:
            self._terminate(sub, drain_first=False)
        return len(current)

    def replay_since(
        self,
        after_seq: int,
        flt: Filter | CompiledFilter,
        *,
        max_items: int,
    ) -> tuple[list[StoredEntry], bool]:
        """Matching records with ``seq > after_seq``, oldest first — the ``Last-Event-ID`` resume.

        Args:
            after_seq: The last seq the client acknowledges having seen. ``-1`` means "nothing
                yet", which is why this is ``after_seq`` and not ``from_seq``: an SSE ``id`` is
                an entry the client *received*, so the resume starts strictly past it.
            flt: The reconnecting client's filter. Applied here as well as to the live tail, so
                a resume cannot deliver rows the same query would not have delivered live.
            max_items: Hard bound on the returned list, mirroring ``SSE_QUEUE_SIZE``. A client
                that was away for an hour must not be able to make the server materialise the
                whole ring into one response.

        Returns:
            ``(items, truncated)``. ``truncated`` is True when the resume is **provably
            incomplete** — either more than ``max_items`` records matched, or the requested
            anchor has already been evicted so the records immediately after it no longer exist.
            When it is True the newest ``max_items`` are returned rather than the oldest: the
            replay is about to be spliced onto the live tail, and keeping the newest end means
            the join is seamless and the gap is at the far, already-flagged end. Returning fewer
            rows *silently* is the one behaviour that must never happen — hence the flag rather
            than a shorter list.
        """
        if max_items < 1:
            return [], True

        oldest = self.oldest_seq()
        # `after_seq + 1` is the first seq the client is owed. If the ring's head has already
        # moved past it, whatever sat in between is gone — the client's gap is real and no
        # amount of scanning will find it.
        truncated = oldest is not None and after_seq + 1 < oldest

        kept: deque[StoredEntry] = deque(maxlen=max_items)
        matched = 0
        # ASC because a replay is delivered oldest-first: the client is rebuilding a timeline,
        # and `_walk` with an anchor already yields strictly beyond it, which is exactly the
        # "strictly past the last id" semantics above.
        for record in self._walk(flt, SortOrder.ASC, after_seq if after_seq >= 0 else None):
            matched += 1
            kept.append(record)
        if matched > max_items:
            truncated = True
        return list(kept), truncated

    def _publish(self, record: StoredEntry) -> None:
        """Fan one record out to every matching subscriber. **Never raises.** Caller holds ``_lock``.

        This method is the reason ``POST /logs`` can promise ``201`` unconditionally. It runs on
        the writer's thread, inside the store's critical section, and everything it touches is
        supplied by clients: the predicate came from a query string, the queue's fullness is set
        by how fast a reader reads. Any of it can misbehave, and none of it may turn a
        successful append into a failed request — so each subscriber is handled inside its own
        ``try`` and any failure removes *that* subscriber and nothing else.

        .. rubric:: The event-loop constraint, stated plainly

        ``asyncio.Queue`` is **not thread-safe**. ``put_nowait`` wakes a parked getter by
        resolving a future, and resolving a future from outside its loop's thread is undefined
        behaviour. This is safe here because the service is single-loop by construction: the
        only caller that publishes to a live subscriber is ``POST /logs``, an ``async def``
        handler running on uvicorn's loop, and the other append path (``append_many`` during
        seeding) runs before the loop exists and therefore before any subscriber can exist. The
        honest statement of the invariant is *appends that can reach a subscriber happen on the
        loop thread*. If this service ever grows a background ingest thread, this is the line
        that has to change — to ``loop.call_soon_threadsafe(queue.put_nowait, record)`` with the
        loop captured at subscribe time — and nothing else does.
        """
        subscribers = self._subscribers
        # Unlocked truthiness check. The common case by far — nobody is streaming — and taking a
        # lock per append to discover that is a cost every ingest pays for a feature nobody is
        # using. A subscriber that registers concurrently with this line simply starts one record
        # later, which is indistinguishable from having connected a microsecond later.
        if not subscribers:
            return

        with self._sub_lock:
            current = list(subscribers)

        failed: list[Subscription] = []
        for sub in current:
            if sub.released:
                continue
            try:
                if not sub.flt.matches(record):
                    continue
                sub.queue.put_nowait(record)
            except asyncio.QueueFull:
                # The whole back-pressure policy, in one branch: the consumer is `queue_size`
                # records behind and still not reading. Buffering more would let it grow the
                # process's memory without bound; blocking would let it stall ingest for every
                # other client. So it loses its subscription and is told so.
                failed.append(sub)
            except Exception:  # noqa: BLE001 - a broken subscriber must not break ingest
                # Reached when a filter raises. Not expected — `compile_filter` and
                # `Filter.from_query` both produce total predicates — but "not expected" is not
                # "impossible", and the failure mode if this were unhandled is that one bad
                # subscriber makes every subsequent `POST /logs` a 500.
                logger.exception(
                    "dropping subscriber for %r: filter raised during fan-out", sub.subject
                )
                failed.append(sub)

        for sub in failed:
            self._terminate(sub, drain_first=True)

    def _terminate(self, sub: Subscription, *, drain_first: bool) -> None:
        """End one subscription: (optionally drain), enqueue the terminal sentinel, unregister.

        ``drain_first`` distinguishes the two reasons a subscription ends without the client
        going away. A **drop** drains, because the queue being full is the entire problem and a
        sentinel that cannot be enqueued is a generator that never learns it was dropped — the
        exact "stalled reader parked forever" outcome the drop exists to prevent. A **shutdown**
        does not, because those records were legitimately delivered-in-flight.

        Ordering matters: the sentinel goes in *before* :meth:`unsubscribe`, so the generator can
        never observe "released, but nothing left in the queue" and be left waiting.
        """
        sub.dropped = sub.dropped or drain_first
        queue = sub.queue
        if drain_first:
            self._drain(queue)
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            # Shutdown against a completely full queue. Make room for the sentinel by discarding
            # the oldest queued record — a terminal frame that arrives beats the last record of
            # a stream that is ending regardless.
            self._drain(queue, limit=1)
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:  # pragma: no cover - the queue cannot still be full
                pass
        except Exception:  # noqa: BLE001 - termination is best-effort by construction
            logger.exception("failed to enqueue terminal sentinel for %r", sub.subject)
        self.unsubscribe(sub)

    @staticmethod
    def _drain(queue: asyncio.Queue[StoredEntry | None], *, limit: int | None = None) -> int:
        """Discard up to ``limit`` (default: all) queued items. Returns how many were discarded.

        ``get_nowait`` rather than rebuilding the queue, so the object identity a parked getter
        is waiting on is preserved.
        """
        discarded = 0
        while limit is None or discarded < limit:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            discarded += 1
        return discarded

    # -- scanning -------------------------------------------------------------------------

    def _record_at(self, seq: int) -> StoredEntry | None:
        """Resolve a seq to its record in O(1)-ish, or ``None`` if it is not resident.

        Relies on invariant 2 (the ring holds a contiguous seq range), so the position is
        arithmetic: ``entries[seq - oldest]``. ``deque`` indexing walks 64-element blocks from
        whichever end is nearer, so this is O(n/64) pointer hops in the worst case rather than a
        true O(1) — sub-microsecond at 100k entries, and index-hinted scans hit seqs near one end
        anyway. It is the difference between a page costing O(page) and O(corpus).
        """
        entries = self._entries
        if not entries:
            return None
        index = seq - entries[0].seq
        if index < 0 or index >= len(entries):
            return None
        return entries[index]

    def iter_matching(self, flt: Filter, order: SortOrder) -> Iterator[StoredEntry]:
        """Yield every matching record in ``order``. The single-pass primitive C11 aggregates.

        No anchor, no limit: this is the whole match set, lazily. ``/stats`` computes on demand
        over exactly this iterator, which is what makes ``StatsSnapshot.total`` and
        ``LogPage.page.total`` agree by construction instead of by two implementations happening
        to round the same way.
        """
        return self._walk(flt, order, None)

    def count(self, flt: Filter) -> int:
        """Number of resident records matching ``flt``.

        ``page.total`` on every list response. The empty filter short-circuits to :meth:`size`,
        which matters more than it looks: an unfiltered first page is the single most common
        request this API serves, and without the short-circuit every one of them would walk all
        100k records just to print a number the deque already knows.
        """
        if flt.is_empty:
            return len(self._entries)
        return sum(1 for _ in self._walk(flt, SortOrder.ASC, None))

    def scan(
        self,
        flt: Filter,
        order: SortOrder,
        *,
        limit: int,
        start_after_seq: int | None = None,
        skip: int = 0,
    ) -> ScanResult:
        """Return one page of matching records, plus everything needed to continue the walk.

        Args:
            flt: The predicate. Every returned record satisfies it.
            order: ``DESC`` (newest-first, the default everywhere) or ``ASC``.
            limit: Maximum records in the page. Already clamped by
                :func:`~src.models.clamp_limit` at the HTTP edge; clamped to ``>= 0`` again here
                so the store cannot be made to misbehave by a non-HTTP caller.
            start_after_seq: Cursor anchor. **Strictly exclusive**: ``DESC`` returns ``seq <
                anchor`` and ``ASC`` returns ``seq > anchor``, so the record the client last saw
                is never re-sent.
            skip: Offset pagination — discard this many *matching* records before collecting.
                Mutually exclusive with ``start_after_seq`` at the HTTP layer (C5 answers 400),
                but honoured together here so a non-HTTP caller gets sane behaviour rather than
                an assertion: the anchor positions the walk, then the skip advances within it.

        .. rubric:: Why a DESC walk can neither skip nor duplicate

        ``seq`` only ever increases (invariant 1), so every entry appended *during* a walk gets a
        seq above every anchor that walk will ever use. A ``DESC`` page returns ``seq < anchor``,
        which makes those new entries structurally invisible to the rest of the walk — no
        duplicates. And nothing can move an existing entry's seq, so no pre-existing entry can
        slide across an anchor it was already on the far side of — no skips. Offset pagination
        has neither property: ``skip=N`` counts from the *current* head, so N new appends shift
        every subsequent page by N and the client sees the same rows twice. That is inherent to
        offset paging, not a bug, and ``tests/unit/test_cursor.py`` asserts both halves of it.

        .. rubric:: Eviction

        If ``start_after_seq`` refers to a seq that has already been evicted (``anchor <
        oldest_seq()``), this neither raises nor silently returns a short page. It sets
        ``truncated=True`` — C5 turns that into ``X-Cursor-Truncated: true`` — and resumes as
        close to the anchor as the ring still allows:

        * ``ASC`` resumes **at the oldest resident record** (the Kafka ``auto.offset.reset=
          earliest`` rule): the entries between the anchor and the ring's head are gone, the
          client is told so, and the walk continues with real rows.
        * ``DESC`` has nowhere to resume *to*. The walk travels downward and everything below the
          anchor is precisely what was evicted, so the honest page is an empty terminal one with
          ``truncated=True``. Snapping the anchor **up** to the oldest resident record — the
          other reading of "resume from the oldest resident" — would emit a record *above* the
          anchor, i.e. one the client has very likely already seen: it would trade a flagged
          empty page for a silent duplicate, and duplicates are the exact failure this whole
          design exists to prevent.
        """
        limit = max(0, int(limit))
        skip = max(0, int(skip))

        anchor = start_after_seq
        truncated = False
        oldest = self.oldest_seq()
        if anchor is not None and oldest is not None and anchor < oldest:
            truncated = True
            if order is SortOrder.ASC:
                # Resume AT the oldest resident record: exclusive anchor one below it.
                anchor = oldest - 1
            # DESC: leave the anchor where it is. Everything below it has been evicted, so the
            # walk below yields nothing — which, flagged, is the truthful answer.

        walker = self._walk(flt, order, anchor)

        # Offset semantics: skip N *matching* records, not N records. Anything else would make
        # `?offset=200` mean something different depending on how selective the filter was.
        # The zero-maxlen deque is itertools' own `consume` recipe: it drains exactly `skip`
        # items at C speed and retains none of them.
        if skip:
            deque(islice(walker, skip), maxlen=0)

        # has_more is decided by pulling one record PAST the page, never by `len(items) ==
        # limit`. The latter is wrong on an exact-boundary page: 20 matches read with limit=20
        # would advertise a 21st page that does not exist, and a client that trusts has_more
        # would issue a request guaranteed to come back empty.
        window = list(islice(walker, limit + 1))
        has_more = len(window) > limit
        items = window[:limit]

        return ScanResult(
            items=items,
            next_seq=items[-1].seq if (has_more and items) else None,
            has_more=has_more,
            truncated=truncated,
        )

    def _walk(
        self, flt: Filter, order: SortOrder, start_after_seq: int | None
    ) -> Iterator[StoredEntry]:
        """Yield matching records in ``order``, strictly beyond ``start_after_seq``.

        Dispatches between the two strategies: an index-hinted merge when the filter constrains
        an indexed dimension *selectively enough to pay for itself*, and a single linear pass
        otherwise. Both are lazy, so a page costs only what it consumes — the store never
        materialises or sorts a copy of the corpus.

        The selectivity test is what keeps "indexes are hints, never authorities" true in the
        performance dimension as well as the correctness one. Following a hint costs a random
        access into the ring per candidate; a linear pass reads it sequentially. Past roughly
        :data:`INDEX_HINT_MIN_SELECTIVITY` the hint stops being a shortcut and starts being a
        slower way to visit most of the corpus, so the planner declines it. Both paths return
        exactly the same records in exactly the same order — ``tests/unit/test_store.py`` asserts
        that directly, because an optimisation that can change an answer is not an optimisation.
        """
        entries = self._entries
        if not entries:
            return iter(())
        oldest = entries[0].seq
        newest = entries[-1].seq
        hint = flt.index_hint(self)
        if hint is None:
            return self._walk_linear(flt, order, start_after_seq, oldest, newest)
        candidates = sum(len(seqs) for seqs in hint)
        if candidates * INDEX_HINT_MIN_SELECTIVITY > len(entries):
            return self._walk_linear(flt, order, start_after_seq, oldest, newest)
        return self._walk_hinted(hint, flt, order, start_after_seq, oldest, newest)

    def _walk_linear(
        self,
        flt: Filter,
        order: SortOrder,
        start_after_seq: int | None,
        oldest: int,
        newest: int,
    ) -> Iterator[StoredEntry]:
        """One pass over the ring in ``order``, starting at the anchor. O(records visited).

        The anchor is turned into a *positional* skip rather than a per-record comparison,
        because the ring is contiguous in seq: the record with seq S sits at offset ``S -
        oldest``. ``islice`` then advances the deque's own C-level iterator, so reaching the
        starting point costs a pointer walk rather than a Python loop.
        """
        entries = self._entries
        if order is SortOrder.DESC:
            first = newest if start_after_seq is None else min(start_after_seq - 1, newest)
            if first < oldest:
                return
            source: Iterator[StoredEntry] = islice(reversed(entries), newest - first, None)
        else:
            first = oldest if start_after_seq is None else max(start_after_seq + 1, oldest)
            if first > newest:
                return
            source = islice(iter(entries), first - oldest, None)

        matches = flt.matches
        for record in source:
            if matches(record):
                yield record

    def _walk_hinted(
        self,
        hint: list[list[int]],
        flt: Filter,
        order: SortOrder,
        start_after_seq: int | None,
        oldest: int,
        newest: int,
    ) -> Iterator[StoredEntry]:
        """Walk only the seqs a secondary index says are worth looking at.

        Each candidate list is already ascending, so the anchor becomes a ``bisect`` — the walk
        starts at the right position instead of filtering its way there. Lists within one
        dimension are disjoint (a record has exactly one level, one service, one host), so
        merging them can never produce a duplicate seq and no de-duplication pass is needed.

        The index is a hint: :meth:`Filter.matches` runs on every candidate, and any seq that no
        longer resolves to a resident record — stale head garbage awaiting compaction — is
        skipped. Cost is O(candidates visited), which for the common ``?level=ERROR&limit=50``
        request is a few hundred list reads instead of a 100k-record sweep.
        """
        streams: list[Iterator[int]] = []
        for seqs in hint:
            if not seqs:
                continue
            if order is SortOrder.ASC:
                start = 0 if start_after_seq is None else bisect_right(seqs, start_after_seq)
                if start < len(seqs):
                    streams.append(islice(seqs, start, None))
            else:
                stop = len(seqs) if start_after_seq is None else bisect_left(seqs, start_after_seq)
                if stop > 0:
                    streams.append(_descending(seqs, stop))

        if not streams:
            return
        if len(streams) == 1:
            merged: Iterator[int] = streams[0]
        else:
            merged = heapq.merge(*streams, reverse=order is SortOrder.DESC)

        matches = flt.matches
        record_at = self._record_at
        descending = order is SortOrder.DESC
        for seq in merged:
            if seq < oldest:
                # Merged output is monotone, so in DESC everything after this is also evicted.
                if descending:
                    return
                continue
            if seq > newest:
                if not descending:
                    return
                continue
            record = record_at(seq)
            if record is None:  # pragma: no cover - concurrent eviction mid-walk
                continue
            if matches(record):
                yield record


def _descending(seqs: list[int], stop: int) -> Iterator[int]:
    """Yield ``seqs[stop-1] … seqs[0]``.

    ``reversed(seqs[:stop])`` would be shorter but copies up to ``stop`` integers on every
    request; at 100k candidates that allocation dwarfs the page it is serving. Index walking
    keeps a hinted scan proportional to the records actually consumed.
    """
    for i in range(stop - 1, -1, -1):
        yield seqs[i]

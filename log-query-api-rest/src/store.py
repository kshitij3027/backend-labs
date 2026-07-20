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

import base64
import binascii
import hashlib
import heapq
import json
import re
import threading
import time
from bisect import bisect_left, bisect_right
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import islice

from src.models import LogEntry, LogQuery, SortOrder

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

    .. rubric:: Extension point for C9

    Three members are all that :meth:`LogStore.scan`, :meth:`LogStore.count` and
    :meth:`LogStore.iter_matching` ever call on a filter: :meth:`matches`, :meth:`index_hint`
    and :attr:`is_empty` (plus :meth:`fingerprint`, which only the cursor codec needs). C9's
    ``compile_filter(node) -> CompiledFilter`` therefore has to produce *any* object exposing
    those four members — a subclass here, or a separate class entirely — and the store's three
    signatures do not change. Do not add filtering logic to the store: it belongs behind
    ``matches``, or the "one evaluator, three entry points" guarantee stops holding.

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

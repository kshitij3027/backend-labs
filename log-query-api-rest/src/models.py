"""The API vocabulary for the Log Query API (REST) — one definition per wire shape.

This module is the **single source of truth** for everything that crosses the HTTP boundary:
the log entry itself, the write body, the pagination envelope, the shared query-filter bundle,
the error envelope, and the two auth response shapes. Every route in ``src/api/v1.py`` declares
one of these as its ``response_model`` or body type, which means the shapes here are literally
what the generated OpenAPI 3.1 document advertises at ``/docs`` and ``/redoc``. Change a field
here and the published contract changes with it — that is the point of concentrating them in one
file rather than letting each handler invent its own dict.

Three contracts in here are load-bearing enough to be worth stating up front:

* **``LogEntry.ts`` serialises as RFC-3339 with a ``Z`` suffix and millisecond precision**
  (``2026-07-27T10:31:04.512Z``). That exact string is in the README's pagination example, and
  the README is the specification. See :func:`_rfc3339_z`.
* **List responses are always an envelope** (:class:`LogPage`), never a bare array. A bare
  top-level JSON array is a compatibility dead end: there is nowhere to add a sibling field
  later without breaking every existing client.
* **``limit`` is clamped, never rejected.** :func:`clamp_limit` returns the effective value and
  a boolean; a client asking for 10,000 rows gets the ceiling and the
  ``X-Page-Limit-Clamped`` header, not a ``422``.

``LogEntry`` deliberately carries **no ``seq`` field**. C4's ``LogStore`` assigns a
process-monotonic ``seq`` (the sort key, the cursor anchor and the SSE event id) and keeps it on
its own internal record alongside a precomputed epoch and lower-cased message. Hoisting ``seq``
into ``LogEntry`` would leak a storage implementation detail into every response body and into
the published schema, where it could never be removed.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from src.auth import Principal, Role, Tier
from src.config import Settings

# ---------------------------------------------------------------------------------------------
# Response header names
#
# These live here, next to the models whose behaviour they describe, so each name has exactly
# ONE definition in the codebase. Both strings are also listed in ``src.main.EXPOSE_HEADERS``
# (the CORS ``expose_headers`` allowlist) — they MUST stay identical, because a header the
# browser is not told to expose is a header browser JavaScript simply cannot read, no matter
# that the server sent it. ``tests/unit/test_models.py`` pins the two lists together so a rename
# in one place cannot silently desync the other.
# ---------------------------------------------------------------------------------------------

#: Set by C5's ``GET /logs`` when the client's ``limit`` was clamped; the value is the *requested*
#: limit, while ``page.limit`` reports the effective one.
CLAMPED_HEADER = "X-Page-Limit-Clamped"

#: Set by C5 when a cursor's anchor had already been evicted from the ring and the walk resumed
#: from the oldest resident entry. Returning fewer rows *silently* is the one thing that must
#: never happen.
CURSOR_TRUNCATED_HEADER = "X-Cursor-Truncated"

#: Upper bound on the free-text ``q`` filter. Long enough for any realistic message fragment,
#: short enough that a hostile client cannot make the substring scan the expensive part of a
#: 100k-entry sweep.
MAX_Q_LEN = 256

#: Bounds on a write's ``attrs`` bag. The store is a fixed-capacity ring, so the entry *count* is
#: bounded but the per-entry *size* is not — without these three caps a writer could grow the
#: process indefinitely by attaching megabytes of attributes to every append.
MAX_ATTRS_KEYS = 32
MAX_ATTR_KEY_LEN = 64
MAX_ATTR_VALUE_LEN = 512

#: Field-length caps on a write. 8 KiB of message is far beyond any real log line and well under
#: anything that would make a single append interesting to an attacker.
MAX_SERVICE_LEN = 128
MAX_HOST_LEN = 128
MAX_MESSAGE_LEN = 8192

#: Bounds on a ``POST /logs/search`` filter tree (C9). This is the one place in the API where a
#: client controls the *structure* of a request rather than only its values, so all three caps are
#: about making a hostile body cheap to **reject** instead of expensive to evaluate.
#:
#: All three are needed, because each bounds a different dimension:
#:
#: * :data:`MAX_FILTER_DEPTH` bounds nesting, so parsing and compiling cannot be driven into a
#:   ``RecursionError`` (which would surface as a ``500`` — an availability bug handed to any
#:   caller who can post a body).
#: * :data:`MAX_FILTER_NODES` bounds *width*, which depth alone does not: a single
#:   ``{"all": [ …fifty thousand leaves… ]}`` is two levels deep and still a denial of service.
#: * :data:`MAX_FILTER_VALUES` bounds the one leaf shape that carries a collection (``in``/``nin``).
#:
#: String values reuse :data:`MAX_Q_LEN` rather than inventing a fourth number — a leaf's
#: ``contains`` needle is exactly the same kind of thing as the ``q`` query parameter, and it is
#: scanned by exactly the same substring search.
MAX_FILTER_DEPTH = 8
MAX_FILTER_NODES = 100
MAX_FILTER_VALUES = 64


class LogLevel(StrEnum):
    """The five severity levels, spelled exactly as they appear on the wire.

    ``StrEnum`` rather than a bare ``Enum`` so a member *is* its wire string:
    ``LogLevel.ERROR == "ERROR"`` is true, serialisation emits ``"ERROR"`` with no conversion
    step, and a raw string lifted straight out of a query parameter can index
    :data:`LEVEL_ORDER` without being coerced first.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


#: Severity ordinals, 0 (least severe) .. 4 (most severe). Enum members compare by identity, not
#: severity, so ``LogLevel.ERROR > LogLevel.INFO`` is meaningless — this map is what makes C9's
#: ``gt``/``gte``/``lt``/``lte`` operators on the ``level`` field expressible at all.
#: ``MappingProxyType`` because it is a constant: a shared mutable dict that any caller could
#: edit would be a very quiet way to corrupt every comparison in the process.
LEVEL_ORDER: Mapping[LogLevel, int] = MappingProxyType(
    {
        LogLevel.DEBUG: 0,
        LogLevel.INFO: 1,
        LogLevel.WARN: 2,
        LogLevel.ERROR: 3,
        LogLevel.FATAL: 4,
    }
)

#: What "an error" means for C11's ``top_errors`` aggregation. Defined once, here, rather than
#: re-spelled as a literal inside ``src/stats.py`` — two definitions of "error" that drift apart
#: would make the stats panel disagree with the search results it is supposed to summarise.
ERROR_LEVELS: frozenset[LogLevel] = frozenset({LogLevel.ERROR, LogLevel.FATAL})


class SortOrder(StrEnum):
    """Scan direction. ``DESC`` (newest-first) is the default on every route that takes one.

    Newest-first is not just a preference: it is what makes cursor pagination safe. Walking
    down from an anchor means concurrent appends land *above* the walk and can neither be
    skipped nor duplicated by it.
    """

    ASC = "asc"
    DESC = "desc"


def _to_utc(value: datetime) -> datetime:
    """Normalise any datetime to an aware UTC datetime.

    A **naive** datetime is interpreted as UTC rather than local time. Treating it as local
    would make the corpus depend on the container's ``TZ``, and a store holding a mix of
    timezones cannot be range-scanned correctly by anything. An aware datetime in another zone
    is converted, so the invariant "every ``ts`` in the store is UTC" holds by construction
    instead of by convention.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _rfc3339_z(value: datetime) -> str:
    """Format a datetime as RFC-3339 UTC with a ``Z`` suffix and millisecond precision.

    Python's own ``datetime.isoformat()`` produces ``2026-07-27T10:31:04.512987+00:00``: six
    fractional digits and a numeric offset. The README's pagination example — which is the
    contract this project is implementing — is ``2026-07-27T10:31:04.512Z``. The difference is
    not cosmetic:

    * ``Z`` is what every JavaScript ``new Date(...)`` parser, ``jq``, and log-ingestion tool
      expects to see on a UTC timestamp; ``+00:00`` is legal RFC-3339 but is the less
      interoperable spelling.
    * Milliseconds match what log tooling actually stores. Microsecond precision here would
      round-trip badly through clients that truncate, and would make two entries that a client
      considers simultaneous compare unequal.

    Fractional digits are truncated, not rounded, which is what ``timespec="milliseconds"``
    does — a timestamp must never be reported as later than it was.
    """
    return _to_utc(value).isoformat(timespec="milliseconds").removesuffix("+00:00") + "Z"


class LogEntry(BaseModel):
    """One structured log entry — the public wire model, identical on every delivery path.

    The same shape comes back from ``GET /logs``, ``GET /logs/{id}``, ``POST /logs/search`` and
    each SSE ``data:`` frame: one schema, four delivery modes. A client writes one parser.

    ``frozen=True`` because an entry is immutable once appended. The store hands the *same*
    object to a paginated scan, to every SSE subscriber, and to the stats pass; if any of them
    could mutate it, a concurrent scan could observe a half-updated entry. Freezing removes the
    question entirely rather than relying on everyone being careful.

    ``extra="forbid"`` because an unrecognised field is a client error worth reporting, not
    something to swallow silently — and because it pins the response schema: a typo'd field
    name in a future handler fails a test instead of quietly shipping.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(
        description="Stable unique id for this entry; the key for GET /logs/{id}.",
        examples=["3f6c1c1e0b7a4a19a4b0b0f0d2b6d1a7"],
    )
    ts: datetime = Field(
        description=(
            "Event timestamp, always UTC. Serialised as RFC-3339 with a 'Z' suffix and "
            "millisecond precision (e.g. 2026-07-27T10:31:04.512Z). Naive input is "
            "interpreted as UTC; input in another zone is converted."
        ),
        examples=["2026-07-27T10:31:04.512Z"],
    )
    level: LogLevel = Field(description="Severity: DEBUG | INFO | WARN | ERROR | FATAL.")
    service: str = Field(
        description="Emitting service name.", examples=["auth-svc"]
    )
    host: str = Field(description="Emitting host/node name.", examples=["node-3"])
    message: str = Field(
        description="Free-text log message; the target of the `q` substring filter.",
        examples=["invalid token"],
    )
    attrs: dict[str, str] = Field(
        default_factory=dict,
        description="Optional flat string->string bag of structured attributes.",
        examples=[{"request_id": "b1c2d3", "status": "401"}],
    )

    @field_validator("ts", mode="after")
    @classmethod
    def _normalise_ts(cls, value: datetime) -> datetime:
        """Force every timestamp to aware UTC before the entry is ever stored.

        ``mode="after"`` runs once pydantic has already turned a string/epoch/datetime input
        into a ``datetime``, so this single hook covers every input form. Doing it here rather
        than at each call site is what guarantees the store never holds a mixed-timezone corpus
        — and a mixed-timezone corpus makes ``since``/``until`` range scans quietly wrong
        rather than loudly broken.
        """
        return _to_utc(value)

    @field_serializer("ts")
    def _serialise_ts(self, value: datetime) -> str:
        """Emit ``ts`` in the README's exact wire form. See :func:`_rfc3339_z` for why.

        Applied in **both** dump modes, not only ``mode="json"``: the app's default response
        class is ``ORJSONResponse``, and orjson serialises a raw ``datetime`` with a ``+00:00``
        offset and microsecond precision. Anything that reaches the encoder as a plain
        ``datetime`` would therefore silently violate the contract, so the string is produced
        here — the one place that owns the format.
        """
        return _rfc3339_z(value)


def _validate_attrs(value: dict[str, str]) -> dict[str, str]:
    """Enforce the :data:`MAX_ATTRS_KEYS` / key-length / value-length caps on an attrs bag."""
    if len(value) > MAX_ATTRS_KEYS:
        raise ValueError(f"attrs may carry at most {MAX_ATTRS_KEYS} keys, got {len(value)}")
    for key, item in value.items():
        if len(key) > MAX_ATTR_KEY_LEN:
            raise ValueError(f"attrs key {key[:32]!r} exceeds {MAX_ATTR_KEY_LEN} characters")
        if len(item) > MAX_ATTR_VALUE_LEN:
            raise ValueError(
                f"attrs value for {key!r} exceeds {MAX_ATTR_VALUE_LEN} characters"
            )
    return value


class LogCreate(BaseModel):
    """Body of ``POST /api/v1/logs`` (writer role, C7) — an entry to append.

    ``ts`` and ``id`` are optional on the wire and defaulted **server-side**: a client that
    omits them gets now-UTC and a fresh uuid4 hex. Both are still accepted when supplied,
    because a shipper replaying its own buffer must be able to preserve the original event time
    and its own idempotency key.

    ``extra="forbid"`` so a misspelled field (``severity`` instead of ``level``) is a ``422``
    the caller can act on, rather than an entry silently appended at the default level.
    """

    model_config = ConfigDict(extra="forbid")

    level: LogLevel = Field(description="Severity of the entry being appended.")
    service: str = Field(
        min_length=1,
        max_length=MAX_SERVICE_LEN,
        description="Emitting service name.",
        examples=["auth-svc"],
    )
    host: str = Field(
        min_length=1,
        max_length=MAX_HOST_LEN,
        description="Emitting host/node name.",
        examples=["node-3"],
    )
    message: str = Field(
        min_length=1,
        max_length=MAX_MESSAGE_LEN,
        description="Free-text log message. Must not be empty.",
        examples=["invalid token"],
    )
    ts: datetime | None = Field(
        default=None,
        description="Event time; defaults to the server's current UTC time when omitted.",
        examples=["2026-07-27T10:31:04.512Z"],
    )
    id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Client-supplied id; the server mints a uuid4 hex when omitted.",
    )
    attrs: dict[str, str] = Field(
        default_factory=dict,
        description=(
            f"Flat string->string attributes. At most {MAX_ATTRS_KEYS} keys, keys "
            f"<= {MAX_ATTR_KEY_LEN} chars, values <= {MAX_ATTR_VALUE_LEN} chars."
        ),
    )

    @field_validator("attrs")
    @classmethod
    def _check_attrs(cls, value: dict[str, str]) -> dict[str, str]:
        """Bound the attrs bag — see :data:`MAX_ATTRS_KEYS` for why an unbounded one is unsafe."""
        return _validate_attrs(value)

    def to_entry(
        self, *, now: datetime | None = None, new_id: str | None = None
    ) -> LogEntry:
        """Materialise the :class:`LogEntry` this write becomes.

        The defaulting rule ("client value wins, server value fills the gap") lives here and
        only here, so C7's route is a one-liner and C12's E2E verifier cannot be testing a
        different rule than the one production uses.

        Args:
            now: Timestamp used when the body omits ``ts``. Defaults to the current UTC time;
                injectable so tests and the seeded generator can drive a deterministic clock
                instead of ``datetime.now()``.
            new_id: Id used when the body omits ``id``. Defaults to a fresh uuid4 hex.
        """
        return LogEntry(
            id=self.id or (new_id if new_id is not None else uuid4().hex),
            ts=self.ts or (now if now is not None else datetime.now(UTC)),
            level=self.level,
            service=self.service,
            host=self.host,
            message=self.message,
            attrs=dict(self.attrs),
        )


class PageInfo(BaseModel):
    """Pagination metadata — the ``page`` half of every list response.

    Field order is part of the contract: the README's example prints ``limit``, ``returned``,
    ``next_cursor``, ``has_more``, ``total`` in that order, and pydantic serialises in
    declaration order, so the order here IS the order on the wire.

    ``limit`` is the **effective** limit (post-clamp), not what the client asked for — the
    requested value comes back in the ``X-Page-Limit-Clamped`` header when the two differ.
    ``total`` is the size of the *filtered* match set, not of the whole store, which is what
    makes ``GET /stats`` and ``GET /logs`` agree for the same filter.
    """

    limit: int = Field(description="Effective page size after clamping to MAX_PAGE_SIZE.")
    returned: int = Field(description="Number of items actually in this page.")
    next_cursor: str | None = Field(
        default=None,
        description="Opaque cursor for the next page; null when the walk is exhausted.",
        examples=["b64:eyJzIjo0MjAsIm8iOiJkZXNjIn0"],
    )
    has_more: bool = Field(description="True when a further page exists.")
    total: int = Field(
        description="Total entries matching the filter (as of walk start once a cursor exists)."
    )


class LogPage(BaseModel):
    """The paginated envelope returned by ``GET /logs`` and ``POST /logs/search``.

    Every list response in this API is this envelope and never a bare JSON array. A top-level
    array is a compatibility dead end: there is no way to add a sibling field — pagination
    metadata, a warning, a deprecation notice — without changing the response's *type* and
    breaking every client that already parses it. Paying for one extra level of nesting on day
    one buys the ability to evolve the response forever.
    """

    items: list[LogEntry] = Field(description="The matching entries, in the requested order.")
    page: PageInfo = Field(description="Pagination metadata for this page.")


class LogQuery(BaseModel):
    """The shared filter bundle: one definition, three entry points.

    ``GET /logs``, ``GET /logs/stream`` and ``GET /stats`` all take exactly this vocabulary, so
    "results for this search", "the live tail of this search" and "stats for this search" are
    guaranteed to describe the same set **by construction** rather than by three handlers
    happening to agree. C9's ``POST /logs/search`` swaps the flat bundle for a nested boolean
    tree, but it compiles down to the same predicate.

    Scalar filters are ANDed and list filters are ORed within themselves (``level=ERROR&
    level=FATAL`` means "either"), which is the only thing a flat query string can honestly
    express.

    ``extra`` is left at pydantic's permissive default on purpose: this bundle is bound to a
    query string that legitimately carries unrelated parameters (``?access_token=`` on the SSE
    route, a browser's cache-busting ``&_=…``), and a ``422`` for an unrecognised query
    parameter would be hostile.
    """

    level: list[LogLevel] | None = Field(
        default=None, description="Match any of these levels (repeatable)."
    )
    service: list[str] | None = Field(
        default=None, description="Match any of these service names (repeatable)."
    )
    host: list[str] | None = Field(
        default=None, description="Match any of these host names (repeatable)."
    )
    since: datetime | None = Field(
        default=None, description="Inclusive lower bound on ts (normalised to UTC)."
    )
    until: datetime | None = Field(
        default=None, description="Inclusive upper bound on ts (normalised to UTC)."
    )
    q: str | None = Field(
        default=None,
        max_length=MAX_Q_LEN,
        description="Case-insensitive substring match over `message`.",
    )
    # No `ge`/`le` on `limit`, deliberately. Constraining it here would turn an over-large page
    # request into a 422, which is exactly what the README says must NOT happen — clamping is
    # `clamp_limit`'s job, and it never raises.
    limit: int | None = Field(
        default=None,
        description=(
            "Requested page size. Clamped into [1, MAX_PAGE_SIZE] rather than rejected; the "
            "response carries X-Page-Limit-Clamped when the requested value was adjusted."
        ),
    )
    cursor: str | None = Field(
        default=None,
        description="Opaque cursor from a previous page's `next_cursor`. Excludes `offset`.",
    )
    offset: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Row offset for 'jump to page N' use. Excludes `cursor`, and drifts under "
            "concurrent appends — that is inherent to offset paging, not a bug."
        ),
    )
    order: SortOrder = Field(
        default=SortOrder.DESC, description="Scan direction; newest-first by default."
    )

    @field_validator("since", "until", mode="after")
    @classmethod
    def _normalise_bounds(cls, value: datetime | None) -> datetime | None:
        """Normalise range bounds the same way :class:`LogEntry` normalises ``ts``.

        Both sides of a comparison must live in the same timezone or the comparison is
        nonsense — and in Python, comparing a naive datetime with an aware one raises
        ``TypeError`` deep inside the scan rather than returning a wrong answer at the edge.
        """
        return None if value is None else _to_utc(value)

    @model_validator(mode="after")
    def _check_coherent(self) -> LogQuery:
        """Reject the two combinations that can only ever produce a misleading answer.

        ``cursor`` + ``offset`` together have no meaningful interpretation — a cursor already
        encodes a position — so guessing which one the caller meant would silently return the
        wrong page. C5 turns this into a ``400``.

        ``since > until`` is an empty range. Returning zero rows for it looks identical to "no
        matching logs", which is precisely the wrong thing to tell someone debugging an
        incident, so it is an error instead.
        """
        if self.cursor is not None and self.offset is not None:
            raise ValueError(
                "cursor and offset are mutually exclusive — supply one or the other"
            )
        if self.since is not None and self.until is not None and self.since > self.until:
            raise ValueError("since must not be after until")
        return self


# =============================================================================================
#  The structured filter tree — the body vocabulary of ``POST /api/v1/logs/search`` (C9)
# ---------------------------------------------------------------------------------------------
#  :class:`LogQuery` above is everything a flat query string can honestly say: fields ANDed,
#  values ORed within a field. There is no way to spell *(level is ERROR or FATAL) and not
#  (service is search-svc)* in a query string without inventing a mini-language inside a
#  parameter value — which is how every "just add a `filter=` param" API ends up with a hand-
#  rolled parser and no schema. So the expressive form moves into a JSON body, where the
#  structure is the structure and pydantic validates it for free.
#
#  Four node shapes, distinguished by their keys, and every one of them ``extra="forbid"``:
#
#      {"all": [ … ]}                          conjunction
#      {"any": [ … ]}                          disjunction
#      {"not": { … }}                          negation
#      {"field": …, "op": …, "value": …}       leaf predicate
#
#  ``extra="forbid"`` is load-bearing rather than merely tidy: it is what makes the union
#  unambiguous. Each shape's required key is absent from the other three, so for any given object
#  exactly one member of the union can possibly validate — no discriminator callable needed, and
#  the generated OpenAPI stays a plain recursive ``anyOf`` of four ``$ref``s that any code
#  generator can consume.
# =============================================================================================


class FilterField(StrEnum):
    """The five entry attributes a leaf predicate may address. Nothing else is addressable.

    Deliberately **not** open to ``attrs.*``. The attrs bag is an arbitrary string->string map,
    so an addressable path into it would be an unbounded key space with no index behind it and no
    schema to publish; every query over it would be a full linear scan with a per-record dict
    lookup. Keeping the vocabulary closed is also what lets an unknown ``field`` be a ``422`` with
    the valid set named in the message, rather than a filter that silently matches nothing —
    which, to someone debugging an incident, is indistinguishable from "there are no such logs".
    """

    LEVEL = "level"
    SERVICE = "service"
    HOST = "host"
    MESSAGE = "message"
    TS = "ts"


class FilterOp(StrEnum):
    """The nine comparison operators. Which ones are legal depends on the field — see
    :data:`FIELD_OPS`."""

    EQ = "eq"
    NE = "ne"
    IN = "in"
    NIN = "nin"
    CONTAINS = "contains"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"


#: The operators whose ``value`` is a **list**. Everything else takes a scalar, and the mismatch
#: is rejected at validation rather than at evaluation — see :func:`coerce_filter_value`.
LIST_OPS: frozenset[FilterOp] = frozenset({FilterOp.IN, FilterOp.NIN})

#: The operators that compare by **order** rather than by identity. Only meaningful on a field
#: that has an order: ``ts`` (an instant) and ``level`` (via :data:`LEVEL_ORDER`).
ORDER_OPS: frozenset[FilterOp] = frozenset(
    {FilterOp.GT, FilterOp.GTE, FilterOp.LT, FilterOp.LTE}
)

#: **The field x operator matrix.** Not every operator is meaningful on every field, and an
#: operator that is merely *tolerated* on a field it does not suit is worse than one that is
#: refused: it returns rows, so nobody notices it answered the wrong question.
#:
#: The three rules that generate this table:
#:
#: * **Ordering (`gt`/`gte`/`lt`/`lte`) needs an ordered field.** ``ts`` is an instant, and
#:   ``level`` has :data:`LEVEL_ORDER` — which is the entire reason that ordinal map exists, so
#:   ``{"field": "level", "op": "gte", "value": "WARN"}`` means *at least as severe as WARN*
#:   rather than a lexicographic accident (alphabetically, "WARN" > "ERROR" > "DEBUG", which is
#:   almost exactly backwards). Service/host/message have no order worth exposing: sorting host
#:   names lexicographically answers no question anybody asks.
#: * **`contains` needs free text.** ``service``/``host``/``message`` are text a human wrote, so a
#:   substring search over them is meaningful. ``level`` is a closed five-member enum where
#:   ``contains "ERROR"`` is just a slower, subtly wrong ``eq`` — and ``contains "R"`` would match
#:   ERROR and WARN, which nobody means. ``ts`` is an instant, not a string.
#: * **`eq`/`ne`/`in`/`nin` are identity**, and work anywhere a value can be compared for
#:   equality — except ``in``/``nin`` on ``ts``, where an explicit *set of exact instants* is a
#:   query nobody writes and an invitation to sub-millisecond confusion. Range operators are what
#:   time is for.
FIELD_OPS: Mapping[FilterField, frozenset[FilterOp]] = MappingProxyType(
    {
        FilterField.LEVEL: frozenset(
            {FilterOp.EQ, FilterOp.NE, FilterOp.IN, FilterOp.NIN, *ORDER_OPS}
        ),
        FilterField.SERVICE: frozenset(
            {FilterOp.EQ, FilterOp.NE, FilterOp.IN, FilterOp.NIN, FilterOp.CONTAINS}
        ),
        FilterField.HOST: frozenset(
            {FilterOp.EQ, FilterOp.NE, FilterOp.IN, FilterOp.NIN, FilterOp.CONTAINS}
        ),
        FilterField.MESSAGE: frozenset(
            {FilterOp.EQ, FilterOp.NE, FilterOp.IN, FilterOp.NIN, FilterOp.CONTAINS}
        ),
        FilterField.TS: frozenset({FilterOp.EQ, FilterOp.NE, *ORDER_OPS}),
    }
)

#: What a leaf's ``value`` may be on the wire, before the per-field rules run. Broad on purpose:
#: accepting ``{"field": "level", "op": "eq", "value": 3}`` as far as the *type* system and then
#: refusing it with "3 is not a log level; expected one of [...]" is a far more useful ``422``
#: than pydantic's generic "input should be a valid string".
FilterScalar = str | bool | int | float

#: Parses a leaf's ``ts`` value using **pydantic's own** datetime rules — the same ones behind
#: ``LogQuery.since``/``until``. Sharing the parser is what guarantees that ``?since=…`` and
#: ``{"field": "ts", "op": "gte", …}`` accept exactly the same spellings (RFC-3339 with any
#: offset, ``Z``, or a POSIX epoch number); two hand-rolled parsers would eventually disagree
#: about one of them, and the disagreement would look like a filtering bug.
_TS_ADAPTER: TypeAdapter[datetime] = TypeAdapter(datetime)


def _as_level(value: Any) -> LogLevel:
    """Coerce a leaf value to a :class:`LogLevel`, naming the valid set on failure."""
    try:
        return LogLevel(value)
    except ValueError as exc:
        valid = [level.value for level in LogLevel]
        raise ValueError(f"{value!r} is not a log level; expected one of {valid}") from exc


def _as_text(field: FilterField, value: Any) -> str:
    """Coerce a leaf value to a bounded string, or explain why it is not one.

    ``bool`` fails the ``isinstance(value, str)`` test, which is what we want: ``True`` is not a
    service name, and silently stringifying it would produce a filter matching nothing.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"field {field.value!r} compares against text, but the value is a "
            f"{type(value).__name__}"
        )
    if len(value) > MAX_Q_LEN:
        raise ValueError(
            f"filter values are limited to {MAX_Q_LEN} characters, got {len(value)}"
        )
    return value


def _as_epoch(value: Any) -> float:
    """Coerce a leaf value to a POSIX timestamp in UTC.

    Returns a float rather than a ``datetime`` because that is what the compiled predicate
    compares against: :class:`~src.store.StoredEntry` precomputes ``ts_epoch`` for exactly this,
    so a range test is two float compares instead of re-deriving a timestamp per record.
    """
    if isinstance(value, bool):
        # `isinstance(True, int)` is True in Python, and pydantic reads an int as an epoch — so
        # without this, `{"field": "ts", "op": "gt", "value": true}` would quietly become
        # 1970-01-01T00:00:01Z and match the entire corpus.
        raise ValueError("field 'ts' compares against a timestamp; a boolean is not one")
    try:
        parsed = _TS_ADAPTER.validate_python(value)
    except ValidationError as exc:
        raise ValueError(
            f"field 'ts' expects an RFC-3339 timestamp or a POSIX epoch, got {value!r}"
        ) from exc
    return _to_utc(parsed).timestamp()


def _coerce_scalar(field: FilterField, op: FilterOp, value: Any) -> str | int | float:
    """Turn one wire scalar into the operand the compiled predicate will compare against."""
    if field is FilterField.TS:
        return _as_epoch(value)

    if field is FilterField.LEVEL:
        level = _as_level(value)
        # Ordering compares ordinals; identity compares the wire string. Resolving which one here
        # — at compile time, once — is what keeps `LEVEL_ORDER[...]` off the per-record path.
        return LEVEL_ORDER[level] if op in ORDER_OPS else level.value

    text = _as_text(field, value)
    if op is FilterOp.CONTAINS:
        if not text:
            raise ValueError(
                "'contains' needs a non-empty needle: '' is a substring of every string, so an "
                "empty one is not a filter at all"
            )
        # Lower-cased **once, here**, so the per-record test is a plain substring search against
        # the store's precomputed `message_lower`. Same rule as `Filter.from_query`'s `q`.
        return text.lower()
    return text


def coerce_filter_value(field: FilterField, op: FilterOp, value: Any) -> Any:
    """Validate a leaf's ``value`` against its ``(field, op)`` pair and return the **operand**.

    One function, two callers, and that is the point: :class:`FilterLeaf` calls it during
    validation so a bad value is a ``422`` at the edge, and :func:`~src.store.compile_filter`
    calls it again to obtain the comparison operand. A second implementation of "what does this
    value mean" living inside the compiler is how a filter that validates cleanly ends up
    evaluating differently — so there is only one.

    Returns:
        ``frozenset[str | int | float]`` for ``in``/``nin`` (membership is a hash lookup, not a
        list scan), a ``float`` epoch for ``ts``, an ``int`` ordinal for ordered ``level``
        comparisons, a lower-cased ``str`` for ``contains``, and the exact ``str`` otherwise.

    Raises:
        ValueError: On any type or bound violation. Pydantic renders it as the ``422`` detail.
    """
    if op in LIST_OPS:
        if not isinstance(value, list):
            raise ValueError(
                f"operator {op.value!r} requires a list value, got a {type(value).__name__}"
            )
        if not value:
            # An empty `in` matches nothing. Accepting it would answer "no matching logs" to a
            # request that was almost certainly a client bug — the same reasoning that makes
            # `since > until` a 400 on `GET /logs` rather than a cheerful empty page.
            raise ValueError(
                f"operator {op.value!r} needs at least one value; an empty list matches nothing, "
                "which is indistinguishable from 'no such logs'"
            )
        if len(value) > MAX_FILTER_VALUES:
            raise ValueError(
                f"operator {op.value!r} accepts at most {MAX_FILTER_VALUES} values, "
                f"got {len(value)}"
            )
        return frozenset(_coerce_scalar(field, op, item) for item in value)

    if isinstance(value, list):
        raise ValueError(
            f"operator {op.value!r} requires a single value, not a list — did you mean 'in'?"
        )
    return _coerce_scalar(field, op, value)


class FilterLeaf(BaseModel):
    """One predicate: ``{"field": "level", "op": "in", "value": ["ERROR", "FATAL"]}``.

    The leaves are where all the real validation happens. An unknown ``field`` or ``op`` is
    rejected by the enums; an operator that does not suit its field is rejected by
    :data:`FIELD_OPS`; and a value of the wrong shape (a list where a scalar belongs, a
    non-level where a level belongs, an unparseable timestamp) is rejected by
    :func:`coerce_filter_value`. All four are ``422``s **at parse time**, before a single record
    is touched — a filter that only fails on the thousandth row is a filter that has already
    burned the request.

    ``contains`` is a **case-insensitive substring** test, matching the ``q`` query parameter
    exactly. The store precomputes ``message_lower`` for precisely this, so a ``contains`` over
    ``message`` costs a substring search and no allocation.
    """

    model_config = ConfigDict(extra="forbid")

    field: FilterField = Field(
        description="Entry attribute to test: level | service | host | message | ts.",
        examples=["level"],
    )
    op: FilterOp = Field(
        description=(
            "Comparison to apply. Valid operators depend on the field: ordering "
            "(gt/gte/lt/lte) only on `ts` and `level` (by severity, not alphabetically); "
            "`contains` (case-insensitive substring) only on `service`/`host`/`message`; "
            "`in`/`nin` take a list, everything else takes a scalar."
        ),
        examples=["in"],
    )
    value: FilterScalar | list[FilterScalar] = Field(
        description=(
            "The operand. A list for `in`/`nin` (1..64 entries), a scalar otherwise. Strings are "
            f"capped at {MAX_Q_LEN} characters. `ts` accepts RFC-3339 or a POSIX epoch."
        ),
        examples=[["ERROR", "FATAL"]],
    )

    @model_validator(mode="after")
    def _check_operator_and_value(self) -> FilterLeaf:
        """Enforce the field x operator matrix and the value's shape. See :data:`FIELD_OPS`."""
        allowed = FIELD_OPS[self.field]
        if self.op not in allowed:
            raise ValueError(
                f"operator {self.op.value!r} is not valid on field {self.field.value!r}; "
                f"valid operators there are {sorted(item.value for item in allowed)}"
            )
        # Result discarded: this call is here to *reject*, and the compiler calls the same
        # function again for the operand. Caching it on the model would mean carrying a
        # non-serialisable frozenset on a wire type for no gain — the tree is at most 100 nodes.
        coerce_filter_value(self.field, self.op, self.value)
        return self


class FilterAll(BaseModel):
    """Conjunction: ``{"all": [ … ]}`` — every child must match.

    .. rubric:: ``{"all": []}`` matches **everything**, and that surprises people

    An empty conjunction is *vacuously true*: "every one of these zero conditions holds" is a
    true statement about any record, and ``True`` is the identity of AND (``x and True == x``, so
    the empty product must be ``True`` for nesting to compose). It is the mathematically standard
    reading, it is the only one under which ``{"all": [A, {"all": []}]}`` still means ``A``, and
    it is what makes an empty filter and an omitted filter agree.

    The alternative — treating it as "match nothing" — would make the natural client behaviour
    (start with an empty ``all`` and push conditions into it as the user ticks boxes) return zero
    rows until the first box is ticked, which reads as a broken search. See :class:`FilterAny`
    for the mirror-image rule, which is the one that genuinely catches people out.
    """

    model_config = ConfigDict(extra="forbid")

    all: list[FilterNode] = Field(
        description=(
            "Child nodes that must ALL match. An empty list matches every entry (vacuous truth "
            "— the identity of AND)."
        )
    )


class FilterAny(BaseModel):
    """Disjunction: ``{"any": [ … ]}`` — at least one child must match.

    .. rubric:: ``{"any": []}`` matches **nothing**

    The mirror of :class:`FilterAll`'s rule and the one that actually catches people out: an
    empty disjunction is vacuously *false*, because ``False`` is the identity of OR
    (``x or False == x``). "At least one of these zero conditions holds" is false for every
    record.

    So the two empty collections are opposites, which is exactly right and exactly why it is
    documented here rather than left for someone to discover: ``{"all": []}`` matches everything
    and ``{"any": []}`` matches nothing. ``tests/unit/test_filters.py`` pins both directly.
    """

    model_config = ConfigDict(extra="forbid")

    any: list[FilterNode] = Field(
        description=(
            "Child nodes of which at least ONE must match. An empty list matches no entry "
            "(the identity of OR)."
        )
    )


class FilterNot(BaseModel):
    """Negation: ``{"not": { … }}`` — the child must **not** match.

    The field is spelled ``not_`` in Python and ``not`` on the wire, because ``not`` is a
    reserved word. ``populate_by_name=True`` accepts either, so a Python caller constructing a
    tree directly is not forced to remember the alias.

    Negation is also the node that makes index hints dangerous: everything under a ``not`` is a
    statement about records that must be **excluded**, so deriving a candidate set from it would
    return precisely the wrong rows. :func:`~src.store.compile_filter` therefore refuses to
    descend into it when gathering hints — see that function's soundness note.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    not_: FilterNode = Field(
        alias="not", description="The node to negate. Matches when the child does not."
    )


#: One node of the filter tree: a boolean combinator or a leaf predicate, recursively.
#:
#: A plain union rather than a discriminated one. Because every member forbids extra keys and no
#: member's required key appears in another, at most one alternative can validate any given
#: object — the union is already unambiguous, and the resulting OpenAPI is a recursive ``anyOf``
#: of four ``$ref``s, which every schema consumer understands. A callable ``Discriminator`` would
#: buy tidier error paths at the cost of a less portable published document.
FilterNode = FilterAll | FilterAny | FilterNot | FilterLeaf

# The three combinators reference ``FilterNode`` before it exists (they are what it is made of),
# so pydantic leaves them incomplete at class-creation time and these calls finish the job now
# that the name resolves. Without them the models raise on first use rather than at import —
# and, worse, ``/openapi.json`` would 500 instead of publishing the schema.
FilterAll.model_rebuild()
FilterAny.model_rebuild()
FilterNot.model_rebuild()


#: The keys that mark a node as a combinator. A node carrying none of them is leaf-shaped.
_BRANCH_KEYS: tuple[str, ...] = ("all", "any", "not")


def check_filter_shape(value: Any) -> Any:
    """Reject an over-deep, over-wide or ambiguously-shaped tree **before** pydantic parses it.

    This runs as a ``mode="before"`` validator on :attr:`SearchRequest.filter`, which means it
    sees the **raw decoded JSON** — and that timing is the entire point. Pydantic validates a
    recursive model bottom-up, so by the time any ``mode="after"`` check could measure the depth,
    the whole tree has already been constructed; a body nested ten thousand levels deep would
    have exhausted the interpreter stack on the way in and surfaced as a ``500``. Measuring first,
    on the raw structure, turns that into a ``422`` that costs one iterative pass.

    The walk is **iterative** (an explicit stack) for the same reason. A recursive depth-checker
    that blows its own stack while proving the input is too deep has not checked anything.

    Three things are enforced here, and only these three — everything about a node's *contents*
    stays with :class:`FilterLeaf`, so no rule is written down twice:

    * depth <= :data:`MAX_FILTER_DEPTH`,
    * total node count <= :data:`MAX_FILTER_NODES`,
    * each node carries **at most one** of ``all`` / ``any`` / ``not``. A node mixing two of them
      has no meaning, and letting it through would produce four parallel union errors instead of
      one sentence naming the mistake.

    A non-``dict`` input (``None``, a list, a string, or a already-constructed model instance
    built in Python rather than parsed from JSON) is passed straight through for the union
    validator to complain about in its own vocabulary. The depth bound is therefore enforced a
    second time inside :func:`~src.store.compile_filter`, which is the one path a
    Python-constructed tree cannot skip.
    """
    if not isinstance(value, dict):
        return value

    stack: list[tuple[Any, int]] = [(value, 1)]
    nodes = 0
    while stack:
        node, depth = stack.pop()
        nodes += 1
        if depth > MAX_FILTER_DEPTH:
            raise ValueError(
                f"filter tree is nested deeper than {MAX_FILTER_DEPTH} levels; flatten it or "
                "split the query"
            )
        if nodes > MAX_FILTER_NODES:
            raise ValueError(
                f"filter tree carries more than {MAX_FILTER_NODES} nodes; depth alone does not "
                "bound a wide tree, so the total is capped too"
            )
        if not isinstance(node, dict):
            continue

        present = [key for key in _BRANCH_KEYS if key in node]
        if len(present) > 1:
            raise ValueError(
                f"a filter node carries {present} together; each node is exactly one of "
                "'all', 'any', 'not', or a leaf {'field', 'op', 'value'}"
            )
        if not present:
            continue  # leaf-shaped: FilterLeaf owns everything about it

        branch = node[present[0]]
        if present[0] == "not":
            stack.append((branch, depth + 1))
        elif isinstance(branch, list):
            stack.extend((child, depth + 1) for child in branch)
        # A non-list 'all'/'any' is left for the union validator to report as a type error.
    return value


class SortField(StrEnum):
    """The sortable dimension. There is exactly one, and that is a statement about the store.

    The ring's ``seq`` spine **is** time order (appends only ever add a larger seq, and the corpus
    is generated oldest-first), so a walk ordered by ``ts`` costs nothing beyond the scan itself.
    Every other ordering — by service, by level, by message — would require materialising and
    sorting the whole match set per request, which at 100k entries is the one thing a paginated
    read API exists to avoid. Offering it and then serving it slowly would be worse than not
    offering it.

    It is still spelled as a field rather than hardcoded, so the wire shape is honest about what
    is being ordered and a future secondary ordering is an added enum member — additive, not a
    breaking change to the request schema.
    """

    TS = "ts"


class SortSpec(BaseModel):
    """``{"field": "ts", "order": "desc"}`` — the sort half of a search request.

    Both halves default, so ``sort`` can be omitted entirely and still mean the same thing
    ``GET /logs`` means with no parameters: newest first.
    """

    model_config = ConfigDict(extra="forbid")

    field: SortField = Field(
        default=SortField.TS, description="Dimension to sort by. Only `ts` is supported."
    )
    order: SortOrder = Field(
        default=SortOrder.DESC, description="Scan direction; newest-first by default."
    )


class SearchRequest(BaseModel):
    """Body of ``POST /api/v1/logs/search`` — a filter tree, a sort, a page size and a cursor.

    .. rubric:: Why a POST for a read

    Two reasons, both in the README. A nested boolean filter does not fit in a URL — encoding one
    into a query parameter means inventing a serialisation format that no schema describes and no
    generated client can build. And a ``POST`` body keeps search terms out of proxy access logs,
    reverse-proxy dashboards and browser history: a query string is written down by every hop it
    passes through, and "which user id was somebody searching the logs for" is exactly the sort of
    thing that should not be sitting in an nginx log forever. (It is also why the C10 SSE route's
    ``?access_token=`` escape hatch is deliberately **not** extended to this route.)

    .. rubric:: There is no ``offset``

    ``GET /logs`` offers one, with the documented caveat that it drifts under concurrent appends.
    Search does not, and that is a deliberate narrowing rather than an omission. Offset paging
    exists for "jump to page 7" UIs over a stable table; a nested filter over a live append-only
    ring is a **stream** — the caller walks it to the end, and the cursor is the only pagination
    that survives concurrent writes while doing so. Offering both here would mean publishing an
    option whose only distinguishing property is that it can silently skip rows.

    ``limit`` is clamped exactly as it is on ``GET /logs`` (never a ``422``), and the cursor is
    the identical opaque ``b64:`` token — bound to this filter's fingerprint, so replaying a
    cursor from a different search is a ``400`` rather than a plausible-looking wrong page.
    """

    model_config = ConfigDict(extra="forbid")

    filter: FilterNode | None = Field(
        default=None,
        description=(
            "The boolean filter tree. Omit it (or send null) to match every entry. Nodes are "
            "`{\"all\": [...]}`, `{\"any\": [...]}`, `{\"not\": {...}}`, or a leaf "
            "`{\"field\": ..., \"op\": ..., \"value\": ...}`. Nesting is capped at "
            f"{MAX_FILTER_DEPTH} levels and {MAX_FILTER_NODES} nodes."
        ),
        examples=[
            {
                "all": [
                    {"field": "level", "op": "in", "value": ["ERROR", "FATAL"]},
                    {"not": {"field": "service", "op": "eq", "value": "search-svc"}},
                ]
            }
        ],
    )
    sort: SortSpec = Field(
        default_factory=SortSpec, description="Sort dimension and direction."
    )
    # No `ge`/`le`, for the same reason `LogQuery.limit` has none: clamping is `clamp_limit`'s
    # job and it never raises. An over-large page request is a 200 plus a header, never a 422.
    limit: int | None = Field(
        default=None,
        description=(
            "Requested page size. Clamped into [1, MAX_PAGE_SIZE] rather than rejected; the "
            "response carries X-Page-Limit-Clamped when the requested value was adjusted."
        ),
    )
    cursor: str | None = Field(
        default=None,
        description=(
            "Opaque `next_cursor` from a previous search page. Bound to this filter and sort "
            "order; replaying it against a different search is a 400, never a wrong page."
        ),
    )

    @field_validator("filter", mode="before")
    @classmethod
    def _bound_filter_shape(cls, value: Any) -> Any:
        """Bound the tree's size before pydantic recurses into it. See :func:`check_filter_shape`."""
        return check_filter_shape(value)


class ErrorBody(BaseModel):
    """The uniform error envelope for every non-2xx response.

    ``detail`` is first and always present, which keeps the body a superset of FastAPI's own
    default error shape (``{"detail": ...}``) — so a client, a test, or the Swagger UI that
    already knows how to read a FastAPI error keeps working, and everything else this API adds
    is strictly extra.
    """

    detail: str = Field(description="Human-readable explanation of the failure.")
    code: str | None = Field(
        default=None,
        description="Stable machine-readable error code, when one applies.",
        examples=["invalid_cursor"],
    )
    request_id: str | None = Field(
        default=None,
        description="The X-Request-ID of the failing request, for log correlation.",
    )


# ---------------------------------------------------------------------------------------------
# Auth response shapes
#
# ``Role`` and ``Tier`` are imported from ``src.auth`` rather than redeclared here, so the enum a
# token is *signed* with and the enum a response is *validated* against are the same object. Two
# parallel declarations would be a contract that only holds by coincidence. The import direction
# is safe and checked: ``src.auth`` depends on nothing but the standard library, jwt, bcrypt,
# pydantic and ``src.config`` — in particular it does NOT import this module or ``src.store`` —
# so ``models -> auth -> config`` is a chain, not a cycle.
# ---------------------------------------------------------------------------------------------


class TokenResponse(BaseModel):
    """Body of a successful ``POST /api/v1/auth/token``.

    ``access_token`` / ``token_type`` / ``expires_in`` are the RFC 6749 §5.1 access-token
    response, spelled exactly as the standard does — ``token_type`` is the literal lowercase
    ``"bearer"``, and ``expires_in`` is a **relative** lifetime in seconds. Keeping the standard
    field names means a generic OAuth2 client, the Swagger UI's *Authorize* button, and every
    HTTP library's bearer helper all work against this endpoint with no adapter.

    ``expires_at`` / ``role`` / ``tier`` are this API's additions on top. They are strictly
    redundant — all three are already claims inside the signed token — but a client that would
    otherwise have to base64-decode a JWT just to render "you are an analyst until 14:32" is a
    client that will do it wrong. An absolute RFC-3339 instant is also what the C12 verifier
    asserts against, since a relative ``expires_in`` cannot be compared to anything without first
    knowing when the response was received.
    """

    access_token: str = Field(
        description="The signed HS256 JWT. Send it as `Authorization: Bearer <token>`.",
    )
    token_type: str = Field(
        default="bearer",
        description=(
            "Always the literal `bearer`, lowercase, per RFC 6750. It is a constant rather than "
            "an omission because RFC 6749 §5.1 marks it REQUIRED."
        ),
        examples=["bearer"],
    )
    expires_in: int = Field(
        description="Token lifetime in seconds (ACCESS_TOKEN_TTL_MIN * 60).",
        examples=[1800],
    )
    expires_at: datetime = Field(
        description=(
            "Absolute expiry as RFC-3339 UTC with a 'Z' suffix — the same instant the token's "
            "`exp` claim carries, so the two can never disagree."
        ),
        examples=["2026-07-27T11:01:04.000Z"],
    )
    role: Role = Field(
        description="The granted role: viewer | analyst | writer | admin. Also the `role` claim."
    )
    tier: Tier = Field(
        description="The rate-limit tier: free | pro | enterprise. Also the `tier` claim."
    )

    @field_serializer("expires_at")
    def _serialise_expires_at(self, value: datetime) -> str:
        """Emit the same RFC-3339 ``Z`` form every other timestamp in this API uses.

        Reuses :func:`_rfc3339_z` rather than restating the format: one definition of "how this
        API writes a timestamp" means a client writes one parser. See that function for why
        ``+00:00`` and microseconds are not acceptable here.
        """
        return _rfc3339_z(value)


class PrincipalResponse(BaseModel):
    """Body of ``GET /api/v1/auth/me`` — the decoded principal, echoed back.

    The fastest way to prove a token works: one authenticated round trip that touches no store,
    no filter and no pagination, so a `200` here isolates the auth chain from everything else. It
    is also what a dashboard calls on load to decide which controls to render, which is why the
    role and tier are here rather than left inside the token for the client to dig out.

    Field names deliberately match :class:`~src.auth.Principal` one-for-one, so
    :meth:`from_principal` is a straight projection with nowhere for a mapping mistake to hide.
    """

    subject: str = Field(
        description="The authenticated username (the token's `sub` claim).",
        examples=["analyst"],
    )
    role: Role = Field(description="Access role: viewer | analyst | writer | admin.")
    tier: Tier = Field(description="Rate-limit tier: free | pro | enterprise.")
    issued_at: datetime = Field(
        description="When the token was signed (`iat`), RFC-3339 UTC with a 'Z' suffix.",
        examples=["2026-07-27T10:31:04.000Z"],
    )
    expires_at: datetime = Field(
        description="When the token stops being accepted (`exp`), same format.",
        examples=["2026-07-27T11:01:04.000Z"],
    )

    @field_serializer("issued_at", "expires_at")
    def _serialise_instants(self, value: datetime) -> str:
        """Both instants in the API's one timestamp format. See :func:`_rfc3339_z`."""
        return _rfc3339_z(value)

    @classmethod
    def from_principal(cls, principal: Principal) -> PrincipalResponse:
        """Project a :class:`~src.auth.Principal` onto its wire shape.

        The projection lives here, beside the model, rather than being spelled out in the
        handler: ``GET /auth/me`` is not the only thing that will ever want to render a
        principal, and two hand-written projections are two chances to map ``subject`` to the
        wrong field.
        """
        return cls(
            subject=principal.subject,
            role=principal.role,
            tier=principal.tier,
            issued_at=principal.issued_at,
            expires_at=principal.expires_at,
        )


def clamp_limit(requested: int | None, settings: Settings) -> tuple[int, bool]:
    """Resolve a client's ``limit`` into an effective page size. **Never raises.**

    Returns ``(effective_limit, was_clamped)``. C5 sets the ``X-Page-Limit-Clamped`` header
    (:data:`CLAMPED_HEADER`) to the *requested* value when the boolean is true, and reports the
    effective value in ``page.limit`` — so the client is told both what it asked for and what
    it got.

    Clamping rather than rejecting is an explicit README requirement: "a client asking for
    10,000 rows gets the ceiling and a header saying so — not a ``422``". Rejecting would be
    defensible in isolation, but it makes every naive client's first request fail, and a
    paginated API whose whole job is to protect the server from over-large reads already knows
    the right answer — so it should just give it. The floor is clamped the same way
    (``limit=0`` and ``limit=-5`` both become 1) purely for consistency: there is no reading of
    "give me zero rows" that is more useful than "give me one".

    ``None`` means the client expressed no preference, so it gets ``DEFAULT_PAGE_SIZE`` and
    ``was_clamped=False`` — nothing of *theirs* was adjusted, so no header is warranted. The
    default is still capped by the ceiling, because a ``DEFAULT_PAGE_SIZE > MAX_PAGE_SIZE``
    misconfiguration must not be able to serve a page larger than the documented maximum.
    """
    ceiling = max(1, settings.max_page_size)
    if requested is None:
        return min(max(1, settings.default_page_size), ceiling), False
    if requested > ceiling:
        return ceiling, True
    if requested < 1:
        return 1, True
    return requested, False

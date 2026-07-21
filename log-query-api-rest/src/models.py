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
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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

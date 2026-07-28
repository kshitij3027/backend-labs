"""Input validation and sanitisation for **every** filter and mutation input (spec §2 item 34).

One module, so the rules can be read in one sitting rather than reconstructed from four resolvers.
Both entry points funnel through here and neither can be bypassed:

* :meth:`src.graphql.inputs.LogFilterInput.to_log_query` — the single conversion every read path
  uses (``Query.logs``, ``Query.logsConnection``, and C7's cache warm path when it lands).
* :func:`validate_create_log` — called by ``Mutation.createLog`` before anything touches a session.

.. rubric:: The rules exist to move a rejection *earlier*, not to add ceremony

Every cap below has a specific failure it prevents, and in most cases the failure is not "bad data
gets stored" but "the client is told something useless":

* **Length caps mirror the column widths** in :mod:`src.db.models`. A 200-character ``service``
  reaches PostgreSQL as a ``VARCHAR(64)`` bind and comes back as an asyncpg ``DataError`` — which,
  with :class:`~src.graphql.errors.MaskInternalErrors` installed, the client sees as *"an
  unexpected internal error occurred"*. The value was wrong, it was wrong in a way the client could
  fix, and the only honest place to say so is before the statement is built.
* **A too-long** ``service`` **filter is rejected rather than run.** It could not match any row —
  no stored service is that long — so running it returns ``[]``, and an empty list is
  indistinguishable from "nothing happened in that window". Rejecting it says which of the two.
* **NUL bytes are refused.** PostgreSQL ``text`` cannot represent ``U+0000`` at all; the driver
  raises on encode. Same masked-500 outcome as above, from a single stray byte.
* **Metadata is bounded in shape as well as size.** ``JSON`` is an untyped scalar, so a client can
  send ``metadata: 5`` or a thousand-deep nest and the schema will not object. The column is
  ``JSONB`` and stores either happily; what suffers is everything downstream that assumes an
  object (C11's ``metadata ? 'host'`` aggregations, the C13 dashboard's key/value rendering).
* **NaN and Infinity are refused.** ``json.dumps`` emits them by default and ``json.loads``
  accepts them, but they are not JSON, and ``JSONB`` rejects them at the server.

.. rubric:: What is deliberately NOT validated here

``limit`` is **clamped, not rejected** — see :func:`src.db.repository.clamp_limit`. A client asking
for a million rows should get ``MAX_QUERY_LIMIT`` of them and a usable answer; turning the cap into
an error would make raising it a breaking change for every client that guessed high, and C3 pins
that behaviour.

``level`` needs nothing: it is a GraphQL **enum**, so an unknown value is rejected during
validation, before a resolver runs, by graphql-core itself (see :mod:`src.graphql.enums`).

``search_text`` may be **empty**. An empty search box means "no substring constraint", and C2
documents ``search_text=""`` as matching every message on purpose. Its LIKE metacharacters are
neutralised by :func:`src.db.repository.escape_like` inside the statement builder, which is the one
place every path goes through — nothing here re-escapes them, because escaping twice is how a
search for ``100%`` starts matching nothing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.db.models import SERVICE_MAX_LENGTH, TRACE_ID_MAX_LENGTH
from src.db.repository import as_utc
from src.graphql.errors import ValidationError

if TYPE_CHECKING:  # pragma: no cover - import-cycle break, evaluated by type checkers only
    # `src.graphql.inputs` imports this module, so importing it back at run time would be a cycle.
    # `from __future__ import annotations` makes the annotations below strings, so nothing here
    # needs the real classes.
    from src.graphql.inputs import CreateLogInput, LogFilterInput

# ---------------------------------------------------------------------------------------------
# Caps.
#
# The first two are RE-EXPORTED from src.db.models rather than restated, because a validator that
# disagreed with its column would be worse than no validator: it would reject values the database
# accepts, or pass values it does not. The rest have no column to mirror and are documented
# individually.
# ---------------------------------------------------------------------------------------------

#: Mirrors ``String(SERVICE_MAX_LENGTH)`` on ``log_entries.service``.
MAX_SERVICE_LENGTH = SERVICE_MAX_LENGTH

#: Mirrors ``String(TRACE_ID_MAX_LENGTH)`` on ``log_entries.trace_id``.
MAX_TRACE_ID_LENGTH = TRACE_ID_MAX_LENGTH

#: ``message`` is a ``Text`` column, so PostgreSQL imposes no length of its own (its hard ceiling
#: is ~1GB per value). This cap is therefore a **resource bound we choose**, not a column mirror:
#: 32 KiB comfortably holds a real log line including a full stack trace, while stopping one
#: request from writing a megabyte into a row that the C13 dashboard then tries to render.
MAX_MESSAGE_LENGTH = 32_768

#: ``search_text`` becomes ``message ILIKE '%…%'``, served by the ``gin_trgm_ops`` index. A pattern
#: is decomposed into overlapping three-character shingles, so a 256-character needle already
#: expands to ~254 index probes followed by a heap recheck; beyond that the index stops helping and
#: the query degrades into a scan. 256 characters is also far past any substring a human types.
MAX_SEARCH_TEXT_LENGTH = 256

#: Nesting depth allowed in ``metadata``. Log context is flat by nature (``host``, ``region``,
#: ``latency_ms``); five levels is generous for a serialised request envelope and still bounds the
#: recursion in :func:`_measure_json` to something a stack can hold.
MAX_METADATA_DEPTH = 5

#: Total container members allowed in ``metadata`` (object keys + array elements, recursively).
#: Bounds a payload that is shallow but enormous, which the depth cap alone does not.
MAX_METADATA_NODES = 200

#: Serialised size ceiling for ``metadata``, in bytes of compact JSON. The row is what gets stored,
#: replicated and streamed to every C6 subscriber, so this is the number that decides how much a
#: single ``createLog`` can cost everyone downstream.
MAX_METADATA_BYTES = 8_192


@dataclass(frozen=True, slots=True)
class CreateLogParams:
    """A validated, normalised ``createLog`` payload, ready for the repository.

    Deliberately **not** a GraphQL type and deliberately not the input object: it is what survives
    validation, with the enum already reduced to the string the ``level`` column holds and
    whitespace already resolved. A resolver holding one of these cannot accidentally persist the
    raw input, because the raw input is no longer in scope.

    ``timestamp`` stays ``None`` when the client omitted it. Defaulting it to "now" is left to
    :meth:`src.db.repository.LogRepository.insert_log`, which already does exactly that and
    documents why the wall-clock read is correct *there* — resolving it in two places would give
    the project two answers to "when did this log line happen".
    """

    service: str
    level: str
    message: str
    timestamp: datetime | None
    metadata: dict[str, Any] | None
    trace_id: str | None


# ---------------------------------------------------------------------------------------------
# Primitive rules. Each raises ValidationError naming the field, the limit and the offending
# value's size — enough for a client to fix the request without reading this file.
# ---------------------------------------------------------------------------------------------


def _reject_nul(field: str, value: str) -> None:
    """Refuse ``U+0000``, which PostgreSQL ``text`` cannot store at all."""
    if "\x00" in value:
        raise ValidationError(
            f"{field} must not contain NUL (U+0000) characters: PostgreSQL text columns cannot "
            "represent one, so the value would be rejected by the driver rather than stored"
        )


def _require_max_length(field: str, value: str, maximum: int) -> None:
    """Refuse a value longer than the column (or the documented bound) can hold."""
    if len(value) > maximum:
        raise ValidationError(
            f"{field} must be at most {maximum} characters, got {len(value)}"
        )


def _require_non_blank(field: str, value: str) -> None:
    """Refuse an empty or whitespace-only value for a field that names something."""
    if not value.strip():
        raise ValidationError(
            f"{field} must not be empty or whitespace-only; omit the field entirely if you have "
            "no value for it"
        )


def _measure_json(value: Any, field: str, depth: int = 1) -> int:
    """Walk a decoded JSON value, enforcing depth and type rules; return its container-member count.

    Returns the number of object keys plus array elements at every level, which the caller compares
    against :data:`MAX_METADATA_NODES`. Scalars contribute nothing — they are bounded by the byte
    ceiling instead.

    ``depth`` counts **containers**, not recursion steps: ``{"a": 1}`` is one level deep, not two.
    That is why the check below is guarded on the container test rather than sitting at the top of
    the function — an unguarded check would count the scalar leaf as a level and make
    :data:`MAX_METADATA_DEPTH` mean one less than it says, which is exactly the kind of off-by-one
    that only ever surfaces as "why was this payload rejected?".

    Raises:
        ValidationError: If the value nests deeper than :data:`MAX_METADATA_DEPTH`, contains a
            non-string object key, or contains a value of a type JSON cannot express.
    """
    if isinstance(value, (dict, list)) and depth > MAX_METADATA_DEPTH:
        raise ValidationError(
            f"{field} nests deeper than {MAX_METADATA_DEPTH} levels; log context is flat by "
            "nature, and an unbounded nest is a storage and rendering cost paid by every reader"
        )

    if isinstance(value, dict):
        nodes = 0
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValidationError(
                    f"{field} object keys must be strings, got {type(key).__name__}"
                )
            nodes += 1 + _measure_json(item, field, depth + 1)
        return nodes

    if isinstance(value, list):
        return sum(1 + _measure_json(item, field, depth + 1) for item in value)

    # bool before int is unnecessary here (bool IS an int and both are legal), but the explicit
    # tuple is what makes the *rejection* branch reachable: anything else — a datetime, a set, a
    # Decimal handed in by a Python caller — is not JSON and must not reach a JSONB bind.
    if value is None or isinstance(value, (str, bool, int, float)):
        return 0

    raise ValidationError(
        f"{field} contains a value of type {type(value).__name__}, which is not valid JSON"
    )


# ---------------------------------------------------------------------------------------------
# Public rules
# ---------------------------------------------------------------------------------------------


def validate_metadata(value: Any, field: str = "metadata") -> dict[str, Any] | None:
    """Validate a ``JSON`` scalar as a bounded JSON **object**; ``None`` passes through.

    The object requirement is the load-bearing half. ``JSON`` is untyped on the wire, so
    ``metadata: 5``, ``metadata: "text"`` and ``metadata: [1, 2]`` all satisfy the schema and all
    store cleanly into ``JSONB`` — and then every consumer that treats metadata as a mapping
    (C11's key-existence aggregations, the dashboard's key/value table, ``jsonb_typeof`` filters)
    silently skips or breaks on those rows. Rejecting at the edge keeps one shape in the column.

    Returns:
        The value unchanged when it is an acceptable object, or ``None``.

    Raises:
        ValidationError: If the value is not an object, nests too deep, has too many members,
            serialises above :data:`MAX_METADATA_BYTES`, or contains a non-JSON value such as
            ``NaN``.
    """
    if value is None:
        return None

    if not isinstance(value, dict):
        raise ValidationError(
            f"{field} must be a JSON object, got {type(value).__name__}; scalars and arrays are "
            "rejected so every stored row has the same shape for consumers to read"
        )

    nodes = _measure_json(value, field)
    if nodes > MAX_METADATA_NODES:
        raise ValidationError(
            f"{field} holds {nodes} members, which is over the limit of {MAX_METADATA_NODES}"
        )

    try:
        # `allow_nan=False` is the point of doing this with json rather than len(str(value)):
        # NaN and Infinity round-trip through Python's json module but are not JSON, and JSONB
        # rejects them at the server — another masked 500 for a value we can name precisely here.
        encoded = json.dumps(value, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:
        raise ValidationError(
            f"{field} is not serialisable as JSON ({exc}); NaN and Infinity in particular are "
            "accepted by Python but rejected by the JSONB column"
        ) from exc

    size = len(encoded.encode("utf-8"))
    if size > MAX_METADATA_BYTES:
        raise ValidationError(
            f"{field} serialises to {size} bytes, which is over the limit of "
            f"{MAX_METADATA_BYTES}"
        )

    return value


def validate_time_range(
    start_time: datetime | None, end_time: datetime | None
) -> tuple[datetime | None, datetime | None]:
    """Normalise both bounds to UTC and refuse an inverted range.

    An inverted range is not merely empty, it is *always* empty: ``timestamp >= :start AND
    timestamp <= :end`` with ``start > end`` cannot match, so the query succeeds, returns nothing,
    and the client reads that as "no logs in this window". Two datetime pickers in the wrong order
    is the most ordinary way to produce it, and the least ordinary to diagnose from an empty table.

    Both bounds are inclusive (see :func:`src.db.repository.build_predicates`), so
    ``start == end`` is a legal one-instant window and is accepted.

    Normalisation is :func:`src.db.repository.as_utc` — reused, not reimplemented, so the
    comparison made here is against exactly the instants the WHERE clause will use. Comparing the
    raw inputs would be wrong for a mixed pair: ``13:00+02:00`` is *earlier* than ``12:00Z``, and a
    naive comparison of aware datetimes in different zones would still be attempted and still be
    misleading.

    Returns:
        The two bounds as UTC-aware datetimes (or ``None``), in the order given.
    """
    start = as_utc(start_time)
    end = as_utc(end_time)

    if start is not None and end is not None and start > end:
        raise ValidationError(
            f"startTime ({start.isoformat()}) must not be after endTime ({end.isoformat()}); "
            "an inverted range can never match a row, so it returns an empty result that looks "
            "exactly like a quiet window"
        )

    return start, end


def validate_log_filter(filters: LogFilterInput) -> None:
    """Check a ``LogFilterInput`` in place. Raises on the first failure; returns nothing.

    Called from :meth:`~src.graphql.inputs.LogFilterInput.to_log_query`, which is the single
    conversion every read path performs — so "the filters were validated" is structurally true of
    ``logs``, ``logsConnection`` and anything later that reuses the conversion, rather than being a
    line each resolver has to remember.

    Nothing is mutated: a filter is compared against stored values, and silently trimming a
    ``service`` the client actually typed with a space would change which rows match without
    saying so.
    """
    if filters.service is not None:
        _reject_nul("service", filters.service)
        _require_non_blank("service", filters.service)
        # Rejected rather than run: no stored service can be this long, so the query would return
        # an empty list that reads as "this service was quiet".
        _require_max_length("service", filters.service, MAX_SERVICE_LENGTH)

    if filters.search_text is not None:
        _reject_nul("searchText", filters.search_text)
        # No blank check — see the module docstring: an empty needle means "no constraint".
        _require_max_length("searchText", filters.search_text, MAX_SEARCH_TEXT_LENGTH)

    validate_time_range(filters.start_time, filters.end_time)


def validate_subscription_filter(service: str | None) -> None:
    """Check ``Subscription.logStream``'s ``service`` argument — spec §2 item 33, on the WS path.

    ``level`` needs nothing here for the same reason ``LogFilterInput.level`` does not: it is the
    :class:`~src.graphql.enums.LogLevel` enum, so an unknown severity is rejected during validation
    with a message naming the five legal values, before the resolver runs and before a queue is
    allocated.

    ``service`` is the one free-form argument, and the rules are deliberately **the same three**
    ``validate_log_filter`` applies. A subscription filter and a query filter that disagreed about
    what a legal service name is would be a genuinely confusing surface: the same string would be
    accepted by ``logs`` and rejected by ``logStream``, or worse, accepted by both and match on one.

    Nothing is trimmed, for the same reason nothing is trimmed in ``validate_log_filter``: the
    filter is compared against stored values, and silently normalising a name the client typed
    would change which entries stream without saying so.

    Raises:
        ValidationError: ``service`` is blank, over-long, or contains a NUL byte.
    """
    if service is None:
        return
    _reject_nul("service", service)
    _require_non_blank("service", service)
    _require_max_length("service", service, MAX_SERVICE_LENGTH)


def validate_create_log(log_data: CreateLogInput) -> CreateLogParams:
    """Validate and normalise a ``createLog`` payload.

    .. rubric:: What is trimmed, and what deliberately is not

    ``service`` and ``trace_id`` are **stripped**. Both are identifiers used for grouping —
    ``logStats`` groups by service, C5's ``related_logs`` groups by trace id — and
    ``"auth-service "`` arriving from a copy-pasted config would become a second, invisible service
    in every aggregate, sitting next to the real one and impossible to spot in a chart.

    ``message`` is **not** stripped. It is content, not a key: leading indentation carries meaning
    in a wrapped stack trace, and nothing ever groups by it. It is still refused when it is
    *entirely* whitespace, which is never a log line anybody meant to write.

    ``trace_id`` supplied as blank is refused rather than silently turned into ``None``. The two
    mean very different things downstream — ``NULL`` means "not correlated" and ``related_logs``
    returns an empty list, while an empty string would be a trace id that every other blank-trace
    row also shares, silently correlating unrelated requests into one enormous group.

    Returns:
        A :class:`CreateLogParams` whose fields can be handed straight to
        :meth:`src.db.repository.LogRepository.insert_log`.

    Raises:
        ValidationError: On the first rule that fails, naming the field and the limit.
    """
    service = log_data.service
    _reject_nul("service", service)
    _require_non_blank("service", service)
    service = service.strip()
    _require_max_length("service", service, MAX_SERVICE_LENGTH)

    message = log_data.message
    _reject_nul("message", message)
    _require_non_blank("message", message)
    _require_max_length("message", message, MAX_MESSAGE_LENGTH)

    trace_id = log_data.trace_id
    if trace_id is not None:
        _reject_nul("traceId", trace_id)
        _require_non_blank("traceId", trace_id)
        trace_id = trace_id.strip()
        _require_max_length("traceId", trace_id, MAX_TRACE_ID_LENGTH)

    metadata = validate_metadata(log_data.metadata)

    # A single instant needs no range check, but it does need the same UTC normalisation every
    # other timestamp in the system gets — a naive value bound against a `timestamptz` column is
    # interpreted in the SERVER's TimeZone setting, so the row would land at an hour nobody chose.
    timestamp = as_utc(log_data.timestamp)

    return CreateLogParams(
        service=service,
        # Strawberry hands the resolver a LogLevel MEMBER; the column holds the member's value.
        # The two are identical strings by construction — see
        # `src.graphql.enums._assert_levels_match_the_corpus` — but passing the member through
        # would bind an Enum against a VARCHAR and asyncpg would refuse it.
        level=log_data.level.value,
        message=message,
        timestamp=timestamp,
        metadata=metadata,
        trace_id=trace_id,
    )

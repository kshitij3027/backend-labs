"""Mapped tables and the plain value object that feeds them.

Two representations of one log line live here, side by side and on purpose:

* :class:`LogEntryORM` — the mapped row. Has an ``id``, belongs to a session, is mutable.
* :class:`LogRecord` — a frozen dataclass with no identity. What :mod:`src.generators` emits and
  what every oracle in the test suite and the E2E verifier compares against.

Keeping them in one module means the translation between them (which is really just the
``metadata_`` / ``metadata`` name split, see below) is written down once, in the only place a
reader would look for it.

.. rubric:: Why two representations rather than one

The generated corpus is the project's **ground truth**: a test computes the expected result of a
filter in Python and then asserts the database agrees. That only works if two generator runs with
the same arguments produce *equal* corpora — and ORM instances compare by identity, so
``generate(...) == generate(...)`` would be ``False`` for two perfectly identical corpora. A frozen
dataclass compares by value, which is what makes the determinism test able to fail for the right
reason. It is also, secondarily, the cheaper object: seeding builds thousands of these and hands
them straight to a multi-row INSERT without ever putting them in a session's identity map.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base

#: Column length caps. Named constants rather than literals in the ``mapped_column`` calls because
#: C4's input validation has to reject an over-long value *before* it reaches SQL — a rejection at
#: the database boundary surfaces to the client as an opaque driver error rather than as a typed
#: ``VALIDATION_ERROR``, and the two limits must be the same number to stay honest.
SERVICE_MAX_LENGTH = 64
LEVEL_MAX_LENGTH = 16
TRACE_ID_MAX_LENGTH = 64

#: Business-key caps for the C10 e-commerce event tables. ``order_id`` and ``user_id`` are opaque
#: identifiers minted upstream (``ord-60000``, ``u-1001``), so 64 characters is the same generous
#: ceiling ``trace_id`` gets and for the same reason: it is wide enough for a UUID or a prefixed
#: ULID, and narrow enough that a client cannot use the column as free-form storage.
ORDER_ID_MAX_LENGTH = 64
USER_ID_MAX_LENGTH = 64

#: Caps for the four small controlled vocabularies (status / method / outcome / activity). They are
#: stored as ``String`` rather than as PostgreSQL ``ENUM`` types — see the note on
#: :attr:`OrderEventORM.status` — so the width is the only storage-level statement about them.
ORDER_STATUS_MAX_LENGTH = 24
PAYMENT_METHOD_MAX_LENGTH = 24
PAYMENT_OUTCOME_MAX_LENGTH = 24
USER_ACTIVITY_MAX_LENGTH = 24


class LogEntryORM(Base):
    """One log line, as stored.

    Field-for-field the spec's §2 ``LogEntry``: ``id``, ``timestamp``, ``service``, ``level``,
    ``message``, optional ``metadata``, optional ``trace_id``. C3 maps this onto the GraphQL type
    of the same shape; there is nothing here that the API does not expose, and nothing the API
    exposes that is not here.
    """

    __tablename__ = "log_entries"

    #: The GraphQL ``ID``. ``BigInteger`` (``BIGSERIAL`` in PostgreSQL) rather than ``Integer``:
    #: a log store is append-only and the load harness alone writes tens of thousands of rows per
    #: run, so the 2.1-billion ceiling of a 32-bit serial is a real number rather than a
    #: theoretical one. Widening a primary key later is a rewrite of the table and every index on
    #: it; starting wide costs four bytes a row.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    #: Always stored timezone-aware in UTC (``TIMESTAMP WITH TIME ZONE``). Both halves matter:
    #: ``timezone=True`` is what lets asyncpg hand back an aware ``datetime``, and normalising to
    #: UTC on the way in (see :func:`src.db.repository.as_utc`) is what stops a naive value from
    #: being compared against a ``timestamptz`` column under the *server's* TimeZone setting.
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    service: Mapped[str] = mapped_column(String(SERVICE_MAX_LENGTH), nullable=False)

    #: A plain ``String``, **not** a native PostgreSQL ``ENUM``, and that is a decision rather
    #: than an omission. A database-level enum makes adding a severity a migration — ``ALTER TYPE
    #: … ADD VALUE`` plus a coordinated deploy — to gain a constraint this system already enforces
    #: one layer earlier and far more usefully: C3's strongly-typed ``LogLevel`` GraphQL enum
    #: rejects an unknown level during *validation*, before a resolver runs, with a GraphQL error
    #: naming the legal values. A constraint at the edge produces a good error message; the same
    #: constraint in the storage engine produces a 500 from a driver exception.
    level: Mapped[str] = mapped_column(String(LEVEL_MAX_LENGTH), nullable=False)

    #: ``Text``, not ``String(n)``: messages are the free-form field, and a length cap here would
    #: truncate or reject real log lines for no storage benefit (PostgreSQL stores ``text`` and
    #: ``varchar`` identically; the cap is a constraint, not a layout).
    message: Mapped[str] = mapped_column(Text, nullable=False)

    # !!! DO NOT RENAME THIS TO `metadata`. !!!
    #
    # ``metadata`` is a reserved attribute on every SQLAlchemy declarative class: ``Base.metadata``
    # IS the MetaData registry that holds every table, and declaring ``metadata: Mapped[...]``
    # raises `InvalidRequestError: Attribute name 'metadata' is reserved` at class-definition
    # time — an import-time crash of the whole application, not a runtime bug.
    #
    # The fix is the two-name split below: the PYTHON attribute is ``metadata_`` and the first
    # positional argument to ``mapped_column`` renames the DB COLUMN back to ``metadata``. So the
    # database, the JSON payload and the GraphQL field are all spelled ``metadata`` (C3 resolves
    # it from this attribute), and the trailing underscore exists in exactly one place: here.
    #
    # `none_as_null=True` IS NOT REDUNDANT WITH `nullable=True` — do not delete it as noise.
    # SQLAlchemy's JSON/JSONB type defaults to `none_as_null=False`, meaning a Python `None` is
    # *serialised* into the JSON scalar `null` and stored as the JSONB value `'null'`. That is a
    # different thing from SQL NULL, and the difference is invisible from Python because JSONB
    # `'null'` deserialises straight back to `None` — so a round-trip test passes while the column
    # holds the wrong thing. It only surfaces in SQL: `metadata IS NULL` matches no row,
    # `jsonb_typeof(metadata)` returns the string 'null', `metadata ?? '{}'` never fires, and any
    # partial index or `WHERE metadata IS NOT NULL` aggregation in C11 silently counts every row.
    # With this flag a Python `None` becomes a real SQL NULL on every write path — the Core
    # multi-row INSERT that seeds the corpus and the ORM insert behind C4's `createLog` alike.
    metadata_: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", JSONB(none_as_null=True), nullable=True
    )

    #: Correlation id. ``JSONB`` above and this column are the two nullable fields, and both
    #: nullabilities are load-bearing: C5's ``related_logs`` must return an **empty list** for a
    #: row whose ``trace_id`` is NULL (spec §2 item 17), so rows without one are not an edge case
    #: to be tidied away — they are half of the behaviour under test.
    trace_id: Mapped[str | None] = mapped_column(String(TRACE_ID_MAX_LENGTH), nullable=True)

    __table_args__ = (
        # --- Index set. Each one exists for a named query, and none of them is speculative. ---
        #
        # A NOTE ON THE TRAILING `id`, because it looks like a filter column and is not one:
        # every composite index below ends in `id`, yet nothing ever filters on `id` alongside a
        # service or a level. It is there as the TOTAL-ORDER TIEBREAK. The one ordering this
        # project issues is `ORDER BY timestamp DESC, id DESC` (timestamps collide readily —
        # `createLog` under load writes several rows in the same instant), and C3's keyset cursor
        # pages with `WHERE (timestamp, id) < (:cursor_ts, :cursor_id)`, so the cursor is only a
        # total order if `id` participates. An index that stopped at `timestamp` can still satisfy
        # the *filter*, but the planner then has to feed the matched rows through a Sort node to
        # resolve the tiebreak — observed on the two-column form: `Bitmap Index Scan on
        # ix_log_entries_service_ts` under `Sort (Sort Key: timestamp DESC, id DESC)`.
        #
        # Honesty about what is and is not measured here: carrying `id` is what MAKES an ordered
        # index scan possible, but at the seeded corpus (2000 rows / ~69 heap pages) the planner
        # does not pick these indexes for the ordered read at all — it scans `ix_log_entries_ts_id`
        # backwards and filters, which is genuinely cheaper at that size. So this is a design
        # expectation for a corpus large enough to matter, NOT a benchmark result on today's data.
        # Do not cite it as one. The index NAMES keep the `_ts` suffix — they are the names the
        # schema test and every EXPLAIN reads, and renaming them buys nothing.
        #
        # A NOTE ON SORT DIRECTION, because the absence of `DESC` here is deliberate:
        # every index below is a plain ascending btree, yet the reads they serve are all
        # newest-first. That is fine — PostgreSQL can walk a btree BACKWARD, and a backward scan
        # satisfies an ORDER BY whose every column is reversed in the same direction at exactly
        # the same cost as a forward scan on a DESC-declared index. `ORDER BY timestamp DESC,
        # id DESC` is precisely that case. A DESC-declared index only buys something for MIXED
        # directions (`timestamp DESC, id ASC`), which this project never issues. So the DDL stays
        # simple and portable, and the plan is identical.
        #
        # 1. The default ordering, and the keyset cursor C3's LogConnection pages with:
        #        WHERE (timestamp, id) < (:cursor_ts, :cursor_id)
        #        ORDER BY timestamp DESC, id DESC LIMIT :n
        #    The unfiltered case, so the tiebreak note above applies here most directly: this is
        #    the index that makes pagination unable to skip or repeat a row.
        Index("ix_log_entries_ts_id", "timestamp", "id"),
        # 2. `LogFilterInput.service` + the default ordering: equality on the leading column, then
        #    an ordered range on the second, then the tiebreak. This is the shape of nearly every
        #    dashboard query, and the one a service-scoped keyset cursor pages through.
        Index("ix_log_entries_service_ts", "service", "timestamp", "id"),
        # 3. Same shape for `level`, which is also what C4's `logStats.errorCount` scans
        #    (`WHERE level = 'ERROR' AND timestamp BETWEEN …`) — counted in SQL, never by
        #    pulling rows into Python.
        Index("ix_log_entries_level_ts", "level", "timestamp", "id"),
        # 4. C5's `related_logs`: "every row sharing this trace_id". Without it, a single
        #    `LogEntry.relatedLogs` field on a 100-row result is 100 sequential scans, and the
        #    DataLoader that batches them into one round trip would still be scanning the whole
        #    table once per operation.
        Index("ix_log_entries_trace_id", "trace_id"),
        # 5. `LogFilterInput.search_text` -> `message ILIKE '%…%'`. A leading wildcard makes a
        #    btree useless (there is no prefix to seek on), so a plain index would be ignored by
        #    the planner and every substring search would be a sequential scan over the corpus —
        #    which is exactly the spec's sub-100ms budget spent on I/O. A GIN index over trigrams
        #    indexes the three-character shingles of every message instead, so an infix match
        #    becomes an index lookup.
        #
        #    This requires the `pg_trgm` extension, and it must exist BEFORE this index is
        #    created or `gin_trgm_ops` is an unknown operator class and CREATE INDEX fails.
        #    `Database.init_db` runs `CREATE EXTENSION IF NOT EXISTS pg_trgm` first, in the same
        #    transaction — see the ordering note there.
        Index(
            "ix_log_entries_message_trgm",
            "message",
            postgresql_using="gin",
            postgresql_ops={"message": "gin_trgm_ops"},
        ),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"LogEntryORM(id={self.id!r}, timestamp={self.timestamp!r}, "
            f"service={self.service!r}, level={self.level!r}, "
            f"trace_id={self.trace_id!r}, message={self.message!r})"
        )


@dataclass(frozen=True, slots=True)
class LogRecord:
    """A log line with **no identity** — the unit the deterministic generator emits.

    Deliberately has no ``id``: an id is something the database assigns, and a generator that
    invented one would either collide with the sequence or force the seeder to override it. What
    this carries is exactly the tuple of values that determines whether a row matches a filter,
    which is what makes it usable as an oracle.

    ``frozen=True`` because an oracle is something you compare against, never something you adjust
    until a comparison passes. ``slots=True`` because seeding builds ``SEED_ENTRIES`` of them at
    startup and a ``__dict__`` per instance is pure overhead for an object with six fields.

    Note it is *not* hashable in practice despite being frozen: ``metadata`` is a ``dict``, so
    ``hash()`` raises. Compare and sort lists of these rather than putting them in sets.
    """

    timestamp: datetime
    service: str
    level: str
    message: str
    metadata: dict[str, Any] | None
    trace_id: str | None

    def as_insert_params(self) -> dict[str, Any]:
        """Return this record keyed by **database column name**, ready for a Core INSERT.

        The one place the ``metadata_`` (Python) / ``metadata`` (SQL) split is spelled out on the
        way *in*, mirroring :meth:`from_orm_row` on the way out. Seeding uses a Core multi-row
        ``insert(LogEntryORM.__table__).values([...])`` rather than the ORM, so these keys are
        resolved against ``Table.c`` and must be column names.

        ``id`` is deliberately absent: leaving it out is what lets PostgreSQL's ``BIGSERIAL``
        assign ids in insert order, which — because the generator emits oldest-first — makes id
        order agree with time order and the ``(timestamp, id)`` tiebreak deterministic.
        """
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "level": self.level,
            "message": self.message,
            "metadata": self.metadata,
            "trace_id": self.trace_id,
        }

    @classmethod
    def from_orm_row(cls, row: LogEntryORM) -> LogRecord:
        """Project a stored row back onto the identity-free value object.

        The inverse of :meth:`as_insert_params`, and the reason integration tests can compare a
        database result against a locally regenerated corpus with a plain ``==``: it drops the
        ``id`` (which the oracle cannot know) and undoes the ``metadata_`` rename (which is the
        only naming difference between the two representations).
        """
        return cls(
            timestamp=row.timestamp,
            service=row.service,
            level=row.level,
            message=row.message,
            metadata=row.metadata_,
            trace_id=row.trace_id,
        )


# =================================================================================================
# C10 — the e-commerce event tables (spec §3 Feature Area A)
#
# Three more append-only streams beside `log_entries`: order events, payment events and user
# activity events. Every one of them carries the SAME four correlation columns the log line does
# (`timestamp`, `service`, `level`, `trace_id`), which is not a coincidence to be factored away —
# it is the requirement. `src.graphql.types.LogEvent` publishes exactly those four as a GraphQL
# interface implemented by all four types, so a client can ask one question ("everything correlated
# with this trace") and get a heterogeneous answer.
#
# ---------------------------------------------------------------------------------------------
# WHY THERE IS NO `relationship()` HERE, AND NO `ForeignKey` EITHER
# ---------------------------------------------------------------------------------------------
#
# The spec asks for "modeled relationships between them" — order -> user, order -> payments. Those
# relationships are modeled as an INDEXED SHARED KEY (`order_id`, `user_id`, `trace_id`) resolved
# through the repository and, from C11, through cross-entity DataLoaders. Not as ORM relationships.
# Three independent reasons, and the first one is decisive on its own:
#
#   1. THERE IS NO PARENT TABLE TO POINT AT. This system stores an event LOG, not entities: there
#      is no `orders` table and no `users` table, because `order_events` IS the order's history and
#      `user_events` IS the user's. `ForeignKey("orders.id")` cannot be written because `orders`
#      does not exist, and inventing two entity tables to satisfy a `relationship()` would be
#      modelling the ORM's preferences rather than the domain's.
#
#   2. A LAZY-LOADING RELATIONSHIP FIGHTS C11's DESIGN AND SILENTLY REINTRODUCES N+1. SQLAlchemy's
#      default loader strategy emits one SELECT per parent on first attribute access. In an async
#      resolver that access does not even get to be slow — a lazy load with no greenlet context
#      raises `MissingGreenlet`, and with `expire_on_commit=False` the instances are frequently
#      detached, so the failure is intermittent rather than immediate. Set `lazy="selectin"` to
#      avoid that and the eager load fires for every query that touches the parent, including
#      `{ orderEvents { id } }`, which asked for none of it. C11 batches these loads explicitly
#      through DataLoaders keyed on `order_id` / `user_id`, where the batching is provable with a
#      statement counter; a relationship would put a second, invisible path beside it.
#
#   3. THE JOIN IS NOT ALWAYS AN EQUALITY ON ONE ROW. "An order's payments" is every payment event
#      sharing its `order_id` — a stream, ordered and capped like every other list this API
#      publishes (spec §2 item 22). That capping and ordering lives in the statement builder, which
#      a relationship would bypass.
#
# What the tables DO carry is an index on every column those joins and C11's filters use. The
# trailing-`id` tiebreak on the composite indexes is the same decision argued at length on
# `LogEntryORM.__table_args__` above; it is not restated here.
# =================================================================================================


class OrderEventORM(Base):
    """One transition in an order's lifecycle — spec §3 Feature Area A ("order events carry status").

    Two identifiers rather than one: ``order_id`` is what the event is *about* and ``user_id`` is
    the modeled order -> user relationship, denormalised onto the row because there is no users
    table to join to (see the section comment above). Both are indexed, so C11 can traverse in
    either direction without a second lookup.
    """

    __tablename__ = "order_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    #: Same ``timestamptz`` contract as ``log_entries.timestamp``, for the same reason: a naive
    #: value compared against this column would be interpreted in the *server's* TimeZone.
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    service: Mapped[str] = mapped_column(String(SERVICE_MAX_LENGTH), nullable=False)
    level: Mapped[str] = mapped_column(String(LEVEL_MAX_LENGTH), nullable=False)

    #: Nullable, and deliberately so even though the seeded corpus always sets one. The published
    #: ``LogEvent.traceId`` is nullable because ``LogEntry.traceId`` is (60% of the log corpus
    #: carries a trace id, and C5's empty-list branch depends on the other 40%), and an interface
    #: field cannot be non-null on one implementor and null on another. A NOT NULL here would also
    #: refuse a perfectly ordinary event: one ingested from a producer with no tracing configured.
    trace_id: Mapped[str | None] = mapped_column(String(TRACE_ID_MAX_LENGTH), nullable=True)

    order_id: Mapped[str] = mapped_column(String(ORDER_ID_MAX_LENGTH), nullable=False)

    #: The order -> user edge. Denormalised onto every order event rather than reached through a
    #: join, because there is no ``users`` table and because "which user placed this order" must be
    #: answerable from the event alone — a subscriber receiving one status transition over
    #: ``orderStatusStream`` (C12) has no other row to consult.
    user_id: Mapped[str] = mapped_column(String(USER_ID_MAX_LENGTH), nullable=False)

    #: A plain ``String`` and NOT a PostgreSQL ``ENUM``, for exactly the reason
    #: :attr:`LogEntryORM.level` is one: a database-level enum makes adding a status an
    #: ``ALTER TYPE … ADD VALUE`` plus a coordinated deploy, to gain a constraint this system
    #: already enforces a layer earlier and far more usefully. ``src.graphql.enums.OrderStatus``
    #: rejects an unknown status during GraphQL *validation*, before a resolver runs, with a message
    #: naming every legal value. A constraint at the edge produces a good error message; the same
    #: constraint in the storage engine produces a driver exception and a masked 500.
    status: Mapped[str] = mapped_column(String(ORDER_STATUS_MAX_LENGTH), nullable=False)

    # See the `none_as_null=True` essay on `LogEntryORM.metadata_`: without the flag a Python
    # `None` is stored as the JSONB scalar `'null'` rather than SQL NULL, which reads back as
    # `None` either way and is therefore invisible from Python — but breaks `metadata IS NULL`,
    # `jsonb_typeof`, and every partial index or aggregation C11 builds over it. The attribute is
    # `metadata_` because `metadata` is reserved on a declarative class.
    metadata_: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", JSONB(none_as_null=True), nullable=True
    )

    __table_args__ = (
        # The default ordering and the keyset shape, exactly as on `log_entries`.
        Index("ix_order_events_ts_id", "timestamp", "id"),
        # The order -> its own history read, and the join C11 traverses from a payment event.
        Index("ix_order_events_order_ts", "order_id", "timestamp", "id"),
        # The order -> user edge, read in the "everything this user did" direction.
        Index("ix_order_events_user_ts", "user_id", "timestamp", "id"),
        # `OrderEventFilterInput.status`, and C12's `orderStatusStream(status:)` replay.
        Index("ix_order_events_status_ts", "status", "timestamp", "id"),
        # The correlation lookup `Query.correlatedEvents` issues against all four tables.
        Index("ix_order_events_trace_id", "trace_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"OrderEventORM(id={self.id!r}, order_id={self.order_id!r}, "
            f"status={self.status!r}, timestamp={self.timestamp!r})"
        )


class PaymentEventORM(Base):
    """One payment attempt event — "payment events carry method and outcome" (spec §3 Area A).

    ``order_id`` is the modeled order -> payments edge. There is deliberately no separate payment
    identifier: the primary key identifies the event, ``order_id`` identifies what it is about, and
    a third id would be 1:1 with the order in this corpus and therefore dead weight a reader would
    have to reason about.
    """

    __tablename__ = "payment_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(String(SERVICE_MAX_LENGTH), nullable=False)
    level: Mapped[str] = mapped_column(String(LEVEL_MAX_LENGTH), nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(TRACE_ID_MAX_LENGTH), nullable=True)

    #: The order -> payments edge. Several payment events share one ``order_id`` (a payment is
    #: authorized, then captured, then possibly refunded), which is why this is the *many* side and
    #: why the index below leads with it.
    order_id: Mapped[str] = mapped_column(String(ORDER_ID_MAX_LENGTH), nullable=False)

    #: How the customer paid. ``String`` + GraphQL enum, per the note on ``OrderEventORM.status``.
    method: Mapped[str] = mapped_column(String(PAYMENT_METHOD_MAX_LENGTH), nullable=False)

    #: What happened. The event's own verb — one payment (one ``order_id``) produces several rows
    #: differing only in ``outcome`` and ``timestamp``, which is what makes a payment a *stream*
    #: rather than a mutable record with a status column.
    outcome: Mapped[str] = mapped_column(String(PAYMENT_OUTCOME_MAX_LENGTH), nullable=False)

    metadata_: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", JSONB(none_as_null=True), nullable=True
    )

    __table_args__ = (
        Index("ix_payment_events_ts_id", "timestamp", "id"),
        # The order -> payments join. C11's cross-entity DataLoader batches this into a single
        # `WHERE order_id IN (…)`; without the index that batch is one sequential scan per
        # operation rather than one per parent — batched, and still O(table).
        Index("ix_payment_events_order_ts", "order_id", "timestamp", "id"),
        # `outcome` is this table's analogue of `status`: the dimension a dashboard filters on
        # ("show me declines"), and the one C11 aggregates by.
        Index("ix_payment_events_outcome_ts", "outcome", "timestamp", "id"),
        # `method` is published as a filter too, so it gets the same treatment. Its cardinality is
        # tiny (five values), so the planner will often prefer a scan at the seeded corpus size —
        # the index is here for the shape the filter has, not for a measured win at 1000 rows.
        Index("ix_payment_events_method_ts", "method", "timestamp", "id"),
        Index("ix_payment_events_trace_id", "trace_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"PaymentEventORM(id={self.id!r}, order_id={self.order_id!r}, "
            f"method={self.method!r}, outcome={self.outcome!r})"
        )


class UserEventORM(Base):
    """One user activity event — "user events carry activity type" (spec §3 Area A).

    The third stream, and the one that closes the correlation triangle: an order's events, its
    payment events and the acting user's activity all share a ``trace_id``, so a single
    ``correlatedEvents(traceId:)`` returns the whole session as a heterogeneous list.
    """

    __tablename__ = "user_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(String(SERVICE_MAX_LENGTH), nullable=False)
    level: Mapped[str] = mapped_column(String(LEVEL_MAX_LENGTH), nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(TRACE_ID_MAX_LENGTH), nullable=True)

    user_id: Mapped[str] = mapped_column(String(USER_ID_MAX_LENGTH), nullable=False)

    #: What the user did. Named ``activity_type`` rather than ``activity`` because the spec names
    #: it that way and because the published field is ``activityType`` — the two must agree, or the
    #: SDL and the requirement stop being checkable against each other.
    activity_type: Mapped[str] = mapped_column(String(USER_ACTIVITY_MAX_LENGTH), nullable=False)

    metadata_: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", JSONB(none_as_null=True), nullable=True
    )

    __table_args__ = (
        Index("ix_user_events_ts_id", "timestamp", "id"),
        # The order -> user edge, arrived at from the order side: given `OrderEventORM.user_id`,
        # "what else was this user doing". C11 batches it as `WHERE user_id IN (…)`.
        Index("ix_user_events_user_ts", "user_id", "timestamp", "id"),
        Index("ix_user_events_activity_ts", "activity_type", "timestamp", "id"),
        Index("ix_user_events_trace_id", "trace_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"UserEventORM(id={self.id!r}, user_id={self.user_id!r}, "
            f"activity_type={self.activity_type!r})"
        )


# =================================================================================================
# The identity-free value objects for the three event streams.
#
# Same argument as `LogRecord` above, unchanged: ORM instances compare by identity, so
# `generate_event_corpus(...) == generate_event_corpus(...)` would be False for two byte-identical
# corpora and the determinism test could not fail for the right reason. These compare by value.
#
# `as_insert_params()` keys by DATABASE COLUMN NAME (so `metadata`, never `metadata_`) because
# seeding uses a Core multi-row INSERT against `Table.c`, and omits `id` so BIGSERIAL assigns ids in
# insert order — which is timestamp order, which is what makes `ORDER BY timestamp DESC, id DESC`
# exactly the reverse of the generated list.
# =================================================================================================


@dataclass(frozen=True, slots=True)
class OrderEventRecord:
    """One generated order-lifecycle event, with no identity."""

    timestamp: datetime
    service: str
    level: str
    trace_id: str | None
    order_id: str
    user_id: str
    status: str
    metadata: dict[str, Any] | None

    def as_insert_params(self) -> dict[str, Any]:
        """This record keyed by database column name, ready for a Core INSERT."""
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "level": self.level,
            "trace_id": self.trace_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "status": self.status,
            "metadata": self.metadata,
        }

    @classmethod
    def from_orm_row(cls, row: OrderEventORM) -> OrderEventRecord:
        """Project a stored row back onto the value object the oracle is made of."""
        return cls(
            timestamp=row.timestamp,
            service=row.service,
            level=row.level,
            trace_id=row.trace_id,
            order_id=row.order_id,
            user_id=row.user_id,
            status=row.status,
            metadata=row.metadata_,
        )


@dataclass(frozen=True, slots=True)
class PaymentEventRecord:
    """One generated payment event, with no identity."""

    timestamp: datetime
    service: str
    level: str
    trace_id: str | None
    order_id: str
    method: str
    outcome: str
    metadata: dict[str, Any] | None

    def as_insert_params(self) -> dict[str, Any]:
        """This record keyed by database column name, ready for a Core INSERT."""
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "level": self.level,
            "trace_id": self.trace_id,
            "order_id": self.order_id,
            "method": self.method,
            "outcome": self.outcome,
            "metadata": self.metadata,
        }

    @classmethod
    def from_orm_row(cls, row: PaymentEventORM) -> PaymentEventRecord:
        """Project a stored row back onto the value object the oracle is made of."""
        return cls(
            timestamp=row.timestamp,
            service=row.service,
            level=row.level,
            trace_id=row.trace_id,
            order_id=row.order_id,
            method=row.method,
            outcome=row.outcome,
            metadata=row.metadata_,
        )


@dataclass(frozen=True, slots=True)
class UserEventRecord:
    """One generated user activity event, with no identity."""

    timestamp: datetime
    service: str
    level: str
    trace_id: str | None
    user_id: str
    activity_type: str
    metadata: dict[str, Any] | None

    def as_insert_params(self) -> dict[str, Any]:
        """This record keyed by database column name, ready for a Core INSERT."""
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "level": self.level,
            "trace_id": self.trace_id,
            "user_id": self.user_id,
            "activity_type": self.activity_type,
            "metadata": self.metadata,
        }

    @classmethod
    def from_orm_row(cls, row: UserEventORM) -> UserEventRecord:
        """Project a stored row back onto the value object the oracle is made of."""
        return cls(
            timestamp=row.timestamp,
            service=row.service,
            level=row.level,
            trace_id=row.trace_id,
            user_id=row.user_id,
            activity_type=row.activity_type,
            metadata=row.metadata_,
        )

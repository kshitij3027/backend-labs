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

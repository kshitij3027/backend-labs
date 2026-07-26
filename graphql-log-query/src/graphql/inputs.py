"""``LogFilterInput`` and the one function that turns it into C2's :class:`LogQuery`.

.. rubric:: Every field is optional, and that is a requirement rather than a convenience

Spec §2 item 19: *"omitted filters are ignored"*. Two things have to be true for that to hold, and
they are easy to satisfy separately and get wrong together:

1. **Omitted means "no filter".** Every field defaults to ``None`` and
   :func:`src.db.repository.build_predicates` contributes a WHERE clause only for values that are
   ``is not None``. Nothing is defaulted to a value that would narrow the result.
2. **An explicitly-supplied ``null`` means the same thing.** A GraphQL client that builds a filter
   object from a form will send ``{"service": null}`` for an empty box rather than omitting the
   key, and a variables payload built with ``json.dumps`` on a partially-filled dict does the same.
   With ``= None`` defaults both cases arrive at the resolver as the identical Python ``None``, so
   they *cannot* diverge — there is no third state to mishandle. (Strawberry's ``UNSET`` sentinel
   would distinguish them, which is useful for a partial-update mutation where "set this to null"
   and "leave it alone" are different intentions. For a filter they are the same intention, and
   introducing a distinction the domain does not have only creates a branch to get wrong.)

The integration suite pins this: omitted, ``filters: null``, and a filter object whose every field
is explicitly ``null`` all return the identical result set.

.. rubric:: The limit is resolved here and clamped somewhere else, on purpose

:meth:`LogFilterInput.to_log_query` resolves an omitted ``limit`` to ``DEFAULT_QUERY_LIMIT`` so the
:class:`~src.db.repository.LogQuery` this produces is fully explicit about what it is asking for.
It does **not** clamp to ``MAX_QUERY_LIMIT``. That clamp lives in
:func:`src.db.repository.clamp_limit`, inside the statement builder, and it stays there because the
spec (§2 item 22) requires the cap on *every* query path — the resolver, the connection resolver,
C5's DataLoader, C7's cache warm path and the C12 E2E script. A clamp applied at the GraphQL edge
protects only the callers that come through the GraphQL edge, which is a property that quietly
stops being true the first time something else calls the repository. Applying it in the one
function that constructs the statement makes "every path is capped" structurally true.

(The two are not redundant with each other: ``clamp_limit(None, settings)`` would also resolve to
``DEFAULT_QUERY_LIMIT``, so resolving here changes no behaviour. It is done anyway because a
``LogQuery`` that leaves ``limit`` as ``None`` carries less information than one that says what it
wants, and both read the same ``settings.default_query_limit`` so the two cannot disagree.)

.. rubric:: Validation happens in the conversion, and that placement is the requirement

Spec §2 item 34 asks for validation on **all** filter and mutation inputs.
:meth:`LogFilterInput.to_log_query` is the single conversion every read path performs — ``logs``,
``logsConnection``, and whatever C7's cache warm path turns out to be — so calling
:func:`src.graphql.validation.validate_log_filter` from inside it makes "the filters were checked"
a structural property rather than a line each resolver has to remember. A resolver added later
cannot forget it, because a resolver that skipped it would have no ``LogQuery`` to run.

It does mean a *conversion* function raises, which is worth stating out loud rather than
discovering. The alternative — validating in each resolver — puts the guarantee back in the hands
of whoever writes the next one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import strawberry
from strawberry.scalars import JSON

from src.config import Settings
from src.db.repository import LogQuery
from src.graphql.enums import LogLevel
from src.graphql.validation import validate_log_filter


@strawberry.input
class LogFilterInput:
    """The spec's §2 item 18 filter set. All six fields, all optional.

    Field names are published camel-cased (``startTime``, ``endTime``, ``searchText``) — see the
    naming note in :mod:`src.graphql.types`.
    """

    service: Optional[str] = None
    level: Optional[LogLevel] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    search_text: Optional[str] = None
    limit: Optional[int] = None

    def to_log_query(self, settings: Settings) -> LogQuery:
        """Validate this input, then map it onto the request object the repository understands.

        The only interesting conversion is ``level``: Strawberry hands the resolver a
        :class:`~src.graphql.enums.LogLevel` **member**, and the ``level`` column holds the
        member's ``value`` (the two are identical strings, pinned by
        :func:`src.graphql.enums._assert_levels_match_the_corpus`). Passing the member straight
        through would compare an ``Enum`` against a ``VARCHAR`` — asyncpg would reject it, and a
        driver rejection surfaces to the client as an opaque internal error rather than as the
        clean answer this conversion produces.

        Raises:
            src.graphql.errors.ValidationError: If any supplied filter breaks a rule in
                :mod:`src.graphql.validation` — an over-long or blank ``service``, an over-long
                ``searchText``, a NUL byte, or a ``startTime`` after its ``endTime``. Carries
                ``extensions.code = "VALIDATION_ERROR"`` and reaches the client as a normal errors
                envelope.
        """
        # Before the mapping, not after: a value that fails here must never reach a statement
        # builder, and validating the LogQuery instead would lose which GraphQL field to name.
        validate_log_filter(self)

        return LogQuery(
            service=self.service,
            level=self.level.value if self.level is not None else None,
            start_time=self.start_time,
            end_time=self.end_time,
            search_text=self.search_text,
            # Resolved, not clamped. See the module docstring.
            limit=self.limit if self.limit is not None else settings.default_query_limit,
        )


@strawberry.input
class CreateLogInput:
    """The ``createLog`` payload — spec §2 item 24, published as ``logData``.

    Three required fields and three optional ones, and the split is the domain's rather than a
    convenience: a log line without a source, a severity or a message is not a log line, while a
    timestamp, a metadata object and a correlation id are all things a real emitter legitimately
    does not have.

    ``level`` is the :class:`~src.graphql.enums.LogLevel` **enum**, so ``level: "EROR"`` is
    rejected during validation with a message naming the five legal values — before a resolver
    runs, before a session is opened. That is the same guarantee ``LogFilterInput`` gets, applied
    to the write path, and it is why nothing in :mod:`src.graphql.validation` checks ``level``.

    ``timestamp`` omitted means **now, server-side**. Not "now, client-side": a client's clock is
    not something this server can vouch for, and the C6 subscription stream orders by this column.
    The default is applied in :meth:`src.db.repository.LogRepository.insert_log`, which is the one
    place in the project allowed to read the wall clock for a stored row.

    ``metadata`` is a ``JSON`` scalar — untyped on the wire — so
    :func:`src.graphql.validation.validate_metadata` is what enforces that it is an *object* of
    bounded depth and size. Omitted, it is stored as SQL ``NULL`` rather than the JSONB scalar
    ``'null'``; see the ``none_as_null`` note on :class:`src.db.models.LogEntryORM`.
    """

    service: str
    level: LogLevel
    message: str
    timestamp: Optional[datetime] = None
    metadata: Optional[JSON] = None
    trace_id: Optional[str] = None


def to_log_query(filters: Optional[LogFilterInput], settings: Settings) -> LogQuery:
    """``LogFilterInput | None`` -> :class:`LogQuery`, with ``None`` meaning "no filters at all".

    Exists so that every resolver spells the "the client sent no ``filters`` argument" case the
    same way. ``filters: null`` and an omitted ``filters`` argument both arrive here as ``None``
    and both produce an unfiltered query capped at ``DEFAULT_QUERY_LIMIT`` — which is precisely
    the spec's "omitted filters are ignored", applied one level up from the individual fields.

    Nothing to validate in the ``None`` branch: "no filters" cannot break a rule.
    """
    if filters is None:
        return LogQuery(limit=settings.default_query_limit)
    return filters.to_log_query(settings)

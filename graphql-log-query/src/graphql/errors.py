"""The error taxonomy: one machine-readable ``extensions.code`` per failure kind, and the masking.

Spec §2 item 35: *errors return GraphQL-shaped error responses rather than raw stack traces or
HTTP 500s*. That is two separate promises, and they pull in opposite directions:

* A client must be able to **act** on a failure. ``"an unexpected error occurred"`` for a mistyped
  filter is unusable — the client cannot tell a bug in its own request from an outage, so it
  retries forever or gives up on data it was entitled to.
* A client must never learn how the server is **built**. An exception message, a type name or a
  ``/app/src/db/repository.py`` in a traceback is a free map of the internals.

The line between them is the whole design of this module: an error we *raised on purpose* keeps its
message and gains a code; an exception that *escaped* is replaced wholesale.

.. rubric:: The classification, and why it is not "does it have an ``original_error``?"

:func:`is_expected_error` sorts every error in a result into expected (pass through) and unexpected
(mask). One gate, then three accepting cases:

0. **Stamped** ``INTERNAL_ERROR`` **already.** Never expected, whatever else is true of it. That
   code is the only one this module *invents* (see :class:`MaskInternalErrors`), so an error
   wearing it has already been through masking, and reclassifying it as expected would un-mask it.
   The gate is checked **first** so that no accepting case below can reach such an error at all —
   keeping ``INTERNAL_ERROR`` out of :data:`EXPECTED_CODES` is necessary but demonstrably not
   sufficient, because case 3 does not consult the codes.
1. **A** :class:`DomainError` **we raised.** Deliberate, client-facing, already carries its code.
   Looked for anywhere beneath the error, not just one level down: graphql-core wraps what a
   resolver raises, and nothing promises it wraps it exactly once.
2. **An error carrying one of our codes.** The same thing seen after graphql-core re-wrapped it
   (see below), or an error minted by a later commit's extension — C8's cost gate reports through
   a ``ValidationRule``, which constructs a ``GraphQLError`` rather than raising ours.
3. **An error with nothing but** ``GraphQLError`` **all the way down.** graphql-core
   *manufactured* it while reading the **client's own document**: a parse failure, a validation
   failure, an input-coercion failure. No server exception is involved anywhere in the chain and
   the message describes the request (``Value 'EROR' does not exist in 'LogLevel' enum.``).
   Masking these is the single most common way a ``MaskErrors`` install makes an API unusable —
   every typo becomes "an unexpected error occurred" and the playground stops being able to teach
   anybody the schema.

Everything else — anything with a non-``GraphQLError`` exception under it — escaped our code, and
gets masked.

.. rubric:: The literal path and the variable path do NOT look alike, and assuming so cost a bug

An earlier revision of case 3 read ``error.original_error is None``, on the premise that
graphql-core attaches no cause to a coercion failure. That premise holds for **literals** and
fails for **variables** — which is the half every real client uses, so the wrong half was the one
under test:

* ``{ logs(filters: {level: NOT_A_LEVEL}) { id } }`` is rejected during validation by
  ``ValuesOfCorrectTypeRule``, which catches the enum's own ``GraphQLError`` and reports **that
  object**. What reaches the classification therefore has ``original_error is None``.
* The same filter sent as ``{"level": "NOT_A_LEVEL"}`` is rejected by ``coerce_variable_values``,
  which builds a **new** ``GraphQLError`` — ``Variable '$filters' got invalid value 'NOT_A_LEVEL'
  at 'filters.level'; Value 'NOT_A_LEVEL' does not exist in 'LogLevel' enum.`` — whose
  ``original_error`` is the inner ``GraphQLError`` it caught.

One mistake, two shapes, and the old rule classified them oppositely: the literal came back with a
usable message while the variable came back as "an unexpected internal error occurred" with a
25-line traceback on ``strawberry.execution``. So the tell is not whether there is a cause but
**what type** the cause is, and the depth is not fixed at one — a scalar interposes an
``Expected type 'X'.`` wrapper — which is why the classification walks the chain to the bottom
rather than peeking one level down.

The conservative edge of that rule, stated so it is a decision rather than a surprise: if a scalar's
``parse_value`` let a plain Python exception escape, that exception would sit at the bottom of the
chain and the mistake would be masked. Measured against the pinned Strawberry (0.324.0), that does
**not** happen for the scalars this schema uses — ``strawberry/schema/types/base_scalars.py``'s
``wrap_parser`` raises ``GraphQLError(...) from None``, so ``startTime: "not-a-date"`` has no cause
at all, the chain is all-``GraphQLError``, and the client gets a readable message on both the
literal and the variable path. The edge is real but currently unreached; it is written down because
a scalar added later that raises through would fall off it silently.

Related, and a deliberate acceptance rather than an oversight: that readable message embeds the
underlying parser's text (``invalid literal for int() with base 10: b'not-'``) because Strawberry's
``wrap_parser`` defaults to ``include_error=True``. No path, class name, or traceback escapes — but
it is a lower-level detail than the rest of this surface emits, and it is kept only because the
alternative is a scalar-level override that would make the message less actionable, not more.

.. rubric:: Why raising a ``GraphQLError`` subclass is enough to carry the code

graphql-core wraps whatever a resolver raises in :func:`graphql.located_error`, which builds a
**new** ``GraphQLError`` carrying the field ``path``. That constructor copies ``extensions`` off
``original_error`` when it has none of its own, so a code set here survives the wrapping and reaches
the wire. It also means the error a later stage inspects is usually *not* our instance but a plain
``GraphQLError`` whose chain contains ours — hence case 1 searching the whole chain rather than
testing the object it was handed, and hence case 2 above.

.. rubric:: The logging half, which is the part a load test notices

Strawberry logs every error through ``Schema.process_errors`` ->
``StrawberryLogger.error(message, exc_info=error.original_error or error)``. ``exc_info`` means a
**full stack trace at ERROR level**, and it fires for handled client mistakes too: a bad enum, a
malformed cursor, a non-numeric id. Each of those already returns a correct 200 ``errors`` envelope
— the response is fine, the *log* is the problem. Under the C14 load harness that is thousands of
tracebacks, and every one of them prints internal file paths into a log an operator may ship
somewhere.

The fix is a ``process_errors`` override on the schema (see :mod:`src.graphql.schema`) that routes
expected errors to :func:`log_expected_error` — one INFO line, no ``exc_info``, no paths — and
hands everything else to Strawberry unchanged, so a genuine fault still gets its full trace on the
``strawberry.execution`` logger. Silencing that logger wholesale would have "fixed" the noise by
also hiding every real crash.

.. rubric:: Ordering: ``process_errors`` runs BEFORE the extensions

Strawberry calls ``process_errors`` on the raw errors specifically so a server can log the truth
while returning something sanitised (its own source says so, citing ``MaskErrors``). This module
relies on that ordering: the classification above only works while ``original_error`` and the
original ``extensions`` are still attached, and :class:`MaskInternalErrors` destroys both.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from enum import Enum
from typing import Any, ClassVar

from graphql import GraphQLError
from strawberry.extensions import MaskErrors

#: Errors are logged under **this module's** logger, not ``strawberry.execution``. That separation
#: is the point: an operator can silence expected client mistakes (``src.graphql.errors``) without
#: touching the logger that carries genuine faults.
logger = logging.getLogger(__name__)

#: What a masked error says on the wire. Deliberately says nothing: no exception type, no module,
#: no hint about which resolver failed. The correlation between this and the real cause is the
#: server log, which carries the full trace under ``strawberry.execution``.
MASKED_ERROR_MESSAGE = "an unexpected internal error occurred"


class ErrorCode(Enum):
    """The published ``extensions.code`` vocabulary. One value per *kind* of failure.

    A code is a contract with a client's error handling: ``VALIDATION_ERROR`` means "fix the
    request and it will work", ``INTERNAL_ERROR`` means "retry or report it", and the difference
    decides whether a dashboard shows a form error or a toast. Message text is for humans and is
    free to change; these strings are not.
    """

    #: An input the client supplied cannot be accepted. See :mod:`src.graphql.validation`.
    VALIDATION_ERROR = "VALIDATION_ERROR"
    #: A named entity does not exist, **and its absence is exceptional**. Deliberately unused in
    #: C4: ``Query.log(id:)`` returns ``null`` for a miss because "is there a row with this id" is
    #: an ordinary question with an ordinary answer (see its resolver). The code exists for the
    #: lookups where absence really is a failure — C11's ``order(id:)``, where every other field of
    #: the response is built from an order that turned out not to be there.
    NOT_FOUND = "NOT_FOUND"
    #: An ``after`` cursor could not be decoded. Emitted by ``Query.logsConnection`` since C3.
    INVALID_CURSOR = "INVALID_CURSOR"
    #: C8: the operation's computed complexity or depth exceeds the configured budget. Rejected in
    #: validation, before any resolver runs; the rejection carries the computed cost and the limit
    #: alongside this code so a client can shrink deliberately rather than guess.
    COST_LIMIT_EXCEEDED = "COST_LIMIT_EXCEEDED"
    #: C9: an automatic-persisted-query hash arrived with no document and nothing cached under it.
    #: The client's correct response is to resend with the document attached, which is why this is
    #: a distinct code and not a generic validation failure.
    PERSISTED_QUERY_NOT_FOUND = "PERSISTED_QUERY_NOT_FOUND"
    #: Something escaped. The only code a client can never act on, and the only one this module
    #: ever *invents* — see :class:`MaskInternalErrors`.
    INTERNAL_ERROR = "INTERNAL_ERROR"


#: Every code except ``INTERNAL_ERROR``. An error carrying one of these was raised on purpose, so
#: its message is safe and useful; ``INTERNAL_ERROR`` is excluded because it is what masking
#: *produces*, and treating it as expected would let an already-masked error skip the check on a
#: second pass. Excluding it here is only half the guard — :func:`is_expected_error` also rejects
#: it outright, because the cases that do not consult a code at all would otherwise accept it.
EXPECTED_CODES: frozenset[str] = frozenset(
    code.value for code in ErrorCode if code is not ErrorCode.INTERNAL_ERROR
)


class DomainError(GraphQLError):
    """Base for every error this server raises on purpose. Never raised directly.

    Subclasses set :attr:`code` and inherit the whole mechanism: the code lands in ``extensions``,
    the message reaches the client intact, :func:`is_expected_error` recognises it, and neither
    :class:`MaskInternalErrors` nor the ``process_errors`` override treats it as a fault.

    Subclassing :class:`~graphql.GraphQLError` rather than wrapping a plain exception at the
    resolver boundary is what makes that automatic. A ``ValueError`` raised in a helper would have
    to be caught and translated by every caller, and the one caller that forgot would produce a
    masked error for a problem the client could have fixed itself — a bug that is invisible in a
    test asserting only that "an error came back".
    """

    #: Set by every concrete subclass. Deliberately has no default: a subclass that forgot it
    #: fails loudly with an ``AttributeError`` at raise time rather than quietly inheriting a code
    #: that misdescribes it.
    code: ClassVar[ErrorCode]

    def __init__(
        self,
        message: str,
        *,
        extensions: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Build the error with ``extensions.code`` already populated.

        Args:
            message: The client-facing text. It **will** be shown, so it must name what is wrong
                and, where possible, what to do instead — never an internal identifier.
            extensions: Extra machine-readable detail merged alongside the code (C8 attaches
                ``computedCost``/``maxCost`` this way). It cannot overwrite ``code``... except by
                explicitly passing one, which is the caller taking responsibility.
            **kwargs: Passed through to :class:`~graphql.GraphQLError` (``nodes``, ``path``, ...).
        """
        merged: dict[str, Any] = {"code": self.code.value}
        if extensions:
            merged.update(extensions)
        super().__init__(message, extensions=merged, **kwargs)


class ValidationError(DomainError):
    """An input failed a rule in :mod:`src.graphql.validation`."""

    code = ErrorCode.VALIDATION_ERROR


class NotFoundError(DomainError):
    """A named entity does not exist and its absence is exceptional. See :attr:`ErrorCode.NOT_FOUND`."""

    code = ErrorCode.NOT_FOUND


class InvalidCursorError(DomainError):
    """A pagination cursor could not be decoded.

    Distinct from :class:`src.graphql.cursor.InvalidCursorError`, which is a plain ``ValueError``
    so that the cursor codec stays free of the GraphQL layer and unit-testable as pure logic. The
    resolver catches that one and raises this one; the translation is the boundary, and this is
    the only class of the two that a client ever sees.
    """

    code = ErrorCode.INVALID_CURSOR


class CostLimitExceededError(DomainError):
    """C8: the operation is over the depth/complexity budget."""

    code = ErrorCode.COST_LIMIT_EXCEEDED


class PersistedQueryNotFoundError(DomainError):
    """C9: a persisted-query hash arrived with no document and nothing cached under it."""

    code = ErrorCode.PERSISTED_QUERY_NOT_FOUND


def error_code(error: GraphQLError) -> str | None:
    """Return the ``extensions.code`` on ``error``, or ``None`` when it carries no code."""
    extensions = getattr(error, "extensions", None)
    if not isinstance(extensions, dict):
        return None
    code = extensions.get("code")
    return code if isinstance(code, str) else None


def _cause_chain(error: GraphQLError) -> Iterator[Exception]:
    """Yield ``error`` and every exception nested beneath it, outermost first.

    graphql-core nests, and the depth is not a constant. ``located_error`` wraps what a resolver
    raised; ``coerce_variable_values`` wraps the coercion error it caught; and for a scalar that
    coercion error itself wraps whatever ``parse_value`` raised. A classification that inspected
    only ``error.original_error`` would answer correctly for one of those and wrongly for the next,
    which is exactly the bug this replaced.

    The ``id`` guard is not distrust of graphql-core: ``original_error`` is an ordinary writable
    attribute, and a cycle in it would hang the classification inside a live request.
    """
    seen: set[int] = set()
    current: Exception | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = getattr(current, "original_error", None)


def is_expected_error(error: GraphQLError) -> bool:
    """Is this error something we produced on purpose, safe to show a client verbatim?

    The gate and the three accepting cases are spelled out in this module's docstring. **The order
    is load-bearing, not cosmetic**: the ``INTERNAL_ERROR`` gate has to precede case 3, because
    masking drops ``original_error`` and case 3 would otherwise read an already-masked error as
    "manufactured by graphql-core" and promote it back to expected.

    Returns:
        ``True`` when the error's message and code may be sent to the client unchanged; ``False``
        when an exception escaped our code and the whole error must be replaced.
    """
    code = error_code(error)

    # 0. Already masked (or claiming to be). Nothing below may overturn this.
    if code == ErrorCode.INTERNAL_ERROR.value:
        return False

    # 1. Ours, as raised or as graphql-core re-wrapped it — at whatever depth it ended up.
    if any(isinstance(cause, DomainError) for cause in _cause_chain(error)):
        return True

    # 2. Carries a code from the published taxonomy. Covers errors *constructed* rather than
    #    raised — C8's cost gate reports through a validation rule, which never raises.
    if code is not None and code in EXPECTED_CODES:
        return True

    # 3. Nothing but GraphQLErrors all the way down: graphql-core built this while parsing,
    #    validating or coercing the client's own document, and a GraphQLError is by construction
    #    a description of the request rather than of the server. Note this is a question about
    #    the chain, NOT about `original_error is None` — a bad *variable* arrives as a
    #    GraphQLError wrapping a GraphQLError, and treating that as unexpected masked the most
    #    common client mistake there is.
    return all(isinstance(cause, GraphQLError) for cause in _cause_chain(error))


def should_mask_error(error: GraphQLError) -> bool:
    """The predicate :class:`MaskInternalErrors` is configured with — the negation of the above."""
    return not is_expected_error(error)


def log_expected_error(error: GraphQLError) -> None:
    """Record a handled client-side failure as **one INFO line with no traceback**.

    This is the whole fix for the log spam described in the module docstring. What is deliberately
    absent: ``exc_info``. A bad cursor is not a server fault, and attaching the stack that produced
    it prints ``/app/src/graphql/cursor.py`` into the log once per malformed request.

    The message logged is the same text already sent to the client, so this adds no disclosure
    beyond what the response carried.
    """
    logger.info(
        "rejected operation: %s (code=%s, path=%s)",
        error.message,
        error_code(error) or "-",
        error.path,
    )


class MaskInternalErrors(MaskErrors):
    """``MaskErrors`` wired to :func:`should_mask_error`, plus an ``INTERNAL_ERROR`` code.

    Two deviations from the stock extension, both necessary:

    * **The predicate.** Strawberry's default masks *everything*, which would replace every
      validation message in the schema with "Unexpected error." and make the API impossible to
      develop against. Here only genuinely unexpected errors are replaced.
    * **The code.** ``MaskErrors.anonymise_error`` rebuilds the error without ``extensions``, so a
      masked error would arrive carrying no code at all — and a client cannot distinguish "the
      server broke" from "the server said something I do not understand". The override delegates
      the rebuild to the base class (so a future change to what it preserves is inherited) and
      only adds the code back.

    Installed **outermost** in the extension list, so nothing added later — metrics, caching, the
    cost gate — can put an unmasked error past it.

    Installed as the **class**, never as an instance: Strawberry's ``Schema.get_extensions`` calls
    ``ext()`` for every entry that is not already a ``SchemaExtension``, so the class form gets a
    fresh extension per request, while an instance is shared across concurrent requests *and*
    raises a ``DeprecationWarning`` since 0.323. That is the whole reason :meth:`__init__` takes no
    arguments — the configuration it would otherwise accept is fixed here instead, so the class
    itself is a valid zero-argument factory.
    """

    def __init__(self) -> None:
        super().__init__(
            should_mask_error=should_mask_error,
            error_message=MASKED_ERROR_MESSAGE,
        )

    def anonymise_error(self, error: GraphQLError) -> GraphQLError:
        """Replace ``error`` with the generic one, tagged :attr:`ErrorCode.INTERNAL_ERROR`."""
        masked = super().anonymise_error(error)
        existing = masked.extensions if isinstance(masked.extensions, dict) else {}
        masked.extensions = {**existing, "code": ErrorCode.INTERNAL_ERROR.value}
        return masked

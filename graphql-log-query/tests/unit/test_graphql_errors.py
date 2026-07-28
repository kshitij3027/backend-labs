"""The error taxonomy, the masking predicate, and the logging split — spec §2 item 35.

Pure: no database, no HTTP. The classification and the masking are ordinary functions over
``GraphQLError`` objects, so they are tested as ordinary functions; the integration suite then
proves the same behaviour end to end through the real transport, which is where "no traceback on
the wire" can actually be asserted against bytes.

.. rubric:: What these tests are guarding

Getting masking *approximately* right is worse than not having it. The two failure modes are
opposite and both are quiet:

* **Mask too much** and every validation message becomes "an unexpected internal error occurred".
  Nothing errors, no test that only checks ``result.errors`` notices, and the API becomes
  undebuggable for anybody who does not have the source open.
* **Mask too little** and an ``AttributeError`` from a resolver reaches a client with its message,
  its type and (through the log) a file path.

So the tests below assert both directions for every case, and the log tests assert on
``exc_info`` — the actual thing that turns a log line into a stack trace — rather than on the
presence of a record.

.. rubric:: And they assert on both COERCION PATHS, because an earlier revision did not

graphql-core rejects a bad enum in two entirely different places depending on how the client sent
it, and the two produce differently *shaped* errors: a literal is reported by a validation rule
with no ``original_error``, a variable is reported by ``coerce_variable_values`` as a fresh
``GraphQLError`` wrapping the one it caught. A classification keyed on ``original_error is None``
therefore passed every literal-only test in this file while masking the variable path — i.e. while
masking what every real client sends. The shape tests below pin both, plus a nested chain, so a
fix that only looks one level down cannot go green.
"""

from __future__ import annotations

import logging

import pytest
from graphql import GraphQLError

from src.config import Settings
from src.graphql.context import Context
from src.graphql.errors import (
    MASKED_ERROR_MESSAGE,
    CostLimitExceededError,
    DomainError,
    ErrorCode,
    InvalidCursorError,
    MaskInternalErrors,
    NotFoundError,
    PersistedQueryNotFoundError,
    SlowConsumerError,
    SubscriptionLimitError,
    ValidationError,
    error_code,
    is_expected_error,
    should_mask_error,
)
from src.graphql.schema import schema

DOMAIN_ERRORS = [
    (ValidationError, ErrorCode.VALIDATION_ERROR),
    (NotFoundError, ErrorCode.NOT_FOUND),
    (InvalidCursorError, ErrorCode.INVALID_CURSOR),
    (CostLimitExceededError, ErrorCode.COST_LIMIT_EXCEEDED),
    (PersistedQueryNotFoundError, ErrorCode.PERSISTED_QUERY_NOT_FOUND),
    (SlowConsumerError, ErrorCode.SLOW_CONSUMER),
    (SubscriptionLimitError, ErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED),
]


def _wrapped_like_graphql_core(raised: Exception, message: str | None = None) -> GraphQLError:
    """Rebuild ``raised`` the way :func:`graphql.located_error` does inside the executor.

    Nothing in the classification path ever sees the exception a resolver raised. graphql-core
    wraps it in a **new** ``GraphQLError`` carrying the field ``path``, and that wrapper is what
    reaches ``process_errors`` and the extensions. Testing the raw exception instead would leave
    the only shape that actually occurs untested — including the ``extensions``-inheritance the
    codes depend on.
    """
    return GraphQLError(
        message if message is not None else str(raised),
        original_error=raised,
        path=["logs", 0],
    )


#: The message graphql-core's enum coercion produces, identically on both paths — it is the inner
#: error on the variable path and the whole error on the literal path.
BAD_ENUM_MESSAGE = "Value 'NOT_A_LEVEL' does not exist in 'LogLevel' enum."


def _coerced_like_a_variable(inner: GraphQLError) -> GraphQLError:
    """Rebuild the shape ``coerce_variable_values`` produces for an invalid variable.

    graphql-core does **not** report the coercion error it caught. It builds a new
    ``GraphQLError`` naming the variable and the path inside it, and hangs the caught error
    underneath as ``original_error`` — an ``original_error`` that is itself a ``GraphQLError``.
    That distinction is invisible to a client and decisive to the classification, so it is
    reproduced here exactly rather than approximated with a message.
    """
    return GraphQLError(
        f"Variable '$filters' got invalid value 'NOT_A_LEVEL' at 'filters.level'; {inner.message}",
        original_error=inner,
    )


def _records(caplog: pytest.LogCaptureFixture, logger_name: str) -> list[logging.LogRecord]:
    """Captured records from one named logger. Which logger an error lands on IS the assertion."""
    return [record for record in caplog.records if record.name == logger_name]


# --- The taxonomy ---------------------------------------------------------------------------------


@pytest.mark.parametrize(("error_class", "code"), DOMAIN_ERRORS)
def test_every_domain_error_carries_its_code_in_extensions(
    error_class: type[DomainError], code: ErrorCode
) -> None:
    """The code is the machine-readable half of the contract; the message is for humans."""
    error = error_class("something specific went wrong")

    assert error.extensions["code"] == code.value
    assert error.message == "something specific went wrong"
    assert isinstance(error, GraphQLError), "it must be raisable straight out of a resolver"


def test_the_codes_are_distinct_and_include_the_placeholders_later_commits_need() -> None:
    """One value per failure *kind*, and the C8/C9 codes exist before their producers do.

    Defining them now is what stops C8 and C9 from each inventing their own spelling
    (``COST_EXCEEDED``? ``QUERY_TOO_COMPLEX``?) in a client-visible contract.

    ``SLOW_CONSUMER`` and ``SUBSCRIPTION_LIMIT_EXCEEDED`` arrived with C6, which is when the two
    failures they name became possible. They are pinned here for the reason every other code is:
    the strings are the contract a client branches on, and a rename is a silent break.
    """
    values = [code.value for code in ErrorCode]

    assert len(values) == len(set(values))
    assert {
        "VALIDATION_ERROR",
        "NOT_FOUND",
        "INVALID_CURSOR",
        "COST_LIMIT_EXCEEDED",
        "PERSISTED_QUERY_NOT_FOUND",
        "SLOW_CONSUMER",
        "SUBSCRIPTION_LIMIT_EXCEEDED",
        "INTERNAL_ERROR",
    } == set(values)


def test_extra_extensions_are_merged_alongside_the_code() -> None:
    """C8 attaches ``computedCost``/``maxCost`` so a client can shrink deliberately, not guess."""
    error = CostLimitExceededError(
        "operation costs 4200, limit is 1000",
        extensions={"computedCost": 4200, "maxCost": 1000},
    )

    assert error.extensions == {
        "code": "COST_LIMIT_EXCEEDED",
        "computedCost": 4200,
        "maxCost": 1000,
    }


def test_the_code_survives_the_wrapping_graphql_core_applies_to_a_resolver_exception() -> None:
    """This is the mechanism the whole taxonomy rests on, so it gets its own test.

    ``GraphQLError.__init__`` copies ``extensions`` off ``original_error`` when it has none of its
    own, which is why raising a ``GraphQLError`` subclass from a resolver is enough to put a code
    on the wire. If that ever stopped holding, every code would silently vanish from every response
    while the messages kept arriving — and no test asserting on messages would notice.
    """
    wrapped = _wrapped_like_graphql_core(ValidationError("bad id"))

    assert error_code(wrapped) == "VALIDATION_ERROR"
    assert wrapped.path == ["logs", 0]


def test_error_code_returns_none_for_an_error_that_carries_no_code() -> None:
    assert error_code(GraphQLError("plain")) is None


# --- Classification -------------------------------------------------------------------------------


@pytest.mark.parametrize(("error_class", "code"), DOMAIN_ERRORS)
def test_a_domain_error_is_expected_both_raw_and_wrapped(
    error_class: type[DomainError], code: ErrorCode
) -> None:
    """Recognised as ours whether it arrives as itself or as graphql-core's wrapper around it."""
    raw = error_class("nope")

    assert is_expected_error(raw)
    assert is_expected_error(_wrapped_like_graphql_core(raw))
    assert not should_mask_error(raw)


def test_an_error_carrying_a_taxonomy_code_is_expected_even_without_our_class() -> None:
    """C8's cost gate reports through a validation rule, which *constructs* rather than raises.

    A ``ValidationRule`` calls ``context.report_error(GraphQLError(...))``; there is no exception
    to be an instance of ours. Classifying on the code as well as the class is what keeps that
    rejection readable instead of masked.
    """
    reported = GraphQLError(
        "operation costs 4200, limit is 1000",
        extensions={"code": ErrorCode.COST_LIMIT_EXCEEDED.value},
    )

    assert is_expected_error(reported)


def test_a_graphql_core_validation_error_is_expected_and_must_never_be_masked() -> None:
    """The single most damaging way to configure ``MaskErrors``, pinned so it cannot regress.

    graphql-core builds these while reading the **client's own document**, and their messages
    ("Value 'NOT_A_LEVEL' does not exist in 'LogLevel' enum.") describe nothing but the request.
    Masking them turns every typo into "an unexpected internal error occurred" and makes the schema
    unlearnable from the playground.

    This is the shape a **literal** produces — a validation rule reports the enum's own error, so
    there is no ``original_error``. That is a fact about this one path and **not** a general
    property of coercion failures; the next test is the same mistake sent the way clients actually
    send it, and it does carry one.
    """
    validation_error = GraphQLError(BAD_ENUM_MESSAGE)

    assert validation_error.original_error is None
    assert is_expected_error(validation_error)
    assert not should_mask_error(validation_error)


def test_the_same_rejection_sent_as_a_variable_is_expected_too() -> None:
    """The regression this file could not previously see, pinned as a shape.

    The test above is the **literal** path, and it is the path a hand-written test naturally
    reaches for. Sent as a variable — which is what every real client does — the identical mistake
    arrives with an ``original_error``, because ``coerce_variable_values`` wraps the error it
    caught instead of reporting it. Classifying on "has no ``original_error``" therefore masked
    the common path while the rare one stayed readable: the API answered a typo with "an
    unexpected internal error occurred" and logged a stack trace for it.
    """
    error = _coerced_like_a_variable(GraphQLError(BAD_ENUM_MESSAGE))

    assert error.original_error is not None, "the premise the old rule was built on"
    assert isinstance(error.original_error, GraphQLError), "a GraphQLError, not a server fault"
    assert is_expected_error(error)
    assert not should_mask_error(error)
    assert "NOT_A_LEVEL" in error.message


def test_a_chain_of_graphql_errors_is_expected_however_deep_it_goes() -> None:
    """Depth is not fixed at one, so a fix that peeks one level down is not a fix.

    A scalar produces exactly this: the variable wrapper, over the ``Expected type 'X'.`` wrapper
    coercion adds, over the error the scalar itself reported. Nothing in that chain is a server
    fault, and the client needs the message.
    """
    deepest = GraphQLError(BAD_ENUM_MESSAGE)
    middle = GraphQLError(f"Expected type 'LogLevel'. {BAD_ENUM_MESSAGE}", original_error=deepest)
    outer = _coerced_like_a_variable(middle)

    assert is_expected_error(outer)
    assert not should_mask_error(outer)


def test_an_escaped_exception_is_masked_however_deeply_it_is_wrapped() -> None:
    """The other direction of the same rule: the type at the BOTTOM of the chain decides.

    A one-level-deep test would see a ``GraphQLError`` under this error and pass the
    ``RuntimeError`` — file path included — straight to the client. Two levels is not a
    hypothetical shape: a resolver that raises inside a scalar's serialisation, or any future
    extension that re-wraps an error, produces it.
    """
    escaped = RuntimeError("could not connect: /app/src/db/session.py:117")
    middle = GraphQLError("wrapped once", original_error=escaped)
    outer = GraphQLError("wrapped twice", original_error=middle)

    assert not is_expected_error(outer)
    assert should_mask_error(outer)


def test_a_domain_error_is_recognised_however_deeply_it_is_wrapped() -> None:
    """Case 1 searches the chain for the same reason case 3 walks it.

    ``extensions={}`` on the outer error suppresses the code-inheritance ``GraphQLError`` performs,
    so this can only pass by finding the :class:`ValidationError` itself — otherwise the assertion
    would be satisfied by the code check and prove nothing about the search.
    """
    inner = _wrapped_like_graphql_core(ValidationError("service must not be empty"))
    outer = GraphQLError("wrapped again", original_error=inner, extensions={})

    assert error_code(outer) is None, "no code to fall back on; the class is the only signal"
    assert is_expected_error(outer)


def test_an_escaped_exception_is_unexpected_and_is_masked() -> None:
    """The other direction: anything with a real exception behind it that is not ours."""
    escaped = _wrapped_like_graphql_core(
        RuntimeError("connection to /app/src/db/repository.py failed"),
        message="connection to /app/src/db/repository.py failed",
    )

    assert not is_expected_error(escaped)
    assert should_mask_error(escaped)


def test_an_already_masked_error_is_not_treated_as_expected() -> None:
    """``INTERNAL_ERROR`` is excluded from the expected set on purpose.

    It is what masking *produces*. Treating it as expected would mean a second pass over an
    already-masked result quietly reclassified it as safe — which matters the moment anything
    other than the mask extension inspects errors.

    Excluding the code from :data:`EXPECTED_CODES` is necessary and **not sufficient**, which is
    the trap this test exists to catch: masking also drops ``original_error``, so an accepting case
    that reasons about the cause rather than the code sees a bare ``GraphQLError`` and calls it a
    graphql-core rejection. The code has to be checked *before* any such case runs, and the second
    half of this test asserts that against the real extension's own output rather than a hand-built
    imitation of it.
    """
    masked = GraphQLError(MASKED_ERROR_MESSAGE, extensions={"code": "INTERNAL_ERROR"})

    assert not is_expected_error(masked)
    assert should_mask_error(masked)

    produced = MaskInternalErrors().anonymise_error(
        _wrapped_like_graphql_core(RuntimeError("boom"), message="boom")
    )

    assert produced.original_error is None, "masking drops the cause; that is the trap"
    assert not is_expected_error(produced)


# --- The mask extension ---------------------------------------------------------------------------


def test_masking_replaces_the_message_and_stamps_internal_error() -> None:
    """The masked error says nothing about the cause and carries a code a client can branch on."""
    extension = MaskInternalErrors()
    escaped = _wrapped_like_graphql_core(
        RuntimeError("secret detail"), message="secret detail"
    )

    masked = extension.anonymise_error(escaped)

    assert masked.message == MASKED_ERROR_MESSAGE
    assert masked.extensions["code"] == ErrorCode.INTERNAL_ERROR.value
    assert masked.original_error is None, "the cause must not travel with the masked error"
    assert "secret" not in masked.message
    # The path is preserved: it names which field failed, which is structural information the
    # client already has from its own document and which it needs to render a partial result.
    assert masked.path == ["logs", 0]


def test_the_extension_is_configured_with_our_predicate_not_the_default() -> None:
    """Strawberry's default masks everything. Using it here would be the failure described above."""
    extension = MaskInternalErrors()

    assert extension.should_mask_error(
        _wrapped_like_graphql_core(RuntimeError("boom"), message="boom")
    )
    assert not extension.should_mask_error(ValidationError("service must not be empty"))
    assert extension.error_message == MASKED_ERROR_MESSAGE


# --- Wired into the real schema, through a real execution -----------------------------------------
#
# A correct extension nobody installed protects nothing, and asserting on `schema.extensions` would
# only prove the list contains an object. These two run actual operations against the actual schema
# — no database needed, because both failures happen before a session would be opened.


class _ExplodingSessionFactory:
    """A session factory that fails the way a lost database does: on the way to a connection."""

    def __call__(self) -> object:
        raise RuntimeError("could not connect: /app/src/db/session.py:117")


def _context() -> Context:
    """A GraphQL context whose database cannot be reached."""
    return Context(
        settings=Settings(_env_file=None, seed_entries=0, log_level="WARNING"),
        session_factory=_ExplodingSessionFactory(),  # type: ignore[arg-type]
    )


async def test_an_escaped_exception_is_masked_through_the_real_schema(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End to end through ``schema.execute``: nothing about the cause survives into the result.

    The exception message deliberately contains a file path, because that is what actually leaks
    when masking is missing or misconfigured — and the assertion is on its **absence**, which is
    the only way this test can fail for the right reason.
    """
    caplog.set_level(logging.DEBUG)

    result = await schema.execute("{ logs { id } }", context_value=_context())

    assert result.errors, "the resolver raised, so there must be an error"
    error = result.errors[0]
    assert error.message == MASKED_ERROR_MESSAGE
    assert error.extensions["code"] == ErrorCode.INTERNAL_ERROR.value
    assert "/app/" not in error.message
    assert "RuntimeError" not in error.message

    # ...and the real cause is not lost, it is on the server's crash logger with its trace.
    crash = _records(caplog, "strawberry.execution")
    assert crash, "masking must not swallow the fault; the server still needs to know"
    assert crash[0].exc_info is not None
    assert "could not connect" not in error.message, "the cause stays on the server side"


async def test_a_validation_error_keeps_its_message_through_the_real_schema(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The other direction, and the reason the predicate exists at all.

    A blank ``service`` filter is rejected by :mod:`src.graphql.validation` *before* a session is
    opened — which this context proves, since opening one would raise something entirely
    different. The client must get the real message and the ``VALIDATION_ERROR`` code, not the
    generic mask.
    """
    caplog.set_level(logging.DEBUG)

    result = await schema.execute(
        '{ logs(filters: {service: ""}) { id } }', context_value=_context()
    )

    assert result.errors
    error = result.errors[0]
    assert error.extensions["code"] == ErrorCode.VALIDATION_ERROR.value
    assert error.message != MASKED_ERROR_MESSAGE
    assert "service" in error.message
    assert "could not connect" not in error.message, (
        "validation must run before anything touches the database"
    )
    assert _records(caplog, "strawberry.execution") == []


async def test_a_bad_enum_sent_as_a_variable_survives_the_real_schema_intact(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The end-to-end form of the shape tests above — both symptoms in one execution.

    Every other test in this file that touches an invalid enum builds the error by hand or writes
    it as a literal. This one goes through ``schema.execute`` with a real variable, which is the
    only way to get graphql-core's own ``coerce_variable_values`` to construct the error, and
    therefore the only way to notice that its shape differs from the literal one.

    Two assertions, one per symptom the old classification produced here:

    * **On the wire** — the message still names ``NOT_A_LEVEL`` instead of the generic mask, so the
      client can fix its request.
    * **In the log** — one INFO line on our own logger and *nothing* on ``strawberry.execution``.
      A masked client mistake does not merely read badly in the response; it takes the crash branch
      and prints a full traceback per request, which is what the C3 logging split exists to stop.

    No database is involved: coercion fails before any resolver runs, which the exploding session
    factory would prove loudly if it did not.
    """
    caplog.set_level(logging.DEBUG)

    result = await schema.execute(
        "query Logs($filters: LogFilterInput) { logs(filters: $filters) { id } }",
        variable_values={"filters": {"level": "NOT_A_LEVEL"}},
        context_value=_context(),
    )

    assert result.errors, "an unknown enum member must be rejected"
    assert result.data is None, "coercion failed, so execution must not have started"
    error = result.errors[0]
    assert error.message != MASKED_ERROR_MESSAGE
    assert "NOT_A_LEVEL" in error.message
    assert "LogLevel" in error.message
    assert error_code(error) != ErrorCode.INTERNAL_ERROR.value

    assert _records(caplog, "strawberry.execution") == [], (
        "a bad enum is a client mistake; routing it to the crash logger prints a traceback per "
        "request and undoes the C3 split"
    )
    ours = _records(caplog, "src.graphql.errors")
    assert len(ours) == 1
    assert ours[0].levelno == logging.INFO
    assert ours[0].exc_info is None


# --- The logging split ----------------------------------------------------------------------------
#
# This is the C3 verifier's finding: Strawberry logs EVERY error at ERROR level with
# `exc_info=error.original_error or error`, i.e. a full stack trace — including for handled client
# mistakes, whose traces name internal files. The assertions are on `exc_info` specifically,
# because a log line is not the problem; the traceback attached to it is.


def test_a_handled_client_error_is_logged_without_a_stack_trace(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One INFO line on our own logger, no ``exc_info``, nothing on ``strawberry.execution``."""
    caplog.set_level(logging.DEBUG)

    schema.process_errors([_wrapped_like_graphql_core(InvalidCursorError("cursor is not base64"))])

    assert _records(caplog, "strawberry.execution") == [], (
        "a malformed cursor is a client mistake; routing it to the crash logger is what produced "
        "thousands of tracebacks under load"
    )

    ours = _records(caplog, "src.graphql.errors")
    assert len(ours) == 1
    assert ours[0].levelno == logging.INFO
    assert ours[0].exc_info is None, "no traceback, so no internal file paths in the log"
    assert "cursor is not base64" in ours[0].getMessage()
    assert "INVALID_CURSOR" in ours[0].getMessage()


def test_a_graphql_validation_error_is_also_logged_quietly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The bad-enum case from the C3 report — no ``original_error``, still a client mistake."""
    caplog.set_level(logging.DEBUG)

    schema.process_errors([GraphQLError('Value "NOT_A_LEVEL" does not exist in "LogLevel" enum.')])

    assert _records(caplog, "strawberry.execution") == []
    assert len(_records(caplog, "src.graphql.errors")) == 1


def test_an_unexpected_error_still_gets_the_full_trace_on_strawberrys_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The half that must NOT be lost.

    Silencing ``strawberry.execution`` would have removed the log spam and every genuine crash
    report with it. This asserts the crash path is untouched: same logger, ERROR level, and
    ``exc_info`` carrying the real exception so the trace is printed.
    """
    caplog.set_level(logging.DEBUG)
    try:
        raise RuntimeError("the database went away")
    except RuntimeError as exc:
        escaped = _wrapped_like_graphql_core(exc, message="the database went away")

    schema.process_errors([escaped])

    crash = _records(caplog, "strawberry.execution")
    assert len(crash) == 1
    assert crash[0].levelno == logging.ERROR
    assert crash[0].exc_info is not None
    assert crash[0].exc_info[1] is escaped.original_error


def test_a_mixed_batch_is_split_rather_than_treated_as_one(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One response can carry both kinds; each must be routed on its own merits.

    A per-*batch* decision (``if any(...)``) would either bury a real crash under a client typo or
    print a traceback for the typo — and a batch containing both is exactly what a partially
    failing multi-field query produces.
    """
    caplog.set_level(logging.DEBUG)
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        escaped = _wrapped_like_graphql_core(exc, message="boom")

    schema.process_errors([ValidationError("service must not be empty"), escaped])

    assert len(_records(caplog, "src.graphql.errors")) == 1
    assert len(_records(caplog, "strawberry.execution")) == 1

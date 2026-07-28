"""The per-operation resource machinery, in the parts that need no database.

Three small things are pinned here, and each of them is load-bearing somewhere a test cannot easily
reach:

* **The subscription detection**, including what it answers when the document has not been parsed
  yet. That case is not hypothetical — it is the state ``on_operation`` starts in, and it decides
  whether the operation is allowed a long-lived session.
* **The refusal to invent loaders** outside an operation, which is what keeps the "a loader never
  outlives its operation" claim structural rather than conventional.
* **The extension being installed on the schema at all.** Every batching test in the integration
  suite is downstream of that one list entry; deleting it turns them all red for a reason nobody
  would guess from the failures.
"""

from __future__ import annotations

import inspect

import pytest
from strawberry.types.graphql import OperationType

from src.config import Settings
from src.graphql.context import Context, PerOperationResources, operation_is_subscription
from src.graphql.schema import schema


class _StubExecutionContext:
    """Just the one property :func:`operation_is_subscription` reads.

    A stub rather than a real ``ExecutionContext`` because the interesting case is the one where
    the property **raises** — Strawberry's own object cannot be put in that state without an
    unparsed document behind it, and building one of those to observe a ``RuntimeError`` would be
    testing Strawberry rather than this function's response to it.
    """

    def __init__(self, operation_type: OperationType | None) -> None:
        self._operation_type = operation_type

    @property
    def operation_type(self) -> OperationType:
        if self._operation_type is None:
            # The exact failure Strawberry raises before the document is parsed.
            raise RuntimeError("No GraphQL document available")
        return self._operation_type


def _settings() -> Settings:
    return Settings(_env_file=None, seed_entries=0, seed_orders=0)


def test_a_subscription_is_recognised_and_nothing_else_is() -> None:
    """The predicate that decides whether an operation may hold a session open."""
    assert operation_is_subscription(_StubExecutionContext(OperationType.SUBSCRIPTION)) is True
    assert operation_is_subscription(_StubExecutionContext(OperationType.QUERY)) is False
    assert operation_is_subscription(_StubExecutionContext(OperationType.MUTATION)) is False


def test_an_undetermined_operation_type_is_treated_as_a_subscription() -> None:
    """Unknown means "do not hold a connection", which is the answer that is never harmful.

    Guessing the other way would open a long-lived session for an operation nobody could classify —
    and if that operation turned out to be a subscription, the session would live for the life of
    the socket. Guessing this way costs a subscription-shaped session (short-lived, one per unit of
    work) for an operation that might have been a query: slower, never wrong.
    """
    assert operation_is_subscription(_StubExecutionContext(None)) is True


def test_a_context_with_no_operation_in_scope_has_no_loaders() -> None:
    """Reaching for loaders outside an operation raises, and the message names the fix.

    Building one on demand would be the friendly thing to do and would put the cache on whatever
    object asked for it — on the WebSocket transport, a context that lives as long as the socket.
    The error is the design.
    """
    context = Context(settings=_settings(), session_factory=lambda: None)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError) as excinfo:
        _ = context.loaders

    assert "PerOperationResources" in str(excinfo.value)


def test_the_extension_is_installed_on_the_schema_as_a_class() -> None:
    """The one line every batching test depends on.

    Asserted as *the class*, not an instance: Strawberry constructs one extension per execution, so
    an instance in this list would be shared by every concurrent request — which for this extension
    means one operation's loaders and session handed to another. C4 hit the same rule for
    ``MaskInternalErrors``; it is restated here because the consequence is different and worse.
    """
    assert PerOperationResources in schema.extensions
    assert isinstance(PerOperationResources, type)


def test_the_operation_hook_is_an_async_generator() -> None:
    """The hook has to be able to ``await`` on the way out, because closing a session is async.

    Stated as an assertion because the alternative fails silently in the wrong direction: a plain
    generator would be accepted by Strawberry, would run, and would have no way to close the
    operation's session — leaking a pooled connection per request rather than raising.

    The cost is that this schema is async-only (``execute_sync`` refuses an async hook), which is
    why ``tests/unit/test_graphql_schema.py`` introspects through ``schema.execute``.
    """
    assert inspect.isasyncgenfunction(PerOperationResources.on_operation)

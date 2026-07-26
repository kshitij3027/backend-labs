"""The fixed corpus every integration test grades itself against, plus the oracle helpers.

Not a test module (pytest collects ``test_*.py`` only) and not a conftest — it holds no fixtures.
It exists so the *values* and the *projections* that define "what the database should contain" are
written once, and so the fixtures in ``conftest.py`` and the assertions in each test module are
looking at the same corpus rather than at two that happen to agree today.

.. rubric:: What makes the integration suite worth having

Almost every assertion computes its expected answer **in Python**, by running the same filter over
the objects :func:`~src.generators.generate_log_records` returned, and then asserts the database
(or the GraphQL layer above it) returned exactly that set. That is a comparison between two
independent computations. The tempting alternative — asserting that a ``service`` filter returns
rows whose service is the one asked for — is a tautology: it passes against an implementation that
returns one arbitrary matching row and silently drops the other forty.

That only works because the generator is pure: fixed seed, fixed anchor instant, no wall clock.
See its module docstring for the contract.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import TypeVar

from src.db.models import LogEntryORM, LogRecord

#: Rows in the fixed corpus.
#:
#: 1200 rather than a couple of hundred because the level mix has a 1% CRITICAL tail: at 300 rows
#: the expected count is three, and "does the CRITICAL filter return the right rows" would be one
#: unlucky seed away from asserting that an empty set equals an empty set. At 1200 the thinnest
#: bucket is a dozen rows and every filter test grades a real subset.
CORPUS_SIZE = 1200

#: RNG seed. Same seed, same corpus, in any process — which is what lets a test regenerate the
#: expected answer instead of asking the database twice.
SEED = 20260725

#: The newest instant in the generated corpus. A CONSTANT, not ``now()``: the oracle a test
#: computes and the rows the database holds must describe the same corpus no matter when the suite
#: runs.
ANCHOR = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)

_T = TypeVar("_T")


def run_sync(coro: Awaitable[_T]) -> _T:
    """Run ``coro`` on a private event loop, leaving the ambient loop policy untouched.

    Used by the synchronous session-scoped schema fixture. :func:`asyncio.run` would also work, but
    it *sets* and then clears the current event loop, and pytest-asyncio manages that same global
    around every test — so this creates a loop, uses it, and closes it without ever touching
    :func:`asyncio.set_event_loop`. Anything opened inside the coroutine (an engine, its
    connections) must also be closed inside it, since the loop does not outlive the call.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)  # type: ignore[arg-type]
    finally:
        loop.close()


def newest_first(records: list[LogRecord]) -> list[LogRecord]:
    """Put a generated corpus into the order the database returns it in.

    The generator emits oldest-first with strictly increasing timestamps, and seeding inserts in
    that order — so ``BIGSERIAL`` ids ascend with time and ``ORDER BY timestamp DESC, id DESC`` is
    exactly the reverse of generation order. Reversing (rather than re-sorting) states that
    relationship instead of re-deriving it, so a test would notice if it ever stopped holding.
    """
    return list(reversed(records))


def matching(
    records: list[LogRecord], predicate: Callable[[LogRecord], bool]
) -> list[LogRecord]:
    """The subset of the corpus a filter should select, newest first."""
    return newest_first([record for record in records if predicate(record)])


def as_records(rows: list[LogEntryORM]) -> list[LogRecord]:
    """Project database rows onto the identity-free value objects the oracle is made of."""
    return [LogRecord.from_orm_row(row) for row in rows]

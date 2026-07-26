"""``LogLevel`` — the strongly-typed severity enum the spec asks for (§2 item 14).

.. rubric:: This is where an invalid level dies, and it dies before any SQL exists

``level`` could have been a ``String`` on both the filter input and the entry type, and everything
would appear to work. What that costs is the whole point of the requirement:

* ``logs(filters: {level: "EROR"})`` would compile, reach the repository, produce
  ``WHERE level = 'EROR'``, and return an empty list. Zero rows is a legitimate answer to a valid
  question, so the client has no way to tell a typo from a quiet hour.
* With an enum, the same request never reaches a resolver. graphql-core rejects it during
  **validation** — before execution starts, before a session is opened, before a statement is
  built — with a message naming every legal value. The client learns what it did wrong, and the
  database is never asked a question nobody meant to ask.

That is also why the storage column stays a plain ``String`` (see the comment on
:attr:`src.db.models.LogEntryORM.level`): the constraint is enforced once, at the edge, where it
can produce a good error message, rather than twice, with the second copy producing a driver
exception and an HTTP 500.

.. rubric:: The enum is pinned to the generator, not merely similar to it

:data:`src.generators.LOG_LEVELS` is the single definition of what a level can be — it is what the
seeded corpus draws from, what every test oracle compares against, and what C4's ``createLog``
persists. If this enum ever gained a member the corpus cannot produce (or lost one it can), the
symptom would be a filter that silently matches nothing, or a stored row whose level cannot be
serialised back out through the schema. :func:`_assert_levels_match_the_corpus` turns that into an
**import-time** failure naming both sides, matching the register of
:func:`src.generators._validate_vocabulary`. The unit suite asserts the same thing through
introspection, so the guarantee holds against the *published* schema and not only against the
Python object.

Both the member **names** and the member **values** are checked, because they play different
roles: the name is what appears in the SDL and in a client's query (``level: ERROR``), and the
value is the string written to and compared against the ``level`` column.
"""

from __future__ import annotations

from enum import Enum

import strawberry

from src.generators import LOG_LEVELS


# NOTE: deliberately no class docstring. Descriptions on this schema are opt-in — the SDL is a
# committed build output compared byte-for-byte by tests/unit/test_schema_sdl.py, and a docstring
# that some library version decides to promote into a description would be an invisible source of
# drift. The prose that explains this type lives in the module docstring above, where it cannot
# leak into the published contract.
@strawberry.enum
class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def _assert_levels_match_the_corpus() -> None:
    """Fail at import if this enum and :data:`src.generators.LOG_LEVELS` have drifted apart.

    Raises:
        ValueError: If the member names or the member values, in order, differ from
            ``LOG_LEVELS``.
    """
    names = tuple(member.name for member in LogLevel)
    values = tuple(member.value for member in LogLevel)

    if names != LOG_LEVELS:
        raise ValueError(
            f"LogLevel member names {names!r} do not match src.generators.LOG_LEVELS "
            f"{LOG_LEVELS!r}: the enum names are what a client writes in a query, so a mismatch "
            "means either a level the corpus contains cannot be asked for, or a level that can be "
            "asked for matches nothing that exists"
        )
    if values != LOG_LEVELS:
        raise ValueError(
            f"LogLevel member values {values!r} do not match src.generators.LOG_LEVELS "
            f"{LOG_LEVELS!r}: the values are the strings compared against the `level` column, so "
            "a mismatch means a validated query still selects the wrong rows"
        )


_assert_levels_match_the_corpus()

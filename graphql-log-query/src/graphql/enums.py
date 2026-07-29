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

from src.generators import (
    LOG_LEVELS,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_OUTCOMES,
    USER_ACTIVITIES,
)


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


# =================================================================================================
# C10 — the e-commerce vocabularies (spec §3 Feature Area A)
#
# Four more enums, each pinned to its roster in `src.generators` by the same import-time guard, for
# the same reason argued at the top of this module: an unknown status must die during VALIDATION
# with a message naming the legal values, not reach a resolver, become
# `WHERE status = 'SHIPED'`, and return an empty list a client cannot tell from a quiet period.
#
# THE COLUMNS STAY `String`. See the note on `OrderEventORM.status` — a database-level ENUM makes
# adding a status a migration to gain a constraint that is already enforced one layer earlier and
# produces a far better error there.
# =================================================================================================


@strawberry.enum
class OrderStatus(Enum):
    CREATED = "CREATED"
    PAID = "PAID"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


@strawberry.enum
class PaymentMethod(Enum):
    CARD = "CARD"
    PAYPAL = "PAYPAL"
    APPLE_PAY = "APPLE_PAY"
    BANK_TRANSFER = "BANK_TRANSFER"
    GIFT_CARD = "GIFT_CARD"


@strawberry.enum
class PaymentOutcome(Enum):
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    DECLINED = "DECLINED"
    REFUNDED = "REFUNDED"


@strawberry.enum
class UserActivity(Enum):
    SIGNUP = "SIGNUP"
    LOGIN = "LOGIN"
    BROWSE = "BROWSE"
    ADD_TO_CART = "ADD_TO_CART"
    CHECKOUT = "CHECKOUT"
    REVIEW = "REVIEW"
    LOGOUT = "LOGOUT"


def _assert_enum_matches_roster(
    enum_class: type[Enum], roster: tuple[str, ...], roster_name: str
) -> None:
    """Fail at import if ``enum_class`` and the generator roster it publishes have drifted.

    Both halves are checked, and they play different roles — the same split
    :func:`_assert_levels_match_the_corpus` makes for ``LogLevel``:

    * the member **name** is what a client writes in a query (``status: SHIPPED``) and what appears
      in the SDL;
    * the member **value** is the string written to and compared against the column.

    Order is checked too, because the SDL prints members in declaration order and a reordering is a
    (cosmetic, but real) contract diff that should arrive as a reviewed SDL change rather than as a
    surprise.

    Raises:
        ValueError: If the names or the values, in order, differ from ``roster``.
    """
    names = tuple(member.name for member in enum_class)
    values = tuple(member.value for member in enum_class)

    if names != roster:
        raise ValueError(
            f"{enum_class.__name__} member names {names!r} do not match "
            f"src.generators.{roster_name} {roster!r}: the names are what a client writes in a "
            "query, so a mismatch means either a value the corpus contains cannot be asked for, "
            "or a value that can be asked for matches nothing that exists"
        )
    if values != roster:
        raise ValueError(
            f"{enum_class.__name__} member values {values!r} do not match "
            f"src.generators.{roster_name} {roster!r}: the values are the strings compared against "
            "the column, so a mismatch means a validated query still selects the wrong rows"
        )


_assert_enum_matches_roster(OrderStatus, ORDER_STATUSES, "ORDER_STATUSES")
_assert_enum_matches_roster(PaymentMethod, PAYMENT_METHODS, "PAYMENT_METHODS")
_assert_enum_matches_roster(PaymentOutcome, PAYMENT_OUTCOMES, "PAYMENT_OUTCOMES")
_assert_enum_matches_roster(UserActivity, USER_ACTIVITIES, "USER_ACTIVITIES")

"""Unit tests for the C7 role gate — ``src.deps.require_role`` and its documentation machinery.

These call the guard **directly**, as a coroutine taking a :class:`~src.auth.Principal`, rather
than driving it over HTTP. ``tests/integration/test_rbac_api.py`` already exercises the full
chain (token -> ``current_principal`` -> guard -> handler); what is under test here is the
decision itself, and isolating it means the exhaustive 4x4 role matrix costs sixteen function
calls instead of sixteen HTTP round trips with sixteen bcrypt logins behind them.

Calling ``guard(principal)`` positionally works because the guard declares its principal as
``principal: Principal = Depends(current_principal)`` — the ``Depends`` is a *default*, so it is
only consulted when FastAPI is the caller. That is the same property that makes the dependency
testable without a request object at all.

``pytest.ini`` sets ``asyncio_mode = auto``, so the ``async def`` tests below need no marker.

Nothing here sleeps and nothing here hashes a password.
"""

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, status

from src.auth import ROLE_ORDER, Principal, Role, Tier, role_satisfies
from src.deps import (
    REQUIRED_ROLE_EXTENSION,
    _declared_minimum_role,
    current_principal,
    require_role,
    role_denied_detail,
    role_requirement_note,
)

#: The ladder, least privileged first. Taken from the enum rather than typed out, so a fifth role
#: joins every matrix below automatically instead of silently escaping them.
ROLES: tuple[Role, ...] = tuple(Role)

#: Every (held, minimum) pair where the caller out-ranks the requirement.
HIGHER_PAIRS = [
    (held, minimum)
    for held in ROLES
    for minimum in ROLES
    if ROLE_ORDER[held] > ROLE_ORDER[minimum]
]

#: Every (held, minimum) pair where the caller falls short — the whole 403 surface.
LOWER_PAIRS = [
    (held, minimum)
    for held in ROLES
    for minimum in ROLES
    if ROLE_ORDER[held] < ROLE_ORDER[minimum]
]


def principal_with(role: Role, *, subject: str = "unit-test-subject") -> Principal:
    """A valid, unexpired principal holding ``role``.

    The tier is irrelevant to every assertion in this file — the role gate must not consult it,
    and C8's limiter is the only thing that ever should — so it is pinned to ``free`` rather than
    parametrised.
    """
    now = datetime.now(UTC)
    return Principal(
        subject=subject,
        role=role,
        tier=Tier.FREE,
        issued_at=now,
        expires_at=now + timedelta(minutes=30),
    )


# ---------------------------------------------------------------------------------------------
# The ladder is inclusive: a role satisfies itself and everything below it
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ROLES, ids=[r.value for r in ROLES])
async def test_require_role_allows_equal_role(role: Role):
    """Every role satisfies its own requirement, and the guard hands the principal straight back.

    The identity assertion matters: the gate is a *pass-through*, not a re-derivation. A guard
    that rebuilt the principal could quietly hand the handler a different set of claims from the
    ones it just approved.
    """
    guard = require_role(role)
    principal = principal_with(role)

    assert await guard(principal) is principal


@pytest.mark.parametrize(
    ("held", "minimum"), HIGHER_PAIRS, ids=[f"{h.value}>={m.value}" for h, m in HIGHER_PAIRS]
)
async def test_require_role_allows_higher_role(held: Role, minimum: Role):
    """An admin can do anything a viewer can. Six ordered pairs, all of them admitted."""
    guard = require_role(minimum)
    principal = principal_with(held)

    assert await guard(principal) is principal


# ---------------------------------------------------------------------------------------------
# ...and only 403 in the other direction
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("held", "minimum"), LOWER_PAIRS, ids=[f"{h.value}<{m.value}" for h, m in LOWER_PAIRS]
)
async def test_require_role_rejects_lower_role_with_403(held: Role, minimum: Role):
    """Insufficient privilege is a ``403`` — never a ``401``, and never a ``500``.

    The status code is the entire contract of this dependency. A ``401`` here would tell the
    caller that re-authenticating is the remedy, which for a correctly-issued token that simply
    does not out-rank the route is a retry loop that can never terminate.
    """
    guard = require_role(minimum)

    with pytest.raises(HTTPException) as raised:
        await guard(principal_with(held))

    assert raised.value.status_code == status.HTTP_403_FORBIDDEN
    assert raised.value.status_code != status.HTTP_401_UNAUTHORIZED


@pytest.mark.parametrize(
    ("held", "minimum"), LOWER_PAIRS, ids=[f"{h.value}<{m.value}" for h, m in LOWER_PAIRS]
)
async def test_403_carries_no_www_authenticate_challenge(held: Role, minimum: Role):
    """RFC 9110 reserves the challenge for ``401``; offering it on a ``403`` invites a retry loop.

    ``current_principal`` attaches ``WWW-Authenticate: Bearer`` to every ``401`` it raises,
    because "go and get a token" is the correct remedy there. Here the caller already has a
    perfectly good token and re-presenting it will fail identically forever, so the challenge
    must be absent.
    """
    guard = require_role(minimum)

    with pytest.raises(HTTPException) as raised:
        await guard(principal_with(held))

    headers = raised.value.headers or {}
    assert "WWW-Authenticate" not in headers
    assert not headers


def test_role_ladder_covers_all_sixteen_pairs():
    """Exhaustive 4x4: :func:`~src.auth.role_satisfies` *is* the ``ROLE_ORDER`` comparison.

    Sixteen pairs, no gaps and no overlap — the two parametrised suites above between them cover
    the four equal pairs, the six higher pairs and the six lower pairs, and this asserts that
    those three sets are the whole matrix. A hole in the ladder (say ``writer`` accidentally not
    satisfying ``analyst``) is invisible to a spot-check and obvious here.
    """
    assert len(ROLES) == 4
    assert len(HIGHER_PAIRS) == 6
    assert len(LOWER_PAIRS) == 6

    pairs = [(held, minimum) for held in ROLES for minimum in ROLES]
    assert len(pairs) == 16

    for held, minimum in pairs:
        expected = ROLE_ORDER[held] >= ROLE_ORDER[minimum]
        assert role_satisfies(held, minimum) is expected, (held, minimum)

    equal_pairs = [(r, r) for r in ROLES]
    assert set(equal_pairs) | set(HIGHER_PAIRS) | set(LOWER_PAIRS) == set(pairs)


# ---------------------------------------------------------------------------------------------
# The factory itself
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ROLES, ids=[r.value for r in ROLES])
def test_require_role_is_cached_per_minimum(role: Role):
    """One callable per role, not one per route.

    FastAPI's per-request dependency cache is keyed on the callable, and the OpenAPI machinery
    identifies the guard by attribute — both are simpler and cheaper when four routes gated at
    ``viewer`` share one guard object instead of instantiating four indistinguishable closures.
    """
    assert require_role(role) is require_role(role)


def test_distinct_roles_get_distinct_guards():
    """The cache must key on the *minimum*, not collapse every role onto one guard."""
    guards = [require_role(role) for role in ROLES]
    assert len({id(guard) for guard in guards}) == len(ROLES)


@pytest.mark.parametrize("role", ROLES, ids=[r.value for r in ROLES])
def test_require_role_nests_current_principal(role: Role):
    """The guard depends on ``current_principal`` rather than decoding the token itself.

    This is what makes a gated request pay for exactly one signature verification, and it is what
    guarantees the ``401`` a bad token produces is byte-identical on a gated route and an ungated
    one — there is only ever one implementation of it.
    """
    parameter = inspect.signature(require_role(role)).parameters["principal"]
    assert parameter.default.dependency is current_principal


@pytest.mark.parametrize("role", ROLES, ids=[r.value for r in ROLES])
def test_guard_is_named_after_its_role(role: Role):
    """A dependency-resolution traceback should say ``require_writer``, not ``<locals>._guard``."""
    assert require_role(role).__name__ == f"require_{role.value}"


# ---------------------------------------------------------------------------------------------
# The 403 body
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("held", "minimum"), LOWER_PAIRS, ids=[f"{h.value}<{m.value}" for h, m in LOWER_PAIRS]
)
async def test_403_detail_names_the_requirement(held: Role, minimum: Role):
    """The body says what was needed — that is what makes the refusal actionable.

    It names the requirement and the role actually held, and nothing else: no list of the routes
    this principal *can* reach, no hint about which accounts hold the missing role. "Here is what
    to ask for" is help; "here is the shape of the authorisation surface" is a map for someone who
    has just been told no.
    """
    guard = require_role(minimum)

    with pytest.raises(HTTPException) as raised:
        await guard(principal_with(held))

    detail = raised.value.detail
    assert detail == role_denied_detail(held, minimum)
    assert f"{minimum.value!r} or higher required" in detail
    assert repr(held.value) in detail
    # The refusal must not leak the caller's identity back into a body that may be logged or
    # surfaced in a browser console.
    assert "unit-test-subject" not in detail


def test_role_denied_detail_reads_as_the_readme_specifies():
    """The exact string the README's `403` example implies, pinned once."""
    assert (
        role_denied_detail(Role.VIEWER, Role.WRITER)
        == "role 'viewer' is not permitted; 'writer' or higher required"
    )


# ---------------------------------------------------------------------------------------------
# Publishing the requirement into the OpenAPI document
#
# The route class that consumes these is exercised end-to-end by
# `tests/integration/test_rbac_api.py::test_openapi_documents_role_requirements`; what is checked
# here is the two pieces it is built from.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ROLES, ids=[r.value for r in ROLES])
def test_role_requirement_note_names_the_role_and_the_ladder(role: Role):
    note = role_requirement_note(role)
    assert f"`{role.value}`" in note
    assert "or higher" in note
    # The note has to be greppable as a whole string, because the route class uses `note not in
    # description` as its idempotency check — a note that varied per call would append twice.
    assert role_requirement_note(role) == note


def test_required_role_extension_is_a_legal_openapi_vendor_extension():
    """OpenAPI 3.1 only permits unspecified keys in an operation object under an ``x-`` prefix."""
    assert REQUIRED_ROLE_EXTENSION.startswith("x-")


def test_declared_minimum_role_finds_a_nested_guard():
    """The route class reads the requirement out of the dependency tree, at any depth.

    ``_declared_minimum_role`` walks the public ``dependencies``/``call`` attributes of FastAPI's
    ``Dependant`` nodes, so a stand-in with those two attributes is a faithful stub — and it keeps
    this test from having to build a real route just to assert a tree walk.
    """
    leaf = SimpleNamespace(call=require_role(Role.WRITER), dependencies=[])
    middle = SimpleNamespace(call=object(), dependencies=[leaf])
    root = SimpleNamespace(call=object(), dependencies=[middle])

    assert _declared_minimum_role(root) is Role.WRITER


def test_declared_minimum_role_is_none_for_an_ungated_tree():
    """An ungated route publishes nothing; "requires nothing" would just be noise."""
    root = SimpleNamespace(
        call=object(),
        dependencies=[SimpleNamespace(call=current_principal, dependencies=[])],
    )

    assert _declared_minimum_role(root) is None


def test_declared_minimum_role_reports_the_strictest_of_several():
    """Two gates on one route means the strictest one is what actually applies.

    A route carrying two guards is an oddity, but if one ever appears the published document must
    describe the requirement a caller actually has to meet — advertising the weaker of the two
    would send clients into a ``403`` the docs said could not happen.
    """
    root = SimpleNamespace(
        call=object(),
        dependencies=[
            SimpleNamespace(call=require_role(Role.VIEWER), dependencies=[]),
            SimpleNamespace(call=require_role(Role.ADMIN), dependencies=[]),
            SimpleNamespace(call=require_role(Role.ANALYST), dependencies=[]),
        ],
    )

    assert _declared_minimum_role(root) is Role.ADMIN

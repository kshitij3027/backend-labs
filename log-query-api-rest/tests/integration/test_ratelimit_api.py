"""Integration tests for C8: the token bucket as it behaves over real HTTP.

``tests/unit/test_ratelimit.py`` proves the arithmetic. This file proves the *wiring*, and the
wiring is where the interesting failures live: whether the headers survive an exception, whether
the role gate really runs before the bucket, and whether a ``429`` tells the client anything
useful. None of that is visible from the algorithm.

.. rubric:: The clock is frozen, and it has to be

Every app here is built with ``Runtime.build(settings, limiter_clock=FakeClock())``. Without that
seam these tests would race their own runtime: the free tier refills at 10 tokens/s, so the few
tens of milliseconds it takes ``TestClient`` to push 21 requests is worth a fraction of a token,
and whether the 21st is refused would depend on how loaded the machine is. That is the classic
flaky rate-limit test. With a frozen clock, "exactly 20 succeed and the 21st does not" is an
exact statement — and the one test that needs time to pass advances the clock by hand.

.. rubric:: Fixtures are function-scoped, so every test starts with full buckets

Each test builds its own app, hence its own :class:`~src.ratelimit.RateLimiter`, hence a fresh
bucket per principal. No test can exhaust another's allowance.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth import DEV_ACCOUNTS, DEV_PASSWORDS, Tier
from src.config import Settings
from src.main import API_V1_PREFIX, EXPOSE_HEADERS, Runtime, create_app
from src.ratelimit import (
    HEADER_LIMIT,
    HEADER_REMAINING,
    HEADER_RESET,
    HEADER_RETRY_AFTER,
)

LOGS_URL = f"{API_V1_PREFIX}/logs"
ME_URL = f"{API_V1_PREFIX}/auth/me"
TOKEN_URL = f"{API_V1_PREFIX}/auth/token"

#: The demo accounts, chosen for the tier each one holds. ``viewer`` is on ``free`` (burst 20)
#: precisely so a 429 is reachable in a handful of requests; ``analyst`` is on ``pro`` (burst 200)
#: so the same loop proves the tiers are genuinely different sizes rather than accidentally
#: identical. Only the *usernames* are written here — every burst and tier this file asserts on is
#: read back from :data:`~src.auth.DEV_ACCOUNTS` and ``Settings.tier_limits``, and
#: ``test_tiers_match_the_demo_accounts`` is what fails if that pairing ever changes underneath.
FREE_USER = "viewer"
PRO_USER = "analyst"


class FakeClock:
    """A manually-driven clock. Reading it never moves it; only :meth:`advance` does.

    Defined locally rather than imported from the unit suite: a shared test helper between two
    suites is a coupling that makes one file's refactor break the other, and this is five lines.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def headers_for(client: TestClient, username: str) -> dict[str, str]:
    """``Authorization`` header for a demo account, minted through the real token endpoint.

    A local helper rather than an import from ``conftest``, matching ``test_rbac_api.py``: a test
    module that imports from another module's ``conftest`` couples two suites through a file
    pytest owns the import of.

    Going through HTTP rather than :func:`~src.auth.create_access_token` matters here more than
    elsewhere — it is what proves the token endpoint costs the caller **nothing**, because every
    fixture in this file would otherwise start one token down.
    """
    response = client.post(
        TOKEN_URL, data={"username": username, "password": DEV_PASSWORDS[username]}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def append_body(**overrides: object) -> dict[str, object]:
    """A minimal valid ``LogCreate`` body — used only to make the 403 tests realistic.

    A function, not a constant, so no test can mutate another's payload. The body must be *valid*
    even though every use of it here expects a ``403``: it is what proves the role gate refuses
    the request on the caller's identity rather than on a malformed payload.
    """
    body: dict[str, object] = {
        "level": "INFO",
        "service": "c8-svc",
        "host": "c8-node",
        "message": "append attempt from a rate-limit test",
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------


@pytest.fixture()
def clock() -> FakeClock:
    """The limiter's clock, frozen at construction. Tests advance it explicitly or not at all."""
    return FakeClock()


@pytest.fixture()
def metered_app(settings: Settings, clock: FakeClock) -> FastAPI:
    """An app whose limiter runs on the frozen clock.

    ``Runtime.build`` rather than ``build_seeded``: the store stays empty, which is fine because
    every route here is exercised for its *gate*, not its payload — ``GET /logs`` over an empty
    ring is a perfectly good ``200``.
    """
    return create_app(runtime=Runtime.build(settings, limiter_clock=clock))


@pytest.fixture()
def metered_client(metered_app: FastAPI) -> TestClient:
    """Unauthenticated by default — each test attaches the token for the tier it is testing."""
    return TestClient(metered_app)


@pytest.fixture()
def free_headers(metered_client: TestClient) -> dict[str, str]:
    """``Authorization`` for the free-tier (viewer) account."""
    return headers_for(metered_client, FREE_USER)


@pytest.fixture()
def pro_headers(metered_client: TestClient) -> dict[str, str]:
    """``Authorization`` for the pro-tier (analyst) account."""
    return headers_for(metered_client, PRO_USER)


def free_burst(settings: Settings) -> int:
    """The free tier's burst, read from configuration rather than typed into the test."""
    return int(settings.tier_limits[Tier.FREE.value].burst)


def remaining(response) -> int:
    """``X-RateLimit-Remaining`` as an int, asserting it was actually present."""
    assert HEADER_REMAINING in response.headers, (
        f"{HEADER_REMAINING} missing from a {response.status_code} response"
    )
    return int(response.headers[HEADER_REMAINING])


# ---------------------------------------------------------------------------------------------
# The headers, on every kind of response
# ---------------------------------------------------------------------------------------------


def test_header_names_are_the_documented_wire_contract() -> None:
    """The three header names are the README's three names, spelled out once.

    Everything else in this file reads the constants, which means a rename inside
    ``src.ratelimit`` would rename the wire contract and leave every other assertion passing.
    This is the one place the literal strings are pinned.
    """
    assert HEADER_LIMIT == "X-RateLimit-Limit"
    assert HEADER_REMAINING == "X-RateLimit-Remaining"
    assert HEADER_RESET == "X-RateLimit-Reset"
    assert HEADER_RETRY_AFTER == "Retry-After"
    # And every one of them is readable by browser JS, or the dashboard's badge cannot exist.
    for header in (HEADER_LIMIT, HEADER_REMAINING, HEADER_RESET, HEADER_RETRY_AFTER):
        assert header in EXPOSE_HEADERS


def test_every_response_carries_ratelimit_headers(metered_client, free_headers, settings):
    """A plain ``200`` advertises the ceiling — the README's "not just on rejection" clause.

    The whole point is that a well-behaved client can pace itself instead of discovering the
    limit by tripping it, which is impossible if the numbers only appear on the response that
    already refused you.
    """
    response = metered_client.get(LOGS_URL, headers=free_headers)

    assert response.status_code == 200
    assert response.headers[HEADER_LIMIT] == str(free_burst(settings))
    assert remaining(response) == free_burst(settings) - 1
    # Delay-seconds, not a UNIX timestamp: one token spent at 10/s is 0.1s to full, rounded up.
    assert response.headers[HEADER_RESET] == "1"


def test_headers_present_on_403_response(metered_client, free_headers, settings):
    """A ``403`` still reports the caller's allowance, because the caller is known.

    The role gate refuses this request before the limiter is ever consulted, so there is no
    decision to report — the middleware *peeks* the bucket instead. That is worth doing: a client
    that just hit a ``403`` is exactly the client about to retry something, and sending it away
    with no pacing information is how a permissions bug becomes a thundering herd.
    """
    response = metered_client.post(LOGS_URL, json=append_body(), headers=free_headers)

    assert response.status_code == 403
    assert response.headers[HEADER_LIMIT] == str(free_burst(settings))
    # Peeked, not spent: nothing was consumed, so the full burst is still there.
    assert remaining(response) == free_burst(settings)


def test_headers_absent_on_401_response(metered_client):
    """A ``401`` emits **nothing**, and that is the design, not an oversight.

    With no principal there is no bucket, so every possible value here would be invented. A
    header claiming a ceiling that was never evaluated is worse than a missing one: a client can
    handle absence, but it cannot detect fiction.
    """
    response = metered_client.get(LOGS_URL)

    assert response.status_code == 401
    for header in (HEADER_LIMIT, HEADER_REMAINING, HEADER_RESET):
        assert header not in response.headers
    # The request-id middleware still ran, which is what proves the absence is a decision rather
    # than the middleware having been skipped entirely.
    assert response.headers["X-Request-ID"]


def test_remaining_decrements_monotonically(metered_client, free_headers, settings):
    """Consecutive requests count down one at a time — the number a client paces against."""
    burst = free_burst(settings)

    observed = [
        remaining(metered_client.get(LOGS_URL, headers=free_headers)) for _ in range(5)
    ]

    assert observed == [burst - 1, burst - 2, burst - 3, burst - 4, burst - 5]


# ---------------------------------------------------------------------------------------------
# Exhaustion
# ---------------------------------------------------------------------------------------------


def test_free_tier_burst_then_429(metered_client, free_headers, settings):
    """Exactly ``burst`` requests pass instantaneously; the next one is refused.

    Exact, not approximate, because the clock is frozen — no refill happens during the loop, so
    this is the bucket's capacity being measured rather than capacity plus however long the loop
    took.
    """
    burst = free_burst(settings)

    for i in range(burst):
        response = metered_client.get(LOGS_URL, headers=free_headers)
        assert response.status_code == 200, f"request {i + 1} of {burst} was refused"

    refused = metered_client.get(LOGS_URL, headers=free_headers)
    assert refused.status_code == 429


def test_429_carries_retry_after_and_zero_remaining(metered_client, free_headers, settings):
    """The refusal is self-describing: how many, how many left, and when to come back."""
    burst = free_burst(settings)
    for _ in range(burst):
        metered_client.get(LOGS_URL, headers=free_headers)

    refused = metered_client.get(LOGS_URL, headers=free_headers)

    assert refused.status_code == 429
    assert refused.headers[HEADER_REMAINING] == "0"
    assert refused.headers[HEADER_LIMIT] == str(burst)
    # `Retry-After: 0` would invite an immediate retry, turning a throttled client into a hot
    # loop against the endpoint that just shed it. The floor of 1 is the whole point.
    assert int(refused.headers[HEADER_RETRY_AFTER]) >= 1


def test_429_body_has_detail(metered_client, free_headers, settings):
    """The refusal carries a JSON body a human can act on, and the correlation id."""
    for _ in range(free_burst(settings)):
        metered_client.get(LOGS_URL, headers=free_headers)

    refused = metered_client.get(LOGS_URL, headers=free_headers)

    assert refused.status_code == 429
    detail = refused.json()["detail"]
    assert isinstance(detail, str) and detail
    assert Tier.FREE.value in detail, "the body should name the tier that was exhausted"
    assert refused.headers["X-Request-ID"]


def test_refill_lets_a_throttled_client_back_in(metered_client, free_headers, settings, clock):
    """A ``429`` is transient, and ``Retry-After`` is honest about it.

    Also the proof that the limiter is running on the *injected* clock: if the wiring had
    ignored ``limiter_clock`` and used ``time.monotonic``, advancing this fake clock would change
    nothing and the request would still be refused.
    """
    for _ in range(free_burst(settings)):
        metered_client.get(LOGS_URL, headers=free_headers)
    refused = metered_client.get(LOGS_URL, headers=free_headers)
    assert refused.status_code == 429

    clock.advance(float(refused.headers[HEADER_RETRY_AFTER]))

    allowed = metered_client.get(LOGS_URL, headers=free_headers)
    assert allowed.status_code == 200


# ---------------------------------------------------------------------------------------------
# THE headline tests — a refusal must not cost the caller anything
# ---------------------------------------------------------------------------------------------


def test_403_does_not_consume_a_token(metered_client, free_headers, settings):
    """**A 403 must leave the bucket untouched.** The single most important test in this file.

    The role check is a *parent* of the limiter in the dependency graph (``src.deps.guarded``), so
    a forbidden request raises before a token is ever spent. If that ordering inverted, a caller
    hammering a route they are permanently forbidden from would drain their own bucket and lock
    themselves out of every route they *are* entitled to — an authorization failure escalating
    itself into an availability failure, for a client that is behaving no worse than confusedly.

    Note the arithmetic: the second probe is itself a metered request, so the expected drop is
    exactly one, not zero — and emphatically not one-per-403.
    """
    before = remaining(metered_client.get(LOGS_URL, headers=free_headers))

    for _ in range(5):
        forbidden = metered_client.post(LOGS_URL, json=append_body(), headers=free_headers)
        assert forbidden.status_code == 403
        # Every 403 reports the same untouched allowance on the way out.
        assert remaining(forbidden) == before

    after = remaining(metered_client.get(LOGS_URL, headers=free_headers))
    assert after == before - 1, "the 403s consumed tokens"


def test_401_does_not_consume_a_token(metered_client, free_headers):
    """An unauthenticated request cannot spend somebody else's tokens — it has no bucket at all.

    Worth pinning separately from the ``403`` case because it fails differently: a limiter keyed
    on something attacker-supplied (a claimed username, a client IP behind a shared NAT) would
    let an anonymous caller drain a real principal's allowance from outside the auth chain.
    """
    before = remaining(metered_client.get(LOGS_URL, headers=free_headers))

    for _ in range(5):
        assert metered_client.get(LOGS_URL).status_code == 401

    after = remaining(metered_client.get(LOGS_URL, headers=free_headers))
    assert after == before - 1


# ---------------------------------------------------------------------------------------------
# Isolation and tiers
# ---------------------------------------------------------------------------------------------


def test_limits_are_isolated_per_principal(
    metered_client, free_headers, pro_headers, settings
):
    """One principal exhausting its bucket must not affect anybody else's."""
    for _ in range(free_burst(settings)):
        metered_client.get(LOGS_URL, headers=free_headers)
    assert metered_client.get(LOGS_URL, headers=free_headers).status_code == 429

    unaffected = metered_client.get(LOGS_URL, headers=pro_headers)

    assert unaffected.status_code == 200
    assert remaining(unaffected) == int(settings.tier_limits[Tier.PRO.value].burst) - 1


def test_pro_tier_survives_free_tier_burst_size(metered_client, pro_headers, settings):
    """The tiers are really different sizes, not one size with three names.

    The analyst sails past the number that stops a viewer dead, and advertises the larger
    ceiling while doing it.
    """
    over_free = free_burst(settings) + 5

    for i in range(over_free):
        response = metered_client.get(LOGS_URL, headers=pro_headers)
        assert response.status_code == 200, f"pro tier refused at request {i + 1}"

    assert response.headers[HEADER_LIMIT] == str(
        int(settings.tier_limits[Tier.PRO.value].burst)
    )


def test_tiers_match_the_demo_accounts(metered_client, free_headers, pro_headers, settings):
    """The advertised ceiling is the tier the account actually holds.

    Guards against the whole limiter accidentally collapsing onto one tier — every principal
    getting, say, ``free``'s bucket would leave most of the tests above still green.
    """
    free_limit = metered_client.get(ME_URL, headers=free_headers).headers[HEADER_LIMIT]
    pro_limit = metered_client.get(ME_URL, headers=pro_headers).headers[HEADER_LIMIT]

    _password, _role, free_tier = DEV_ACCOUNTS[FREE_USER]
    _password, _role, pro_tier = DEV_ACCOUNTS[PRO_USER]

    assert free_limit == str(int(settings.tier_limits[free_tier.value].burst))
    assert pro_limit == str(int(settings.tier_limits[pro_tier.value].burst))
    assert free_limit != pro_limit


# ---------------------------------------------------------------------------------------------
# The operability switch, and the unmetered route
# ---------------------------------------------------------------------------------------------


def test_disabled_limiter_never_429s_but_still_reports_ceiling(settings, clock):
    """``RATE_LIMIT_ENABLED=false`` changes enforcement, never the response shape.

    A client pacing itself against ``X-RateLimit-Remaining`` must not break when an operator
    flips the switch mid-incident — which is exactly when it gets flipped. So the headers stay
    present and keep reporting the tier's real ceiling; only the refusal goes away.
    """
    disabled = settings.model_copy(update={"rate_limit_enabled": False})
    client = TestClient(create_app(runtime=Runtime.build(disabled, limiter_clock=clock)))
    headers = headers_for(client, FREE_USER)
    burst = free_burst(settings)

    for i in range(burst + 10):
        response = client.get(LOGS_URL, headers=headers)
        assert response.status_code == 200, f"request {i + 1} was refused with the limiter off"
        assert response.headers[HEADER_LIMIT] == str(burst)
        # Nothing is being consumed, so the honest report is a full bucket every time.
        assert remaining(response) == burst


def test_token_endpoint_is_not_rate_limited(metered_client, settings):
    """``POST /auth/token`` is unmetered — hammering it past the free burst must still work.

    Two reasons it has to be, and the first is structural: there is no principal yet, so there is
    nothing to key a bucket on. Keying one on the *claimed* username instead would let anyone
    lock a real account out of logging in by spraying wrong passwords at it, which turns the rate
    limiter into a denial-of-service weapon aimed at the users it exists to protect. Second,
    bcrypt is already the brake — every call costs a full hash whatever the caller does.
    """
    password = DEV_ACCOUNTS[FREE_USER][0]

    for i in range(free_burst(settings) + 5):
        response = metered_client.post(
            TOKEN_URL, data={"username": FREE_USER, "password": password}
        )
        assert response.status_code == 200, f"token request {i + 1} was refused"
        # Unmetered means no principal was resolved, so there is nothing truthful to advertise.
        assert HEADER_REMAINING not in response.headers


def test_openapi_documents_the_429(client):
    """The rate limit is part of the published contract, not something clients discover by hand.

    A limit a client cannot read from the schema is a limit it finds out about by tripping — and
    the generated client that was never told about ``429`` is the one that retries into a wall.
    """
    paths = client.get("/openapi.json").json()["paths"]

    for path, method in ((LOGS_URL, "get"), (LOGS_URL, "post"), (ME_URL, "get")):
        assert "429" in paths[path][method]["responses"], f"{method.upper()} {path}"

    # ...and the deliberately unmetered route does not advertise one.
    assert "429" not in paths[TOKEN_URL]["post"]["responses"]

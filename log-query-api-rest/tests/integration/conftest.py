"""Integration fixtures: a TestClient over an app built through the SEEDED runtime path.

``seeded_app`` / ``seeded_client`` exercise :meth:`src.main.Runtime.build_seeded` — the same
constructor the production lifespan uses — rather than the cheap :meth:`~src.main.Runtime.build`,
so the integration suite covers the path that actually ships.

.. rubric:: Why the corpus is appended on top instead of configured in

``tests/conftest.py`` pins ``seed_entries=0``, so ``build_seeded`` produces an empty store here.
The corpus is then appended explicitly from :func:`~src.generators.generate_entries` with a
**fixed seed and a small fixed size**, which buys two things a settings-driven seed would not:

* The tests can hold the *same* entry objects the store holds, so an assertion can be a
  brute-force tally over :data:`SEEDED_CORPUS` rather than a hardcoded magic number. Every
  expected count then stays correct if :data:`SEEDED_CORPUS_SIZE` changes.
* ``build_seeded`` is still the constructor under test, so the production wiring (generator ->
  ``append_many`` -> ring) keeps its coverage instead of being replaced by a test-only path.

The corpus is generated **once at import** and shared: :class:`~src.models.LogEntry` is frozen and
:class:`~src.store.LogStore` never mutates what it is given, so there is nothing for one test to
leak into another. Each test still gets a fresh store — only the immutable entries are shared.

.. rubric:: Why ``seeded_client`` carries a token

C7 puts a role gate on every data route, so an anonymous ``GET /api/v1/logs`` is now a ``401``.
That is the correct behaviour and it is asserted directly in
``tests/integration/test_rbac_api.py``; it also means the read-surface suites, whose subject is
pagination and filtering rather than authorization, would otherwise have to repeat an
``Authorization`` header on every single call. So this client sets one **default** header, which
httpx merges into each request and any per-request ``headers=`` overrides cleanly. Tests that
care about the auth chain either pass their own header or use the unauthenticated ``client``
fixture from ``tests/conftest.py``.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth import DEV_PASSWORDS
from src.config import Settings
from src.generators import generate_entries
from src.main import API_V1_PREFIX, Runtime, create_app
from src.models import LogEntry

#: The demo account ``seeded_client`` authenticates as. ``admin`` is the top of the role ladder,
#: so a single token satisfies every gate in the project — the ``viewer`` reads C7 gates today and
#: the ``analyst``/``admin`` routes C9–C11 add later — and no read-surface suite has to be revisited
#: as the ladder is applied to more routes.
SEEDED_CLIENT_USER = "admin"

#: Corpus size. Small enough that a full cursor walk is a handful of pages and a brute-force
#: tally is instant; large enough that every level, service and host is represented and that a
#: page boundary is a real boundary rather than the whole corpus.
SEEDED_CORPUS_SIZE = 200

#: RNG seed for the integration corpus. Deliberately NOT
#: :data:`src.generators.DEFAULT_SEED`: the E2E verifier (C12) grades the *production* corpus,
#: which uses the default seed, and reusing it here would make a test that accidentally depends
#: on one specific corpus look like it depends on "the" corpus.
SEEDED_CORPUS_SEED = 20260720

#: Generated once per test session — same seed, byte-identical entries, shared safely because
#: ``LogEntry`` is frozen.
SEEDED_CORPUS: tuple[LogEntry, ...] = tuple(
    generate_entries(SEEDED_CORPUS_SIZE, seed=SEEDED_CORPUS_SEED)
)


@pytest.fixture(scope="session")
def corpus() -> tuple[LogEntry, ...]:
    """The exact entries loaded into ``seeded_app``'s store, oldest first.

    Tests assert against a brute-force tally of *this* — ``sum(1 for e in corpus if …)`` — rather
    than against a number typed into the test, so an expectation cannot quietly become wrong when
    the fixture size or seed changes.
    """
    return SEEDED_CORPUS


@pytest.fixture()
def seeded_app(settings: Settings, corpus: tuple[LogEntry, ...]) -> FastAPI:
    """An app over a production-shaped Runtime whose ring holds exactly ``corpus``."""
    runtime = Runtime.build_seeded(settings)
    assert runtime.store is not None, "build_seeded must always construct a store"
    assert len(corpus) < settings.store_capacity, (
        "the fixture corpus must fit in the ring without eviction, or every expected count "
        "derived from it is wrong"
    )
    runtime.store.append_many(corpus)
    return create_app(runtime=runtime)


def bearer_token(client: TestClient, username: str) -> str:
    """Log a demo account in through the real ``POST /auth/token`` and return its JWT.

    Deliberately the HTTP endpoint rather than :func:`~src.auth.create_access_token`: a token
    minted in-process would be faster but would not be a token this API has ever issued, and the
    ``bcrypt_rounds=4`` the ``settings`` fixture pins makes the honest option cost ~2 ms.
    """
    response = client.post(
        f"{API_V1_PREFIX}/auth/token",
        data={"username": username, "password": DEV_PASSWORDS[username]},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.fixture()
def seeded_client(seeded_app: FastAPI) -> TestClient:
    """A TestClient against the production (seeded) Runtime path, authenticated as ``admin``.

    The ``Authorization`` header is set on the *client*, so httpx attaches it to every request
    while a per-request ``headers={"Authorization": …}`` still wins — which is what lets
    ``test_rbac_api.py`` drive the same fixture as a viewer, an analyst or a writer. See this
    module's docstring for why the default exists at all.
    """
    client = TestClient(seeded_app)
    client.headers["Authorization"] = f"Bearer {bearer_token(client, SEEDED_CLIENT_USER)}"
    return client

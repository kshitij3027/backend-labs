"""Integration tests for ``GET /api/v1/debug/memory`` — the admin-only operational probe.

Two things are pinned here.

**The gate is the top rung, and it is a real one.** Every other route in ``src/api/v1.py``
reports on the *logs*; this one reports on the *deployment* — resident memory, ring occupancy,
open SSE connections, the size of the limiter's bucket table. A ``writer`` that may append log
entries has no business reading how much memory the box is using, so the sweep below asserts a
``403`` for viewer, analyst **and** writer, not merely for the bottom of the ladder. The role
ladder is inclusive, which means the only way to know that ``admin`` is genuinely required is to
check the rung immediately below it.

**The RSS is server-reported.** This route exists because C12's E2E verifier and load harness
gate on backend memory, and a harness can only measure itself. ``test_debug_memory_reports_
positive_rss`` is what stops the probe silently degrading to the ``0.0`` fallback and taking
every memory gate downstream with it — a gate that always passes is worse than no gate, because
it is believed.

Nothing here sleeps.
"""

from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.auth import DEV_ACCOUNTS, DEV_PASSWORDS
from src.deps import REQUIRED_ROLE_EXTENSION
from src.main import API_V1_PREFIX

MEMORY = f"{API_V1_PREFIX}/debug/memory"
LOGS = f"{API_V1_PREFIX}/logs"
TOKEN_URL = f"{API_V1_PREFIX}/auth/token"

#: Every demo account that must NOT be able to read this route. Spelled as the three rungs below
#: `admin` rather than as "everything except admin", so adding a fifth role to the ladder is a
#: visible decision here instead of a silently-unswept gap.
FORBIDDEN_USERS = ("viewer", "analyst", "writer")


def headers_for(client: TestClient, username: str) -> dict[str, str]:
    """``Authorization`` header for a demo account, minted through the real token route."""
    response = client.post(
        TOKEN_URL, data={"username": username, "password": DEV_PASSWORDS[username]}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def read_memory(client: TestClient) -> dict[str, Any]:
    """GET the probe as ``admin``, assert ``200``, and return the decoded snapshot."""
    response = client.get(MEMORY, headers=headers_for(client, "admin"))
    assert response.status_code == 200, response.text
    return response.json()


# ---------------------------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("username", FORBIDDEN_USERS)
def test_debug_memory_requires_admin_403_for_writer(seeded_client, username):
    """viewer, analyst and writer are all refused — ``403``, and not a ``401``.

    The distinction is the contract: ``401`` means "I don't know who you are" and invites a
    retry with credentials; ``403`` means "I know exactly who you are, and no". Collapsing the
    second into the first would send a perfectly-authenticated client off to re-authenticate
    forever. The absence of a ``WWW-Authenticate`` header is the machine-readable half of that
    same statement.
    """
    response = seeded_client.get(MEMORY, headers=headers_for(seeded_client, username))

    assert response.status_code == 403, response.text
    assert "WWW-Authenticate" not in response.headers
    assert DEV_ACCOUNTS[username][1].value in response.json()["detail"]


def test_debug_memory_without_token_is_401(client):
    """No credential is a ``401`` with a challenge, before the role is ever considered.

    Ordering matters: an unauthenticated caller must not be able to learn from the status code
    whether this route exists or what it would have required.
    """
    response = client.get(MEMORY)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"].startswith("Bearer")


def test_admin_can_read_debug_memory(seeded_client):
    """The top of the ladder gets through. The positive half of the matrix."""
    response = seeded_client.get(MEMORY, headers=headers_for(seeded_client, "admin"))

    assert response.status_code == 200, response.text


def test_openapi_documents_debug_route(client):
    """The probe publishes ``admin`` as its requirement, and its schema alongside.

    A diagnostic route that is gated but undocumented is the kind of thing that gets exposed by a
    proxy rule written from the OpenAPI document. Publishing the requirement is what lets a
    policy linter — or a human reading ``/docs`` — see that this one is not like the others.
    """
    spec = client.get("/openapi.json").json()

    assert MEMORY in spec["paths"], sorted(spec["paths"])
    operation = spec["paths"][MEMORY]["get"]

    assert operation[REQUIRED_ROLE_EXTENSION] == "admin"
    assert operation["security"] == [{"bearerAuth": []}]
    assert "403" in operation["responses"]
    assert "MemorySnapshot" in spec["components"]["schemas"], sorted(
        spec["components"]["schemas"]
    )


# ---------------------------------------------------------------------------------------------
# The measurement
# ---------------------------------------------------------------------------------------------


def test_debug_memory_shape_matches_model(seeded_client):
    """Exactly the six documented fields, each a number, none negative.

    C12's harnesses parse this body, so the field set is a contract with two scripts that do not
    exist yet. An extra key would be harmless; a missing or renamed one would make a load run
    fail somewhere far away from the cause.
    """
    body = read_memory(seeded_client)

    assert set(body) == {
        "memory_mb",
        "entries",
        "capacity",
        "evicted",
        "subscribers",
        "rate_buckets",
    }
    assert isinstance(body["memory_mb"], float)
    for field in ("entries", "capacity", "evicted", "subscribers", "rate_buckets"):
        assert isinstance(body[field], int), field
        assert body[field] >= 0, field


def test_debug_memory_reports_positive_rss(seeded_client):
    """RSS is a real measurement, not the ``0.0`` fallback.

    This is the assertion the whole route exists for. ``memory_mb`` degrades to ``0.0`` rather
    than raising when the platform will not answer, which is the right behaviour for a probe —
    and it is also a silent failure mode that would make every downstream memory gate pass
    unconditionally. A Python process with FastAPI, pydantic and a log ring loaded is tens of
    megabytes; the bounds below are wide enough never to be flaky and narrow enough that a
    fallback, a unit mix-up (bytes reported as MiB), or a gauge stuck at a constant all fail.
    """
    body = read_memory(seeded_client)

    assert body["memory_mb"] > 1.0, "0.0 means psutil failed and the gate would be meaningless"
    assert body["memory_mb"] < 100_000.0, "a 100 GB test process means the units are wrong"


def test_debug_memory_reports_store_entries(seeded_client, corpus, settings):
    """Occupancy is read from the live ring, and moves when the ring does.

    Checked against the fixture corpus rather than a constant, and then re-checked after an
    append — a field that reported a plausible-looking cached number would pass the first
    assertion and fail the second.
    """
    before = read_memory(seeded_client)

    assert before["entries"] == len(corpus)
    assert before["capacity"] == settings.store_capacity
    assert before["evicted"] == 0, "the fixture corpus must fit without eviction"

    created = seeded_client.post(
        LOGS,
        json={
            "level": "INFO",
            "service": "debug-probe",
            "host": "node-probe",
            "message": "occupancy marker",
        },
    )
    assert created.status_code == 201, created.text

    assert read_memory(seeded_client)["entries"] == len(corpus) + 1


def test_debug_memory_reports_limiter_buckets(seeded_client):
    """``rate_buckets`` is the limiter's live footprint — one bucket per recent principal.

    Included in the same response as RSS on purpose: a load harness watching memory climb needs
    to know whether the limiter's per-principal table is what is climbing. Four demo accounts
    have made requests by the time this runs, so the count is at least one and never wilder than
    the number of principals that exist.
    """
    body = read_memory(seeded_client)

    assert body["rate_buckets"] >= 1
    assert body["rate_buckets"] <= len(DEV_ACCOUNTS) + 1


def test_debug_memory_reports_no_subscribers_when_nothing_is_streaming(seeded_client):
    """No SSE connection is open, so the count is zero — the baseline C12 measures against.

    Streams are counted, not estimated, and the count is decremented on every one of the six exit
    paths a subscription can take (C10). A baseline that was quietly non-zero here would mean a
    leak had already happened before any load was applied.
    """
    assert read_memory(seeded_client)["subscribers"] == 0

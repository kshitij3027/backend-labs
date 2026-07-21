"""Integration tests for the C6 auth surface: ``POST /auth/token`` and ``GET /auth/me``.

These drive the whole ASGI stack — middleware, the ``HTTPBearer`` security scheme, the
``current_principal`` dependency, the response models and FastAPI's generated OpenAPI document —
rather than calling ``src.auth`` directly (``tests/unit/test_auth.py`` already does that). What is
under test here is the *wiring*: that a real form POST mints a real token, that a real
``Authorization`` header is decoded into a real principal, and that every rejection is a ``401``
with the right challenge header instead of a ``403`` or a ``500``.

The ``client`` fixture from ``tests/conftest.py`` is used rather than ``seeded_client``: neither
route reads the corpus, so seeding 200 entries would only make the file slower. That same fixture
pins ``bcrypt_rounds=4`` (~2 ms per hash instead of ~250 ms), which is what keeps a file that
performs a dozen real logins running in milliseconds.

Nothing here sleeps. The expired-token case mints an already-expired token by injecting a past
``now`` into :func:`~src.auth.create_access_token`.
"""

from datetime import UTC, datetime, timedelta

import pytest

from src.api.v1 import INVALID_CREDENTIALS_DETAIL
from src.auth import (
    DEV_ACCOUNTS,
    DEV_PASSWORDS,
    Principal,
    Role,
    Tier,
    create_access_token,
    decode_token,
)
from src.config import Settings
from src.main import API_V1_PREFIX

TOKEN_URL = f"{API_V1_PREFIX}/auth/token"
ME_URL = f"{API_V1_PREFIX}/auth/me"

#: A valid signing key that is NOT the one the app fixture was built with. Used to prove that a
#: perfectly well-formed token from a different issuer is still refused.
FOREIGN_SECRET = "a-different-valid-signing-key-0123456789abcdef"


def login(client, username: str, password: str):
    """POST the OAuth2 password form. Returns the raw response, asserting nothing."""
    return client.post(TOKEN_URL, data={"username": username, "password": password})


def token_for(client, username: str) -> str:
    """Log in as a demo user and return the bearer token, asserting the login worked."""
    response = login(client, username, DEV_PASSWORDS[username])
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------------------------
# POST /auth/token — success
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("username", sorted(DEV_PASSWORDS))
def test_token_endpoint_issues_jwt_for_each_demo_user(client, settings, username):
    """All four bootstrap accounts log in, and the token they get back says who they are."""
    response = login(client, username, DEV_PASSWORDS[username])

    assert response.status_code == 200, response.text
    body = response.json()

    _password, expected_role, expected_tier = DEV_ACCOUNTS[username]
    assert body["role"] == expected_role.value
    assert body["tier"] == expected_tier.value

    # The response fields are not merely *consistent with* the token — they are the token's own
    # claims. Decoding proves the two cannot have been assembled from different sources.
    principal = decode_token(body["access_token"], settings=settings)
    assert isinstance(principal, Principal)
    assert principal.subject == username
    assert principal.role is expected_role
    assert principal.tier is expected_tier
    # The advertised `expires_at` is the token's own `exp`, to the second — not a recomputation.
    assert principal.expires_at == datetime.fromisoformat(
        body["expires_at"].replace("Z", "+00:00")
    )


def test_token_response_shape(client, settings):
    """Exactly the six documented fields, in the RFC 6749 spelling."""
    response = login(client, "analyst", DEV_PASSWORDS["analyst"])

    assert response.status_code == 200, response.text
    body = response.json()

    assert set(body) == {
        "access_token",
        "token_type",
        "expires_in",
        "expires_at",
        "role",
        "tier",
    }

    # RFC 6750: the literal lowercase string. A capitalised "Bearer" here is a real-world source
    # of client breakage, so it is pinned exactly.
    assert body["token_type"] == "bearer"

    assert isinstance(body["expires_in"], int)
    assert body["expires_in"] > 0
    assert body["expires_in"] == settings.access_token_ttl_min * 60

    # RFC-3339 with a `Z` suffix and millisecond precision — the same wire format every other
    # timestamp in this API uses, so a client writes one date parser and not two.
    assert body["expires_at"].endswith("Z")
    assert "+00:00" not in body["expires_at"]
    expires_at = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
    assert expires_at > datetime.now(UTC)

    # The absolute and relative expiries describe the same instant.
    assert abs(
        (expires_at - datetime.now(UTC)).total_seconds() - body["expires_in"]
    ) < 5

    assert body["access_token"].count(".") == 2  # header.payload.signature


def test_token_is_immediately_usable_on_me(client):
    """The whole point of the endpoint: the token it hands out works on the very next request."""
    token = token_for(client, "writer")

    response = client.get(ME_URL, headers=auth_header(token))

    assert response.status_code == 200, response.text
    assert response.json()["subject"] == "writer"


# ---------------------------------------------------------------------------------------------
# POST /auth/token — rejection
# ---------------------------------------------------------------------------------------------


def test_token_endpoint_rejects_bad_password_401(client):
    response = login(client, "analyst", "not-the-password")

    assert response.status_code == 401, response.text
    assert response.json()["detail"] == INVALID_CREDENTIALS_DETAIL


def test_token_endpoint_rejects_unknown_user_401(client):
    response = login(client, "mallory", "any-password-at-all")

    assert response.status_code == 401, response.text
    assert response.json()["detail"] == INVALID_CREDENTIALS_DETAIL


def test_bad_password_and_unknown_user_are_indistinguishable(client):
    """The response body must not be an enumeration oracle either.

    ``src.auth.authenticate`` already equalises the *timing* of these two paths by hashing a
    dummy value when the record is absent. That work is wasted if the response then says
    "unknown user" versus "wrong password", so the status, the body and the challenge header are
    asserted byte-identical here.
    """
    wrong_password = login(client, "analyst", "not-the-password")
    unknown_user = login(client, "definitely-not-a-user", "not-the-password")

    assert wrong_password.status_code == unknown_user.status_code == 401
    assert wrong_password.json() == unknown_user.json()
    assert wrong_password.headers.get("WWW-Authenticate") == unknown_user.headers.get(
        "WWW-Authenticate"
    )
    # And the message names neither the user nor the failure mode.
    assert "analyst" not in wrong_password.text
    assert "definitely-not-a-user" not in unknown_user.text


def test_token_401_carries_www_authenticate(client):
    """RFC 9110 §11.6.1: a 401 must name the scheme the client should authenticate with."""
    response = login(client, "analyst", "not-the-password")

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize(
    "form",
    [
        {},  # neither field
        {"username": "analyst"},  # no password
        {"password": "analyst-dev-pw"},  # no username
    ],
)
def test_token_endpoint_rejects_missing_form_fields_422(client, form):
    """A malformed *request* is a 422, not a 401 — the credentials were never even evaluated."""
    response = client.post(TOKEN_URL, data=form)

    assert response.status_code == 422, response.text


def test_token_endpoint_rejects_json_body_422(client):
    """The grant is form-encoded (RFC 6749 §4.3); a JSON body is a client mistake, not a login."""
    response = client.post(
        TOKEN_URL, json={"username": "analyst", "password": DEV_PASSWORDS["analyst"]}
    )

    assert response.status_code == 422, response.text


# ---------------------------------------------------------------------------------------------
# GET /auth/me
# ---------------------------------------------------------------------------------------------


def test_me_without_token_is_401_with_www_authenticate(client):
    """No credentials at all. Note this is a 401, never a 403 — see README's `401` vs `403`."""
    response = client.get(ME_URL)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert "detail" in response.json()


@pytest.mark.parametrize(
    "header",
    [
        {"Authorization": "Bearer not-a-jwt"},
        {"Authorization": "Bearer "},  # scheme, no credential
        {"Authorization": "Bearer a.b.c"},
        {"Authorization": "Basic YWRtaW46YWRtaW4="},  # wrong scheme entirely
        {"Authorization": "some-token-without-a-scheme"},
    ],
)
def test_me_with_garbage_bearer_is_401(client, header):
    """Every malformed-credential shape lands on the same 401 with the same challenge."""
    response = client.get(ME_URL, headers=header)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_me_with_expired_token_is_401(client, settings):
    """Minted two hours ago with a 30-minute TTL — expired without a single `sleep`."""
    token, expires_at = create_access_token(
        subject="analyst",
        role=Role.ANALYST,
        tier=Tier.PRO,
        settings=settings,
        now=datetime.now(UTC) - timedelta(hours=2),
    )
    assert expires_at < datetime.now(UTC)

    response = client.get(ME_URL, headers=auth_header(token))

    assert response.status_code == 401, response.text
    assert "expired" in response.json()["detail"]
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_me_with_token_signed_by_other_secret_is_401(client):
    """A structurally perfect token from a different issuer is still not our token."""
    foreign = Settings(_env_file=None, jwt_secret=FOREIGN_SECRET, bcrypt_rounds=4)
    token, _ = create_access_token(
        subject="admin", role=Role.ADMIN, tier=Tier.ENTERPRISE, settings=foreign
    )

    response = client.get(ME_URL, headers=auth_header(token))

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_me_with_tampered_token_is_401(client):
    """Editing the payload without the key cannot buy a session."""
    token = token_for(client, "viewer")
    header_seg, payload_seg, signature_seg = token.split(".")
    replacement = "A" if payload_seg[5] != "A" else "B"
    forged = f"{header_seg}.{payload_seg[:5]}{replacement}{payload_seg[6:]}.{signature_seg}"

    response = client.get(ME_URL, headers=auth_header(forged))

    assert response.status_code == 401, response.text


@pytest.mark.parametrize("username", sorted(DEV_PASSWORDS))
def test_me_returns_principal(client, username):
    """Round trip: log in, present the token, get the same identity back."""
    token = token_for(client, username)

    response = client.get(ME_URL, headers=auth_header(token))

    assert response.status_code == 200, response.text
    body = response.json()

    assert set(body) == {"subject", "role", "tier", "issued_at", "expires_at"}

    _password, expected_role, expected_tier = DEV_ACCOUNTS[username]
    assert body["subject"] == username
    assert body["role"] == expected_role.value
    assert body["tier"] == expected_tier.value

    # Both instants use the API's one timestamp format, and `exp` is after `iat`.
    assert body["issued_at"].endswith("Z")
    assert body["expires_at"].endswith("Z")
    issued_at = datetime.fromisoformat(body["issued_at"].replace("Z", "+00:00"))
    expires_at = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
    assert expires_at > issued_at


def test_me_response_carries_request_id(client):
    """C1's RequestContextMiddleware still wraps the authenticated path."""
    token = token_for(client, "analyst")

    minted = client.get(ME_URL, headers=auth_header(token))
    assert minted.status_code == 200, minted.text
    assert minted.headers["X-Request-ID"]

    # A client-supplied id is echoed, so a caller can correlate this request across services.
    supplied = client.get(
        ME_URL, headers={**auth_header(token), "X-Request-ID": "trace-me-123"}
    )
    assert supplied.headers["X-Request-ID"] == "trace-me-123"


def test_401_response_also_carries_request_id(client):
    """Correlation ids matter most on the failure path, so the middleware must cover it."""
    response = client.get(ME_URL)

    assert response.status_code == 401
    assert response.headers["X-Request-ID"]


# ---------------------------------------------------------------------------------------------
# The generated OpenAPI document
# ---------------------------------------------------------------------------------------------


def test_openapi_documents_auth_routes(client):
    """The auth contract must live in the published document, not in tribal knowledge.

    This is the test behind the README's claim that "enforcement is a route dependency, so the
    required role is part of the generated OpenAPI document". If `GET /auth/me` carried its auth
    check as an inline `if` inside the handler, every assertion below about `security` would fail
    while the runtime behaviour looked identical — which is exactly the difference the
    dependency-based design exists to make visible.
    """
    response = client.get("/openapi.json")
    assert response.status_code == 200, response.text
    spec = response.json()

    assert TOKEN_URL in spec["paths"]
    assert ME_URL in spec["paths"]

    schemas = spec["components"]["schemas"]
    assert "TokenResponse" in schemas
    assert "PrincipalResponse" in schemas
    assert set(schemas["TokenResponse"]["properties"]) == {
        "access_token",
        "token_type",
        "expires_in",
        "expires_at",
        "role",
        "tier",
    }
    assert set(schemas["PrincipalResponse"]["properties"]) == {
        "subject",
        "role",
        "tier",
        "issued_at",
        "expires_at",
    }

    # The bearer scheme is declared, so /docs renders a working *Authorize* button.
    security_schemes = spec["components"]["securitySchemes"]
    assert "bearerAuth" in security_schemes
    assert security_schemes["bearerAuth"]["type"] == "http"
    assert security_schemes["bearerAuth"]["scheme"] == "bearer"

    # /auth/me REQUIRES it...
    me_operation = spec["paths"][ME_URL]["get"]
    assert me_operation["security"] == [{"bearerAuth": []}]
    assert "401" in me_operation["responses"]

    # ...and /auth/token must NOT, or a generated client would demand a token to get a token.
    token_operation = spec["paths"][TOKEN_URL]["post"]
    assert not token_operation.get("security")
    assert "401" in token_operation["responses"]

    # Both are tagged `auth`, so /docs groups them away from the log routes.
    assert token_operation["tags"] == ["auth"]
    assert me_operation["tags"] == ["auth"]

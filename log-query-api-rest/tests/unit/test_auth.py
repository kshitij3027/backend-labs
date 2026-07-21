"""Unit tests for src.auth — the role ladder, bcrypt hashing, and HS256 token handling.

Everything here runs at ``bcrypt_rounds=4`` (~2 ms per hash) instead of the production 12
(~250 ms). Four demo users plus a dummy hash at cost 12 is roughly a second of pure CPU; at
cost 4 the whole module's hashing budget is under 20 ms, which is the difference between a
suite that runs in seconds and one nobody runs.

``Settings`` is constructed **directly** with ``_env_file=None`` rather than through the
environment or ``get_settings()``: these tests need several different signing keys and TTLs in
one process, and mutating ``os.environ`` would leak across tests through the ``get_settings``
LRU cache.

Nothing here sleeps. Token expiry is exercised by injecting a past issue time into
:func:`~src.auth.create_access_token` and a future clock into
:func:`~src.auth.decode_token`, which makes the expiry tests both instant and deterministic.
"""

import base64
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
import pytest
from pydantic import ValidationError

from src.auth import (
    BCRYPT_HASH_RE,
    DEV_ACCOUNTS,
    DEV_PASSWORDS,
    ROLE_ORDER,
    TOKEN_CLAIMS,
    AuthError,
    Principal,
    Role,
    Tier,
    authenticate,
    create_access_token,
    decode_token,
    get_user_directory,
    hash_password,
    role_satisfies,
    verify_password,
)
from src.config import Settings

#: Test work factor. bcrypt's cost is exponential in this number, so 4 vs the production 12 is
#: a ~256x speedup per hash.
ROUNDS = 4

#: Two distinct, valid (long enough, non-placeholder) signing keys. The second exists purely to
#: prove that a token signed with one key is refused by a service holding the other.
SECRET = "unit-test-signing-key-0123456789abcdef"
OTHER_SECRET = "a-completely-different-key-fedcba9876543210"


def build_settings(**kwargs: Any) -> Settings:
    """Construct Settings hermetically: explicit kwargs, no .env file, cheap bcrypt."""
    kwargs.setdefault("jwt_secret", SECRET)
    kwargs.setdefault("bcrypt_rounds", ROUNDS)
    return Settings(_env_file=None, **kwargs)


@pytest.fixture()
def auth_settings() -> Settings:
    """The default settings used by most tests: SECRET, HS256, 30-minute TTL."""
    return build_settings()


# ---------------------------------------------------------------------------------------------
# JWT segment helpers — used to hand-craft forged tokens the library would never emit.
# ---------------------------------------------------------------------------------------------


def b64url(raw: bytes) -> str:
    """base64url-encode without padding, exactly as a JWT segment is encoded."""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def b64url_decode(segment: str) -> bytes:
    """Decode a JWT segment, restoring the padding the encoding strips."""
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def encode_segment(payload: dict[str, Any]) -> str:
    """Serialise a claim/header dict into a JWT segment (compact JSON, base64url)."""
    return b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def valid_claims(**overrides: Any) -> dict[str, Any]:
    """A claim set this service would consider entirely valid, before any tampering."""
    now = datetime.now(UTC).replace(microsecond=0)
    claims: dict[str, Any] = {
        "sub": "analyst",
        "role": "analyst",
        "tier": "pro",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=30)).timestamp()),
    }
    claims.update(overrides)
    return claims


# =============================================================================================
# Password hashing
# =============================================================================================


def test_password_roundtrip():
    hashed = hash_password("correct horse battery staple", rounds=ROUNDS)

    assert hashed.startswith("$2b$")
    # The cost is embedded in the hash, so the stored string is self-describing.
    assert f"${ROUNDS:02d}$" in hashed
    assert verify_password("correct horse battery staple", hashed) is True


def test_wrong_password_fails():
    hashed = hash_password("correct horse battery staple", rounds=ROUNDS)

    assert verify_password("Correct horse battery staple", hashed) is False
    assert verify_password("", hashed) is False
    assert verify_password("correct horse battery stapl", hashed) is False


def test_salt_makes_two_hashes_of_the_same_password_differ():
    """A fresh salt per hash means identical passwords do not produce identical rows."""
    first = hash_password("same-password", rounds=ROUNDS)
    second = hash_password("same-password", rounds=ROUNDS)

    assert first != second
    assert verify_password("same-password", first)
    assert verify_password("same-password", second)


@pytest.mark.parametrize(
    "garbage",
    [
        "",  # empty — a record that was never populated
        "not-a-hash",
        "$2b$",  # right prefix, nothing after it
        "plaintext-password-stored-by-mistake",
        "$9z$04$" + "x" * 53,  # unknown bcrypt variant tag
        "$2c$04$" + "x" * 53,  # plausible but unsupported variant letter
        "$2b$4$" + "x" * 53,  # single-digit cost
        "$2b$04$" + "!" * 53,  # right length, outside bcrypt's radix-64 alphabet
        "$2b$04$" + "x" * 52,  # one character short of a complete digest
        "$2b$04$" + "x" * 54,  # one character too long
        # --- the truncated-body family: a VALID `$2b$NN$` prefix with a short tail. This is the
        # dangerous shape: bcrypt 4.x slices [..22] off the remainder to read the salt, and a
        # shorter remainder is a Rust panic (pyo3_runtime.PanicException) rather than a
        # ValueError. It does not derive from Exception, so it is uncatchable by any ordinary
        # handler — which is why BCRYPT_HASH_RE rejects these before they reach Rust at all.
        "$2b$04$too-short",
        "$2b$12$short",
        "$2b$04$" + "x" * 20,
        "$2b$10$",
    ],
)
def test_verify_password_returns_false_on_garbage_hash(garbage):
    """A corrupt stored hash is an auth failure, not a 500 and not an interpreter-level panic."""
    assert verify_password("anything", garbage) is False


def test_bcrypt_hash_shape_check_accepts_real_hashes():
    """The pre-check must never be tightened into rejecting a digest bcrypt actually produced.

    ``BCRYPT_HASH_RE`` sits in front of every password verification, so a pattern that is too
    strict does not fail loudly — it silently rejects every login in the process. This pins it
    against real ``bcrypt.gensalt`` output at both cost spellings: zero-padded (``$04$``) and
    not (``$10$``). The production cost of 12 is deliberately not exercised here — it is ~250 ms
    and its hash is shape-identical to 10's.
    """
    for rounds in (4, 5, 10):
        hashed = hash_password("a-real-password", rounds=rounds)

        assert len(hashed) == 60
        assert BCRYPT_HASH_RE.fullmatch(hashed) is not None, hashed
        # And the end-to-end path still works, not just the regex.
        assert verify_password("a-real-password", hashed) is True
        assert verify_password("wrong", hashed) is False

    # The four demo users' stored hashes pass the same check.
    for user in get_user_directory(ROUNDS).values():
        assert BCRYPT_HASH_RE.fullmatch(user.password_hash) is not None


# =============================================================================================
# The role ladder
# =============================================================================================


def test_role_order_is_strictly_increasing():
    """viewer < analyst < writer < admin, with no ties and no gaps in the enumeration."""
    ladder = [Role.VIEWER, Role.ANALYST, Role.WRITER, Role.ADMIN]

    assert set(ROLE_ORDER) == set(Role)
    assert [ROLE_ORDER[role] for role in ladder] == [0, 1, 2, 3]

    ordinals = [ROLE_ORDER[role] for role in ladder]
    assert all(lower < higher for lower, higher in zip(ordinals, ordinals[1:]))
    # Distinct ordinals: two roles sharing a rank would make the ladder silently non-strict.
    assert len(set(ROLE_ORDER.values())) == len(Role)


def test_role_satisfies_is_inclusive():
    """All 4x4 pairs: a role satisfies itself and everything below it, and nothing above."""
    ladder = [Role.VIEWER, Role.ANALYST, Role.WRITER, Role.ADMIN]

    checked = 0
    for held_index, held in enumerate(ladder):
        for minimum_index, minimum in enumerate(ladder):
            expected = held_index >= minimum_index
            assert role_satisfies(held, minimum) is expected, (
                f"role_satisfies({held}, {minimum}) should be {expected}"
            )
            checked += 1
    assert checked == 16

    # The two cases the README calls out by name.
    assert role_satisfies(Role.ADMIN, Role.VIEWER) is True
    assert role_satisfies(Role.VIEWER, Role.ANALYST) is False


def test_role_satisfies_accepts_raw_strings():
    """Roles arrive as plain strings from a decoded token; the comparison must still work."""
    assert role_satisfies("admin", "writer") is True
    assert role_satisfies("viewer", "admin") is False

    with pytest.raises(ValueError):
        role_satisfies("superuser", "viewer")


# =============================================================================================
# Bootstrap users
# =============================================================================================


def test_dev_users_have_expected_roles_and_tiers():
    """All four demo accounts, exact role and exact tier."""
    directory = get_user_directory(ROUNDS)

    assert set(directory) == {"viewer", "analyst", "writer", "admin"}

    expected = {
        "viewer": (Role.VIEWER, Tier.FREE),
        "analyst": (Role.ANALYST, Tier.PRO),
        "writer": (Role.WRITER, Tier.PRO),
        "admin": (Role.ADMIN, Tier.ENTERPRISE),
    }
    for username, (role, tier) in expected.items():
        user = directory[username]
        assert user.username == username
        assert user.role is role
        assert user.tier is tier
        assert user.password_hash.startswith("$2b$")


def test_dev_passwords_mirror_the_account_table():
    """DEV_PASSWORDS is derived from DEV_ACCOUNTS, so the two can never list different users."""
    assert set(DEV_PASSWORDS) == set(DEV_ACCOUNTS)
    assert DEV_PASSWORDS == {
        "viewer": "viewer-dev-pw",
        "analyst": "analyst-dev-pw",
        "writer": "writer-dev-pw",
        "admin": "admin-dev-pw",
    }


@pytest.mark.parametrize(("username", "password"), sorted(DEV_PASSWORDS.items()))
def test_dev_passwords_authenticate(username, password):
    """Every credential the E2E/load harnesses import must actually log in."""
    user = authenticate(username, password, rounds=ROUNDS)

    assert user is not None
    assert user.username == username
    assert user.role is DEV_ACCOUNTS[username][1]
    assert user.tier is DEV_ACCOUNTS[username][2]


def test_authenticate_rejects_wrong_password():
    assert authenticate("analyst", "analyst-dev-pw!", rounds=ROUNDS) is None
    assert authenticate("analyst", "", rounds=ROUNDS) is None
    # A valid password belonging to a *different* account is still wrong for this one.
    assert authenticate("analyst", "admin-dev-pw", rounds=ROUNDS) is None


def test_authenticate_rejects_unknown_user():
    assert authenticate("mallory", "any-password", rounds=ROUNDS) is None
    assert authenticate("", "any-password", rounds=ROUNDS) is None
    # Usernames are case-sensitive; "Admin" is not "admin".
    assert authenticate("Admin", "admin-dev-pw", rounds=ROUNDS) is None


def test_unknown_user_and_wrong_password_take_similar_time():
    """Both failure paths must cost a full bcrypt verification — no enumeration oracle.

    The assertion band is deliberately wide. The point is not to measure bcrypt precisely (a
    shared CI container is far too noisy for that); it is to catch the specific regression where
    someone "optimises" ``authenticate`` with an early ``return None`` on a directory miss. That
    change makes the unknown-user path a dict lookup — microseconds against milliseconds, a
    ratio around 0.001 — which this band catches by three orders of magnitude while tolerating
    any amount of ordinary scheduling jitter.

    ``min`` over several attempts rather than a mean: the fastest observed run is the least
    noise-contaminated estimate of the true cost, and an early return would drive the minimum
    down hard.
    """
    # Prime the cache (four user hashes AND the dummy hash) so the measurement covers only the
    # per-call verification cost, not one-off setup.
    get_user_directory(ROUNDS)

    def fastest(call, attempts: int = 7) -> float:
        best = float("inf")
        for _ in range(attempts):
            start = time.perf_counter()
            call()
            best = min(best, time.perf_counter() - start)
        return best

    wrong_password = fastest(
        lambda: authenticate("analyst", "definitely-not-the-password", rounds=ROUNDS)
    )
    unknown_user = fastest(
        lambda: authenticate("no-such-user", "definitely-not-the-password", rounds=ROUNDS)
    )

    assert wrong_password > 0, "wrong-password path did not run bcrypt at all"
    ratio = unknown_user / wrong_password
    assert 0.25 <= ratio <= 4.0, (
        "unknown-username and wrong-password paths must cost the same order of magnitude "
        f"(unknown={unknown_user * 1000:.3f} ms, wrong={wrong_password * 1000:.3f} ms, "
        f"ratio={ratio:.4f}) — an early `return None` on a directory miss makes this endpoint "
        "a user-enumeration oracle"
    )


def test_user_directory_is_cached():
    """The lru_cache factory returns the same object for the same work factor.

    Without the cache, every login would re-hash all four demo passwords — ~1 s per request at
    the production cost of 12.
    """
    first = get_user_directory(ROUNDS)
    second = get_user_directory(ROUNDS)

    assert first is second
    assert get_user_directory(ROUNDS + 1) is not first

    # Read-only: a shared, process-wide directory that any caller could edit would be a very
    # quiet way to add an account at runtime.
    with pytest.raises(TypeError):
        first["intruder"] = first["admin"]  # type: ignore[index]


# =============================================================================================
# Token issue
# =============================================================================================


def test_token_carries_sub_role_tier_iat_exp(auth_settings):
    """Decoded with PyJWT directly: exactly the five documented claims, with the right values."""
    before = datetime.now(UTC)
    token, expires_at = create_access_token(
        subject="analyst",
        role=Role.ANALYST,
        tier=Tier.PRO,
        settings=auth_settings,
    )

    claims = jwt.decode(token, auth_settings.jwt_secret, algorithms=["HS256"])

    # Exactly these five — no extras leaking in, none missing.
    assert set(claims) == set(TOKEN_CLAIMS) == {"sub", "role", "tier", "iat", "exp"}
    assert claims["sub"] == "analyst"
    assert claims["role"] == "analyst"
    assert claims["tier"] == "pro"

    # Both timestamps are integer epoch seconds, not floats or ISO strings.
    assert isinstance(claims["iat"], int)
    assert isinstance(claims["exp"], int)

    # `iat` is now (allowing for the one-second truncation), `exp` is iat + the configured TTL.
    assert claims["exp"] - claims["iat"] == auth_settings.access_token_ttl_min * 60
    assert int(before.timestamp()) - 1 <= claims["iat"] <= int(datetime.now(UTC).timestamp())

    # The returned expires_at is exactly the `exp` claim — no sub-second drift between what the
    # server reports to the client and what the token actually says.
    assert int(expires_at.timestamp()) == claims["exp"]
    assert expires_at.tzinfo is not None


def test_token_ttl_follows_settings():
    """A different ACCESS_TOKEN_TTL_MIN produces a correspondingly different exp."""
    settings = build_settings(access_token_ttl_min=5)

    token, expires_at = create_access_token(
        subject="viewer", role=Role.VIEWER, tier=Tier.FREE, settings=settings
    )
    claims = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])

    assert claims["exp"] - claims["iat"] == 5 * 60
    assert expires_at - datetime.fromtimestamp(claims["iat"], UTC) == timedelta(minutes=5)


# =============================================================================================
# Token verification
# =============================================================================================


def test_decode_accepts_freshly_issued_token(auth_settings):
    token, expires_at = create_access_token(
        subject="writer",
        role=Role.WRITER,
        tier=Tier.PRO,
        settings=auth_settings,
    )

    principal = decode_token(token, settings=auth_settings)

    assert isinstance(principal, Principal)
    assert principal.subject == "writer"
    assert principal.role is Role.WRITER
    assert principal.tier is Tier.PRO
    assert principal.expires_at == expires_at
    assert principal.expires_at - principal.issued_at == timedelta(
        minutes=auth_settings.access_token_ttl_min
    )
    assert principal.issued_at.tzinfo is not None

    # Frozen: a handler must not be able to promote itself after the gate approved it.
    with pytest.raises(ValidationError):
        principal.role = Role.ADMIN


def test_expired_token_rejected(auth_settings):
    """Issued two hours ago with a 30-minute TTL — expired before it was ever presented."""
    long_ago = datetime.now(UTC) - timedelta(hours=2)
    token, expires_at = create_access_token(
        subject="analyst",
        role=Role.ANALYST,
        tier=Tier.PRO,
        settings=auth_settings,
        now=long_ago,
    )

    assert expires_at < datetime.now(UTC)

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "expired" in excinfo.value.reason


def test_injected_future_clock_expires_a_fresh_token(auth_settings):
    """The `now` seam expires a valid token without a single sleep."""
    token, _ = create_access_token(
        subject="analyst", role=Role.ANALYST, tier=Tier.PRO, settings=auth_settings
    )

    # Valid right now...
    assert decode_token(token, settings=auth_settings).subject == "analyst"

    # ...and dead once the clock is moved past its TTL.
    future = datetime.now(UTC) + timedelta(minutes=auth_settings.access_token_ttl_min + 1)
    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings, now=future)
    assert "expired" in excinfo.value.reason


def test_tampered_payload_rejected(auth_settings):
    """Rewriting the claims without the key cannot produce an acceptable token."""
    token, _ = create_access_token(
        subject="viewer", role=Role.VIEWER, tier=Tier.FREE, settings=auth_settings
    )
    header_seg, payload_seg, signature_seg = token.split(".")

    # (a) The actual attack: keep the signature, escalate the role in the payload.
    claims = json.loads(b64url_decode(payload_seg))
    assert claims["role"] == "viewer"
    claims["role"] = "admin"
    escalated = f"{header_seg}.{encode_segment(claims)}.{signature_seg}"

    with pytest.raises(AuthError) as excinfo:
        decode_token(escalated, settings=auth_settings)
    assert "signature" in excinfo.value.reason

    # (b) The blunt version: flip a single character of the payload segment.
    index = 5
    replacement = "A" if payload_seg[index] != "A" else "B"
    flipped_seg = payload_seg[:index] + replacement + payload_seg[index + 1 :]
    flipped = f"{header_seg}.{flipped_seg}.{signature_seg}"

    assert flipped != token
    with pytest.raises(AuthError):
        decode_token(flipped, settings=auth_settings)


def test_tampered_signature_rejected(auth_settings):
    """Editing the signature itself is the same failure as editing the payload."""
    token, _ = create_access_token(
        subject="viewer", role=Role.VIEWER, tier=Tier.FREE, settings=auth_settings
    )
    header_seg, payload_seg, signature_seg = token.split(".")
    replacement = "A" if signature_seg[0] != "A" else "B"
    forged = f"{header_seg}.{payload_seg}.{replacement}{signature_seg[1:]}"

    with pytest.raises(AuthError):
        decode_token(forged, settings=auth_settings)


def test_token_signed_with_other_secret_rejected(auth_settings):
    """A perfectly well-formed token signed with somebody else's key is not our token."""
    foreign = build_settings(jwt_secret=OTHER_SECRET)
    token, _ = create_access_token(
        subject="admin", role=Role.ADMIN, tier=Tier.ENTERPRISE, settings=foreign
    )

    # It is genuinely valid — against the key that signed it.
    assert decode_token(token, settings=foreign).role is Role.ADMIN

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "signature" in excinfo.value.reason


def test_alg_none_token_rejected(auth_settings):
    """The headline security test: an unsigned ``{"alg":"none"}`` forgery must not be accepted.

    This is a *genuine* forgery, not a malformed string: the header and payload are valid
    base64url JSON, the claim set is exactly what this service issues, and the requested role is
    ``admin``. A decoder that honours the token's own ``alg`` field — i.e. one that passes
    ``algorithms=None`` or includes ``"none"`` in the allowlist — accepts it and hands the
    bearer an admin session with no key at all. The assertion below is what
    ``algorithms=[settings.jwt_algorithm]`` in ``decode_token`` buys.
    """
    header = encode_segment({"alg": "none", "typ": "JWT"})
    claims = valid_claims(sub="mallory", role="admin", tier="enterprise")
    forged = f"{header}.{encode_segment(claims)}."

    # Sanity: the forgery is well-formed, so the rejection below is about the ALGORITHM and not
    # about the token being unparseable. A permissive decoder reads it back perfectly.
    assert jwt.decode(forged, options={"verify_signature": False}) == claims

    with pytest.raises(AuthError) as excinfo:
        decode_token(forged, settings=auth_settings)
    assert "algorithm" in excinfo.value.reason

    # The same forgery with junk in the signature slot — some libraries treat an empty third
    # segment as "malformed" and a populated one as "unsigned"; both must fail here.
    with pytest.raises(AuthError):
        decode_token(f"{header}.{encode_segment(claims)}.ZmFrZS1zaWc", settings=auth_settings)


def test_token_signed_with_unexpected_algorithm_rejected(auth_settings):
    """HS512 is a real algorithm and a real signature — it is still not the one we allow."""
    token = jwt.encode(valid_claims(), auth_settings.jwt_secret, algorithm="HS512")

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "algorithm" in excinfo.value.reason


def test_token_with_unknown_role_rejected(auth_settings):
    """Signed with the REAL key, but claiming a role that does not exist: reject, never coerce."""
    token = jwt.encode(
        valid_claims(sub="mallory", role="superuser"),
        auth_settings.jwt_secret,
        algorithm="HS256",
    )

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "role" in excinfo.value.reason


def test_token_with_unknown_tier_rejected(auth_settings):
    """An unrecognised tier has no rate-limit bucket, and "no bucket" would mean unlimited."""
    token = jwt.encode(
        valid_claims(tier="unlimited"), auth_settings.jwt_secret, algorithm="HS256"
    )

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "tier" in excinfo.value.reason


def test_token_missing_sub_rejected(auth_settings):
    """An absent `sub` is a missing-claim rejection; an empty one is caught explicitly."""
    absent = valid_claims()
    del absent["sub"]
    token = jwt.encode(absent, auth_settings.jwt_secret, algorithm="HS256")

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert "sub" in excinfo.value.reason

    # An empty string is present as far as PyJWT's `require` check is concerned, so auth.py
    # tests it itself — an anonymous principal must not be constructible.
    empty = jwt.encode(valid_claims(sub=""), auth_settings.jwt_secret, algorithm="HS256")
    with pytest.raises(AuthError) as excinfo:
        decode_token(empty, settings=auth_settings)
    assert "sub" in excinfo.value.reason


@pytest.mark.parametrize("claim", ["role", "tier", "iat", "exp"])
def test_token_missing_any_required_claim_rejected(auth_settings, claim):
    """Every one of the five claims is required; none of them defaults."""
    claims = valid_claims()
    del claims[claim]
    token = jwt.encode(claims, auth_settings.jwt_secret, algorithm="HS256")

    with pytest.raises(AuthError) as excinfo:
        decode_token(token, settings=auth_settings)
    assert claim in excinfo.value.reason


@pytest.mark.parametrize(
    "garbage",
    [
        "",
        "not-a-token",
        "only.two",
        "a.b.c",
        "....",
        "Bearer eyJhbGciOiJIUzI1NiJ9.e30.sig",  # the scheme word left on by mistake
    ],
)
def test_malformed_token_raises_auth_error_not_a_pyjwt_exception(auth_settings, garbage):
    """No raw PyJWT exception may escape — one that did would surface as a 500, not a 401."""
    with pytest.raises(AuthError):
        decode_token(garbage, settings=auth_settings)


def test_round_trip_for_every_demo_account(auth_settings):
    """Issue and verify a token for all four accounts: role and tier survive the round trip."""
    for username, (_password, role, tier) in DEV_ACCOUNTS.items():
        token, _ = create_access_token(
            subject=username, role=role, tier=tier, settings=auth_settings
        )
        principal = decode_token(token, settings=auth_settings)

        assert principal.subject == username
        assert principal.role is role
        assert principal.tier is tier

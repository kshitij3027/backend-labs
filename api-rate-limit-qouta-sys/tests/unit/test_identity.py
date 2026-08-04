"""Unit tests for :mod:`src.identity` — header parsing, the digest, the cache, and JWT rejection.

The properties asserted here are the ones whose regression would be **silent**:

1. **The raw API key never leaves this process.** Not into a Redis key name, not into a log line,
   not onto a :class:`~src.models.Principal`. A leak here is not a bug that fails a request — it is
   a credential sitting in a log aggregator, replayable verbatim, discovered months later.
2. **The pepper is load-bearing.** The same key under a different pepper must produce a different
   digest, or "the pepper lives in the environment and never in Redis" is decoration.
3. **A JWT never chooses how it is verified.** ``alg: none`` and an algorithm other than the
   configured one are both refused. A decoder that honours the token's own ``alg`` accepts a
   forgery with no signature at all, and the forged ``sub`` is whatever the attacker wants.
4. **A ``tier`` claim in a token has no effect.** Identity from the token, authority from the store.
5. **The cache is an LRU with a TTL, and it caches negatives.** A positive hit, a negative hit and
   an expiry are three different code paths with three different counters, and a "cache" that is
   really a FIFO evicts the busiest customer first.
6. **A Redis failure propagates.** C8 must decide the fail-open question for identity deliberately;
   inheriting a silent default from this module would make an outage an authentication bypass.

The clock is injected, so every TTL assertion is exact rather than slept for.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
import pytest

from src.config import Settings
from src.identity import (
    ACCEPTED_SCHEMES,
    API_KEY_HEADER,
    APIKEY_SCHEME,
    AUTH_REALM,
    AUTHORIZATION_HEADER,
    BEARER_SCHEME,
    DEMO_CREDENTIALS,
    DEMO_KEY_BY_TIER,
    DEMO_KEY_BY_USER,
    HTTP_OWS,
    IDENTITY_CACHE_MAX_ENTRIES,
    IDENTITY_CACHE_TTL_SEC,
    SCHEME_KINDS,
    STATUS_ACTIVE,
    WWW_AUTHENTICATE,
    IdentityResolver,
    apikey_digest,
    header_value,
    identity_concurrency,
    issue_token,
    parse_credential,
    seed_demo_credentials,
    verify_api_key,
)
from src.keys import apikey_key, user_key
from src.models import CredentialKind, Principal, Tier
from src.redis_client import BackingStoreUnavailable

# ---------------------------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------------------------


class FakeClient:
    """The three commands :mod:`src.identity` issues, over a plain dict of HASHes.

    Values are stored as ``bytes`` because the real gateway is built with
    ``decode_responses=False`` — a double that handed back ``str`` would let a decoding bug in
    ``_decode_record`` pass every unit test and fail against the real server.
    """

    def __init__(self, owner: FakeGateway) -> None:
        self._owner = owner

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        self._owner.reads.append(key)
        return dict(self._owner.store.get(key, {}))

    def _hsetnx(self, key: str, field: str, value: str) -> int:
        self._owner.writes.append(("hsetnx", key, field, value))
        bucket = self._owner.store.setdefault(key, {})
        if field.encode() in bucket:
            return 0
        bucket[field.encode()] = value.encode()
        return 1

    def _hset(self, key: str, mapping: dict[str, str] | None = None) -> int:
        bucket = self._owner.store.setdefault(key, {})
        created = 0
        for field, value in (mapping or {}).items():
            self._owner.writes.append(("hset", key, field, value))
            created += int(field.encode() not in bucket)
            bucket[field.encode()] = value.encode()
        return created

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self, transaction=transaction)


class FakePipeline:
    """A command buffer that applies everything on ONE ``execute()``.

    The whole point of the double is the counter: :attr:`FakeGateway.round_trips` increments once
    per ``execute()``, so "the seed is one round trip" is an assertion rather than a claim about
    code that happens to be written with a pipeline.
    """

    def __init__(self, client: FakeClient, *, transaction: bool) -> None:
        self._client = client
        self.transaction = transaction
        self._buffered: list[tuple[str, tuple, dict]] = []
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakePipeline:
        self.entered = True
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.exited = True

    # Buffering methods return `self` WITHOUT being awaited — the same contract redis-py's async
    # Pipeline has. A double whose commands were coroutines would let `pipe.hsetnx(...)` without an
    # await pass here and emit a "coroutine was never awaited" warning against the real client.
    def hsetnx(self, key: str, field: str, value: str) -> FakePipeline:
        self._buffered.append(("hsetnx", (key, field, value), {}))
        return self

    def hset(self, key: str, mapping: dict[str, str] | None = None) -> FakePipeline:
        self._buffered.append(("hset", (key,), {"mapping": mapping}))
        return self

    async def execute(self) -> list[int]:
        self._client._owner.round_trips += 1
        replies: list[int] = []
        for name, args, kwargs in self._buffered:
            handler = self._client._hsetnx if name == "hsetnx" else self._client._hset
            replies.append(handler(*args, **kwargs))
        self._buffered.clear()
        return replies


class FakeGateway:
    """A :class:`~src.redis_client.RedisGateway` stand-in that actually runs the factory.

    Running the factory matters: the resolver passes a *lambda*, and a double that only recorded
    the call would not catch a resolver that built the wrong key name inside it.
    """

    def __init__(self, *, error: Exception | None = None) -> None:
        # Values are ``bytes`` by default (see :class:`FakeClient`); one test deliberately stores
        # ``str`` to exercise the decoder's tolerance of a ``decode_responses=True`` client, so the
        # annotation is deliberately loose rather than a lie.
        self.store: dict[str, dict[Any, Any]] = {}
        self.error = error
        self.reads: list[str] = []
        self.writes: list[tuple[str, str, str, str]] = []
        self.ops: list[str] = []
        #: One per `pipeline.execute()`. The seed's round-trip budget is asserted off this.
        self.round_trips = 0
        self.client = FakeClient(self)

    async def run(self, factory, *, op: str):
        self.ops.append(op)
        if self.error is not None:
            # Raised WITHOUT calling the factory, exactly as the real gateway refuses when the
            # breaker is open: an un-awaited coroutine would be a RuntimeWarning, and the point of
            # the refusal is that nothing is built on the way to it.
            raise self.error
        return await factory()


class FakeClock:
    """A monotonic clock a test drives by hand, so TTL boundaries are hit exactly."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------

#: A raw API key that looks like the real thing: not hex, not 64 characters, so it could never be
#: mistaken for a digest by `apikey_key`'s validator.
RAW_KEY = "rlq_live_9f2c4b8e-not-a-digest"


def headers(*pairs: tuple[str, str]) -> list[tuple[bytes, bytes]]:
    """Build a raw ASGI header list. Names are NOT lower-cased — that is the case test's subject."""
    return [(name.encode("latin-1"), value.encode("latin-1")) for name, value in pairs]


def store_api_key(
    gateway: FakeGateway,
    settings: Settings,
    raw_key: str = RAW_KEY,
    *,
    user_id: str = "alice",
    label: str | None = "ci-runner",
    status: str = STATUS_ACTIVE,
) -> str:
    """Write an ``apikey:v1:<digest>`` record and return the digest."""
    digest = apikey_digest(raw_key, pepper=settings.api_key_pepper)
    record = {
        b"user_id": user_id.encode(),
        b"status": status.encode(),
        b"created_at": b"1754800000",
    }
    if label is not None:
        record[b"label"] = label.encode()
    gateway.store[apikey_key(digest)] = record
    return digest


def _b64(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def forge_alg_none(claims: dict[str, object]) -> str:
    """Hand-build the classic forgery: ``{"alg":"none"}`` with the signature segment removed."""
    header = _b64(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    body = _b64(json.dumps(claims).encode())
    return f"{header}.{body}."


def live_claims(subject: str = "alice", **extra: object) -> dict[str, object]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        **extra,
    }


@pytest.fixture()
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def resolver(gateway: FakeGateway, settings: Settings, clock: FakeClock) -> IdentityResolver:
    return IdentityResolver(gateway, settings, clock=clock)


# =============================================================================================
# Header parsing
# =============================================================================================


def test_header_value_reads_a_header_and_strips_it():
    assert header_value(headers(("x-api-key", "  abc  ")), API_KEY_HEADER) == "abc"


def test_header_value_is_case_insensitive_in_the_header_name():
    """ASGI lower-cases header names. This does not rely on that.

    The resolver is also driven by tests, by C13's harness and (one day) by a server that has not
    read the spec; folding case here costs one `.lower()` on a short list and removes a class of
    "works in production, mysteriously 401s in the test" bug.
    """
    assert header_value(headers(("X-API-Key", "abc")), API_KEY_HEADER) == "abc"
    assert header_value(headers(("AUTHORIZATION", "Bearer t")), AUTHORIZATION_HEADER) == "Bearer t"


def test_header_value_reports_an_absent_or_empty_header_as_none():
    """`X-API-Key:` with nothing after it is a header carrying no credential, i.e. absent."""
    assert header_value(headers(), API_KEY_HEADER) is None
    assert header_value(headers(("accept", "*/*")), API_KEY_HEADER) is None
    assert header_value(headers(("x-api-key", "   ")), API_KEY_HEADER) is None


def test_header_value_never_raises_on_a_non_utf8_byte():
    """latin-1 is total. A UTF-8 decode would make a garbage header a 500 anyone can trigger."""
    assert header_value([(b"x-api-key", b"\xff\xfe")], API_KEY_HEADER) == "\xff\xfe"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Bearer tok-123", (CredentialKind.JWT, "tok-123")),
        ("bearer tok-123", (CredentialKind.JWT, "tok-123")),
        ("BEARER tok-123", (CredentialKind.JWT, "tok-123")),
        ("Bearer   tok-123", (CredentialKind.JWT, "tok-123")),
        ("Bearer\ttok-123", (CredentialKind.JWT, "tok-123")),
        ("ApiKey k-1", (CredentialKind.API_KEY, "k-1")),
        ("apikey k-1", (CredentialKind.API_KEY, "k-1")),
        ("APIKEY k-1", (CredentialKind.API_KEY, "k-1")),
    ],
    ids=[
        "bearer",
        "bearer-lower",
        "bearer-upper",
        "bearer-multi-space",
        "bearer-tab",
        "apikey",
        "apikey-lower",
        "apikey-upper",
    ],
)
def test_authorization_schemes_are_case_insensitive(value, expected):
    """RFC 9110 makes the auth-scheme case-insensitive; a client that shouts still authenticates."""
    assert parse_credential(headers(("authorization", value))) == expected


def test_x_api_key_header_is_the_third_option():
    assert parse_credential(headers(("x-api-key", "k-1"))) == (CredentialKind.API_KEY, "k-1")


@pytest.mark.parametrize(
    "given",
    [
        headers(),
        headers(("accept", "*/*")),
        headers(("authorization", "")),
        headers(("authorization", "Bearer")),
        headers(("authorization", "Basic dXNlcjpwdw==")),
        headers(("authorization", "Negotiate abc")),
        headers(("x-api-key", "")),
    ],
    ids=[
        "no-headers",
        "unrelated-header",
        "empty-authorization",
        "bearer-with-no-token",
        "basic-scheme",
        "unknown-scheme",
        "empty-x-api-key",
    ],
)
def test_nothing_usable_resolves_to_no_credential(given):
    """Every unparseable shape ends at None — which C6 turns into ONE 401, not seven."""
    assert parse_credential(given) is None


def test_bearer_wins_over_a_simultaneously_present_api_key():
    """Documented precedence, asserted. First match wins and the order is Authorization first."""
    presented = headers(("authorization", "Bearer tok-123"), ("x-api-key", "k-1"))

    assert parse_credential(presented) == (CredentialKind.JWT, "tok-123")


def test_an_unreadable_authorization_header_falls_through_to_the_api_key():
    """A header this service does not speak is not a credential, and must not veto a real one.

    Aborting on `Basic` would let any proxy that attaches one deny service to every API-key caller
    behind it — a denial of service with a security-sounding justification. Nothing is *granted*:
    the fall-through still authenticates only whoever presented a valid key.
    """
    presented = headers(("authorization", "Basic dXNlcjpwdw=="), ("x-api-key", "k-1"))

    assert parse_credential(presented) == (CredentialKind.API_KEY, "k-1")


def test_the_scheme_constants_are_lower_case():
    """They are compared against a lower-cased scheme; an upper-case constant never matches."""
    assert BEARER_SCHEME == BEARER_SCHEME.lower()
    assert APIKEY_SCHEME == APIKEY_SCHEME.lower()


# ---------------------------------------------------------------------------------------------
# HTTP optional whitespace is SP and HTAB — and nothing else
# ---------------------------------------------------------------------------------------------

#: The four byte sequences measured to authenticate as the bare key under a naive `str.strip()`.
#: NBSP, the C0 file/group/record/unit separators, NEL and vertical tab are all "whitespace" to
#: Python and none of them are HTTP optional whitespace.
NON_OWS_PADDING = ["\xa0", "\x1c", "\x85", "\x0b", "\x1d", "\x1e", "\x1f", "\x0c"]


def test_http_ows_is_exactly_sp_and_htab():
    """RFC 9110 §5.6.3: ``OWS = *( SP / HTAB )``. Pinned, because the default is wider."""
    assert HTTP_OWS == " \t"


@pytest.mark.parametrize("padding", NON_OWS_PADDING)
@pytest.mark.parametrize("position", ["suffix", "prefix"])
async def test_non_ows_padding_does_not_authenticate(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, padding, position
):
    """**One credential must have exactly ONE accepted spelling.**

    Bare ``str.strip()`` removes everything *Python* calls whitespace — U+00A0, U+0085, ``\\x0b``,
    ``\\x1c``-``\\x1f`` — while RFC 9110 optional whitespace is SP and HTAB only. Measured before
    the fix: ``demo-free-key\\xa0``, ``\\x1cdemo-free-key``, ``demo-free-key\\x85`` and
    ``demo-free-key\\x0b`` all authenticated as the same principal.

    That is not an escalation — the real key is still required — but it hands one credential a
    family of accepted spellings, and every consumer that treats the presented credential as an
    identifier then has a hole: exact-match audit logging records a string that differs from the
    one accepted, an upstream blocklist keyed on the credential is bypassed by appending a byte,
    and future per-credential accounting counts one caller as several.
    """
    store_api_key(gateway, settings)
    padded = padding + RAW_KEY if position == "prefix" else RAW_KEY + padding

    assert await resolver.resolve(headers(("x-api-key", padded))) is None
    assert await resolver.resolve(headers(("authorization", f"ApiKey {padded}"))) is None


@pytest.mark.parametrize("padding", NON_OWS_PADDING)
async def test_non_ows_between_the_scheme_and_the_token_is_not_a_credential(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, padding
):
    """The other half of the same bug: ``str.split()`` also treated these as separators.

    ``ApiKey\\xa0<key>`` must be one unrecognised scheme-shaped string, not a scheme plus a token.
    """
    store_api_key(gateway, settings)

    assert parse_credential(headers(("authorization", f"ApiKey{padding}{RAW_KEY}"))) is None
    assert await resolver.resolve(headers(("authorization", f"ApiKey{padding}{RAW_KEY}"))) is None


@pytest.mark.parametrize(
    "spelling",
    [f" {RAW_KEY}", f"{RAW_KEY} ", f"\t{RAW_KEY}\t", f"  \t {RAW_KEY} \t "],
    ids=["leading-sp", "trailing-sp", "htab-both", "mixed-run"],
)
async def test_real_ows_is_still_trimmed(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, spelling
):
    """Tightening the trim must not break the padding a real client or proxy actually emits."""
    store_api_key(gateway, settings)

    principal = await resolver.resolve(headers(("x-api-key", spelling)))

    assert principal is not None
    assert principal.user_id == "alice"


# ---------------------------------------------------------------------------------------------
# The 401 challenge advertises every accepted scheme
# ---------------------------------------------------------------------------------------------


def test_the_challenge_advertises_every_accepted_scheme():
    """**A 401 that names one of two accepted schemes is actively misleading.**

    It tells a client holding an API key that its credential type is not supported here. RFC 9110
    §11.6.1 allows a comma-separated challenge list, so both are advertised.

    The assertion is driven off ``ACCEPTED_SCHEMES``, which is also what ``parse_credential``
    dispatches on and what ``WWW_AUTHENTICATE`` is generated from — so a scheme added to the
    service without reaching the challenge cannot exist, and this test fails if that linkage is
    ever replaced by two hand-maintained lists.
    """
    assert ACCEPTED_SCHEMES, "there is at least one way to authenticate"

    for scheme in ACCEPTED_SCHEMES:
        assert f'{scheme.display} realm="{AUTH_REALM}"' in WWW_AUTHENTICATE
        assert scheme.display.lower() == scheme.token
        # Advertised AND actually accepted: the challenge must not name a scheme that 401s.
        assert parse_credential(headers(("authorization", f"{scheme.display} tok"))) == (
            scheme.kind,
            "tok",
        )

    # And nothing is accepted that is not advertised — the dispatch table IS the accepted set.
    assert set(SCHEME_KINDS) == {scheme.token for scheme in ACCEPTED_SCHEMES}
    assert parse_credential(headers(("authorization", "Basic dXNlcjpwdw=="))) is None


def test_the_challenge_covers_both_api_key_spellings():
    """``X-API-Key`` has no challenge form of its own, so the ``ApiKey`` challenge advertises it.

    The two spellings are interchangeable on input (asserted elsewhere), so one challenge honestly
    describes both.
    """
    assert "ApiKey" in WWW_AUTHENTICATE
    assert "Bearer" in WWW_AUTHENTICATE
    assert SCHEME_KINDS[APIKEY_SCHEME] is CredentialKind.API_KEY


# =============================================================================================
# The digest
# =============================================================================================


def test_digest_is_hmac_sha256_over_the_pepper(settings: Settings):
    """Spelled out independently of the implementation, so a "refactor" cannot redefine it."""
    expected = hmac.new(
        settings.api_key_pepper.encode(), RAW_KEY.encode(), hashlib.sha256
    ).hexdigest()

    assert apikey_digest(RAW_KEY, pepper=settings.api_key_pepper) == expected
    assert len(expected) == 64
    assert expected == expected.lower()


def test_the_same_key_under_a_different_pepper_is_a_different_digest():
    """**The pepper is load-bearing, and this is the proof.**

    If the digest did not depend on the pepper, "the pepper lives in the process environment and
    never in Redis" would be a sentence rather than a defence: a stolen dump would be a list of
    plain SHA-256 digests that anyone could reproduce, and rotating the pepper would invalidate
    nothing.
    """
    one = apikey_digest(RAW_KEY, pepper="pepper-one-0123456789")
    two = apikey_digest(RAW_KEY, pepper="pepper-two-0123456789")

    assert one != two
    # And a key presented to a deployment with a different pepper names no record at all.
    assert apikey_key(one) != apikey_key(two)


def test_verify_api_key_is_a_constant_time_match(settings: Settings):
    digest = apikey_digest(RAW_KEY, pepper=settings.api_key_pepper)

    assert verify_api_key(RAW_KEY, digest, pepper=settings.api_key_pepper) is True
    assert verify_api_key("some-other-key", digest, pepper=settings.api_key_pepper) is False
    assert verify_api_key(RAW_KEY, digest, pepper="a-completely-different-pepper") is False


def test_the_resolver_digest_helper_uses_the_configured_pepper(
    resolver: IdentityResolver, settings: Settings
):
    assert resolver.digest(RAW_KEY) == apikey_digest(RAW_KEY, pepper=settings.api_key_pepper)


# =============================================================================================
# The raw key never leaves this process
# =============================================================================================


async def test_the_raw_key_never_reaches_a_redis_key_name_or_a_principal(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, caplog
):
    """**The credential-leak test.** Three places the raw key must never appear.

    A Redis key name is the worst of the three: it lands in MONITOR output, in slowlog entries, in
    the RDB/AOF file and in every backup of them. `src.keys.apikey_key` refuses a non-digest, so
    this asserts the *composition* is right — the resolver hashes before it builds a key.
    """
    digest = store_api_key(gateway, settings)

    with caplog.at_level(logging.DEBUG):
        principal = await resolver.resolve(headers(("x-api-key", RAW_KEY)))
        # And the unhappy paths, which are the ones that log.
        await resolver.resolve(headers(("x-api-key", "totally-unknown-key")))

    assert principal == Principal(
        user_id="alice", credential=CredentialKind.API_KEY, key_id="ci-runner"
    )

    # 1. The Redis key name is the digest, and only the digest.
    assert gateway.reads[0] == f"apikey:v1:{digest}"
    assert all(RAW_KEY not in key for key in gateway.reads)

    # 2. `key_id` is the operator-chosen LABEL — never the key (replayable) and never the digest
    #    (the lookup handle for the whole record, so it correlates every log line about a user).
    assert principal.key_id == "ci-runner"
    assert principal.key_id != RAW_KEY
    assert principal.key_id != digest

    # 3. Nothing that was logged carries either the key or the digest.
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert RAW_KEY not in logged
    assert digest not in logged
    assert "totally-unknown-key" not in logged


async def test_a_record_without_a_label_carries_no_key_id(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """An absent name is better than a fabricated one — and far better than the digest."""
    store_api_key(gateway, settings, label=None)

    principal = await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert principal is not None
    assert principal.key_id is None


# =============================================================================================
# API-key resolution
# =============================================================================================


async def test_an_unknown_key_resolves_to_no_principal(resolver: IdentityResolver):
    assert await resolver.resolve(headers(("x-api-key", "no-such-key"))) is None


@pytest.mark.parametrize("status", ["revoked", "suspended", "", "ACTIVE", "actve"])
async def test_a_key_that_is_not_active_does_not_authenticate(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, status
):
    """**Revocation is the whole point of the status field.**

    An allowlist, not a denylist: `ACTIVE` and `actve` fail shut. A denylist of known-bad statuses
    would mean a typo in a revocation (`revokd`) leaves the key working, which is the one direction
    this check must not fail in.
    """
    store_api_key(gateway, settings, status=status)

    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is None


@pytest.mark.parametrize(
    "user_id",
    ["", "alice}x{bob"],
    ids=["empty", "brace-forges-a-hash-tag"],
)
async def test_a_record_with_an_unusable_user_id_does_not_authenticate(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, user_id
):
    """An id that cannot be safely hash-tagged is refused at the door, not deep in the limiter.

    An empty id would put every unidentified principal in one shared bucket; a braced one can
    collide with another principal's Redis Cluster slot (see `src.keys.sanitise_user_id`). Both
    would otherwise surface as a ValueError from inside `bucket_key` on the request path.
    """
    gateway.store[apikey_key(apikey_digest(RAW_KEY, pepper=settings.api_key_pepper))] = {
        b"user_id": user_id.encode(),
        b"status": STATUS_ACTIVE.encode(),
    }

    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is None


async def test_a_record_missing_user_id_entirely_does_not_authenticate(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    gateway.store[apikey_key(apikey_digest(RAW_KEY, pepper=settings.api_key_pepper))] = {
        b"status": STATUS_ACTIVE.encode()
    }

    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is None


async def test_a_record_from_a_decoding_client_is_tolerated(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """The decoder handles ``str`` fields as well as ``bytes``, and that is not busywork.

    The shipped gateway uses ``decode_responses=False`` because the decision script's 19-element
    reply is parsed positionally into integers and a per-value UTF-8 pass would buy nothing. But
    that is a *gateway* setting, and an identity lookup that silently returned "no such key" the
    day someone flipped it — because ``record.get("status")`` missed a ``b"status"`` key — would be
    a total authentication outage whose cause is one keyword argument in a different module.
    """
    gateway.store[apikey_key(apikey_digest(RAW_KEY, pepper=settings.api_key_pepper))] = {
        "user_id": "alice",
        "status": STATUS_ACTIVE,
        "label": "laptop",
    }

    principal = await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert principal == Principal(
        user_id="alice", credential=CredentialKind.API_KEY, key_id="laptop"
    )


async def test_the_apikey_scheme_and_the_x_api_key_header_resolve_identically(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    store_api_key(gateway, settings)

    via_scheme = await resolver.resolve(headers(("authorization", f"ApiKey {RAW_KEY}")))
    via_header = await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert via_scheme == via_header
    assert via_scheme is not None
    assert via_scheme.credential is CredentialKind.API_KEY


async def test_no_credential_means_no_redis_call(resolver: IdentityResolver, gateway: FakeGateway):
    """An unauthenticated flood must not be a Redis flood. The refusal happens before the lookup."""
    assert await resolver.resolve(headers()) is None

    assert gateway.reads == []
    assert resolver.misses == 0


# =============================================================================================
# A backing-store failure propagates
# =============================================================================================


async def test_backing_store_unavailable_propagates_out_of_resolve(settings: Settings):
    """**Deliberately not caught. C8 must decide this case, and decide it visibly.**

    "We could not check your limits" and "we could not establish who you are" are different
    questions with different safe answers. Failing open on the first serves an unmetered request to
    a known customer; failing open on the second would serve an unauthenticated request to anyone
    holding any string, for as long as the outage lasted — the graceful-degradation path would
    become an authentication bypass.
    """
    broken = FakeGateway(error=BackingStoreUnavailable("redis is down", op="identity:apikey"))
    resolver = IdentityResolver(broken, settings)

    with pytest.raises(BackingStoreUnavailable):
        await resolver.resolve(headers(("x-api-key", RAW_KEY)))


async def test_a_failed_lookup_caches_nothing(settings: Settings):
    """An outage must not poison the cache with five seconds of "no such key" for a real key."""
    broken = FakeGateway(error=BackingStoreUnavailable("down", op="identity:apikey"))
    resolver = IdentityResolver(broken, settings)

    with pytest.raises(BackingStoreUnavailable):
        await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert resolver.cache_stats()["size"] == 0


# =============================================================================================
# The cache
# =============================================================================================


async def test_a_positive_hit_avoids_a_second_redis_call(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    store_api_key(gateway, settings)

    first = await resolver.resolve(headers(("x-api-key", RAW_KEY)))
    second = await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert first == second
    assert len(gateway.reads) == 1
    assert (resolver.hits, resolver.misses, resolver.negative_hits) == (1, 1, 0)


async def test_a_repeated_wrong_credential_costs_one_redis_call(
    resolver: IdentityResolver, gateway: FakeGateway
):
    """**What negative caching actually buys: it absorbs REPEATS, not enumeration.**

    The realistic case is a client that is misconfigured or retrying — a stale key in a CI job, a
    copy-paste error in a deploy, a retry loop hammering the same rejected string. Without this,
    that is one Redis round trip per attempt, forever, from a caller that will never succeed.

    See `test_distinct_guesses_are_not_absorbed_by_the_negative_cache` for the claim this
    deliberately does *not* make.
    """
    for _ in range(5):
        assert await resolver.resolve(headers(("x-api-key", "the-same-wrong-key"))) is None

    assert len(gateway.reads) == 1
    assert (resolver.hits, resolver.misses, resolver.negative_hits) == (4, 1, 4)
    # negative_hits is a SUBSET of hits, so the hit rate still describes all served lookups.
    assert resolver.cache_stats()["hit_rate"] == 0.8


async def test_distinct_guesses_are_not_absorbed_by_the_negative_cache(
    settings: Settings, gateway: FakeGateway
):
    """**The honest limit, asserted so the docstring cannot quietly drift back into overselling.**

    Enumeration uses *distinct* keys, so every guess has a distinct digest and costs a full round
    trip; the measured figure on the real thing was 20 001 round trips for 20 000 guesses. It also
    evicts legitimate entries from a bounded LRU, so real traffic pays extra Redis reads while the
    flood runs.

    Neither is fixable at this layer, and pretending otherwise is worse than saying so: the vector
    is documented in the module docstring against the resource it actually threatens (the shared
    connection pool and circuit breaker), where C8 has to make a decision about it.
    """
    resolver = IdentityResolver(gateway, settings, max_entries=4)
    legitimate = store_api_key(gateway, settings)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))
    reads_after_warmup = len(gateway.reads)

    for guess in range(10):
        assert await resolver.resolve(headers(("x-api-key", f"guess-{guess}"))) is None

    # One round trip per distinct guess. Nothing absorbed.
    assert len(gateway.reads) - reads_after_warmup == 10
    assert resolver.negative_hits == 0
    # And the legitimate principal was pushed out of the cache by the flood, so a real caller now
    # pays for a refetch. Bounded (entries live at most the TTL anyway), but real while it lasts.
    assert resolver.evictions == 7
    assert resolver.invalidate(legitimate) is False


async def test_the_cache_is_keyed_on_the_digest_and_not_on_the_raw_key(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """A process dump, a debugger or a stray repr must not yield replayable credentials."""
    digest = store_api_key(gateway, settings)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    # Invalidating BY DIGEST empties it, which is only possible if the digest is the key.
    assert resolver.invalidate(digest) is True
    assert resolver.cache_stats()["size"] == 0


async def test_an_expired_entry_is_refetched(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, clock: FakeClock
):
    """Serving a stale identity would mean authenticating a revoked key indefinitely.

    Note the contrast with `TierRegistry`, which deliberately serves its snapshot *past* the TTL:
    a five-second-old tier limit is a non-event, while a five-minute-old answer to "is this key
    still valid?" is a revocation that did not happen.
    """
    store_api_key(gateway, settings)

    await resolver.resolve(headers(("x-api-key", RAW_KEY)))
    clock.advance(IDENTITY_CACHE_TTL_SEC - 0.001)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))
    assert len(gateway.reads) == 1, "still inside the TTL"

    # `>=` at the boundary: at exactly the TTL the entry has lived its full configured life.
    clock.advance(0.001)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    assert len(gateway.reads) == 2
    # An expiry is a miss, NOT an eviction: it aged out having done its job, whereas an eviction
    # means the working set does not fit the cap.
    assert resolver.misses == 2
    assert resolver.evictions == 0


async def test_revocation_takes_effect_within_the_ttl(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings, clock: FakeClock
):
    """The documented trade, asserted: a revoked key works for at most TTL seconds longer."""
    digest = store_api_key(gateway, settings)
    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is not None

    gateway.store[apikey_key(digest)][b"status"] = b"revoked"

    # Still cached, so still authenticating — this is the exposure window, and it is bounded.
    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is not None

    clock.advance(IDENTITY_CACHE_TTL_SEC)
    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is None


async def test_invalidate_makes_a_revocation_immediate_on_this_replica(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """C10 calls this on the replica that served the revocation, so that replica is exact."""
    digest = store_api_key(gateway, settings)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))
    gateway.store[apikey_key(digest)][b"status"] = b"revoked"

    assert resolver.invalidate(digest) is True

    assert await resolver.resolve(headers(("x-api-key", RAW_KEY))) is None
    # Invalidating something that was never cached is a no-op that says so. (Note the resolve
    # above re-cached the digest — as a NEGATIVE now — so a digest nobody has presented is what
    # actually exercises the "nothing to drop" path.)
    assert resolver.invalidate(resolver.digest("a-key-nobody-has-presented")) is False


def test_invalidate_refuses_a_raw_key(resolver: IdentityResolver, settings: Settings):
    """A revocation that silently does nothing is the worst possible outcome for this method.

    Passing the raw key where the digest belongs would `pop` a key that is not in the cache and
    return False — indistinguishable, to a caller that ignores the return value, from "it was not
    cached anyway". `apikey_key`'s 64-lower-case-hex validation turns that into a loud refusal.
    """
    with pytest.raises(ValueError, match="64 lower-case hex"):
        resolver.invalidate(RAW_KEY)


async def test_clear_empties_the_cache_but_keeps_the_counters(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """Lifetime totals, not since-the-last-rotation totals — a metric that resets always looks fine."""
    store_api_key(gateway, settings)
    await resolver.resolve(headers(("x-api-key", RAW_KEY)))

    resolver.clear()

    stats = resolver.cache_stats()
    assert stats["size"] == 0
    assert stats["misses"] == 1


async def test_the_cache_evicts_the_least_recently_used_entry_at_the_cap(
    settings: Settings, gateway: FakeGateway
):
    """A bounded cache is what keeps the NEGATIVE cache from being an unbounded-heap attack.

    Caching every failed guess is the right answer for Redis and the wrong one for the pod unless
    there is a cap: without it, a key-guessing flood simply moves from the store into this process.
    """
    resolver = IdentityResolver(gateway, settings, max_entries=2)

    for key in ("k-a", "k-b", "k-c"):
        await resolver.resolve(headers(("x-api-key", key)))

    assert resolver.evictions == 1
    assert resolver.cache_stats()["size"] == 2

    # `k-a` was the oldest, so it is the one that went.
    await resolver.resolve(headers(("x-api-key", "k-a")))
    assert len(gateway.reads) == 4


async def test_a_repeatedly_used_key_survives_eviction(settings: Settings, gateway: FakeGateway):
    """**LRU, not FIFO — and the difference is which customer pays.**

    Without `move_to_end` on a hit, an entry is evicted purely by insertion age, so the key used on
    every single request is evicted as soon as `max_entries` other digests have been seen after it.
    The busiest caller in the system would be the one paying for a Redis round trip, which is
    exactly backwards.
    """
    resolver = IdentityResolver(gateway, settings, max_entries=2)

    await resolver.resolve(headers(("x-api-key", "hot")))
    await resolver.resolve(headers(("x-api-key", "cold")))
    # Touching `hot` makes it the most-recently-used, so `cold` becomes the eviction candidate.
    await resolver.resolve(headers(("x-api-key", "hot")))
    await resolver.resolve(headers(("x-api-key", "new")))

    reads_before = len(gateway.reads)
    assert await resolver.resolve(headers(("x-api-key", "hot"))) is None
    assert len(gateway.reads) == reads_before, "hot survived: served from cache"

    await resolver.resolve(headers(("x-api-key", "cold")))
    assert len(gateway.reads) == reads_before + 1, "cold was evicted: refetched"


async def test_re_caching_a_key_refreshes_its_recency(settings: Settings, gateway: FakeGateway):
    """Assignment to an existing OrderedDict key keeps its position; the explicit move fixes that.

    Without it a refreshed entry inherits its predecessor's recency and is evicted early — the
    same "busiest caller pays" bug as above, arriving through the expiry path instead.
    """
    clock = FakeClock()
    resolver = IdentityResolver(gateway, settings, max_entries=2, clock=clock)

    await resolver.resolve(headers(("x-api-key", "one")))
    await resolver.resolve(headers(("x-api-key", "two")))
    clock.advance(IDENTITY_CACHE_TTL_SEC)
    # Re-resolving `one` expires and re-inserts it, which must put it at the FRESH end.
    await resolver.resolve(headers(("x-api-key", "one")))
    await resolver.resolve(headers(("x-api-key", "three")))

    reads_before = len(gateway.reads)
    await resolver.resolve(headers(("x-api-key", "one")))
    assert len(gateway.reads) == reads_before, "one was re-inserted at the fresh end"


def test_the_cap_is_floored_at_one(settings: Settings, gateway: FakeGateway):
    """A cap of zero would evict the entry it just inserted, every time: overhead with no cache."""
    assert IdentityResolver(gateway, settings, max_entries=0).cache_stats()["max_entries"] == 1


def test_cache_stats_on_a_process_that_has_authenticated_nobody(
    resolver: IdentityResolver, settings: Settings
):
    """No lookups means no hit rate. 0.0, not a ZeroDivisionError on the stats endpoint.

    The three concurrency counters joined this payload at C8, when the pre-auth path acquired a
    bound; they are asserted here rather than only in ``tests/unit/test_overload.py`` because this
    is the test that pins the payload's exact shape, and a counter nobody publishes is a counter
    nobody reads.
    """
    assert resolver.cache_stats() == {
        "size": 0,
        "max_entries": IDENTITY_CACHE_MAX_ENTRIES,
        "ttl_sec": IDENTITY_CACHE_TTL_SEC,
        "hits": 0,
        "misses": 0,
        "negative_hits": 0,
        "evictions": 0,
        "hit_rate": 0.0,
        "max_concurrency": identity_concurrency(settings),
        "gate_waits": 0,
        "peak_in_flight": 0,
    }


def test_a_negative_ttl_is_floored_at_zero(settings: Settings, gateway: FakeGateway):
    assert IdentityResolver(gateway, settings, ttl_sec=-5).cache_stats()["ttl_sec"] == 0.0


# =============================================================================================
# JWT
# =============================================================================================


async def test_a_valid_token_resolves(resolver: IdentityResolver, settings: Settings):
    token = issue_token("alice", settings=settings)

    principal = await resolver.resolve(headers(("authorization", f"Bearer {token}")))

    assert principal == Principal(user_id="alice", credential=CredentialKind.JWT, key_id=None)


async def test_the_jwt_path_never_touches_redis(
    resolver: IdentityResolver, gateway: FakeGateway, settings: Settings
):
    """Which is also why it is not cached: there is no round trip to save."""
    await resolver.resolve(
        headers(("authorization", f"Bearer {issue_token('alice', settings=settings)}"))
    )

    assert gateway.reads == []
    assert resolver.cache_stats()["size"] == 0


async def test_issue_token_round_trips_through_resolve(
    resolver: IdentityResolver, settings: Settings
):
    """C13 mints a fresh uuid4 principal per double-spend run; this is that path, in miniature."""
    for subject in ("e2e-9f2c4b8e", "load-harness-1", "demo-free"):
        token = issue_token(subject, settings=settings)
        principal = await resolver.resolve(headers(("authorization", f"Bearer {token}")))
        assert principal is not None
        assert principal.user_id == subject
        assert principal.credential is CredentialKind.JWT


async def test_an_expired_token_resolves_to_none(resolver: IdentityResolver, settings: Settings):
    """A non-positive ttl mints an already-expired token, so expiry is asserted without sleeping."""
    token = issue_token("alice", settings=settings, ttl_min=-5)

    assert await resolver.resolve(headers(("authorization", f"Bearer {token}"))) is None


async def test_a_token_signed_with_another_key_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    forged = jwt.encode(live_claims(), "a-completely-different-signing-key", algorithm="HS256")

    assert await resolver.resolve(headers(("authorization", f"Bearer {forged}"))) is None


async def test_an_alg_none_token_resolves_to_none(resolver: IdentityResolver):
    """**The classic forgery: no signature at all, and a `sub` of the attacker's choosing.**

    A decoder that honours the token's own `alg` header accepts this. `algorithms=[...]` is an
    allowlist pinned to the ONE algorithm this service signs with, so the header is checked against
    our choice and the token never gets to say how it should be verified.
    """
    forged = forge_alg_none(live_claims(subject="admin"))

    assert await resolver.resolve(headers(("authorization", f"Bearer {forged}"))) is None


async def test_a_token_signed_with_a_different_algorithm_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    """HS512 is a perfectly good algorithm and it is not the configured one, so it is refused.

    Same allowlist, different attack: this is the shape of HS/RS confusion, where a token claiming
    one algorithm is verified against a key intended for another.
    """
    assert settings.jwt_algorithm == "HS256"
    other = jwt.encode(live_claims(), settings.jwt_secret, algorithm="HS512")

    assert await resolver.resolve(headers(("authorization", f"Bearer {other}"))) is None


@pytest.mark.parametrize(
    "token",
    ["", "not-a-jwt", "a.b.c", "eyJhbGciOiJIUzI1NiJ9"],
    ids=["empty", "garbage", "three-garbage-segments", "one-segment"],
)
async def test_a_malformed_token_resolves_to_none_rather_than_raising(
    resolver: IdentityResolver, token
):
    """No PyJWT exception may escape: it would be a 500 on input anyone can send unauthenticated."""
    assert await resolver.resolve(headers(("authorization", f"Bearer {token}"))) is None


async def test_a_token_with_no_expiry_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    """An `exp`-less token is a permanent bearer credential nothing can retire.

    PyJWT accepts one by default — there is no claim, so there is nothing to check. Requiring it
    turns "valid forever" from a silent property into a rejection.
    """
    forever = jwt.encode({"sub": "alice"}, settings.jwt_secret, algorithm=settings.jwt_algorithm)

    assert await resolver.resolve(headers(("authorization", f"Bearer {forever}"))) is None


async def test_a_token_with_no_subject_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    """An absent `sub` is not "anonymous" — there is nobody to meter."""
    anonymous = jwt.encode(
        {"exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp())},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )

    assert await resolver.resolve(headers(("authorization", f"Bearer {anonymous}"))) is None


async def test_a_non_string_subject_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    """`require: sub` proves presence, not type.

    A numeric `sub` would flow on as an int and become a Redis key component via `str()`, so the
    callers `7` and `"7"` would silently share one bucket and one quota.
    """
    numeric = jwt.encode(
        live_claims() | {"sub": 7}, settings.jwt_secret, algorithm=settings.jwt_algorithm
    )

    assert await resolver.resolve(headers(("authorization", f"Bearer {numeric}"))) is None


async def test_a_subject_that_cannot_be_hash_tagged_resolves_to_none(
    resolver: IdentityResolver, settings: Settings
):
    """A braced `sub` can forge or collide with another principal's Redis Cluster slot."""
    braced = jwt.encode(
        live_claims(subject="alice}x{bob"),
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )

    assert await resolver.resolve(headers(("authorization", f"Bearer {braced}"))) is None


async def test_a_tier_claim_in_a_token_is_ignored(
    resolver: IdentityResolver, settings: Settings
):
    """**Identity from the token, authority from the store.**

    A signed `tier` claim would be either stale (a downgrade does not take effect until the token
    expires) or attacker-chosen (the moment anything other than this service issues tokens, a
    self-selected tier is privilege escalation). So the resolved Principal has nowhere to *put* a
    tier, and the decision script reads `user:{uid}` on every request instead.
    """
    escalated = jwt.encode(
        live_claims(subject="alice", tier="enterprise", role="admin", rate_limit_per_min=999_999),
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )

    principal = await resolver.resolve(headers(("authorization", f"Bearer {escalated}")))

    assert principal == Principal(user_id="alice", credential=CredentialKind.JWT, key_id=None)
    # Nothing reads a tier off a principal because there is no field to read: the structure itself
    # is what makes the claim inert, rather than a rule someone has to remember not to break.
    assert {f.name for f in dataclasses.fields(Principal)} == {
        "user_id",
        "credential",
        "key_id",
    }
    assert not hasattr(principal, "tier")


def test_issue_token_emits_no_tier_claim(settings: Settings):
    """The producer side of the same rule: there is no tier in a token this service signs."""
    claims = jwt.decode(
        issue_token("alice", settings=settings),
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )

    assert set(claims) == {"sub", "iat", "exp"}


def test_issue_token_honours_an_explicit_ttl(settings: Settings):
    claims = jwt.decode(
        issue_token("alice", settings=settings, ttl_min=1),
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )

    assert claims["exp"] - claims["iat"] == 60


def test_issue_token_defaults_to_the_configured_ttl(settings: Settings):
    claims = jwt.decode(
        issue_token("alice", settings=settings),
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )

    assert claims["exp"] - claims["iat"] == settings.access_token_ttl_min * 60


def test_issue_token_accepts_an_injected_issue_instant(settings: Settings):
    minted_at = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)

    claims = jwt.decode(
        issue_token("alice", settings=settings, now=minted_at, ttl_min=10),
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
        options={"verify_exp": False},
    )

    assert claims["iat"] == int(minted_at.timestamp())


def test_issue_token_refuses_a_subject_that_cannot_be_metered(settings: Settings):
    """Refused where the mistake is, not as a ValueError from inside the limiter's key builder."""
    with pytest.raises(ValueError, match="brace"):
        issue_token("alice}x{bob", settings=settings)


def test_issue_token_refuses_a_naive_datetime(settings: Settings):
    """**A naive ``now`` would silently mean the process's local timezone.**

    ``datetime.astimezone()`` reads a naive value as local time, so a harness on a UTC+2 laptop
    calling ``issue_token(now=datetime.now())`` mints a token whose ``iat`` is two hours ahead.
    PyJWT then rejects it with "The token is not yet valid (iat)" — a message naming neither the
    timezone nor the caller, about a token that looks perfectly well-formed. It fails safe and it
    fails baffling, and the second half is what this branch removes.

    Refusing rather than assuming UTC is the deliberate opposite of `src.keys._as_utc_datetime`,
    which may assume because every producer feeding it is provably UTC. Here the caller is a
    harness author on an arbitrary machine.
    """
    with pytest.raises(ValueError, match="timezone-aware"):
        issue_token("alice", settings=settings, now=datetime(2026, 8, 10, 12, 0, 0))


def test_issue_token_normalises_a_non_utc_aware_datetime(settings: Settings):
    """An aware value in any zone is fine — it names a real instant, so it is simply converted."""
    tokyo = timezone(timedelta(hours=9))
    minted_at = datetime(2026, 8, 10, 21, 0, 0, tzinfo=tokyo)

    claims = jwt.decode(
        issue_token("alice", settings=settings, now=minted_at, ttl_min=10),
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
        options={"verify_exp": False},
    )

    assert claims["iat"] == int(datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp())


# =============================================================================================
# Demo credentials
# =============================================================================================


def test_the_demo_declaration_is_the_spec_names_and_the_three_tiers():
    """The spec names the first two literally; the harnesses import this rather than restating it."""
    assert [credential.raw_key for credential in DEMO_CREDENTIALS] == [
        "demo-free-key",
        "demo-premium-key",
        "demo-enterprise-key",
    ]
    assert DEMO_KEY_BY_TIER[Tier.FREE] == "demo-free-key"
    assert DEMO_KEY_BY_TIER[Tier.PREMIUM] == "demo-premium-key"
    assert DEMO_KEY_BY_TIER[Tier.ENTERPRISE] == "demo-enterprise-key"
    assert DEMO_KEY_BY_USER["demo-premium"] == "demo-premium-key"
    # One credential per tier, so a harness asking for "a premium caller" gets an unambiguous answer.
    assert len(DEMO_KEY_BY_TIER) == len(DEMO_CREDENTIALS)


async def test_seeding_writes_key_and_user_records_and_never_the_raw_key(
    gateway: FakeGateway, settings: Settings
):
    written = await seed_demo_credentials(gateway, settings)

    # 4 apikey fields + 3 user fields, per credential.
    assert written == len(DEMO_CREDENTIALS) * 7

    for credential in DEMO_CREDENTIALS:
        digest = apikey_digest(credential.raw_key, pepper=settings.api_key_pepper)
        assert gateway.store[apikey_key(digest)][b"user_id"] == credential.user_id.encode()
        assert gateway.store[apikey_key(digest)][b"status"] == STATUS_ACTIVE.encode()
        assert gateway.store[user_key(credential.user_id)][b"tier"] == credential.tier.encode()

    # The plaintext key appears in NO key name and in NO stored value. A Redis key name lands in
    # MONITOR output, slowlog entries, the AOF file and every backup of it.
    stored = "".join(gateway.store) + "".join(
        value.decode() for record in gateway.store.values() for value in record.values()
    )
    assert all(credential.raw_key not in stored for credential in DEMO_CREDENTIALS)


async def test_seeding_is_twenty_one_commands_in_one_round_trip(
    gateway: FakeGateway, settings: Settings
):
    """**Per-field ``HSETNX`` semantics, pipelined into a single round trip.**

    The granularity has to stay per field — that is what makes an operator's edit to one field
    survive a restart — but 21 sequential round trips per replica boot is 21 x RTT of dead time
    before the process serves, multiplied by the replica count, for a write that is a no-op after
    the first boot in the store's life.

    Asserted on both axes so neither can regress silently: the command count proves the semantics
    were not coarsened into 6 whole-hash writes, and the round-trip count proves they were actually
    batched. One ``run()`` as well, so the whole seed succeeds or fails as one unit to the breaker.
    """
    await seed_demo_credentials(gateway, settings)

    assert len(gateway.writes) == len(DEMO_CREDENTIALS) * 7 == 21
    assert gateway.round_trips == 1
    assert gateway.ops == ["identity:seed"]


async def test_seeding_uses_hsetnx_and_twice_is_a_no_op(gateway: FakeGateway, settings: Settings):
    """**The HSETNX contract**, the same one C3 established for the tier table.

    With `HSET`, an operator who revokes a demo key gets it silently restored the next time any
    replica restarts — not immediately, and with no error and no log line, which is the most
    expensive shape a bug can have.
    """
    await seed_demo_credentials(gateway, settings)

    assert {write[0] for write in gateway.writes} == {"hsetnx"}
    assert await seed_demo_credentials(gateway, settings) == 0


async def test_seeding_is_per_field_so_one_operator_edit_survives(
    gateway: FakeGateway, settings: Settings
):
    """Per-field HSETNX: a revoked status stays revoked while a genuinely missing field is created."""
    await seed_demo_credentials(gateway, settings)
    digest = apikey_digest("demo-free-key", pepper=settings.api_key_pepper)
    gateway.store[apikey_key(digest)][b"status"] = b"revoked"
    del gateway.store[apikey_key(digest)][b"label"]

    written = await seed_demo_credentials(gateway, settings)

    assert written == 1, "only the deleted label was recreated"
    assert gateway.store[apikey_key(digest)][b"status"] == b"revoked"


async def test_reseed_overwrites(gateway: FakeGateway, settings: Settings):
    """The escape hatch: no production caller, so nobody reaches it by accident."""
    await seed_demo_credentials(gateway, settings)
    digest = apikey_digest("demo-free-key", pepper=settings.api_key_pepper)
    gateway.store[apikey_key(digest)][b"status"] = b"revoked"

    await seed_demo_credentials(gateway, settings, reseed=True)

    assert gateway.store[apikey_key(digest)][b"status"] == STATUS_ACTIVE.encode()
    assert {write[0] for write in gateway.writes} == {"hsetnx", "hset"}


async def test_a_seeded_demo_key_resolves(gateway: FakeGateway, settings: Settings):
    """End to end within this module: the thing seeded is the thing authenticated."""
    resolver = IdentityResolver(gateway, settings)
    await seed_demo_credentials(gateway, settings)

    principal = await resolver.resolve(headers(("x-api-key", "demo-premium-key")))

    assert principal is not None
    assert principal.user_id == "demo-premium"
    assert principal.key_id == "demo-premium"
    assert principal.credential is CredentialKind.API_KEY


async def test_start_seeds_and_clears_the_cache(gateway: FakeGateway, settings: Settings):
    resolver = IdentityResolver(gateway, settings)
    await resolver.resolve(headers(("x-api-key", "unknown")))
    assert resolver.cache_stats()["size"] == 1

    await resolver.start()

    assert resolver.cache_stats()["size"] == 0
    assert await resolver.resolve(headers(("x-api-key", "demo-free-key"))) is not None


async def test_start_never_raises_when_redis_is_down(settings: Settings, caplog):
    """**Startup must not crash-loop.**

    A replica that refuses to boot because it could not seed enforces nothing at all, on every
    request, for as long as the loop lasts. The service authenticates perfectly well against
    credentials another replica (or an operator) already wrote, so there is no failure here worth
    trading a serving process for — it is logged at ERROR and the demo keys 401 until a seed lands.
    """
    broken = FakeGateway(error=BackingStoreUnavailable("down", op="identity:seed-key"))
    resolver = IdentityResolver(broken, settings)

    with caplog.at_level(logging.ERROR):
        await resolver.start()

    assert "demo credential seeding failed" in caplog.text


async def test_start_can_reseed(gateway: FakeGateway, settings: Settings):
    resolver = IdentityResolver(gateway, settings)
    await resolver.start()
    digest = apikey_digest("demo-free-key", pepper=settings.api_key_pepper)
    gateway.store[apikey_key(digest)][b"status"] = b"revoked"

    await resolver.start(reseed=True)

    assert gateway.store[apikey_key(digest)][b"status"] == STATUS_ACTIVE.encode()

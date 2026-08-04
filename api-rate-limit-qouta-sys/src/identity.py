"""Who is calling: an API key or a Bearer token in, a :class:`~src.models.Principal` out.

This is the second thing the middleware does on every metered request (after classifying the
endpoint) and the first thing that can refuse one. It runs *before* the limiter, on the same 5 ms
budget, so everything here is either arithmetic on bytes already in memory or a single ``HGETALL``
that is normally served from an in-process cache.

.. rubric:: Resolution order — first match wins, documented because it is observable

1. ``Authorization: Bearer <jwt>``   -> the JWT path
2. ``Authorization: ApiKey <key>``   -> the API-key path
3. ``X-API-Key: <key>``              -> the API-key path
4. nothing, or nothing parseable    -> ``None``

``None`` means "no principal", and C6 turns that into a 401 with ``WWW-Authenticate`` and
**without** any ``X-RateLimit-*`` header: with no principal there is no bucket, and every number
those headers could carry would be fiction.

An ``Authorization`` header this module cannot read (``Basic ...``, a bare ``Bearer`` with no
token) does not abort the search — it falls through to ``X-API-Key``. A header we do not
understand is not a credential, and refusing a perfectly good API key because some proxy also
attached a ``Basic`` header would be a denial of service with a security-sounding justification.
Nothing is granted by the fall-through: an unusable header still authenticates nobody.

.. rubric:: API keys are stored as HMAC-SHA256(pepper, key). This is a deliberate deviation

The scaffold README says bcrypt. It is wrong here, and not marginally — bcrypt would be both
slower than the entire latency budget and *structurally unable* to answer the question this module
asks. Four reasons, in the order they bite:

* **bcrypt exists to defend low-entropy, human-chosen secrets.** Its whole value is making an
  offline dictionary attack expensive. An API key here is 256 bits of CSPRNG output: there is no
  dictionary to try, no reuse across sites, no human pattern to exploit, and no offline search
  that terminates before the heat death of anything. A password KDF applied to a random 256-bit
  string buys nothing measurable.
* **bcrypt at its default cost is ~250 ms — 50x this project's entire 5 ms budget** — on a path
  that runs on *every* metered request, in a process whose target is 1000 rps.
* **Worse: bcrypt salts every hash individually, so you cannot look a key up by its hash.** The
  digest of a presented key does not equal the stored digest, by design. Answering "who is this?"
  would mean bcrypt-comparing the presented key against *every* stored key until one matched:
  250 ms x N per request, growing with the customer count. There is no index that fixes this,
  because a per-hash random salt is precisely the absence of an index.
* **HMAC-SHA256 is ~1 microsecond, is a deterministic index (one ``HGETALL``), and keeps the one
  property bcrypt was reached for:** the pepper lives in the process environment
  (``API_KEY_PEPPER``), never in Redis. A stolen RDB/AOF dump therefore yields digests that cannot
  be inverted or replayed without also stealing the application's environment, and rotating the
  pepper invalidates every stored key by construction.

``bcrypt`` is consequently absent from ``requirements.txt``. It must not come back.

The ``v1:`` in ``apikey:v1:<digest>`` is a **scheme version**, not decoration: it is what lets the
pepper or the digest algorithm be rotated by writing ``apikey:v2:`` records *alongside* the old
ones rather than by a migration that has to be atomic with a deploy.

.. rubric:: Identity from the token, authority from the store

The JWT path reads exactly one claim: ``sub``. **Any ``tier`` claim in the token is ignored**, and
:func:`issue_token` does not emit one. The tier is read from ``user:{uid}`` *inside* the decision
script, on every request.

That is not fastidiousness, it is the difference between a limit and a suggestion:

* A signed tier claim freezes the caller's plan for the lifetime of the token. Downgrade someone
  at 14:00 and they keep enterprise limits until their token expires — and "we cannot lower your
  limits for another 30 minutes" is not a rate limiter, it is a delay.
* The moment tokens are issued by anything other than this service — an SSO provider, a partner
  IdP, a sibling service sharing the secret — a self-selected ``tier`` claim is straightforward
  privilege escalation, signed and therefore trusted.

So the token says *who*, and Redis says *what they are allowed*. The cost is one ``HGET`` inside a
script that is already touching that exact key slot; the benefit is that
``PUT /admin/users/alice/tier`` takes effect on the very next request, on every replica.

.. rubric:: The cache, and precisely what the negative half does and does not buy

:class:`IdentityResolver` keeps an ``OrderedDict`` of ``digest -> Principal | None``, capped at
:data:`IDENTITY_CACHE_MAX_ENTRIES` and expiring after :data:`IDENTITY_CACHE_TTL_SEC`. It caches
**negatives as well as positives**.

What that is worth, stated honestly because the obvious claim is wrong: **caching negatives absorbs
*repeats* of one wrong credential, not credential enumeration.** The realistic and very common case
is a client that is misconfigured or retrying — a stale key in a CI job, a copy-paste error in a
deploy, a retry loop hammering the same rejected string — which without this would be one Redis
round trip per attempt, forever, from a caller that will never succeed. That collapses to a dict
lookup here.

Enumeration is a different shape and this does not stop it. Distinct guesses have distinct digests,
so each one is still a full round trip: measured, 20 000 distinct guesses cost 20 001 round trips.
Worse, they also cost ~10 000 **evictions of legitimate entries**, because the flood pushes real
principals out of a bounded LRU — so genuine traffic pays extra Redis reads for as long as it runs.
That is bounded rather than fixed (entries live at most :data:`IDENTITY_CACHE_TTL_SEC` anyway, so
the steady-state penalty is small) and it is not the layer that should be solving it; see the
pre-auth exhaustion rubric below, which is the same attack described at the resource it actually
threatens.

The cache is keyed on the **digest**, never on the raw key, for the same reason the Redis key name
is: a process dump, a debugger, or a stray ``repr`` of the resolver must not contain replayable
credentials.

**Revocation latency is therefore up to :data:`IDENTITY_CACHE_TTL_SEC` (5 s).** That is the
trade, stated rather than discovered: a revoked key can be used for at most five more seconds by a
caller who was *already* using it, and C10's key-management calls :meth:`IdentityResolver.invalidate`
on the replica that served the revocation so that replica is exact. Five seconds of exposure on a
credential the holder already had, against a Redis round trip on every request forever, is not a
close call. If a deployment ever needs zero, the knob is ``ttl_sec=0`` and the cost is visible.

.. rubric:: A Redis failure PROPAGATES out of :meth:`IdentityResolver.resolve`

:class:`~src.redis_client.BackingStoreUnavailable` is deliberately **not** caught here, and C8's
author must handle it explicitly rather than inherit a default from this module.

"We could not check your limits" and "we could not establish who you are" are different questions
with different safe answers. Failing open on the first serves an unmetered request to a known
customer — the documented, bounded degradation this project ships. Failing open on the *second*
would serve an unauthenticated request to anyone holding any string, for as long as Redis is down:
the fail-open path would become an authentication bypass. Failing *closed* on identity while the
limiter fails open is a perfectly coherent policy, and it is the one this module's shape nudges
toward — but it is C8's call, made once, in the module that owns ``FAIL_MODE``, and made visibly.

.. rubric:: READ THIS BEFORE WRITING C8: identity resolution is a PRE-AUTH, UNMETERED path

This is the most consequential property of this module and it is not visible from inside it.

Identity resolution runs **before** the limiter — it has to, because the limiter needs a principal
to meter — so a request that never authenticates is **never rate limited**. A cold resolve (any
digest not currently in this process's cache) issues a Redis command through the *shared*
:class:`~src.redis_client.RedisGateway`: the same bounded connection pool
(``REDIS_MAX_CONNECTIONS``, 32) and the same :class:`~src.redis_client.CircuitBreaker` that the
decision script depends on. Both are therefore reachable by an attacker holding **no credential at
all**, simply by sending random ``X-API-Key`` values.

Measured against the shipped pool of 32:

* 33 concurrent cold resolves -> 1 ``BackingStoreUnavailable``.
* 200 distinct unknown keys -> 168 errors and the shared circuit breaker **OPEN**.

So roughly 33 concurrent connections and a random-string generator are enough to degrade
enforcement for every *legitimate* caller, without ever presenting a credential. The negative cache
does not help here, because enumeration never repeats a digest (see the cache rubric above).

**What this means for C8, concretely.** ``FAIL_MODE`` must not be applied as one undifferentiated
rule to every :class:`~src.redis_client.BackingStoreUnavailable` in the request path. The two
failures are not the same event:

* *Limits could not be checked* — the principal is known, and serving them through the bounded
  local fallback bucket with ``X-RateLimit-Degraded: 1`` is the documented graceful degradation.
* *Identity could not be resolved* — nothing is known about the caller. Failing open here is **an
  authentication bypass, not a degradation**, and it is one that an unauthenticated attacker can
  *cause on demand* using the vector above: break the store, then walk in. That is a failure mode
  the attacker controls the timing of.

Whatever C8 decides, these must be two decisions and not one, and the identity one must be
justified in writing. Related and separate: C8's own plan note already requires distinguishing
"the store is unreachable" from "this process ran out of connections to reach it" — the vector
above is precisely how a caller manufactures the second condition, so the pool-exhaustion case and
the identity case are the same incident seen from two modules.

.. rubric:: WHAT C8 DECIDED — the answers to the rubric above

**A failure to resolve identity is a 503.** :class:`~src.redis_client.BackingStoreUnavailable`
still propagates out of :meth:`IdentityResolver.resolve` exactly as described above;
:class:`~src.middleware.RateLimitMiddleware` catches it in a branch of its own and refuses the
request. Never a principal, never a pass-through, and ``FAIL_MODE`` deliberately gets no vote:
that setting configures what happens when *limits* cannot be checked, and there is no deployment
for which "we could not establish who you are, so come in" is the right answer.

**The JWT path keeps working while the API-key path is refused.** This falls out of the shape this
module already had — :meth:`_resolve_jwt` verifies one HMAC over bytes in memory and touches no
Redis at all — and it is strictly better than 503-ing everyone, so it is now a property with a test
on it rather than an accident. A Bearer caller authenticates normally through a total Redis outage
and is then metered by the limiter's local fallback bucket; only ``X-API-Key`` callers get the 503.

**The pre-auth pool exhaustion measured above is bounded here**, by
:data:`IDENTITY_POOL_SHARE`: the API-key lookup runs under a semaphore admitting at most
``REDIS_MAX_CONNECTIONS // 4`` concurrent lookups, so unauthenticated traffic can occupy a quarter
of the pool and no more — the limiter always has connections left for the callers that *are*
authenticated. The bound lives here, wrapped around the lookup itself, rather than in the
middleware around ``resolve``: at that level it would also throttle the JWT branch, making the
credential form that needs no Redis queue behind the one that does, which would undo the carve-out
in the paragraph above. Derived from ``REDIS_MAX_CONNECTIONS`` rather than configured separately,
so the two cannot be tuned into disagreement.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Any, Final

import jwt

from src.config import Settings
from src.keys import apikey_key, sanitise_user_id, user_key
from src.models import CredentialKind, Principal, Tier
from src.redis_client import RedisGateway

logger = logging.getLogger(__name__)

__all__ = [
    "ACCEPTED_SCHEMES",
    "API_KEY_HEADER",
    "APIKEY_SCHEME",
    "AUTHORIZATION_HEADER",
    "AUTH_REALM",
    "BEARER_SCHEME",
    "DEMO_CREDENTIALS",
    "DEMO_KEY_BY_TIER",
    "DEMO_KEY_BY_USER",
    "DemoCredential",
    "HTTP_OWS",
    "IDENTITY_CACHE_MAX_ENTRIES",
    "IDENTITY_CACHE_TTL_SEC",
    "IDENTITY_POOL_SHARE",
    "IdentityResolver",
    "identity_concurrency",
    "SCHEME_KINDS",
    "STATUS_ACTIVE",
    "WWW_AUTHENTICATE",
    "apikey_digest",
    "header_value",
    "issue_token",
    "parse_credential",
    "seed_demo_credentials",
    "verify_api_key",
]


# ---------------------------------------------------------------------------------------------
# Header names and credential schemes
#
# ASGI lower-cases header names before they reach an application, and every server in the stack
# (uvicorn, hypercorn, the test transport) does. These constants are lower-case bytes to match —
# but `header_value` still folds case itself rather than trusting that, because "the transport
# guarantees it" is exactly the kind of assumption that holds until someone drives this function
# from a test, a CLI, or a server that does not.
# ---------------------------------------------------------------------------------------------

AUTHORIZATION_HEADER: Final = b"authorization"
API_KEY_HEADER: Final = b"x-api-key"

#: **HTTP optional whitespace is SP and HTAB, and nothing else** (RFC 9110 §5.6.3). This constant is
#: what every trim in this module uses, and using it rather than :meth:`str.strip` is a security
#: control rather than pedantry.
#:
#: ``str.strip()`` with no argument removes everything *Python* calls whitespace, which includes
#: U+00A0 (NBSP), U+0085 (NEL), ``\x0b``, and ``\x1c``-``\x1f``. Measured against the bare form:
#: ``demo-free-key\xa0``, ``\x1cdemo-free-key``, ``demo-free-key\x85`` and ``demo-free-key\x0b``
#: **all authenticated as the same principal**.
#:
#: That is not an escalation — you still need the real key — but it silently gives one credential a
#: family of accepted spellings, and everything downstream that treats the presented credential as
#: an identifier then has a hole in it: exact-match audit logging records a string that is not the
#: one that was accepted, an upstream blocklist keyed on the credential is bypassed by appending one
#: byte, and any future per-credential accounting counts one caller as several. A credential should
#: have exactly one accepted spelling.
HTTP_OWS: Final = " \t"

#: Splits the auth-scheme from its token on a run of OWS. RFC 9110 grammar is ``auth-scheme 1*SP
#: token68``; HTAB is additionally tolerated because real clients emit it and it is unambiguous.
#: Nothing else separates them — in particular ``Bearer\xa0<token>`` is NOT a credential, it is one
#: unrecognised scheme-shaped string, which is exactly the point of the constant above.
_SCHEME_SPLIT_RE: Final = re.compile(f"[{HTTP_OWS}]+")

#: Scheme tokens, compared **lower-cased**. RFC 9110 says the auth-scheme is case-insensitive, so
#: ``bearer``, ``Bearer`` and ``BEARER`` are the same scheme and a client that spells it unusually
#: gets authenticated rather than a confusing 401.
BEARER_SCHEME: Final = "bearer"
APIKEY_SCHEME: Final = "apikey"

#: Realm named in every challenge. One value, because there is one protection space here.
AUTH_REALM: Final = "api-rate-limiter"


@dataclass(frozen=True, slots=True)
class _AuthScheme:
    """One accepted ``Authorization`` scheme: how it is matched, how it is advertised, what it is."""

    #: Lower-case token compared against the presented scheme.
    token: str
    #: Canonical spelling used in the ``WWW-Authenticate`` challenge.
    display: str
    #: Which credential path it selects.
    kind: CredentialKind


#: THE list of accepted schemes. :func:`parse_credential` dispatches off it and
#: :data:`WWW_AUTHENTICATE` is generated from it, so "what this service accepts" and "what this
#: service *says* it accepts" are the same object rather than two lists maintained in parallel.
#:
#: That linkage is the fix for a real drift: the challenge previously advertised ``Bearer`` only,
#: while the service also accepted ``ApiKey`` — so C6's 401 would never have told a client that the
#: scheme it needed existed. Adding a scheme here now updates the challenge by construction, and
#: ``tests/unit/test_identity.py`` asserts both directions of the correspondence.
ACCEPTED_SCHEMES: Final[tuple[_AuthScheme, ...]] = (
    _AuthScheme(token=BEARER_SCHEME, display="Bearer", kind=CredentialKind.JWT),
    _AuthScheme(token=APIKEY_SCHEME, display="ApiKey", kind=CredentialKind.API_KEY),
)

#: ``lower-case scheme -> credential kind``, the dispatch table :func:`parse_credential` reads.
SCHEME_KINDS: Mapping[str, CredentialKind] = MappingProxyType(
    {scheme.token: scheme.kind for scheme in ACCEPTED_SCHEMES}
)

#: The challenge C6 emits alongside its 401, generated from :data:`ACCEPTED_SCHEMES`.
#:
#: RFC 9110 §11.6.1 allows a comma-separated **list** of challenges, and a 401 that names only one
#: of two accepted schemes is actively misleading: it tells a client holding an API key that its
#: credential is not supported here. ``X-API-Key`` has no standard challenge form of its own (it is
#: not an ``Authorization`` scheme at all), so the ``ApiKey`` challenge is what advertises that
#: credential type; the two spellings are interchangeable on input.
#:
#: No ``charset`` parameter: it is defined for ``Basic`` (RFC 7617) and has no meaning for either
#: scheme here, and omitting it keeps the challenge list unambiguous to parse.
WWW_AUTHENTICATE: Final = ", ".join(
    f'{scheme.display} realm="{AUTH_REALM}"' for scheme in ACCEPTED_SCHEMES
)

# ---------------------------------------------------------------------------------------------
# Stored record shape
#
# `apikey:v1:<digest>`  HASH: user_id, label, status, created_at
# `user:{uid}`          HASH: tier, status, created_at
#
# Field names are constants because three different commits write them (C5 seeds, C10 manages,
# C13 asserts) and a HASH field typo is a silent read of `None`, not an error.
# ---------------------------------------------------------------------------------------------

FIELD_USER_ID: Final = "user_id"
FIELD_LABEL: Final = "label"
FIELD_STATUS: Final = "status"
FIELD_CREATED_AT: Final = "created_at"
FIELD_TIER: Final = "tier"

#: The **only** status that authenticates. Anything else — ``revoked``, ``suspended``, a typo, an
#: empty string — resolves to no principal. Allowlist and not a denylist: a status nobody
#: anticipated must fail shut, because the alternative is that ``status: revokd`` keeps working.
STATUS_ACTIVE: Final = "active"

# ---------------------------------------------------------------------------------------------
# Cache sizing
# ---------------------------------------------------------------------------------------------

#: Hard cap on cached identities. 10 000 principals is far more than this demo will ever have and
#: is still a bounded amount of process memory (~100 entries per MB).
#:
#: The cap is what keeps the *negative* cache from being its own denial of service: without it,
#: caching every failed guess would turn a flood of distinct keys into unbounded heap growth — the
#: attack relocated from Redis into the pod rather than stopped. What the cap costs in exchange is
#: that such a flood evicts legitimate entries (see the module docstring); that is the right way
#: round, because extra Redis reads for real traffic are recoverable and an OOM kill is not.
IDENTITY_CACHE_MAX_ENTRIES: Final = 10_000

#: Seconds an entry stays usable. Also the worst-case revocation latency on a replica that did not
#: serve the revocation — see the module docstring for why that trade is the right one.
IDENTITY_CACHE_TTL_SEC: Final = 5.0

#: Reciprocal of the share of ``REDIS_MAX_CONNECTIONS`` the **pre-auth** identity lookup may hold
#: at once. ``4`` means one quarter — 8 concurrent lookups against the shipped pool of 32.
#:
#: This number is the fix for the vector measured in the module docstring: identity resolution runs
#: before the limiter, so it is reachable by a caller holding no credential, and without a bound a
#: flood of distinct unknown keys takes the entire pool — leaving the *limiter*, which every
#: authenticated caller needs, with nothing. Capping the unauthenticated path at a quarter means
#: the worst an anonymous flood can do is make itself slow.
#:
#: Derived from ``REDIS_MAX_CONNECTIONS`` rather than declared as its own setting, deliberately.
#: The invariant that matters is *relative* ("well under the pool"), and two independent numbers is
#: how an operator raising the pool ends up with an identity bound that no longer bounds anything —
#: or lowering it ends up with a bound larger than the pool, which is no bound at all.
IDENTITY_POOL_SHARE: Final = 4


def identity_concurrency(settings: Settings) -> int:
    """How many API-key lookups may be in flight at once. See :data:`IDENTITY_POOL_SHARE`.

    Floored at 1, so a tiny or mis-set ``REDIS_MAX_CONNECTIONS`` produces a serialised identity
    path rather than a semaphore of zero permits, which would deadlock every authenticated request
    in the process — a config typo turning into a total outage on the one path that has to work
    before anything else can.
    """
    return max(1, settings.redis_max_connections // IDENTITY_POOL_SHARE)

#: Raw ASGI headers: the ``scope["headers"]`` list of ``(name, value)`` byte pairs.
RawHeaders = Sequence[tuple[bytes, bytes]]


# ---------------------------------------------------------------------------------------------
# Pure helpers — no I/O, no state
# ---------------------------------------------------------------------------------------------


def header_value(headers: RawHeaders, name: bytes) -> str | None:
    """Return the first value of header ``name`` from raw ASGI headers, stripped, or ``None``.

    ``headers`` is ``scope["headers"]`` — a list of ``(bytes, bytes)`` pairs — and the resolver
    takes that shape rather than a Starlette ``Request`` on purpose. Constructing a ``Request``
    and reading its ``.headers`` costs roughly 30 microseconds of object graph for what is, here,
    two dictionary-free scans of a short list; on the hot path of a service targeting 1000 rps
    that is a measurable fraction of the whole rate-limit budget spent to read two strings.

    ``name`` must already be lower-case bytes; the stored header name is folded on each comparison
    so a non-conforming server or a hand-built test list still matches.

    Decoded as **latin-1**, which is what RFC 9110 says a header value is and — the part that
    matters — is a total function: every byte sequence decodes. A UTF-8 decode would raise on a
    malformed byte, which would turn a garbage header from an unauthenticated caller into an
    exception on the hot path, i.e. a 500 anyone can trigger.

    Trimmed with :data:`HTTP_OWS` — **SP and HTAB only**, never bare ``str.strip()``. See that
    constant: a wider trim gives one credential many accepted spellings, and the measured
    equivalence class included NBSP, NEL and the C0 separators. An empty result is reported as
    ``None``: ``X-API-Key:`` with nothing after it is a header that is present and carries no
    credential, and every caller here wants to treat that as "absent" rather than "the empty key".
    """
    for raw_name, raw_value in headers:
        if raw_name.lower() == name:
            value = raw_value.decode("latin-1").strip(HTTP_OWS)
            return value or None
    return None


def parse_credential(headers: RawHeaders) -> tuple[CredentialKind, str] | None:
    """Find the caller's credential in raw ASGI headers. See the module docstring for the order.

    Returns ``(kind, secret)`` — the *presented* secret, not a digest and not a principal — or
    ``None`` when there is nothing usable to check.

    The scheme is split off on a run of :data:`HTTP_OWS` (``[ \\t]+``), which absorbs the two
    spellings a real client produces — ``Bearer  <token>`` with several spaces, and a tab — while
    refusing every other byte a bare ``split()`` would have treated as a separator. ``Bearer`` with
    no token yields one part and lands on the "not parseable" path rather than on a lookup for the
    empty key. Because :func:`header_value` has already trimmed OWS from both ends, neither part
    can come back empty.

    Dispatch is through :data:`SCHEME_KINDS`, so the accepted set is the one
    :data:`WWW_AUTHENTICATE` advertises.
    """
    authorization = header_value(headers, AUTHORIZATION_HEADER)
    if authorization is not None:
        parts = _SCHEME_SPLIT_RE.split(authorization, maxsplit=1)
        if len(parts) == 2:
            kind = SCHEME_KINDS.get(parts[0].lower())
            if kind is not None:
                return kind, parts[1]

    # Fall-through, not abort: an Authorization header this service does not speak (`Basic`, a
    # bare `Bearer`) is not a credential, and it must not veto one the caller *did* present.
    raw_key = header_value(headers, API_KEY_HEADER)
    if raw_key is not None:
        return CredentialKind.API_KEY, raw_key
    return None


def apikey_digest(raw_key: str, *, pepper: str) -> str:
    """``hmac_sha256(pepper, raw_key)`` as 64 lower-case hex characters. THE storage transform.

    This one function is the reason a Redis dump is not a list of usable API keys: the pepper is
    an ``API_KEY_PEPPER`` that lives only in the process environment, so the digests in the store
    cannot be inverted, and a key presented to a *different* deployment (a different pepper) hashes
    to a digest that names no record. See the module docstring for why this is HMAC and not bcrypt.

    UTF-8 on both inputs, explicitly: the default encoding is a property of the platform, and a
    digest that depended on it would make an API key portable between two containers only when
    their locales agreed.
    """
    return hmac.new(pepper.encode("utf-8"), raw_key.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_api_key(raw_key: str, expected_digest: str, *, pepper: str) -> bool:
    """Constant-time check that ``raw_key`` is the key behind ``expected_digest``.

    .. rubric:: Note where this is *not* used: the resolution path

    :meth:`IdentityResolver.resolve` never compares two secrets. It computes the digest and uses it
    as a **key name** — Redis either has that record or it does not — which is the entire point of
    a deterministic, peppered digest and the exact property bcrypt's per-hash salt destroys. A
    lookup is O(1) and leaks no timing signal about *which* stored key was close, because no stored
    key is ever examined.

    This helper exists for the cases that genuinely do compare: C10's key management confirming
    that the raw key an operator pasted corresponds to the record they are about to revoke, and
    tests. ``hmac.compare_digest`` rather than ``==`` because those callers are comparing
    credential-derived material, and a short-circuiting comparison over attacker-influenced input
    is a timing oracle even when the thing compared is "only" a digest.
    """
    return hmac.compare_digest(apikey_digest(raw_key, pepper=pepper), expected_digest)


def _as_text(value: object) -> str:
    """Decode one Redis reply element (the gateway runs with ``decode_responses=False``)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_record(raw: Mapping[Any, Any]) -> dict[str, str]:
    """Normalise a raw ``HGETALL`` reply to ``str -> str``.

    One pass over at most four fields, so the cost is noise, and in exchange nothing downstream has
    to remember whether it is holding ``b"status"`` or ``"status"``. ``errors="replace"`` means a
    mangled byte in an operator-typed label surfaces as a visibly wrong character rather than as a
    ``UnicodeDecodeError`` thrown from inside authentication.
    """
    return {_as_text(key): _as_text(value) for key, value in raw.items()}


# ---------------------------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------------------------

#: Claims every token this service issues carries, and every claim it *requires* when reading one
#: back. Passed to ``jwt.decode`` as ``options={"require": ...}``.
#:
#: ``exp`` is required, not merely verified-if-present. A token with no expiry is a permanent
#: bearer credential that no rotation, revocation or incident response can retire, and PyJWT
#: happily accepts one — the absence of the claim means there is nothing to check, so the default
#: is "valid forever". Requiring it turns that from a silent property into a rejection.
#:
#: ``tier`` is conspicuously **not** here, and never will be. See the module docstring.
REQUIRED_CLAIMS: Final[tuple[str, ...]] = ("sub", "exp")


def issue_token(
    user_id: str,
    *,
    settings: Settings,
    ttl_min: int | None = None,
    now: datetime | None = None,
) -> str:
    """Sign a short-lived HS256 token naming ``user_id``. Returns the encoded token.

    There is deliberately **no login endpoint in this project** and this is not one. C13's E2E
    verifier needs to mint throwaway principals (a fresh ``uuid4`` per double-spend run, so the
    bucket it drains is provably untouched) and C14's load harness needs the same; both import this
    function rather than reimplementing the signing, which is what keeps "what a valid token looks
    like" a single definition instead of three that agree by luck.

    The payload is three claims — ``sub``, ``iat``, ``exp`` — and **no ``tier``**. A tier in the
    token would be either stale or attacker-chosen; see the module docstring. Adding one here would
    also be inert, because :meth:`IdentityResolver.resolve` does not read it.

    Args:
        user_id: Becomes ``sub``. Validated against :func:`~src.keys.sanitise_user_id` at the point
            of issue, so a token that could never be metered (an id containing a Redis Cluster
            brace) is refused where the mistake is, not deep inside the limiter's key builder.
        ttl_min: Lifetime in minutes; defaults to ``ACCESS_TOKEN_TTL_MIN``. A non-positive value
            mints an **already-expired** token, which is exactly how a test asserts the expiry path
            without sleeping through a TTL.
        now: Issue instant, defaulting to now. Injectable for the same reason. **Must be
            timezone-aware** — see below.

    Raises:
        ValueError: ``user_id`` cannot be hash-tagged, or ``now`` is a naive datetime.

    .. rubric:: A naive ``now`` is refused rather than assumed

    ``datetime.astimezone()`` reads a naive value as the **process's** local time, so
    ``issue_token(now=datetime.now())`` from a harness on a UTC+2 laptop would mint a token whose
    ``iat`` is two hours in the future. PyJWT then rejects it with *"The token is not yet valid
    (iat)"* — a message that names neither the timezone nor the caller, on a token that looks
    perfectly well-formed. It fails safe and it fails baffling, which is the combination worth
    spending one branch to remove.

    This is deliberately the opposite convention to :func:`src.keys._as_utc_datetime`, which reads a
    naive value *as* UTC. That is correct there because every producer feeding it is internal and
    provably UTC (the limiter's clock is ``redis.call('TIME')``). Here the caller is a harness
    author on an arbitrary machine, so the same assumption would be a guess about somebody else's
    environment. Guess when you know; refuse when you do not.
    """
    sanitise_user_id(user_id)
    issued_at = now if now is not None else datetime.now(timezone.utc)
    if issued_at.tzinfo is None:
        raise ValueError(
            "issue_token(now=...) requires a timezone-aware datetime; a naive one would be read "
            "as this process's local time and mint a token with a shifted iat/exp. Pass "
            "datetime.now(timezone.utc), or attach the tzinfo the instant actually has."
        )
    # Truncated to whole seconds before `exp` is derived, so the token's own arithmetic matches the
    # integer timestamps it carries rather than being 999 ms out from them.
    issued_at = issued_at.astimezone(timezone.utc).replace(microsecond=0)
    minutes = settings.access_token_ttl_min if ttl_min is None else ttl_min
    expires_at = issued_at + timedelta(minutes=minutes)
    payload = {
        "sub": user_id,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


# ---------------------------------------------------------------------------------------------
# Demo credentials
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DemoCredential:
    """One shipped demo principal: the raw key, who it is, and what tier they are on.

    Frozen and slotted like every other value type in this project. The raw key is a *literal in
    source* on purpose — these are demo credentials for a local stack, not secrets — and that is
    the whole reason this declaration is importable: the E2E verifier and the load harness read
    the same object the server seeded from, so a rename cannot desync the harness from the service
    it is verifying.
    """

    raw_key: str
    user_id: str
    tier: Tier
    label: str


#: The three seeded demo principals. ``demo-free-key`` and ``demo-premium-key`` are named
#: literally by the spec; ``demo-enterprise-key`` completes the tier ladder so the E2E verifier can
#: show all three ceilings without inventing a fourth naming convention.
DEMO_CREDENTIALS: Final[tuple[DemoCredential, ...]] = (
    DemoCredential(
        raw_key="demo-free-key", user_id="demo-free", tier=Tier.FREE, label="demo-free"
    ),
    DemoCredential(
        raw_key="demo-premium-key",
        user_id="demo-premium",
        tier=Tier.PREMIUM,
        label="demo-premium",
    ),
    DemoCredential(
        raw_key="demo-enterprise-key",
        user_id="demo-enterprise",
        tier=Tier.ENTERPRISE,
        label="demo-enterprise",
    ),
)

#: ``tier -> raw key``. What a harness actually wants ("give me a premium caller"), so it never has
#: to filter the tuple itself and never has to hard-code the string.
DEMO_KEY_BY_TIER: Mapping[Tier, str] = MappingProxyType(
    {credential.tier: credential.raw_key for credential in DEMO_CREDENTIALS}
)

#: ``user_id -> raw key``. The reverse lookup C13 needs after reading a ``user_id`` back out of the
#: admin usage endpoint.
DEMO_KEY_BY_USER: Mapping[str, str] = MappingProxyType(
    {credential.user_id: credential.raw_key for credential in DEMO_CREDENTIALS}
)


def _demo_records(settings: Settings, *, created_at: str) -> list[tuple[str, dict[str, str]]]:
    """Every ``(key, fields)`` pair one seed writes, in a deterministic order. Pure, no I/O.

    Separated from the write so the *what* is testable and reviewable without a server, and so the
    write below is a loop with no business logic in it.
    """
    records: list[tuple[str, dict[str, str]]] = []
    for credential in DEMO_CREDENTIALS:
        digest = apikey_digest(credential.raw_key, pepper=settings.api_key_pepper)
        records.append(
            (
                # `apikey_key` validates that this is 64 lower-case hex characters, which is what
                # makes it impossible to write the plaintext key into a Redis key name from here.
                apikey_key(digest),
                {
                    FIELD_USER_ID: credential.user_id,
                    FIELD_LABEL: credential.label,
                    FIELD_STATUS: STATUS_ACTIVE,
                    FIELD_CREATED_AT: created_at,
                },
            )
        )
        records.append(
            (
                user_key(credential.user_id),
                {
                    FIELD_TIER: credential.tier.value,
                    FIELD_STATUS: STATUS_ACTIVE,
                    FIELD_CREATED_AT: created_at,
                },
            )
        )
    return records


async def seed_demo_credentials(
    gateway: RedisGateway, settings: Settings, *, reseed: bool = False
) -> int:
    """Seed :data:`DEMO_CREDENTIALS` into ``apikey:v1:*`` and ``user:{id}``. Returns fields written.

    .. rubric:: ``HSETNX``, never ``HSET`` — the same rule, for the same reason, as C3's tier seed

    Every replica runs this on startup. With ``HSET``, an operator who revokes a demo key or moves
    a demo user to another tier gets it silently reverted the next time *any* replica restarts — a
    deploy, an OOM kill, a node drain. The change does not error and does not revert immediately;
    it reverts at some unrelated later moment, which is the most expensive shape a bug can have and
    is exactly what makes people stop trusting runtime configuration.

    **Per field, not per hash.** An operator who revokes ``demo-free-key`` by setting
    ``status=revoked`` has edited one field, and a replica restarting an hour later must leave that
    field alone while still being able to create a genuinely *missing* one. A whole-hash "does it
    exist?" check would restore the revoked key the first time any field was absent.

    .. rubric:: 21 ``HSETNX`` commands, ONE round trip

    The per-field granularity above is 21 commands (3 credentials x (4 key fields + 3 user fields)),
    and issuing them one at a time was 21 sequential round trips on every replica boot — 21 x RTT
    of dead time before the process serves, multiplied by the replica count, for a write that is a
    no-op after the first boot in the store's life.

    They are pipelined instead: identical commands, identical semantics, one round trip.
    ``transaction=False`` because this deliberately is **not** a transaction — each ``HSETNX`` is
    already atomic on its own, and wrapping them in ``MULTI``/``EXEC`` would add two commands and
    a stronger guarantee than the operation wants (a partially applied seed is fine; the next boot
    completes it, which is the whole property ``HSETNX`` is chosen for).

    Args:
        reseed: Overwrite with ``HSET``. Nothing in the service calls this — not startup, not the
            admin API — and that is deliberate: it is the escape hatch for a harness that needs a
            *known* state in a shared store, or an operator resetting a demo account they have
            edited into a corner. A parameter, so choosing it is a decision someone made; with no
            production caller, so nobody makes it by accident.

    Raises:
        BackingStoreUnavailable: Redis did not answer. Callers on the startup path
            (:meth:`IdentityResolver.start`) absorb it; a harness calling this directly wants to
            know, because a harness that silently seeded nothing then fails a dozen assertions
            about credentials that were never written.
    """
    # One timestamp for the whole seed, so every record from one boot agrees rather than differing
    # by however long the round trip took. Unix seconds because it is unambiguous about timezone
    # and sorts as a string of the same width for the next ~250 years.
    records = _demo_records(settings, created_at=str(int(time.time())))

    async def _write() -> list[Any]:
        async with gateway.client.pipeline(transaction=False) as pipe:
            for key, fields in records:
                if reseed:
                    pipe.hset(key, mapping=dict(fields))
                else:
                    for name, value in fields.items():
                        pipe.hsetnx(key, name, value)
            return list(await pipe.execute())

    # ONE `run()`, so the whole seed is one unit as far as the breaker and the counters are
    # concerned — which is also correct: it is one round trip, so it succeeds or fails as one.
    replies = await gateway.run(_write, op="identity:seed")

    if reseed:
        # `HSET` replies with the number of fields that were NEW, so an overwrite of an existing
        # record answers 0. That is the wrong number to report here: this call wrote every field by
        # definition, and returning 0 would make a successful reseed look like a no-op.
        return sum(len(fields) for _key, fields in records)
    return sum(int(bool(reply)) for reply in replies)


# ---------------------------------------------------------------------------------------------
# The resolver
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    """One cached answer and the monotonic instant it stops being usable.

    ``principal is None`` is a **cached negative** — a real answer meaning "no such key, or not an
    active one" — and is as valuable as a positive. See the module docstring.
    """

    principal: Principal | None
    expires_at: float


class IdentityResolver:
    """Turns raw ASGI headers into a :class:`~src.models.Principal`, or into ``None``.

    Constructed synchronously and performs **no I/O** in ``__init__`` — the same contract as
    :class:`~src.redis_client.RedisGateway`, :class:`~src.tiers.TierRegistry` and
    :class:`~src.limiter.Limiter` — so ``Runtime.build`` stays a plain function and the
    ``create_app(runtime=...)`` seam never opens a socket.

    Deliberately a sibling of :class:`~src.tiers.TierRegistry` in shape (an in-process cache in
    front of a Redis read, with injected clock and explicit counters) and deliberately *not* a
    sibling in policy. The tier registry serves a **stale** snapshot past its TTL and refreshes
    behind the caller, because a five-second-old tier limit is a non-event. This cache **expires**
    instead: serving a stale identity would mean authenticating a revoked key indefinitely, and
    "who is this?" is not a question worth answering out of date to save a round trip.
    """

    def __init__(
        self,
        gateway: RedisGateway,
        settings: Settings,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = IDENTITY_CACHE_MAX_ENTRIES,
        ttl_sec: float = IDENTITY_CACHE_TTL_SEC,
        max_concurrency: int | None = None,
    ) -> None:
        self._gateway = gateway
        self._settings = settings
        self._clock = clock
        # Floored at 1 so a mis-set cap cannot produce a cache that evicts the entry it just
        # inserted on every single call — a "cache" whose only effect is bookkeeping overhead.
        self._max_entries = max(1, max_entries)
        self._ttl_sec = max(0.0, float(ttl_sec))

        self._cache: OrderedDict[str, _CacheEntry] = OrderedDict()

        # The pre-auth concurrency bound. See `IDENTITY_POOL_SHARE`.
        #
        # The semaphore is built lazily, on first use, and rebuilt if the running event loop
        # changes. `Runtime.build` is synchronous and may run with no loop at all, and
        # `asyncio.Semaphore` binds to the loop that first awaits it and refuses every other one
        # — a failure that surfaces as "attached to a different loop" hundreds of lines from its
        # cause. Rebuilding on a loop change costs one identity comparison per lookup and removes
        # the whole class of failure; the same reasoning that makes the integration fixtures
        # function-scoped.
        self._max_concurrency = max(
            1, identity_concurrency(settings) if max_concurrency is None else max_concurrency
        )
        self._gate: asyncio.Semaphore | None = None
        self._gate_loop: asyncio.AbstractEventLoop | None = None
        self._in_flight = 0

        #: Cache lookups served without touching Redis, **including** cached negatives.
        self.hits = 0
        #: Lookups that had to ask Redis: never cached, or cached and expired.
        self.misses = 0
        #: The subset of :attr:`hits` that were cached negatives. A *subset*, not a separate
        #: bucket, so ``hits / (hits + misses)`` stays the hit rate rather than under-reporting it
        #: by exactly the traffic the negative cache exists to absorb. A large number here next to
        #: a small :attr:`misses` is the signature of a key-guessing flood being absorbed in-process.
        self.negative_hits = 0
        #: Entries dropped because the cache was full. Expiry is **not** counted here: an entry that
        #: aged out did its job, while an eviction means the working set is larger than the cap and
        #: is the number that says so.
        self.evictions = 0
        #: Lookups that had to wait for a permit because the concurrency bound was already full.
        #: The signature of the pre-auth flood the bound exists for: a large number here means
        #: unauthenticated traffic is being made to queue instead of being allowed to take the
        #: pool the limiter needs.
        self.gate_waits = 0
        #: High-water mark of concurrent lookups. Must never exceed
        #: :attr:`_max_concurrency`; ``tests/unit/test_overload.py`` asserts exactly that.
        self.peak_in_flight = 0

    # ------------------------------------------------------------------ #
    # The hot path
    # ------------------------------------------------------------------ #
    async def resolve(self, headers: RawHeaders) -> Principal | None:
        """Resolve the caller behind ``headers``, or ``None`` if there is no usable credential.

        Takes ``scope["headers"]`` directly rather than a Starlette ``Request``: see
        :func:`header_value`.

        ``None`` is the answer for *every* failure that is the caller's: no header, an unreadable
        one, an expired or forged token, an unknown key, a revoked key. C6 turns all of them into
        one 401, because distinguishing them on the wire is a credential-enumeration oracle and
        distinguishing them in a metric is C11's job, not the client's business.

        Raises:
            BackingStoreUnavailable: the API-key lookup could not reach Redis. **Not caught here**
                — see the module docstring. C8 must decide this case explicitly.
        """
        credential = parse_credential(headers)
        if credential is None:
            return None
        kind, secret = credential
        if kind is CredentialKind.JWT:
            return self._resolve_jwt(secret)
        return await self._resolve_api_key(secret)

    def digest(self, raw_key: str) -> str:
        """This resolver's digest for ``raw_key``, using the configured pepper.

        A bound convenience so no caller (C10's key management, the tests, a future CLI) has to
        reach into :class:`~src.config.Settings` for the pepper and pass it around — which is how a
        secret ends up in a log line, a function signature and eventually a stack trace.
        """
        return apikey_digest(raw_key, pepper=self._settings.api_key_pepper)

    # ------------------------------------------------------------------ #
    # JWT
    # ------------------------------------------------------------------ #
    def _resolve_jwt(self, token: str) -> Principal | None:
        """Verify a Bearer token and return its subject as a principal. Never raises.

        Not cached, and that is a decision rather than an omission: verification is one HMAC over a
        few hundred bytes (~10 microseconds) and touches **no Redis at all**, so a cache would save
        nothing measurable while adding a second in-memory copy of credential-derived material and
        a second revocation window. The cache exists to remove a *round trip*; there is no round
        trip on this path.
        """
        try:
            claims = jwt.decode(
                token,
                self._settings.jwt_secret,
                # =====================================================================
                # THE most important line on this path. `algorithms` is an ALLOWLIST and
                # it is pinned to the ONE algorithm this service signs with — never
                # `None`, never a list read out of the token.
                #
                # A decoder that honours the token's own `alg` header accepts
                # {"alg":"none"} with the signature segment removed: the token chooses
                # how it is verified, and anyone can then mint any `sub` they like. The
                # same allowlist blocks HS/RS confusion, where a token claiming HS256 is
                # verified against an RSA *public* key the attacker also holds.
                #
                # tests/unit/test_identity.py forges both and asserts they are refused.
                # =====================================================================
                algorithms=[self._settings.jwt_algorithm],
                # A token missing `sub` or `exp` is rejected by PyJWT before this module reads the
                # payload. Neither may be defaulted: an absent `sub` is not "anonymous", and an
                # absent `exp` is not "valid for a while" — it is "valid forever".
                options={"require": list(REQUIRED_CLAIMS)},
            )
        except jwt.PyJWTError:
            # ONE handler for every rejection — expired, tampered, wrong key, wrong algorithm,
            # missing claim, structurally malformed — because they are one fact to the caller:
            # this token is not usable. A `PyJWTError` escaping to the middleware would be a 500
            # on an input any unauthenticated client can send.
            #
            # DEBUG, and with NO token, no fragment of one, and no claim in the message. A rejected
            # token is still a credential (an expired one is a valid one that was valid a moment
            # ago), and log aggregators are the least private place in an incident.
            logger.debug("bearer token rejected", exc_info=True)
            return None

        subject = claims.get("sub")
        if not isinstance(subject, str):
            # `require: sub` proves the claim is present, not that it is a string. A numeric `sub`
            # would flow on as an int and become a Redis key component via str() — two callers,
            # `7` and `"7"`, silently sharing one bucket and one quota.
            logger.debug("bearer token carries a non-string 'sub' claim; refusing")
            return None
        try:
            sanitise_user_id(subject)
        except ValueError as exc:
            # An id containing a Redis Cluster brace can forge or collide with another principal's
            # key slot (see `src.keys.sanitise_user_id`). Refused here, at the door, rather than as
            # a ValueError from inside the limiter's key builder on the request path.
            logger.warning("bearer token subject is not a usable principal id: %s", exc)
            return None

        # `key_id=None`: a JWT has no stored credential record and therefore no operator-chosen
        # label. Putting the token, or any part of it, here would be the exact leak `Principal`
        # documents against.
        return Principal(user_id=subject, credential=CredentialKind.JWT, key_id=None)

    # ------------------------------------------------------------------ #
    # API keys
    # ------------------------------------------------------------------ #
    async def _resolve_api_key(self, raw_key: str) -> Principal | None:
        """Look ``raw_key`` up by its peppered digest, through the LRU+TTL cache.

        The Redis lookup — and **only** the Redis lookup — runs under the concurrency bound. The
        digest, the cache read and the cache write are microseconds of local work with no await in
        them, so holding a permit across them would shrink the effective bound for no reason; and
        a cache *hit*, which is the overwhelming majority of real traffic, never takes a permit at
        all. What is bounded is exactly the thing that consumes a pooled connection.
        """
        digest = self.digest(raw_key)

        hit, cached = self._cache_get(digest)
        if hit:
            return cached

        gate = self._acquire_gate()
        if gate.locked():
            # Sampled before the acquire: `locked()` is true precisely when every permit is out, so
            # this call is about to wait. Checked rather than timed because the number that matters
            # operationally is "how often is the pre-auth path being made to queue", not how long
            # any individual queue was.
            self.gate_waits += 1
        async with gate:
            self._in_flight += 1
            if self._in_flight > self.peak_in_flight:
                self.peak_in_flight = self._in_flight
            try:
                record = await self._gateway.run(
                    lambda: self._gateway.client.hgetall(apikey_key(digest)),
                    op="identity:apikey",
                )
            finally:
                # `finally`, so a `BackingStoreUnavailable` (which propagates to the middleware's
                # 503 branch) cannot leak a permit. A leaked permit is permanent: the bound would
                # shrink by one on every outage until the identity path serialised itself and then
                # deadlocked, long after the outage that caused it.
                self._in_flight -= 1

        principal = self._principal_from_record(record)
        # Cached whatever the answer was. A negative is a real answer and it is the one an attacker
        # generates in volume; see the module docstring.
        self._cache_put(digest, principal)
        return principal

    def _acquire_gate(self) -> asyncio.Semaphore:
        """Return the semaphore bound to the running loop, building it on first use.

        See ``__init__`` for why this is lazy and loop-aware rather than a field built in the
        constructor.
        """
        loop = asyncio.get_running_loop()
        if self._gate is None or self._gate_loop is not loop:
            self._gate = asyncio.Semaphore(self._max_concurrency)
            self._gate_loop = loop
            self._in_flight = 0
        return self._gate

    def _principal_from_record(self, raw: Mapping[Any, Any]) -> Principal | None:
        """Turn an ``apikey:v1:<digest>`` HASH into a principal, or ``None``. Never raises.

        Four ways this returns ``None``, and each is a different real event:

        * **The hash is empty.** No such key. The overwhelmingly common case, and the one the
          negative cache exists for.
        * **``status`` is not ``active``.** Revoked or suspended. A key whose record still exists
          but is not active must not authenticate — that is the entire mechanism behind revocation,
          and reading the record without checking the status would make ``status`` decorative.
        * **``user_id`` is missing.** A half-written record. There is nothing to meter, and
          inventing a fallback id would put an unknown caller into somebody else's bucket.
        * **``user_id`` is unusable** (empty, or containing a Redis Cluster brace). See
          :func:`~src.keys.sanitise_user_id`: such an id can collide with another principal's key
          slot, so it is refused rather than sanitised into a different account.

        The last three are logged at WARNING because they are *data defects an operator can fix*,
        and none of them logs the digest or the key: identifying which credential was malformed is
        worth less than not writing a lookup handle into stdout.
        """
        record = _decode_record(raw)
        if not record:
            return None

        status = record.get(FIELD_STATUS, "")
        if status != STATUS_ACTIVE:
            logger.debug("api key record is not active (status=%r); refusing", status)
            return None

        user_id = record.get(FIELD_USER_ID, "")
        try:
            sanitise_user_id(user_id)
        except ValueError as exc:
            logger.warning("api key record carries an unusable user_id: %s", exc)
            return None

        # `key_id` is the operator-chosen LABEL and nothing else. Never the raw key (it would be
        # replayable straight out of a log aggregator) and never the digest (it is the lookup key
        # for the whole record, so it correlates a user's every log line). `None` when the record
        # has no label, because an absent name is better than a fabricated one.
        return Principal(
            user_id=user_id,
            credential=CredentialKind.API_KEY,
            key_id=record.get(FIELD_LABEL) or None,
        )

    # ------------------------------------------------------------------ #
    # Cache
    # ------------------------------------------------------------------ #
    def _cache_get(self, digest: str) -> tuple[bool, Principal | None]:
        """Return ``(hit, principal)``. ``hit`` is False when Redis must be asked.

        ``(True, None)`` and ``(False, None)`` are different answers — a cached negative versus a
        cache miss — which is why this returns a pair rather than an ``Optional`` that would make
        "we know there is no such key" indistinguishable from "we have not looked".

        An expired entry is deleted and counted as a **miss**, not as an eviction: it aged out
        having done its job, whereas an eviction means the working set does not fit.
        """
        entry = self._cache.get(digest)
        if entry is None:
            self.misses += 1
            return False, None
        if self._clock() >= entry.expires_at:
            del self._cache[digest]
            self.misses += 1
            return False, None

        # `move_to_end` on every hit is what makes this an LRU rather than a FIFO. Without it, a
        # key used on every request would still be evicted once 10 000 other digests had been seen
        # after it — i.e. the busiest customer in the system would be the one paying for a Redis
        # round trip, which is precisely backwards.
        self._cache.move_to_end(digest)
        self.hits += 1
        if entry.principal is None:
            self.negative_hits += 1
        return True, entry.principal

    def _cache_put(self, digest: str, principal: Principal | None) -> None:
        """Store one answer, refresh its recency, and evict from the cold end at the cap."""
        self._cache[digest] = _CacheEntry(
            principal=principal, expires_at=self._clock() + self._ttl_sec
        )
        # Assigning to an existing key leaves its position alone in an OrderedDict, so a refreshed
        # entry would keep the recency of its predecessor and be evicted early.
        self._cache.move_to_end(digest)

        # `popitem(last=False)` takes the OLDEST end. `while`, not `if`, so a cap lowered at
        # runtime (or a resolver constructed with a smaller one) converges instead of staying one
        # entry over forever.
        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)
            self.evictions += 1

    def invalidate(self, digest_hex: str) -> bool:
        """Drop one cached identity by digest. Returns whether an entry was actually removed.

        C10 calls this after revoking or re-tiering a key, so the replica that served the change is
        exact rather than up to :data:`IDENTITY_CACHE_TTL_SEC` behind it. Every *other* replica
        converges on the TTL, which is a bound a test can assert rather than a race it must
        tolerate — the same trade C3's tier registry makes, for the same reason.

        Takes the **digest**, not the raw key, so a caller does not have to hold the plaintext to
        revoke it. The argument is validated through :func:`~src.keys.apikey_key`: passing a raw key
        here would otherwise be a silent no-op, and a revocation that silently does nothing is the
        worst possible outcome for this method. Use :meth:`digest` to convert.
        """
        # Built and discarded purely for its validation, which is the one definition of "this is a
        # digest and not a secret". This is an admin-path call, not a hot-path one.
        apikey_key(digest_hex)
        return self._cache.pop(digest_hex, None) is not None

    def clear(self) -> None:
        """Empty the cache. Used by C10 after a bulk credential change, and by tests.

        Counters are deliberately **not** reset: they are lifetime totals for ``/health`` and C11,
        and a hit rate that silently restarted every time somebody rotated a key would be a metric
        that only ever looks fine.
        """
        self._cache.clear()

    def cache_stats(self) -> dict[str, Any]:
        """Counter snapshot for C11's stats payload.

        ``hit_rate`` is computed here rather than by each consumer so "what counts as a hit" has
        one definition — cached negatives included, since absorbing them is the cache's second job.
        Zero lookups report ``0.0`` rather than dividing by zero: a process that has authenticated
        nobody has no hit rate, and 0.0 is the reading that does not look like a problem.
        """
        lookups = self.hits + self.misses
        return {
            "size": len(self._cache),
            "max_entries": self._max_entries,
            "ttl_sec": self._ttl_sec,
            "hits": self.hits,
            "misses": self.misses,
            "negative_hits": self.negative_hits,
            "evictions": self.evictions,
            "hit_rate": round(self.hits / lookups, 4) if lookups else 0.0,
            # The pre-auth concurrency bound and how hard it is being leaned on. Published beside
            # the cache counters because they are two halves of one story: `negative_hits` is the
            # part of a key-guessing flood absorbed in-process, and `gate_waits` is the part that
            # reached Redis and was made to queue for it.
            "max_concurrency": self._max_concurrency,
            "gate_waits": self.gate_waits,
            "peak_in_flight": self.peak_in_flight,
        }

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self, *, reseed: bool = False) -> None:
        """Seed the demo credentials. Called by ``Runtime.start``. **Never raises.**

        Same contract as :meth:`~src.tiers.TierRegistry.start`, and for the same reason: a replica
        that crash-loops because Redis was unreachable at boot enforces nothing at all, on every
        request, for as long as the loop lasts. Seeding is a convenience for a fresh store — the
        service authenticates perfectly well against credentials some other replica seeded, or
        against ones an operator created by hand — so there is no failure here worth trading a
        serving process for. Logged at ERROR, and the missing records are visible immediately as
        401s on the demo keys.

        The cache is cleared first so a re-started runtime cannot serve identities it resolved
        against a store it is no longer talking to.
        """
        self.clear()
        try:
            written = await seed_demo_credentials(self._gateway, self._settings, reseed=reseed)
        except Exception:  # noqa: BLE001 - see the docstring: startup must not crash-loop
            logger.error(
                "demo credential seeding failed; anything already stored is unchanged and the "
                "demo keys will 401 until a seed succeeds",
                exc_info=True,
            )
            return
        logger.info(
            "demo credentials seeded (%d fields written, users=%s)",
            written,
            ",".join(credential.user_id for credential in DEMO_CREDENTIALS),
        )

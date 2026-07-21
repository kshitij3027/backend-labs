"""Authentication primitives: the role/tier vocabulary, password hashing, and HS256 tokens.

**Threat model, in one paragraph.** A token here proves exactly one thing: that its bearer was
handed a string signed with this process's ``JWT_SECRET`` at some point inside the last
``ACCESS_TOKEN_TTL_MIN`` minutes, and that the ``sub``/``role``/``tier`` claims are the ones this
service put there — they are integrity-protected, so a client cannot promote itself from
``viewer`` to ``admin`` by editing the payload. It proves nothing else. The claims are
base64url, **not** encrypted, so anything in them is readable by whoever holds the token; there
is no revocation list, so a leaked token stays valid until it expires (which is why the TTL is
short rather than long); there is no proof-of-possession, so a stolen token is as good as the
original — bearer means bearer; and a token says nothing about the *current* state of the
account, only about its state at issue time. There is deliberately **no user-registration
surface**: this project is about the query API and its gates, not about identity management, so
the four bootstrap accounts below are the entire user population. That is a scope decision, not
an oversight — a real deployment replaces :func:`get_user_directory` with a lookup against a
real identity store and changes nothing else in this module.

This module imports only the standard library, :mod:`jwt`, :mod:`bcrypt`, :mod:`pydantic` and
:mod:`src.config`. It must never import ``src.main``, ``src.api``, ``src.store`` or
``src.models``: everything else in the app depends on auth, so auth depending back on any of
them would make the import graph a cycle and the module untestable in isolation.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from types import MappingProxyType

import bcrypt
import jwt
from pydantic import BaseModel, ConfigDict, Field

from src.config import Settings

logger = logging.getLogger(__name__)


# =============================================================================================
# The role ladder
# =============================================================================================


class Role(StrEnum):
    """The four access roles, spelled exactly as they appear in a token's ``role`` claim.

    ``StrEnum`` for the same reason :class:`~src.models.LogLevel` uses it: a member *is* its
    wire string, so ``Role.ADMIN == "admin"`` holds, ``jwt.encode`` serialises it with no
    conversion step, and a raw string lifted out of a decoded payload can be validated with
    ``Role(value)`` — which raises :class:`ValueError` on anything unrecognised rather than
    quietly coercing.
    """

    VIEWER = "viewer"
    ANALYST = "analyst"
    WRITER = "writer"
    ADMIN = "admin"


#: Role ordinals, 0 (least privileged) .. 3 (most privileged).
#:
#: An **ordinal ladder** rather than a permission set, on purpose. A permission set (``{"read",
#: "search", "append", "debug"}`` per role) is the more general model and the one a growing
#: product eventually needs — but the README specifies exactly four roles that are *strictly
#: ordered*, each including everything below it, and an integer comparison is the smallest
#: correct implementation of that statement. A set-based model would encode the same four
#: inclusions in twelve membership facts, giving twelve chances for the ladder to develop a hole
#: that no test notices. When a fifth role arrives that is genuinely *not* on this line, that is
#: the moment to switch representations — not before.
#:
#: ``MappingProxyType`` because it is a constant: a shared mutable dict that any caller could
#: edit would be a very quiet way to grant an entire role tier admin access at runtime.
ROLE_ORDER: Mapping[Role, int] = MappingProxyType(
    {
        Role.VIEWER: 0,
        Role.ANALYST: 1,
        Role.WRITER: 2,
        Role.ADMIN: 3,
    }
)


def role_satisfies(held: Role, minimum: Role) -> bool:
    """Does ``held`` meet a route's ``minimum`` role requirement?

    The ladder is **inclusive**: a role satisfies itself and everything below it, so an admin
    can do anything a viewer can. C7's ``require_role`` dependency is a thin wrapper over this
    one comparison.
    """
    return ROLE_ORDER[Role(held)] >= ROLE_ORDER[Role(minimum)]


class Tier(StrEnum):
    """The three rate-limit tiers.

    The member *values* are the lookup keys into ``Settings.tier_limits`` (whose default spec is
    ``free:10:20,pro:100:200,enterprise:1000:2000``), so these three strings and that config
    string have to agree. C8's limiter looks a bucket up by ``principal.tier``; a tier with no
    matching config entry would mean an *unlimited* principal, which is the worst possible
    failure mode for a rate limiter — hence the enum, which makes an unknown tier a token
    rejection at the door instead of a silent bypass deep in the limiter.
    """

    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class Principal(BaseModel):
    """The authenticated caller — what every guarded route receives.

    This is the *decoded* form of a token, not the token itself. Frozen because it describes a
    fact that was fixed the moment the token was signed: a handler that could mutate
    ``principal.role`` mid-request would be able to escalate its own privileges after the
    dependency chain had already approved it.

    ``extra="forbid"`` pins the shape, so C6's ``GET /auth/me`` response and the object the
    RBAC and rate-limit dependencies read are provably the same five fields.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    subject: str = Field(description="The `sub` claim — the authenticated username.")
    role: Role = Field(description="Access role: viewer | analyst | writer | admin.")
    tier: Tier = Field(description="Rate-limit tier: free | pro | enterprise.")
    issued_at: datetime = Field(description="The `iat` claim, as aware UTC.")
    expires_at: datetime = Field(description="The `exp` claim, as aware UTC.")


# =============================================================================================
# Password hashing
#
# bcrypt is called DIRECTLY here. Do NOT reintroduce passlib, however tempting its CryptContext
# API is: passlib 1.7.4 reads `bcrypt.__about__.__version__` during backend detection, and
# bcrypt 4.x removed the `__about__` module. The pair "works" only in the sense that it logs a
# spurious `(trapped) error reading bcrypt version` traceback on every process start and then
# limps on with a degraded backend. It is a known, unfixed breakage — and passlib is not in
# `requirements.txt` at all, so importing it would be an undeclared dependency on top of a
# broken one. `bcrypt.hashpw` / `bcrypt.checkpw` are three lines of wrapper; that is the trade.
# =============================================================================================

#: bcrypt hashes at most the first 72 bytes of a password. That is the algorithm's limit, not
#: this implementation's. bcrypt >= 4.1 *raises* ``ValueError`` on a longer input instead of
#: silently truncating, so the encode step below truncates explicitly — a 200-character
#: passphrase must authenticate, not 500.
BCRYPT_MAX_PASSWORD_BYTES = 72


def _password_bytes(plain: str) -> bytes:
    """Encode a password for bcrypt: UTF-8, truncated to the algorithm's 72-byte window.

    Truncation is applied to the *bytes*, so a multi-byte character straddling the boundary is
    cut mid-sequence. That is fine — bcrypt takes an opaque byte string and never decodes it —
    and it is exactly what bcrypt itself did before 4.1 turned the condition into an exception.
    """
    return plain.encode("utf-8")[:BCRYPT_MAX_PASSWORD_BYTES]


def hash_password(plain: str, *, rounds: int) -> str:
    """Hash a password with a freshly generated bcrypt salt at the given work factor.

    ``rounds`` is explicit rather than read from a global so the caller decides the cost:
    production uses ``Settings.bcrypt_rounds`` (12, ~250 ms), tests pass 4 (~2 ms). A work
    factor is the one security parameter that legitimately differs between the two, and passing
    it in beats a module-level default that a test has to monkeypatch.

    Returns the standard modular-crypt string (``$2b$<rounds>$<salt+digest>``), which is pure
    ASCII and carries its own salt and cost — so :func:`verify_password` needs no extra state.
    """
    hashed = bcrypt.hashpw(_password_bytes(plain), bcrypt.gensalt(rounds=rounds))
    return hashed.decode("ascii")


#: The exact shape of a bcrypt modular-crypt digest: a ``$2[abxy]$`` variant tag, a two-digit
#: cost, and a 53-character radix-64 tail (22 salt + 31 digest) — 60 characters in total.
#: bcrypt's radix-64 alphabet is ``./A-Za-z0-9``, which is NOT standard base64.
#:
#: Compiled once at module import because :func:`verify_password` runs it on every failed login,
#: and a per-call ``re.compile`` on the authentication hot path is pure waste.
BCRYPT_HASH_RE = re.compile(r"\$2[abxy]\$\d{2}\$[./A-Za-z0-9]{53}")


def verify_password(plain: str, hashed: str) -> bool:
    """Check a password against a bcrypt hash. **Returns False on garbage; never raises.**

    A corrupt stored hash is an *authentication failure*, not a server fault: letting the
    failure escape would turn a bad row in the user directory into a ``500``, which both leaks
    that the record exists and takes the endpoint down for everyone else. So the caller only
    ever sees a plain ``False``.

    .. rubric:: Why the shape is pre-checked instead of just catching the failure

    bcrypt 4.x is a **PyO3 extension**, and its hash parsing is Rust. A malformed digest with a
    *valid* ``$2b$NN$`` prefix but a truncated body (``$2b$04$too-short``) makes that code slice
    ``[..22]`` for the salt off a shorter remainder, which is a Rust **panic**, not a Python
    exception — it crosses the FFI boundary as ``pyo3_runtime.PanicException``, whose MRO is
    ``(PanicException, BaseException, object)``. It does not derive from :class:`Exception`, so
    neither ``except (ValueError, TypeError)`` nor even ``except Exception`` can catch it, and a
    single corrupt row would take down the token endpoint with an uncatchable error.

    The handler below does catch it, but catching a panic is the *backstop*, not the fix. A
    panic that has already unwound across the FFI boundary leaves the extension's internal state
    undefined by construction — the Rust side never completed its own cleanup — so the correct
    posture is to **not trigger it at all**. :data:`BCRYPT_HASH_RE` rejects anything that is not
    a well-formed digest before a single byte reaches Rust, which makes the panic path
    unreachable for every input we know of rather than merely survivable.
    """
    if not isinstance(hashed, str) or BCRYPT_HASH_RE.fullmatch(hashed) is None:
        # Deliberately logs neither the hash nor the password — only that one was unusable.
        logger.warning("password verification refused a malformed bcrypt hash")
        return False

    try:
        return bcrypt.checkpw(_password_bytes(plain), hashed.encode("utf-8"))
    except (ValueError, TypeError) as exc:
        logger.warning("password verification failed on a malformed hash: %s", exc)
        return False
    except BaseException as exc:  # noqa: BLE001 - see comment
        # bcrypt 4.x is a PyO3 extension; a Rust panic surfaces as
        # pyo3_runtime.PanicException, which derives from BaseException and NOT
        # from Exception, so no ordinary handler can catch it. Re-raise anything
        # that is not that panic so KeyboardInterrupt/SystemExit still propagate.
        if type(exc).__name__ != "PanicException":
            raise
        logger.error(
            "bcrypt panicked verifying a password; the stored hash passed the shape check "
            "but the extension could not parse it"
        )
        return False


# =============================================================================================
# Bootstrap users
# =============================================================================================


@dataclass(frozen=True, slots=True)
class DemoUser:
    """One bootstrap account: the credential record :func:`authenticate` matches against.

    Frozen so a handler holding a user cannot rewrite its own role, and because the directory is
    a process-wide cached singleton — a mutable record in a shared cache is a data race waiting
    for the first concurrent request.
    """

    username: str
    password_hash: str
    role: Role
    tier: Tier


#: The four bootstrap accounts, as ``username -> (plaintext password, role, tier)``.
#:
#: This is the **hash-free** description of the user population, and it is the only place the
#: four accounts are enumerated: :data:`DEV_PASSWORDS` and :func:`get_user_directory` are both
#: derived from it, so the credentials the E2E verifier uses and the hashes the API checks
#: against cannot drift apart.
#:
#: The tier assignment is not arbitrary — it is what makes the rate-limit behaviour testable:
#: ``viewer`` is on ``free`` (burst 20, so C12 can provoke a ``429`` in a handful of requests)
#: while ``admin`` is on ``enterprise`` (burst 2000, so the load harness measures the server
#: rather than its own tier ceiling).
DEV_ACCOUNTS: Mapping[str, tuple[str, Role, Tier]] = MappingProxyType(
    {
        "viewer": ("viewer-dev-pw", Role.VIEWER, Tier.FREE),
        "analyst": ("analyst-dev-pw", Role.ANALYST, Tier.PRO),
        "writer": ("writer-dev-pw", Role.WRITER, Tier.PRO),
        "admin": ("admin-dev-pw", Role.ADMIN, Tier.ENTERPRISE),
    }
)

#: ``username -> plaintext password`` for the demo accounts. Imported by
#: ``scripts/verify_e2e.py`` and ``scripts/load_test.py`` (C12) so the harnesses obtain their
#: credentials from the same declaration the server authenticates against, instead of
#: hard-coding a second copy that a rename would silently break.
DEV_PASSWORDS: Mapping[str, str] = MappingProxyType(
    {username: password for username, (password, _role, _tier) in DEV_ACCOUNTS.items()}
)

#: The password hashed for the "no such user" branch of :func:`authenticate`. Any constant does;
#: it exists only to be *checked against*, never to match.
_DUMMY_PASSWORD = "not-a-real-account-password"


@lru_cache(maxsize=8)
def _dummy_hash(rounds: int) -> str:
    """A throwaway hash at the given work factor, used by the unknown-user path."""
    return hash_password(_DUMMY_PASSWORD, rounds=rounds)


@lru_cache(maxsize=8)
def get_user_directory(rounds: int) -> Mapping[str, DemoUser]:
    """Build (once per work factor) the ``username -> DemoUser`` directory.

    **Hashing happens here, lazily — never at module import.** Four bcrypt hashes at the
    production cost of 12 rounds is roughly one second of pure CPU. Doing that at import time
    would add a second to every container start *and* to every pytest collection, for a
    directory most test modules never touch. Behind an ``lru_cache`` keyed on ``rounds``, the
    cost is paid at most once per process per work factor, and only if something actually
    authenticates: tests ask for ``rounds=4`` (~2 ms per hash, ~8 ms total) while production
    asks for 12.

    The cache also *is* the identity guarantee — the same call returns the same mapping object,
    so a hash computed for the first login is reused by every login after it.
    """
    directory = {
        username: DemoUser(
            username=username,
            password_hash=hash_password(password, rounds=rounds),
            role=role,
            tier=tier,
        )
        for username, (password, role, tier) in DEV_ACCOUNTS.items()
    }
    # Prime the dummy hash while we are already paying for bcrypt. Without this, the very first
    # unknown-username login would pay for a `gensalt` + `hashpw` on top of its `checkpw` and
    # take visibly longer than a wrong-password login — reintroducing, at exactly one request,
    # the enumeration signal the dummy hash exists to remove.
    _dummy_hash(rounds)
    return MappingProxyType(directory)


def dev_users(settings: Settings) -> Mapping[str, DemoUser]:
    """The user directory sized by ``settings.bcrypt_rounds`` — the route-level entry point.

    A one-line convenience so handlers never have to remember which settings field carries the
    work factor. There is intentionally **no module-level ``DEV_USERS`` constant**: any such
    constant would have to be hashed at import, which is precisely what
    :func:`get_user_directory` exists to avoid.
    """
    return get_user_directory(settings.bcrypt_rounds)


def authenticate(username: str, password: str, *, rounds: int) -> DemoUser | None:
    """Verify credentials, returning the matched user or ``None``.

    **The unknown-username path deliberately costs the same as the wrong-password path.** The
    obvious implementation returns ``None`` the moment the directory lookup misses, which makes
    the endpoint a user-enumeration oracle: a wrong password costs a full bcrypt verification
    (~250 ms at cost 12) while an unknown username costs a dict miss (~1 µs), so an attacker
    can map the entire user list with a stopwatch and never a single successful login. Hashing
    a fixed dummy value when the record is absent puts both branches through the same
    ``bcrypt.checkpw`` at the same work factor, so the timings are indistinguishable and the
    only thing an attacker learns is what the response body already tells them.

    The ``user is None`` test is evaluated *after* the verification, for the same reason — an
    early return would undo the work the dummy hash just did.
    """
    directory = get_user_directory(rounds)
    user = directory.get(username)
    reference_hash = user.password_hash if user is not None else _dummy_hash(rounds)

    password_ok = verify_password(password, reference_hash)

    if user is None or not password_ok:
        logger.info("authentication failed for %r", username)
        return None
    return user


# =============================================================================================
# Tokens
# =============================================================================================

#: Every claim this service puts in a token, and every claim it requires to be present when
#: reading one back. Passed to ``jwt.decode`` as ``options={"require": ...}``, so a token that
#: omits any of them is rejected by PyJWT before this module ever inspects the payload.
TOKEN_CLAIMS: tuple[str, ...] = ("sub", "role", "tier", "iat", "exp")


class AuthError(Exception):
    """A token could not be turned into a :class:`Principal`.

    One exception type for every failure mode — expired, tampered, wrong key, wrong algorithm,
    missing claim, unknown role — because they all mean the same thing to a caller: *this token
    is not usable*. The distinguishing detail lives in :attr:`reason`, which is short and safe
    to return to the client: telling someone their token expired is helpful, and telling them
    which byte of the signature was wrong is not something we know or would say.

    C7's ``current_principal`` dependency is the only intended consumer; it maps this to a
    ``401`` with a ``WWW-Authenticate: Bearer`` header. A raw PyJWT exception must never escape
    this module — one that did would surface as a ``500``, which is both wrong and a hint that
    the token got further than it should have.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        #: Short, client-safe description of why the token was rejected.
        self.reason = reason


def create_access_token(
    *,
    subject: str,
    role: Role,
    tier: Tier,
    settings: Settings,
    now: datetime | None = None,
) -> tuple[str, datetime]:
    """Sign a short-lived HS256 access token. Returns ``(token, expires_at)``.

    The five claims are exactly :data:`TOKEN_CLAIMS`: ``sub`` (username), ``role``, ``tier``,
    ``iat`` and ``exp``. Role and tier travel *in the token* rather than being looked up per
    request, which is what makes every gate downstream a pure function of the token — no user
    directory hit on the hot path — at the documented cost that a role change does not take
    effect until the current token expires.

    ``iat`` is truncated to whole seconds before ``exp`` is derived from it, so the returned
    ``expires_at`` is bit-identical to what :func:`decode_token` will report. Returning a
    microsecond-precision datetime while the token carried an integer timestamp would make the
    ``expires_in`` a client computes disagree with the one the server printed.

    Args:
        now: Issue time; defaults to the current UTC time. Injectable so a test can mint an
            already-expired token instead of sleeping through a TTL.
    """
    issued_at = (now if now is not None else datetime.now(UTC)).astimezone(UTC)
    issued_at = issued_at.replace(microsecond=0)
    expires_at = issued_at + timedelta(minutes=settings.access_token_ttl_min)

    payload = {
        "sub": subject,
        "role": Role(role).value,
        "tier": Tier(tier).value,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, expires_at


def decode_token(
    token: str, *, settings: Settings, now: datetime | None = None
) -> Principal:
    """Verify a token and return the :class:`Principal` it describes.

    Raises :class:`AuthError` — and only :class:`AuthError` — on every rejection path: an
    expired token, a tampered payload, a signature made with a different key, a token asking to
    be read with a different algorithm, a missing claim, or a ``role``/``tier`` that is not a
    real enum member.

    Args:
        now: Clock used for the expiry check, on top of PyJWT's own wall-clock check. Injectable
            so expiry is testable without sleeping. Note that this can only ever make the check
            **stricter**: PyJWT's ``verify_exp`` still runs against the real clock, so a test can
            fast-forward time but cannot resurrect a token that has genuinely expired.
    """
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            # =================================================================================
            # THE most important line in this module.
            #
            # `algorithms` is an ALLOWLIST, and it must be pinned to the one algorithm we sign
            # with. Passing `None`, or a list that includes "none", is the classic JWT forgery
            # hole: an attacker rewrites the header to {"alg":"none"}, drops the signature
            # segment entirely, sets "role":"admin", and a decoder that honours the token's own
            # `alg` field accepts it — the token gets to choose how it is verified. Pinning the
            # list to `[settings.jwt_algorithm]` means the header is checked against OUR choice
            # and an unexpected `alg` raises InvalidAlgorithmError before any signature work
            # happens. `tests/unit/test_auth.py::test_alg_none_token_rejected` forges exactly
            # that token and asserts it is refused.
            #
            # The same allowlist also blocks the HS/RS confusion attack, where a token claiming
            # `alg: HS256` is verified against an RSA *public* key that the attacker also has.
            # =================================================================================
            algorithms=[settings.jwt_algorithm],
            # A token missing any of our five claims is rejected here rather than defaulted
            # below — an absent `role` must never be read as "the lowest role", because that
            # turns a malformed token into a valid, if limited, session.
            options={"require": list(TOKEN_CLAIMS)},
        )
    except jwt.ExpiredSignatureError as exc:
        raise AuthError("token expired") from exc
    except jwt.InvalidSignatureError as exc:
        # Subclass of DecodeError, so it must be caught first or the generic handler wins.
        raise AuthError("token signature is invalid") from exc
    except jwt.InvalidAlgorithmError as exc:
        raise AuthError("token algorithm is not permitted") from exc
    except jwt.MissingRequiredClaimError as exc:
        raise AuthError(f"token is missing the {exc.claim!r} claim") from exc
    except jwt.DecodeError as exc:
        raise AuthError("token is malformed") from exc
    except jwt.PyJWTError as exc:
        # Catch-all so no PyJWT exception can ever escape as a 500. Anything reaching here is a
        # validation error we did not enumerate, which is still just "unusable token".
        raise AuthError("token is not valid") from exc

    return _principal_from_claims(payload, now=now)


def _epoch_to_utc(value: object) -> datetime:
    """Convert a numeric epoch-seconds claim into an aware UTC datetime.

    Raises :class:`TypeError` / :class:`ValueError` / :class:`OverflowError` / :class:`OSError`
    on anything that is not a usable timestamp; the single caller turns all four into an
    :class:`AuthError`.
    """
    return datetime.fromtimestamp(float(value), UTC)  # type: ignore[arg-type]


def _principal_from_claims(
    payload: Mapping[str, object], *, now: datetime | None
) -> Principal:
    """Turn a verified claim set into a :class:`Principal`, or raise :class:`AuthError`.

    Split out from :func:`decode_token` so the signature-verification concerns and the
    claim-shape concerns are readable separately. Everything here runs *after* the signature
    checked out, so these failures mean "we signed this, but it says something we do not
    recognise" — which is still a rejection, never a coercion.
    """
    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject:
        raise AuthError("token 'sub' claim is missing or empty")

    # `TypeError` alongside `ValueError` because the claim is attacker-chosen JSON: a `role` of
    # `{"a": 1}` or `[1, 2]` is not a string, and an unhandled TypeError here would be a 500.
    try:
        role = Role(payload.get("role"))
    except (ValueError, TypeError) as exc:
        # A token signed with OUR key but carrying "role": "superuser" is rejected, not coerced
        # and not downgraded to the lowest role. If our own key ever signed such a token, the
        # correct response is to refuse it loudly rather than to invent an interpretation.
        raise AuthError("token carries an unknown role") from exc

    try:
        tier = Tier(payload.get("tier"))
    except (ValueError, TypeError) as exc:
        # An unknown tier has no bucket in `Settings.tier_limits`, and "no bucket" would mean
        # "unlimited" — so this is refused at the door rather than papered over in C8.
        raise AuthError("token carries an unknown tier") from exc

    # `.get` rather than `[...]`: PyJWT's `require` option has already guaranteed both claims are
    # present, but a KeyError here would escape the except clause below and surface as a 500.
    # Belt and braces on the one path that must only ever produce an AuthError.
    try:
        issued_at = _epoch_to_utc(payload.get("iat"))
        expires_at = _epoch_to_utc(payload.get("exp"))
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise AuthError("token timestamps are not readable") from exc

    # Second expiry gate, against the injectable clock. Redundant with PyJWT's own check when
    # `now` is None (which is the point — belt and braces on the one claim that bounds the blast
    # radius of a leaked token), and strictly tighter when a future `now` is supplied. `>=`
    # rather than `>`: a token is dead *at* its expiry instant, not one second after it.
    effective_now = (now if now is not None else datetime.now(UTC)).astimezone(UTC)
    if effective_now >= expires_at:
        raise AuthError("token expired")

    return Principal(
        subject=subject,
        role=role,
        tier=tier,
        issued_at=issued_at,
        expires_at=expires_at,
    )

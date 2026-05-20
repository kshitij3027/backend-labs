"""Four redaction strategies behind a single ``Strategy`` Protocol.

Each :class:`Detection` from the detection layer (C2) is paired with a
strategy at runtime by the :class:`StrategyRegistry`. The four strategies
trade off reversibility against operational simplicity:

==============  ================  ====================  =====================
Strategy        Length-preserving  Reversible            Hot-path cost
==============  ================  ====================  =====================
``mask``        yes               no                    ``len(value)`` writes
``partial``     yes               partial (keeps tail)  ``O(len(value))``
``hash``        no (16 hex)       no                    one SHA-256 call
``tokenize``    no (~22 chars)    yes (RBAC-gated)      one dict lookup
==============  ================  ====================  =====================

The ``Strategy`` Protocol is :func:`runtime_checkable` so callers (and
tests) can ``isinstance(obj, Strategy)`` without importing every concrete
class. ``redact()`` is the entire surface — strategies are stateless apart
from any injected dependencies (salt for hash, token store for tokenize),
which makes them safe to share across the request pool.

Pattern dispatch in PartialStrategy
-----------------------------------
:class:`PartialStrategy` is the only strategy that varies by
``Detection.pattern_name`` — it has hand-tuned outputs for each of the
five PII shapes (SSN, credit card, US phone, email, MRN) plus a generic
"keep first + last char" fallback for NER hits (``person`` / ``org``)
and any future pattern that hasn't been special-cased yet.

The credit-card / phone / MRN cases share an internal helper
``_mask_digits_keep_last_n`` that walks the string, counts digits, and
replaces every digit except the trailing ``n`` with ``*``. Separators
(``-``, ``.``, ``+``, ``(``, ``)``, space) are preserved verbatim.

No timeouts here
----------------
Unlike the detection layer (which uses the ``regex`` library with a
per-pattern timeout), the redaction transforms run on inputs that have
already been size-bounded by the detection layer's match coordinates.
The work is linear in the match length, so a stdlib ``re`` import is
sufficient and a timeout would just be noise.
"""
from __future__ import annotations

import hashlib
import re
from typing import Protocol, runtime_checkable

from src.detection.patterns import Detection
from src.redaction.token_store import TokenStore


# ---------------------------------------------------------------------------
# Strategy protocol — the common surface all four implementations satisfy
# ---------------------------------------------------------------------------

@runtime_checkable
class Strategy(Protocol):
    """The minimal contract every redaction strategy must satisfy.

    Attributes
    ----------
    name : str
        Class-level identifier matching the key under which the strategy
        is registered (``"mask"``, ``"partial"``, ``"hash"``,
        ``"tokenize"``). Carried into audit logs so reviewers can see at
        a glance which transform was applied to a given field.
    """

    name: str

    def redact(self, value: str, detection: Detection) -> str:
        """Return the redacted form of ``value``.

        ``detection`` carries the pattern type and coordinates so
        strategies that need context (notably :class:`PartialStrategy`)
        can dispatch on ``detection.pattern_name``. Strategies that
        don't need it ignore the argument.
        """
        ...


# ---------------------------------------------------------------------------
# MaskStrategy — replace every char with ``*``
# ---------------------------------------------------------------------------

class MaskStrategy:
    """Length-preserving mask: every character becomes ``*``.

    Trade-off: zero information leak in the redacted output, but the
    consumer learns nothing about the original (not even "looks like an
    SSN" — the length is the only side channel). Useful as the default
    for high-sensitivity fields where partial preservation isn't worth
    the risk.

    The spec calls out:

        ``MaskStrategy().redact("123-45-6789", det) == "***********"``

    Note the hyphens count toward the length — every char in the match,
    separator or not, becomes a ``*``.
    """

    name: str = "mask"

    def redact(self, value: str, detection: Detection) -> str:
        """Return ``"*" * len(value)``; ``detection`` is unused."""
        # Constant-time relative to input length; the simplest possible
        # transform. We intentionally ignore ``detection`` here so the
        # mask is uniform across every pattern type.
        return "*" * len(value)


# ---------------------------------------------------------------------------
# PartialStrategy — pattern-aware partial redaction
# ---------------------------------------------------------------------------

class PartialStrategy:
    """Pattern-aware partial redaction: keep enough to debug, hide the PII.

    The output preserves separator characters verbatim and reveals only
    the last few digits / characters that are useful for support-team
    workflows (e.g., "is this the card ending in 1111?") without echoing
    the full PII.

    Spec-verified mappings
    ----------------------
    ============  ===============================  ===========================
    pattern        input                            output
    ============  ===============================  ===========================
    ssn           ``"123-45-6789"``                 ``"***-**-6789"``
    credit_card   ``"4111-1111-1111-1111"``         ``"****-****-****-1111"``
    credit_card   ``"4111 1111 1111 1111"``         ``"**** **** **** 1111"``
    us_phone      ``"(415) 555-1234"``              ``"(***) ***-1234"``
    email         ``"alice@example.com"``           ``"a***@example.com"``
    mrn           ``"MRN-123456"``                  ``"MRN-***456"``
    (default)     ``"Alice"``                       ``"A***e"``
    ============  ===============================  ===========================

    Why a single ``redact`` instead of one method per pattern
    ---------------------------------------------------------
    Pattern dispatch lives in a small if/elif ladder rather than a method
    table because (a) there are only six branches today and (b) keeping
    them in one place makes the spec-verification tests easier to read —
    a reviewer can scan the whole behavior at once.
    """

    name: str = "partial"
    # Compiled once at import; reused across every call. Used by the
    # internal helper to detect a digit without slicing the string.
    _DIGIT = re.compile(r"\d")

    def redact(self, value: str, detection: Detection) -> str:
        """Return the partially-redacted form of ``value``.

        Dispatch on ``detection.pattern_name``; the email case requires
        looking at the actual string (find the ``@``), the others delegate
        to :meth:`_mask_digits_keep_last_n` with a pattern-specific
        "keep last N" count.
        """
        pattern_name = detection.pattern_name

        if pattern_name == "ssn":
            # SSN: 9 digits total, keep last 4 → mask first 5. The
            # walk-and-count helper handles the embedded hyphens.
            return self._mask_digits_keep_last_n(value, keep_last_n=4)

        if pattern_name == "credit_card":
            # 13-19 digits per ISO/IEC 7812-1; we keep the last 4 in line
            # with PCI-DSS guidance for "truncated" PANs.
            return self._mask_digits_keep_last_n(value, keep_last_n=4)

        if pattern_name == "us_phone":
            # 10-11 digits; keep last 4 (the line number / subscriber id).
            return self._mask_digits_keep_last_n(value, keep_last_n=4)

        if pattern_name == "email":
            # ``a***@domain`` — keep the first char of the local-part so
            # the user is still recognizable to themselves in a support
            # ticket, replace the rest with three stars, keep the domain.
            at_idx = value.find("@")
            if at_idx >= 1:
                # Locator is at >= 1 to ensure there IS a local-part char
                # to preserve. Empty local-parts fall through to default.
                local_first = value[0]
                domain = value[at_idx + 1:]
                return f"{local_first}***@{domain}"
            # No "@" or empty local-part → use the generic fallback so
            # we never return an unredacted value.

        if pattern_name == "mrn":
            # MRN: "MRN-NNNNNN" (6 digits). Keep the last 3, mask the
            # first 3. The "MRN-" literal stays intact via the digit
            # walker (it never touches non-digit chars).
            return self._mask_digits_keep_last_n(value, keep_last_n=3)

        # ---- Default fallback (NER hits + unknown patterns) ----------
        # Examples: pattern_name in {"person", "org"}.
        if len(value) <= 2:
            # No room for "first + last with stars in between"; just mask
            # the whole thing so we don't leak the single character.
            return "*" * len(value)
        # ``A`` + ``***`` + ``e`` → "A***e" for "Alice" (the spec example).
        return value[0] + "*" * (len(value) - 2) + value[-1]

    # -- internal --------------------------------------------------------

    def _mask_digits_keep_last_n(self, value: str, keep_last_n: int) -> str:
        """Replace every digit except the trailing ``keep_last_n`` with ``*``.

        Separators (``-``, ``.``, ``+``, ``(``, ``)``, space, etc.) are
        preserved verbatim — the walker only acts on characters matched
        by :attr:`_DIGIT`. This keeps the output structurally identical
        to the input (same length, same separator positions) which is
        the whole point of the "partial" strategy.

        Algorithm: one pass to count total digits, a second to rewrite.
        Both are O(len(value)); we could collapse to a single pass with
        a deque but the simpler two-pass version is faster to read and
        the inputs are small (PII fragments, never large blobs).
        """
        # Count total digits so we know how many "leading" digits to mask.
        total_digits = sum(1 for ch in value if ch.isdigit())
        # Number of digits we need to replace before we start keeping.
        mask_count = max(0, total_digits - keep_last_n)

        out_chars: list[str] = []
        digits_seen = 0
        for ch in value:
            if ch.isdigit():
                digits_seen += 1
                # Replace this digit if we haven't yet hit the keep window.
                if digits_seen <= mask_count:
                    out_chars.append("*")
                else:
                    out_chars.append(ch)
            else:
                # Non-digit (separator / letter): preserve verbatim.
                out_chars.append(ch)
        return "".join(out_chars)


# ---------------------------------------------------------------------------
# HashStrategy — salted SHA-256, truncated to 16 hex chars
# ---------------------------------------------------------------------------

class HashStrategy:
    """Salted SHA-256 prefix: deterministic, non-reversible, fixed-width output.

    The 16-character hex prefix is short enough to stay legible in log
    output (e.g., ``"a1b2c3d4e5f60718"``) and long enough that the
    birthday-collision bound on 64 bits of digest is comfortably above
    the volume of unique PII a single deployment ingests in its lifetime.

    The salt (32 bytes / 64 hex chars) is loaded from the
    ``REDACTION_HASH_SALT`` env var by :mod:`src.redaction.salt`. The
    same plaintext always produces the same hash *within a deployment*
    so downstream consumers can correlate redacted records; different
    deployments have different salts so the same plaintext does NOT
    hash to the same value across them.
    """

    name: str = "hash"

    def __init__(self, salt: bytes) -> None:
        # Salt is checked for length by load_salt() — we don't re-validate
        # here. Stored as-is for the encode/concat path in redact().
        self._salt = salt

    def redact(self, value: str, detection: Detection) -> str:
        """Return ``sha256(salt || value).hexdigest()[:16]``.

        ``detection`` is unused — every plaintext hashes to the same
        prefix regardless of which pattern it came from. (Two different
        PII shapes that happen to share the same literal value will
        therefore hash to the same prefix; that's the dedup behavior
        we want for correlation.)
        """
        # Salt concatenation rather than HMAC is sufficient here because
        # the threat model is "non-reversibility from log output", not
        # "key recovery from oracle queries". SHA-256(salt || value) is
        # standard for simple salted hashing.
        return hashlib.sha256(self._salt + value.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# TokenizeStrategy — opaque token via TokenStore (reversible by admin)
# ---------------------------------------------------------------------------

class TokenizeStrategy:
    """Reversible tokenization via the bidirectional :class:`TokenStore`.

    Same plaintext ⇒ same token (the store deduplicates internally), so
    downstream joins on the redacted token still work. The reverse map
    is RBAC-gated behind ``role == "admin"`` — see :class:`TokenStore`.

    This strategy is the only one whose output is dependent on store
    state: a token issued in one process is NOT recognizable by another
    process running an independent store. The C10 cache layer (Redis-
    backed) is where cross-process consistency lives; until then, tokens
    are deployment-local.
    """

    name: str = "tokenize"

    def __init__(self, store: TokenStore) -> None:
        # We don't take ownership of the store — multiple strategies could
        # in principle share one if a future deployment wanted to. For
        # now there's only one TokenizeStrategy per registry, so it
        # effectively owns the store but the indirection is cheap.
        self._store = store

    def redact(self, value: str, detection: Detection) -> str:
        """Return the store's token for ``value``; ``detection`` is unused."""
        # Delegates entirely to the store; raises ``TokenStoreFullError``
        # if the store's max_size has been reached on a NEW plaintext.
        return self._store.tokenize(value)


# ---------------------------------------------------------------------------
# StrategyRegistry — name → Strategy instance lookup
# ---------------------------------------------------------------------------

class StrategyRegistry:
    """Name → :class:`Strategy` map; eagerly instantiates the four built-ins.

    The registry is constructed once at service startup with the loaded
    salt and an empty token store; the per-pattern dispatch in C4's
    ``ConfigurationManager`` uses :meth:`get` to look up the strategy
    referenced by each pattern's config entry.

    Why eager instantiation
    -----------------------
    All four strategies are cheap to construct (the only one with real
    work is :class:`HashStrategy`, which just stashes the salt). There's
    no reason to defer construction — eagerness means a misconfigured
    salt fails at startup, not on the first request.
    """

    def __init__(self, salt: bytes, token_store: TokenStore) -> None:
        # Insertion order in the dict matches the documented spec order
        # so iteration (e.g., for "list available strategies" introspection)
        # is deterministic.
        self._strategies: dict[str, Strategy] = {
            "mask": MaskStrategy(),
            "partial": PartialStrategy(),
            "hash": HashStrategy(salt),
            "tokenize": TokenizeStrategy(token_store),
        }

    def get(self, name: str) -> Strategy:
        """Return the registered :class:`Strategy` for ``name``.

        Raises
        ------
        KeyError
            If ``name`` is not one of the registered strategies. The
            error message lists the available names so the operator can
            spot the typo without digging through the source.
        """
        try:
            return self._strategies[name]
        except KeyError:
            # Sorted for stable error output; the set is small enough
            # that the sort cost is negligible.
            raise KeyError(
                f"unknown strategy {name!r}; available: {sorted(self._strategies)}"
            )

    def __contains__(self, name: str) -> bool:
        """Return ``True`` iff ``name`` is a registered strategy."""
        return name in self._strategies

"""In-memory bidirectional plaintext ↔ token store with RBAC + audit hooks.

The :class:`TokenStore` underpins the reversible ``tokenize`` redaction
strategy. A plaintext value (e.g., a customer ID) is mapped to an opaque
URL-safe token, the token is what flows into the redacted log line, and a
privileged caller can later reverse the mapping via :meth:`detokenize`.

Why a separate store rather than encrypted ciphertext
-----------------------------------------------------
Tokenization gives shorter, separator-free output than AES ciphertext,
which is friendlier to log aggregators that truncate long fields. The
trade-off is that tokenization is **stateful** — losing the store loses
the mapping — and reversibility is gated on a single role check.

Design contracts
----------------
* **Deduplication**: the same plaintext always produces the same token so
  downstream consumers can join across log lines.
* **Thread-safety**: an :class:`threading.RLock` (reentrant in case a
  callback re-enters the store) guards both directions of the map.
* **Capacity ceiling**: ``max_size`` caps the forward map so a malicious
  caller cannot exhaust memory by feeding unique plaintexts. The cap is
  enforced at insert time only; existing entries continue to work.
* **RBAC**: :meth:`detokenize` requires ``role == "admin"``. Any other
  role triggers a :class:`PermissionError` AFTER the audit hook records
  the denial — we want a tamper-evident trail of the attempt.
* **Audit hook**: every detokenize call invokes ``audit_callback`` exactly
  once with ``outcome ∈ {"success", "failure"}`` and a free-form
  ``reason``. The callback runs BEFORE we raise on failure so the trail
  is durable even when the caller swallows the exception.
"""
from __future__ import annotations

import logging
import secrets
import threading
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    # Type-only import to avoid creating a runtime dependency on the
    # cache package from the redaction layer. The Backend Protocol is
    # purely a static-typing aid here; the constructor accepts any
    # duck-typed object that walks the Backend shape.
    from src.cache.backend import Backend

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TokenStoreFullError(Exception):
    """Raised by :meth:`TokenStore.tokenize` when ``max_size`` is reached.

    Distinct from :class:`MemoryError` so callers can distinguish "I hit
    the configured ceiling" from "the OS refused more memory" — the former
    is recoverable by widening the cap, the latter is a process-level
    incident.
    """


# ---------------------------------------------------------------------------
# Audit callback type alias
# ---------------------------------------------------------------------------

# Keyword-only callable. We intentionally don't use Protocol here because
# the surface is small enough that a Callable alias is clearer, and the
# callback may be a lambda in tests where Protocol conformance is noise.
AuditCallback = Callable[..., None]


# ---------------------------------------------------------------------------
# TokenStore
# ---------------------------------------------------------------------------

class TokenStore:
    """Thread-safe bidirectional plaintext ↔ token map with capacity ceiling.

    Parameters
    ----------
    max_size : int, default ``100_000``
        Maximum number of unique plaintexts the store will accept.
        Reaching the cap causes :meth:`tokenize` to raise
        :class:`TokenStoreFullError` — but only for *new* plaintexts;
        re-tokenizing an already-stored value continues to work.
    backend : :class:`~src.cache.backend.Backend`, optional
        Cross-process cache backend. When provided, the store keeps its
        in-process dicts authoritative for fast lookups but the C10
        scope only requires acceptance of the parameter — actual mirror
        writes are deferred to a follow-up commit so the change stays
        bounded to "introduce the abstraction, wire Redis through the
        lifespan". See ``plan.md`` C10 deviations note.

    The two dicts (`_forward`, `_reverse`) are kept in lock-step: every
    `tokenize` either inserts into BOTH or NEITHER. We assert this via the
    lock — there is no path that updates only one direction.
    """

    def __init__(
        self,
        max_size: int = 100_000,
        backend: Optional["Backend"] = None,
    ) -> None:
        # plaintext → token. Used by tokenize() for dedup ("have I seen
        # this plaintext before?"). Lookups here are also why repeated
        # tokenize() calls are O(1) and deterministic.
        self._forward: dict[str, str] = {}
        # token → plaintext. Used by detokenize() for the reverse map.
        # A second dict is cheaper than scanning _forward for the value.
        self._reverse: dict[str, str] = {}
        self._max_size: int = max_size
        # Optional cross-process backend. C10 introduces the abstraction
        # and threads it through the lifespan; mirror writes happen in a
        # follow-up commit. Stored on the instance so future commits
        # don't have to touch __init__ again.
        self._backend: Optional["Backend"] = backend
        # RLock so an audit callback that (hypothetically) introspects the
        # store via size() doesn't deadlock. Plain Lock would be enough
        # for the current code but the extra cost is negligible.
        self._lock = threading.RLock()

    # -- public ----------------------------------------------------------

    def tokenize(self, plaintext: str) -> str:
        """Return the token for ``plaintext``, creating one on first sight.

        Same plaintext ⇒ same token (deterministic dedup). New plaintexts
        consume one slot of capacity; once ``len(forward) >= max_size``,
        further new plaintexts raise :class:`TokenStoreFullError`.

        The generated token uses :func:`secrets.token_urlsafe` with 16
        bytes of entropy ⇒ ~22 url-safe chars — short enough to remain
        legible in log output, long enough that brute-forcing the reverse
        map is infeasible.
        """
        with self._lock:
            # Fast path: the plaintext is already stored. We return the
            # existing token without touching capacity — re-tokenizing is
            # a no-op for the cap.
            existing = self._forward.get(plaintext)
            if existing is not None:
                return existing

            # Capacity check is done AFTER the dedup lookup so callers can
            # still re-tokenize known plaintexts when the store is full.
            if len(self._forward) >= self._max_size:
                raise TokenStoreFullError(
                    f"token store full (max_size={self._max_size})"
                )

            # 16 raw bytes → ~22 char base64url string. Collision probability
            # at 100k entries is vanishingly small (≈ 2^-100 per pair) so we
            # don't bother with a collision-retry loop.
            token = secrets.token_urlsafe(16)
            self._forward[plaintext] = token
            self._reverse[token] = plaintext
            return token

    def detokenize(
        self,
        token: str,
        *,
        role: str,
        audit_callback: Optional[AuditCallback] = None,
    ) -> str:
        """Return the plaintext that ``token`` maps to.

        RBAC: only ``role == "admin"`` is permitted. Any other role is
        denied at the gate BEFORE we touch the map — this avoids leaking
        existence information (i.e., a non-admin cannot distinguish "token
        exists but you can't have it" from "token doesn't exist").

        Audit ordering: the callback is invoked exactly once per call,
        BEFORE any exception is raised. That ensures the audit log records
        the attempt even if the caller swallows the resulting exception.

        Parameters
        ----------
        token : str
            The opaque token previously returned by :meth:`tokenize`.
        role : str
            Caller role; must be ``"admin"`` to succeed.
        audit_callback : callable, optional
            Invoked as ``audit_callback(token=..., role=..., outcome=...,
            reason=...)``. ``outcome`` is ``"success"`` or ``"failure"``;
            ``reason`` is ``None`` on success and a short tag
            (``"role_denied"`` / ``"not_found"``) on failure.

        Raises
        ------
        PermissionError
            If ``role != "admin"``.
        KeyError
            If the token is not present in the store.
        """
        # ---- RBAC gate (before lookup so we don't leak existence) -----
        if role != "admin":
            if audit_callback is not None:
                # Audit BEFORE raising so the denial is recorded even if
                # the caller's try/except swallows the PermissionError.
                audit_callback(
                    token=token,
                    role=role,
                    outcome="failure",
                    reason="role_denied",
                )
            raise PermissionError(f"role {role!r} cannot detokenize")

        # ---- Reverse lookup under lock --------------------------------
        with self._lock:
            plaintext = self._reverse.get(token)

        if plaintext is None:
            if audit_callback is not None:
                audit_callback(
                    token=token,
                    role=role,
                    outcome="failure",
                    reason="not_found",
                )
            # Use KeyError with just the token so callers can pattern-match
            # on the argument tuple if they need to.
            raise KeyError(token)

        # ---- Success path ---------------------------------------------
        if audit_callback is not None:
            audit_callback(
                token=token,
                role=role,
                outcome="success",
                reason=None,
            )
        return plaintext

    def size(self) -> int:
        """Return the number of unique plaintexts currently stored.

        Exposed for stats endpoints + capacity-pressure monitoring. Reads
        ``_forward`` under the lock so callers always see a consistent
        snapshot relative to in-flight :meth:`tokenize` calls.
        """
        with self._lock:
            return len(self._forward)

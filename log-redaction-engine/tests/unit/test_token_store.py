"""Unit tests for the C3 in-memory :class:`TokenStore`.

Coverage layout:

* Forward path (``tokenize``)   — dedup, capacity, thread-safety.
* Reverse path (``detokenize``) — RBAC gate, audit callback ordering,
  unknown-token handling.
* Misc                           — ``size()``, callback=None safety.

Every test that exercises detokenize uses a list-backed callback so we
can assert the exact invocation count and kwargs without mocking.
"""
from __future__ import annotations

import threading
from typing import Any

import pytest

from src.redaction.token_store import TokenStore, TokenStoreFullError


# ---------------------------------------------------------------------------
# Audit callback helper — captures every invocation as a kwargs dict
# ---------------------------------------------------------------------------

def _make_capturing_callback() -> tuple[list[dict[str, Any]], Any]:
    """Return ``(captured_list, callback)``.

    The callback appends each invocation's kwargs to ``captured_list`` so
    tests can assert ``len(captured) == N`` and inspect the recorded
    fields without dealing with ``MagicMock`` ergonomics.
    """
    captured: list[dict[str, Any]] = []

    def callback(**kwargs: Any) -> None:
        captured.append(kwargs)

    return captured, callback


# ---------------------------------------------------------------------------
# Forward path — tokenize()
# ---------------------------------------------------------------------------

class TestTokenize:
    """Dedup, capacity, and the basic token format."""

    def test_tokenize_returns_non_empty_string(self) -> None:
        # A first-sight plaintext returns a fresh, non-empty token.
        store = TokenStore()
        token = store.tokenize("alice@example.com")
        assert isinstance(token, str)
        assert token  # non-empty

    def test_tokenize_same_input_returns_same_token(self) -> None:
        # Dedup contract: ``store.tokenize(x)`` is referentially transparent
        # for a given plaintext. Same input twice ⇒ identical output.
        store = TokenStore()
        t1 = store.tokenize("same-input")
        t2 = store.tokenize("same-input")
        assert t1 == t2

    def test_tokenize_different_inputs_different_tokens(self) -> None:
        # Distinct plaintexts get distinct tokens.
        store = TokenStore()
        a = store.tokenize("alice")
        b = store.tokenize("bob")
        assert a != b

    def test_size_reflects_unique_count(self) -> None:
        # ``size()`` counts distinct plaintexts, NOT total tokenize calls.
        store = TokenStore()
        assert store.size() == 0
        store.tokenize("a")
        assert store.size() == 1
        store.tokenize("a")  # duplicate → no growth
        assert store.size() == 1
        store.tokenize("b")
        assert store.size() == 2


# ---------------------------------------------------------------------------
# Reverse path — detokenize() with RBAC + audit hooks
# ---------------------------------------------------------------------------

class TestDetokenize:
    """RBAC gate, audit callback ordering, error paths."""

    def test_admin_round_trip_returns_plaintext(self) -> None:
        # The basic round-trip: tokenize, then detokenize with admin role,
        # returns the original plaintext.
        store = TokenStore()
        token = store.tokenize("alice@example.com")
        recovered = store.detokenize(token, role="admin")
        assert recovered == "alice@example.com"

    def test_admin_success_invokes_callback_with_success_outcome(self) -> None:
        # Audit callbacks fire on the success path too — not just failures.
        store = TokenStore()
        token = store.tokenize("alice")
        captured, callback = _make_capturing_callback()
        store.detokenize(token, role="admin", audit_callback=callback)
        assert len(captured) == 1
        # Outcome / reason fields match the documented schema.
        assert captured[0]["outcome"] == "success"
        assert captured[0]["reason"] is None
        assert captured[0]["token"] == token
        assert captured[0]["role"] == "admin"

    def test_non_admin_role_raises_permission_error(self) -> None:
        # Any role other than "admin" is denied at the gate.
        store = TokenStore()
        token = store.tokenize("alice")
        with pytest.raises(PermissionError):
            store.detokenize(token, role="user")

    def test_non_admin_audit_called_once_with_role_denied(self) -> None:
        # The denial audit MUST fire even though the call raises — the
        # implementation calls the callback BEFORE the raise so swallowed
        # exceptions don't lose the audit trail.
        store = TokenStore()
        token = store.tokenize("alice")
        captured, callback = _make_capturing_callback()
        with pytest.raises(PermissionError):
            store.detokenize(token, role="user", audit_callback=callback)
        assert len(captured) == 1
        assert captured[0]["outcome"] == "failure"
        assert captured[0]["reason"] == "role_denied"
        assert captured[0]["role"] == "user"

    def test_unknown_token_raises_keyerror(self) -> None:
        # Admin role, but the token was never issued → KeyError.
        store = TokenStore()
        with pytest.raises(KeyError):
            store.detokenize("never-issued-token", role="admin")

    def test_unknown_token_audit_called_with_not_found(self) -> None:
        # Audit fires for "token not found" too. The reason tag is
        # documented as "not_found" so the dashboard can differentiate
        # missing tokens from role-denial events.
        store = TokenStore()
        captured, callback = _make_capturing_callback()
        with pytest.raises(KeyError):
            store.detokenize(
                "never-issued", role="admin", audit_callback=callback
            )
        assert len(captured) == 1
        assert captured[0]["outcome"] == "failure"
        assert captured[0]["reason"] == "not_found"
        assert captured[0]["role"] == "admin"
        assert captured[0]["token"] == "never-issued"

    def test_callback_none_does_not_raise_on_success(self) -> None:
        # ``audit_callback=None`` is the documented "no audit" sentinel
        # and must be a silent no-op (no NoneType.__call__ explosion).
        store = TokenStore()
        token = store.tokenize("alice")
        # Default value of audit_callback is None — exercise both call shapes.
        assert store.detokenize(token, role="admin") == "alice"
        assert (
            store.detokenize(token, role="admin", audit_callback=None)
            == "alice"
        )

    def test_callback_none_does_not_raise_on_failure(self) -> None:
        # Same contract on the failure paths — None audit doesn't crash.
        store = TokenStore()
        with pytest.raises(PermissionError):
            store.detokenize("x", role="user", audit_callback=None)
        with pytest.raises(KeyError):
            store.detokenize("x", role="admin", audit_callback=None)


# ---------------------------------------------------------------------------
# Capacity — max_size enforcement
# ---------------------------------------------------------------------------

class TestCapacity:
    """``max_size`` caps NEW plaintexts; re-tokenizing is unaffected."""

    def test_full_store_raises_on_new_plaintext(self) -> None:
        # max_size=2 → "a" + "b" fills the store; "c" must raise.
        store = TokenStore(max_size=2)
        store.tokenize("a")
        store.tokenize("b")
        with pytest.raises(TokenStoreFullError) as exc_info:
            store.tokenize("c")
        # The exception message includes the cap so operators know what
        # to raise it to.
        assert "max_size=2" in str(exc_info.value)

    def test_full_store_still_serves_existing_plaintext(self) -> None:
        # A full store can still re-tokenize an already-known plaintext —
        # the capacity check is gated on dedup miss only.
        store = TokenStore(max_size=2)
        t_a = store.tokenize("a")
        store.tokenize("b")
        # "a" is already known → no growth, no raise, same token.
        assert store.tokenize("a") == t_a
        assert store.size() == 2


# ---------------------------------------------------------------------------
# Concurrency — RLock makes parallel tokenize safe
# ---------------------------------------------------------------------------

class TestConcurrency:
    """10 threads × 100 unique inserts each → 1000 entries, no lost writes."""

    def test_concurrent_tokenize_sees_no_lost_writes(self) -> None:
        # Spawn 10 threads, each tokenizing 100 distinct plaintexts.
        # All inserts are disjoint (item-<thread>-<i>), so the final size
        # MUST be exactly 1000. Anything less means the RLock-guarded
        # dict mutation lost an entry to a race.
        store = TokenStore(max_size=10_000)

        num_threads = 10
        per_thread = 100

        def worker(thread_id: int) -> None:
            for i in range(per_thread):
                store.tokenize(f"item-{thread_id}-{i}")

        threads = [
            threading.Thread(target=worker, args=(tid,))
            for tid in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Every insert was disjoint → exactly num_threads * per_thread entries.
        assert store.size() == num_threads * per_thread

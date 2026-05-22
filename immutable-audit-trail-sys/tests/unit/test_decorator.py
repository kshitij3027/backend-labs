"""Unit tests for the @audit_access decorator.

We swap in a fake ChainAppender so we can inspect what was appended
without touching SQLite. The decorator's contract is: capture call-site
facts, forward to the appender, never swallow the wrapped exception,
never let audit-side errors propagate.
"""
import asyncio
import base64
import os
import time
from typing import Any

import pytest

from src.chain.appender import ChainAppender
from src.crypto.signer import Ed25519Signer
from src.interceptor.decorator import (
    audit_access,
    clear_appender,
    get_appender,
    set_appender,
)
from src.persistence.db import init_db, make_engine, make_session_factory


# --- Fixtures ----------------------------------------------------------------

class _RecordingAppender:
    """Stand-in for ChainAppender that just records the call args."""
    def __init__(self):
        self.calls: list[dict] = []
        self.raise_next = False

    async def append(self, **kwargs):
        if self.raise_next:
            self.raise_next = False
            raise RuntimeError("audit storage exploded")
        self.calls.append(kwargs)
        return None


@pytest.fixture
def fake_appender():
    a = _RecordingAppender()
    set_appender(a)  # type: ignore[arg-type]
    yield a
    clear_appender()


@pytest.fixture
def fake_request_with_header():
    """A mock Request with a .headers.get(name) interface."""
    class _Headers:
        def __init__(self, d): self._d = d
        def get(self, k, default=None): return self._d.get(k, default)
    class _Req:
        def __init__(self, h): self.headers = _Headers(h)
    return _Req


# --- Async function wrapping ------------------------------------------------

@pytest.mark.asyncio
async def test_async_success_records_one_entry(fake_appender):
    @audit_access(action="read", resource_static="LOG_X")
    async def find(q: str) -> list[str]:
        return [q, q]

    result = await find("alpha")
    assert result == ["alpha", "alpha"]
    assert len(fake_appender.calls) == 1
    call = fake_appender.calls[0]
    assert call["action"] == "read"
    assert call["resource"] == "LOG_X"
    assert call["success"] is True
    assert call["processing_ms"] >= 0
    assert call["error_message"] is None


@pytest.mark.asyncio
async def test_async_failure_records_and_reraises(fake_appender):
    @audit_access(action="read", resource_static="LOG_X")
    async def boom():
        raise ValueError("kapow")

    with pytest.raises(ValueError, match="kapow"):
        await boom()
    assert len(fake_appender.calls) == 1
    call = fake_appender.calls[0]
    assert call["success"] is False
    assert call["error_message"] == "kapow"
    assert call["result_digest"] == ""


# --- Sync function wrapping -------------------------------------------------

def test_sync_success_records_one_entry(fake_appender):
    @audit_access(action="search", resource_static="LOG_Y")
    def lookup(q: str) -> int:
        return len(q)

    out = lookup("hi")
    assert out == 2
    # The sync wrapper schedules audit via asyncio.run() if no loop is running.
    # Give it a beat to land — but asyncio.run() blocks, so it's already done.
    assert len(fake_appender.calls) == 1
    assert fake_appender.calls[0]["action"] == "search"
    assert fake_appender.calls[0]["success"] is True


def test_sync_failure_records_and_reraises(fake_appender):
    @audit_access(action="search", resource_static="LOG_Y")
    def explode():
        raise KeyError("missing")

    with pytest.raises(KeyError):
        explode()
    assert len(fake_appender.calls) == 1
    assert fake_appender.calls[0]["success"] is False
    assert "missing" in fake_appender.calls[0]["error_message"]


# --- Actor extraction -------------------------------------------------------

@pytest.mark.asyncio
async def test_anonymous_fallback_when_no_request(fake_appender):
    @audit_access(action="read", resource_static="X")
    async def fn(): return 1
    await fn()
    assert fake_appender.calls[0]["actor"] == "anonymous"


@pytest.mark.asyncio
async def test_actor_from_request_header(fake_appender, fake_request_with_header):
    @audit_access(action="read", resource_static="X")
    async def fn(req): return 1
    await fn(fake_request_with_header({"X-User-ID": "alice"}))
    assert fake_appender.calls[0]["actor"] == "alice"


@pytest.mark.asyncio
async def test_actor_from_request_kwarg(fake_appender, fake_request_with_header):
    @audit_access(action="read", resource_static="X")
    async def fn(*, request): return 1
    await fn(request=fake_request_with_header({"X-User-ID": "bob"}))
    assert fake_appender.calls[0]["actor"] == "bob"


# --- Audit failure isolation ------------------------------------------------

@pytest.mark.asyncio
async def test_audit_failure_does_not_propagate(fake_appender):
    """If appender.append raises, the wrapped function's result still returns."""
    @audit_access(action="read", resource_static="X")
    async def fn(): return "important_result"

    fake_appender.raise_next = True
    out = await fn()
    assert out == "important_result"  # wrapped result NOT swallowed


@pytest.mark.asyncio
async def test_audit_failure_does_not_swallow_wrapped_exception(fake_appender):
    """If both the wrapped fn AND the audit append raise, the wrapped exc wins."""
    @audit_access(action="read", resource_static="X")
    async def fn(): raise ValueError("original")

    fake_appender.raise_next = True
    with pytest.raises(ValueError, match="original"):
        await fn()


# --- Resource extraction ----------------------------------------------------

@pytest.mark.asyncio
async def test_resource_from_attribute_path(fake_appender):
    """resource_from='query.target' should pull from query.target on the arg."""
    class Query:
        def __init__(self, target): self.target = target

    @audit_access(action="search", resource_from="query.target")
    async def search(query): return None
    await search(Query("PATIENT_42"))
    assert fake_appender.calls[0]["resource"] == "PATIENT_42"


# --- Overhead sanity (loose) -----------------------------------------------

@pytest.mark.asyncio
async def test_decorator_overhead_under_10ms_median(fake_appender):
    """Median overhead per call should be well under 10ms (assignment goal).

    This is a smoke check, not a hard SLA — we use median over 100 calls
    inside the test container. CI noise may push p95 over 10ms; median is
    a calmer metric.
    """
    @audit_access(action="r", resource_static="x")
    async def noop(): return 0

    durations_ms = []
    for _ in range(100):
        t0 = time.perf_counter()
        await noop()
        durations_ms.append((time.perf_counter() - t0) * 1000)
    durations_ms.sort()
    median = durations_ms[len(durations_ms) // 2]
    # Median should be very small; even on a slow CI box, <10ms is generous.
    assert median < 10.0, f"median overhead {median:.3f}ms exceeds 10ms"


# --- Registry helpers -------------------------------------------------------

def test_registry_set_get_clear(fake_appender):
    assert get_appender() is fake_appender
    clear_appender()
    assert get_appender() is None

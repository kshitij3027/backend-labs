"""Tests for coordinator.retry.retry_async."""

from __future__ import annotations

import asyncio

import pytest

from coordinator import retry as retry_mod
from coordinator.retry import retry_async


async def test_retry_success_first_try(monkeypatch) -> None:
    calls = {"n": 0, "sleeps": 0}

    async def fake_sleep(_: float) -> None:
        calls["sleeps"] += 1

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    async def fn() -> int:
        calls["n"] += 1
        return 42

    out = await retry_async(fn, attempts=3, base_delay=0.1)
    assert out == 42
    assert calls["n"] == 1
    assert calls["sleeps"] == 0


async def test_retry_success_after_failures(monkeypatch) -> None:
    calls = {"n": 0}

    async def fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    async def fn() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    out = await retry_async(fn, attempts=5, base_delay=0.1)
    assert out == "ok"
    assert calls["n"] == 3


async def test_retry_exhausted_raises(monkeypatch) -> None:
    async def fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    calls = {"n": 0}

    async def fn() -> None:
        calls["n"] += 1
        raise ValueError(f"boom-{calls['n']}")

    with pytest.raises(ValueError) as ei:
        await retry_async(fn, attempts=3, base_delay=0.1)
    assert "boom-3" in str(ei.value)
    assert calls["n"] == 3


async def test_retry_delays_exponential(monkeypatch) -> None:
    delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        delays.append(d)

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    async def fn() -> None:
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        await retry_async(
            fn, attempts=3, base_delay=0.1, max_delay=10.0, jitter=0.1
        )

    # 2 sleeps between 3 attempts.
    assert len(delays) == 2
    # Lower bounds (without jitter): 0.1, 0.2
    assert delays[0] >= 0.1
    assert delays[1] >= 0.2
    # Upper bounds (jitter < 0.1): < base + jitter
    assert delays[0] < 0.1 + 0.1 + 1e-6
    assert delays[1] < 0.2 + 0.1 + 1e-6

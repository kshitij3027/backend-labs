"""Tests for ``ProgressEmitter`` and ``ProgressRegistry``."""

from __future__ import annotations

import asyncio

import pytest

from src.coordinator.progress import (
    ProgressEmitter,
    ProgressRegistry,
    default_registry,
)
from src.shared.models import ProgressEvent


# ---------------------------------------------------------------------------
# ProgressEmitter
# ---------------------------------------------------------------------------


async def test_emit_and_iterate_delivers_events_in_order() -> None:
    emitter = ProgressEmitter()

    events_in = [
        ProgressEvent(stage="plan_ready", payload={"steps": 4}),
        ProgressEvent(stage="partition_partition-1_started", payload={}),
        ProgressEvent(
            stage="partition_partition-1_complete", payload={"rows": 10}
        ),
        ProgressEvent(stage="aggregation_start", payload={}),
        ProgressEvent(stage="done", payload={"partials": 1}),
    ]

    async def produce() -> None:
        for e in events_in:
            await emitter.emit(e)
        await emitter.close()

    async def consume() -> list[ProgressEvent]:
        out: list[ProgressEvent] = []
        async for e in emitter.iter():
            out.append(e)
        return out

    received, _ = await asyncio.gather(consume(), produce())

    assert [e.stage for e in received] == [e.stage for e in events_in]
    assert received[0].payload == {"steps": 4}
    assert received[2].payload == {"rows": 10}


async def test_emit_after_close_is_noop() -> None:
    emitter = ProgressEmitter()
    await emitter.close()

    # Second close is safe and doesn't raise.
    await emitter.close()

    # Draining the iterator immediately ends.
    seen: list[ProgressEvent] = []
    async for e in emitter.iter():
        seen.append(e)
    assert seen == []

    # Emitting after close silently drops.
    await emitter.emit(ProgressEvent(stage="late", payload={}))
    assert emitter.closed is True


# ---------------------------------------------------------------------------
# ProgressRegistry
# ---------------------------------------------------------------------------


async def test_registry_create_get_remove() -> None:
    registry = ProgressRegistry()

    em = await registry.create("q-1")
    assert registry.get("q-1") is em
    assert registry.get("missing") is None

    popped = await registry.remove("q-1")
    assert popped is em
    assert registry.get("q-1") is None


async def test_default_registry_is_shared_singleton() -> None:
    r1 = default_registry()
    r2 = default_registry()
    assert r1 is r2

    # Smoke-test create/remove on the shared registry.
    em = await r1.create("q-shared")
    try:
        assert r2.get("q-shared") is em
    finally:
        await r1.remove("q-shared")

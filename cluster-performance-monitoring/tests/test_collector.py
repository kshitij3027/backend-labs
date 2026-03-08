"""Tests for the async MetricCollector."""

from __future__ import annotations

import asyncio

import pytest

from src.collector import MetricCollector
from src.models import MetricPoint, NodeInfo
from src.simulator import NodeSimulator
from src.storage import MetricStore


@pytest.mark.asyncio
async def test_collector_populates_store() -> None:
    """Collector stores metric points in the MetricStore over time."""
    node = NodeInfo(node_id="node-1", role="primary", host="localhost", port=8001)
    sim = NodeSimulator(node, seed=1)
    store = MetricStore(max_points_per_series=100)
    collector = MetricCollector(simulator=sim, store=store, interval=0.1)

    await collector.start()
    await asyncio.sleep(0.5)
    await collector.stop()

    assert store.point_count() > 0, "Store should contain points after collection"


@pytest.mark.asyncio
async def test_collector_stop_works() -> None:
    """After stop(), the internal task is done and no longer running."""
    node = NodeInfo(node_id="node-1", role="primary", host="localhost", port=8001)
    sim = NodeSimulator(node, seed=2)
    store = MetricStore(max_points_per_series=100)
    collector = MetricCollector(simulator=sim, store=store, interval=0.1)

    await collector.start()
    assert collector._task is not None
    await asyncio.sleep(0.2)
    await collector.stop()

    assert collector._task is None
    assert not collector._running


@pytest.mark.asyncio
async def test_callback_fires() -> None:
    """The on_new_metrics callback is invoked with lists of MetricPoint."""
    received: list[list[MetricPoint]] = []

    async def callback(points: list[MetricPoint]) -> None:
        received.append(points)

    node = NodeInfo(node_id="node-1", role="primary", host="localhost", port=8001)
    sim = NodeSimulator(node, seed=3)
    store = MetricStore(max_points_per_series=100)
    collector = MetricCollector(
        simulator=sim, store=store, interval=0.1, on_new_metrics=callback
    )

    await collector.start()
    await asyncio.sleep(0.5)
    await collector.stop()

    assert len(received) > 0, "Callback should have been called at least once"
    for batch in received:
        assert len(batch) > 0
        assert all(isinstance(p, MetricPoint) for p in batch)

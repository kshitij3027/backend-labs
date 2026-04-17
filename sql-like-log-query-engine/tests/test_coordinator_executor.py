"""Tests for ``QueryExecutor`` and its partition client plumbing.

We exercise the httpx code path end-to-end using ``httpx.MockTransport`` so
the retry, failure, and progress-event behaviour runs against the real
``httpx.AsyncClient`` wiring (no monkey-patching of internals).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable

import httpx
import pytest

from src.coordinator.executor import QueryExecutor
from src.coordinator.node_client import post_execute
from src.shared.models import (
    ExecutionPlan,
    ExecutionStep,
    PartitionMetadata,
    ProgressEvent,
    TimeRange,
)

from datetime import datetime


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------


PARTITION_IDS = ("partition-1", "partition-2", "partition-3")


def _make_partitions() -> list[PartitionMetadata]:
    tr = TimeRange(
        start=datetime(2026, 4, 1),
        end=datetime(2026, 4, 30),
    )
    return [
        PartitionMetadata(
            id=pid,
            url=f"http://{pid}:810{i}",
            time_range=tr,
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        )
        for i, pid in enumerate(PARTITION_IDS, start=1)
    ]


def _make_plan(partitions: list[PartitionMetadata]) -> ExecutionPlan:
    steps: list[ExecutionStep] = [
        ExecutionStep(op="prune", estimated_cost=1.0),
    ]
    for p in partitions:
        steps.append(
            ExecutionStep(
                op="filter",
                partition_id=p.id,
                filter={"ast": {"kind": "identifier", "name": "level"}},
                estimated_cost=100.0,
            )
        )
    steps.append(ExecutionStep(op="gather", estimated_cost=20.0))

    return ExecutionPlan(
        steps=steps,
        total_cost=sum(s.estimated_cost for s in steps),
        parallelism=len(partitions),
        optimization_notes=[
            f"Partition pruning: {len(partitions)}/{len(partitions)} partitions selected",
        ],
    )


@dataclass
class MockSpec:
    """Specification for how a mock transport should respond to each URL."""

    # Sequence of responses for each URL (consumed in order, last repeats).
    responses: dict[str, list[httpx.Response]] = field(default_factory=dict)
    # Per-URL call counter for assertions.
    call_counts: dict[str, int] = field(default_factory=dict)


def _make_transport(spec: MockSpec) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        # Match against URL up to path, ignoring host/scheme specifics.
        base = f"{request.url.scheme}://{request.url.host}"
        if request.url.port is not None:
            base += f":{request.url.port}"
        key = base
        spec.call_counts[key] = spec.call_counts.get(key, 0) + 1

        responses = spec.responses.get(key, [])
        if not responses:
            return httpx.Response(500, json={"detail": "no mock configured"})
        idx = min(spec.call_counts[key] - 1, len(responses) - 1)
        return responses[idx]

    return httpx.MockTransport(handler)


def _ok_response(
    rows: list[dict] | None = None,
    partial_aggregate: dict | None = None,
    records_scanned: int = 0,
) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "rows": rows or [],
            "partial_aggregate": partial_aggregate,
            "records_scanned": records_scanned,
        },
    )


def _collect_events() -> tuple[list[ProgressEvent], Callable]:
    events: list[ProgressEvent] = []

    async def cb(event: ProgressEvent) -> None:
        events.append(event)

    return events, cb


# ---------------------------------------------------------------------------
# all-success path
# ---------------------------------------------------------------------------


async def test_all_partitions_succeed() -> None:
    partitions = _make_partitions()
    lookup = {p.id: p for p in partitions}
    plan = _make_plan(partitions)

    spec = MockSpec(
        responses={
            "http://partition-1:8101": [_ok_response(rows=[{"a": 1}], records_scanned=10)],
            "http://partition-2:8102": [_ok_response(rows=[{"a": 2}], records_scanned=20)],
            "http://partition-3:8103": [_ok_response(rows=[{"a": 3}], records_scanned=30)],
        }
    )
    transport = _make_transport(spec)

    async with httpx.AsyncClient(transport=transport) as client:
        executor = QueryExecutor(client=client, request_timeout=1.0)
        events, cb = _collect_events()
        result = await executor.run(plan, lookup, progress_callback=cb)

    # All partials present.
    assert len(result["partials"]) == 3
    assert result["failed_partitions"] == []
    assert result["records_processed"] == 60

    # Progress events emitted in a sensible order.
    stages = [e.stage for e in events]
    assert stages[0] == "plan_ready"
    # One start + one complete per partition.
    for pid in PARTITION_IDS:
        assert f"partition_{pid}_started" in stages
        assert f"partition_{pid}_complete" in stages
    assert "aggregation_start" in stages
    assert stages[-1] == "done"

    # plan_ready must come before any partition_*_started event.
    plan_ready_idx = stages.index("plan_ready")
    first_start_idx = next(
        i for i, s in enumerate(stages) if s.endswith("_started")
    )
    assert plan_ready_idx < first_start_idx


# ---------------------------------------------------------------------------
# one-failure path
# ---------------------------------------------------------------------------


async def test_one_partition_always_fails_returns_others() -> None:
    partitions = _make_partitions()
    lookup = {p.id: p for p in partitions}
    plan = _make_plan(partitions)

    failing = httpx.Response(500, json={"detail": "boom"})
    spec = MockSpec(
        responses={
            "http://partition-1:8101": [_ok_response(rows=[{"a": 1}])],
            "http://partition-2:8102": [failing, failing, failing],
            "http://partition-3:8103": [_ok_response(rows=[{"a": 3}])],
        }
    )
    transport = _make_transport(spec)

    async with httpx.AsyncClient(transport=transport) as client:
        executor = QueryExecutor(client=client, request_timeout=1.0)
        events, cb = _collect_events()
        result = await executor.run(plan, lookup, progress_callback=cb)

    # Surviving partials.
    pids = [pid for pid, _ in result["partials"]]
    assert set(pids) == {"partition-1", "partition-3"}
    assert result["failed_partitions"] == ["partition-2"]

    # A failed event must have been emitted for partition-2.
    stages = [e.stage for e in events]
    assert "partition_partition-2_failed" in stages

    # partition-2 should have been retried 3 times (500 then 500 then 500).
    assert spec.call_counts["http://partition-2:8102"] == 3


# ---------------------------------------------------------------------------
# retry-succeeds path
# ---------------------------------------------------------------------------


async def test_retry_behaviour_first_fails_second_succeeds() -> None:
    partitions = _make_partitions()[:1]  # only partition-1 for clarity
    lookup = {p.id: p for p in partitions}
    plan = _make_plan(partitions)

    spec = MockSpec(
        responses={
            "http://partition-1:8101": [
                httpx.Response(500, json={"detail": "flake"}),
                _ok_response(rows=[{"a": 42}], records_scanned=5),
            ],
        }
    )
    transport = _make_transport(spec)

    async with httpx.AsyncClient(transport=transport) as client:
        executor = QueryExecutor(client=client, request_timeout=1.0)
        result = await executor.run(plan, lookup, progress_callback=None)

    assert len(result["partials"]) == 1
    pid, resp = result["partials"][0]
    assert pid == "partition-1"
    assert resp.rows == [{"a": 42}]
    assert result["failed_partitions"] == []
    # Retry was required — at least 2 calls.
    assert spec.call_counts["http://partition-1:8101"] >= 2


# ---------------------------------------------------------------------------
# post_execute unit coverage — raises after all retries consumed
# ---------------------------------------------------------------------------


async def test_post_execute_retries_and_raises() -> None:
    spec = MockSpec(
        responses={
            "http://partition-x:9999": [
                httpx.Response(500, json={"err": "nope"})
                for _ in range(5)
            ],
        }
    )
    transport = _make_transport(spec)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(Exception):
            await post_execute(
                client,
                "http://partition-x:9999",
                {"filter_ast_json": None},
                timeout=1.0,
                max_retries=3,
                backoff=(0.0, 0.0, 0.0),
            )

    # Called exactly `max_retries` times.
    assert spec.call_counts["http://partition-x:9999"] == 3


# ---------------------------------------------------------------------------
# empty plan
# ---------------------------------------------------------------------------


async def test_empty_plan_emits_aggregation_done_only() -> None:
    # Only a prune + gather step — no partition-bound work.
    plan = ExecutionPlan(
        steps=[
            ExecutionStep(op="prune", estimated_cost=1.0),
            ExecutionStep(op="gather", estimated_cost=20.0),
        ],
        total_cost=21.0,
        parallelism=1,
        optimization_notes=[],
    )

    async with httpx.AsyncClient(transport=_make_transport(MockSpec())) as client:
        executor = QueryExecutor(client=client, request_timeout=1.0)
        events, cb = _collect_events()
        result = await executor.run(plan, partition_lookup={}, progress_callback=cb)

    assert result == {
        "partials": [],
        "failed_partitions": [],
        "records_processed": 0,
    }
    stages = [e.stage for e in events]
    assert stages == ["plan_ready", "aggregation_start", "done"]

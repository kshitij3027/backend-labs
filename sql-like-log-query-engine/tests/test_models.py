from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.shared.models import (
    ExecutionPlan,
    ExecutionStep,
    PartitionExecuteResponse,
    QueryRequest,
    QueryResponse,
    TimeRange,
)


def test_time_range_rejects_end_not_greater_than_start():
    with pytest.raises(ValidationError):
        TimeRange(
            start=datetime(2026, 4, 10, 0, 0, 0),
            end=datetime(2026, 4, 10, 0, 0, 0),
        )
    with pytest.raises(ValidationError):
        TimeRange(
            start=datetime(2026, 4, 10, 0, 0, 0),
            end=datetime(2026, 4, 9, 0, 0, 0),
        )


def test_query_request_rejects_empty_query():
    with pytest.raises(ValidationError):
        QueryRequest(query="")


def test_query_response_round_trip():
    plan = ExecutionPlan(
        steps=[
            ExecutionStep(
                op="scan_filter",
                partition_id="partition-1",
                filter={"op": "=", "field": "level", "value": "ERROR"},
                estimated_cost=12.5,
            )
        ],
        total_cost=12.5,
        parallelism=3,
        optimization_notes=["Partition pruning: 2/3 partitions selected"],
    )
    original = QueryResponse(
        query_id="q-1",
        results=[{"service": "api", "n": 7}],
        records_processed=42,
        execution_time_ms=123.45,
        optimizations_applied=["Partition pruning"],
        plan=plan,
        partial_results=False,
        failed_partitions=[],
    )
    dumped = original.model_dump()
    rebuilt = QueryResponse.model_validate(dumped)
    assert rebuilt == original


def test_execution_plan_defaults():
    plan = ExecutionPlan(steps=[])
    assert plan.parallelism == 1
    assert plan.total_cost == 0.0
    assert plan.optimization_notes == []


def test_partition_execute_response_defaults():
    resp = PartitionExecuteResponse()
    assert resp.rows == []
    assert resp.records_scanned == 0
    assert resp.partial_aggregate is None

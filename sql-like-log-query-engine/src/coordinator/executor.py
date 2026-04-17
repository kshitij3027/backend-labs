"""Distributed query executor.

Given an :class:`ExecutionPlan` and a lookup of live ``PartitionMetadata``,
fan out one ``POST /execute`` per partition-bound step, gather their
responses, and return the raw partials for the aggregator to merge.

Per-partition failures are non-fatal: we collect them in ``failed_partitions``
and continue with whichever nodes survived. Every interesting transition
also emits a :class:`ProgressEvent` so downstream consumers (the WebSocket
in Commit 7) can surface real-time progress.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

import httpx

from src.shared.models import (
    ExecutionPlan,
    ExecutionStep,
    PartitionExecuteRequest,
    PartitionExecuteResponse,
    PartitionMetadata,
    ProgressEvent,
)

from .node_client import post_execute


ProgressCallback = Callable[[ProgressEvent], Awaitable[None]]


class QueryExecutor:
    """Scatter-gather runner over the healthy partitions."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        request_timeout: float = 5.0,
    ) -> None:
        self._client = client
        self._request_timeout = request_timeout

    # ------------------------------------------------------------------

    async def run(
        self,
        plan: ExecutionPlan,
        partition_lookup: dict[str, PartitionMetadata],
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """Execute all partition-bound steps concurrently.

        Returns ``{"partials": [(pid, resp), ...],
                   "failed_partitions": [pid, ...],
                   "records_processed": int}``.
        """

        emit = _make_emit(progress_callback)

        # Identify partition-bound steps (filter / partial_aggregate).
        partition_steps: list[ExecutionStep] = [
            s
            for s in plan.steps
            if s.op in ("filter", "partial_aggregate") and s.partition_id
        ]

        await emit(
            ProgressEvent(
                stage="plan_ready",
                payload={
                    "steps": len(plan.steps),
                    "parallelism": plan.parallelism,
                },
            )
        )

        # No partitions to hit? Hand control back to the aggregator with a
        # clean result set so the coordinator can still produce a valid
        # QueryResponse (empty, but well-formed).
        if not partition_steps:
            await emit(ProgressEvent(stage="aggregation_start", payload={}))
            await emit(ProgressEvent(stage="done", payload={}))
            return {
                "partials": [],
                "failed_partitions": [],
                "records_processed": 0,
            }

        # Issue every partition request concurrently.
        tasks = [
            self._run_one(step, partition_lookup, emit) for step in partition_steps
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        partials: list[tuple[str, PartitionExecuteResponse]] = []
        failed: list[str] = []
        records_processed = 0

        for step, result in zip(partition_steps, results):
            pid = step.partition_id or ""
            if isinstance(result, PartitionExecuteResponse):
                partials.append((pid, result))
                records_processed += result.records_scanned
            else:
                # Exception path — either our own bail-out or a gather-swallowed
                # exception. Already emitted `partition_{id}_failed` above.
                if pid and pid not in failed:
                    failed.append(pid)

        await emit(ProgressEvent(stage="aggregation_start", payload={}))
        await emit(
            ProgressEvent(
                stage="done",
                payload={
                    "partials": len(partials),
                    "failed": len(failed),
                    "records_processed": records_processed,
                },
            )
        )

        return {
            "partials": partials,
            "failed_partitions": failed,
            "records_processed": records_processed,
        }

    # ------------------------------------------------------------------

    async def _run_one(
        self,
        step: ExecutionStep,
        partition_lookup: dict[str, PartitionMetadata],
        emit: ProgressCallback,
    ) -> PartitionExecuteResponse:
        """Issue one partition request. Returning a response = success.

        Raising any exception = the partition is considered failed and the
        caller will record it in ``failed_partitions``. We always emit the
        matching ``partition_{id}_failed`` event before re-raising.
        """

        pid = step.partition_id or ""
        meta = partition_lookup.get(pid)

        await emit(
            ProgressEvent(
                stage=f"partition_{pid}_started", payload={"op": step.op}
            )
        )

        if meta is None:
            await emit(
                ProgressEvent(
                    stage=f"partition_{pid}_failed",
                    payload={"reason": "unknown partition"},
                )
            )
            raise ValueError(f"unknown partition {pid!r}")

        body = _build_request(step)

        try:
            response = await post_execute(
                self._client,
                meta.url,
                body,
                timeout=self._request_timeout,
            )
        except Exception as exc:
            await emit(
                ProgressEvent(
                    stage=f"partition_{pid}_failed",
                    payload={"error": str(exc)},
                )
            )
            raise

        await emit(
            ProgressEvent(
                stage=f"partition_{pid}_complete",
                payload={
                    "rows": len(response.rows),
                    "records_scanned": response.records_scanned,
                },
            )
        )
        return response


# --- helpers ---------------------------------------------------------------


def _build_request(step: ExecutionStep) -> PartitionExecuteRequest:
    """Translate an ``ExecutionStep`` into the partition's wire-level request."""

    filter_payload: dict | None = None
    if step.filter:
        # The planner wraps its serialized AST as {"ast": ...}. If the step
        # has some other shape we pass through — partition tolerates None too.
        filter_payload = step.filter.get("ast") if "ast" in step.filter else step.filter

    aggregation: dict | None = None
    group_by: list[str] = []
    if step.op == "partial_aggregate" and step.aggregation is not None:
        aggregation = dict(step.aggregation)
        group_by = list(aggregation.get("group_by", []) or [])

    return PartitionExecuteRequest(
        filter_ast_json=filter_payload,
        aggregation=aggregation,
        group_by=group_by,
        limit=None,
    )


def _make_emit(cb: ProgressCallback | None) -> ProgressCallback:
    """Return a callback that's a no-op when the caller passed ``None``."""

    if cb is None:
        async def _noop(_event: ProgressEvent) -> None:
            return None

        return _noop
    return cb

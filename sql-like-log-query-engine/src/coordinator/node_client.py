"""Thin HTTP wrapper around the partition nodes.

The coordinator creates one long-lived ``httpx.AsyncClient`` in its lifespan
and passes it into every function below. Nothing is instantiated at module
scope — that would leak between tests and make mock transports impossible.
"""

from __future__ import annotations

import asyncio
from typing import Sequence

import httpx

from src.shared.models import (
    PartitionExecuteRequest,
    PartitionExecuteResponse,
    PartitionMetadata,
)


_DEFAULT_BACKOFF: Sequence[float] = (0.1, 0.2, 0.4)


async def post_execute(
    client: httpx.AsyncClient,
    partition_url: str,
    request_body: PartitionExecuteRequest | dict,
    timeout: float = 5.0,
    max_retries: int = 3,
    backoff: Sequence[float] = _DEFAULT_BACKOFF,
) -> PartitionExecuteResponse:
    """POST the execute request to a partition, retrying on failure.

    Retry policy: up to ``max_retries`` total attempts, sleeping
    ``backoff[attempt]`` between attempts (``backoff`` is indexed by the
    *just-completed* attempt number). Any ``httpx`` transport exception or
    5xx HTTP response counts as a failure. The last exception is re-raised
    after the final attempt — the caller decides how to mark the partition.
    """

    if isinstance(request_body, PartitionExecuteRequest):
        payload = request_body.model_dump(mode="json")
    else:
        payload = dict(request_body)

    url = _join(partition_url, "/execute")

    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        try:
            response = await client.post(url, json=payload, timeout=timeout)
        except Exception as exc:  # pragma: no cover - network-level failure
            last_exc = exc
        else:
            if response.status_code < 500:
                # 2xx → parse; 4xx → surface as a non-retryable error.
                response.raise_for_status()
                return PartitionExecuteResponse.model_validate(response.json())
            # 5xx: retryable. Build a friendly exception to preserve for the
            # final raise in case we run out of retries.
            last_exc = httpx.HTTPStatusError(
                f"partition {partition_url} returned {response.status_code}",
                request=response.request,
                response=response,
            )

        # Not the last attempt? Sleep and retry.
        if attempt < max_retries - 1:
            delay = backoff[attempt] if attempt < len(backoff) else backoff[-1]
            await asyncio.sleep(delay)

    assert last_exc is not None  # defensive — we only enter the else path
    raise last_exc


async def fetch_metadata(
    client: httpx.AsyncClient,
    partition_url: str,
    timeout: float = 2.0,
) -> PartitionMetadata:
    """GET ``/metadata`` from a partition and return it as a model."""

    url = _join(partition_url, "/metadata")
    response = await client.get(url, timeout=timeout)
    response.raise_for_status()
    return PartitionMetadata.model_validate(response.json())


async def check_health(
    client: httpx.AsyncClient,
    partition_url: str,
    timeout: float = 2.0,
) -> bool:
    """GET ``/health`` from a partition and return True iff it responds 200."""

    url = _join(partition_url, "/health")
    try:
        response = await client.get(url, timeout=timeout)
    except Exception:
        return False
    return response.status_code == 200


def _join(base: str, path: str) -> str:
    """Join a base URL and path without introducing a double slash."""

    if base.endswith("/") and path.startswith("/"):
        return base + path[1:]
    if not base.endswith("/") and not path.startswith("/"):
        return f"{base}/{path}"
    return base + path

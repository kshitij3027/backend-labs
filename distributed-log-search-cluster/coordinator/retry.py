"""Exponential-backoff retry helper for async callables."""

from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 1.0,
    jitter: float = 0.1,
) -> T:
    """Retry ``fn`` up to ``attempts`` times with exponential backoff.

    Delays follow ``base_delay * 2**i`` (clamped at ``max_delay``) plus a
    random jitter in ``[0, jitter)``. Re-raises the last exception on
    exhaustion.
    """
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return await fn()
        except Exception as e:  # noqa: BLE001 - broad on purpose, surfaces below
            last_exc = e
            if i == attempts - 1:
                break
            delay = min(max_delay, base_delay * (2 ** i))
            delay += random.random() * jitter
            await asyncio.sleep(delay)
    assert last_exc is not None  # for type checkers
    raise last_exc

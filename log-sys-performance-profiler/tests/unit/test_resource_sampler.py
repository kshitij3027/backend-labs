from __future__ import annotations

import asyncio

from src.resource_sampler.sampler import ResourceSampler, ResourceSnapshot


async def test_sample_loop_populates_latest() -> None:
    sampler = ResourceSampler(stages=["parse", "validate"], interval_sec=0.05)
    task = asyncio.create_task(sampler.sample_loop())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    snap = sampler.latest_for("parse")
    assert snap is not None
    assert isinstance(snap, ResourceSnapshot)
    assert snap.mem_mb >= 0.0


async def test_queue_depth_callback_invoked() -> None:
    depths = {"parse": 11, "validate": 22}
    sampler = ResourceSampler(
        stages=["parse", "validate"],
        interval_sec=0.05,
        queue_depth_fn=lambda stage: depths[stage],
    )
    task = asyncio.create_task(sampler.sample_loop())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    parse_snap = sampler.latest_for("parse")
    validate_snap = sampler.latest_for("validate")
    assert parse_snap is not None and parse_snap.queue_depth == 11
    assert validate_snap is not None and validate_snap.queue_depth == 22


def test_psutil_priming_no_throw() -> None:
    # Constructing the sampler primes psutil — it must not raise.
    sampler = ResourceSampler(stages=["parse"], interval_sec=0.5)
    assert sampler is not None


def test_latest_for_unknown_returns_none() -> None:
    sampler = ResourceSampler(stages=["parse"], interval_sec=0.5)
    assert sampler.latest_for("parse") is None

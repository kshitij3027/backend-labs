"""Tests for the Redis stream consumer + synthetic data generator.

Uses a mixture of pure unit tests (for the backoff iterator and
message parser) and integration tests that hit the real Redis
instance from ``docker-compose.yml``. The integration tests skip
when Redis is unreachable, so the suite still passes on the host.

``REDIS_URL`` defaults to ``redis://redis:6379`` (the compose service
name) so tests run inside the ``test`` container out of the box.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
import uuid
from typing import AsyncIterator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
import redis.asyncio as redis_async
import redis.exceptions
from pydantic import ValidationError

from src.config import Settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry
from src.sample_data import LEVELS, SERVICES, generate_log_entries, generate_log_entry
from src.stream.redis_consumer import RedisStreamConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(**overrides) -> Settings:
    """Build a Settings instance with overridable fields.

    Keeps per-test customisation (unique stream/group, tight backoff,
    etc.) out of the default ``Settings()`` constructor so tests don't
    leak env values into each other.
    """
    base = Settings()
    data = base.model_dump()
    data.update(overrides)
    return Settings(**data)


async def _make_index(tmp_path) -> InvertedIndex:
    """Tiny InvertedIndex suitable for a single consumer test.

    Uses a per-test segment directory so spill-to-disk can't clobber
    another test's output. Doesn't call ``load_from_disk`` because a
    brand-new directory has nothing to load.
    """
    s = _make_settings(disk_segment_dir=str(tmp_path))
    return InvertedIndex(settings=s, tokenizer=LogTokenizer(), disk_dir=tmp_path)


async def _xadd(client, stream: str, entry: dict) -> bytes:
    """XADD a dict entry; returns the message id as bytes."""
    # redis-py's ``xadd`` wants bytes-safe kwargs; passing strings is
    # fine — the client encodes for us.
    return await client.xadd(stream, entry)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator:
    """Real Redis client from the docker-compose stack.

    Skips the test (rather than failing) when Redis is unreachable so
    the suite keeps passing when someone runs pytest on the host
    without the compose stack up.
    """
    url = os.environ.get("REDIS_URL", "redis://redis:6379")
    client = redis_async.from_url(url, decode_responses=False)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip(f"Redis not reachable at {url}")
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def clean_stream(redis_client) -> AsyncIterator[tuple[str, str]]:
    """Provide a unique ``(stream, group)`` pair per test.

    Random suffix means concurrent test runs never step on each
    other's stream. The fixture also cleans up the stream at teardown
    so Redis doesn't accumulate dozens of ``logs_test_*`` keys over
    the life of the container.
    """
    stream = f"logs_test_{uuid.uuid4().hex[:8]}"
    group = f"indexer_{uuid.uuid4().hex[:8]}"
    yield stream, group
    try:
        await redis_client.delete(stream)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Unit — backoff schedule
# ---------------------------------------------------------------------------

def test_backoff_schedule_progression(tmp_path):
    """Backoff doubles each step and saturates at the configured cap."""
    settings = _make_settings(
        redis_reconnect_backoff_base_s=0.5,
        redis_reconnect_backoff_max_s=30.0,
    )
    # We don't need a real index here — we never call run().
    consumer = RedisStreamConsumer(
        settings=settings,
        index=None,  # type: ignore[arg-type]
    )
    it = consumer._backoff_schedule()
    observed = [next(it) for _ in range(10)]
    assert observed == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0, 30.0]


# ---------------------------------------------------------------------------
# Unit — message parsing
# ---------------------------------------------------------------------------

def test_parse_message_valid(tmp_path):
    settings = _make_settings()
    consumer = RedisStreamConsumer(
        settings=settings,
        index=None,  # type: ignore[arg-type]
    )
    entry = consumer._parse_message(
        {
            b"message": b"hello",
            b"timestamp": b"1700000000.0",
            b"service": b"svc",
            b"level": b"INFO",
        }
    )
    assert isinstance(entry, LogEntry)
    assert entry.message == "hello"
    assert entry.timestamp == 1700000000.0
    assert entry.service == "svc"
    assert entry.level == "INFO"
    # doc_id is always 0 at parse time; the index assigns the real id.
    assert entry.doc_id == 0


def test_parse_message_missing_field_raises(tmp_path):
    settings = _make_settings()
    consumer = RedisStreamConsumer(
        settings=settings,
        index=None,  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError):
        consumer._parse_message(
            {
                # no b"message"
                b"timestamp": b"1700000000.0",
                b"service": b"svc",
                b"level": b"INFO",
            }
        )


def test_parse_message_invalid_level_raises(tmp_path):
    settings = _make_settings()
    consumer = RedisStreamConsumer(
        settings=settings,
        index=None,  # type: ignore[arg-type]
    )
    with pytest.raises(ValidationError):
        consumer._parse_message(
            {
                b"message": b"hi",
                b"timestamp": b"1700000000.0",
                b"service": b"svc",
                b"level": b"WTF",
            }
        )


# ---------------------------------------------------------------------------
# Integration — consumer group lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_group_idempotent(redis_client, clean_stream, tmp_path):
    """_ensure_group swallows BUSYGROUP on the second call."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index)
    await consumer._ensure_group(redis_client)
    # Second call must not raise.
    await consumer._ensure_group(redis_client)


# ---------------------------------------------------------------------------
# Integration — end-to-end ingest against real Redis
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consume_and_index_real_redis(redis_client, clean_stream, tmp_path):
    """XADD 50 messages, assert the consumer indexes all 50."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=50,
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index, batch_count=100)

    task = asyncio.create_task(consumer.run())
    # Give the consumer a moment to create the group before we add.
    # If we XADD before the group exists at id="0", the messages are
    # still delivered (mkstream + id="0" grabs everything from the
    # start), so this sleep is belt-and-braces rather than required.
    await asyncio.sleep(0.1)

    entries = generate_log_entries(50, rng=random.Random(42))
    for e in entries:
        await _xadd(redis_client, stream, e)

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if consumer.messages_processed >= 50:
            break
        await asyncio.sleep(0.05)

    await consumer.stop()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    assert consumer.messages_processed == 50
    assert index.stats()["docs_indexed"] == 50


@pytest.mark.asyncio
async def test_malformed_message_is_xacked_and_error_bumped(
    redis_client, clean_stream, tmp_path
):
    """Malformed messages bump ``errors`` but still get XACKed."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=50,
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index, batch_count=100)

    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.1)

    # One valid, one missing "message" field.
    valid = generate_log_entry(rng=random.Random(7))
    malformed = {"timestamp": str(time.time()), "service": "svc", "level": "INFO"}
    await _xadd(redis_client, stream, valid)
    bad_id = await _xadd(redis_client, stream, malformed)

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if consumer.messages_processed >= 1 and consumer.errors >= 1:
            break
        await asyncio.sleep(0.05)

    await consumer.stop()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    assert consumer.errors == 1
    assert consumer.messages_processed == 1

    # Verify both ids were XACKed — XPENDING summary should report
    # zero pending entries for the group. If the malformed id were
    # left pending, Redis would keep redelivering it on every
    # reconnect / consumer restart.
    pending = await redis_client.xpending(stream, group)
    # redis-py 5.x parses the summary into a dict; tolerate the raw
    # list form too for older clients.
    if isinstance(pending, dict):
        pending_count = pending.get("pending", 0)
    else:
        pending_count = pending[0]
    assert pending_count == 0


@pytest.mark.asyncio
async def test_consumer_group_shared_state(redis_client, clean_stream, tmp_path):
    """Two consumers on the same group split a 10-message backlog."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=50,
    )
    index = await _make_index(tmp_path)

    c1 = RedisStreamConsumer(
        settings=settings,
        index=index,
        consumer_name="c1",
        batch_count=3,
    )
    c2 = RedisStreamConsumer(
        settings=settings,
        index=index,
        consumer_name="c2",
        batch_count=3,
    )

    # Pre-create the group before XADDing. Otherwise one consumer may
    # grab everything before the other has even registered.
    await c1._ensure_group(redis_client)

    entries = generate_log_entries(10, rng=random.Random(101))
    for e in entries:
        await _xadd(redis_client, stream, e)

    t1 = asyncio.create_task(c1.run())
    t2 = asyncio.create_task(c2.run())

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if c1.messages_processed + c2.messages_processed >= 10:
            break
        await asyncio.sleep(0.05)

    await c1.stop()
    await c2.stop()
    for t in (t1, t2):
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()

    total = c1.messages_processed + c2.messages_processed
    assert total == 10
    assert index.stats()["docs_indexed"] == 10
    # Both consumers must have gotten at least one message — otherwise
    # the group sharing isn't actually being exercised.
    assert c1.messages_processed >= 1
    assert c2.messages_processed >= 1


# ---------------------------------------------------------------------------
# Integration-with-mock — reconnect behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconnect_on_connection_error(
    redis_client, clean_stream, tmp_path, monkeypatch
):
    """Simulate a short outage: first 2 xreadgroup calls fail, then succeed."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=50,
        # Very short backoff so the test doesn't wait 500 ms per retry.
        redis_reconnect_backoff_base_s=0.05,
        redis_reconnect_backoff_max_s=0.2,
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index, batch_count=10)

    # Pre-populate the stream so that, once the consumer successfully
    # reconnects and reads, there's actually something to index.
    await consumer._ensure_group(redis_client)
    entries = generate_log_entries(5, rng=random.Random(11))
    for e in entries:
        await _xadd(redis_client, stream, e)

    # Patch the consumer's ``_consume_once`` to raise on the first two
    # invocations, then delegate to the real implementation. Mocking
    # at this layer means we don't have to monkey-patch the raw Redis
    # client, which ``_make_settings``-bound code re-instantiates on
    # every reconnect.
    real_consume_once = consumer._consume_once
    call_count = {"n": 0}

    async def flaky_consume_once(client):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise redis.exceptions.ConnectionError("synthetic outage")
        return await real_consume_once(client)

    monkeypatch.setattr(consumer, "_consume_once", flaky_consume_once)

    task = asyncio.create_task(consumer.run())

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if consumer.messages_processed >= 5 and consumer.reconnects >= 1:
            break
        await asyncio.sleep(0.05)

    await consumer.stop()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    assert consumer.reconnects >= 1
    assert consumer.messages_processed >= 1


# ---------------------------------------------------------------------------
# Integration — shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_event_exits_run_quickly(redis_client, clean_stream, tmp_path):
    """stop_event flips → run() returns within 1 s."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=100,
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index)
    task = asyncio.create_task(consumer.run())

    await asyncio.sleep(0.2)
    t0 = time.monotonic()
    await consumer.stop()
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except asyncio.TimeoutError:
        task.cancel()
        pytest.fail("run() did not exit within 1 s after stop()")
    assert time.monotonic() - t0 < 1.0


# ---------------------------------------------------------------------------
# Sample data generator
# ---------------------------------------------------------------------------

def test_generate_log_entry_shape():
    entry = generate_log_entry(rng=random.Random(1))
    assert set(entry.keys()) == {"message", "timestamp", "service", "level"}
    assert isinstance(entry["message"], str) and entry["message"]
    assert isinstance(entry["timestamp"], float)
    assert entry["service"] in SERVICES
    assert entry["level"] in LEVELS


def test_generate_log_entries_deterministic():
    a = generate_log_entries(50, rng=random.Random(42))
    b = generate_log_entries(50, rng=random.Random(42))
    # ``timestamp`` is the one field that legitimately differs between
    # runs (``time.time()`` marches forward). Everything else must be
    # byte-identical for a deterministic seed.
    assert len(a) == len(b) == 50
    for ea, eb in zip(a, b):
        assert ea["message"] == eb["message"]
        assert ea["service"] == eb["service"]
        assert ea["level"] == eb["level"]


def test_generate_log_entries_contains_compound_tokens():
    """Across 200 entries, at least some messages embed an IP / UUID / email."""
    import re as _re

    entries = generate_log_entries(200, rng=random.Random(12345))
    joined = " ".join(e["message"] for e in entries)
    has_ip = bool(_re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", joined))
    has_uuid = bool(
        _re.search(
            r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
            joined,
            _re.I,
        )
    )
    has_email = bool(_re.search(r"\b[\w\.\+\-]+@[\w\-]+\.[\w\-\.]+\b", joined))
    assert has_ip or has_uuid or has_email


# ---------------------------------------------------------------------------
# Integration — bulk latency sanity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bulk_ingest_hits_latency_target(redis_client, clean_stream, tmp_path):
    """1000 messages drained within 5 s — sanity, not strict SLA."""
    stream, group = clean_stream
    settings = _make_settings(
        redis_stream_name=stream,
        redis_consumer_group=group,
        disk_segment_dir=str(tmp_path),
        batch_timeout_ms=50,
    )
    index = await _make_index(tmp_path)
    consumer = RedisStreamConsumer(settings=settings, index=index, batch_count=500)

    # Pre-create the group so XADDs before ``run()`` starts are delivered.
    await consumer._ensure_group(redis_client)

    entries = generate_log_entries(1000, rng=random.Random(777))
    # Pipeline the XADDs for throughput — a 1000-iteration await loop
    # over the network is noticeably slower than a single pipeline.
    pipe = redis_client.pipeline()
    for e in entries:
        pipe.xadd(stream, e)
    await pipe.execute()

    task = asyncio.create_task(consumer.run())
    t0 = time.monotonic()
    deadline = t0 + 5.0
    while time.monotonic() < deadline:
        if consumer.messages_processed >= 1000:
            break
        await asyncio.sleep(0.05)
    elapsed = time.monotonic() - t0

    await consumer.stop()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    assert consumer.messages_processed == 1000, (
        f"only processed {consumer.messages_processed} in {elapsed:.2f}s"
    )
    assert elapsed < 5.0, f"bulk ingest took {elapsed:.2f}s (>5s)"

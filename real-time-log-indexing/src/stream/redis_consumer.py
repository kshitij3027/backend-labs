"""Async Redis stream consumer — XREADGROUP with batching + backoff.

This module owns the ingest side of the pipeline. It:

* creates the consumer group idempotently on startup (``BUSYGROUP`` is
  swallowed);
* reads the stream in bounded batches with a ``BLOCK`` so we get the
  1-to-100 ms latency target without busy-looping;
* parses each message into a :class:`LogEntry`, delegates to
  :class:`InvertedIndex.add_documents_bulk`, then XACKs every id —
  malformed ones included, so a poison pill can't wedge the group;
* on connection errors, tears down the client and reconnects with
  exponential backoff capped at ``redis_reconnect_backoff_max_s``.

Design notes
------------

* We hold the raw ``redis.asyncio`` client in a local variable inside
  :meth:`run`, not on ``self``. That way a reconnect just rebinds the
  local without exposing torn state to the outside world.
* Parse errors raise ``ValueError`` from :meth:`_parse_message`; the
  batch loop catches them, bumps ``errors``, and still appends the
  id to the ACK list so Redis doesn't keep redelivering the message.
* :meth:`stop` is idempotent — tests call it more than once.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterator

import redis.asyncio as redis_async
import redis.exceptions

from src.config import Settings
from src.index.inverted_index import InvertedIndex
from src.models import LogEntry


logger = logging.getLogger("real_time_log_indexing.stream")


class RedisStreamConsumer:
    """Consumes a Redis stream in batches and indexes each message.

    Contract
    --------
    * Creates the consumer group at startup (idempotent — ``BUSYGROUP``
      is swallowed).
    * Loops: ``XREADGROUP GROUP <group> <consumer> STREAMS <stream> >
      COUNT <batch> BLOCK <timeout_ms>``.
    * Parses each message into :class:`LogEntry`; bumps ``errors`` on
      malformed.
    * ``await index.add_documents_bulk(entries)`` then ``XACK`` all ids
      (including malformed) so the stream doesn't keep redelivering.
    * On connection errors, disconnects, sleeps with exponential
      backoff (``base`` → ``max``), then retries from the top.
    * :meth:`stop` cancels the loop and closes the Redis client.
    """

    def __init__(
        self,
        settings: Settings,
        index: InvertedIndex,
        *,
        consumer_name: str | None = None,
        batch_count: int = 500,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._index = index
        self._consumer_name = consumer_name or f"consumer-{os.getpid()}"
        self._batch_count = batch_count
        self._stop_event = stop_event if stop_event is not None else asyncio.Event()

        # Held-state counters surfaced via ``stats``.
        self.errors: int = 0
        self.reconnects: int = 0
        self.messages_processed: int = 0
        self.last_stream_id: str | None = None
        self._last_successful_read_at: float | None = None

        # The active client. Created at the start of :meth:`run` and
        # rebound on reconnect. ``None`` until the first connect or
        # after :meth:`stop`.
        self._client: redis_async.Redis | None = None

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main loop. Returns when the stop event is set.

        The inner ``while`` is wrapped in a broad except for connection
        errors so a transient Redis outage triggers a reconnect rather
        than a fatal crash. Any other exception propagates — they
        indicate a programming error rather than an operational blip.
        """
        backoff_iter = self._backoff_schedule()
        self._client = redis_async.from_url(
            self._settings.redis_url, decode_responses=False
        )

        while not self._stop_event.is_set():
            try:
                await self._ensure_group(self._client)
                while not self._stop_event.is_set():
                    # ``_consume_once`` returns the count of messages
                    # processed; we don't sleep between iterations
                    # because ``BLOCK`` inside xreadgroup already paces
                    # us (up to ``batch_timeout_ms``).
                    await self._consume_once(self._client)
            except (
                ConnectionError,
                TimeoutError,
                redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError,
                OSError,
            ) as exc:
                if self._stop_event.is_set():
                    # Losing the client during shutdown is expected;
                    # don't count it as a reconnect.
                    break
                self.reconnects += 1
                delay = next(backoff_iter)
                logger.warning(
                    "Redis connection lost: %s; reconnecting in %.1fs",
                    exc,
                    delay,
                )
                # Best-effort close on the old client — don't let a
                # second failure here mask the original error.
                try:
                    await self._client.aclose()
                except Exception:
                    pass
                # Honour the stop event during backoff so tests that
                # flip stop mid-reconnect exit promptly.
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=delay
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                self._client = redis_async.from_url(
                    self._settings.redis_url, decode_responses=False
                )

        # Shutdown path — close whatever we still have.
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass

    async def stop(self) -> None:
        """Signal the loop to exit and close the Redis client.

        Idempotent: calling it twice is fine. Tests lean on that.
        """
        self._stop_event.set()
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ensure_group(self, client: redis_async.Redis) -> None:
        """Create the consumer group if it doesn't already exist.

        The ``BUSYGROUP`` error is swallowed — that's Redis's way of
        saying the group already exists, which is the happy path on
        every call after the first one.
        """
        stream = self._settings.redis_stream_name
        group = self._settings.redis_consumer_group
        try:
            await client.xgroup_create(stream, group, id="0", mkstream=True)
            logger.info("created consumer group %s on %s", group, stream)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _consume_once(self, client: redis_async.Redis) -> int:
        """Read one batch and process it. Returns the message count.

        Returns 0 on a BLOCK timeout (no new messages) so the outer
        loop can tick without side effects.
        """
        stream = self._settings.redis_stream_name
        group = self._settings.redis_consumer_group
        res = await client.xreadgroup(
            groupname=group,
            consumername=self._consumer_name,
            streams={stream: ">"},
            count=self._batch_count,
            block=self._settings.batch_timeout_ms,
        )
        if not res:
            return 0

        # ``res`` shape with ``decode_responses=False`` is a list of
        # ``(stream_name_bytes, [(msg_id_bytes, {field_bytes:
        # value_bytes}), ...])``. We only read one stream per call, so
        # index 0 is safe.
        messages = res[0][1]
        if not messages:
            return 0

        await self._process_batch(messages, client)
        return len(messages)

    async def _process_batch(
        self,
        messages: list[tuple[bytes, dict[bytes, bytes]]],
        client: redis_async.Redis,
    ) -> None:
        """Parse each message, bulk-index the valid ones, XACK all ids.

        Malformed messages (missing fields / bad level / unparseable
        timestamp) are logged and counted but *still* XACKed so Redis
        doesn't keep redelivering them. Leaving them pending would wedge
        the consumer group — exactly what we want to avoid.
        """
        entries: list[LogEntry] = []
        ids: list[bytes] = []
        for msg_id, fields in messages:
            ids.append(msg_id)
            try:
                entry = self._parse_message(fields)
            except Exception as exc:
                self.errors += 1
                logger.exception(
                    "malformed stream message %s: %s",
                    msg_id.decode(errors="replace"),
                    exc,
                )
                continue
            # Tag the entry with its Redis id so downstream code can
            # correlate / re-XACK if needed.
            entry.stream_id = msg_id.decode()
            entries.append(entry)

        if entries:
            await self._index.add_documents_bulk(entries)

        if ids:
            stream = self._settings.redis_stream_name
            group = self._settings.redis_consumer_group
            await client.xack(stream, group, *ids)

        self.messages_processed += len(entries)
        if ids:
            self.last_stream_id = ids[-1].decode()

        if entries:
            logger.info(
                "indexed batch size=%d stream_id=%s",
                len(entries),
                self.last_stream_id,
            )

    def _parse_message(self, fields: dict[bytes, bytes]) -> LogEntry:
        """Decode a single XREADGROUP field map into a :class:`LogEntry`.

        Accepts any of ``bytes``/``str`` keys and values — ``bytes``
        are the common case because we construct the client with
        ``decode_responses=False``. Required fields: ``message``,
        ``timestamp``, ``service``, ``level``. Timestamp may be int or
        float; anything else raises ``ValueError``.

        The returned entry has ``doc_id=0`` because the real id is
        assigned by the inverted index when the entry is admitted.
        """
        def _get(key: str) -> str:
            # Tolerate both bytes and str keys/values so mock tests and
            # real Redis calls hit the same path.
            if key.encode() in fields:
                val = fields[key.encode()]
            elif key in fields:
                val = fields[key]
            else:
                raise ValueError(f"missing field: {key}")
            if isinstance(val, bytes):
                return val.decode("utf-8")
            return str(val)

        message = _get("message")
        raw_ts = _get("timestamp")
        service = _get("service")
        level = _get("level")

        try:
            timestamp = float(raw_ts)
        except (TypeError, ValueError) as e:
            raise ValueError(f"invalid timestamp: {raw_ts!r}") from e

        return LogEntry(
            doc_id=0,
            message=message,
            timestamp=timestamp,
            service=service,
            level=level,
        )

    def _backoff_schedule(self) -> Iterator[float]:
        """Exponential backoff capped at the configured max.

        Yields ``base``, ``2*base``, ``4*base``, … saturating at
        ``redis_reconnect_backoff_max_s``. Every call to :meth:`run`
        re-starts the schedule from the base so a short outage doesn't
        poison the next recovery with a long delay.
        """
        delay = self._settings.redis_reconnect_backoff_base_s
        cap = self._settings.redis_reconnect_backoff_max_s
        while True:
            yield min(delay, cap)
            delay *= 2

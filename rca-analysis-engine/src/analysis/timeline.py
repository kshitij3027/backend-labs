"""Timeline reconstruction for the RCA Analysis Engine (C2).

The :class:`TimelineReconstructor` turns an unordered batch of :class:`LogEvent`
into a chronologically-ordered list of :class:`TimelineEntry` — the first stage of
``RCAAnalyzer.analyze``. It tolerantly parses each event's timestamp
(``python-dateutil``), stable-sorts events by absolute time, and emits per-entry
metadata the later stages rely on:

* a contiguous 1-based ``sequence_id``,
* a human-readable ``relative_time`` offset from the incident start, formatted
  ``T+M:SS`` (minutes unbounded, seconds zero-padded — e.g. ``T+0:00`` for the
  first event, ``T+2:05`` for one 125s later),
* a stable ``event_id`` — the client-supplied id when present, otherwise a
  deterministic SHA-1 digest so the same input always yields the same id (and the
  causal-graph builder in C3 can key nodes by it), and
* a ``context`` dict of surrounding-event pointers (preceding / following /
  prior-same-service ids plus position/total).

The reconstructor is pure and O(n log n): no network, no globals, no wall-clock
reads. An unparseable timestamp raises :class:`ValueError` (the API layer maps it to
HTTP 422 in C5).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from dateutil import parser as _dateutil_parser
from dateutil.parser import isoparse

from src.config import Settings
from src.models import LogEvent, TimelineEntry


def _parse_timestamp(raw: str) -> datetime:
    """Parse an event timestamp into a timezone-aware UTC-comparable datetime.

    Tries strict ISO-8601 first (``isoparse``, which already understands a trailing
    ``Z``), then falls back to the lenient general parser. Naive datetimes are
    assumed to be UTC so aware/naive inputs sort consistently. An unparseable value
    raises :class:`ValueError` with a descriptive message.
    """
    try:
        dt = isoparse(raw)
    except (ValueError, TypeError, OverflowError):
        try:
            dt = _dateutil_parser.parse(raw)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"unparseable event timestamp: {raw!r}") from exc
    # Treat naive timestamps as UTC so every parsed datetime is aware and mutually
    # comparable (mixing naive/aware would raise on sort).
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _derive_event_id(original_index: int, event: LogEvent) -> str:
    """Deterministically derive a stable id for an event that lacks one.

    Keyed on the event's original input position plus its content so the id is
    unique per event and identical across runs for identical input.
    """
    raw_key = (
        f"{original_index}|{event.timestamp}|{event.service}"
        f"|{event.level.value}|{event.message}"
    )
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:12]
    return f"evt-{digest}"


class TimelineReconstructor:
    """Reconstruct a chronological incident timeline from a batch of log events."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def reconstruct(self, events: list[LogEvent]) -> list[TimelineEntry]:
        """Return a chronologically-ordered timeline for ``events``.

        Events are stable-sorted by parsed timestamp (ties keep input order). Each
        entry carries its sequence id, absolute + relative time, a stable event id
        (back-filled onto the source :class:`LogEvent`), and surrounding-event
        context. Empty input yields an empty timeline. Raises :class:`ValueError`
        if any timestamp cannot be parsed.
        """
        if not events:
            return []

        # (parsed_dt, original_index, event). Built in input order; the stable sort
        # below keeps that order for equal timestamps.
        records: list[tuple[datetime, int, LogEvent]] = [
            (_parse_timestamp(event.timestamp), index, event)
            for index, event in enumerate(events)
        ]
        records.sort(key=lambda record: record[0])

        incident_start = records[0][0]
        total = len(records)

        # First pass: resolve every event id (and back-fill it onto the LogEvent) so
        # the context can reference preceding/following neighbours by id.
        entry_ids: list[str] = []
        for _dt, original_index, event in records:
            event_id = event.event_id or _derive_event_id(original_index, event)
            event.event_id = event_id
            entry_ids.append(event_id)

        # Second pass: build the entries with relative offsets and context.
        entries: list[TimelineEntry] = []
        last_id_by_service: dict[str, str] = {}
        for position, (dt, _original_index, event) in enumerate(records):
            event_id = entry_ids[position]
            delta_seconds = (dt - incident_start).total_seconds()
            minutes = int(delta_seconds // 60)
            seconds = int(delta_seconds % 60)
            relative_time = f"T+{minutes}:{seconds:02d}"

            context = {
                "preceding_event_id": entry_ids[position - 1] if position > 0 else None,
                "following_event_id": (
                    entry_ids[position + 1] if position < total - 1 else None
                ),
                "prior_same_service_event_id": last_id_by_service.get(event.service),
                "position": position + 1,
                "total": total,
            }

            entries.append(
                TimelineEntry(
                    sequence_id=position + 1,
                    timestamp=event.timestamp,
                    relative_time=relative_time,
                    service=event.service,
                    level=event.level,
                    message=event.message,
                    event_id=event_id,
                    context=context,
                )
            )
            last_id_by_service[event.service] = event_id

        return entries

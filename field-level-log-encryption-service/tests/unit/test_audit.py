"""Unit tests for the C6 audit subsystem.

Three suites:

* :class:`TestRingBuffer` — bounded deque wrapper invariants:
  ordering, overflow, ``__len__``, ``clear``, and thread safety.
* :class:`TestAuditEventSchema` — sealed pydantic model: defaults,
  ``extra="forbid"``, literal-validated ``event_type``, and the
  ``exclude_none=True`` JSON shape.
* :class:`TestAuditLogger` — recorder behaviour: appends to the
  buffer, ``recent(limit=)`` trims correctly, and a structured log
  line is emitted (caplog) that never carries plaintext.
"""
from __future__ import annotations

import json
import logging
import threading

import pytest
from pydantic import ValidationError

from src.audit import AuditEvent, AuditLogger, RingBuffer


# ---------------------------------------------------------------------------
# TestRingBuffer
# ---------------------------------------------------------------------------


class TestRingBuffer:
    """Invariants of the bounded, thread-safe ring buffer."""

    def test_append_and_snapshot_preserve_order(self) -> None:
        # Snapshot is oldest-first (deque iteration order) — the same
        # order callers will use to walk audit history chronologically.
        buf: RingBuffer[int] = RingBuffer(maxlen=10)
        for v in (1, 2, 3):
            buf.append(v)
        assert buf.snapshot() == [1, 2, 3]

    def test_overflow_drops_oldest(self) -> None:
        # maxlen=3, append 5 → only the last 3 survive. This is the
        # whole reason for a bounded buffer (memory safety).
        buf: RingBuffer[int] = RingBuffer(maxlen=3)
        for v in (1, 2, 3, 4, 5):
            buf.append(v)
        assert buf.snapshot() == [3, 4, 5]

    def test_len_reflects_current_size_and_caps_at_maxlen(self) -> None:
        # __len__ tracks live size and caps at maxlen (not the total
        # number of appends).
        buf: RingBuffer[int] = RingBuffer(maxlen=3)
        assert len(buf) == 0
        buf.append(1)
        assert len(buf) == 1
        for v in (2, 3, 4, 5):
            buf.append(v)
        assert len(buf) == 3

    def test_clear_empties_buffer(self) -> None:
        # clear() drops every element so a subsequent snapshot is
        # empty. Used by tests; never by production code.
        buf: RingBuffer[int] = RingBuffer(maxlen=10)
        for v in range(5):
            buf.append(v)
        buf.clear()
        assert buf.snapshot() == []
        assert len(buf) == 0

    def test_snapshot_returns_copy_not_live_view(self) -> None:
        # Mutating the returned list must NOT affect future snapshots —
        # otherwise callers could corrupt the buffer.
        buf: RingBuffer[int] = RingBuffer(maxlen=10)
        buf.append(1)
        snap = buf.snapshot()
        snap.append(99)
        assert buf.snapshot() == [1]

    def test_thread_safety_under_concurrent_appends(self) -> None:
        # 10 threads × 100 appends = 1000 total; maxlen=1000 fits all.
        # Final length must be exactly 1000 — if the lock were missing
        # we'd see torn appends and a length below 1000.
        buf: RingBuffer[int] = RingBuffer(maxlen=1000)

        def worker(start: int) -> None:
            for i in range(100):
                buf.append(start + i)

        threads = [
            threading.Thread(target=worker, args=(t * 100,)) for t in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(buf) == 1000
        # The snapshot should hold all 1000 distinct values (0..999) in
        # some interleaved order. Using a set drops ordering concerns —
        # this is purely a "no items lost or duplicated" check.
        assert set(buf.snapshot()) == set(range(1000))


# ---------------------------------------------------------------------------
# TestAuditEventSchema
# ---------------------------------------------------------------------------


class TestAuditEventSchema:
    """Pydantic model invariants on the sealed AuditEvent schema."""

    def test_optional_fields_default_to_none(self) -> None:
        # All optional metadata defaults to None, actor defaults to
        # "system", outcome defaults to "success". Required: event_type.
        ev = AuditEvent(event_type="encrypt")
        assert ev.outcome == "success"
        assert ev.actor == "system"
        assert ev.request_id is None
        assert ev.record_id is None
        assert ev.key_id is None
        assert ev.field_path is None
        assert ev.field_type is None
        assert ev.failure_reason is None
        assert ev.byte_count is None
        assert ev.duration_us is None
        # Defaults that aren't None — event_id is a uuid4 hex (32 chars)
        # and timestamp_utc is a real datetime.
        assert isinstance(ev.event_id, str)
        assert len(ev.event_id) == 32
        assert ev.timestamp_utc is not None

    def test_extra_fields_are_rejected(self) -> None:
        # extra="forbid" is the load-bearing property that prevents an
        # accidental "plaintext" or "ciphertext" slot from slipping in.
        # Verify it actually fires.
        with pytest.raises(ValidationError):
            AuditEvent(event_type="encrypt", plaintext="alice@example.com")  # type: ignore[call-arg]

    def test_event_type_must_be_in_literal_set(self) -> None:
        # A typo at the call site (e.g. "encypt") must raise rather
        # than silently land in the audit log under a never-queried
        # name.
        with pytest.raises(ValidationError):
            AuditEvent(event_type="encypt")  # type: ignore[arg-type]

    def test_outcome_must_be_in_literal_set(self) -> None:
        # Same closed-literal protection for outcome.
        with pytest.raises(ValidationError):
            AuditEvent(event_type="encrypt", outcome="kinda-ok")  # type: ignore[arg-type]

    def test_each_legal_event_type_constructs(self) -> None:
        # All five legal types must construct without raising — this
        # guards against typos in the Literal declaration itself.
        for t in ("encrypt", "decrypt", "detect", "key_rotate", "key_destroy"):
            ev = AuditEvent(event_type=t)  # type: ignore[arg-type]
            assert ev.event_type == t

    def test_model_dump_json_excludes_none_fields(self) -> None:
        # exclude_none=True keeps the wire JSON compact: only fields
        # that were actually set appear. We never want
        # "failure_reason": null on every success event.
        ev = AuditEvent(
            event_type="encrypt",
            record_id="r1",
            key_id="k1",
            field_path="user.email",
        )
        wire = ev.model_dump_json(exclude_none=True)
        parsed = json.loads(wire)
        # Set keys are present, None ones omitted.
        assert "record_id" in parsed
        assert "key_id" in parsed
        assert "field_path" in parsed
        assert "failure_reason" not in parsed
        assert "byte_count" not in parsed
        assert "duration_us" not in parsed
        # Defaulted-non-None keys (event_id, timestamp_utc, outcome,
        # actor) are still emitted because they aren't None.
        assert "event_id" in parsed
        assert "outcome" in parsed
        assert "actor" in parsed


# ---------------------------------------------------------------------------
# TestAuditLogger
# ---------------------------------------------------------------------------


class TestAuditLogger:
    """End-to-end behaviour of the recorder."""

    def test_record_appends_to_ring_buffer(self) -> None:
        # The recorder writes to whatever buffer it was given so the
        # dashboard can read events synchronously without going via
        # the stdlib logger.
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=10)
        logger = AuditLogger(buf)
        logger.record(
            event_type="encrypt",
            record_id="r-1",
            key_id="k-1",
            field_path="user.email",
            field_type="email",
            byte_count=17,
        )
        snap = buf.snapshot()
        assert len(snap) == 1
        assert snap[0].event_type == "encrypt"
        assert snap[0].record_id == "r-1"
        assert snap[0].field_path == "user.email"

    def test_record_returns_the_created_event(self) -> None:
        # Returning the event lets callers chain on event_id, e.g.
        # echoing it in an HTTP response for client-side correlation.
        logger = AuditLogger(RingBuffer())
        ev = logger.record(event_type="decrypt", record_id="r-2")
        assert isinstance(ev, AuditEvent)
        assert ev.event_type == "decrypt"
        assert ev.record_id == "r-2"

    def test_recent_returns_last_n_events(self) -> None:
        # After 5 appends, recent(limit=2) returns the last 2 (most
        # recent). Order within the slice is oldest-first.
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=10)
        logger = AuditLogger(buf)
        for i in range(5):
            logger.record(event_type="encrypt", record_id=f"r-{i}")
        recent = logger.recent(limit=2)
        assert len(recent) == 2
        assert recent[0].record_id == "r-3"
        assert recent[1].record_id == "r-4"

    def test_recent_without_limit_returns_full_snapshot(self) -> None:
        # No limit → entire buffer.
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=10)
        logger = AuditLogger(buf)
        for i in range(3):
            logger.record(event_type="encrypt", record_id=f"r-{i}")
        assert len(logger.recent()) == 3

    def test_recent_with_zero_or_negative_limit_returns_empty(self) -> None:
        # Defensive: a buggy caller passing limit=0 or -5 gets an
        # empty list, not the full buffer (which is what a naive
        # snap[-0:] would yield).
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=10)
        logger = AuditLogger(buf)
        for i in range(3):
            logger.record(event_type="encrypt")
        assert logger.recent(limit=0) == []
        assert logger.recent(limit=-1) == []

    def test_structured_log_line_emitted_via_caplog(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The recorder ALSO emits a structured INFO line on the "audit"
        # logger so operators can ship audit records via the same log
        # pipeline as application logs.
        logger = AuditLogger(RingBuffer(), logger_name="audit-test")
        with caplog.at_level(logging.INFO, logger="audit-test"):
            logger.record(
                event_type="encrypt",
                record_id="r-log-1",
                key_id="k-log-1",
                field_path="user.email",
                field_type="email",
            )
        # Exactly one record at INFO on our logger.
        records = [r for r in caplog.records if r.name == "audit-test"]
        assert len(records) == 1
        msg = records[0].getMessage()
        # Structured JSON payload follows the "audit " prefix.
        assert msg.startswith("audit ")
        # Critical fields are present in the JSON tail.
        assert "encrypt" in msg
        assert "r-log-1" in msg
        assert "k-log-1" in msg
        assert "user.email" in msg

    def test_caplog_does_not_contain_plaintext(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The AuditEvent schema has no plaintext slot; this is a smoke
        # check that the line we actually emit doesn't accidentally
        # include the value we DID NOT pass. Since extra="forbid"
        # would reject an unknown field at construction, the caplog
        # should only contain the metadata we set.
        logger = AuditLogger(RingBuffer(), logger_name="audit-leak-test")
        with caplog.at_level(logging.INFO, logger="audit-leak-test"):
            logger.record(
                event_type="encrypt",
                record_id="r-leak-1",
                field_path="user.email",
                field_type="email",
            )
        text = caplog.text
        # Sensitive-looking strings that we DID NOT supply must not
        # appear in the log line. These are canaries against future
        # regressions where a caller might widen the schema.
        assert "alice@example.com" not in text
        assert "secret" not in text.lower() or "secret" in "decrypt"  # noqa: E501
        # Confirm what we DID set appears (positive control so the test
        # isn't trivially passing on an empty caplog).
        assert "r-leak-1" in text

    def test_ring_buffer_overflow_in_logger_drops_oldest(self) -> None:
        # The bounded buffer's overflow semantics are exposed through
        # the logger — a tiny buffer + many records yields only the
        # latest N.
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=3)
        logger = AuditLogger(buf)
        for i in range(5):
            logger.record(event_type="encrypt", record_id=f"r-{i}")
        snap = buf.snapshot()
        assert [e.record_id for e in snap] == ["r-2", "r-3", "r-4"]

    def test_default_buffer_is_used_when_none_supplied(self) -> None:
        # Constructing with no buffer must give us a usable default
        # (size 1000). Recording then reading recent() must work.
        logger = AuditLogger()
        logger.record(event_type="key_rotate")
        recent = logger.recent()
        assert len(recent) == 1
        assert recent[0].event_type == "key_rotate"

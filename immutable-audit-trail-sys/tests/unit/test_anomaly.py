"""Unit tests for AlertSink + AnomalyDetector."""
import asyncio
import base64
import os
from datetime import datetime, timezone

import pytest

from src.anomaly.alerts import Alert, AlertSink, reset_sink_for_tests, get_sink, set_sink
from src.anomaly.detector import (
    AnomalyDetector,
    _is_off_hours_hour,
    emit_integrity_break_alert,
)
from src.chain.appender import ChainAppender
from src.crypto.signer import Ed25519Signer
from src.persistence.db import init_db, make_engine, make_session_factory


# --- AlertSink ----------------------------------------------------------

def test_sink_add_and_recent():
    sink = AlertSink()
    a1 = sink.add(type="frequency_spike", severity="warning", message="x")
    a2 = sink.add(type="off_hours_access", severity="info", message="y")
    recent = sink.recent()
    assert len(recent) == 2
    # Newest first
    assert recent[0].id == a2.id
    assert recent[1].id == a1.id


def test_sink_ring_buffer_evicts_oldest():
    sink = AlertSink(capacity=3)
    for i in range(5):
        sink.add(type="off_hours_access", severity="info", message=f"m{i}")
    recent = sink.recent()
    assert len(recent) == 3
    # Newest first: m4, m3, m2 — m0 and m1 evicted.
    assert [a.message for a in recent] == ["m4", "m3", "m2"]


def test_sink_singleton():
    reset_sink_for_tests()
    a = get_sink()
    b = get_sink()
    assert a is b
    reset_sink_for_tests()


# --- Helpers ------------------------------------------------------------

def test_off_hours_classifier():
    assert _is_off_hours_hour("2026-05-22T23:30:00+00:00") is True
    assert _is_off_hours_hour("2026-05-22T03:15:00+00:00") is True
    assert _is_off_hours_hour("2026-05-22T14:00:00+00:00") is False
    assert _is_off_hours_hour("bad") is False


# --- AnomalyDetector ----------------------------------------------------

@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def chain(tmp_path, signer):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    yield factory, appender
    await engine.dispose()


@pytest.mark.asyncio
async def test_frequency_spike_detected(chain):
    factory, appender = chain
    sink = AlertSink()
    # Append 12 from alice (threshold is 10).
    for _ in range(12):
        await appender.append(
            actor="alice", action="read", resource="X", success=True,
            args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
        )
    detector = AnomalyDetector(factory, sink)
    alerts = await detector.evaluate()
    spike_alerts = [a for a in alerts if a.type == "frequency_spike"]
    assert len(spike_alerts) == 1
    assert spike_alerts[0].actor == "alice"
    # Second evaluate shouldn't re-fire for the same actor.
    alerts2 = await detector.evaluate()
    assert [a for a in alerts2 if a.type == "frequency_spike"] == []


@pytest.mark.asyncio
async def test_off_hours_detected(chain):
    factory, appender = chain
    sink = AlertSink()
    # The detector pulls rows where timestamp_utc >= now-60s (string compare).
    # To satisfy both "lexicographically >= window_start" and "hour in
    # off-hours range [22, 06)", we use a far-future ISO timestamp with
    # an off-hours hour. fromisoformat() will parse it and .hour == 3.
    await appender.append(
        actor="alice", action="read", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
        timestamp_utc="2099-01-01T03:30:00+00:00",  # 03:30 UTC -> off-hours
    )
    detector = AnomalyDetector(factory, sink)
    alerts = await detector.evaluate()
    off_alerts = [a for a in alerts if a.type == "off_hours_access"]
    assert len(off_alerts) >= 1


@pytest.mark.asyncio
async def test_unknown_actor_only_with_known_set(chain, tmp_path):
    factory, appender = chain
    sink = AlertSink()
    # Without known_actors_path: unknown-actor check is inactive.
    await appender.append(
        actor="mallory", action="read", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
    )
    detector_no_set = AnomalyDetector(factory, sink)
    alerts = await detector_no_set.evaluate()
    assert not [a for a in alerts if a.type == "unknown_actor"]

    # Now with a known set that doesn't include mallory.
    sink2 = AlertSink()
    known = tmp_path / "known.txt"
    known.write_text("alice\nbob\n")
    detector_with_set = AnomalyDetector(factory, sink2, known_actors_path=known)
    alerts = await detector_with_set.evaluate()
    unknown = [a for a in alerts if a.type == "unknown_actor"]
    assert any(a.actor == "mallory" for a in unknown)


def test_integrity_break_alert_pushes_to_sink():
    reset_sink_for_tests()
    set_sink(AlertSink())
    emit_integrity_break_alert(first_break_seq=42, reason="hash_mismatch")
    recent = get_sink().recent()
    assert len(recent) == 1
    assert recent[0].type == "integrity_break"
    assert "42" in recent[0].message
    assert "hash_mismatch" in recent[0].message
    reset_sink_for_tests()

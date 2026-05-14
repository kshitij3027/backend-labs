"""Unit tests for AuditService append/query/summary/security_events."""
from datetime import datetime, timezone

import pytest

from src.audit.models import AuditEntry, SecurityEvent
from src.audit.service import AuditService


def _entry(**overrides) -> AuditEntry:
    base = dict(
        timestamp=datetime.now(timezone.utc),
        user_id=None,
        username=None,
        method="GET",
        path="/health",
        status=200,
        duration_ms=1.0,
        source_ip="127.0.0.1",
        user_agent="pytest",
    )
    base.update(overrides)
    return AuditEntry(**base)


def test_append_and_len() -> None:
    svc = AuditService()
    assert len(svc) == 0
    svc.append(_entry())
    svc.append(_entry(path="/api/auth/login"))
    assert len(svc) == 2


def test_query_returns_newest_first() -> None:
    svc = AuditService()
    svc.append(_entry(path="/a"))
    svc.append(_entry(path="/b"))
    svc.append(_entry(path="/c"))
    paths = [e.path for e in svc.query(limit=10)]
    assert paths == ["/c", "/b", "/a"]


def test_query_respects_limit() -> None:
    svc = AuditService()
    for i in range(5):
        svc.append(_entry(path=f"/p{i}"))
    assert len(svc.query(limit=2)) == 2


def test_query_filter_by_username() -> None:
    svc = AuditService()
    svc.append(_entry(username="alice"))
    svc.append(_entry(username="bob"))
    svc.append(_entry(username="alice"))
    out = svc.query(limit=10, username="alice")
    assert len(out) == 2
    assert all(e.username == "alice" for e in out)


def test_summary_counts() -> None:
    svc = AuditService()
    svc.append(_entry(username="alice", status=200, decision="allow"))
    svc.append(_entry(username="bob", status=403, decision="deny"))
    svc.append(_entry(username="alice", status=401))
    summary = svc.summary()
    assert summary["total_entries"] == 3
    assert summary["by_status"] == {200: 1, 403: 1, 401: 1}
    assert summary["by_user"] == {"alice": 2, "bob": 1}
    assert summary["allow_decisions"] == 1
    assert summary["deny_decisions"] == 1


def test_security_events_append_and_query() -> None:
    svc = AuditService()
    svc.append_security_event(SecurityEvent(
        timestamp=datetime.now(timezone.utc),
        event_type="auth_failure",
        username=None,
        path="/api/auth/login",
        status=401,
        source_ip="127.0.0.1",
        reason="bad password",
    ))
    events = svc.security_events()
    assert len(events) == 1
    assert events[0].event_type == "auth_failure"
    assert svc.summary()["security_events"] == 1


def test_clear_resets_state() -> None:
    svc = AuditService()
    svc.append(_entry())
    svc.append_security_event(SecurityEvent(
        timestamp=datetime.now(timezone.utc),
        event_type="auth_failure",
        username=None,
        path="/x",
        status=401,
        source_ip=None,
    ))
    svc.clear()
    assert len(svc) == 0
    assert svc.security_events() == []

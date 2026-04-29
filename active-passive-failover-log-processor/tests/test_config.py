"""Tests for src/config.py — env loading, peer parsing, priority hashing."""

from __future__ import annotations

import pytest

from src.config import NodeConfig


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every config env var except NODE_ID before constructing config."""
    for var in (
        "IS_PRIMARY",
        "PORT",
        "REDIS_HOST",
        "REDIS_PORT",
        "HEARTBEAT_INTERVAL",
        "HEARTBEAT_TIMEOUT",
        "ELECTION_TIMEOUT",
        "STATE_SYNC_INTERVAL",
        "LOCK_TTL",
        "PEER_NODES",
    ):
        monkeypatch.delenv(var, raising=False)


def test_defaults_when_only_node_id_set(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    cfg = NodeConfig()
    assert cfg.node_id == "node-1"
    assert cfg.is_primary is False
    assert cfg.port == 8001
    assert cfg.redis_host == "redis"
    assert cfg.redis_port == 6379
    assert cfg.heartbeat_interval == 2.0
    assert cfg.heartbeat_timeout == 6.0
    assert cfg.election_timeout == 10.0
    assert cfg.state_sync_interval == 5.0
    assert cfg.lock_ttl == 6
    assert cfg.peer_nodes == ""


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-2")
    monkeypatch.setenv("IS_PRIMARY", "true")
    monkeypatch.setenv("PORT", "8002")
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "1.5")
    cfg = NodeConfig()
    assert cfg.is_primary is True
    assert cfg.port == 8002
    assert cfg.heartbeat_interval == 1.5


def test_peer_list_parses_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    monkeypatch.setenv("PEER_NODES", "node-2:8002,node-3:8003")
    cfg = NodeConfig()
    assert cfg.peer_list() == [("node-2", 8002), ("node-3", 8003)]


def test_peer_list_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    cfg = NodeConfig()
    assert cfg.peer_list() == []


def test_peer_list_trailing_comma_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    monkeypatch.setenv("PEER_NODES", "node-2:8002,")
    cfg = NodeConfig()
    assert cfg.peer_list() == [("node-2", 8002)]


def test_peer_list_skips_blank_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    monkeypatch.setenv("PEER_NODES", "node-2:8002,,node-3:8003,")
    cfg = NodeConfig()
    assert cfg.peer_list() == [("node-2", 8002), ("node-3", 8003)]


def test_priority_in_range(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-1")
    cfg = NodeConfig()
    p = cfg.priority()
    assert 0 <= p <= 999


def test_priority_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same node_id must produce the same priority on every call."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("NODE_ID", "node-7")
    cfg1 = NodeConfig()
    cfg2 = NodeConfig()
    assert cfg1.priority() == cfg2.priority()


def test_priority_distribution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Distinct node_ids should mostly give distinct priorities."""
    _clean_env(monkeypatch)
    sample_ids = ["node-a", "node-b", "node-c", "node-d", "node-e"]
    priorities: set[int] = set()
    for nid in sample_ids:
        monkeypatch.setenv("NODE_ID", nid)
        priorities.add(NodeConfig().priority())
    # We require at least 2 distinct priorities out of 5 IDs (the spec's bar).
    assert len(priorities) >= 2

from __future__ import annotations

import pytest

from src.shared.config import CoordinatorSettings, PartitionSettings


def test_partition_settings_reads_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PARTITION_ID", "partition-x")
    monkeypatch.setenv("PARTITION_PORT", "9999")
    settings = PartitionSettings()
    assert settings.partition_id == "partition-x"
    assert settings.partition_port == 9999


def test_indexed_fields_list_splits_and_strips():
    settings = PartitionSettings(indexed_fields="  level ,service,  timestamp  ")
    assert settings.indexed_fields_list() == ["level", "service", "timestamp"]


def test_coordinator_partition_urls_dict_parses_default():
    settings = CoordinatorSettings()
    urls = settings.partition_urls_dict()
    assert urls == {
        "partition-1": "http://partition-1:8101",
        "partition-2": "http://partition-2:8102",
        "partition-3": "http://partition-3:8103",
    }
    assert len(urls) == 3


def test_coordinator_partition_urls_dict_skips_invalid_entries():
    settings = CoordinatorSettings(partition_urls="foo,bar=http://bar:1,baz")
    urls = settings.partition_urls_dict()
    assert urls == {"bar": "http://bar:1"}

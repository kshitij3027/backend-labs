"""Shared pytest fixtures for the Real-Time Log Pattern Clustering test suite.

These fixtures keep the unit and integration tests hermetic: ``config`` hands back a
fresh, default :class:`~src.config.AppConfig`, and ``write_yaml`` writes an arbitrary
(nested) mapping to a temp YAML file and returns its path so config-loading tests
never touch the real ``config/config.yaml``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from src.config import AppConfig


@pytest.fixture
def config() -> AppConfig:
    """Return a fresh :class:`AppConfig` populated entirely from model defaults.

    Independent per test (no shared mutable state), so tests can read defaults or
    construct comparisons without affecting one another.
    """
    return AppConfig()


@pytest.fixture
def write_yaml(tmp_path: Path) -> Callable[[dict[str, Any]], str]:
    """Return a helper that dumps a (nested) dict to a temp YAML file and returns its path.

    Usage::

        def test_yaml_override(write_yaml):
            path = write_yaml({"api": {"port": 9000}})
            cfg = load_config(path)
            assert cfg.api.port == 9000

    Each call writes a uniquely-named file under pytest's ``tmp_path`` so multiple
    YAML files can coexist within a single test.
    """
    counter = {"n": 0}

    def _write(data: dict[str, Any]) -> str:
        counter["n"] += 1
        target = tmp_path / f"config_{counter['n']}.yaml"
        target.write_text(yaml.safe_dump(data), encoding="utf-8")
        return str(target)

    return _write

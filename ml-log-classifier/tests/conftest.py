"""Shared pytest fixtures for the ML Log Classifier test suite.

These fixtures keep the unit and integration tests hermetic: ``config`` hands back a
fresh, default :class:`~src.config.Settings`, and ``write_yaml`` writes an arbitrary
mapping to a temp YAML file and returns its path so config-loading tests never touch
the real ``config/config.yaml``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from src.config import Settings


@pytest.fixture
def config() -> Settings:
    """Return a fresh :class:`Settings` populated entirely from dataclass defaults.

    Independent per test (no shared mutable state), so tests can read defaults or
    construct comparisons without affecting one another.
    """
    return Settings()


@pytest.fixture
def write_yaml(tmp_path: Path) -> Callable[[dict[str, Any]], str]:
    """Return a helper that dumps a dict to a temp YAML file and returns its path.

    Usage::

        def test_yaml_override(write_yaml):
            path = write_yaml({"port": 9000})
            settings = load_config(path)
            assert settings.port == 9000

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

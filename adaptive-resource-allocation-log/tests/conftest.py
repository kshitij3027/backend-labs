"""Shared pytest fixtures for the Adaptive Resource Allocation test suite.

``src`` is imported absolutely (``from src.config import ...``). Inside Docker the
test image sets ``PYTHONPATH=/app`` so the package resolves without any sys.path
manipulation; on a host run with ``pytest`` from the project root the cwd is on the
path as well.
"""

from pathlib import Path

import pytest

from src.config import Settings


@pytest.fixture
def config() -> Settings:
    """Return a fresh :class:`Settings` with all default values."""
    return Settings()


@pytest.fixture
def write_yaml(tmp_path: Path):
    """Return a helper that writes YAML text to a temp file and returns its path.

    Usage::

        path = write_yaml("scaling:\\n  min_workers: 5\\n")
        cfg = load_config(str(path))
    """

    def _write(contents: str, name: str = "config.yaml") -> str:
        target = tmp_path / name
        target.write_text(contents, encoding="utf-8")
        return str(target)

    return _write

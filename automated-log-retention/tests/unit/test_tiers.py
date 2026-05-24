"""Unit tests for ``src/storage/tiers.py``.

These pin down three things:

  1. The ``TIERS`` constant is the immutable contract the rest of the
     code depends on — order and membership both matter.
  2. ``tier_dir`` and ``compose_segment_path`` are pure (no FS side
     effects) and reject unknown tier names loudly.
  3. ``ensure_tier_dirs`` is idempotent so it's safe to call on every
     lifespan startup.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.storage.tiers import (
    TIERS,
    compose_segment_path,
    ensure_tier_dirs,
    tier_dir,
)


def test_TIERS_constant():
    """The 5 tier names are fixed and ordered hot -> ... -> pending."""
    assert TIERS == ("hot", "warm", "cold", "archive", "pending")


def test_tier_dir_valid(tmp_path: Path):
    """``tier_dir`` returns ``<root>/<tier>`` for valid tier names."""
    assert tier_dir(tmp_path, "hot") == tmp_path / "hot"
    assert tier_dir(tmp_path, "warm") == tmp_path / "warm"
    assert tier_dir(tmp_path, "cold") == tmp_path / "cold"
    assert tier_dir(tmp_path, "archive") == tmp_path / "archive"
    assert tier_dir(tmp_path, "pending") == tmp_path / "pending"


def test_tier_dir_invalid_raises(tmp_path: Path):
    """Unknown tier names crash with ``ValueError`` instead of silently
    creating a typo'd directory."""
    with pytest.raises(ValueError):
        tier_dir(tmp_path, "lukewarm")


def test_ensure_tier_dirs_creates_all(tmp_path: Path):
    """After one call, all 5 subdirectories must exist as directories."""
    ensure_tier_dirs(tmp_path)
    for tier in TIERS:
        sub = tmp_path / tier
        assert sub.exists(), f"{tier} dir was not created"
        assert sub.is_dir(), f"{tier} exists but is not a directory"


def test_ensure_tier_dirs_idempotent(tmp_path: Path):
    """Two calls in a row must not raise — startup may run multiple times."""
    ensure_tier_dirs(tmp_path)
    ensure_tier_dirs(tmp_path)
    # All 5 dirs still present after the second call.
    for tier in TIERS:
        assert (tmp_path / tier).is_dir()


def test_compose_segment_path_no_side_effects(tmp_path: Path):
    """``compose_segment_path`` is pure: returns the right path but does
    not create the file or its parent dir."""
    seg = "segment-1700000000.jsonl"
    result = compose_segment_path(tmp_path, "hot", seg)
    assert result == tmp_path / "hot" / seg
    # The file must not exist.
    assert not result.exists()
    # The parent dir must not exist either (we did not call
    # ``ensure_tier_dirs``).
    assert not result.parent.exists()


def test_compose_segment_path_invalid_tier(tmp_path: Path):
    """Bad tier propagates the ``ValueError`` from ``tier_dir``."""
    with pytest.raises(ValueError):
        compose_segment_path(tmp_path, "scorching", "x.jsonl")


def test_tier_dir_accepts_str_root(tmp_path: Path):
    """``root`` may be a ``str`` (not just a ``Path``)."""
    result = tier_dir(str(tmp_path), "cold")
    assert result == tmp_path / "cold"

"""Pure path helpers for the 5 storage tiers.

The on-disk layout for the retention engine is fixed:

    <STORAGE_ROOT>/
        hot/         segment-<unix_ts>.jsonl
        warm/        segment-<unix_ts>.jsonl
        cold/        segment-<unix_ts>.jsonl.zst
        archive/     segment-<unix_ts>.jsonl.zst
        pending/     segment-<unix_ts>.jsonl.<ext>

This module owns *only* the directory math — no DB calls, no I/O beyond
``mkdir``, no async. The lifecycle pipeline (C09/C10/C11) layers the
catalog, compressor, and mover on top; this file just answers "where
should segment X live for tier Y?" deterministically.

Keeping path composition free of side effects lets the applier compute
target paths during planning without provisionally creating anything,
and lets tests assert exact filesystem layouts without faking I/O.
"""
from __future__ import annotations

from pathlib import Path

# Tuple (not list) because the membership is static and we want it
# hashable / immutable. Order matches the lifecycle progression:
# hot -> warm -> cold -> archive, with ``pending`` as the
# mark-then-sweep staging tier the sweeper drains.
TIERS: tuple[str, ...] = ("hot", "warm", "cold", "archive", "pending")


def tier_dir(root: Path | str, tier: str) -> Path:
    """Return the absolute directory for ``tier`` under ``root``.

    Pure path composition — does not touch the filesystem. Raises
    ``ValueError`` for any tier name not in ``TIERS`` so callers get a
    deterministic crash instead of a typo silently writing to a sibling
    directory.
    """
    if tier not in TIERS:
        raise ValueError(
            f"unknown tier {tier!r}: must be one of {TIERS}"
        )
    return Path(root) / tier


def ensure_tier_dirs(root: Path | str) -> None:
    """Create all 5 tier subdirectories under ``root`` if missing.

    Idempotent: ``parents=True, exist_ok=True`` makes repeat calls safe
    on every app boot. Called once during lifespan startup (C12) so the
    applier can ``os.rename`` into the target dirs without first
    checking they exist.
    """
    root_path = Path(root)
    for tier in TIERS:
        (root_path / tier).mkdir(parents=True, exist_ok=True)


def compose_segment_path(root: Path | str, tier: str, segment_name: str) -> Path:
    """Return ``<root>/<tier>/<segment_name>`` without touching the FS.

    Used by the applier when computing the target path for a planned
    move: the file does not exist at the returned path yet — the mover
    will ``stage_and_promote`` to it. Validation of ``tier`` is delegated
    to ``tier_dir``.
    """
    return tier_dir(root, tier) / segment_name

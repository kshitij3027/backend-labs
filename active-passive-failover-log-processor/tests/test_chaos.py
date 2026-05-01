"""Chaos integration tests. Skipped by default — set ``RUN_CHAOS_TESTS=1`` to enable.

These tests assume the cluster is already running via ``docker compose up``.
They invoke ``scripts/chaos.py`` as a subprocess and assert the exit code,
mirroring how a CI job would gate on chaos behaviour.

Why opt-in
----------
Chaos tests need a real Docker-managed cluster, which is not available
inside the unit-test container. We therefore guard the entire module
behind an environment switch so the everyday ``make test`` run stays
fast and isolated.

Run these tests with::

    RUN_CHAOS_TESTS=1 pytest tests/test_chaos.py

after ``make run`` has started the cluster on the host.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

CHAOS_SCRIPT = (
    Path(__file__).resolve().parent.parent / "scripts" / "chaos.py"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_CHAOS_TESTS") != "1",
    reason="set RUN_CHAOS_TESTS=1 to enable chaos integration tests",
)


def _run_chaos(
    scenario: str, duration: float, *extra: str
) -> subprocess.CompletedProcess:
    """Invoke ``scripts/chaos.py`` and return the completed process."""
    cmd = [
        sys.executable,
        str(CHAOS_SCRIPT),
        "--scenario",
        scenario,
        "--duration",
        str(duration),
        *extra,
    ]
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=duration + 60
    )


def test_chaos_partition_recovers() -> None:
    """Partition the primary, reconnect, expect it to rejoin as STANDBY."""
    res = _run_chaos("partition", 30.0)
    assert res.returncode == 0, (
        f"partition chaos failed:\n"
        f"STDOUT:\n{res.stdout}\n"
        f"STDERR:\n{res.stderr}"
    )

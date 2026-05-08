"""Smoke test for the CLI demo entry point."""
import asyncio
import io
import sys
import contextlib

import pytest

from src.demo import run_demo, parse_args


@pytest.mark.asyncio
async def test_run_demo_completes_quickly():
    args = parse_args(["--logs", "3", "--simulate-duration", "1", "--seed", "42", "--no-wait-recovery"])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = await run_demo(args)
    output = buf.getvalue()
    assert rc == 0
    assert "Demo completed" in output
    assert "Circuit Breaker Statistics" in output


@pytest.mark.asyncio
async def test_run_demo_emits_breaker_table():
    args = parse_args(["--logs", "2", "--simulate-duration", "1", "--seed", "1", "--no-wait-recovery"])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = await run_demo(args)
    output = buf.getvalue()
    assert rc == 0
    # Demo must emit at least the four named breakers in the stats table
    for name in ("database_primary", "database_backup", "queue_main", "external_api"):
        assert name in output


def test_parse_args_defaults():
    args = parse_args([])
    assert args.logs == 20
    assert args.simulate_duration == 3
    assert args.wait_recovery is True


def test_parse_args_no_wait_recovery():
    args = parse_args(["--no-wait-recovery"])
    assert args.wait_recovery is False

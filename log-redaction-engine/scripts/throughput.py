#!/usr/bin/env python3
"""Batch throughput probe for the Intelligent Log Redaction Engine.

Hammers ``POST /api/redact`` with a synthetic 1 000-entry batch, times
the round-trip wall-clock, and asserts the service can sustain at least
**1 000 logs/second** (the C11 baseline target from `plan.md`). A
"stretch" target of **10 000 logs/sec** is checked separately: missing
it produces only a warning so a slower-but-still-passing run does not
fail CI.

The script intentionally:

- imports nothing from ``src.*`` — it has to run inside the bare
  ``tester`` container which only carries the runtime deps;
- depends only on ``httpx`` (already in ``requirements.txt``) and the
  standard library (``json``, ``os``, ``sys``, ``time``);
- reads the target base URL from the ``BASE_URL`` env var (default
  ``http://app:8000``) so the same script works whether you invoke it
  from inside the compose network or override for local debugging.

Warm-up
-------
A 10-entry batch is POSTed once before the timed run. That pays the
spaCy NER warm-up cost — the model lazy-loads on first detection — so
the 1 000-entry timed run measures steady-state throughput, not
cold-start. The warm-up result is discarded.

Exit codes
----------
- ``0`` — batch redact succeeded AND logs/sec >= 1 000.
- ``1`` — batch failed structurally OR logs/sec < 1 000.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import httpx


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: Number of logs sent in the timed batch request. 1 000 matches the
#: C11 acceptance test in plan.md §C11.
BATCH_SIZE: int = 1000

#: Logs sent in the warm-up batch. Small enough to be cheap, large
#: enough to load the spaCy model + JIT-compile the regex patterns.
WARMUP_SIZE: int = 10

#: Baseline throughput target. Hard failure if the timed run is below
#: this (exit code 1).
BASELINE_TARGET_LOGS_PER_SEC: float = 1000.0

#: Stretch throughput target. Soft warning if the timed run is below
#: this — does NOT fail. Documented in plan.md §8 (risk register).
STRETCH_TARGET_LOGS_PER_SEC: float = 10000.0

#: HTTP timeout (seconds). Comfortably above the failure threshold so a
#: slow-but-still-passing run does not false-positive on timeout.
HTTP_TIMEOUT_SEC: float = 120.0


# ---------------------------------------------------------------------------
# Batch construction
# ---------------------------------------------------------------------------


def make_batch(n: int) -> list[dict[str, Any]]:
    """Build ``n`` synthetic log entries with short, redactable messages.

    Every message is kept STRICTLY below ``NER_MIN_LENGTH`` (40 chars)
    so the detection layer's length gate short-circuits spaCy entirely
    and only regex runs. NER is ~5-10 ms per entry; bypassing it is the
    primary throughput mitigation identified in plan.md §8 risk
    register entry R7. The probe deliberately measures the regex-only
    hot path because that's what production log shippers actually hit
    (most log lines are short).

    Each entry still carries a redactable hit so the redaction strategy
    pipeline runs for real — a batch of no-op entries would over-state
    throughput. We rotate among three short templates so detection has
    variety across patterns (SSN / email / credit card) instead of
    every entry triggering only one regex.

    Length budget (longest variant at n<=999): ``u999 ssn 123-45-6789``
    = 20 chars — all three templates stay well under the 40-char gate
    even as ``i`` grows into 4+ digits.
    """
    entries: list[dict[str, Any]] = []
    for i in range(n):
        # Three short templates, one per branch of i % 3:
        #   * SSN (regex hit, length-preserving mask)
        #   * email (regex hit, often masked by default preset)
        #   * Luhn-valid card (regex + Luhn check, tokenize/mask)
        # Each template's longest form is under 30 chars, leaving plenty
        # of headroom under the 40-char NER gate.
        if i % 3 == 0:
            msg = f"u{i} ssn 123-45-6789"
        elif i % 3 == 1:
            msg = f"u{i} a@x.io paid"
        else:
            msg = f"card 4111-1111-1111-1111"
        entries.append(
            {
                "message": msg,
                "timestamp": "2026-05-21T10:00:00Z",
                "level": "INFO",
            }
        )
    return entries


# ---------------------------------------------------------------------------
# Main probe
# ---------------------------------------------------------------------------


def main() -> int:
    """Execute the throughput probe and return an exit code.

    Returns
    -------
    int
        ``0`` on success (>= baseline target); ``1`` if the request
        failed or threw, or if the measured rate is below the baseline.
    """
    base_url = os.environ.get("BASE_URL", "http://app:8000")

    # ---- Warm-up: pay the spaCy load + regex JIT cost ----------------
    # Result is discarded — we only care that the call returned 200
    # so the timed run below isn't measuring cold-start.
    warmup_batch = make_batch(WARMUP_SIZE)
    warmup_payload = {"log_entries": warmup_batch}
    try:
        warmup_resp = httpx.post(
            f"{base_url}/api/redact",
            json=warmup_payload,
            timeout=HTTP_TIMEOUT_SEC,
        )
        warmup_resp.raise_for_status()
    except httpx.HTTPError as exc:
        # Surface the warm-up failure with the URL so the operator can
        # see immediately whether the service is even reachable.
        print(
            f"ERROR: warm-up POST to {base_url}/api/redact failed: {exc}",
            file=sys.stderr,
        )
        return 1

    # ---- Timed 1000-entry batch -------------------------------------
    batch = make_batch(BATCH_SIZE)
    payload = {"log_entries": batch}

    t0 = time.monotonic()
    try:
        resp = httpx.post(
            f"{base_url}/api/redact",
            json=payload,
            timeout=HTTP_TIMEOUT_SEC,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(
            f"ERROR: timed POST to {base_url}/api/redact failed: {exc}",
            file=sys.stderr,
        )
        return 1
    elapsed = time.monotonic() - t0

    # Spot-check the response shape so a route that returned 200 but
    # silently dropped entries shows up as a failure here.
    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        print(f"ERROR: response was not valid JSON: {exc}", file=sys.stderr)
        return 1
    processed = body.get("processed_entries")
    if not isinstance(processed, list) or len(processed) != BATCH_SIZE:
        n_got = len(processed) if isinstance(processed, list) else "missing"
        print(
            f"ERROR: expected {BATCH_SIZE} processed_entries, got {n_got}",
            file=sys.stderr,
        )
        return 1

    # ---- Compute + report -------------------------------------------
    # Guard against the (unlikely) zero-duration corner case so we
    # never divide by zero on a sub-microsecond run.
    logs_per_sec = len(batch) / elapsed if elapsed > 0 else float("inf")

    print("=== Throughput probe ===")
    print(f"batch_size: {BATCH_SIZE}")
    print(f"elapsed_s: {elapsed:.4f}")
    print(f"logs_per_sec: {logs_per_sec:.1f}")
    print(f"baseline_target: {BASELINE_TARGET_LOGS_PER_SEC:.0f}")
    print(f"stretch_target: {STRETCH_TARGET_LOGS_PER_SEC:.0f}")

    # Hard failure if below baseline.
    if logs_per_sec < BASELINE_TARGET_LOGS_PER_SEC:
        print(
            f"FAIL: {logs_per_sec:.1f} logs/sec < baseline "
            f"{BASELINE_TARGET_LOGS_PER_SEC:.0f} logs/sec",
            file=sys.stderr,
        )
        return 1

    # Soft warning if below stretch — print but return success.
    if logs_per_sec < STRETCH_TARGET_LOGS_PER_SEC:
        print(
            f"WARN: {logs_per_sec:.1f} logs/sec is below stretch target "
            f"{STRETCH_TARGET_LOGS_PER_SEC:.0f} logs/sec "
            f"(non-fatal — see plan.md §8 risk register)",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Batch throughput probe for the Field-Level Log Encryption Service.

Hammers ``POST /v1/logs/encrypt/batch`` with 100 distinct e-commerce log
copies, times the round-trip wall-clock, and asserts the service can
sustain at least **50 logs/second** (i.e. 100 logs in under 2.0 s).

The script intentionally:

- imports nothing from ``src.*`` — it has to run inside the bare
  ``tester`` container which only carries the runtime deps;
- depends only on ``httpx`` (already in ``requirements.txt``) and the
  standard library (``json``, ``time``, ``pathlib``, ``sys``);
- accepts the target base URL as the first positional argument so the
  same script works whether you invoke it from the host (default
  ``http://localhost:8000``) or from inside the compose network
  (``python scripts/throughput.py http://app:8000``).

Exit codes
----------
- ``0`` — batch encrypt succeeded AND throughput ≥ 50 logs/sec.
- ``1`` — batch failed structurally OR throughput < 50 logs/sec.
- ``2`` — unrecoverable setup error (fixture missing, network down).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: Number of logs sent in the single batch request. 100 matches the
#: project_requirements / plan section on batch throughput.
BATCH_SIZE: int = 100

#: Throughput floor in logs/second. Below this we fail; the project
#: requirement is ≥50 logs/sec, equivalent to <2.0s for a 100-log batch.
THROUGHPUT_FLOOR_LOGS_PER_SEC: float = 50.0

#: HTTP timeout in seconds. Comfortably above the failure threshold so
#: we don't false-positive on a slow-but-still-passing run.
HTTP_TIMEOUT_SEC: float = 30.0


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------


def _project_root() -> Path:
    """Resolve the project root from this script's location.

    The script lives at ``<root>/scripts/throughput.py``; ``.parent.parent``
    is the project root regardless of where Python is invoked from.
    """
    return Path(__file__).resolve().parent.parent


def _load_ecommerce_fixture() -> dict[str, Any]:
    """Load ``tests/fixtures/ecommerce_log.json`` once.

    Raises
    ------
    SystemExit
        If the fixture is missing or unreadable. Exits with code 2
        (setup error) rather than 1 (assertion failure).
    """
    path = _project_root() / "tests" / "fixtures" / "ecommerce_log.json"
    if not path.exists():
        print(
            f"ERROR: fixture not found at {path} — "
            f"ensure tests/fixtures is mounted into the container",
            file=sys.stderr,
        )
        sys.exit(2)
    return json.loads(path.read_text())


def _build_batch(base_log: dict[str, Any], n: int) -> list[dict[str, Any]]:
    """Build ``n`` slight variations of ``base_log``.

    Each copy gets a unique ``order_id`` suffix (``-0000``, ``-0001``,
    ...) so the AAD-binding cache cannot get false hits across copies —
    every log goes through a real encryption path.

    Parameters
    ----------
    base_log : dict
        The fixture log to clone.
    n : int
        Number of variants to produce.

    Returns
    -------
    list[dict]
        A new list of fresh dicts; ``base_log`` is untouched.
    """
    base_order = str(base_log.get("order_id", "ORD-PERF"))
    out: list[dict[str, Any]] = []
    for i in range(n):
        # Shallow copy is enough: the script only mutates the top-level
        # `order_id`, never nested structures.
        copy = dict(base_log)
        copy["order_id"] = f"{base_order}-{i:04d}"
        out.append(copy)
    return out


# ---------------------------------------------------------------------------
# Main probe
# ---------------------------------------------------------------------------


def run(base_url: str) -> int:
    """Execute the batch throughput probe.

    Parameters
    ----------
    base_url : str
        Base URL of the running service (e.g. ``http://localhost:8000``
        or ``http://app:8000`` inside the compose network).

    Returns
    -------
    int
        Exit code suitable for ``sys.exit``. See module docstring.
    """
    base_log = _load_ecommerce_fixture()
    batch = _build_batch(base_log, BATCH_SIZE)

    payload = {"logs": batch}

    # Single `httpx.Client` reused for the one request — no point in
    # spinning up an `AsyncClient` for a single call.
    with httpx.Client(base_url=base_url, timeout=HTTP_TIMEOUT_SEC) as client:
        start = time.perf_counter()
        try:
            resp = client.post(
                "/v1/logs/encrypt/batch",
                json=payload,
            )
        except httpx.HTTPError as exc:
            print(
                f"ERROR: HTTP transport failed against {base_url}: {exc}",
                file=sys.stderr,
            )
            return 2
        elapsed = time.perf_counter() - start

    # Surface non-200s with the body so the operator sees the server's
    # complaint (validation error, missing key, etc.).
    if resp.status_code != 200:
        print(
            f"ERROR: batch encrypt returned {resp.status_code}: {resp.text}",
            file=sys.stderr,
        )
        return 1

    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        print(f"ERROR: response was not valid JSON: {exc}", file=sys.stderr)
        return 1

    encrypted_logs = body.get("encrypted_logs")
    if not isinstance(encrypted_logs, list):
        print(
            f"ERROR: response missing 'encrypted_logs' list, got keys: "
            f"{sorted(body.keys())}",
            file=sys.stderr,
        )
        return 1

    if len(encrypted_logs) != BATCH_SIZE:
        print(
            f"ERROR: expected {BATCH_SIZE} encrypted logs, got "
            f"{len(encrypted_logs)}",
            file=sys.stderr,
        )
        return 1

    # Spot-check every entry has the `_processing` envelope. Cheap loop;
    # catches the failure mode where the route returned 200 but a per-entry
    # path silently dropped the envelope.
    for idx, entry in enumerate(encrypted_logs):
        if not isinstance(entry, dict):
            print(
                f"ERROR: encrypted_logs[{idx}] is not a dict: {type(entry)!r}",
                file=sys.stderr,
            )
            return 1
        if "_processing" not in entry:
            print(
                f"ERROR: encrypted_logs[{idx}] missing _processing envelope; "
                f"keys: {sorted(entry.keys())}",
                file=sys.stderr,
            )
            return 1
        # And one customer_email per entry should be an EncryptedField
        # dict (signal that PII was actually encrypted, not passed
        # through). A regression that silently no-ops the detector would
        # surface here.
        ce = entry.get("customer_email")
        if not isinstance(ce, dict) or "encrypted_value" not in ce:
            print(
                f"ERROR: encrypted_logs[{idx}].customer_email missing "
                f"encrypted_value; got: {ce!r}",
                file=sys.stderr,
            )
            return 1

    # Compute and print throughput. We use perf_counter so this works
    # consistently across host + container.
    logs_per_sec = BATCH_SIZE / elapsed if elapsed > 0 else float("inf")
    print(
        f"OK: {BATCH_SIZE} logs encrypted in {elapsed:.2f}s "
        f"= {logs_per_sec:.1f} logs/sec "
        f"(threshold >= {THROUGHPUT_FLOOR_LOGS_PER_SEC:.0f})"
    )

    if logs_per_sec < THROUGHPUT_FLOOR_LOGS_PER_SEC:
        print(
            f"ERROR: throughput {logs_per_sec:.1f} logs/sec < "
            f"required {THROUGHPUT_FLOOR_LOGS_PER_SEC:.0f} logs/sec",
            file=sys.stderr,
        )
        return 1

    return 0


def main(argv: list[str]) -> int:
    """CLI entrypoint.

    Accepts an optional positional ``base_url`` argument; defaults to
    ``http://localhost:8000``. The Makefile target invokes this inside
    the ``tester`` compose service and passes ``http://app:8000`` so the
    request travels via the compose network rather than the host
    loopback.
    """
    if len(argv) > 2:
        print(
            f"usage: {argv[0]} [base_url]  (default: http://localhost:8000)",
            file=sys.stderr,
        )
        return 2
    base_url = argv[1] if len(argv) == 2 else "http://localhost:8000"
    return run(base_url)


if __name__ == "__main__":
    sys.exit(main(sys.argv))

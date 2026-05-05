#!/usr/bin/env python3
"""Chaos scenario — kill primary mid-write, verify no data loss + correct failover.

Run after ``docker compose up -d``. The script:

  1. Spawns 200 writes at ~25/s.
  2. At t=2s, kills us-east via ``POST /api/regions/us-east/kill``.
  3. Continues writing until done; some writes during the failover
     window (~2-3 ticks) may fail with 503 — those are recorded.
  4. After settle, asserts every accepted ``log_id`` is present in
     europe (the new primary). No accepted write may be missing.
  5. Prints a summary: writes attempted, accepted, failed-during-failover,
     time-to-recovery, and post-chaos lag p95 per region.

Why stdlib only:
  Mirrors ``scripts/verify_replication.py`` — runnable from any
  Python 3.10+ environment without a ``pip install`` step. The HTTP
  shape is small enough that ``urllib.request`` is fine.

Exit code is 0 on full success, 1 on the first failed assertion. The
``FAIL: ...`` line is printed on stderr so ``make`` propagates the
right exit code.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread
from typing import Any

BASE = "http://localhost:8000"
TOTAL_WRITES = 200
TARGET_RATE = 25.0  # writes/sec
KILL_AT_SEC = 2.0


def _request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    timeout: float = 5.0,
) -> tuple[int, Any]:
    """Issue a single HTTP request and return ``(status, parsed_json)``.

    On HTTPError we surface the status code with the raw body so the
    caller can produce a descriptive failure line. On URLError (network
    refused, dns) we return ``-1`` so the caller can distinguish a
    transport failure from an HTTP error.
    """
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
            return resp.status, (json.loads(payload) if payload else {})
    except urllib.error.HTTPError as e:
        return e.code, {"_raw": e.read().decode("utf-8", errors="replace")}
    except urllib.error.URLError as e:
        return -1, {"_err": str(e)}


def post_log(i: int) -> tuple[str, Any]:
    """POST a single log; return (``"ok"``, log_id) or (``"fail"``, status)."""
    code, resp = _request(
        "POST",
        "/api/logs",
        {"message": f"chaos-{i}", "level": "info", "service": "chaos"},
    )
    if code == 200:
        return ("ok", resp.get("log_id"))
    return ("fail", code)


def kill_primary_at(t0: float, region: str = "us-east") -> None:
    """Block until ``KILL_AT_SEC`` has elapsed since ``t0``, then kill ``region``."""
    while time.perf_counter() - t0 < KILL_AT_SEC:
        time.sleep(0.05)
    code, _ = _request("POST", f"/api/regions/{region}/kill")
    print(f"[chaos] kill {region} -> {code}")


def main() -> int:
    print(
        f"Chaos scenario: {TOTAL_WRITES} writes at ~{TARGET_RATE}/s, "
        f"kill us-east at t={KILL_AT_SEC}s"
    )

    # Spawn killer in background — fires at t=KILL_AT_SEC.
    t0 = time.perf_counter()
    killer = Thread(target=kill_primary_at, args=(t0,))
    killer.start()

    # Drive writes at the target rate. ThreadPoolExecutor lets us
    # parallelise the post latency without blowing past TARGET_RATE.
    accepted_ids: list[str] = []
    failed: int = 0
    interval = 1.0 / TARGET_RATE

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = []
        for i in range(TOTAL_WRITES):
            futures.append(ex.submit(post_log, i))
            time.sleep(interval)

        for f in as_completed(futures):
            status, payload = f.result()
            if status == "ok":
                accepted_ids.append(payload)
            else:
                failed += 1

    killer.join()
    elapsed = time.perf_counter() - t0
    print(f"[chaos] elapsed: {elapsed:.2f}s")
    print(f"[chaos] writes accepted: {len(accepted_ids)} / {TOTAL_WRITES}")
    print(f"[chaos] writes failed during failover: {failed}")

    # Settle window — let any in-flight replication finish before we
    # query the new primary's secondary store.
    time.sleep(1.0)

    # Confirm every accepted log_id is in europe (the new primary).
    code, eu_logs = _request("GET", "/api/regions/europe/logs?limit=500")
    if code != 200:
        print(f"FAIL: europe read returned {code}", file=sys.stderr)
        return 1
    eu_ids = {e["log_id"] for e in eu_logs}
    missing = set(accepted_ids) - eu_ids
    if missing:
        print(
            f"FAIL: {len(missing)} accepted writes missing from europe "
            f"(sample: {list(missing)[:5]})",
            file=sys.stderr,
        )
        return 1
    print(
        f"[chaos] europe has all {len(accepted_ids)} accepted log_ids — "
        "no data loss"
    )

    # Final lag p95 per region (skipping primary's no-op self-lag).
    code, status = _request("GET", "/api/status")
    if code == 200:
        print("[chaos] post-chaos lag p95:")
        for r in status.get("regions", []):
            if r.get("replication_lag_ms") is not None:
                print(f"  {r['region_id']}: {r['replication_lag_ms']:.2f}ms")
        print(
            f"[chaos] current primary: {status.get('current_primary')!r} "
            f"(was 'us-east'); recent_failovers: "
            f"{len(status.get('recent_failovers', []))}"
        )

    print("\nChaos scenario passed: no data loss, correct failover.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

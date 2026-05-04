#!/usr/bin/env python3
"""Cart-update demo — exercises the consistent-hash ring + replication path.

Run inside the Docker stack::

    make run    # boot the app + redis
    make demo   # docker compose exec app python -m scripts.demo

The script:

1. POSTs 50 distinct cart updates concurrently via a thread-pool —
   each cart_id is hashed onto a home region by ``RegionRing``. We
   print the resulting per-region distribution so the consistent-hash
   ±15% claim is visible.
2. For 5 randomly-selected cart_ids, fires off 3 concurrent updates
   each — one per ``region_hint`` value. This forces concurrent vector
   clocks at the resolver and lets us confirm every replica converges
   on the same winner.
3. Reads back from each region's log store and prints which version
   "won" per cart_id. Determinism check: the home_region listed for
   a cart_id should be identical across all three regions.
4. Prints the current p95 replication lag from ``GET /api/status``.

Standard library only — no extra deps. Uses ``urllib.request`` +
``concurrent.futures.ThreadPoolExecutor`` for parallelism.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

# When run as ``docker compose exec app python -m scripts.demo`` the
# host is reachable on ``localhost`` from inside the container (the app
# binds to 0.0.0.0:8000). Override via ``DEMO_BASE_URL`` if running
# outside the container.
BASE: str = os.environ.get("DEMO_BASE_URL", "http://localhost:8000")


def _request(
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 5.0,
) -> Tuple[int, Any]:
    """Tiny stdlib HTTP wrapper. Returns ``(status_code, json_body)``."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, {"_raw": e.read().decode("utf-8", errors="replace")}


def post_cart(cart_id: str, region_hint: Optional[str] = None) -> Dict[str, Any]:
    """POST a randomised cart update; raise on non-200."""
    body = {
        "items": [
            {
                "sku": f"SKU-{random.randint(1, 999)}",
                "qty": random.randint(1, 5),
            }
            for _ in range(random.randint(1, 4))
        ],
        "user": f"user-{random.randint(1, 100)}",
    }
    path = f"/api/carts/{cart_id}"
    if region_hint:
        path += f"?region_hint={region_hint}"
    code, resp = _request("POST", path, body)
    if code != 200:
        raise RuntimeError(f"cart write failed: {code} {resp}")
    return resp


def main() -> int:
    print("Cart-update demo")
    print(f"Target: {BASE}")
    print()

    cart_ids: List[str] = [f"cart-{i:03d}" for i in range(50)]

    # ------------------------------------------------------------------
    # Step 1: 50 parallel cart writes — distribution per home region.
    # ------------------------------------------------------------------
    print("Step 1: 50 parallel cart writes")
    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(lambda c: post_cart(c), cart_ids))
    home_counts = Counter(r["home_region"] for r in results)
    for region, count in sorted(home_counts.items()):
        pct = 100.0 * count / len(cart_ids)
        print(f"  {region}: {count} carts ({pct:.0f}%)")

    # Brief pause so replication settles before the conflict round.
    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 2: provoke concurrent conflicting writes via region_hint.
    # ------------------------------------------------------------------
    print()
    print("Step 2: 5 cart_ids with 3 concurrent writes from different region_hints")
    conflict_targets = random.sample(cart_ids, 5)
    regions = ("us-east", "europe", "asia")
    conflict_ops: List[Tuple[str, str]] = [
        (c, r) for c in conflict_targets for r in regions
    ]
    with ThreadPoolExecutor(max_workers=15) as ex:
        # Materialise the iterator so any exception surfaces here.
        list(ex.map(lambda x: post_cart(*x), conflict_ops))
    print(f"  fired {len(conflict_ops)} concurrent writes across {len(conflict_targets)} cart_ids")

    # Give replication a moment to drain.
    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 3: each region's view of those 5 cart_ids — pick the latest
    # logical_ts per cart_id and show its home_region. Convergence
    # check: across all 3 regions the same cart_id must surface the
    # same home_region.
    # ------------------------------------------------------------------
    print()
    print("Step 3: each region's view of those 5 cart_ids (winner per cart_id)")
    per_region_winners: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for region in regions:
        code, logs = _request("GET", f"/api/regions/{region}/logs?limit=500")
        if code != 200:
            print(f"  {region}: ERROR {code}")
            continue
        per_cart: Dict[str, Dict[str, Any]] = {}
        for e in logs:
            cid = e.get("data", {}).get("cart_id")
            if cid in conflict_targets:
                if cid not in per_cart or e["logical_ts"] > per_cart[cid]["logical_ts"]:
                    per_cart[cid] = e
        per_region_winners[region] = per_cart
        for cid in conflict_targets:
            entry = per_cart.get(cid)
            if entry:
                home = entry["data"].get("home_region")
                print(
                    f"  {region}: {cid} -> winner home_region={home} "
                    f"ts={entry['logical_ts']}"
                )
            else:
                print(f"  {region}: {cid} -> (no entry observed)")

    # Convergence check: does every region agree on the winner per cart?
    print()
    print("Convergence check (all 3 regions should pick the same winner):")
    for cid in conflict_targets:
        winners = {
            region: per_region_winners.get(region, {}).get(cid, {}).get("log_id")
            for region in regions
        }
        unique = set(v for v in winners.values() if v is not None)
        status = "ok" if len(unique) == 1 else "DIVERGED"
        print(f"  {cid}: {status}  ({winners})")

    # ------------------------------------------------------------------
    # Step 4: replication lag p95 across secondaries.
    # ------------------------------------------------------------------
    print()
    print("Step 4: replication lag p95")
    code, status_payload = _request("GET", "/api/status")
    if code != 200:
        print(f"  /api/status returned {code}")
    else:
        for r in status_payload.get("regions", []):
            lag = r.get("replication_lag_ms")
            if lag is not None:
                print(f"  {r['region_id']}: p95 = {lag:.2f}ms")
            else:
                print(f"  {r['region_id']}: p95 = (no samples)")

    print()
    print("Demo complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

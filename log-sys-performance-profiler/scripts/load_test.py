#!/usr/bin/env python3
"""Standalone load-test runner. Hits POST /api/runs against BASE_URL, polls
until summary returns, prints throughput / p50 / p95 / peak_cpu / peak_mem.
Exits non-zero on timeout or zero-throughput failure.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

import httpx


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://app:8000"))
    ap.add_argument("--optimization", default=None)
    args = ap.parse_args()

    payload: dict = {"log_count": args.count, "concurrency": args.concurrency, "seed": args.seed}
    if args.optimization:
        payload["optimization_name"] = args.optimization

    async with httpx.AsyncClient(base_url=args.base_url, timeout=30.0) as ac:
        r = await ac.post("/api/runs", json=payload)
        r.raise_for_status()
        run_id = r.json()["run_id"]
        print(f"[load_test] started run_id={run_id} mode={r.json()['mode']}")

        for _ in range(120):
            await asyncio.sleep(1.0)
            r = await ac.get(f"/api/runs/{run_id}")
            if r.status_code == 200:
                summary = r.json()
                if summary["throughput_lps"] <= 0:
                    print(f"[load_test] ERROR throughput_lps <= 0: {summary['throughput_lps']}")
                    return 2
                print(
                    f"[load_test] throughput={summary['throughput_lps']:.1f} lps "
                    f"p50={summary['p50_ms']:.2f}ms p95={summary['p95_ms']:.2f}ms "
                    f"p99={summary['p99_ms']:.2f}ms "
                    f"peak_cpu={summary['peak_cpu']:.1f}% peak_mem={summary['peak_mem_mb']:.1f}MB"
                )
                return 0
        print(f"[load_test] TIMEOUT waiting for run {run_id}")
        return 3


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

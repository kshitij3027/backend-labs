"""User-friendly walkthrough of the real-time log indexing engine.

Prints a scripted tour of the HTTP surface so a reader can see, end
to end, how a log flows from ``/api/generate-sample`` through the
Redis stream into the inverted index and back out via ``/api/search``.
Runs in the Docker test profile (``make demo``) so it hits the real
FastAPI app at ``APP_URL``.

Produces pretty stdout — no assertions, no exit codes. The load test
(``scripts/load_test.py``) is the gate for success criteria; this
script just reads nicely.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import httpx


APP_URL = os.environ.get("APP_URL", "http://app:8080")

# Big enough that at least one common term (error/timeout/auth/payment)
# will land in results, but small enough that the whole demo runs in a
# few seconds even on a cold container.
GENERATE_COUNT = int(os.environ.get("DEMO_COUNT", "200"))


# ---------------------------------------------------------------------------
# Pretty-print helpers. Kept intentionally simple so the output reads
# like a terminal cheat-sheet rather than a JSON dump.
# ---------------------------------------------------------------------------

def _hr(title: str = "") -> None:
    """Print a heading with a dashed separator on both sides."""
    bar = "-" * 72
    print()
    print(bar)
    if title:
        print(f"  {title}")
        print(bar)


def _dump(obj: object) -> None:
    """Pretty-print a JSON-serialisable object."""
    print(json.dumps(obj, indent=2, sort_keys=True))


async def _wait_for_health(http: httpx.AsyncClient) -> dict:
    """Poll ``/health`` until it returns 200, then return the body."""
    for _ in range(30):
        try:
            r = await http.get("/health")
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        await asyncio.sleep(1)
    print("demo: service never became healthy", file=sys.stderr)
    sys.exit(1)


async def main() -> None:
    print(f"demo: app={APP_URL}")
    async with httpx.AsyncClient(base_url=APP_URL, timeout=30.0) as http:
        # ------------------------------------------------------------------
        # 1) Health probe
        # ------------------------------------------------------------------
        _hr("1) GET /health")
        health = await _wait_for_health(http)
        _dump(health)

        # ------------------------------------------------------------------
        # 2) Stats before ingest
        # ------------------------------------------------------------------
        _hr("2) GET /api/stats (before generating any sample logs)")
        stats_before = (await http.get("/api/stats")).json()
        _dump(stats_before)

        # ------------------------------------------------------------------
        # 3) Generate sample logs
        # ------------------------------------------------------------------
        _hr(f"3) POST /api/generate-sample  count={GENERATE_COUNT}")
        t0 = time.perf_counter()
        gen = (
            await http.post(
                "/api/generate-sample", json={"count": GENERATE_COUNT}
            )
        ).json()
        gen_took = (time.perf_counter() - t0) * 1000.0
        _dump(gen)
        print(f"(XADD round-trip took {gen_took:.1f} ms)")

        # Give the consumer a moment to drain the stream. We poll rather
        # than sleep blindly so a fast machine doesn't wait longer than
        # necessary and a slow machine doesn't bail early.
        print("\nwaiting for consumer to index the batch ...")
        deadline = time.time() + 10
        while time.time() < deadline:
            cur = (await http.get("/api/stats")).json()["docs_indexed"]
            if cur >= stats_before["docs_indexed"] + GENERATE_COUNT:
                break
            await asyncio.sleep(0.2)

        # ------------------------------------------------------------------
        # 4) Stats after ingest — docs_indexed should have jumped
        # ------------------------------------------------------------------
        _hr("4) GET /api/stats (after generating sample logs)")
        stats_after = (await http.get("/api/stats")).json()
        _dump(stats_after)
        delta = stats_after["docs_indexed"] - stats_before["docs_indexed"]
        print(
            f"(docs_indexed grew by {delta} — "
            f"expected >= {GENERATE_COUNT})"
        )

        # ------------------------------------------------------------------
        # 5) Search for "error" — any batch from generate_log_entries
        #    should have at least one ERROR-level log.
        # ------------------------------------------------------------------
        _hr("5) GET /api/search?q=error&limit=5")
        r = await http.get("/api/search?q=error&limit=5")
        body = r.json()
        print(
            f"total={body['total']}  took_ms={body['took_ms']}  "
            f"terms={body['terms']}"
        )
        for i, result in enumerate(body["results"][:3], start=1):
            print(
                f"  [{i}] doc_id={result['doc_id']} "
                f"service={result['service']} level={result['level']}"
            )
            print(f"       {result['highlighted_message']}")

        # ------------------------------------------------------------------
        # 6) Narrowed search — service filter
        # ------------------------------------------------------------------
        _hr(
            "6) GET /api/search?q=payment&service=payment-service  "
            "(filter demo)"
        )
        r = await http.get(
            "/api/search?q=payment&service=payment-service&limit=5"
        )
        body = r.json()
        print(
            f"total={body['total']}  took_ms={body['took_ms']}  "
            f"terms={body['terms']}"
        )
        for i, result in enumerate(body["results"][:3], start=1):
            print(
                f"  [{i}] doc_id={result['doc_id']} "
                f"service={result['service']} level={result['level']}"
            )
            print(f"       {result['highlighted_message']}")

        # ------------------------------------------------------------------
        # 7) Summary
        # ------------------------------------------------------------------
        _hr("summary")
        print(f"docs_indexed   : {stats_before['docs_indexed']} -> "
              f"{stats_after['docs_indexed']}")
        print(f"vocab_size     : {stats_before['vocab_size']} -> "
              f"{stats_after['vocab_size']}")
        print(f"memory_bytes   : {stats_before['memory_bytes']} -> "
              f"{stats_after['memory_bytes']}")
        print(
            f"disk_segments  : {stats_before['disk_segments']} -> "
            f"{stats_after['disk_segments']}"
        )
        print(
            f"open the dashboard: http://localhost:8080/  "
            f"(hit 'Generate 500' to watch the numbers move)"
        )


if __name__ == "__main__":
    asyncio.run(main())

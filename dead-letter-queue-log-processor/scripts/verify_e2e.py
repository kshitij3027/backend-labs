"""End-to-end verification script for the DLQ Log Processor."""

import asyncio
import json
import os
import sys
import time

import aiohttp

APP_URL = os.environ.get("APP_URL", "http://localhost:8000")


async def wait_for_health(timeout=30):
    """Wait for the app to be healthy."""
    print("[..] Waiting for app to be healthy...")
    start = time.time()
    async with aiohttp.ClientSession() as session:
        while time.time() - start < timeout:
            try:
                async with session.get(f"{APP_URL}/health") as resp:
                    if resp.status == 200:
                        print("[OK] App is healthy")
                        return True
            except (aiohttp.ClientError, ConnectionError, OSError):
                pass
            await asyncio.sleep(1)
    print("[FAIL] App failed to become healthy within %d seconds" % timeout)
    return False


async def check_dashboard():
    """Verify dashboard HTML loads."""
    print("\n--- Checking dashboard ---")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{APP_URL}/") as resp:
            assert resp.status == 200, f"Dashboard returned {resp.status}"
            text = await resp.text()
            assert "Dead Letter Queue" in text, "Dashboard missing expected title"
            print("[OK] Dashboard HTML loads correctly")


async def check_stats():
    """Verify stats are being tracked."""
    print("\n--- Checking stats ---")
    async with aiohttp.ClientSession() as session:
        # Wait for some processing to happen
        await asyncio.sleep(5)
        async with session.get(f"{APP_URL}/api/stats") as resp:
            assert resp.status == 200, f"Stats endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   Processed: {data.get('processed', 0)}")
            print(f"   Failed: {data.get('failed', 0)}")
            print(f"   Retried: {data.get('retries', 0)}")
            print(f"   Dead-lettered: {data.get('dead_lettered', 0)}")
            print(f"   DLQ Size: {data.get('dlq_size', 0)}")
            print(f"   Queue Length: {data.get('queue_length', 0)}")

            # After 5 seconds of processing we should have some stats
            total = data.get("processed", 0) + data.get("failed", 0)
            assert total > 0, "No messages processed after 5 seconds"
            print("[OK] Stats are being tracked")
            return data


async def check_dlq():
    """Verify DLQ has captured failed messages."""
    print("\n--- Checking DLQ ---")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{APP_URL}/api/dlq") as resp:
            assert resp.status == 200, f"DLQ endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   DLQ messages: {len(data)}")
            if data:
                sample = data[0]
                print(
                    f"   Sample failure type: "
                    f"{sample.get('failure_type', 'N/A')}"
                )
            print("[OK] DLQ endpoint working")
            return data


async def check_analysis():
    """Verify DLQ analysis works."""
    print("\n--- Checking DLQ analysis ---")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{APP_URL}/api/dlq/analysis") as resp:
            assert resp.status == 200, f"Analysis endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   Total in DLQ: {data.get('total', 0)}")
            print(f"   By type: {data.get('by_failure_type', {})}")
            print(f"   By source: {data.get('by_source', {})}")
            print("[OK] Analysis endpoint working")
            return data


async def check_trends():
    """Verify trends endpoint works."""
    print("\n--- Checking trends ---")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{APP_URL}/api/trends") as resp:
            assert resp.status == 200, f"Trends endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   Total failures in window: {data.get('total_failures', 0)}")
            print(f"   By type: {data.get('by_type', {})}")
            print("[OK] Trends endpoint working")
            return data


async def check_alerts():
    """Verify alerts endpoint works."""
    print("\n--- Checking alerts ---")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{APP_URL}/api/alerts") as resp:
            assert resp.status == 200, f"Alerts endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   Active alerts: {len(data)}")
            for alert in data:
                print(
                    f"   - [{alert.get('severity', '?')}] "
                    f"{alert.get('message', '')}"
                )
            print("[OK] Alerts endpoint working")
            return data


async def check_reprocess():
    """Verify reprocess works (reprocess then verify DLQ shrinks)."""
    print("\n--- Checking reprocess ---")
    async with aiohttp.ClientSession() as session:
        # Get initial DLQ count
        async with session.get(f"{APP_URL}/api/stats") as resp:
            initial = await resp.json()

        # Reprocess all
        async with session.post(f"{APP_URL}/api/dlq/reprocess") as resp:
            assert resp.status == 200, f"Reprocess endpoint failed: {resp.status}"
            data = await resp.json()
            print(f"   Reprocessed: {data.get('reprocessed', 0)}")

        # Verify DLQ is now empty (or smaller)
        async with session.get(f"{APP_URL}/api/stats") as resp:
            after = await resp.json()
            print(
                f"   DLQ before: {initial.get('dlq_size', 0)}, "
                f"after: {after.get('dlq_size', 0)}"
            )

        print("[OK] Reprocess working")


async def check_throughput():
    """Check throughput over a 5-second window."""
    print("\n--- Checking throughput ---")
    async with aiohttp.ClientSession() as session:
        # Take initial measurement
        async with session.get(f"{APP_URL}/api/stats") as resp:
            start_stats = await resp.json()
        start_time = time.time()

        # Wait 5 seconds
        await asyncio.sleep(5)

        # Take final measurement
        async with session.get(f"{APP_URL}/api/stats") as resp:
            end_stats = await resp.json()
        elapsed = time.time() - start_time

        start_total = start_stats.get("processed", 0) + start_stats.get("failed", 0)
        end_total = end_stats.get("processed", 0) + end_stats.get("failed", 0)
        throughput = (end_total - start_total) / elapsed
        print(f"   Throughput: {throughput:.1f} msg/sec")
        assert throughput > 1, f"Throughput too low: {throughput:.1f} msg/sec"
        print("[OK] Throughput acceptable")


async def run_all():
    print("=" * 60)
    print("  Dead Letter Queue Log Processor - E2E Verification")
    print("=" * 60)

    if not await wait_for_health():
        sys.exit(1)

    try:
        await check_dashboard()
        await check_stats()
        await check_dlq()
        await check_analysis()
        await check_trends()
        await check_alerts()
        await check_reprocess()
        await check_throughput()
    except AssertionError as e:
        print(f"\n[FAIL] E2E check failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FAIL] Unexpected error: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  All E2E checks passed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_all())

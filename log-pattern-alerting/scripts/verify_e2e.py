"""End-to-end verification script.

Checks that the application is running and the health endpoint responds.
"""

import os
import sys

import httpx


def main():
    app_url = os.environ.get("APP_URL", "http://localhost:8000")
    print(f"Running E2E checks against {app_url}", flush=True)

    # Check health endpoint
    try:
        resp = httpx.get(f"{app_url}/health", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        print(f"Health response: {data}", flush=True)

        if data.get("status") != "healthy":
            print("FAIL: Health status is not healthy", flush=True)
            sys.exit(1)

        if data.get("database") != "connected":
            print("FAIL: Database is not connected", flush=True)
            sys.exit(1)

        if data.get("redis") != "connected":
            print("FAIL: Redis is not connected", flush=True)
            sys.exit(1)

    except Exception as exc:
        print(f"FAIL: Health check failed: {exc}", flush=True)
        sys.exit(1)

    print("E2E PASSED", flush=True)


if __name__ == "__main__":
    main()

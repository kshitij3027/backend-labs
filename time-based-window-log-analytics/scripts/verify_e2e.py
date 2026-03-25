"""End-to-end verification script for the log analytics service."""

import os
import sys

import httpx

APP_URL = os.environ.get("APP_URL", "http://localhost:8080")


def main() -> None:
    print(f"E2E: Testing against {APP_URL}")

    r = httpx.get(f"{APP_URL}/health", timeout=10)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    print("PASS: Health check OK")

    print("All E2E tests passed!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

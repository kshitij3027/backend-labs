"""Demo-data helper — POST /reports/generate for every framework × format combo.

Useful for populating the dashboard during a screen-share / Chrome MCP walk.
Reads ``BASE_URL`` from the environment (defaults to ``http://localhost:8000``).

Run inside the ``tester`` container via::

    docker compose --profile test run --rm tester python scripts/seed_reports.py

The script does NOT wait for the reports to finish — it just kicks off
the coordinator and prints the resulting ``report_id``. Poll
``GET /reports/{id}`` (or open the dashboard) to watch the state machine.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx

# Mirrors the registered set in ``src/frameworks/`` — kept in sync by hand
# rather than imported so the script can run outside the ``app`` container
# (the dashboard demo on localhost is the common case).
FRAMEWORKS = ["SOX", "HIPAA", "PCI_DSS", "GDPR", "FINHEALTH"]
FORMATS = ["PDF", "CSV", "JSON", "XML"]


async def main() -> int:
    base_url = os.environ.get("BASE_URL", "http://localhost:8000")
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=30)
    async with httpx.AsyncClient(base_url=base_url, timeout=15.0) as client:
        fail = 0
        for framework in FRAMEWORKS:
            for fmt in FORMATS:
                payload = {
                    "framework": framework,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "export_format": fmt,
                    "title": f"Demo {framework} ({fmt})",
                }
                r = await client.post("/reports/generate", json=payload)
                if r.status_code != 202:
                    print(
                        f"FAIL {framework}/{fmt}: HTTP {r.status_code} {r.text}",
                        file=sys.stderr,
                    )
                    fail += 1
                else:
                    print(f"OK   {framework}/{fmt} -> {r.json()['report_id']}")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

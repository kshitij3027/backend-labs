"""CLI: seed synthetic compliance log events into the database.

Run inside the ``tester`` container via::

    docker compose --profile test run --rm tester \
        python scripts/seed_logs.py --count 5000

The script reads ``DATABASE_URL`` from the environment (falling back to
:class:`src.settings.Settings` defaults), builds an async engine via
:func:`src.persistence.db.make_engine`, makes the tables exist
idempotently with ``init_db``, generates ``--count`` events spread
across the last ``--days`` days, bulk-inserts them, and prints a
one-line summary. ``--seed`` is exposed so demo data can be made
exactly reproducible.

This is a developer / CI tool — there's no need for graceful retries
or chunked inserts at the scales this project targets (~5k events).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

# Make ``src`` importable when invoked as ``python scripts/seed_logs.py``
# from the repo root inside the container (which is the entry pattern
# the Makefile and the Test Agent use).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.logging_config import configure_logging, get_logger  # noqa: E402
from src.logs.repository import insert_log_events  # noqa: E402
from src.logs.seeder import generate_synthetic_logs  # noqa: E402
from src.persistence.db import init_db, make_engine, make_session_factory  # noqa: E402
from src.settings import get_settings  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed synthetic compliance log events into the configured database."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5000,
        help="Number of events to generate (default: 5000)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Span of the time window in days, ending at now (default: 30)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic output (default: 42)",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("scripts.seed_logs")

    database_url = os.environ.get("DATABASE_URL", settings.database_url)
    frameworks = settings.supported_frameworks_list

    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=args.days)

    logger.info(
        "seed_starting",
        count=args.count,
        days=args.days,
        seed=args.seed,
        frameworks=frameworks,
        period_start=period_start.isoformat(),
        period_end=period_end.isoformat(),
    )

    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)
    try:
        # Idempotent — safe to run repeatedly against the same DB.
        await init_db(engine)

        events = generate_synthetic_logs(
            args.count,
            frameworks=frameworks,
            seed=args.seed,
            period_start=period_start,
            period_end=period_end,
        )

        async with session_factory() as session:
            inserted = await insert_log_events(session, events)
            await session.commit()
    finally:
        await engine.dispose()

    summary = (
        f"Seeded {inserted} log events across frameworks {frameworks} "
        f"for window {period_start.isoformat()} to {period_end.isoformat()}"
    )
    print(summary)
    logger.info("seed_complete", inserted=inserted)


if __name__ == "__main__":
    asyncio.run(main())

"""One-shot, idempotent DB initializer for the multi-tier caching layer.

Run by the compose ``db-init`` service before the app starts:

    python scripts/init_db.py

It applies the schema and seeds ``raw_logs`` with ``settings.seed_rows``
deterministic synthetic rows. Safe to run repeatedly: if the table already
holds at least ``seed_rows`` rows it skips re-seeding. The aggregation queries
and L3 materialization come later (C10); this script only establishes schema +
the slow source data.
"""
from __future__ import annotations

import asyncio
import logging
import time

from src.db.pool import apply_schema, create_pool
from src.db.seed import count_raw_logs, seed_raw_logs
from src.settings import get_settings

logger = logging.getLogger("init_db")


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    logger.info("Connecting to Postgres and applying schema...")
    pool = await create_pool(settings.database_url)
    try:
        await apply_schema(pool)
        logger.info("Schema applied.")

        existing = await count_raw_logs(pool)
        if existing >= settings.seed_rows:
            logger.info(
                "raw_logs already has %d rows (>= %d target); already seeded, skipping.",
                existing,
                settings.seed_rows,
            )
            return

        # Reset to a clean slate so the seed is exactly deterministic.
        logger.info(
            "raw_logs has %d rows (< %d target); truncating and re-seeding...",
            existing,
            settings.seed_rows,
        )
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE raw_logs")

        inserted = await seed_raw_logs(
            pool,
            settings.seed_rows,
            seed=settings.seed_random_seed,
            end_ts=time.time(),
        )
        total = await count_raw_logs(pool)
        logger.info("Seeded %d rows; raw_logs now has %d rows.", inserted, total)
    finally:
        await pool.close()
        logger.info("Done; pool closed.")


if __name__ == "__main__":
    asyncio.run(main())

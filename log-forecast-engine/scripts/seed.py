"""Seed the database with a synthetic metric dataset.

Generates time series for the three metric families (response_time, error_rate,
throughput) over the default training window and inserts them directly via the
repository (fast bulk path — no HTTP round-trip).

Run inside the container (WORKDIR ``/app``, ``PYTHONPATH=/app``)::

    docker compose run --rm api python -m scripts.seed
    docker compose run --rm api python -m scripts.seed --days 3 --interval 300
    docker compose run --rm api python -m scripts.seed --metric response_time

or via the Make target::

    make seed

Idempotency note: this *appends* rows; running it repeatedly stacks duplicate
windows. For a clean slate, recreate the volume (``make clean``) first.
"""

from __future__ import annotations

import argparse
import sys

from src.db.session import get_session
from src.generator import METRIC_NAMES, generate_default_dataset


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts.seed",
        description="Generate and ingest a synthetic metric dataset.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Window length in days (default: configured training window, 7).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Interval between points in seconds (default: 300 = 5 min).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base RNG seed for deterministic output (default: 42).",
    )
    parser.add_argument(
        "--metric",
        choices=METRIC_NAMES,
        default=None,
        help="Seed only this metric (default: all three).",
    )
    return parser.parse_args(argv)


def seed(
    days: int | None = None,
    interval_seconds: int = 300,
    seed_value: int = 42,
    only_metric: str | None = None,
) -> dict[str, int]:
    """Generate the dataset and insert it; return rows-inserted per metric."""
    dataset = generate_default_dataset(
        days=days, interval_seconds=interval_seconds, seed=seed_value
    )
    if only_metric is not None:
        dataset = {only_metric: dataset[only_metric]}

    inserted: dict[str, int] = {}
    with get_session() as session:
        for name, points in dataset.items():
            rows = [
                {
                    "metric_name": p.metric_name,
                    "timestamp": p.timestamp,
                    "value": p.value,
                }
                for p in points
            ]
            # Import here to keep module import side-effect free.
            from src.db import repository

            repository.add_metrics_bulk(session, rows, commit=False)
            inserted[name] = len(rows)
        session.commit()
    return inserted


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    inserted = seed(
        days=args.days,
        interval_seconds=args.interval,
        seed_value=args.seed,
        only_metric=args.metric,
    )
    total = sum(inserted.values())
    print("Seeded synthetic metrics:")
    for name, count in inserted.items():
        print(f"  {name:>15}: {count} rows")
    print(f"  {'TOTAL':>15}: {total} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())

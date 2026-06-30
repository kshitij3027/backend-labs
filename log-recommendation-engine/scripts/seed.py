"""Seed the database with a synthetic historical-incident corpus.

Generates incidents from the incident families (see :mod:`src.generator`) and
inserts them directly via the repository bulk path (no HTTP round-trip). From C5
each row is **batch-embedded on seed**: every incident's canonical document text
(:func:`src.embeddings.build_incident_text`) is encoded in a single
:func:`src.embeddings.embed_texts` call and the resulting MiniLM vector is attached
to its row before the bulk insert — so a freshly seeded corpus is immediately
searchable (no separate backfill needed).

Run inside the container (WORKDIR ``/app``, ``PYTHONPATH=/app``)::

    docker compose run --rm api python -m scripts.seed
    docker compose run --rm api python -m scripts.seed --count 200
    docker compose run --rm api python -m scripts.seed --count 60 --seed 7

or via the Make target::

    make seed
    make seed ARGS="--count 200"

Idempotency note: this *appends* rows; running it repeatedly stacks more
incidents. For a clean slate, recreate the volume (``make clean``) first.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

from src import embeddings
from src.db import repository
from src.db.session import get_session
from src.generator import generate_default_corpus, generate_incidents


def _default_count() -> int:
    """Size of the default corpus (kept in one place)."""
    return len(generate_default_corpus())


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts.seed",
        description="Generate and ingest a synthetic historical-incident corpus.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of incidents to generate (default: the default-corpus size).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic output (default: 42).",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=180,
        help="Spread created_at across the last N days (default: 180).",
    )
    return parser.parse_args(argv)


def _attach_embeddings(rows: list[dict]) -> int:
    """Batch-embed ``rows`` in place, attaching each vector under ``"embedding"``.

    Builds every row's canonical document text with
    :func:`src.embeddings.build_incident_text` and encodes them all in a single
    :func:`src.embeddings.embed_texts` call (one efficient batch, one model load),
    then stores each ``(384,)`` vector as a plain ``list`` on its row so the bulk
    insert persists it. Returns the number of rows embedded.
    """
    if not rows:
        return 0
    docs = [
        embeddings.build_incident_text(r["title"], r["description"], r.get("tags"))
        for r in rows
    ]
    vectors = embeddings.embed_texts(docs)
    for row, vec in zip(rows, vectors):
        row["embedding"] = vec.tolist()
    return len(rows)


def seed(
    count: int | None = None,
    seed_value: int = 42,
    days_back: int = 180,
) -> list[dict]:
    """Generate the corpus, embed it, and insert it; return the inserted row dicts.

    When ``count`` is ``None`` the default corpus is used. Every row is
    batch-embedded (see :func:`_attach_embeddings`) before insertion so the seeded
    corpus is immediately searchable. Rows are inserted in a single transaction
    (``commit=True`` on the bulk helper).
    """
    if count is None:
        rows = generate_default_corpus(seed=seed_value, days_back=days_back)
    else:
        rows = generate_incidents(count, seed=seed_value, days_back=days_back)

    _attach_embeddings(rows)

    with get_session() as session:
        repository.add_incidents_bulk(session, rows, commit=True)
    return rows


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    rows = seed(
        count=args.count,
        seed_value=args.seed,
        days_back=args.days_back,
    )

    embedded = sum(1 for r in rows if r.get("embedding") is not None)
    per_service = Counter(r["service"] for r in rows)
    print(f"Seeded synthetic incident corpus: {len(rows)} incidents")
    print(f"Embedded on seed: {embedded}/{len(rows)} incidents")
    print("Per-service counts:")
    for service, n in sorted(per_service.items()):
        print(f"  {service:>16}: {n}")
    print(f"  {'TOTAL':>16}: {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

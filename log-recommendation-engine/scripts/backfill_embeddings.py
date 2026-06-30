"""Backfill MiniLM embeddings for incidents whose ``embedding IS NULL``.

From C5, new incidents are embedded on ingest (``POST /incidents``) and the seed
script embeds its whole corpus, so under normal operation nothing is left
NULL-embedded. This script is the recovery / migration path for rows that *are*
missing a vector — e.g. incidents inserted before C5, rows created while the
embedding service was down (future C21), or a bulk import that skipped embedding.

It pages through the missing rows (:func:`repository.get_incidents_missing_embedding`),
**batch-embeds** each page in a single :func:`src.embeddings.embed_texts` call
(one model load, efficient batches), writes each vector back via
:func:`repository.set_incident_embedding`, and **commits per batch** so progress
is durable and the job is resumable — a re-run only picks up whatever remains
NULL. Embeddings are built from the same canonical document text
(:func:`src.embeddings.build_incident_text`) as ingest/queries, keeping every
stored vector comparable.

Run inside the container (WORKDIR ``/app``, ``PYTHONPATH=/app``)::

    docker compose run --rm api python -m scripts.backfill_embeddings
    docker compose run --rm api python -m scripts.backfill_embeddings --batch-size 128

or via the Make target::

    make backfill
    make backfill ARGS="--batch-size 128"
"""

from __future__ import annotations

import argparse
import sys

from sqlalchemy.orm import Session

from src import embeddings, observability
from src.db import repository
from src.db.session import get_session

logger = observability.get_logger(__name__)

DEFAULT_BATCH_SIZE = 256


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts.backfill_embeddings",
        description=(
            "Compute and persist embeddings for incidents whose embedding IS NULL."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            "How many missing rows to load + embed + commit per batch "
            f"(default: {DEFAULT_BATCH_SIZE})."
        ),
    )
    return parser.parse_args(argv)


def _backfill_one_batch(session: Session, batch_size: int) -> int:
    """Embed + persist a single batch of NULL-embedded incidents.

    Loads up to ``batch_size`` rows still missing a vector, batch-encodes their
    document text, sets each embedding, and commits once. Returns the number of
    rows embedded in this batch (``0`` when none remain).
    """
    incidents = repository.get_incidents_missing_embedding(session, limit=batch_size)
    if not incidents:
        return 0

    docs = [
        embeddings.build_incident_text(inc.title, inc.description, inc.tags)
        for inc in incidents
    ]
    vectors = embeddings.embed_texts(docs)
    for inc, vec in zip(incidents, vectors):
        # flush per row; single commit for the whole batch below.
        repository.set_incident_embedding(session, inc.id, vec.tolist(), commit=False)
    session.commit()
    return len(incidents)


def backfill(batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Backfill every NULL-embedded incident in batches; return the total count.

    Loops :func:`_backfill_one_batch` until no missing rows remain. Because each
    batch commits before the next page is fetched, the job is durable and safely
    resumable. ``batch_size`` must be positive.
    """
    if batch_size <= 0:
        raise ValueError("batch-size must be positive")

    total = 0
    with get_session() as session:
        while True:
            n = _backfill_one_batch(session, batch_size)
            if n == 0:
                break
            total += n
            logger.info("backfilled embedding batch", batch=n, total=total)
    return total


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    total = backfill(batch_size=args.batch_size)
    if total == 0:
        print("No incidents needed backfilling (all embeddings present).")
    else:
        print(f"Backfilled embeddings for {total} incident(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

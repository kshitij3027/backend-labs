"""System / observability routes — corpus stats and a deep readiness ``/health`` (C13).

Two read-only endpoints round out the operability surface:

* ``GET /stats`` — an at-a-glance rollup of the durable state: corpus size + embedded
  coverage, incident counts by service/severity, raw feedback tallies, how many
  recommendations have been served, and the busiest learned query-pattern buckets. All
  aggregates are computed in the repository (SQL ``GROUP BY`` / ``COUNT``); this router
  only maps the tuples onto the response schema.

* ``GET /health`` — a **deep** liveness+readiness probe that reports per-subsystem
  status (``database`` / ``redis`` / ``embedding_model``) plus the corpus size. Every
  probe is fast and wrapped so a failure degrades to ``False`` rather than raising, and
  the endpoint **always returns HTTP 200 while the process is alive** — a degraded
  dependency is signalled by ``status: "degraded"`` in the body, not a non-2xx status.
  This is deliberate: the compose healthcheck curls ``/health`` and must stay green as
  long as uvicorn is bound (liveness), while the body still exposes readiness detail for
  a dashboard. The Prometheus ``/metrics`` endpoint is a separate concern (C14) and is
  intentionally *not* added here.

``GET /stats`` runs over a request-scoped :class:`~sqlalchemy.orm.Session`
(``Depends(get_db)``). ``GET /health`` opens its own short-lived session so a DB outage
surfaces as ``database: false`` in the body instead of a 500 from the ``get_db``
dependency failing to connect.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from src import embeddings, observability
from src.clients import redis as redis_client
from src.db import repository
from src.db.session import SessionLocal, get_db
from src.schemas import (
    ComponentsHealth,
    HealthResponse,
    PatternStat,
    StatsResponse,
)

logger = observability.get_logger(__name__)

#: Reported in the /health payload (kept in sync with :mod:`src.api`). Imported lazily
#: inside the handler to avoid a circular import (``src.api`` imports this router).
_SERVICE_NAME = "log-recommendation-engine"
_SERVICE_VERSION = "0.1.0"

router = APIRouter(tags=["system"])


@router.get(
    "/stats",
    response_model=StatsResponse,
    status_code=status.HTTP_200_OK,
    summary="Corpus / feedback summary statistics",
)
def get_stats(db: Session = Depends(get_db)) -> StatsResponse:
    """Return an at-a-glance rollup of the corpus + feedback state.

    Every figure is an aggregate query in the repository:

    * ``corpus_size`` / ``embedded_count`` — total incidents vs. those with a vector.
    * ``by_service`` / ``by_severity`` — grouped incident counts.
    * ``feedback_total`` / ``feedback_helpful`` / ``feedback_unhelpful`` — raw votes.
    * ``recommendations_served`` — persisted recommendation rows.
    * ``top_patterns`` — the ~5 busiest learned query-pattern buckets by total votes.
    """
    corpus_size = repository.count_incidents(db)
    embedded_count = repository.count_embedded_incidents(db)
    by_service = repository.incident_counts_by_service(db)
    by_severity = repository.incident_counts_by_severity(db)
    feedback_total, feedback_helpful, feedback_unhelpful = (
        repository.feedback_totals(db)
    )
    recommendations_served = repository.count_recommendations(db)
    patterns = [
        PatternStat(query_pattern=pattern, helpful=helpful, unhelpful=unhelpful)
        for pattern, helpful, unhelpful in repository.top_patterns(db, limit=5)
    ]

    return StatsResponse(
        corpus_size=corpus_size,
        embedded_count=embedded_count,
        by_service=by_service,
        by_severity=by_severity,
        feedback_total=feedback_total,
        feedback_helpful=feedback_helpful,
        feedback_unhelpful=feedback_unhelpful,
        recommendations_served=recommendations_served,
        top_patterns=patterns,
    )


def _embedding_model_loaded() -> bool:
    """Return ``True`` iff the embedding-model singleton is **already** loaded.

    Inspects the ``@lru_cache`` wrapping :func:`src.embeddings.get_model` via its
    :meth:`cache_info` — ``currsize == 1`` means a prior call has cached the (heavy)
    model in this process. This is a cheap, side-effect-free check: it deliberately
    does **not** call ``get_model()``, so a cold-but-healthy process reports ``False``
    here rather than paying the ~90 MB load inside a health probe. Never raises.
    """
    try:
        return embeddings.get_model.cache_info().currsize > 0
    except Exception:  # noqa: BLE001 - health probe must never raise
        return False


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Deep liveness + per-component readiness probe",
)
def health() -> HealthResponse:
    """Deep health probe — reports per-subsystem readiness, always HTTP 200 when alive.

    Runs a fast, failure-tolerant probe of each dependency and folds the results into
    the response body:

    * ``database`` — ``SELECT 1`` succeeds (``vector_extension`` additionally checks the
      pgvector extension is installed).
    * ``redis`` — :func:`src.clients.redis.ping` answers.
    * ``embedding_model`` — the model singleton is already loaded (cheap ``lru_cache``
      inspection; never forces a load — see :func:`_embedding_model_loaded`).
    * ``corpus_size`` — best-effort incident count (``0`` when the DB is down).

    ``status`` is ``"ok"`` only when the **required** dependencies (database, redis) are
    both up, else ``"degraded"``. The endpoint **always returns HTTP 200** while the
    process is alive so the container's ``curl /health`` healthcheck stays green
    (liveness); a degraded stack is reported in the body, never as a non-2xx status.
    Each probe is individually wrapped so one failing subsystem cannot take the whole
    endpoint down.
    """
    database_ok = False
    vector_ok = False
    corpus_size = 0

    # Open our own short-lived session so a DB outage becomes ``database: false`` in the
    # body rather than a 500 from a failing dependency. Always closed in ``finally``.
    session: Session | None = None
    try:
        session = SessionLocal()
        database_ok = repository.database_ready(session)
        if database_ok:
            vector_ok = repository.vector_extension_present(session)
            try:
                corpus_size = repository.count_incidents(session)
            except Exception:  # noqa: BLE001 - count is best-effort
                corpus_size = 0
    except Exception:  # noqa: BLE001 - never let the health probe raise
        database_ok = False
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001 - closing must not raise from a probe
                pass

    redis_ok = redis_client.ping()
    model_loaded = _embedding_model_loaded()

    # Required dependencies for "ok": the request path needs Postgres (retrieval) and
    # Redis (cache / feedback epoch / config). A not-yet-loaded model is normal on a
    # cold process, so it does NOT by itself mark the service degraded.
    overall = "ok" if (database_ok and redis_ok) else "degraded"

    return HealthResponse(
        status=overall,
        service=_SERVICE_NAME,
        version=_SERVICE_VERSION,
        components=ComponentsHealth(
            database=database_ok,
            vector_extension=vector_ok,
            redis=redis_ok,
            embedding_model=model_loaded,
        ),
        corpus_size=corpus_size,
    )

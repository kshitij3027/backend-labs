"""Persistence layer — SQLAlchemy 2.x async against a real PostgreSQL (spec §2, item 32).

Four modules, in dependency order, and the order is the point:

* :mod:`src.db.base` — the :class:`~sqlalchemy.orm.DeclarativeBase` and nothing else.
* :mod:`src.db.models` — the mapped tables plus :class:`~src.db.models.LogRecord`, the plain
  value object the deterministic generator emits.
* :mod:`src.db.session` — the engine, the session factory, boot-time schema creation and seeding.
* :mod:`src.db.repository` — the filter -> SELECT builder every read path goes through.

Nothing here imports Strawberry, FastAPI, or anything from :mod:`src.graphql` (C3+). That is a
deliberate boundary: the E2E verifier and the load harness import :mod:`src.generators` — which
imports :mod:`src.db.models` for :class:`~src.db.models.LogRecord` — to build their ground truth,
and dragging the whole web stack into a standalone script is how a verifier ends up failing for
reasons that have nothing to do with the service under test.
"""

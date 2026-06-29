"""Persistence layer for the Log Recommendation Engine.

The package groups the SQLAlchemy 2.0 declarative base (:mod:`src.db.base`), the
engine/session machinery (:mod:`src.db.session`), the ORM models
(:mod:`src.db.models`) and the repository CRUD helpers
(:mod:`src.db.repository`). Alembic (``migrations/env.py``) consumes
``Base.metadata`` from here as its ``target_metadata``.
"""

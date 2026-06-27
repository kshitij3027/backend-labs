"""Persistence layer for the Predictive Log Analytics Engine.

Exposes the SQLAlchemy 2.0 declarative ``Base``, ORM models, the session
machinery (engine / ``SessionLocal`` / ``get_session`` / ``get_db``) and the
repository CRUD helpers used by the rest of the system.

Typical usage::

    from src.db.base import Base
    from src.db.models import Metric, Forecast, ModelMetadata, AccuracyRecord
    from src.db import repository
    from src.db.session import get_session

    with get_session() as session:
        repository.add_metric(session, "response_time", ts, 52.3, commit=True)
"""

from __future__ import annotations

from src.db.base import Base
from src.db.models import AccuracyRecord, Forecast, Metric, ModelMetadata
from src.db.session import SessionLocal, get_db, get_engine, get_session

__all__ = [
    "Base",
    "Metric",
    "Forecast",
    "ModelMetadata",
    "AccuracyRecord",
    "SessionLocal",
    "get_db",
    "get_engine",
    "get_session",
]

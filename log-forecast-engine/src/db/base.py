"""SQLAlchemy 2.0 declarative base.

A single :class:`Base` is shared by every ORM model so that ``Base.metadata``
is the authoritative schema description consumed by Alembic
(``migrations/env.py`` sets ``target_metadata = Base.metadata``).
"""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for all ORM models in this project."""

    pass

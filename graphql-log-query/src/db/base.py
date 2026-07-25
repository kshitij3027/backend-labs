"""The declarative base — one class, and a rule about import order.

.. rubric:: Every model module must be imported before ``Base.metadata.create_all`` runs

``Base.metadata`` is a :class:`~sqlalchemy.schema.MetaData` *registry*, and a table only appears
in it as a side effect of its mapped class being **defined**, which only happens when the module
declaring it is imported. So ``create_all`` does not create "the schema"; it creates exactly the
tables Python has executed a ``class …(Base)`` statement for by that moment.

The failure mode is quiet rather than loud: a model module nobody imported produces no error, no
warning and no table — just a ``relation "…" does not exist`` from the first query that touches
it, several layers away from the missing import that caused it. :mod:`src.db.session` therefore
imports :mod:`src.db.models` at module scope specifically so that importing the thing that *runs*
``create_all`` also guarantees the registry is populated, and C10's e-commerce tables must be
declared in that same already-imported module (or imported by it) for the same reason.

Kept in its own module — rather than beside the models — so that a future model module can import
``Base`` without importing every other model, which is what would otherwise create an import
cycle the moment two model modules need to reference each other.
"""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for every mapped class in this project.

    Deliberately empty. Shared columns, naming conventions and mixins are all things that can be
    added later without breaking anything; a base class that already carries them is much harder
    to walk back, and this project has exactly one table until C10.
    """

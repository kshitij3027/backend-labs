"""FastAPI routers for the Log Recommendation Engine.

Each module here exposes an ``APIRouter`` that :func:`src.api.create_app` includes
on the application. Keeping routers in their own package keeps ``api.py`` to a
small, readable factory. In C3 the ``incidents`` router (the historical-incident
corpus surface) is registered; later commits add ``recommend`` / ``feedback`` /
``config`` / ``system`` routers alongside it.
"""

from __future__ import annotations

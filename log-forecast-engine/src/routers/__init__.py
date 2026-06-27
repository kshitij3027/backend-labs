"""FastAPI routers for the Predictive Log Analytics Engine.

Each module here exposes an ``APIRouter`` that :func:`src.api.create_app` includes
on the application. Keeping routers in their own package keeps ``api.py`` to a
small, readable factory.
"""

from __future__ import annotations

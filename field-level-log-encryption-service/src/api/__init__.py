"""HTTP layer for the field-level log encryption service.

C7 wires the C2-C6 in-process components (Detector, KeyStore, LogProcessor,
AuditLogger, StatsCounters) into REST endpoints. This package keeps the
HTTP surface (routes + request/response Pydantic models + Prometheus
metrics) one ``import`` away from :mod:`src.main` so the entrypoint stays
focused on lifecycle wiring rather than request handling.

Public surface:

* :data:`router`              — the ``APIRouter`` aggregating every route.
* request/response models     — re-exported from :mod:`src.api.models`.
* Prometheus counter/histogram singletons — exposed via :mod:`src.api.metrics`.
"""
from __future__ import annotations

from .routes import router

__all__ = ["router"]

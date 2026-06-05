"""FastAPI dependency providers backed by ``app.state``.

The full optimizer object graph (manifest store, pattern tracker, metrics,
compression chooser, index / tier managers, format selector, the per-format
storage backends, and the ingest / query / migration engines) is constructed
once during the lifespan startup (see :mod:`src.main`) and stashed on
``app.state``. These thin providers hand the pieces the REST routes actually
need to handlers via ``Depends(...)`` so the routes never reach into
``app.state`` directly.

Imports of the concrete engine types are kept inside ``TYPE_CHECKING`` so this
module stays import-cheap and free of any risk of an import cycle with
:mod:`src.main` (which imports the routers, which import this module).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request

if TYPE_CHECKING:  # pragma: no cover - typing-only imports, no runtime cost.
    from src.format_selector import FormatSelector
    from src.ingest_engine import IngestEngine
    from src.manifest import ManifestStore
    from src.metrics import Metrics
    from src.pattern_tracker import PatternTracker
    from src.query_engine import QueryEngine
    from src.settings import Settings
    from src.tier_manager import TierManager


def get_ingest_engine(request: Request) -> "IngestEngine":
    """Return the process-wide :class:`~src.ingest_engine.IngestEngine`."""
    return request.app.state.ingest_engine


def get_query_engine(request: Request) -> "QueryEngine":
    """Return the process-wide :class:`~src.query_engine.QueryEngine`."""
    return request.app.state.query_engine


def get_manifest(request: Request) -> "ManifestStore":
    """Return the per-tenant :class:`~src.manifest.ManifestStore`."""
    return request.app.state.manifest


def get_metrics(request: Request) -> "Metrics":
    """Return the :class:`~src.metrics.Metrics` aggregator from ``app.state``."""
    return request.app.state.metrics


def get_settings_dep(request: Request) -> "Settings":
    """Return the process-wide :class:`~src.settings.Settings` from ``app.state``."""
    return request.app.state.settings


def get_selector(request: Request) -> "FormatSelector":
    """Return the process-wide :class:`~src.format_selector.FormatSelector`."""
    return request.app.state.selector


def get_tier_manager(request: Request) -> "TierManager":
    """Return the process-wide :class:`~src.tier_manager.TierManager`."""
    return request.app.state.tier_manager


def get_pattern_tracker(request: Request) -> "PatternTracker":
    """Return the process-wide :class:`~src.pattern_tracker.PatternTracker`."""
    return request.app.state.pattern_tracker

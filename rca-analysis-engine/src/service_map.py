"""Static service-dependency map for the RCA Analysis Engine.

The causal-graph builder (C3) only draws an edge ``u -> v`` when ``service(u)``
sits **upstream** of ``service(v)`` in this map, so the map encodes the allowed
direction of causal propagation. It is externalized to
``src/config/service_dependency_map.json`` (Req §7: the map "should be
externalized") and loaded through :meth:`ServiceDependencyMap.from_settings`, with
a hard-coded copy as a graceful fallback when the file is missing or unreadable so
the engine never fails to start over a config problem.

C1 implements **direct** (one-hop) dependency lookup: :meth:`is_dependency` is true
iff ``downstream`` is a declared direct downstream of ``upstream``. Transitive
reachability over the assembled causal graph (``nx.descendants``) is layered on by
the impact analyzer in C5; this module stays a small, pure lookup over the declared
topology.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)

#: Hard-coded fallback: Req §7's 8-service topology (upstream -> direct downstream).
#: Kept in sync with src/config/service_dependency_map.json; used only when that file
#: is missing/unreadable so the engine still runs with a sane default map.
DEFAULT_SERVICE_DEPENDENCY_MAP: dict[str, list[str]] = {
    "api-gateway": ["auth", "user", "payment"],
    "auth": ["database", "redis"],
    "user": ["database", "file-storage"],
    "payment": ["database", "external-payment-api"],
    "database": [],
    "redis": [],
    "file-storage": [],
    "external-payment-api": [],
}


class ServiceDependencyMap:
    """A directed upstream -> downstream service topology with direct-edge lookups."""

    def __init__(self, mapping: dict[str, list[str]]) -> None:
        # Normalize to sets for O(1) membership. Any service that appears only as a
        # downstream (e.g. a leaf) is still registered as a known node with no
        # outgoing dependencies, so all_services() is complete.
        self._downstream: dict[str, set[str]] = {}
        for upstream, downstreams in mapping.items():
            self._downstream.setdefault(upstream, set()).update(downstreams)
            for downstream in downstreams:
                self._downstream.setdefault(downstream, set())

    @classmethod
    def from_mapping(cls, mapping: dict[str, list[str]]) -> ServiceDependencyMap:
        """Build directly from an in-memory mapping (handy for tests)."""
        return cls(mapping)

    @classmethod
    def from_file(cls, path: str | Path) -> ServiceDependencyMap:
        """Load the map from a JSON file, falling back to the hard-coded default.

        A missing file, unreadable path, malformed JSON or non-object payload logs
        a warning and yields :data:`DEFAULT_SERVICE_DEPENDENCY_MAP`.
        """
        p = Path(path)
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except FileNotFoundError:
            logger.warning("service dependency map %s not found; using built-in default", p)
            return cls(dict(DEFAULT_SERVICE_DEPENDENCY_MAP))
        except (OSError, ValueError) as exc:
            logger.warning("service dependency map %s unreadable (%s); using default", p, exc)
            return cls(dict(DEFAULT_SERVICE_DEPENDENCY_MAP))
        if not isinstance(raw, dict):
            logger.warning("service dependency map %s is not a JSON object; using default", p)
            return cls(dict(DEFAULT_SERVICE_DEPENDENCY_MAP))
        # Coerce defensively to dict[str, list[str]] (skip non-list values).
        mapping = {
            str(k): [str(x) for x in v] for k, v in raw.items() if isinstance(v, list)
        }
        return cls(mapping)

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> ServiceDependencyMap:
        """Load the map from ``settings.service_dependency_map_path``."""
        settings = settings or get_settings()
        return cls.from_file(settings.service_dependency_map_path)

    def is_dependency(self, upstream: str, downstream: str) -> bool:
        """True iff ``downstream`` is a **direct** declared downstream of ``upstream``.

        C1 scope is one-hop lookup (see the module docstring); transitive
        reachability is handled later by the causal graph / impact analyzer.
        """
        return downstream in self._downstream.get(upstream, set())

    def downstream_of(self, service: str) -> set[str]:
        """The direct downstream dependencies of ``service`` (empty if leaf/unknown)."""
        return set(self._downstream.get(service, set()))

    def all_services(self) -> set[str]:
        """Every known service (union of declared upstreams and downstreams)."""
        return set(self._downstream.keys())

"""Framework rule-engine package.

This module owns the registry of available compliance frameworks. Each
concrete ``FrameworkRules`` subclass decorates itself with
``@register_framework("CODE")`` to land in :data:`FRAMEWORK_REGISTRY`,
keyed by its canonical code. The aggregator looks the right ruleset up
by code at runtime, so adding a new framework is purely additive (drop a
new module, import it from the bottom of this file).

Importing this package eagerly imports every implemented framework so
the registry is populated before anyone tries to use it — there's no
lazy registration step the caller has to remember.
"""
from __future__ import annotations

from .base import FrameworkRules

# Module-level registry: canonical framework code -> rules class.
# Populated lazily as each framework module imports and decorates itself.
FRAMEWORK_REGISTRY: dict[str, type[FrameworkRules]] = {}


def register_framework(name: str):
    """Class decorator: register a ``FrameworkRules`` subclass under ``name``.

    The decorator simply stores the class in ``FRAMEWORK_REGISTRY`` and
    returns it unchanged, so the decorated class is otherwise normal.

    Example:
        >>> @register_framework("SOX")
        ... class SOXRules(FrameworkRules):
        ...     name = "SOX"
    """

    def _decorator(cls: type[FrameworkRules]) -> type[FrameworkRules]:
        FRAMEWORK_REGISTRY[name] = cls
        return cls

    return _decorator


__all__ = ["FRAMEWORK_REGISTRY", "FrameworkRules", "register_framework"]


# Eagerly import concrete framework modules at the bottom of this file so
# the registry is populated as soon as the package is imported. Each
# module's ``@register_framework(...)`` decorator runs on import and
# adds an entry to ``FRAMEWORK_REGISTRY``. HIPAA / PCI-DSS / GDPR /
# FinHealth modules are added in later commits.
from . import sox  # noqa: E402, F401  (import-after-definition is intentional)

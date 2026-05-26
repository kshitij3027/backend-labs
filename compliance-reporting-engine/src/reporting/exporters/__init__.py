"""Exporter sub-package — turns an aggregator payload into format-specific bytes.

Each exporter is a small pure function ``(payload: dict) -> bytes`` that
serialises the canonical payload (see
:mod:`src.reporting.aggregator`) into one of the four supported wire
formats: ``PDF``, ``CSV``, ``JSON``, ``XML``. Exporters self-register
into the :data:`EXPORTERS` mapping via the :func:`register_exporter`
decorator at import time, which lets the coordinator look them up by
format code without hard-wiring any imports.

Add a new format by dropping a new module here, decorating its export
function with ``@register_exporter("YOURFORMAT")``, and importing the
module from the bottom of this file so the decorator runs on package
import. No registry edits needed elsewhere.
"""
from __future__ import annotations

from typing import Callable

#: Format-code (upper-cased) -> exporter function. Populated by
#: ``@register_exporter`` calls in the sibling modules at import time.
EXPORTERS: dict[str, Callable[[dict], bytes]] = {}


def register_exporter(format_code: str) -> Callable[[Callable[[dict], bytes]], Callable[[dict], bytes]]:
    """Decorator: register an exporter function under an upper-cased format code.

    Args:
        format_code: Short identifier (``"JSON"``, ``"XML"``, ...). Always
            stored upper-cased so the coordinator can look up by either
            casing without having to normalise on both sides.

    Returns:
        The decorator. The decorated function is returned unchanged so
        registration is transparent at the call site.
    """
    code = format_code.upper()

    def _decorator(fn: Callable[[dict], bytes]) -> Callable[[dict], bytes]:
        EXPORTERS[code] = fn
        return fn

    return _decorator


# Eager-import the per-format modules so their ``@register_exporter``
# decorators fire at package import time. Without this, the
# ``EXPORTERS`` dict stays empty until something happens to import the
# concrete modules directly.
from . import json_exporter  # noqa: E402,F401
from . import xml_exporter  # noqa: E402,F401


__all__ = ["EXPORTERS", "register_exporter"]

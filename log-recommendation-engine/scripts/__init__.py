"""Operational entrypoints (seed / E2E / perf) runnable inside the container.

Marked as a package so ``python -m scripts.<name>`` resolves from the ``/app``
WORKDIR (where ``PYTHONPATH=/app`` makes both ``src`` and ``scripts`` importable).
"""

from __future__ import annotations

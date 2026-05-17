"""Atomic in-process counters for the field-level encryption service.

The dashboard (C8) and the ``GET /api/stats`` endpoint (C7) both read
from a single shared :class:`StatsCounters` instance that the C5
:class:`~src.processor.log_processor.LogProcessor` increments on every
encrypt / decrypt / error.

Counters are integers protected by a single ``threading.Lock`` so they
are safe to call from the parallel encrypt path (C5) and the rotation
background task (C7) without races.

Public surface:

* :class:`StatsCounters` — thread-safe counters with the standard set of
  well-known names pre-populated at zero so the dashboard never sees a
  ``KeyError`` on a fresh process.
"""
from __future__ import annotations

from .counters import StatsCounters

__all__ = ["StatsCounters"]

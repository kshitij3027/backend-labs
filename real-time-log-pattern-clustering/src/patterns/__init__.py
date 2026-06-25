"""Batch pattern-mining modules for the Real-Time Log Pattern Clustering engine.

This package holds the *extended-homework* offline miners that run over the warm-up
corpus (Feature Areas A + C):

* :mod:`src.patterns.temporal` — discovers recurring **temporal** patterns (time-of-day /
  weekday signal): nightly error spikes, weekday service bursts, business-hours
  performance degradation, and hourly volume peaks.
* :mod:`src.patterns.performance` — mines **performance** patterns: response-time bands
  (fast/normal/slow/critical) and per-service/endpoint bottleneck signatures.

Both operate on the same :class:`~src.schemas.LogEntry` / parsed-dict inputs the rest of
the engine uses (reusing :func:`src.preprocessing.parse_log`) and are surfaced through the
``GET /patterns/temporal`` and ``GET /patterns/performance`` REST endpoints.
"""

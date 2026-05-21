"""Integration tests for the live HTTP surface.

Tests in this package drive the FastAPI app through ``httpx.AsyncClient``
wrapped in ``asgi_lifespan.LifespanManager`` so the C7 lifespan handler
runs end-to-end and every singleton on ``app.state`` is constructed
exactly as it would be in production.

Distinct from ``tests/unit`` because:

* These tests instantiate the full ``RedactionProcessor`` pipeline
  (detector + strategies + config manager + audit + stats) rather than
  a stripped-down stub.
* They exercise the wire-format pydantic schemas in :mod:`src.api.models`,
  which the unit tests bypass entirely.
* They share a session-scoped client fixture (see ``conftest.py`` in
  this directory if added; otherwise per-test fixtures inline) so the
  spaCy model load happens at most once per session — the heaviest cost
  in the test run.
"""

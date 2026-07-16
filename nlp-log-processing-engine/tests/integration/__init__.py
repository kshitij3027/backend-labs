"""Integration tests — the HTTP surface exercised against a fully loaded NLP engine.

Unlike the unit suite (which injects a cheap, engine-less Runtime so the app never loads a
model), these tests drive ``/api/analyze`` and friends through a real, loaded
:class:`~src.nlp.NLPEngine`. The model is loaded once for the whole package via the
session-scoped ``loaded_client`` fixture in ``conftest.py``.
"""

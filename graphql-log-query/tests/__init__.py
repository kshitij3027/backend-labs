"""Test suite for the GraphQL Log Query Platform.

A real package (rather than rootdir-relative test modules) so ``tests.unit`` and
``tests.integration`` can hold same-named modules without pytest's module-name collision, and so
a helper is importable as ``from tests.integration.conftest import …`` when one is needed.
``PYTHONPATH=/app`` in both images makes that resolve.
"""

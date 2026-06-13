"""Smoke tests: prove the package imports and the pytest harness runs.

Intentionally dependency-free. Only ``app`` (i.e. ``app/__init__.py``) is
imported here, since no ``app.*`` submodules exist at this commit.
"""


def test_app_package_imports():
    """The top-level ``app`` package imports without raising."""
    import app

    assert app is not None


def test_smoke_truth():
    """The test harness collects and runs a trivial assertion."""
    assert True

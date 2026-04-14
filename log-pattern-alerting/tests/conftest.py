"""Shared pytest configuration and fixtures."""


def pytest_sessionfinish(session, exitstatus):
    """Treat 'no tests collected' (exit code 5) as success."""
    if exitstatus == 5:
        session.exitstatus = 0

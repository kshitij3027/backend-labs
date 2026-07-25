"""Integration tests: the real application construction path, in-process.

These build the app the way production does — through :func:`src.main.create_app` and **through
the lifespan** — and drive it over the ASGI transport. That is what separates them from the unit
tests (which skip the lifespan) and from ``make e2e`` (which drives a genuinely separate
container over a real socket). From C2 they also talk to the live Postgres and Redis services by
name, which is why the compose ``test`` service waits on both being healthy.
"""

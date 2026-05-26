"""HTTP API sub-package: FastAPI routers and Pydantic schemas.

Each ``routes_*`` module owns one resource family and exposes a single
``router`` symbol that's mounted from :mod:`src.main`. The
:mod:`schemas` module holds the request/response Pydantic models
shared across routers.
"""

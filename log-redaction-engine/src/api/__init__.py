"""HTTP surface package for the log redaction engine (C7).

Exposes the public FastAPI router plus the request/response pydantic
models. The application bootstrap in :mod:`src.main` is the sole
consumer — it imports ``router`` and mounts it on the live app.

Keeping the package's surface tiny (no re-exports of internal helpers
like ``_value_preview``) gives the consumer a clean import line and
keeps the test layer free to import sub-modules directly when it
needs them.
"""
